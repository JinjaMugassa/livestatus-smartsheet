import os
import time
import json
import requests
from dotenv import load_dotenv
from datetime import datetime
import pytz
import pandas as pd
import smartsheet

# --------------------------------------------------
# Load env
# --------------------------------------------------
load_dotenv()

WIALON_TOKEN = os.getenv("WIALON_TOKEN")
RESOURCE_ID = int(os.getenv("GEOFENCE_RESOURCE_ID"))  # Should be 22504459
LIVE_STATUS_TEMPLATE_ID = int(os.getenv("LIVE_STATUS_TEMPLATE_ID"))  # 8
REPORT_OBJECT_ID = int(os.getenv("REPORT_OBJECT_ID"))  # 600212044 for TRANSIT_ALL_TRUCKS

SM_TOKEN = os.getenv("SM_TOKEN")
LIVE_SHEET_ID = int(os.getenv("LIVE_SHEET_ID"))

BASE_URL = "https://hst-api.wialon.eu/wialon/ajax.html"

# Tanzania timezone offset in seconds (UTC+3 = 10800 seconds)
TZ_OFFSET = 10800

# --------------------------------------------------
# Core Wialon request helper
# --------------------------------------------------
def wialon_call(service, params=None, sid=None):
    if params is None:
        params = {}

    payload = {"svc": service, "params": json.dumps(params)}
    if sid:
        payload["sid"] = sid

    r = requests.post(BASE_URL, data=payload, timeout=60)
    r.raise_for_status()
    res = r.json()
    if isinstance(res, dict) and res.get("error"):
        raise Exception(f"Wialon API error ({service}): {res}")
    return res

# --------------------------------------------------
# Wialon functions
# --------------------------------------------------
def login(token):
    res = wialon_call("token/login", {"token": token})
    return res["eid"]

def exec_live_status_report(sid):
    """Execute live status report (no time interval needed)"""
    params = {
        "reportResourceId": RESOURCE_ID,
        "reportTemplateId": LIVE_STATUS_TEMPLATE_ID,
        "reportObjectId": REPORT_OBJECT_ID,
        "reportObjectSecId": 0,
        "interval": {
            "from": int(datetime.now().timestamp()),
            "to": int(datetime.now().timestamp()),
            "flags": 0
        },
        "tzOffset": TZ_OFFSET
    }
    return wialon_call("report/exec_report", params, sid)

def fetch_table_rows(sid, table_index, row_count):
    if row_count <= 0:
        return []
    return wialon_call(
        "report/select_result_rows",
        {
            "tableIndex": table_index,
            "config": {"type": "range", "data": {"from": 0, "to": row_count - 1, "level": 0}}
        },
        sid
    )

def parse_live_status_rows(rows, headers):
    """Parse live status report rows"""
    parsed_rows = []
    tz_tz = pytz.timezone("Africa/Dar_es_Salaam")
    
    for row in rows:
        row_data = []
        
        # Check if row has 'c' attribute (cells)
        if isinstance(row, dict) and 'c' in row:
            cells = row['c']
        elif isinstance(row, list):
            cells = row
        else:
            continue
            
        for i, cell in enumerate(cells):
            col_name = headers[i].lower() if i < len(headers) else ""
            
            # Handle cell values
            if isinstance(cell, dict):
                # Check for 't' (text) or 'v' (value)
                cell_value = cell.get('t') or cell.get('v')
                
                # If this is "Last message" column, try to parse date
                if 'last message' in col_name or 'message' in col_name:
                    if 'v' in cell:
                        # It's a timestamp
                        try:
                            dt = datetime.utcfromtimestamp(cell['v']).replace(tzinfo=pytz.utc)
                            dt = dt.astimezone(tz_tz)
                            cell_value = dt.strftime("%Y-%m-%d %H:%M:%S")
                        except:
                            cell_value = str(cell_value) if cell_value else ""
                    elif 't' in cell:
                        # It's already formatted text
                        try:
                            # Try to parse and reformat if possible
                            dt = datetime.strptime(cell['t'], "%d.%m.%Y %I:%M:%S %p")
                            dt = tz_tz.localize(dt)
                            cell_value = dt.strftime("%Y-%m-%d %H:%M:%S")
                        except:
                            cell_value = cell['t']
                
                row_data.append(str(cell_value) if cell_value is not None else "")
            else:
                row_data.append(str(cell) if cell is not None else "")
        
        if row_data:
            parsed_rows.append(row_data)
    
    return parsed_rows

# --------------------------------------------------
# Smartsheet helper with column mapping
# --------------------------------------------------
def push_to_smartsheet(df: pd.DataFrame, sheet_id: int, token: str):
    """
    Push data to Smartsheet with column mapping:
    - Grouping -> VRN_LOCATION_TBL
    - Location -> LOCATION
    - Last message (date only) -> Date Of Last Tx
    """
    sm_client = smartsheet.Smartsheet(token)
    sm_client.errors_as_exceptions(True)

    # Get sheet and columns with their types
    sheet = sm_client.Sheets.get_sheet(sheet_id)
    columns = {}
    column_types = {}
    for col in sheet.columns:
        if col.title and col.title.strip():
            col_name = col.title.strip()
            columns[col_name] = col.id
            column_types[col_name] = col.type

    print(f"📋 Smartsheet columns: {list(columns.keys())}")
    print(f"📋 DataFrame columns: {list(df.columns)}")

    # Column mapping: DataFrame column -> Smartsheet column
    column_mapping = {
        'Grouping': 'VRN_LOCATION_TBL',
        'Location': 'LOCATION',
        'Last message': 'Date Of Last Tx'
    }

    # Rename DataFrame columns according to mapping
    df_renamed = df.copy()
    for old_col, new_col in column_mapping.items():
        if old_col in df_renamed.columns:
            df_renamed.rename(columns={old_col: new_col}, inplace=True)

    # Only keep columns that exist in Smartsheet
    df_columns = [col for col in df_renamed.columns if col in columns]
    
    if not df_columns:
        print("❌ ERROR: No matching columns!")
        print(f"   DataFrame has: {list(df_renamed.columns)}")
        print(f"   Smartsheet has: {list(columns.keys())}")
        return
    
    df_renamed = df_renamed[df_columns]
    print(f"✅ Matched {len(df_columns)} columns: {df_columns}")
    
    # Show column types
    print("\n🔍 Column types:")
    for col in df_columns:
        print(f"   '{col}' -> {column_types.get(col)}")

    # Delete existing rows in batches (to avoid URI too large error)
    existing_row_ids = [r.id for r in sheet.rows]
    if existing_row_ids:
        print(f"\n🗑 Deleting {len(existing_row_ids)} existing rows...")
        delete_batch_size = 200  # Delete max 200 rows at a time
        total_deleted = 0
        
        for i in range(0, len(existing_row_ids), delete_batch_size):
            batch = existing_row_ids[i:i + delete_batch_size]
            sm_client.Sheets.delete_rows(sheet_id, batch, ignore_rows_not_found=True)
            total_deleted += len(batch)
            print(f"   Deleted batch {i//delete_batch_size + 1}: {len(batch)} rows (Total: {total_deleted}/{len(existing_row_ids)})")
        
        print(f"✅ Deleted all {total_deleted} rows")

    # Build rows using Smartsheet models
    new_rows = []
    for _, row_data in df_renamed.iterrows():
        new_row = sm_client.models.Row()
        new_row.to_top = True
        
        for col_name in df_columns:
            cell = sm_client.models.Cell()
            cell.column_id = columns[col_name]
            
            value = row_data[col_name]
            
            # Handle different column types
            if pd.isna(value):
                cell.value = ""
            else:
                col_type = column_types.get(col_name, "TEXT_NUMBER")
                
                # Handle DATE and DATETIME columns
                if col_type in ["DATE", "DATETIME"]:
                    if isinstance(value, str):
                        try:
                            dt = pd.to_datetime(value)
                            # For DATE columns, use ISO date format (YYYY-MM-DD)
                            if col_type == "DATE":
                                cell.value = dt.strftime("%Y-%m-%d")
                            else:  # DATETIME
                                cell.value = dt.strftime("%Y-%m-%dT%H:%M:%S")
                        except:
                            cell.value = str(value)
                    else:
                        cell.value = str(value)
                else:
                    cell.value = str(value) if not isinstance(value, str) else value
            
            new_row.cells.append(cell)
        
        new_rows.append(new_row)

    # Add rows in batches
    batch_size = 500
    total_added = 0
    
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i:i + batch_size]
        sm_client.Sheets.add_rows(sheet_id, batch)
        total_added += len(batch)
        print(f"✅ Added batch {i//batch_size + 1}: {len(batch)} rows (Total: {total_added}/{len(new_rows)})")

    print(f"\n🎉 Successfully added {total_added} rows to Smartsheet")

# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    print("🔐 Logging in to Wialon...")
    sid = login(WIALON_TOKEN)
    print("✅ Logged in")

    print("🚀 Executing live status report for TRANSIT_ALL_TRUCKS...")
    exec_res = exec_live_status_report(sid)

    # Live status reports usually return immediately
    if "reportResult" in exec_res:
        print("⚡ Report returned immediately")
        report_data = exec_res["reportResult"]
    else:
        print(f"⚠️ Unexpected response: {exec_res}")
        return

    tables = report_data.get("tables", [])
    if not tables:
        print("⚠️ No tables in report")
        return

    table = tables[0]
    headers = table.get("header", [])
    row_count = table.get("rows", 0)

    print(f"📊 Table '{table.get('name')}' → {row_count} rows")
    print(f"📋 Headers: {headers}")

    if row_count == 0:
        print("⚠️ No rows in report")
        return

    rows = fetch_table_rows(sid, table_index=0, row_count=row_count)
    if not rows:
        print("⚠️ No row data returned")
        return

    parsed_rows = parse_live_status_rows(rows, headers)

    df = pd.DataFrame(parsed_rows, columns=headers)
    df = df.dropna(how="all")

    print("\n✅ Live status report pulled successfully")
    print(f"Total rows: {len(df)}")
    print("\nSample data:")
    print(df.head(5).to_string())

    # Save to Excel (optional)
    df.to_excel("live_status_report.xlsx", index=False)
    print("\n💾 Saved to live_status_report.xlsx")

    # --------------------------------------------------
    # Push to Smartsheet
    # --------------------------------------------------
    print("\n🚀 Pushing data to Smartsheet...")
    push_to_smartsheet(df, LIVE_SHEET_ID, SM_TOKEN)
    print("✅ Smartsheet update complete")

if __name__ == "__main__":
    main()