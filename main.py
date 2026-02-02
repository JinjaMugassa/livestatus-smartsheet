import os
import time
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import pandas as pd
import smartsheet

# --------------------------------------------------
# Load env
# --------------------------------------------------
load_dotenv()

WIALON_TOKEN = os.getenv("WIALON_TOKEN")
RESOURCE_ID = int(os.getenv("GEOFENCE_RESOURCE_ID"))
TEMPLATE_ID = int(os.getenv("GEOFENCE_TEMPLATE_ID"))
OBJECT_ID = int(os.getenv("REPORT_OBJECT_ID"))

SM_TOKEN = os.getenv("SM_TOKEN")
SM_SHEET_ID = int(os.getenv("SHEET_ID"))

BASE_URL = "https://hst-api.wialon.eu/wialon/ajax.html"

# --------------------------------------------------
# Core Wialon request helper
# --------------------------------------------------
def wialon_call(service, params=None, sid=None):
    if params is None:
        params = {}

    payload = {"svc": service, "params": json.dumps(params)}
    if sid:
        payload["sid"] = sid

    r = requests.post(BASE_URL, data=payload, timeout=120)
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

def exec_report(sid, time_from, time_to):
    params = {
        "reportResourceId": RESOURCE_ID,
        "reportTemplateId": TEMPLATE_ID,
        "reportObjectId": OBJECT_ID,
        "reportObjectSecId": 0,
        "interval": {"from": time_from, "to": time_to, "flags": 0}
    }
    return wialon_call("report/exec_report", params, sid)

def wait_for_report(sid, rid, timeout=300):
    start = time.time()
    while True:
        status = wialon_call("report/get_report_status", {"rid": rid}, sid)
        state = status.get("state") or status.get("status")
        if state in (0, 4):
            return True
        if time.time() - start > timeout:
            return False
        time.sleep(5)

def fetch_table_rows(sid, table_index, row_count):
    if row_count <= 0:
        return []
    return wialon_call(
        "report/select_result_rows",
        {
            "tableIndex": table_index,
            "config": {"type": "range", "data": {"from": 0, "to": row_count, "level": 1}}
        },
        sid
    )

def parse_wialon_rows(rows, headers):
    parsed_rows = []
    tz_tz = pytz.timezone("Africa/Dar_es_Salaam")
    for row in rows:
        if isinstance(row, dict) and "r" in row and isinstance(row["r"], list):
            for nested_row in row["r"]:
                if "c" in nested_row:
                    parsed_rows.append(parse_row_cells(nested_row["c"], headers, tz_tz))
        elif isinstance(row, dict) and "c" in row:
            parsed_rows.append(parse_row_cells(row["c"], headers, tz_tz))
        elif isinstance(row, list):
            parsed_rows.append(row)
    return parsed_rows

def parse_row_cells(cells, headers, tz_tz):
    parsed = []
    for i, cell in enumerate(cells):
        col_name = headers[i].lower() if i < len(headers) else ""
        if isinstance(cell, dict) and ("time" in col_name or "in" in col_name or "out" in col_name):
            if "v" in cell:
                dt = datetime.utcfromtimestamp(cell["v"]).replace(tzinfo=pytz.utc)
                dt = dt.astimezone(tz_tz)
                parsed.append(dt.strftime("%Y-%m-%d %H:%M:%S"))
            elif "t" in cell:
                try:
                    dt = datetime.strptime(cell["t"], "%d.%m.%Y %I:%M:%S %p")
                    dt = tz_tz.localize(dt)
                    parsed.append(dt.strftime("%Y-%m-%d %H:%M:%S"))
                except:
                    parsed.append(cell["t"])
            else:
                parsed.append(None)
        elif isinstance(cell, dict):
            parsed.append(cell.get("t") or cell.get("v") or str(cell))
        else:
            parsed.append(cell)
    return parsed

# --------------------------------------------------
# Fetch report for a date range with retry
# --------------------------------------------------
def fetch_report_chunk(sid, time_from, time_to, chunk_name, max_retries=3):
    """Fetch report for a specific time chunk with retry logic"""
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 30 * (attempt + 1)  # 30s, 60s, 90s
                print(f"  ⏳ Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            
            print(f"  📅 Fetching {chunk_name}...")
            
            exec_res = exec_report(sid, time_from, time_to)

            if "reportResult" in exec_res:
                print(f"  ⚡ {chunk_name} returned immediately")
                report_data = exec_res["reportResult"]
            else:
                rid = exec_res.get("rid")
                if not rid:
                    raise Exception(f"No rid or reportResult returned: {exec_res}")
                print(f"  ⏳ Waiting for {chunk_name} (rid={rid})...")
                if not wait_for_report(sid, rid):
                    raise Exception(f"{chunk_name} timed out")
                report_data = wialon_call("report/get_report_data", {"rid": rid}, sid)

            tables = report_data.get("tables", [])
            if not tables:
                print(f"  ⚠️ No tables in {chunk_name}")
                return None, None

            table = tables[0]
            headers = table.get("header", [])
            row_count = table.get("rows", 0)

            print(f"  ✅ {chunk_name}: {row_count} rows")

            if row_count == 0:
                return headers, []

            rows = fetch_table_rows(sid, table_index=0, row_count=row_count)
            if not rows:
                return headers, []

            parsed_rows = parse_wialon_rows(rows, headers)
            return headers, parsed_rows
            
        except Exception as e:
            error_msg = str(e)
            if "LIMIT msgs_activity" in error_msg:
                if attempt < max_retries - 1:
                    print(f"  ⚠️ Rate limit hit for {chunk_name}, will retry...")
                else:
                    print(f"  ❌ Rate limit - max retries reached for {chunk_name}")
                    raise
            else:
                print(f"  ❌ Error fetching {chunk_name}: {e}")
                raise
    
    return None, None

# --------------------------------------------------
# Smartsheet helper - FIXED
# --------------------------------------------------
def push_to_smartsheet(df: pd.DataFrame, sheet_id: int, token: str):
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

    # Only keep df columns that exist in sheet
    df_columns = [col for col in df.columns if col in columns]
    
    if not df_columns:
        print("❌ ERROR: No matching columns!")
        return
    
    df = df[df_columns]
    print(f"✅ Matched {len(df_columns)} columns")
    
    # Show column types
    print("\n🔍 Column types:")
    for col in df_columns:
        print(f"   '{col}' -> {column_types.get(col)}")

    # Delete existing rows IN BATCHES (FIX for 414 error)
    existing_row_ids = [r.id for r in sheet.rows]
    if existing_row_ids:
        print(f"\n🗑 Deleting {len(existing_row_ids)} existing rows in batches...")
        delete_batch_size = 400  # Smartsheet can handle ~450, use 400 to be safe
        
        for i in range(0, len(existing_row_ids), delete_batch_size):
            batch = existing_row_ids[i:i + delete_batch_size]
            try:
                sm_client.Sheets.delete_rows(sheet_id, batch, ignore_rows_not_found=True)
                print(f"  ✅ Deleted batch {i//delete_batch_size + 1}: {len(batch)} rows")
            except Exception as e:
                print(f"  ⚠️ Error deleting batch {i//delete_batch_size + 1}: {e}")
                # Continue with next batch
            time.sleep(1)  # Small delay between delete batches

    # Build rows using Smartsheet models
    new_rows = []
    for _, row_data in df.iterrows():
        new_row = sm_client.models.Row()
        new_row.to_top = True
        
        for col_name in df_columns:
            cell = sm_client.models.Cell()
            cell.column_id = columns[col_name]
            
            value = row_data[col_name]
            
            if pd.isna(value):
                cell.value = ""
            else:
                col_type = column_types.get(col_name, "TEXT_NUMBER")
                
                if col_type in ["DATE", "DATETIME"]:
                    if isinstance(value, str):
                        try:
                            dt = pd.to_datetime(value)
                            if col_type == "DATE":
                                cell.value = dt.strftime("%Y-%m-%d")
                            else:
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
    
    print(f"\n📤 Adding {len(new_rows)} new rows in batches...")
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i:i + batch_size]
        try:
            result = sm_client.Sheets.add_rows(sheet_id, batch)
            total_added += len(batch)
            print(f"✅ Added batch {i//batch_size + 1}: {len(batch)} rows (Total: {total_added}/{len(new_rows)})")
        except Exception as e:
            print(f"❌ Error adding batch {i//batch_size + 1}: {e}")
        time.sleep(1)  # Small delay between add batches

    print(f"\n🎉 Successfully added {total_added} rows to Smartsheet")


# --------------------------------------------------
# Main with chunking and rate limit handling
# --------------------------------------------------
def main():
    print("🔐 Logging in to Wialon...")
    sid = login(WIALON_TOKEN)
    print("✅ Logged in")

    tz = pytz.timezone("Africa/Dar_es_Salaam")
    now = datetime.now(tz)
    days_ago = now - timedelta(days=50)

    # Split into chunks (7 days each)
    CHUNK_DAYS = 7
    all_data = []
    headers = None
    
    current_start = days_ago
    chunk_num = 1

    print(f"\n📊 Fetching data from {days_ago.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')} ({CHUNK_DAYS}-day chunks)...\n")

    while current_start < now:
        current_end = min(current_start + timedelta(days=CHUNK_DAYS), now)
        
        time_from = int(current_start.timestamp())
        time_to = int(current_end.timestamp())
        
        chunk_name = f"Chunk {chunk_num} ({current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')})"
        
        try:
            chunk_headers, chunk_rows = fetch_report_chunk(sid, time_from, time_to, chunk_name)
            
            if chunk_headers:
                headers = chunk_headers
            
            if chunk_rows:
                all_data.extend(chunk_rows)
            
            # Delay between chunks to avoid rate limits
            time.sleep(5)
            
        except Exception as e:
            print(f"  ❌ Failed to fetch {chunk_name} after retries: {e}")
            # Continue with next chunk instead of failing completely
        
        current_start = current_end
        chunk_num += 1
    
    if not all_data:
        print("\n⚠️ No data retrieved from any chunk")
        return
    
    if not headers:
        print("\n⚠️ No headers found")
        return

    # Create DataFrame from all chunks
    df = pd.DataFrame(all_data, columns=headers)
    df = df.dropna(how="all")
    
    # Remove duplicates if any (based on all columns)
    df = df.drop_duplicates()

    print(f"\n✅ Total geofence records pulled: {len(df)}")
    print("Columns:", list(df.columns))
    print("\nSample data:")
    print(df.head(5).to_string())

    # Save to Excel
    df.to_excel("geofence_report.xlsx", index=False)
    print("\n💾 Saved to geofence_report.xlsx")

    # Push to Smartsheet
    print("\n🚀 Pushing data to Smartsheet...")
    push_to_smartsheet(df, SM_SHEET_ID, SM_TOKEN)
    print("✅ Smartsheet update complete")

if __name__ == "__main__":
    main()