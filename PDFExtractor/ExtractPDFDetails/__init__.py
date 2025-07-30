import logging
import azure.functions as func
import fitz  # PyMuPDF
import io
import json
import pandas as pd
from dotenv import load_dotenv
import os
import time
from datetime import datetime
import math
import traceback
import requests

# Load environment variables from .env file in the same directory
load_dotenv()

SITE_URL = os.environ.get("SHAREPOINT_SITE_URL")
SITE_NAME = os.environ.get("SHAREPOINT_SITE_NAME")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
TENANT_ID = os.environ.get("TENANT_ID")
LIST_NAME = os.environ.get("SHAREPOINT_LIST_NAME")
WELLPLANAON_LIST_NAME = os.environ.get("SHAREPOINT_WELLPLANAON_LIST_NAME")
OUTPUT_LIBRARY = os.environ.get("SHAREPOINT_OUTPUT_LIBRARY")
GRAPH_BASE = os.getenv("GRAPH_BASE", "https://graph.microsoft.com/v1.0")  # Default fallback

def get_graph_token():
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
    headers = {"Accept": "application/json"}
    resp = requests.post(token_url, data=payload, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]

def graph_headers():
    return {
        "Authorization": f"Bearer {get_graph_token()}",
        "Accept": "application/json"
    }

def get_site_id():
    # Extract tenant domain and site name from full site URL
    site_hostname = SITE_URL.split("/")[2]  # "slb001.sharepoint.com"
    site_path = "/" + "/".join(SITE_URL.split("/")[3:])  # "/sites/ADNOCDevelopment
    
    # Format URL as per Microsoft Graph recommendations
    url = f"{GRAPH_BASE}/sites/{site_hostname}:{site_path}"
    
    headers = graph_headers()
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    
    site_id = resp.json()["id"]
   # print(f"Resolved Site ID: {site_id}")
    return site_id


def get_list_id(site_id, list_name):
    url = f"{GRAPH_BASE}/sites/{site_id}/lists"
    headers = graph_headers()
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    for l in resp.json().get("value", []):
        if l["name"] == list_name:
           # print(f"Resolved List ID for '{list_name}': {l['id']}")
            return l["id"]
    raise Exception(f"List '{list_name}' not found in site {site_id}")

def safe_strip(val):
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    return str(val).strip()

def push_to_sharepoint(values, max_retries=3):
    site_id = get_site_id()
    list_id = get_list_id(site_id, LIST_NAME)
    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items"
    headers = graph_headers()
    headers["Content-Type"] = "application/json"
    for value in values:
        item_properties = {
            "fields": {
                "Title": str(value.get("Date", "")),
                "Rig": str(value.get("Rig", "")),
                "Well": str(value.get("Well", "")),
                "BP": str(value.get("BP", "")),
                "EP": str(value.get("EP1", "")),
                "Actuals": str(value.get("Actuals", "")),
                "NextLOC": str(value.get("NextLOC", "")),
                "NextMoveDate": str(value.get("NextMoveDate", "")),
            }
        }
        retries = 0
        while retries < max_retries:
            try:
                resp = requests.post(url, headers=headers, json=item_properties)
                if resp.status_code == 503:
                    retries += 1
                    wait_time = 2 ** retries
                    print(f"503 error, retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                elif resp.ok:
                    print(f"Successfully added Well: {item_properties['fields'].get('Well', '')}")
                    break
                else:
                    print(f"Failed to add item to SharePoint: {resp.text}")
                    break
            except Exception as e:
                print(f"Failed to add item to SharePoint: {e}")
                break

def fetch_filtered_wellplanaon_entries(rig, next_loc, max_retries=3):
    site_id = get_site_id()
    list_id = get_list_id(site_id, WELLPLANAON_LIST_NAME)
    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items"
    headers = graph_headers()
    filter_query = f"fields/RigName eq '{rig}' and fields/WellName eq '{next_loc}'"
    params = {"$filter": filter_query,"$expand": "fields"}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    items = resp.json().get("value", [])
    results = []
    for item in items:
        #print(">> Full item from Graph response:", json.dumps(item, indent=2))
        fields = item.get("fields", {})
        #print(">> Raw SharePoint fields:", json.dumps(fields, indent=2))
        start_date = fields.get('StartDate')
        end_date = fields.get('EndDate')
        diff_days = ""
        try:
            if start_date and end_date:
                start_dt = datetime.strptime(start_date[:10], "%Y-%m-%d")
                end_dt = datetime.strptime(end_date[:10], "%Y-%m-%d")
                diff_days = (end_dt - start_dt).days
        except Exception as e:
            diff_days = f"Error: {e}"
        fields["DaysDiff"] = diff_days
        fields["ID"] = item.get("id")
        results.append(fields)
    return results

def update_sharepoint_list_item(item_id, start_date, end_date):
    site_id = get_site_id()
    list_id = get_list_id(site_id, WELLPLANAON_LIST_NAME)
    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    headers = graph_headers()
    headers["Content-Type"] = "application/json"
    payload = {
        "StartDate": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "EndDate": end_date.strftime("%Y-%m-%dT%H:%M:%S")
    }
    resp = requests.patch(url, headers=headers, json=payload)
    resp.raise_for_status()
    print(f"Updated item ID {item_id} with StartDate {start_date} and EndDate {end_date}")

def upload_no_entries_log_to_sharepoint(no_entries_log, file_name_prefix="NoEntriesFound"):
    if not no_entries_log:
        return
    df = pd.DataFrame(no_entries_log)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{file_name_prefix}_{timestamp}.xlsx"
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)
    site_id = get_site_id()
    # Find the drive (document library) by name
    drive_url = f"{GRAPH_BASE}/sites/{site_id}/drives"
    headers = graph_headers()
    resp = requests.get(drive_url, headers=headers)
    resp.raise_for_status()
    drives = resp.json().get("value", [])
    drive_id = None
    for d in drives:
        if d.get("name") == OUTPUT_LIBRARY:
            drive_id = d["id"]
            break
    if not drive_id:
        print(f"Drive (library) '{OUTPUT_LIBRARY}' not found.")
        return
    upload_url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{file_name}:/content"
    headers = graph_headers()
    headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    resp = requests.put(upload_url, headers=headers, data=excel_buffer.getvalue())
    resp.raise_for_status()
    file_info = resp.json()
    file_url = file_info.get("webUrl")
    #print(f"Uploaded file to SharePoint: {file_name}")
    #print(f"File URL: {file_url}")
    return file_url

def extract_tables_from_pdf(pdf_stream):
    doc = fitz.open(stream=pdf_stream, filetype="pdf")
    all_values = []
    index_counter = 0  # Initialize the index counter
    for page_num in range(len(doc)):
        page = doc[page_num]
        tables = page.find_tables()
        if tables:
            for t in tables:
                df = t.to_pandas()
                dates = []
                rigs = []
                wells = []
                bp = []
                EP1 = []
                Actuals = []
                NextLOC = []
                NextMoveDate = []
                # Check if the first value in column 0 is 'Well Description'
                first_column_values = df[df.columns[0]].tolist() if not df.empty else []
                if not first_column_values or str(first_column_values[0]).strip() != "Well Description":
                    # Skip this page if the first value is not 'Well Description'
                    continue
                else:
                    for index, column in enumerate(df.columns):
                        if index in [0, 27, 36]:
                            column_values = df[column].tolist()
                            # Column 0: RIG, WELL, DATE extraction
                            if index == 0 and isinstance(column, str):
                                # Extract RIG
                                if "RIG:" in column:
                                    try:
                                        rig_match = column.split("RIG:")[1].split()[0]
                                        rigs.append(rig_match)
                                    except Exception:
                                        pass
                                # Extract WELL
                                if "WELL:" in column:
                                    try:
                                        well_match = column.split("WELL:")[1].split()[0]
                                        wells.append(well_match)
                                    except Exception:
                                        pass
                                # Extract DATE
                                if "DATE:" in column:
                                    try:
                                        date_match = column.split("DATE:")[1].strip().split()[0:3]
                                        import re
                                        date_raw = " ".join(date_match)
                                        date_raw = re.sub(r",", "", date_raw)
                                        from datetime import datetime
                                        try:
                                            date_obj = datetime.strptime(date_raw, "%b %d %Y")
                                            date_fmt = date_obj.strftime("%d/%m/%Y")
                                            dates.append(date_fmt)
                                        except Exception:
                                            pass
                                    except Exception:
                                        pass

                            # Column 27: Actuals, BP, EP1 extraction
                            if index == 27:
                                for val in column_values:
                                    if isinstance(val, str):
                                        if val.strip().startswith("Act. Days:"):
                                            try:
                                                actual_val = val.strip().split("Act. Days:")[1].strip()
                                                Actuals.append(actual_val)
                                            except Exception:
                                                pass
                                        elif val.strip().startswith("BP Days:"):
                                            try:
                                                bp_val = val.strip().split("BP Days:")[1].strip()
                                                bp.append(bp_val)
                                            except Exception:
                                                pass
                                        elif val.strip().startswith("EP1 Days:"):
                                            try:
                                                ep1_val = val.strip().split("EP1 Days:")[1].strip()
                                                EP1.append(ep1_val)
                                            except Exception:
                                                pass

                            # Column 36: NextLOC extraction
                            if index == 36:
                                for val in column_values:
                                    if isinstance(val, str):
                                        if val.strip().startswith("Next Loc:"):
                                            try:
                                                next_loc_val = val.strip().split("Next Loc:")[1].strip()
                                                NextLOC.append(next_loc_val)
                                            except Exception:
                                                pass
                                        elif val.strip().startswith("Next Move:"):
                                            try:
                                                # Extract date string after 'Next Move:'
                                                date_str = val.strip().split("Next Move:")[1].strip()
                                                if date_str:
                                                    import re
                                                    date_str = re.sub(r",", "", date_str)
                                                    parts = date_str.split()
                                                    # Expecting ['Aug', '16', '2025']
                                                    if len(parts) == 3 and len(parts[2]) == 4:
                                                        # Normal case
                                                        try:
                                                            date_obj = datetime.strptime(date_str, "%b %d %Y")
                                                            date_fmt = date_obj.strftime("%d/%m/%Y")
                                                            NextMoveDate.append(date_fmt)
                                                        except Exception:
                                                            NextMoveDate.append(date_str)
                                                    elif len(parts) == 3 and len(parts[2]) == 3:
                                                        # Incomplete year, try to guess or skip
                                                        print(f"Warning: Incomplete year in Next Move date: {date_str}")
                                                        NextMoveDate.append(date_str)
                                                    else:
                                                        # Unexpected format
                                                        print(f"Warning: Unexpected Next Move date format: {date_str}")
                                                        NextMoveDate.append(date_str)
                                            except Exception:
                                                pass

                    # Combine all values into a single array for the current page
                    if dates and rigs and wells:
                        record = {
                            "ID": index_counter,
                            "Date": safe_strip(dates[0]) if dates else "",
                            "Rig": safe_strip(rigs[0]) if rigs else "",
                            "Well": safe_strip(wells[0]) if wells else "",
                            "BP": safe_strip(bp[0]) if bp else "",
                            "EP1": safe_strip(EP1[0]) if EP1 else "",
                            "Actuals": safe_strip(Actuals[0]) if Actuals else "",
                            "NextLOC": safe_strip(NextLOC[0]) if NextLOC else "",
                            "NextMoveDate": safe_strip(NextMoveDate[0]) if NextMoveDate else "",
                        }
                        all_values.append(record)
                        index_counter += 1  # Increment the index counter

    doc.close()
    return all_values

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        # Get PDF bytes from HTTP request body
        pdf_bytes = req.get_body()
        if not pdf_bytes:
            return func.HttpResponse(
                "No PDF content found in request body", status_code=400
            )
        
        pdf_stream = io.BytesIO(pdf_bytes)
        all_values = extract_tables_from_pdf(pdf_stream)
        unique_wells = {}
        for row in all_values:
            well = row["Well"]
            if well not in unique_wells:
                unique_wells[well] = row

        unique_data = list(unique_wells.values())
        print("Total number of Unique Wells found:", len(unique_data))

        # Call push_to_sharepoint before updating WellPlanAON entries
        push_to_sharepoint(unique_data)

        no_entries_log = []

        #Fetch filtered Wellplanaon entries for each unique well
        for entry in unique_data:
            rig = entry.get("Rig", "")
            next_loc = entry.get("NextLOC", "")
            filtered = fetch_filtered_wellplanaon_entries(rig, next_loc)
            if filtered:
                print(f"Filtered entries for Well: {next_loc}, Rig: {rig}")
                for item in filtered:
                    try:
                        next_move_date = entry.get("NextMoveDate", "")
                        print(f"Next move date:{next_move_date}")
                        if next_move_date:
                            start_date = parse_date(next_move_date)
                            #print(f"Parsed Start Date: {start_date} (type: {type(start_date)})")
                            item_start_date_str = item.get("StartDate")
                            #print(f"Str Start Date: {item_start_date_str} (type: {type(item_start_date_str)})")
                            # Parse item_start_date_str to datetime for accurate comparison
                            item_start_date = None
                            if item_start_date_str:
                                try:
                                    item_start_date = parse_date(item_start_date_str)
                                    #print(f"Parsed Item Start Date: {item_start_date} (type: {type(item_start_date)})")
                                except Exception as ex:
                                    print(f"Error parsing item_start_date_str: {ex}")
                            # Only update if dates are different
                            if not item_start_date or item_start_date.date() != start_date.date():
                                diff_days = item.get("DaysDiff")
                                if diff_days is None or isinstance(diff_days, str):
                                    s = item.get("StartDate")
                                    e = item.get("EndDate")
                                    print(f"Raw Start: {s}, Raw End: {e}")
                                    if s and e:
                                        try:
                                            s_dt = parse_date(s)
                                            e_dt = parse_date(e)
                                            diff_days = (e_dt - s_dt).days
                                        except Exception as ex:
                                            print(f"Error parsing dates: {ex}")
                                            diff_days = 0
                                    else:
                                        diff_days = 0
                                end_date = start_date + pd.Timedelta(days=diff_days)
                                #print(f"StartDate: {start_date}, EndDate: {end_date}, DiffDays: {diff_days}")
                                update_sharepoint_list_item(item['ID'], start_date, end_date)
                            else:
                                print(f"Skipped update for item ID {item['ID']} as StartDate matches NextMoveDate")
                    except Exception as ex:
                        logging.error(f"Error updating item ID {item.get('ID')}: {ex}")
                        no_entries_log.append({
                            "Well": next_loc,
                            "Rig": rig,
                            "ItemID": item.get('ID'),
                            "Error": str(ex)
                        })
            else:    
                print(f"No entries found for Well: {next_loc}, Rig: {rig}")
                no_entries_log.append({"Well": next_loc, "Rig": rig})

        uploaded_file_url = upload_no_entries_log_to_sharepoint(no_entries_log)


        # Always return a valid JSON response
        result = {
            "message": "PDF processed successfully!",
            "tables_extracted": len(all_values),
            "Total number of Unique Wells found:": len(unique_data),
            "uploaded_file_url": uploaded_file_url
        }
        return func.HttpResponse(
            body=json.dumps(result, indent=4),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        logging.error(f"Error in processing request: {e}\n{traceback.format_exc()}")
        return func.HttpResponse(
            "Internal server error: " + str(e), status_code=500
        )

def parse_date(date_str):
    """
    Parse a date string in various formats to a datetime object.
    Supported formats: 'dd/mm/yyyy', 'yyyy-mm-dd', 'MMM dd yyyy', ISO 8601, etc.
    """
    from datetime import datetime
    import re
    # Remove trailing Z if present
    date_str = date_str.strip()
    if date_str.endswith("Z"):
        date_str = date_str[:-1]
    # Try known formats
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%b %d %Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(date_str, fmt)
        except Exception:
            continue
    # Try to parse with pandas if all else fails
    try:
        return pd.to_datetime(date_str)
    except Exception:
        pass
    raise ValueError(f"Unrecognized date format: {date_str}")




