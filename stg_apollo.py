import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from mysql.connector import Error
import re
import json

def get_sheet_id(sheet_url):
    pattern = r"/d/([a-zA-Z0-9_-]+)"
    match = re.search(pattern, sheet_url)
    
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid Google Sheets URL")

def is_valid_json(value):
    if not value:  
        return False
    try:
        json.loads(value)
        return True
    except ValueError:
        return False

def connect_to_google_sheet(sheet_url, sheet_name=None):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(sheet_url)
    if sheet_name:
        sheet = spreadsheet.worksheet(sheet_name)
    else:
        sheet = spreadsheet.sheet1
    return sheet

def get_actual_headers(sheet):
    return sheet.row_values(1)

def map_headers_to_columns(actual_headers, expected_columns):
    header_map = {}
    for header in actual_headers:
        normalized_header = header.lower().replace(" ", "_")
        if normalized_header in expected_columns:
            header_map[header] = normalized_header
        else:
            print(f"Warning: Unrecognized header '{header}' - will be ignored")
    return header_map

def insert_data_to_mysql(db_config, table_name, data, columns, json_columns):
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor()

            formatted_data = []
            for row in data:
                formatted_row = []
                for col in columns:
                    value = row.get(col, '')
                    if col in json_columns and not is_valid_json(value):
                        value = '{}'
                    formatted_row.append(value)
                formatted_data.append(tuple(formatted_row))
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            update_columns = [f"{col}=VALUES({col})" for col in columns]
            update_str = ", ".join(update_columns)
            query = f"""
            INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_str}
            """
            
            cursor.executemany(query, formatted_data)
            connection.commit()
            print(f"Inserted/updated {cursor.rowcount} rows.")
            cursor.close()
            connection.close()
    except Error as e:
        print(f"Error: {e}")

def retrieve_and_process_data(sheet_urls, db_config, table_name, expected_columns, deletion_on_retrieval, username, json_columns, sheet_names=None):
    for i, sheet_url in enumerate(sheet_urls):
        sheet_name = sheet_names[i] if sheet_names and i < len(sheet_names) else None
        try:
            sheet_id = get_sheet_id(sheet_url) 
            sheet = connect_to_google_sheet(sheet_url, sheet_name)
            actual_headers = get_actual_headers(sheet)
            header_map = map_headers_to_columns(actual_headers, expected_columns)
            
            if not header_map:
                print(f"No valid headers found in sheet {i+1}")
                continue

            expected_columns = [
                "person_name", "person_title", "person_email", "person_phone", "person_linkedin", "person_twitter", 
                "person_location", "company_name", "company_industry", "company_specialties", "company_description", 
                "company_fundinground", "company_technologies", "company_founding_year", "company_website", 
                "company_linkedin", "company_twitter", "company_facebook", "company_location", "company_phone", 
                "company_revenue", "company_employee_count", "company_employee_growth", 
                "company_departments_employee_count", "company_departments_employee_growth", "company_job_posting",
                "user_name", "sheet_id","company_profile_url","tag","platform"
            ]

            rows = sheet.get_all_records()
            
            if not rows:
                print(f"No data found in sheet {i+1}")
                continue
            
            # Map the data to our expected columns
            mapped_data = []
            for row in rows:
                mapped_row = {expected_col: row.get(actual_header, '') 
                              for actual_header, expected_col in header_map.items()}
                mapped_row['user_name'] = username
                mapped_row['sheet_id'] = sheet_id
                mapped_data.append(mapped_row)
            
            insert_data_to_mysql(db_config, table_name, mapped_data, expected_columns, json_columns)

            if deletion_on_retrieval:
                sheet.clear()
                sheet.append_row(actual_headers)  # Re-add original headers
                print(f"Cleared data from sheet {i+1}")
        except gspread.exceptions.APIError as e:
            print(f"Error accessing sheet {i+1}: {e}")
        except Exception as e:
            print(f"Unexpected error processing sheet {i+1}: {e}")

def main():
    tyler_sheet_urls = [ "https://docs.google.com/spreadsheets/d/1H-eoU_rQgvvcL4IUCPXjv5MYLZJ0VFgKbh1Q6LgMceM/edit?usp=sharing",
        "https://docs.google.com/spreadsheets/d/1owypkrz53VG5QcONg1GW3K6O12b0bYtcYyDKnfcjSBU/edit?usp=sharing","https://docs.google.com/spreadsheets/d/1Znf-A88hZFLV4CI-IoOiF6yn9p10GcEptlJsXss7JZQ/edit?usp=sharing","https://docs.google.com/spreadsheets/d/18lpu8tXXZh7oyOLoFraP3yihcpAl49Jz8gZrs5AsrMk/edit?usp=sharing","https://docs.google.com/spreadsheets/d/1L8hNmHJOyCGeW7ygcSV7o2Avoys6P1ij2UK-P2FQd5w/edit?usp=sharing"
     ]
    andrew_sheet_urls = [
        "https://docs.google.com/spreadsheets/d/1wB8nVqQYa1PZ8HLnFDjlVc1a_7Kyy8y0Fni2qDRTvbM/edit?usp=sharing", "https://docs.google.com/spreadsheets/d/19R10PZEx7M3qqcEneYy1VMhg1TRjjPsy_5hqmqx09F4/edit?usp=sharing", "https://docs.google.com/spreadsheets/d/1pX9jHNwQ3Mbw-4_kTvS-P-8DtEU-aUqXMiyMYX_Yvjw/edit?usp=sharing", "https://docs.google.com/spreadsheets/d/1Ne4Y62uHT5ozSF3cgy1_Q3df1CXtXtpjrzNc7t6Q2ao/edit?usp=sharing"
    ]

    expected_columns = ["person_name","person_title","person_email","person_phone","person_linkedin","person_twitter","person_location","company_name","company_industry","company_specialties","company_description","company_fundinground","company_technologies","company_founding_year","company_website","company_linkedin","company_twitter","company_facebook","company_location","company_phone","company_revenue","company_employee_count","company_employee_growth","company_departments_employee_count","company_departments_employee_growth","company_job_posting","company_profile_url","tag","platform"]  
    deletion_on_retrieval = False

    db_config = {
        'user': 'tlubben',
        'password': 'VizualLead21!',
        'host': 'vzleadgen.cma1wzfub9dh.us-west-2.rds.amazonaws.com',
        'port': 3306,
        'database': 'vizleads'
    }

    table_name = "stg_apollo"
    json_columns = [
    'company_fundinground', 'company_departments_employee_count', 'company_job_posting', 'company_technologies', 'company_employee_growth', 'company_departments_employee_growth'
    ]

    retrieve_and_process_data(tyler_sheet_urls, db_config, table_name, expected_columns, deletion_on_retrieval, "tylerlubben", json_columns)
    retrieve_and_process_data(andrew_sheet_urls, db_config, table_name, expected_columns, deletion_on_retrieval, "andrewmorgans", json_columns)

if __name__ == '__main__':
    main()