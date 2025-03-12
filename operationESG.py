import azure.functions as func
import requests
import os, re
import pandas as pd
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import pyodbc
from urllib.parse import unquote
from dotenv import load_dotenv
from datetime import datetime , timedelta
import hashlib
import logging
from azure.storage.blob import BlobServiceClient,generate_blob_sas,BlobSasPermissions
from io import BytesIO
from office365.sharepoint.files.file import File

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="Operations_and_ESG_Function",auth_level=func.AuthLevel.ANONYMOUS)
def Operations_and_ESG_Function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request....')
    index()

    # Return a simplified response
    return func.HttpResponse(
        "This function executed successfully.",
        status_code=200
        )

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

# Test log message
logging.debug("This is a test debug log.")

load_dotenv()

account_name = None
account_key = None
container_name = None
blob_connection_string = None

blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
container_client = blob_service_client.get_container_client(container_name)

blob_list = []
for blob_i in container_client.list_blobs():
    blob_list.append(blob_i.name)

def uploadToBlobStorage(file_path,file_name):
    blob_client = blob_service_client.get_blob_client(container=container_name,blob=file_name)

    # with open(file_path,"rb") as data:
    blob_client.upload_blob(file_path,overwrite=True)
    logging.info("file uploaded successfully")

# Accessing SQL Server credentials
sql_server_host = os.getenv("SQL_SERVER_HOST")
sql_server_port = os.getenv("SQL_SERVER_PORT")
sql_server_user = os.getenv("SQL_SERVER_USER")
sql_server_password = os.getenv("SQL_SERVER_PASSWORD")
sql_database = os.getenv("SQL_DATABASE")

# Accessing SharePoint credentials
site_url = os.getenv("SHAREPOINT_URL")
username = os.getenv("SHAREPOINT_USERNAME")
password = os.getenv("SHAREPOINT_PASSWORD")

# Define sheet-to-table mappings for each folder
sheet_to_table_map_client_a = {
    "Operation Overview": "dbo.OperationOverview",
    "Env - Scope 1 & 2 Emissions": "dbo.Env-Scope1&2Emissions",
    "Env - Scope 2 Electricity": "dbo.Env-Scope2Electricity",
    "Env - Utilities": "dbo.Env-Utilities",
    "Social - Employee by Gender": "dbo.Social-EmployeeByGender",
    "Social - Employee by Age": "dbo.Social-EmployeeByAge",
    "Social - CSR": "dbo.Social-CSR",
    "Gov - Management Diversity": "dbo.Gov-ManagementDiversity",
    "Gov - Board": "dbo.Gov-Board",
    "Targets": "dbo.Targets"
}

sheet_to_table_map_subsidiary = {
    "Financial Performance": "dbo.OP_FinancialPerformance",
    "Project Timeline": "dbo.OP_ProjectTimeline",
    "Construction Timeline": "dbo.OP_ConstructionTimeline",
    "Project S-Curve": "dbo.OP_ProjectSCurve",
    "Project Detail": "dbo.OP_ProjectDetail",
    "Project Expenses": "dbo.OP_ProjectExpenses",
    "Electricity Generation (Annualy": "dbo.OP_AnnualyElectricityGeneration",
    "Electricity Generation (monthly": "dbo.OP_MonthlyElectricityGeneration",
    "Electricity Generation (Daily)": "dbo.OP_DailyElectricityGeneration",
    "Outages & Availability (Monthly": "dbo.OP_MonthlyOutagesAndAvailability",
    "Coal Stockpile (Daily)": "dbo.OP_CoalStockpileDaily",
    "Env - Scope 1 & 2 Emissions": "dbo.SubsidiaryEnv-Scope1&2Emissions",
    "Env - Utilities": "dbo.SubsidiaryEnv-Utilities",
    "Social - Employee by Gender": "dbo.SubsidiarySocial-EmployeeByGender",
    "Social - Employee by Age": "dbo.SubsidiarySocial-EmployeeByAge",
    "Social - CSR": "dbo.SubsidiarySocial-CSR",
    "Gov - Management Diversity": "dbo.SubsidiaryGov-ManagementDiversity",
    "Gov - Board": "dbo.SubsidiaryGov-Board",
    "Targets": "dbo.SubsidiaryTargets"
}

# Sheet-to-table mapping for the Dashboard Configuration Master List
sheet_to_table_map_config = {
    "Subsidiary List": "dbo.SubsidiaryList",
    "Investment List": "dbo.InvestmentAccountList"
}



def escape_special_characters(text):
    # Ensure text is a string before applying the regex
    if not isinstance(text, str):
        text = str(text)  # Convert non-string values to strings
    return re.sub(r'([\\\'\"%_])', r'\\\1', text)

def get_nested_folders(ctx, parent_path="/sites/Dashboard-UAT/Shared%20Documents"):
    folder_collection = ctx.web.get_folder_by_server_relative_url(parent_path).folders
    ctx.load(folder_collection)
    ctx.execute_query()

    main_folders = []
    for folder in folder_collection:
        main_folder_name = folder.properties["Name"]

        main_folders.append(main_folder_name)

    return main_folders


def process_subfolders(ctx, parent_path):
    xlsx_files = []

    # Get all folders and files under the given path
    folder_collection = ctx.web.get_folder_by_server_relative_url(parent_path).folders
    ctx.load(folder_collection)
    ctx.execute_query()

    # Get files directly under the parent path
    file_collection = ctx.web.get_folder_by_server_relative_url(parent_path).files
    ctx.load(file_collection)
    ctx.execute_query()

    # Add files from the current folder
    for file in file_collection:
        file_name = file.properties["Name"]
        if file_name.endswith('.xlsx') and file_name not in []:
            xlsx_files.append(file.properties["ServerRelativeUrl"])

    # Recursively process subfolders
    for folder in folder_collection:
        folder_name = folder.properties["Name"]
        subfolder_path = f"{parent_path}/{folder_name}"
        # Recursive call to process files in subfolders

        xlsx_files += process_subfolders(ctx, subfolder_path)

    return xlsx_files


def get_subfolders(ctx, parent_path):
    """
    Retrieves the subfolders under a given folder path.
    """
    try:
        folder = ctx.web.get_folder_by_server_relative_url(parent_path)
        folder.expand(["Folders"]).get().execute_query()  # Expand to get subfolders
        subfolders = [subfolder.name for subfolder in folder.folders]
        logging.info(f"Subfolders in '{parent_path}': {subfolders}")
        return subfolders
    except Exception as e:
        logging.error(f"Error retrieving subfolders from '{parent_path}': {e}")
        return []


# Function to generate unique_id based on 'Account', 'Year', and 'Company'
def generate_unique_id(account, year, company):
    unique_string = f"{account}{year}{company}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

# Create a connection string for pyodbc
connection_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={sql_server_host},{sql_server_port};"
    f"Database={sql_database};"
    f"UID={sql_server_user};"
    f"PWD={sql_server_password};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=yes;"
)



def OperationESGFunctionIndex():
    global_subfolder = ""
    try:
        local_copy_sas_url = None
        config_local_copy_sas_url = None
        for blob_i in blob_list:
            sas_i = generate_blob_sas(account_name=account_name,container_name=container_name,blob_name=blob_i,account_key=account_key,permission=BlobSasPermissions(read=True),expiry=datetime.now() + timedelta(hours=1))
            sas_url = 'https://' + account_name + '.blob.core.windows.net/' + container_name + '/' + blob_i + '?' + sas_i
            logging.info(sas_i)
            logging.info(sas_url)
            if "config_local_copy" in blob_i:
                config_local_copy_sas_url = sas_url
                logging.info(f"Stored SAS URL for config_local_copy: {config_local_copy_sas_url}")
            elif "local_copy" in blob_i:
                local_copy_sas_url = sas_url
                logging.info(f"Stored SAS URL for local_copy: {local_copy_sas_url}")
        auth_context = AuthenticationContext(site_url)
        if auth_context.acquire_token_for_user(username, password):
            logging.info("Authentication successful.")
            ctx = ClientContext(site_url, auth_context)

            main_and_subfolders = sorted(get_nested_folders(ctx))
            all_xlsx_files = []

            conn = pyodbc.connect(connection_string)
            logging.info("Connected to SQL Server successfully.")
            cursor = conn.cursor()

            for main_folder in main_and_subfolders:
                logging.info(f"Checking folder: {main_folder}")

                # Processing the sheets of Parent subfolder for Operations and ESG Dashboard
                if main_folder == "Parent":
                    logging.info("Processing 'Parent' folder.")
                    # Process only the "Parent" folder and its subfolders
                    xlsx_files = process_subfolders(ctx, parent_path="/sites/Dashboard-UAT/Shared%20Documents/Parent")
                    all_xlsx_files.extend(xlsx_files)  # Add the results from Parent
                    sheet_to_table_map = sheet_to_table_map_client_a  # Use the correct mapping
                    parent_path = "/sites/Dashboard-UAT/Shared%20Documents/Parent"
                    subfolders = get_subfolders(ctx, parent_path)

                    for subfolder in subfolders:
                        subfolder_path = f"{parent_path}/{subfolder}"
                        global_subfolder = subfolder

                        # Extract Dashboard name from subfolder path (assuming it's the last part)

                        # Get XLSX files for the current subfolder
                        xlsx_files = process_subfolders(ctx, parent_path=subfolder_path)
                        all_xlsx_files.extend(xlsx_files)  # Ensure files are mapped to correct subfolder

                        dashboard_folders = get_subfolders(ctx, subfolder_path)

                        sheet_status = {}
                        dashboard_file_map = {}
                        for dashboard in dashboard_folders:
                            dashboard_path = f"{subfolder_path}/{dashboard}"
                            dashboard_files = [file for file in xlsx_files if file.startswith(dashboard_path)]
                            dashboard_file_map[dashboard] = dashboard_files

                            # Process all files in the current folder
                            for file in xlsx_files:
                                # Download the file locally
                                print(f'file is {file}')
                                file_content = File.open_binary(ctx, file)
                                logging.info(f"file content is {file_content}")


                                # Load the workbook to inspect sheet names
                                uploadToBlobStorage(file_content,"local_copy.xlsx")
                                logging.info("upload to blob successfully")
                                xls = pd.ExcelFile(local_copy_sas_url)
                                sheet_names = xls.sheet_names

                                logging.info(f"Sheet names in the workbook: {sheet_names}")
                                # Skip the "Preface" sheet if present
                                if "Preface" in sheet_names:
                                    sheet_names.remove("Preface")

                                for sheet_name in sheet_names:
                                    logging.info(f"Processing sheet: {sheet_name}")

                                    if sheet_name in ["Financial Performance", "Project Timeline", "Construction Timeline"]:
                                        skiprows = 3
                                        header = 0  # First row after skipping rows becomes header
                                    elif sheet_name in ["Electricity Generation (monthly", "Outages & Availability (Monthly",
                                                        "Project Detail"]:
                                        skiprows = 2
                                        header = 0
                                    elif sheet_name in ["Project S-Curve", "Electricity Generation (Daily)",
                                                        "Coal Stockpile (Daily)",
                                                        "Project Expenses"]:
                                        skiprows = 1
                                        if sheet_name in ["Project Expenses"]:
                                            header = [0, 1]  # Combined header from rows 3 and 4
                                        else:
                                            header = 0
                                    elif sheet_name == "Electricity Generation (Annualy":
                                        skiprows = 0
                                        header = 0
                                    elif sheet_name in ["Risk Details", "KRI Details", "Inherent Risk", "Residual Risk",
                                                        "Balance Sheet",
                                                        "RKAP Balance Sheet", "Income Statement", "RKAP Income Statement",
                                                        "Cash Flow",
                                                        "RKAP Cash Flow", "RKAP Cash Flow", "Subsidiary Balance Sheet",
                                                        "Subsidiary FM Balance Sheet", "Subsidiary RKAP Balance Sheet",
                                                        "Subsidiary Income Statement", "Subsidiary FM Income Statement",
                                                        "Subsidiary RKAP Income Statemen",
                                                        "Subsidiary Cash Flow", "Subsidiary FM Cash Flow",
                                                        "Subsidiary RKAP Cash Flow",
                                                        "Debt Management"]:
                                        logging.info(f"Skipping sheet: {sheet_name}")
                                        continue  # Skip processing this sheet
                                    else:
                                        print("Skipped 4 lines")
                                        skiprows = 4
                                        header = 0

                                    inferred_dashboard = None
                                    for dash in dashboard_folders:
                                        if dash.lower() in file.lower():
                                            inferred_dashboard = dash
                                            break

                                    if not inferred_dashboard:
                                        inferred_dashboard = "Unknown"
                                    df = pd.read_excel(local_copy_sas_url, sheet_name=sheet_name, skiprows=skiprows,
                                                    header=header)

                                    # Step 6: Add inferred dashboard and company to DataFrame
                                    df['Company'] = subfolder
                                    df['Dashboard'] = inferred_dashboard
                                    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^a-zA-Z0-9_]",
                                                                                                        "")

                                    # Step 7: Log success
                                    sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                    "Description": "Sheet processed successfully"}

                                    if sheet_name in ["Project Expenses"]:
                                        # Check if the sheet requires flattening
                                        if isinstance(df.columns, pd.MultiIndex):
                                            # Flatten MultiIndex for specific sheets only
                                            df.columns = [' '.join(col).strip() for col in df.columns.values]

                                    for col in df.columns:
                                        if df[col].dtype == 'object':
                                            df[col] = df[col].str.strip()

                                    df['Company'] = subfolder
                                    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^a-zA-Z0-9_]", "")

                                    # Replace NaN values with 0 for numeric columns
                                    df.fillna(0, inplace=True)

                                    if sheet_name == "Operation Overview":
                                        logging.info(f"Processing sheet: {sheet_name} from Dashboard: {dashboard}")
                                        try:
                                            logging.info("Special processing for 'Operation Overview' sheet.")
                                            name_table = 'dbo.OperationOverview'
                                            required_columns = [
                                                'Subsidiary_Name', 'Project', 'Type_(Coal/Hydro/Solar)', 'Stage', 'COD_Date',
                                                'NDC_(MW)',
                                                'Latitude', 'Langitude'
                                            ]
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Subsidiary_Name': 'Subsidiary_Name',
                                                    'Project': 'Project',
                                                    'Type_(Coal/Hydro/Solar)': 'Type_(Coal/Hydro/Solar)',
                                                    'Stage': 'Stage',
                                                    'COD_Date': 'COD_Date',
                                                    'NDC_(MW)': 'NDC_(MW)',
                                                    'Latitude': 'Latitude',
                                                    'Langitude': 'Langitude'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # Insert data into the defined table
                                                table_name = "[dbo].[OperationOverview]"
                                                df['Subsidiary_Name'] = df['Subsidiary_Name'].apply(
                                                    lambda x: str(x) if not isinstance(x, str) else x)
                                                df['Subsidiary_Name'] = df['Subsidiary_Name'].apply(escape_special_characters)

                                                # Ensure the Project column is treated as a string
                                                df['Project'] = df['Project'].apply(
                                                    lambda x: str(x) if not isinstance(x, str) else x)
                                                # Before inserting the 'Project' column data, apply the escaping
                                                df['Project'] = df['Project'].apply(escape_special_characters)
                                                # Insert data into the defined table
                                                df['Subsidiary_Name'] = df['Subsidiary_Name'].astype(str).apply(
                                                    escape_special_characters)
                                                df['Project'] = df['Project'].astype(str).apply(escape_special_characters)
                                                df['COD_Date'] = pd.to_datetime(df['COD_Date'], errors='coerce').dt.date
                                                df['NDC_(MW)'] = df['NDC_(MW)'].apply(pd.to_numeric, errors='coerce').round(2)
                                                df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce').round(6)
                                                df['Langitude'] = pd.to_numeric(df['Langitude'], errors='coerce').round(6)
                                                df.dropna(subset=['Latitude', 'Langitude', 'NDC_(MW)'],
                                                        inplace=True)
                                                df['Latitude'].fillna(0, inplace=True)
                                                df['Langitude'].fillna(0, inplace=True)
                                                df['NDC_(MW)'].fillna(0, inplace=True)

                                                global project_list
                                                if 'Project' in df.columns and 'Subsidiary_Name' in df.columns:
                                                    project_list = df[
                                                        ['Subsidiary_Name', 'Project']].drop_duplicates().values.tolist()

                                                # Step 1: Check if the sheet has rows that are NOT in the database
                                                existing_rows_query = \
                                                    f"""
                                                                                    SELECT Subsidiary_Name, Project 
                                                                                    FROM {table_name}
                                                                                """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in rows}

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Subsidiary_Name'], df['Project']))

                                                missing_rows = df_tuples - existing_rows_set

                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                                    cursor.execute(truncate_query)

                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (Subsidiary_Name, Project, [Type_(Coal/Hydro/Solar)], Stage, COD_Date, [NDC_(MW)], Latitude, Langitude, Company)
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Subsidiary_Name'], row['Project'],
                                                            row['Type_(Coal/Hydro/Solar)'],
                                                            row['Stage'], row['COD_Date'], row['NDC_(MW)'], row['Latitude'],
                                                            row['Langitude'],
                                                            row['Company']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                            IF EXISTS (
                                                                                SELECT 1 FROM {table_name} WHERE Subsidiary_Name = ? AND Project = ?
                                                                            )
                                                                            BEGIN
                                                                                UPDATE {table_name}
                                                                                SET [Type_(Coal/Hydro/Solar)] = ?, Stage = ?, COD_Date = ?, [NDC_(MW)] = ?, Latitude = ?, Langitude = ?, Company = ?
                                                                                WHERE Subsidiary_Name = ? AND Project = ?;
                                                                            END
                                                                            ELSE
                                                                            BEGIN
                                                                                INSERT INTO {table_name} (Subsidiary_Name, Project, [Type_(Coal/Hydro/Solar)], Stage, COD_Date, [NDC_(MW)], Latitude, Langitude, Company)
                                                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                            END
                                                                        """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            row['Subsidiary_Name'], row['Project'],
                                                            row['Type_(Coal/Hydro/Solar)'], row['Stage'], row['COD_Date'],
                                                            row['NDC_(MW)'],
                                                            row['Latitude'], row['Langitude'], row['Company'],
                                                            row['Subsidiary_Name'], row['Project'],
                                                            row['Subsidiary_Name'], row['Project'],
                                                            row['Type_(Coal/Hydro/Solar)'], row['Stage'], row['COD_Date'],
                                                            row['NDC_(MW)'],
                                                            row['Latitude'], row['Langitude'], row['Company'],
                                                        ))
                                                conn.commit()
                                            logging.info(
                                                "Data successfully processed and committed for 'Operation Overview' sheet.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Env - Scope 1 & 2 Emissions":
                                        try:
                                            logging.info("Special processing for 'Env - Scope 1 & 2 Emissions'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            # Clean the column names to remove leading/trailing spaces
                                            df.columns = df.columns.str.strip()
                                            required_columns = ['Month', 'Scope_1_tCO2e', 'Scope_2_tCO2e',
                                                                'Total_Scope_1_&_2']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Month': 'Month',
                                                    'Scope_1_tCO2e': 'Scope1_tCO2e',
                                                    'Scope_2_tCO2e': 'Scope2_tCO2e',
                                                    'Total_Scope_1_&_2': 'Total_Scope1&2'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "[dbo].[Env-Scope1&2Emissions]"
                                                existing_rows_query = f"""
                                                                    SELECT Company, Month
                                                                    FROM {table_name}
                                                                """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in rows}

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(zip(df['Company'], df['Month']))

                                                missing_rows = df_tuples - existing_rows_set
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                                    cursor.execute(truncate_query)

                                                    insert_query = f"""
                                                                        INSERT INTO {table_name} (
                                                                        [Company], [Month], [Scope1_tCO2e], [Scope2_tCO2e], [Total_Scope1&2]
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?)
                                                                        """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Month'],
                                                            row['Scope1_tCO2e'],
                                                            row['Scope2_tCO2e'],
                                                            row['Total_Scope1&2']
                                                        ))
                                                else:
                                                    logging.info(f"Table name set to: {table_name}")
                                                    update_insert_query = f"""
                                                                    IF EXISTS (
                                                                        SELECT 1
                                                                        FROM {table_name}
                                                                        WHERE [Company] = ? AND [Month] = ?
                                                                    )
                                                                    BEGIN
                                                                        UPDATE {table_name}
                                                                        SET [Scope1_tCO2e] = ?, [Scope2_tCO2e] = ?, [Total_Scope1&2] = ?
                                                                        WHERE [Company] = ? AND [Month] = ?;
                                                                    END
                                                                    ELSE
                                                                    BEGIN
                                                                        INSERT INTO {table_name} (
                                                                            [Company], [Month], [Scope1_tCO2e], [Scope2_tCO2e], [Total_Scope1&2]
                                                                        )
                                                                        VALUES (?, ?, ?, ?, ?);
                                                                    END
                                                                """

                                                    logging.info("Beginning insertion into Env-Scope1&2Emissions table.")
                                                    for _, row in df.iterrows():
                                                        # Define the placeholders for this row
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],
                                                            # For UPDATE clause

                                                            row['Scope1_tCO2e'],
                                                            row['Scope2_tCO2e'],
                                                            row['Total_Scope1&2'],

                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Month'],
                                                            row['Scope1_tCO2e'],
                                                            row['Scope2_tCO2e'],
                                                            row['Total_Scope1&2']
                                                        ))
                                                conn.commit()
                                                logging.info(
                                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                                sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                                "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Env - Utilities":
                                        try:
                                            logging.info("Special processing for 'Env-Utilities'")
                                            df = df.drop(columns=["Unnamed:_0"])

                                            required_columns = ['Month', 'IPRen_Electricity_Usage_(Wh)',
                                                                'IPRen_Actual_Water_Consumption_(m3)',
                                                                'IPRen_Actual_Fuel_Consumption_(L)'
                                                                ]
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Month': 'Month',
                                                    'IPRen_Electricity_Usage_(Wh)': 'IPRen_ElectricityUsage(Wh)',
                                                    'IPRen_Actual_Water_Consumption_(m3)': 'IPRen_ActualWaterConsumption(m3)',
                                                    'IPRen_Actual_Fuel_Consumption_(L)': 'IPRen_ActualFuelConsumption(L)'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "[dbo].[Env-Utilities]"
                                                logging.info(f"Table name set to: {table_name}")
                                                existing_rows_query = f"""
                                                                        SELECT Company, Month
                                                                        FROM {table_name}
                                                                        """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in rows}

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Company'], df['Month']))  # Convert df to a set of tuples
                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")
                                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                                    cursor.execute(truncate_query)
                                                    insert_query = f"""

                                                                    INSERT INTO {table_name} ([Company], [Month], [IPRen_ElectricityUsage(Wh)], [IPRen_ActualWaterConsumption(m3)], [IPRen_ActualFuelConsumption(L)])
                                                                                        VALUES (?, ?, ?, ?, ?)
                                                                """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Month'], row['IPRen_ElectricityUsage(Wh)'],
                                                            row['IPRen_ActualWaterConsumption(m3)'],
                                                            row['IPRen_ActualFuelConsumption(L)']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                        IF EXISTS (
                                                            SELECT 1
                                                            FROM {table_name}
                                                            WHERE [Company] = ? AND [Month] = ?
                                                        )
                                                        BEGIN
                                                            UPDATE {table_name}
                                                            SET
                                                            [IPRen_ElectricityUsage(Wh)] = ?,
                                                            [IPRen_ActualWaterConsumption(m3)] = ?,
                                                            [IPRen_ActualFuelConsumption(L)] = ?
                                                            WHERE [Company] = ? AND [Month] = ?;
                                                        END
                                                        ELSE
                                                        BEGIN
                                                            INSERT INTO {table_name} (
                                                                [Company], [Month],
                                                                [IPRen_ElectricityUsage(Wh)],
                                                                [IPRen_ActualWaterConsumption(m3)],
                                                                [IPRen_ActualFuelConsumption(L)]
                                                            )
                                                            VALUES (?, ?, ?, ?, ?);
                                                        END
                                                    """

                                                    logging.info("Beginning insertion into ENV-Utilites table.")

                                                    for _, row in df.iterrows():
                                                        # Define the placeholders for this row
                                                        placeholders = (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],

                                                            # For UPDATE clause
                                                            row['IPRen_ElectricityUsage(Wh)'],
                                                            row['IPRen_ActualWaterConsumption(m3)'],
                                                            row['IPRen_ActualFuelConsumption(L)'],
                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Month'],
                                                            row['IPRen_ElectricityUsage(Wh)'],
                                                            row['IPRen_ActualWaterConsumption(m3)'],
                                                            row['IPRen_ActualFuelConsumption(L)'],
                                                        )

                                                conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Social - Employee by Gender":
                                        try:
                                            logging.info("Special processing for 'Social - Employee by Gender'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            df.columns = df.columns.str.strip()
                                            required_columns = ['Month', 'Total_Male', 'Total_Female', 'New_Hire_Male',
                                                                'New_Hire_Female', 'Turnover_Male', 'Turnover_Female']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Month': 'Month',
                                                    'Total_Male': 'Total_Male',
                                                    'Total_Female': 'Total_Female',
                                                    'New_Hire_Male': 'NewHire_Male',
                                                    'New_Hire_Female': 'NewHire_Female',
                                                    'Turnover_Male': 'Turnover_Male',
                                                    'Turnover_Female': 'Turnover_Female'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # table_name = sheet_to_table_map[sheet_name]
                                                table_name = "[dbo].[Social-EmployeeByGender]"
                                                logging.info(f"Table name set to: {table_name}")

                                                existing_rows_query = f"""
                                                                            SELECT Company, Month
                                                                            FROM {table_name}
                                                                            """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(zip(df['Company'], df['Month']))

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")
                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                                    cursor.execute(truncate_query)

                                                    insert_query = f"""
                                                                        INSERT INTO {table_name} (
                                                                        [Company], [Month],
                                                                        [Total_Male],
                                                                        [Total_Female],
                                                                        [NewHire_Male],
                                                                        [NewHire_Female],
                                                                        [Turnover_Male],
                                                                        [Turnover_Female]
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                                                    """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Month'],
                                                            row['Total_Male'],
                                                            row['Total_Female'],
                                                            row['NewHire_Male'],
                                                            row['NewHire_Female'],
                                                            row['Turnover_Male'],
                                                            row['Turnover_Female']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                IF EXISTS (
                                                                    SELECT 1
                                                                    FROM {table_name}
                                                                    WHERE [Company] = ? AND [Month] = ?
                                                                )
                                                                BEGIN
                                                                    UPDATE {table_name}
                                                                    SET [Total_Male] = ?,
                                                                    [Total_Female] = ?,
                                                                    [NewHire_Male] = ?,
                                                                    [NewHire_Female] = ?,
                                                                    [Turnover_Male] = ?,
                                                                    [Turnover_Female] = ?

                                                                    WHERE [Company] = ? AND [Month] = ?;
                                                                END
                                                                ELSE
                                                                BEGIN
                                                                    INSERT INTO {table_name} (
                                                                        [Company], [Month],
                                                                        [Total_Male],
                                                                        [Total_Female],
                                                                        [NewHire_Male],
                                                                        [NewHire_Female],
                                                                        [Turnover_Male],
                                                                        [Turnover_Female]
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                                                                END
                                                            """

                                                    logging.info("Beginning insertion into Social-EmployeeByGender table.")
                                                    for _, row in df.iterrows():
                                                        # Define the placeholders for this row
                                                        placeholders = (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],
                                                            # For UPDATE clause

                                                            row['Total_Male'],
                                                            row['Total_Female'],
                                                            row['NewHire_Male'],
                                                            row['NewHire_Female'],
                                                            row['Turnover_Male'],
                                                            row['Turnover_Female'],

                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Month'],
                                                            row['Total_Male'],
                                                            row['Total_Female'],
                                                            row['NewHire_Male'],
                                                            row['NewHire_Female'],
                                                            row['Turnover_Male'],
                                                            row['Turnover_Female']
                                                        )
                                                conn.commit()
                                                logging.info(
                                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                                sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                                "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Social - Employee by Age":
                                        try:
                                            logging.info("Special processing for 'Social - Employee by Age'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            required_columns = ['Month', 'Total_<35', 'Total_35-50', 'Total_51-63', 'Total_>63',
                                                                'New_Hire_<35', 'New_Hire_35-50', 'New_Hire_51-63',
                                                                'New_Hire_>63',
                                                                'Turnover_<35', 'Turnover_35-50', 'Turnover_51-63',
                                                                'Turnover_>63'
                                                                ]

                                            for col in required_columns:
                                                column_mapping = {
                                                    'Month': 'Month',
                                                    'Total_<35': 'Total_<35',
                                                    'Total_35-50': 'Total_35-50',
                                                    'Total_51-63': 'Total_51-63',
                                                    'Total_>63': 'Total_>63',
                                                    'New_Hire_<35': 'New Hire_<35',
                                                    'New_Hire_35-50': 'New Hire_35-50',
                                                    'New_Hire_51-63': 'New Hire_51-63',
                                                    'New_Hire_>63': 'New Hire_>63',
                                                    'Turnover_<35': 'Turnover_<35',
                                                    'Turnover_35-50': 'Turnover_35-50',
                                                    'Turnover_51-63': 'Turnover_51-63',
                                                    'Turnover_>63': 'Turnover_>63',
                                                }

                                                df.rename(columns=column_mapping, inplace=True)
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "[dbo].[Social-EmployeeByAge]"
                                                logging.info(f"Table name set to: {table_name}")
                                                existing_rows_query = f"""
                                                                            SELECT Company, Month FROM {table_name}
                                                                        """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in rows}

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(zip(df['Company'], df['Month']))
                                                missing_rows = df_tuples - existing_rows_set
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")
                                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                                    cursor.execute(truncate_query)
                                                    insert_query = f"""
                                                    INSERT INTO {table_name} ([Company], [Month], [Total_<35], [Total_35-50], [Total_51-63],  [Total_>63],
                                                                                [New Hire_<35], [New Hire_35-50], [New Hire_51-63],[New Hire_>63], [Turnover_<35],
                                                                                    [Turnover_35-50],[Turnover_51-63], [Turnover_>63])
                                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                        """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Month'],  # For UPDATE clause
                                                            row['Total_<35'],
                                                            row['Total_35-50'],
                                                            row['Total_51-63'],
                                                            row['Total_>63'],
                                                            row['New Hire_<35'],
                                                            row['New Hire_35-50'],
                                                            row['New Hire_51-63'],
                                                            row['New Hire_>63'],
                                                            row['Turnover_<35'],
                                                            row['Turnover_35-50'],
                                                            row['Turnover_51-63'],
                                                            row['Turnover_>63'],
                                                        ))

                                                else:
                                                    logging.info(f"Table name set to: {table_name}")
                                                    update_insert_query = f"""
                                                                                        IF EXISTS (
                                                                                            SELECT 1
                                                                                            FROM  {table_name}
                                                                                            WHERE [Company] = ? AND [Month] = ?
                                                                                        )
                                                                                        BEGIN
                                                                                            UPDATE  {table_name}
                                                                                            SET [Total_<35] = ?,
                                                                                            [Total_35-50] = ?,
                                                                                            [Total_51-63] = ?,
                                                                                            [Total_>63] = ?,
                                                                                            [New Hire_<35] = ?,
                                                                                            [New Hire_35-50] = ?,
                                                                                            [New Hire_51-63] = ?,
                                                                                            [New Hire_>63] = ?,
                                                                                            [Turnover_<35] = ?,
                                                                                            [Turnover_35-50] = ?,
                                                                                            [Turnover_51-63] = ?,
                                                                                            [Turnover_>63] = ?

                                                                                            WHERE [Company] = ? AND [Month] = ?;
                                                                                        END
                                                                                        ELSE
                                                                                        BEGIN
                                                                                            INSERT INTO  {table_name} (
                                                                                                [Company], [Month],
                                                                                                [Total_<35],
                                                                                                [Total_35-50],
                                                                                                [Total_51-63],
                                                                                                [Total_>63],
                                                                                                [New Hire_<35],
                                                                                                [New Hire_35-50],
                                                                                                [New Hire_51-63],
                                                                                                [New Hire_>63],
                                                                                                [Turnover_<35],
                                                                                                [Turnover_35-50],
                                                                                                [Turnover_51-63],
                                                                                                [Turnover_>63]
                                                                                            )
                                                                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                                        END
                                                                                    """

                                                    logging.info("Beginning insertion into Social-EmployeeByAge table.")
                                                    for _, row in df.iterrows():
                                                        placeholders = (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],

                                                            # For UPDATE clause
                                                            row['Total_<35'],
                                                            row['Total_35-50'],
                                                            row['Total_51-63'],
                                                            row['Total_>63'],
                                                            row['New Hire_<35'],
                                                            row['New Hire_35-50'],
                                                            row['New Hire_51-63'],
                                                            row['New Hire_>63'],
                                                            row['Turnover_<35'],
                                                            row['Turnover_35-50'],
                                                            row['Turnover_51-63'],
                                                            row['Turnover_>63'],
                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Month'],
                                                            row['Total_<35'],
                                                            row['Total_35-50'],
                                                            row['Total_51-63'],
                                                            row['Total_>63'],
                                                            row['New Hire_<35'],
                                                            row['New Hire_35-50'],
                                                            row['New Hire_51-63'],
                                                            row['New Hire_>63'],
                                                            row['Turnover_<35'],
                                                            row['Turnover_35-50'],
                                                            row['Turnover_51-63'],
                                                            row['Turnover_>63']
                                                        )
                                                conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Social - CSR":
                                        try:
                                            logging.info("Special processing for 'Social - CSR'")
                                            # Drop unnecessary columns
                                            df = df.drop(columns=["Unnamed:_0", "Unnamed:_5"])
                                            df.columns = df.columns.str.strip()

                                            # Define required columns and rename
                                            column_mapping = {
                                                'Month': 'Month',
                                                'CSR__Project_Name': 'CSR_ProjectName',
                                                'CSR_Value': 'CSR_Value',
                                                'CSR_Disbursed': 'CSR_Disbursement'
                                            }
                                            df.rename(columns=column_mapping, inplace=True)

                                            # Remove the 'Created' column if it exists
                                            if 'Created' in df.columns:
                                                df.drop(columns=['Created'], inplace=True)
                                                logging.info(f"'Created' column removed.")

                                            table_name = "[dbo].[Social-CSR]"

                                            # Step 1: Check if the sheet has rows that are NOT in the database
                                            existing_rows_query = f"""
                                                                            SELECT Company, Month
                                                                            FROM {table_name}
                                                                        """

                                            cursor.execute(existing_rows_query)
                                            rows = cursor.fetchall()
                                            existing_rows_set = {tuple(row) for row in
                                                                rows}  # Convert rows to tuples for hashing

                                            # Step 2: Compare with DataFrame
                                            df_tuples = set(zip(df['Company'], df['Month']))  # Convert df to a set of tuples

                                            missing_rows = df_tuples - existing_rows_set  # Find missing rows

                                            if missing_rows:
                                                logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                # Step 3: Truncate the table before inserting new data
                                                truncate_query = f"TRUNCATE TABLE {table_name};"
                                                cursor.execute(truncate_query)

                                                insert_query = f"""
                                                                                INSERT INTO {table_name} (Company, Month, CSR_ProjectName, CSR_Value, CSR_Disbursement)
                                                                                VALUES (?, ?, ?, ?, ?)
                                                                            """

                                                for _, row in df.iterrows():
                                                    cursor.execute(insert_query, (
                                                        row['Company'], row['Month'], row['CSR_ProjectName'], row['CSR_Value'],
                                                        row['CSR_Disbursement']
                                                    ))

                                            else:
                                                logging.info("Rows exist. Performing UPDATE or INSERT.")

                                                update_insert_query = f"""
                                                                                IF EXISTS (
                                                                                    SELECT 1 FROM {table_name} WHERE Company = ? AND Month = ?
                                                                                )
                                                                                BEGIN
                                                                                    UPDATE {table_name}
                                                                                    SET CSR_ProjectName = ?, CSR_Value = ?, CSR_Disbursement = ?
                                                                                    WHERE Company = ? AND Month = ?;
                                                                                END
                                                                                ELSE
                                                                                BEGIN
                                                                                    INSERT INTO {table_name} (Company, Month, CSR_ProjectName, CSR_Value, CSR_Disbursement)
                                                                                    VALUES (?, ?, ?, ?, ?);
                                                                                END
                                                                            """

                                                for _, row in df.iterrows():
                                                    cursor.execute(update_insert_query, (
                                                        row['Company'], row['Month'],
                                                        row['CSR_ProjectName'], row['CSR_Value'], row['CSR_Disbursement'],
                                                        row['Company'], row['Month'],
                                                        row['Company'], row['Month'],
                                                        row['CSR_ProjectName'], row['CSR_Value'], row['CSR_Disbursement']
                                                    ))

                                            # Commit the transaction
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Gov - Management Diversity":
                                        try:
                                            logging.info("Special processing for 'Gov - Management Diversity'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            # Clean the column names to remove leading/trailing spaces
                                            df.columns = df.columns.str.strip()
                                            required_columns = ['Month', 'Senior_Male', 'Senior_Female', 'Middle_Male',
                                                                'Middle_Female']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Month': 'Month',
                                                    'Senior_Male': 'Senior_Male',
                                                    'Senior_Female': 'Senior_Female',
                                                    'Middle_Male': 'Middle_Male',
                                                    'Middle_Female': 'Middle_Female'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "[dbo].[Gov-ManagementDiversity]"
                                                logging.info(f"Table name set to: {table_name}")

                                                existing_rows_query = f"""
                                                                    SELECT Company, Month
                                                                    FROM {table_name}
                                                                    """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(zip(df['Company'], df['Month']))
                                                missing_rows = df_tuples - existing_rows_set
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                                    cursor.execute(truncate_query)
                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                                                [Company],[Month],
                                                                                                [Senior_Male],
                                                                                                [Senior_Female],
                                                                                                [Middle_Male],
                                                                                                [Middle_Female]

                                                                                            )
                                                                                    VALUES (?, ?, ?, ?, ?, ?)
                                                                """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Month'],
                                                            row['Senior_Male'],
                                                            row['Senior_Female'],
                                                            row['Middle_Male'],
                                                            row['Middle_Female']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                                IF EXISTS (
                                                                                        SELECT 1
                                                                                        FROM {table_name}
                                                                                        WHERE [Company] = ? AND [Month] = ?
                                                                                    )
                                                                                    BEGIN
                                                                                        UPDATE {table_name}
                                                                                        SET
                                                                                            [Senior_Male] = ?,
                                                                                            [Senior_Female] = ?,
                                                                                            [Middle_Male] = ?,
                                                                                            [Middle_Female] = ?


                                                                                        WHERE [Company] = ? AND [Month] = ?;
                                                                                    END
                                                                                    ELSE
                                                                                    BEGIN
                                                                                        INSERT INTO {table_name} (
                                                                                            [Company],[Month],
                                                                                            [Senior_Male],
                                                                                            [Senior_Female],
                                                                                            [Middle_Male],
                                                                                            [Middle_Female]

                                                                                        )
                                                                                        VALUES (?, ?, ?, ?, ?, ?);
                                                                                    END
                                                                                                                                """

                                                logging.info("Beginning insertion into Gov-ManagementDiversity table.")
                                                for _, row in df.iterrows():
                                                    placeholders = (
                                                        # For IF EXISTS condition
                                                        row['Company'], row['Month'],

                                                        # For UPDATE clause
                                                        row['Senior_Male'],
                                                        row['Senior_Female'],
                                                        row['Middle_Male'],
                                                        row['Middle_Female'],

                                                        row['Company'], row['Month'],

                                                        # For INSERT INTO clause
                                                        row['Company'], row['Month'],
                                                        row['Senior_Male'],
                                                        row['Senior_Female'],
                                                        row['Middle_Male'],
                                                        row['Middle_Female']
                                                    )
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Gov - Board":
                                        try:
                                            logging.info("Special processing for 'Gov - Board'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            # Clean the column names to remove leading/trailing spaces
                                            df.columns = df.columns.str.strip()
                                            required_columns = ['Year', 'Name', 'Gender', 'Types', 'Executive/non-executive',
                                                                'Independence_(yes/no)',
                                                                'Board_Independence_Percentage_(%)', 'Start_Date', 'End_Date',
                                                                'Remaining_Period', 'Tenure_years']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Name': 'Name',
                                                    'Gender': 'Gender',
                                                    'Types': 'Types',
                                                    'Executive/non-executive': 'Executive/Non-Executive',
                                                    'Independence_(yes/no)': 'Independence',
                                                    'Board_Independence_Percentage_(%)': 'BoardIndependencePercentage',
                                                    'Start_Date': 'StartDate',
                                                    'End_Date': 'EndDate',
                                                    'Remaining_Period': 'RemainingPeriod',
                                                    'Tenure_years': 'TenureYears'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")
                                                if 'FY' in df.columns:
                                                    try:
                                                        df['Year'] = df['Year'].astype(int)
                                                        logging.info("Converted 'Year' column to integers.")
                                                    except ValueError as ve:
                                                        logging.error(f"Failed to convert 'FY' column to integers: {ve}")
                                                        raise
                                                df['BoardIndependencePercentage'] = pd.to_numeric(
                                                    df['BoardIndependencePercentage'],
                                                    errors='coerce')
                                                df['StartDate'] = pd.to_datetime(df['StartDate'], errors='coerce')
                                                df['EndDate'] = pd.to_datetime(df['EndDate'], errors='coerce')
                                                logging.info(f"main_folder: {main_folder}, sheet_name: {sheet_name}")

                                                table_name = "[dbo].[Gov-Board]"
                                                logging.info(f"Table name set to: {table_name}")
                                                existing_rows_query = f"""
                                                                        SELECT Name, Company, Year
                                                                        FROM {table_name}
                                                                        """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing
                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Name'], df['Company'], df['Year']))
                                                missing_rows = existing_rows_set - df_tuples
                                                logging.info(f"Table name set to: {table_name}")
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                                    cursor.execute(truncate_query)
                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                                            [Name],
                                                                                            [Year],
                                                                                            [Gender],
                                                                                            [Types],
                                                                                            [Executive/Non-Executive],
                                                                                            [Independence],
                                                                                            [BoardIndependencePercentage],
                                                                                            [StartDate],
                                                                                            [EndDate],
                                                                                            [RemainingPeriod],
                                                                                            [TenureYears],
                                                                                            [Company]
                                                                                        )
                                                                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                    """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Name'],
                                                            row['Year'],
                                                            row['Gender'],
                                                            row['Types'],
                                                            row['Executive/Non-Executive'],
                                                            row['Independence'],
                                                            row['BoardIndependencePercentage'],
                                                            row['StartDate'],
                                                            row['EndDate'],
                                                            row['RemainingPeriod'],
                                                            row['TenureYears'],
                                                            row['Company']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                IF EXISTS (
                                                                    SELECT 1
                                                                    FROM {table_name}
                                                                    WHERE [Name] = ? AND [Company] = ? AND [Year] = ?
                                                                )
                                                                BEGIN
                                                                    UPDATE {table_name}
                                                                    SET

                                                                        [Gender] = ?,
                                                                        [Types] = ?,
                                                                        [Executive/Non-Executive] = ?,
                                                                        [Independence] = ?,
                                                                        [BoardIndependencePercentage] = ?,
                                                                        [StartDate] = ?,
                                                                        [EndDate] = ?,
                                                                        [RemainingPeriod] = ?,
                                                                        [TenureYears] = ?
                                                                        WHERE [Name] = ? AND [Company] = ? AND   [Year] = ?;
                                                                END
                                                                ELSE
                                                                BEGIN
                                                                    INSERT INTO {table_name} (
                                                                        [Name],
                                                                        [Year],
                                                                        [Gender],
                                                                        [Types],
                                                                        [Executive/Non-Executive],
                                                                        [Independence],
                                                                        [BoardIndependencePercentage],
                                                                        [StartDate],
                                                                        [EndDate],
                                                                        [RemainingPeriod],
                                                                        [TenureYears],
                                                                        [Company]

                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                END
                                                                                        """

                                                    logging.info("Beginning insertion into Gov-Board table.")

                                                    # Log the DataFrame columns
                                                    # print(f"Columns in DataFrame: {df.columns}")
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Name'], row['Company'], row['Year'],

                                                            # For UPDATE clause

                                                            row['Gender'],
                                                            row['Types'],
                                                            row['Executive/Non-Executive'],
                                                            row['Independence'],
                                                            row['BoardIndependencePercentage'],
                                                            row['StartDate'],
                                                            row['EndDate'],
                                                            row['RemainingPeriod'],
                                                            row['TenureYears'],

                                                            row['Name'], row['Company'], row['Year'],

                                                            # For INSERT INTO clause
                                                            row['Name'],
                                                            row['Year'],
                                                            row['Gender'],
                                                            row['Types'],
                                                            row['Executive/Non-Executive'],
                                                            row['Independence'],
                                                            row['BoardIndependencePercentage'],
                                                            row['StartDate'],
                                                            row['EndDate'],
                                                            row['RemainingPeriod'],
                                                            row['TenureYears'],
                                                            row['Company']
                                                        ))

                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Targets":
                                        try:
                                            logging.info("Special processing for 'Targets'")
                                            # table_name = '[dbo].[Targets]'
                                            # logging.info("Gov - Board", df.columns)
                                            df = df.drop(columns=["Unnamed:_0"])
                                            print("DF", df.columns)
                                            # Clean the column names to remove leading/trailing spaces
                                            df.columns = df.columns.str.strip()
                                            required_columns = ['FY', 'Scope_1_Threshold_(tCO2e)',
                                                                'Projected_Scope_1__(tCO2e)', 'Turnover_Target', 'Target_CSR_%',
                                                                'Total_CSR_Budget_(IDR)', 'IPRen_Fuel_Consumption_Target_(L)',
                                                                'IPRen_Water_Consumption_Target_(L)']
                                            for col in required_columns:
                                                # if col not in df.columns:
                                                #     logging.error(
                                                #         f"Missing required column '{col}' in GOV-BOARD")
                                                #     continue

                                                # Rename columns to match the database schema if necessary
                                                column_mapping = {
                                                    'FY': 'FY',
                                                    'Scope_1_Threshold_(tCO2e)': 'Scope1_Threshold_tCO2',
                                                    'Projected_Scope_1__(tCO2e)': 'Projected_Scope1t_CO2',
                                                    'Turnover_Target': 'TurnoverTarget',
                                                    'Target_CSR_%': 'TargetCSR',
                                                    'Total_CSR_Budget_(IDR)': 'TotalCSRBudget',
                                                    'IPRen_Fuel_Consumption_Target_(L)': 'IPRen_FuelConsumptionTarget(L)',
                                                    'IPRen_Water_Consumption_Target_(L)': 'IPRen_WaterConsumptionTarget(L)'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "[dbo].[Targets]"
                                                logging.info(f"Table name set to: {table_name}")
                                                existing_rows_query = f"""
                                                                                                            SELECT FY, Company
                                                                                                            FROM {table_name}
                                                                                                            """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing
                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(zip(df['FY'], df['Company']))  # Convert df to a set of tuples

                                                missing_rows = existing_rows_set - df_tuples  # Find missing rows

                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                                    cursor.execute(truncate_query)
                                                    logging.info(f"Table name set to: {table_name}")
                                                    insert_query = f"""
                                                                        INSERT INTO {table_name} (
                                                                                    [FY],
                                                                                    [Company],
                                                                                    [Scope1_Threshold_tCO2],
                                                                                    [Projected_Scope1t_CO2],
                                                                                    [TurnoverTarget],
                                                                                    [TargetCSR],
                                                                                    [TotalCSRBudget],
                                                                                    [IPRen_FuelConsumptionTarget(L)],
                                                                                    [IPRen_WaterConsumptionTarget(L)]

                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                            """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['FY'], row['Company'],
                                                            row['Scope1_Threshold_tCO2'],
                                                            row['Projected_Scope1t_CO2'],
                                                            row['TurnoverTarget'],
                                                            row['TargetCSR'],
                                                            row['TotalCSRBudget'],
                                                            row['IPRen_FuelConsumptionTarget(L)'],
                                                            row['IPRen_WaterConsumptionTarget(L)']

                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                IF EXISTS (
                                                                    SELECT 1
                                                                    FROM {table_name}
                                                                    WHERE [FY] = ? AND [Company] = ?
                                                                )
                                                                BEGIN
                                                                    UPDATE {table_name}
                                                                    SET
                                                                        [Scope1_Threshold_tCO2] = ?,
                                                                        [Projected_Scope1t_CO2] = ?,
                                                                        [TurnoverTarget] = ?,
                                                                        [TargetCSR] = ?,
                                                                        [TotalCSRBudget] = ?,
                                                                        [IPRen_FuelConsumptionTarget(L)] = ?,
                                                                        [IPRen_WaterConsumptionTarget(L)] = ?

                                                                WHERE [FY] = ? AND [Company] = ?;
                                                                END
                                                                ELSE
                                                                BEGIN
                                                                    INSERT INTO {table_name} (
                                                                        [FY],
                                                                        [Company],
                                                                        [Scope1_Threshold_tCO2],
                                                                        [Projected_Scope1t_CO2],
                                                                        [TurnoverTarget],
                                                                        [TargetCSR],
                                                                        [TotalCSRBudget],
                                                                        [IPRen_FuelConsumptionTarget(L)],
                                                                        [IPRen_WaterConsumptionTarget(L)]

                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                END
                                                                                                                """

                                                    logging.info(f"Beginning insertion into {table_name}.")
                                                    # Log the DataFrame columns
                                                    # print(f"Columns in DataFrame: {df.columns}")
                                                    for _, row in df.iterrows():
                                                        # Define the placeholders for this row
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['FY'], row['Company'],

                                                            # For UPDATE clause
                                                            row['Scope1_Threshold_tCO2'],
                                                            row['Projected_Scope1t_CO2'],
                                                            row['TurnoverTarget'],
                                                            row['TargetCSR'],
                                                            row['TotalCSRBudget'],
                                                            row['IPRen_FuelConsumptionTarget(L)'],
                                                            row['IPRen_WaterConsumptionTarget(L)'],

                                                            row['FY'], row['Company'],

                                                            # For INSERT INTO clause
                                                            row['FY'], row['Company'],
                                                            row['Scope1_Threshold_tCO2'],
                                                            row['Projected_Scope1t_CO2'],
                                                            row['TurnoverTarget'],
                                                            row['TargetCSR'],
                                                            row['TotalCSRBudget'],
                                                            row['IPRen_FuelConsumptionTarget(L)'],
                                                            row['IPRen_WaterConsumptionTarget(L)'],

                                                        ))
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                        for (dashboard, sheet), status in sheet_status.items():
                            insert_log_query = """
                                    INSERT INTO [dbo].[OperationESGDataLog] ([CompanyName], [Dashboard], [ModifiedDate], [SheetName], [Status], [Description])
                                    VALUES (?, ?, GETDATE(), ?, ?, ?)
                                """
                            cursor.execute(insert_log_query,
                                        (subfolder, dashboard, sheet, status["Status"], status["Description"]))
                            conn.commit()

                if main_folder == "Subsidiary":
                    logging.info("Processing 'Subsidiary' folder.")
                    # Process only the "Subsidiary" folder and its subfolders
                    xlsx_files = process_subfolders(ctx, parent_path="/sites/Dashboard-UAT/Shared%20Documents/Subsidiary")
                    all_xlsx_files.extend(xlsx_files)  # Add the results from Subsidiary
                    sheet_to_table_map = sheet_to_table_map_subsidiary  # Use the correct mapping
                    parent_path = "/sites/Dashboard-UAT/Shared%20Documents/Subsidiary"
                    subfolders = get_subfolders(ctx, parent_path)
                    logging.info(f"Found subfolders: {subfolders}")
                    for subfolder in subfolders:
                        subfolder_path = f"{parent_path}/{subfolder}"
                        logging.info(f"Processing subfolder: {subfolder}")
                        global_subfolder = subfolder
                        # dashboard_name = subfolder_path.strip('/').split('/')[-1]
                        # Get XLSX files for the current subfolder
                        xlsx_files = process_subfolders(ctx, parent_path=subfolder_path)
                        all_xlsx_files.extend(xlsx_files)  # Ensure files are mapped to correct subfolder

                        dashboard_folders = get_subfolders(ctx, subfolder_path)

                        sheet_status = {}
                        dashboard_file_map = {}
                        for dashboard in dashboard_folders:
                            dashboard_path = f"{subfolder_path}/{dashboard}"
                            dashboard_files = [file for file in xlsx_files if file.startswith(dashboard_path)]
                            dashboard_file_map[dashboard] = dashboard_files

                            # Process all files in the current folder
                            for file in xlsx_files:
                                # Download the file locally
                                print(f'file is {file}')
                                file_content = File.open_binary(ctx, file)
                                logging.info(f"file content is {file_content}")


                                # Load the workbook to inspect sheet names
                                uploadToBlobStorage(file_content,"local_copy.xlsx")
                                logging.info("upload to blob successfully")
                                xls = pd.ExcelFile(local_copy_sas_url)
                                sheet_names = xls.sheet_names

                                logging.info(f"Sheet names in the workbook: {sheet_names}")
                                # Skip the "Preface" sheet if present
                                if "Preface" in sheet_names:
                                    sheet_names.remove("Preface")

                                for sheet_name in sheet_names:
                                    logging.info(f"Processing sheet: {sheet_name}")

                                    if sheet_name in ["Financial Performance", "Project Timeline", "Construction Timeline"]:
                                        skiprows = 3
                                        header = 0  # First row after skipping rows becomes header
                                    elif sheet_name in ["Electricity Generation (monthly",
                                                        "Outages & Availability (Monthly",
                                                        "Project Detail"]:
                                        skiprows = 2
                                        header = 0
                                    elif sheet_name in ["Project S-Curve", "Electricity Generation (Daily)",
                                                        "Coal Stockpile (Daily)",
                                                        "Project Expenses"]:
                                        skiprows = 1
                                        if sheet_name in ["Project Expenses"]:
                                            header = [0, 1]  # Combined header from rows 3 and 4
                                        else:
                                            header = 0
                                    elif sheet_name == "Electricity Generation (Annualy":
                                        skiprows = 0
                                        header = 0
                                    elif sheet_name in ["Risk Details", "KRI Details", "Inherent Risk", "Residual Risk",
                                                        "Balance Sheet",
                                                        "RKAP Balance Sheet", "Income Statement", "RKAP Income Statement",
                                                        "Cash Flow",
                                                        "RKAP Cash Flow", "RKAP Cash Flow", "Subsidiary Balance Sheet",
                                                        "Subsidiary FM Balance Sheet", "Subsidiary RKAP Balance Sheet",
                                                        "Subsidiary Income Statement", "Subsidiary FM Income Statement",
                                                        "Subsidiary RKAP Income Statemen",
                                                        "Subsidiary Cash Flow", "Subsidiary FM Cash Flow",
                                                        "Subsidiary RKAP Cash Flow",
                                                        "Debt Management"]:
                                        # print(f"Ignoring sheet: {sheet_name}")  # Optionally, print which sheet is being ignored
                                        logging.info(f"Skipping sheet: {sheet_name}")
                                        continue  # Skip processing this sheet
                                    else:
                                        skiprows = 4
                                        header = 0  # First row after skipping rows becomes header
                                    inferred_dashboard = None
                                    for dash in dashboard_folders:
                                        if dash.lower() in file.lower():
                                            inferred_dashboard = dash
                                            break

                                    if not inferred_dashboard:
                                        inferred_dashboard = "Unknown"
                                    df = pd.read_excel(local_copy_sas_url, sheet_name=sheet_name, skiprows=skiprows,
                                                    header=header)
                                    df['Company'] = subfolder
                                    df['Dashboard'] = inferred_dashboard
                                    # Check if the sheet is 'Construction Timeline' and flatten columns only for this sheet
                                    # Check if the sheet is 'Construction Timeline' and flatten columns only for this sheet
                                    if sheet_name in ["Project Expenses"]:
                                        # Check if the sheet requires flattening
                                        if isinstance(df.columns, pd.MultiIndex):
                                            # Flatten MultiIndex for specific sheets only
                                            df.columns = [' '.join(col).strip() for col in df.columns.values]

                                    for col in df.columns:
                                        if df[col].dtype == 'object':  # Check if the column is of string type
                                            df[col] = df[col].str.strip()  # Remove leading and trailing spaces

                                    df['Company'] = subfolder
                                    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^a-zA-Z0-9_]",
                                                                                                        "")

                                    # Replace NaN values with 0 for numeric columns
                                    df.fillna(0, inplace=True)

                                    if sheet_name == "Financial Performance":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Financial Performance' sheet.")

                                            required_columns = ['Date', 'Penalty_Cost_(IDR)', 'Remarks', 'Notes', ]

                                            for col in required_columns:
                                                # Rename columns to match the database schema if necessary
                                                column_mapping = {
                                                    'Date': 'Date',
                                                    'Penalty_Cost_(IDR)': 'PenaltyCost(IDR)',
                                                    'Remarks': 'Remarks',
                                                    'Notes': 'Notes'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # Insert data into the defined table
                                                table_name = 'dbo.OP_FinancialPerformance'
                                                existing_rows_query = f"""
                                                                                        SELECT Date, Remarks ,Company
                                                                                        FROM {table_name}
                                                                                    """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Date'], df['Remarks'],
                                                        df['Company']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                                INSERT INTO {table_name} (
                                                                                    [PenaltyCost(IDR)], [Notes], [Company], [Date], 
                                                                                    [Remarks]
                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?)
                                                                            """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['PenaltyCost(IDR)'], row['Notes'], row['Company'],
                                                            row['Date'],
                                                            row['Remarks']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")

                                                    update_insert_query = f"""
                                                                                                    IF EXISTS (
                                                                                                        SELECT 1
                                                                                                        FROM {table_name}
                                                                                                        WHERE  [Date] = ? AND [Remarks] = ? AND [Company] = ?
                                                                                                    )
                                                                                                    BEGIN
                                                                                                        UPDATE {table_name}
                                                                                                        SET 
                                                                                                            [PenaltyCost(IDR)] = ?, 
                                                                                                            [Notes] = ?

                                                                                                        WHERE [Date] = ? AND [Remarks] = ? AND [Company] = ?;
                                                                                                    END
                                                                                                    ELSE
                                                                                                    BEGIN
                                                                                                        INSERT INTO {table_name} (
                                                                                                            [PenaltyCost(IDR)], [Notes], [Company], [Date], 
                                                                                                            [Remarks]
                                                                                                        )
                                                                                                        VALUES (?, ?, ?, ?, ?);
                                                                                                    END
                                                                                                    """

                                                    logging.info("Beginning insertion into OP_FinancialPerformance table.")

                                                    for _, row in df.iterrows():
                                                        # Define the placeholders for this row
                                                        placeholders = (
                                                            # For IF EXISTS condition
                                                            row['Date'], row['Remarks'], row['Company'],

                                                            # For UPDATE clause
                                                            row['PenaltyCost(IDR)'], row['Notes'],

                                                            # WHERE conditions for UPDATE
                                                            row['Date'], row['Remarks'], row['Company'],

                                                            # For INSERT INTO clause
                                                            row['PenaltyCost(IDR)'], row['Notes'], row['Company'],
                                                            row['Date'],
                                                            row['Remarks']
                                                        )

                                            conn.commit()
                                            logging.info(
                                                "Data successfully processed and committed for 'FinancialPerformance' sheet.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Project Timeline":
                                        try:
                                            logging.info("Special processing for 'Project Timeline' sheet.")
                                            company_name = df['Company'].iloc[0]
                                            required_columns = ['Phase', 'Stage', 'Planned_Completion_Date',
                                                                'Actual_Completion_Date',
                                                                'Status', 'Progression']

                                            column_mapping = {
                                                'Phase': 'Phase',
                                                'Stage': 'Stage',
                                                'Planned_Completion_Date': 'PlannedCompletionDate',
                                                'Actual_Completion_Date': 'ActualCompletionDate',
                                                'Status': 'Status',
                                                'Progression': 'Progression'

                                            }
                                            df.rename(columns=column_mapping, inplace=True)
                                            df['PlannedCompletionDate'] = pd.to_datetime(df['PlannedCompletionDate'],
                                                                                        errors='coerce')
                                            df['ActualCompletionDate'] = pd.to_datetime(df['ActualCompletionDate'],
                                                                                        errors='coerce')

                                            # Remove the 'Created' column if it exists
                                            if 'Created' in df.columns:
                                                df.drop(columns=['Created'], inplace=True)
                                                logging.info("'Created' column removed.")

                                            table_name = '[dbo].[OP_ProjectTimeline]'
                                            existing_rows_query = f"""
                                                                    SELECT Company, Stage ,Status
                                                                    FROM {table_name}
                                                                """
                                            cursor.execute(existing_rows_query)
                                            rows = cursor.fetchall()
                                            existing_rows_set = {tuple(row) for row in
                                                                rows}  # Convert rows to tuples for hashing

                                            # Step 2: Compare with DataFrame
                                            df_tuples = set(
                                                zip(df['Company'], df['Stage'],
                                                    df['Status']))  # Convert df to a set of tuples

                                            missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                            if missing_rows:
                                                logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                # Step 3: Truncate the table before inserting new data
                                                truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                cursor.execute(truncate_query, (company_name,))

                                                insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                        [Company], [Phase], [Stage], [PlannedCompletionDate], [ActualCompletionDate],
                                                                        [Status], [Progression]                                             
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                                                """

                                                for _, row in df.iterrows():
                                                    cursor.execute(insert_query, (
                                                        row.get('Company', None),
                                                        row.get('Phase', None), row.get('Stage', None),
                                                        row.get('PlannedCompletionDate', None),
                                                        row.get('ActualCompletionDate', None), row.get('Status', None),
                                                        row.get('Progression', None)

                                                    ))
                                            else:
                                                logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                update_insert_query = f"""
                                                                IF EXISTS (
                                                                    SELECT 1
                                                                    FROM {table_name}
                                                                    WHERE [Company] = ? AND [Stage] = ? AND [Status] = ?
                                                                )
                                                                BEGIN
                                                                    UPDATE {table_name}
                                                                    SET 
                                                                        [PlannedCompletionDate] = ?, 
                                                                        [ActualCompletionDate] = ?, 
                                                                        [Progression] = ?,                                                                                                              
                                                                        [Phase] = ?

                                                                    WHERE [Company] = ? AND [Stage] = ? AND [Status] = ?;
                                                                END
                                                                ELSE
                                                                BEGIN
                                                                    INSERT INTO {table_name} (
                                                                        [Company], [Phase], [Stage], [PlannedCompletionDate], [ActualCompletionDate],
                                                                        [Status], [Progression]                                            
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?);
                                                                END
                                                            """
                                                logging.info("Beginning insertion into 'Project Timeline' table.")

                                                for _, row in df.iterrows():
                                                    cursor.execute(update_insert_query, (
                                                        # For IF EXISTS condition
                                                        row.get('Company', None), row.get('Stage', None),
                                                        row.get('Status', None),

                                                        # For UPDATE clause
                                                        row.get('PlannedCompletionDate', None),
                                                        row.get('ActualCompletionDate', None),
                                                        row.get('Progression', None),
                                                        row.get('Phase', None),

                                                        # WHERE conditions for UPDATE
                                                        row.get('Company', None), row.get('Stage', None),
                                                        row.get('Status', None),

                                                        # For INSERT INTO clause
                                                        row.get('Company', None),
                                                        row.get('Phase', None), row.get('Stage', None),
                                                        row.get('PlannedCompletionDate', None),
                                                        row.get('ActualCompletionDate', None), row.get('Status', None),
                                                        row.get('Progression', None)

                                                    ))

                                                # Commit the transaction
                                            conn.commit()
                                            logging.info(
                                                "Data successfully processed and committed for 'Project Timeline' sheet.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Project Detail":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Project Detail' sheet.")
                                            df.fillna(0, inplace=True)

                                            required_columns = ['Project_Duration_(Days)',
                                                                'Construction_Duration_(Days)', 'Currency',
                                                                'Construction', 'Others', 'Total', 'Currency.1',
                                                                'Construction.1', 'Others.1', 'Total.1', '%_Construction',
                                                                '%_Others', '%_total']

                                            for col in required_columns:
                                                column_mapping = {
                                                    'Project_Duration_(Days)': 'ProjectDuration_Days',
                                                    'Construction_Duration_(Days)': 'ConstructionDuration',
                                                    'Currency': 'Budget_Currency',
                                                    'Construction': 'Budget_Construction',
                                                    'Others': 'Budget_Other',
                                                    'Total': 'Budget_Total',
                                                    'Currency.1': 'Actual_Currency',
                                                    'Construction.1': 'Actual_Construction',
                                                    'Others.1': 'Actual_Other',
                                                    'Total.1': 'Actual_Total',
                                                    '%_Construction': 'ConstructionPercentage',
                                                    '%_Others': 'OtherPercentage',
                                                    '%_total': 'TotalPercentage'
                                                }

                                                df.rename(columns=column_mapping, inplace=True)
                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                project_list_df = pd.DataFrame(project_list,
                                                                            columns=['Subsidiary_Name', 'Project'])

                                                matching_projects = \
                                                    project_list_df[project_list_df['Subsidiary_Name'] == company_name][
                                                        'Project'].tolist()

                                                if matching_projects:
                                                    # Store the Project in DataFrame
                                                    df['Project'] = ', '.join(matching_projects)
                                                    logging.info(
                                                        f"Mapped Projects for Company '{company_name}': {df['Project'].iloc[0]}")

                                                current_project = df['Project'].iloc[0]
                                                table_name = "dbo.OP_ProjectDetail"
                                                existing_rows_query = f"""
                                                                    SELECT [Company], [Project] FROM {table_name} WHERE [Project] = ?
                                                                        """
                                                cursor.execute(existing_rows_query, (current_project,))
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in rows}


                                                all_current_records = set()
                                                for _, row in df.iterrows():
                                                    all_current_records.add(
                                                        (row['Company'], row['Project']))


                                                missing_rows = existing_rows_set - all_current_records
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")
                                                    truncate_query = f"""
                                                                                                                DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                                                                                WHERE [Company] = ? AND [Project] = ?
                                                                                                        """
                                                    missing_rows = list(missing_rows)
                                                    cursor.executemany(truncate_query, missing_rows)
                                                    conn.commit()
                                                    logging.info("Obsolete records deleted successfully.")

                                                    insert_query = f"""
                                                                                INSERT INTO {table_name} (
                                                                                    [Company],
                                                                                    [Project],
                                                                                    [ProjectDuration_Days],
                                                                                    [ConstructionDuration],
                                                                                    [Budget_Currency],
                                                                                    [Budget_Construction],
                                                                                    [Budget_Other],
                                                                                    [Budget_Total],
                                                                                    [Actual_Currency],
                                                                                    [Actual_Construction],
                                                                                    [Actual_Other],
                                                                                    [Actual_Total],
                                                                                    [ConstructionPercentage],
                                                                                    [OtherPercentage],
                                                                                    [TotalPercentage]
                                                                                )
                                                                                VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                            """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Project'],
                                                            row['ProjectDuration_Days'],
                                                            row['ConstructionDuration'], row['Budget_Currency'],
                                                            row['Budget_Construction'],
                                                            row['Budget_Other'],
                                                            row['Budget_Total'], row['Actual_Currency'],
                                                            row['Actual_Construction'],
                                                            row['Actual_Other'],
                                                            row['Actual_Total'],
                                                            row['ConstructionPercentage'], row['OtherPercentage'],
                                                            row['TotalPercentage']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                                    IF EXISTS (
                                                                                            SELECT 1
                                                                                            FROM {table_name}
                                                                                            WHERE [Company] = ? AND [Project] = ?
                                                                                        )
                                                                                        BEGIN
                                                                                            UPDATE {table_name}
                                                                                            SET 
                                                                                                [ProjectDuration_Days] = ?,
                                                                                                [ConstructionDuration] = ?,
                                                                                                [Budget_Currency] = ?,
                                                                                                [Budget_Construction] = ?,
                                                                                                [Budget_Other] = ?,
                                                                                                [Budget_Total] = ?,
                                                                                                [Actual_Currency] = ?,
                                                                                                [Actual_Construction] = ?,
                                                                                                [Actual_Other] = ?,
                                                                                                [Actual_Total] = ?,
                                                                                                [ConstructionPercentage] = ?,
                                                                                                [OtherPercentage] = ?,
                                                                                                [TotalPercentage] = ?                                                       

                                                                                            WHERE [Company] = ? AND [Project] = ?;
                                                                                        END
                                                                                        ELSE
                                                                                        BEGIN
                                                                                            INSERT INTO {table_name} (
                                                                                                [Company],
                                                                                                [Project],
                                                                                                [ProjectDuration_Days],
                                                                                                [ConstructionDuration],
                                                                                                [Budget_Currency],
                                                                                                [Budget_Construction],
                                                                                                [Budget_Other],
                                                                                                [Budget_Total],
                                                                                                [Actual_Currency],
                                                                                                [Actual_Construction],
                                                                                                [Actual_Other],
                                                                                                [Actual_Total],
                                                                                                [ConstructionPercentage],
                                                                                                [OtherPercentage],
                                                                                                [TotalPercentage]
                                                                                            )
                                                                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                                        END
                                                                                    """

                                                    logging.info("Beginning insertion into OP_Project Details table.")
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Project'],

                                                            # For UPDATE clause
                                                            row['ProjectDuration_Days'],
                                                            row['ConstructionDuration'],
                                                            row['Budget_Currency'],
                                                            row['Budget_Construction'],
                                                            row['Budget_Other'],
                                                            row['Budget_Total'],
                                                            row['Actual_Currency'],
                                                            row['Actual_Construction'],
                                                            row['Actual_Other'],
                                                            row['Actual_Total'],
                                                            row['ConstructionPercentage'],
                                                            row['OtherPercentage'],
                                                            row['TotalPercentage'],

                                                            # WHERE conditions for UPDATE
                                                            row['Company'], row['Project'],

                                                            # For INSERT INTO clause
                                                            row['Company'],
                                                            row['Project'],
                                                            row['ProjectDuration_Days'],
                                                            row['ConstructionDuration'],
                                                            row['Budget_Currency'],
                                                            row['Budget_Construction'],
                                                            row['Budget_Other'],
                                                            row['Budget_Total'],
                                                            row['Actual_Currency'],
                                                            row['Actual_Construction'],
                                                            row['Actual_Other'],
                                                            row['Actual_Total'],
                                                            row['ConstructionPercentage'],
                                                            row['OtherPercentage'],
                                                            row['TotalPercentage']
                                                        ))
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Project Expenses":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            # print(df.columns, "Expenses")
                                            logging.info("Special processing for 'Project Expenses' sheet.")
                                            df.columns = df.columns.str.replace(' ', '_').str.replace(r"[^a-zA-Z0-9_]", "")
                                            df['Actual_Construction'] = pd.to_numeric(df['Actual_Construction'],
                                                                                    errors='coerce')
                                            df['Actual_Others'] = pd.to_numeric(df['Actual_Others'], errors='coerce')
                                            df['Actual_Total'] = pd.to_numeric(df['Actual_Total'], errors='coerce')
                                            df.fillna(0, inplace=True)

                                            required_columns = ['Date_Unnamed:_0_level_1', 'Actual_Currency',
                                                                'Actual_Construction',
                                                                'Actual_Others', 'Actual_Total']

                                            for col in required_columns:
                                                column_mapping = {
                                                    'Date_Unnamed:_0_level_1': 'Date',
                                                    'Actual_Currency': 'Currency',
                                                    'Actual_Construction': 'Construction',
                                                    'Actual_Others': 'Other',
                                                    'Actual_Total': 'Total'

                                                }

                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # Insert data into the defined table
                                                # table_name = sheet_to_table_map[sheet_name]
                                                table_name = "dbo.OP_ProjectExpenses"
                                                existing_rows_query = f"""
                                                                        SELECT Date, Company
                                                                        FROM {table_name}
                                                                    """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Date'], df['Company']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                            [Date], [Company], [Currency], [Construction], [Other], [Total]
                                                                        )
                                                                        VALUES (?, ?, ?, ?, ?, ?)
                                                            """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row.get('Date', None),
                                                            row.get('Company', None),
                                                            row.get('Currency', None),
                                                            row.get('Construction', None),
                                                            row.get('Other', None),
                                                            row.get('Total', None)

                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                IF EXISTS (
                                                                        SELECT 1
                                                                        FROM {table_name}
                                                                        WHERE [Date] = ? AND [Company] = ?
                                                                    )
                                                                    BEGIN
                                                                        UPDATE {table_name}
                                                                        SET
                                                                            [Currency] = ?,
                                                                            [Construction] = ?,
                                                                            [Other] = ?,
                                                                            [Total] = ?

                                                                        WHERE [Date] = ? AND [Company] = ?;
                                                                    END
                                                                    ELSE
                                                                    BEGIN
                                                                        INSERT INTO {table_name} (
                                                                            [Date], [Company], [Currency], [Construction], [Other], [Total]

                                                                        )
                                                                        VALUES (?, ?, ?, ?, ?, ?);
                                                                    END

                                                                                                                    """

                                                    logging.info("Beginning insertion into OP_Project Expenses table.")

                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Date'], row['Company'],
                                                            # For UPDATE clause

                                                            row['Currency'],  # If Actual Completion is being updated
                                                            row['Construction'],  # Assuming Target Completion is updated
                                                            row['Other'],
                                                            row['Total'],

                                                            # WHERE conditions for UPDATE
                                                            row['Date'], row['Company'],

                                                            # For INSERT INTO clause
                                                            row['Date'], row['Company'], row['Currency'],
                                                            row['Construction'],
                                                            row['Other'],
                                                            row['Total']
                                                        ))
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Construction Timeline":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            df.columns = df.columns.str.replace(' ', '_').str.replace(r"[^a-zA-Z0-9_]", "")
                                            required_columns = ['Milestone', 'Planned', 'Actual', 'Planned.1', 'Forecasted',
                                                                'Unnamed:_5', 'Unnamed:_6', 'Unnamed:_7']
                                            # Ensure date columns are properly converted
                                            df['Planned'] = pd.to_datetime(df['Planned'], errors='coerce')
                                            df['Actual'] = pd.to_datetime(df['Actual'], errors='coerce')
                                            df['Planned.1'] = pd.to_datetime(df['Planned.1'], errors='coerce')
                                            df['Forecasted'] = pd.to_datetime(df['Forecasted'], errors='coerce')

                                            df['Unnamed:_5'] = df['Unnamed:_5'].fillna(
                                                0.0)  # Default fill for 'Completion' column
                                            df['Unnamed:_6'] = df['Unnamed:_6'].fillna(
                                                0)  # Default fill for 'Duration' column

                                            # For date columns, you can use forward-fill or backward-fill for missing dates
                                            df['Planned'] = df['Planned'].fillna(method='ffill')
                                            df['Actual'] = df['Actual'].fillna(method='ffill')
                                            df['Planned.1'] = df['Planned.1'].fillna(
                                                method='bfill')  # Assuming backward fill is appropriate for this column
                                            df['Forecasted'] = df['Forecasted'].fillna(method='bfill')  # Same as above

                                            # Check if there are any NaT values after conversion
                                            if df['Planned'].isna().any():
                                                print("Warning: Some 'PlannedStartDate' values could not be converted.")
                                            if df['Actual'].isna().any():
                                                print("Warning: Some 'ActualStartDate' values could not be converted.")
                                            if df['Planned.1'].isna().any():
                                                print("Warning: Some 'PlannedEndDate' values could not be converted.")
                                            if df['Forecasted'].isna().any():
                                                print("Warning: Some 'ForecastedEndDate' values could not be converted.")
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Milestone': 'Milestone',
                                                    'Planned': 'PlannedStartDate',
                                                    'Actual': 'ActualStartDate',
                                                    'Planned.1': 'PlannedEndDate',
                                                    'Forecasted': 'ForecastedEndDate',
                                                    'Unnamed:_5': 'Duration',
                                                    'Unnamed:_6': 'Completion'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info("'Created' column removed.")

                                                # Insert or update data into the database
                                                # table_name = sheet_to_table_map[sheet_name]
                                                table_name = 'dbo.OP_ConstructionTimeline'
                                                existing_rows_query = f"""
                                                                        SELECT Milestone, Company
                                                                        FROM {table_name}
                                                                    """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Milestone'], df['Company'], ))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                        INSERT INTO {table_name} (
                                                                            [Milestone], [PlannedStartDate], [ActualStartDate],
                                                                                [PlannedEndDate], [ForecastedEndDate], [Duration],
                                                                                [Completion], [Company]
                                                                        )
                                                                        VALUES (?, ?, ?, ?, ?, ?, ?,?)
                                                                    """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Milestone'],
                                                            row['PlannedStartDate'],
                                                            row['ActualStartDate'],
                                                            row['PlannedEndDate'],
                                                            row['ForecastedEndDate'],
                                                            row['Duration'],
                                                            row['Completion'],
                                                            row['Company']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")

                                                    if_exists_query = f"""
                                                            IF EXISTS (
                                                                SELECT 1
                                                                FROM {table_name}
                                                                WHERE [Milestone] = ? AND [Company] = ?
                                                            )
                                                            BEGIN
                                                                UPDATE {table_name}
                                                                SET
                                                                    [PlannedStartDate] = ?,
                                                                    [ActualStartDate] = ?,
                                                                    [PlannedEndDate] = ?,
                                                                    [ForecastedEndDate] = ?,
                                                                    [Duration] = ?,
                                                                    [Completion] = ?
                                                                WHERE [Milestone] = ? AND [Company] = ?;
                                                            END
                                                            ELSE
                                                            BEGIN
                                                                INSERT INTO {table_name} (
                                                                    [Milestone], [PlannedStartDate], [ActualStartDate],
                                                                    [PlannedEndDate], [ForecastedEndDate], [Duration],
                                                                    [Completion], [Company]
                                                                )
                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                                                            END
                                                        """
                                                    # Ensure correct data types in the DataFrame
                                                    for _, row in df.iterrows():
                                                        try:
                                                            row['PlannedStartDate'] = row[
                                                                'PlannedStartDate'].to_pydatetime() if \
                                                                row[
                                                                    'PlannedStartDate'] else None
                                                            row['ActualStartDate'] = row[
                                                                'ActualStartDate'].to_pydatetime() if row[
                                                                'ActualStartDate'] else None
                                                            row['PlannedEndDate'] = row['PlannedEndDate'].to_pydatetime() if \
                                                            row[
                                                                'PlannedEndDate'] else None
                                                            row['ForecastedEndDate'] = row[
                                                                'ForecastedEndDate'].to_pydatetime() if \
                                                                row[
                                                                    'ForecastedEndDate'] else None

                                                            row['Duration'] = int(row['Duration']) if row[
                                                                                                        'Duration'] is not None else 0
                                                            row['Completion'] = float(row['Completion']) if row[
                                                                                                                'Completion'] is not None else 0.0

                                                            # Prepare query placeholders
                                                            placeholders = (
                                                                row['Milestone'],
                                                                row['Company'],
                                                                row['PlannedStartDate'],
                                                                row['ActualStartDate'],
                                                                row['PlannedEndDate'],
                                                                row['ForecastedEndDate'],
                                                                row['Duration'],
                                                                row['Completion'],
                                                                row['Milestone'],
                                                                row['Company'],
                                                                row['Milestone'],
                                                                row['PlannedStartDate'],
                                                                row['ActualStartDate'],
                                                                row['PlannedEndDate'],
                                                                row['ForecastedEndDate'],
                                                                row['Duration'],
                                                                row['Completion'],
                                                                row['Company'],
                                                            )
                                                            cursor.execute(if_exists_query, placeholders)

                                                        except Exception as e:
                                                            logging.error(
                                                                f"Error converting row data: {row.to_dict()} -> {e}")
                                            conn.commit()
                                            logging.info(
                                                "Data successfully processed and committed for 'Construction Timeline' sheet.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Electricity Generation (Annualy":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Electricity Generation (Annualy)' sheet.")

                                            required_columns = ['Year', 'Contracted Electricity Delivered (MWh)',
                                                                'Tariff/MWh (IDR)']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Contracted_Electricity_Delivered_(MWh)': 'ContractedElectricityDelivered',
                                                    'Tariff/MWh_(IDR)': 'Tariff_MWh'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")
                                                df['ContractedElectricityDelivered'] = df[
                                                    'ContractedElectricityDelivered'].fillna(0)
                                                df['Tariff_MWh'] = df['Tariff_MWh'].fillna(0)

                                                # Insert data into the defined table
                                                # table_name = sheet_to_table_map[sheet_name]
                                                table_name = 'dbo.OP_AnnualyElectricityGeneration'
                                                existing_rows_query = f"""
                                                                        SELECT Year, Company
                                                                        FROM {table_name}
                                                                                    """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Year'], df['Company']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                        INSERT INTO {table_name} (
                                                                            [Year], [Company], [ContractedElectricityDelivered], [Tariff_MWh]

                                                                        )
                                                                        VALUES (?, ?, ?, ?)
                                                                                                    """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Year'], row['Company'],
                                                            row['ContractedElectricityDelivered'],
                                                            row['Tariff_MWh']

                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                IF EXISTS (
                                                                        SELECT 1
                                                                        FROM {table_name}
                                                                        WHERE [Year] = ? AND [Company] = ?
                                                                    )
                                                                    BEGIN
                                                                        UPDATE {table_name}
                                                                        SET
                                                                            [ContractedElectricityDelivered] = ?,
                                                                            [Tariff_MWh] = ?

                                                                        WHERE [Year] = ? AND [Company] = ?;
                                                                    END
                                                                    ELSE
                                                                    BEGIN
                                                                        INSERT INTO {table_name} (
                                                                            [Year], [Company], [ContractedElectricityDelivered], [Tariff_MWh]

                                                                        )
                                                                        VALUES (?, ?, ?, ?);
                                                                    END

                                                                    """

                                                    logging.info(
                                                        "Beginning insertion into OP_Electricity Generation (Annualy table.")
                                                    df = df.sort_values(by=['Company'])

                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Year'], row['Company'],

                                                            # For UPDATE clause
                                                            row['ContractedElectricityDelivered'], row['Tariff_MWh'],

                                                            # WHERE conditions for UPDATE
                                                            row['Year'], row['Company'],

                                                            # For INSERT INTO clause
                                                            row['Year'], row['Company'],
                                                            row['ContractedElectricityDelivered'],
                                                            row['Tariff_MWh']
                                                        ))
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail",
                                                                                            "Description": str(e)}

                                    elif sheet_name == "Electricity Generation (monthly":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Electricity Generation (Monthly)' sheet.")

                                            df = df.drop(columns=['Unnamed:_0'])
                                            # print(df.columns, "Monthly")
                                            required_columns = ['Year', 'Recorded Electricity Delivered (MWh)',
                                                                'Cummulative Electricity Delivered (MWh)'
                                                                'Settled Electricity Delivered (MWh)',
                                                                'Settlement Variance',
                                                                'CF (%)',
                                                                'AF (%)'
                                                                ]
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Recorded_Electricity_Delivered_(MWh)': 'RecordedElectricityDelivered',
                                                    'Cummulative_Electricity_Delivered_(MWh)': 'CummulativeElectricityDelivered',
                                                    'Settled_Electricity_Delivered_(MWh)': 'SettledElectricityDelivered',
                                                    'Settlement_Variance': 'SettlementVariance',
                                                    'CF_(%)': 'CFPercentage',
                                                    'AF_(%)': 'AFPercentage'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # Insert data into the defined table
                                                # table_name = sheet_to_table_map[sheet_name]
                                                table_name = 'dbo.OP_MonthlyElectricityGeneration'
                                                existing_rows_query = f"""
                                                                            SELECT Company, Month
                                                                            FROM {table_name}
                                                                        """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Company'], df['Month']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                            INSERT INTO {table_name} (
                                                                                    [Company], [Year], [RecordedElectricityDelivered],
                                                                                    [CummulativeElectricityDelivered], [SettledElectricityDelivered],
                                                                                    [SettlementVariance], [CFPercentage], [AFPercentage], [Month]

                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                        """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row.get('Company', None),
                                                            row.get('Year', None),
                                                            row.get('RecordedElectricityDelivered', None),
                                                            row.get('CummulativeElectricityDelivered', None),
                                                            row.get('SettledElectricityDelivered', None),
                                                            row.get('SettlementVariance', None),
                                                            row.get('CFPercentage', None),
                                                            row.get('AFPercentage', None),
                                                            row.get('Month', None)

                                                        ))
                                                else:

                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")

                                                    update_insert_query = f"""
                                                                        IF EXISTS (
                                                                                SELECT 1
                                                                                FROM {table_name}
                                                                                WHERE [Company] = ? AND [Month] = ?
                                                                            )
                                                                            BEGIN
                                                                                UPDATE {table_name}
                                                                                SET
                                                                                    [RecordedElectricityDelivered] = ?,
                                                                                    [CummulativeElectricityDelivered] = ?,
                                                                                    [SettledElectricityDelivered] = ?,
                                                                                    [SettlementVariance] = ?,
                                                                                    [CFPercentage] = ?,
                                                                                    [AFPercentage] = ?

                                                                                WHERE [Company] = ? AND [Month] = ?;
                                                                            END
                                                                            ELSE
                                                                            BEGIN
                                                                                INSERT INTO {table_name} (
                                                                                    [Company], [Year], [RecordedElectricityDelivered],
                                                                                    [CummulativeElectricityDelivered], [SettledElectricityDelivered],
                                                                                    [SettlementVariance], [CFPercentage], [AFPercentage], [Month]

                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                            END

                                                                            """

                                                    logging.info(
                                                        "Beginning insertion into OP_Electricity Generation (Monthly) table.")
                                                    # Log the DataFrame columns
                                                    # print(f"Columns in DataFrame: {df.columns}")
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],

                                                            # For UPDATE clause
                                                            row['RecordedElectricityDelivered'],
                                                            row['CummulativeElectricityDelivered'],
                                                            row['SettledElectricityDelivered'], row['SettlementVariance'],
                                                            row['CFPercentage'], row['AFPercentage'],

                                                            # WHERE conditions for UPDATE
                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Year'],
                                                            row['RecordedElectricityDelivered'],
                                                            row['CummulativeElectricityDelivered'],
                                                            row['SettledElectricityDelivered'], row['SettlementVariance'],
                                                            row['CFPercentage'], row['AFPercentage'], row['Month']
                                                        ))
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Electricity Generation (Daily)":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Electricity Generation (Daily)' sheet.")
                                            df = df.drop(columns=['Unnamed:_0'])
                                            df['Date'] = pd.to_datetime(df['Date'],
                                                                        errors='coerce')  # Converts invalid dates to NaT (null)

                                            required_columns = ['Date', 'Daily_Electricity_Generated',
                                                                'Cummulative_Electricity_Delivered_(MWh)'
                                                                'Progress_Bar']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Date': 'Date',
                                                    'Daily_Electricity_Generated': 'DailyElectricityGenerated',
                                                    'Cummulative_Electricity_Delivered_(MWh)': 'CummulativeElectricityDelivered',
                                                    'Progress_bar': 'ProgressBar'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = 'OP_DailyElectricityGeneration'

                                                existing_rows_query = f"""
                                                                        SELECT Date, Company
                                                                        FROM {table_name}
                                                                    """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Date'], df['Company']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")
                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                                    [Date], [Company], [DailyElectricityGenerated],
                                                                                    [CummulativeElectricityDelivered], [ProgressBar]

                                                                    )     VALUES (?, ?, ?, ?, ?)
                                                            """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row.get('Date', None),
                                                            row.get('Company', None),
                                                            row.get('DailyElectricityGenerated', None),
                                                            row.get('CummulativeElectricityDelivered', None),
                                                            row.get('ProgressBar', None)

                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                        IF EXISTS (
                                                                                SELECT 1
                                                                                FROM {table_name}
                                                                                WHERE [Date] = ? AND [Company] = ?
                                                                            )
                                                                            BEGIN
                                                                                UPDATE {table_name}
                                                                                SET
                                                                                    [DailyElectricityGenerated] = ?,
                                                                                    [CummulativeElectricityDelivered] = ?,
                                                                                    [ProgressBar] = ?

                                                                                WHERE [Date] = ? AND [Company] = ?;
                                                                            END
                                                                            ELSE
                                                                            BEGIN
                                                                                INSERT INTO {table_name} (
                                                                                    [Date], [Company], [DailyElectricityGenerated],
                                                                                    [CummulativeElectricityDelivered], [ProgressBar]

                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?);
                                                                            END
                                                                                                                            """

                                                    logging.info(
                                                        "Beginning insertion into OP_Electricity Generation (Daily) table.")
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Date'],row['Company'],

                                                            # For UPDATE clause
                                                            row['DailyElectricityGenerated'],
                                                            row['CummulativeElectricityDelivered'],
                                                            row['ProgressBar'],

                                                            # WHERE conditions for UPDATE
                                                            row['Date'],row['Company'],

                                                            # For INSERT INTO clause
                                                            row['Date'], row['Company'], row['DailyElectricityGenerated'],
                                                            row['CummulativeElectricityDelivered'], row['ProgressBar']

                                                        ))
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Coal Stockpile (Daily)":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Coal Stockpile (Daily)' sheet.")
                                            df = df.drop(columns=['Unnamed:_0'])

                                            required_columns = ['Date', 'Coal_Stockpile_(Days)',
                                                                'Mandated_Days_of_Coal_Stockpile']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Date': 'Date',
                                                    'Coal_Stockpile_(Days)': 'CoalStockpileDays',
                                                    'Mandated_Days_of_Coal_Stockpile': 'MandatedDaysOfCoalStockpile'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # Insert data into the defined table
                                                # table_name = sheet_to_table_map[sheet_name]
                                                table_name = 'dbo.OP_CoalStockpileDaily'
                                                existing_rows_query = f"""
                                                                            SELECT Company, Date
                                                                            FROM {table_name}
                                                                        """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Company'], df['Date']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                                INSERT INTO {table_name} (
                                                                                    [Company], [Date], [CoalStockpileDays],
                                                                                    [MandatedDaysOfCoalStockpile]
                                                                                )
                                                                                VALUES (?, ?, ?, ?)
                                                                        """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row.get('Company', None),
                                                            row.get('Date', None), row.get('CoalStockpileDays', None),
                                                            row.get('MandatedDaysOfCoalStockpile', None)

                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")

                                                    update_insert_query = f"""
                                                                        IF EXISTS (
                                                                                SELECT 1
                                                                                FROM {table_name}
                                                                                WHERE [Company] = ? AND [Date] = ?
                                                                            )
                                                                            BEGIN
                                                                                UPDATE {table_name}
                                                                                SET
                                                                                    [CoalStockpileDays] = ?,
                                                                                    [MandatedDaysOfCoalStockpile] = ?

                                                                                WHERE [Company] = ? AND [Date] = ?;
                                                                            END
                                                                            ELSE
                                                                            BEGIN
                                                                                INSERT INTO {table_name} (
                                                                                    [Company], [Date], [CoalStockpileDays],
                                                                                    [MandatedDaysOfCoalStockpile]


                                                                                )
                                                                                VALUES (?, ?, ?, ?);
                                                                            END

                                                                                                                            """

                                                    logging.info(
                                                        "Beginning insertion into OP_Electricity Generation (Daily) table.")

                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Date'],

                                                            # For UPDATE clause
                                                            row['CoalStockpileDays'], row['MandatedDaysOfCoalStockpile'],

                                                            # WHERE conditions for UPDATE
                                                            row['Company'], row['Date'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Date'], row['CoalStockpileDays'],
                                                            row['MandatedDaysOfCoalStockpile']

                                                        ))

                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Outages & Availability (Monthly":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Outages & Availability (Monthly' sheet.")
                                            df = df.drop(columns=['Unnamed:_0'])
                                            required_columns = ['Year', 'Month', 'Maintenance_Outage_(hours)', '%',
                                                                'Scheduled_Outage_(hours)', '%.1', 'Forced_Outage_(hours)',
                                                                '%.2',
                                                                'Actual_Outage_Hours', 'Permitted_Outage_Hours',
                                                                'Availability_Percentage',
                                                                'Required_Availability_Percentage'
                                                                ]

                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Month': 'Month',
                                                    'Maintenance_Outage_(hours)': 'MaintenanceOutageHours',
                                                    '%': 'MaintenanceOutagePercentage',
                                                    'Scheduled_Outage_(hours)': 'ScheduledOutageHours',
                                                    '%.1': 'ScheduledOutagePercentage',
                                                    'Forced_Outage_(hours)': 'ForcedOutageHours',
                                                    '%.2': 'ForcedOutagePercentage',
                                                    'Actual_Outage_Hours': 'ActualOutageHours',
                                                    'Permitted_Outage_Hours': 'PermittedOutageHours',
                                                    'Availability_Percentage': 'AvailabilityPercentage',
                                                    'Required_Availability_Percentage': 'RequiredAvailabilityPercentage'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # Insert data into the defined table
                                                # table_name = sheet_to_table_map[sheet_name]
                                                table_name = 'dbo.OP_MonthlyOutagesAndAvailability'
                                                existing_rows_query = f"""
                                                                            SELECT Company, Month
                                                                            FROM {table_name}
                                                                        """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Company'], df['Month']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                                    [Company], [Month], [Year],
                                                                                    [MaintenanceOutageHours],
                                                                                    [MaintenanceOutagePercentage],
                                                                                    [ScheduledOutageHours],
                                                                                    [ScheduledOutagePercentage],
                                                                                    [ForcedOutageHours],
                                                                                    [ForcedOutagePercentage],
                                                                                    [ActualOutageHours],
                                                                                    [PermittedOutageHours],
                                                                                    [AvailabilityPercentage],
                                                                                    [RequiredAvailabilityPercentage]


                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row.get('Company', None),
                                                            row.get('Month', None), row.get('Year', None),
                                                            row.get('MaintenanceOutageHours', None),
                                                            row.get('MaintenanceOutagePercentage', None),
                                                            row.get('ScheduledOutageHours', None),
                                                            row.get('ScheduledOutagePercentage', None),
                                                            row.get('ForcedOutageHours', None),
                                                            row.get('ForcedOutagePercentage', None),
                                                            row.get('ActualOutageHours', None),
                                                            row.get('PermittedOutageHours', None),
                                                            row.get('AvailabilityPercentage', None),
                                                            row.get('RequiredAvailabilityPercentage', None)

                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                        IF EXISTS (
                                                                                SELECT 1
                                                                                FROM {table_name}
                                                                                WHERE [Company] = ? AND [Month] = ?
                                                                            )
                                                                            BEGIN
                                                                                UPDATE {table_name}
                                                                                SET
                                                                                    [Year] = ?,
                                                                                    [MaintenanceOutageHours] = ?,
                                                                                    [MaintenanceOutagePercentage] = ?,
                                                                                    [ScheduledOutageHours] = ?,
                                                                                    [ScheduledOutagePercentage] = ?,
                                                                                    [ForcedOutageHours] = ?,
                                                                                    [ForcedOutagePercentage] = ?,
                                                                                    [ActualOutageHours] = ?,
                                                                                    [PermittedOutageHours] = ?,
                                                                                    [AvailabilityPercentage] = ?,
                                                                                    [RequiredAvailabilityPercentage] = ?


                                                                                WHERE [Company] = ? AND [Month] = ?;
                                                                            END
                                                                            ELSE
                                                                            BEGIN
                                                                                INSERT INTO {table_name} (
                                                                                    [Company], [Month], [Year],
                                                                                    [MaintenanceOutageHours],
                                                                                    [MaintenanceOutagePercentage],
                                                                                    [ScheduledOutageHours],
                                                                                    [ScheduledOutagePercentage],
                                                                                    [ForcedOutageHours],
                                                                                    [ForcedOutagePercentage],
                                                                                    [ActualOutageHours],
                                                                                    [PermittedOutageHours],
                                                                                    [AvailabilityPercentage],
                                                                                    [RequiredAvailabilityPercentage]


                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                            END

                                                                                                                            """

                                                    logging.info("Beginning insertion into Outages & Availability table.")

                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],

                                                            # For UPDATE clause
                                                            row['Year'], row['MaintenanceOutageHours'],
                                                            row['MaintenanceOutagePercentage'], row['ScheduledOutageHours'],
                                                            row['ScheduledOutagePercentage'], row['ForcedOutageHours'],
                                                            row['ForcedOutagePercentage'], row['ActualOutageHours'],
                                                            row['PermittedOutageHours'], row['AvailabilityPercentage'],
                                                            row['RequiredAvailabilityPercentage'],

                                                            # WHERE conditions for UPDATE
                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Month'], row['Year'],
                                                            row['MaintenanceOutageHours'],
                                                            row['MaintenanceOutagePercentage'], row['ScheduledOutageHours'],
                                                            row['ScheduledOutagePercentage'], row['ForcedOutageHours'],
                                                            row['ForcedOutagePercentage'], row['ActualOutageHours'],
                                                            row['PermittedOutageHours'], row['AvailabilityPercentage'],
                                                            row['RequiredAvailabilityPercentage']

                                                        ))
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Env - Scope 1 & 2 Emissions":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Env - Scope 1 & 2 Emissions'")
                                            # table_name = '[dbo].[Env-Scope1&2Emissions]'

                                            # print("Env-Scope1&2Emissions", df.columns)
                                            df = df.drop(columns=["Unnamed:_0"])
                                            # Clean the column names to remove leading/trailing spaces
                                            df.columns = df.columns.str.strip()
                                            required_columns = ['Month', 'Scope_1_tCO2e', 'Scope_2_tCO2e',
                                                                'Total_Scope_1_&_2']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Month': 'Month',
                                                    'Scope_1_tCO2e': 'Scope1_tCO2e',
                                                    'Scope_2_tCO2e': 'Scope2_tCO2e',
                                                    'Total_Scope_1_&_2': 'Total_Scope1&2'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # table_name = sheet_to_table_map[sheet_name]

                                                table_name = "[dbo].[SubsidiaryEnv-Scope1&2Emissions]"
                                                existing_rows_query = f"""
                                                                                        SELECT Company, Month
                                                                                        FROM {table_name}
                                                                                    """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Company'], df['Month']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                        INSERT INTO {table_name} (
                                                                            [Company], [Month], [Scope1_tCO2e], [Scope2_tCO2e], [Total_Scope1&2]
                                                                        )
                                                                        VALUES (?, ?, ?, ?, ?)
                                                                                    """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Month'],
                                                            row['Scope1_tCO2e'],
                                                            row['Scope2_tCO2e'],
                                                            row['Total_Scope1&2']
                                                        ))
                                                else:
                                                    logging.info(f"Table name set to: {table_name}")
                                                    update_insert_query = f"""
                                                                                IF EXISTS (
                                                                                    SELECT 1
                                                                                    FROM {table_name}
                                                                                    WHERE [Company] = ? AND [Month] = ?
                                                                                )
                                                                                BEGIN
                                                                                    UPDATE {table_name}
                                                                                    SET [Scope1_tCO2e] = ?, [Scope2_tCO2e] = ?, [Total_Scope1&2] = ?
                                                                                    WHERE [Company] = ? AND [Month] = ?;
                                                                                END
                                                                                ELSE
                                                                                BEGIN
                                                                                    INSERT INTO {table_name} (
                                                                                        [Company], [Month], [Scope1_tCO2e], [Scope2_tCO2e], [Total_Scope1&2]
                                                                                    )
                                                                                    VALUES (?, ?, ?, ?, ?);
                                                                                END
                                                                            """

                                                    logging.info("Beginning insertion into Env-Scope1&2Emissions table.")
                                                    # Log the DataFrame columns
                                                    # print(f"Columns in DataFrame: {df.columns}")
                                                    for _, row in df.iterrows():
                                                        # Define the placeholders for this row
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],
                                                            # For UPDATE clause

                                                            row['Scope1_tCO2e'],
                                                            row['Scope2_tCO2e'],
                                                            row['Total_Scope1&2'],

                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Month'],
                                                            row['Scope1_tCO2e'],
                                                            row['Scope2_tCO2e'],
                                                            row['Total_Scope1&2']
                                                        ))
                                                conn.commit()
                                                logging.info(
                                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                                sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                                "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Env - Utilities":
                                        try:
                                            logging.info("Special processing for 'Env-Utilities'")
                                            company_name = df['Company'].iloc[0]
                                            df = df.drop(columns=["Unnamed:_0"])

                                            required_columns = ['Month', 'Subsi_Electricity_Usage_(Wh)',
                                                                'Subsi_Actual_Water_Consumption_(m3)',
                                                                'Subsi_Actual_Fuel_Consumption_(L)'
                                                                ]

                                            for col in required_columns:
                                                column_mapping = {
                                                    'Month': 'Month',
                                                    'Subsi_Electricity_Usage_(Wh)': 'Subsi_ElectricityUsage(Wh)',
                                                    'Subsi_Actual_Water_Consumption_(m3)': 'Subsi_ActualWaterConsumption(m3)',
                                                    'Subsi_Actual_Fuel_Consumption_(L)': 'Subsi_ActualFuelConsumption(L)'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)

                                                table_name = "[dbo].[SubsidiaryEnv-Utilities]"
                                                logging.info(f"Table name set to: {table_name}")
                                                existing_rows_query = f"""
                                                                        SELECT Company, Month
                                                                        FROM {table_name}
                                                                        """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in rows}

                                                df_tuples = set(zip(df['Company'], df['Month']))

                                                missing_rows = df_tuples - existing_rows_set
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                        INSERT INTO {table_name} ([Company], [Month], [Subsi_ElectricityUsage(Wh)], [Subsi_ActualWaterConsumption(m3)], [Subsi_ActualFuelConsumption(L)])

                                                                                        VALUES (?, ?, ?, ?, ?)
                                                                """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Month'], row['Subsi_ElectricityUsage(Wh)'],
                                                            row['Subsi_ActualWaterConsumption(m3)'],
                                                            row['Subsi_ActualFuelConsumption(L)']
                                                        ))

                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                        IF EXISTS (
                                                            SELECT 1
                                                            FROM {table_name}
                                                            WHERE [Company] = ? AND [Month] = ?
                                                        )

                                                        BEGIN
                                                            UPDATE {table_name}
                                                            SET
                                                            [Subsi_ElectricityUsage(Wh)] = ?,
                                                            [Subsi_ActualWaterConsumption(m3)] = ?,
                                                            [Subsi_ActualFuelConsumption(L)] = ?
                                                            WHERE [Company] = ? AND [Month] = ?;
                                                        END
                                                        ELSE
                                                        BEGIN
                                                            INSERT INTO {table_name} (
                                                                [Company], [Month],
                                                                [Subsi_ElectricityUsage(Wh)],
                                                                [Subsi_ActualWaterConsumption(m3)],
                                                                [Subsi_ActualFuelConsumption(L)]
                                                            )
                                                            VALUES (?, ?, ?, ?, ?);
                                                        END

                                                    """
                                                    logging.info("Beginning insertion into ENV-Utilites table.")
                                                    for _, row in df.iterrows():
                                                        # Define the placeholders for this row
                                                        placeholders = (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],
                                                            # For UPDATE clause
                                                            row['Subsi_ElectricityUsage(Wh)'],
                                                            row['Subsi_ActualWaterConsumption(m3)'],
                                                            row['Subsi_ActualFuelConsumption(L)'],
                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Month'],
                                                            row['Subsi_ElectricityUsage(Wh)'],
                                                            row['Subsi_ActualWaterConsumption(m3)'],
                                                            row['Subsi_ActualFuelConsumption(L)'],
                                                        )
                                                conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Social - Employee by Gender":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Social - Employee by Gender'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            df.columns = df.columns.str.strip()
                                            required_columns = ['Month', 'Total_Male', 'Total_Female', 'New_Hire_Male',
                                                                'New_Hire_Female', 'Turnover_Male', 'Turnover_Female']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Month': 'Month',
                                                    'Total_Male': 'Total_Male',
                                                    'Total_Female': 'Total_Female',
                                                    'New_Hire_Male': 'NewHire_Male',
                                                    'New_Hire_Female': 'NewHire_Female',
                                                    'Turnover_Male': 'Turnover_Male',
                                                    'Turnover_Female': 'Turnover_Female'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # table_name = sheet_to_table_map[sheet_name]
                                                table_name = "[dbo].[SubsidiarySocial-EmployeeByGender]"
                                                logging.info(f"Table name set to: {table_name}")

                                                existing_rows_query = f"""
                                                                    SELECT Company, Month
                                                                    FROM {table_name}
                                                                    """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Company'], df['Month']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                INSERT INTO {table_name} (
                                                                [Company], [Month],
                                                                [Total_Male],
                                                                [Total_Female],
                                                                [NewHire_Male],
                                                                [NewHire_Female],
                                                                [Turnover_Male],
                                                                [Turnover_Female]
                                                            )
                                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                                            """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Month'],
                                                            row['Total_Male'],
                                                            row['Total_Female'],
                                                            row['NewHire_Male'],
                                                            row['NewHire_Female'],
                                                            row['Turnover_Male'],
                                                            row['Turnover_Female']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                        IF EXISTS (
                                                            SELECT 1
                                                            FROM {table_name}
                                                            WHERE [Company] = ? AND [Month] = ?
                                                        )
                                                        BEGIN
                                                            UPDATE {table_name}
                                                            SET [Total_Male] = ?,
                                                            [Total_Female] = ?,
                                                            [NewHire_Male] = ?,
                                                            [NewHire_Female] = ?,
                                                            [Turnover_Male] = ?,
                                                            [Turnover_Female] = ?


                                                            WHERE [Company] = ? AND [Month] = ?;
                                                        END
                                                        ELSE
                                                        BEGIN
                                                            INSERT INTO {table_name} (
                                                                [Company], [Month],
                                                                [Total_Male],
                                                                [Total_Female],
                                                                [NewHire_Male],
                                                                [NewHire_Female],
                                                                [Turnover_Male],
                                                                [Turnover_Female]
                                                            )
                                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                                                        END
                                                    """

                                                    logging.info("Beginning insertion into Social-EmployeeByGender table.")
                                                    # Log the DataFrame columns
                                                    # print(f"Columns in DataFrame: {df.columns}")
                                                    for _, row in df.iterrows():
                                                        # Define the placeholders for this row
                                                        placeholders = (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],
                                                            # For UPDATE clause

                                                            row['Total_Male'],
                                                            row['Total_Female'],
                                                            row['NewHire_Male'],
                                                            row['NewHire_Female'],
                                                            row['Turnover_Male'],
                                                            row['Turnover_Female'],

                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Month'],
                                                            row['Total_Male'],
                                                            row['Total_Female'],
                                                            row['NewHire_Male'],
                                                            row['NewHire_Female'],
                                                            row['Turnover_Male'],
                                                            row['Turnover_Female']
                                                        )
                                                conn.commit()
                                                logging.info(
                                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                                sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                                "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Social - Employee by Age":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Social - Employee by Age'")

                                            # table_name = '[dbo].[Social-EmployeeByAge]'

                                            df = df.drop(columns=["Unnamed:_0"])

                                            required_columns = ['Month', 'Total_<35', 'Total_35-50', 'Total_51-63',
                                                                'Total_>63',

                                                                'New_Hire_<35', 'New_Hire_35-50', 'New_Hire_51-63',
                                                                'New_Hire_>63',

                                                                'Turnover_<35', 'Turnover_35-50', 'Turnover_51-63',
                                                                'Turnover_>63'

                                                                ]

                                            for col in required_columns:

                                                # if col not in df.columns:

                                                #     logging.error(

                                                #         f"Missing required column '{col}' in Social-EmployeeByAge")

                                                #     continue

                                                # Rename columns to match the database schema if necessary

                                                column_mapping = {

                                                    'Month': 'Month',

                                                    'Total_<35': 'Total_<35',

                                                    'Total_35-50': 'Total_35-50',

                                                    'Total_51-63': 'Total_51-63',

                                                    'Total_>63': 'Total_>63',

                                                    'New_Hire_<35': 'New Hire_<35',

                                                    'New_Hire_35-50': 'New Hire_35-50',

                                                    'New_Hire_51-63': 'New Hire_51-63',

                                                    'New_Hire_>63': 'New Hire_>63',

                                                    'Turnover_<35': 'Turnover_<35',

                                                    'Turnover_35-50': 'Turnover_35-50',

                                                    'Turnover_51-63': 'Turnover_51-63',

                                                    'Turnover_>63': 'Turnover_>63',

                                                }

                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists

                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)

                                                    logging.info(f"'Created' column removed.")

                                                # Insert data into the defined table

                                                # table_name = sheet_to_table_map[sheet_name]

                                                table_name = "[dbo].[SubsidiarySocial-EmployeeByAge]"

                                                logging.info(f"Table name set to: {table_name}")

                                                existing_rows_query = f"""

                                                                        SELECT Company, Month

                                                                        FROM {table_name}

                                                                        """

                                                cursor.execute(existing_rows_query)

                                                rows = cursor.fetchall()

                                                existing_rows_set = {tuple(row) for row in

                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame

                                                df_tuples = set(
                                                    zip(df['Company'], df['Month']))  # Convert df to a set of tuples

                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows

                                                if missing_rows:

                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""

                                                                    INSERT INTO {table_name} ([Company], [Month], [Total_<35], [Total_35-50], [Total_51-63],  [Total_>63],

                                                                                                [New Hire_<35], [New Hire_35-50], [New Hire_51-63],[New Hire_>63], [Turnover_<35],

                                                                                                    [Turnover_35-50],[Turnover_51-63], [Turnover_>63])

                                                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                                                                """

                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (

                                                            row['Company'], row['Month'],  # For UPDATE clause

                                                            row['Total_<35'],

                                                            row['Total_35-50'],

                                                            row['Total_51-63'],

                                                            row['Total_>63'],

                                                            row['New Hire_<35'],

                                                            row['New Hire_35-50'],

                                                            row['New Hire_51-63'],

                                                            row['New Hire_>63'],

                                                            row['Turnover_<35'],

                                                            row['Turnover_35-50'],

                                                            row['Turnover_51-63'],

                                                            row['Turnover_>63'],

                                                        ))

                                                else:

                                                    logging.info(f"Table name set to: {table_name}")

                                                    update_insert_query = f"""

                                                                                IF EXISTS (

                                                                                    SELECT 1

                                                                                    FROM  {table_name}

                                                                                    WHERE [Company] = ? AND [Month] = ?

                                                                                )

                                                                                BEGIN

                                                                                    UPDATE  {table_name}

                                                                                    SET [Total_<35] = ?,

                                                                                    [Total_35-50] = ?,

                                                                                    [Total_51-63] = ?,

                                                                                    [Total_>63] = ?,

                                                                                    [New Hire_<35] = ?,

                                                                                    [New Hire_35-50] = ?,

                                                                                    [New Hire_51-63] = ?,

                                                                                    [New Hire_>63] = ?,

                                                                                    [Turnover_<35] = ?,

                                                                                    [Turnover_35-50] = ?,

                                                                                    [Turnover_51-63] = ?,

                                                                                    [Turnover_>63] = ?



                                                                                    WHERE [Company] = ? AND [Month] = ?;

                                                                                END

                                                                                ELSE

                                                                                BEGIN

                                                                                    INSERT INTO  {table_name} (

                                                                                        [Company], [Month],

                                                                                        [Total_<35],

                                                                                        [Total_35-50],

                                                                                        [Total_51-63],

                                                                                        [Total_>63],

                                                                                        [New Hire_<35],

                                                                                        [New Hire_35-50],

                                                                                        [New Hire_51-63],
                                                                                        [New Hire_>63],
                                                                                        [Turnover_<35],
                                                                                        [Turnover_35-50],
                                                                                        [Turnover_51-63],
                                                                                        [Turnover_>63]

                                                                                    )
                                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

                                                                                END
                                                                            """

                                                    logging.info("Beginning insertion into Social-EmployeeByAge table.")

                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['Company'], row['Month'],

                                                            # For UPDATE clause

                                                            row['Total_<35'],
                                                            row['Total_35-50'],
                                                            row['Total_51-63'],
                                                            row['Total_>63'],
                                                            row['New Hire_<35'],
                                                            row['New Hire_35-50'],
                                                            row['New Hire_51-63'],
                                                            row['New Hire_>63'],
                                                            row['Turnover_<35'],
                                                            row['Turnover_35-50'],
                                                            row['Turnover_51-63'],
                                                            row['Turnover_>63'],
                                                            row['Company'], row['Month'],

                                                            # For INSERT INTO clause
                                                            row['Company'], row['Month'],
                                                            row['Total_<35'],
                                                            row['Total_35-50'],
                                                            row['Total_51-63'],
                                                            row['Total_>63'],
                                                            row['New Hire_<35'],
                                                            row['New Hire_35-50'],
                                                            row['New Hire_51-63'],
                                                            row['New Hire_>63'],
                                                            row['Turnover_<35'],
                                                            row['Turnover_35-50'],
                                                            row['Turnover_51-63'],
                                                            row['Turnover_>63']
                                                        ))

                                                conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Social - CSR":
                                        try:
                                            logging.info("Special processing for 'Social - CSR'")
                                            company_name = df['Company'].iloc[0]
                                            # Drop unnecessary columns
                                            df = df.drop(columns=["Unnamed:_0", "Unnamed:_5"])

                                            # Clean column names
                                            df.columns = df.columns.str.strip()

                                            # Define required columns and rename
                                            column_mapping = {
                                                'Month': 'Month',
                                                'CSR__Project_Name': 'CSR_ProjectName',
                                                'CSR_Value': 'CSR_Value',
                                                'CSR_Disbursed': 'CSR_Disbursement'
                                            }
                                            df.rename(columns=column_mapping, inplace=True)

                                            # Remove the 'Created' column if it exists
                                            if 'Created' in df.columns:
                                                df.drop(columns=['Created'], inplace=True)
                                                logging.info(f"'Created' column removed.")

                                            table_name = "[dbo].[SubsidiarySocial-CSR]"

                                            # Step 1: Check if the sheet has rows that are NOT in the database
                                            existing_rows_query = f"""
                                                                        SELECT Company, Month
                                                                        FROM {table_name}
                                                                    """

                                            cursor.execute(existing_rows_query)
                                            rows = cursor.fetchall()
                                            existing_rows_set = {tuple(row) for row in
                                                                rows}  # Convert rows to tuples for hashing

                                            # Step 2: Compare with DataFrame
                                            df_tuples = set(
                                                zip(df['Company'], df['Month']))  # Convert df to a set of tuples

                                            missing_rows = df_tuples - existing_rows_set  # Find missing rows

                                            if missing_rows:
                                                logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                # Step 3: Truncate the table before inserting new data
                                                truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                cursor.execute(truncate_query, (company_name,))

                                                insert_query = f"""
                                                            INSERT INTO {table_name} (Company, Month, CSR_ProjectName, CSR_Value, CSR_Disbursement)
                                                            VALUES (?, ?, ?, ?, ?)
                                                        """

                                                for _, row in df.iterrows():
                                                    cursor.execute(insert_query, (
                                                        row['Company'], row['Month'], row['CSR_ProjectName'],
                                                        row['CSR_Value'],
                                                        row['CSR_Disbursement']
                                                    ))
                                            else:
                                                logging.info("Rows exist. Performing UPDATE or INSERT.")

                                                update_insert_query = f"""
                                                            IF EXISTS (
                                                                SELECT 1 FROM {table_name} WHERE Company = ? AND Month = ?
                                                            )
                                                            BEGIN
                                                                UPDATE {table_name}
                                                                SET CSR_ProjectName = ?, CSR_Value = ?, CSR_Disbursement = ?
                                                                WHERE Company = ? AND Month = ?;
                                                            END
                                                            ELSE
                                                            BEGIN
                                                                INSERT INTO {table_name} (Company, Month, CSR_ProjectName, CSR_Value, CSR_Disbursement)
                                                                VALUES (?, ?, ?, ?, ?);
                                                            END
                                                        """

                                                for _, row in df.iterrows():
                                                    cursor.execute(update_insert_query, (
                                                        row['Company'], row['Month'],
                                                        row['CSR_ProjectName'], row['CSR_Value'], row['CSR_Disbursement'],
                                                        row['Company'], row['Month'],
                                                        row['Company'], row['Month'],
                                                        row['CSR_ProjectName'], row['CSR_Value'], row['CSR_Disbursement']
                                                    ))

                                            # Commit the transaction
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Gov - Management Diversity":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Gov - Management Diversity'")
                                            # table_name = '[dbo].[Gov-ManagementDiversity]'
                                            # print("Gov - Management Diversity", df.columns)
                                            df = df.drop(columns=["Unnamed:_0"])
                                            # Clean the column names to remove leading/trailing spaces
                                            df.columns = df.columns.str.strip()
                                            required_columns = ['Month', 'Senior_Male', 'Senior_Female', 'Middle_Male',
                                                                'Middle_Female']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Month': 'Month',
                                                    'Senior_Male': 'Senior_Male',
                                                    'Senior_Female': 'Senior_Female',
                                                    'Middle_Male': 'Middle_Male',
                                                    'Middle_Female': 'Middle_Female'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                # Insert data into the defined table
                                                # table_name = sheet_to_table_map[sheet_name]

                                                table_name = "[dbo].[SubsidiaryGov-ManagementDiversity]"
                                                logging.info(f"Table name set to: {table_name}")

                                                existing_rows_query = f"""
                                                                                                    SELECT Company, Month
                                                                                                    FROM {table_name}
                                                                                                    """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Company'], df['Month']))  # Convert df to a set of tuples
                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))
                                                    insert_query = f"""
                                                                                                    INSERT INTO {table_name} (
                                                                                                                                [Company],[Month],
                                                                                                                                [Senior_Male],
                                                                                                                                [Senior_Female],
                                                                                                                                [Middle_Male],
                                                                                                                                [Middle_Female]

                                                                                                                            )
                                                                                                                    VALUES (?, ?, ?, ?, ?, ?)
                                                                                                """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Month'],
                                                            row['Senior_Male'],
                                                            row['Senior_Female'],
                                                            row['Middle_Male'],
                                                            row['Middle_Female']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                                                                IF EXISTS (
                                                                                                                        SELECT 1
                                                                                                                        FROM {table_name}
                                                                                                                        WHERE [Company] = ? AND [Month] = ?
                                                                                                                    )
                                                                                                                    BEGIN
                                                                                                                        UPDATE {table_name}
                                                                                                                        SET
                                                                                                                            [Senior_Male] = ?,
                                                                                                                            [Senior_Female] = ?,
                                                                                                                            [Middle_Male] = ?,
                                                                                                                            [Middle_Female] = ?


                                                                                                                        WHERE [Company] = ? AND [Month] = ?;
                                                                                                                    END
                                                                                                                    ELSE
                                                                                                                    BEGIN
                                                                                                                        INSERT INTO {table_name} (
                                                                                                                            [Company],[Month],
                                                                                                                            [Senior_Male],
                                                                                                                            [Senior_Female],
                                                                                                                            [Middle_Male],
                                                                                                                            [Middle_Female]

                                                                                                                        )
                                                                                                                        VALUES (?, ?, ?, ?, ?, ?);
                                                                                                                    END
                                                                                                                                                                """

                                                logging.info("Beginning insertion into Gov-ManagementDiversity table.")
                                                # Log the DataFrame columns
                                                # print(f"Columns in DataFrame: {df.columns}")
                                                for _, row in df.iterrows():
                                                    # Define the placeholders for this row
                                                    placeholders = (
                                                        # For IF EXISTS condition
                                                        row['Company'], row['Month'],

                                                        # For UPDATE clause

                                                        row['Senior_Male'],
                                                        row['Senior_Female'],
                                                        row['Middle_Male'],
                                                        row['Middle_Female'],

                                                        row['Company'], row['Month'],

                                                        # For INSERT INTO clause
                                                        row['Company'], row['Month'],
                                                        row['Senior_Male'],
                                                        row['Senior_Female'],
                                                        row['Middle_Male'],
                                                        row['Middle_Female']
                                                    )
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Gov - Board":
                                        try:
                                            logging.info("Special processing for 'Gov - Board'")
                                            company_name = df['Company'].iloc[0]
                                            df = df.drop(columns=["Unnamed:_0"])
                                            df.columns = df.columns.str.strip()

                                            required_columns = ['Year', 'Name', 'Gender', 'Types',
                                                                'Executive/non-executive',
                                                                'Independence_(yes/no)',
                                                                'Board_Independence_Percentage', 'Start_Date', 'End_Date',
                                                                'Remaining_Period', 'Tenure_years']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Name': 'Name',
                                                    'Gender': 'Gender',
                                                    'Types': 'Types',
                                                    'Executive/non-executive': 'Executive/Non-Executive',
                                                    'Independence_(yes/no)': 'Independence',
                                                    'Board_Independence_Percentage': 'BoardIndependencePercentage',
                                                    'Start_Date': 'StartDate',
                                                    'End_Date': 'EndDate',
                                                    'Remaining_Period': 'RemainingPeriod',
                                                    'Tenure_years': 'TenureYears'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")
                                                if 'FY' in df.columns:
                                                    try:
                                                        df['Year'] = df['Year'].astype(int)
                                                        logging.info("Converted 'Year' column to integers.")
                                                    except ValueError as ve:
                                                        logging.error(f"Failed to convert 'FY' column to integers: {ve}")
                                                        raise
                                                # Insert data into the defined table
                                                # table_name = sheet_to_table_map[sheet_name]
                                                # Replace blank and '-' values in specific columns
                                                df['BoardIndependencePercentage'] = pd.to_numeric(
                                                    df['BoardIndependencePercentage'],
                                                    errors='coerce')
                                                df['StartDate'] = pd.to_datetime(df['StartDate'], errors='coerce')
                                                df['EndDate'] = pd.to_datetime(df['EndDate'], errors='coerce')
                                                logging.info(f"main_folder: {main_folder}, sheet_name: {sheet_name}")

                                                table_name = "[dbo].[SubsidiaryGov-Board]"
                                                existing_rows_query = f"""
                                                                        SELECT Company, Name, Year
                                                                            FROM {table_name}
                                                                            """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing

                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['Company'], df['Name'],
                                                        df['Year']))  # Convert df to a set of tuples
                                                missing_rows = df_tuples - existing_rows_set  # Find missing rows
                                                logging.info(f"Table name set to: {table_name}")
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))

                                                    insert_query = f"""
                                                                INSERT INTO {table_name} (
                                                                                            [Company],
                                                                                            [Name],
                                                                                            [Year],
                                                                                            [Gender],
                                                                                            [Types],
                                                                                            [Executive/Non-Executive],
                                                                                            [Independence],
                                                                                            [BoardIndependencePercentage],
                                                                                            [StartDate],
                                                                                            [EndDate],
                                                                                            [RemainingPeriod],
                                                                                            [TenureYears]

                                                                                        )
                                                                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['Name'],
                                                            row['Year'],
                                                            row['Gender'],
                                                            row['Types'],
                                                            row['Executive/Non-Executive'],
                                                            row['Independence'],
                                                            row['BoardIndependencePercentage'],
                                                            row['StartDate'],
                                                            row['EndDate'],
                                                            row['RemainingPeriod'],
                                                            row['TenureYears']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                        IF EXISTS (
                                                                                SELECT 1
                                                                                FROM {table_name}
                                                                                WHERE [Company] = ? AND [Name] = AND [Year] ?
                                                                            )
                                                                            BEGIN
                                                                                UPDATE {table_name}
                                                                                SET
                                                                                    [Year] = ?,
                                                                                    [Gender] = ?,
                                                                                    [Types] = ?,
                                                                                    [Executive/Non-Executive] = ?,
                                                                                    [Independence] = ?,
                                                                                    [BoardIndependencePercentage] = ?,
                                                                                    [StartDate] = ?,
                                                                                    [EndDate] = ?,
                                                                                    [RemainingPeriod] = ?,
                                                                                    [TenureYears] = ?
                                                                                WHERE [Company] = ? AND [Name] AND [Year] = ?;
                                                                            END
                                                                            ELSE
                                                                            BEGIN
                                                                                INSERT INTO {table_name} (
                                                                                    [Company],
                                                                                    [Name],
                                                                                    [Year],
                                                                                    [Gender],
                                                                                    [Types],
                                                                                    [Executive/Non-Executive],
                                                                                    [Independence],
                                                                                    [BoardIndependencePercentage],
                                                                                    [StartDate],
                                                                                    [EndDate],
                                                                                    [RemainingPeriod],
                                                                                    [TenureYears]

                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                            END
                                                                                                """

                                                    logging.info("Beginning insertion into Gov-Board table.")
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "Targets":
                                        try:
                                            logging.info("Special processing for 'Targets'")
                                            company_name = df['Company'].iloc[0]
                                            df = df.drop(columns=["Unnamed:_0"])
                                            df.columns = df.columns.str.strip()
                                            required_columns = ['FY', 'Scope_1_Threshold_(tCO2)',
                                                                'Projected_Scope_1__(tCO2e)', 'Turnover_Target',
                                                                'Target_CSR_%',
                                                                'Total_CSR_Budget_($)', 'Subsi_Fuel_Consumption_Target_(L)',
                                                                'Subsi_Water_Consumption_Target_(L)']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'FY': 'FY',
                                                    'Scope_1_Threshold_(tCO2)': 'Scope1_Threshold_tCO2',
                                                    'Projected_Scope_1_(tCO2)': 'Projected_Scope1t_CO2',
                                                    'Turnover_Target': 'TurnoverTarget',
                                                    'Target_CSR_%': 'TargetCSR',
                                                    'Total_CSR_Budget_($)': 'TotalCSRBudget',
                                                    'Subsi_Fuel_Consumption_Target_(L)': 'Subsi_FuelConsumptionTarget(L)',
                                                    'Subsi_Water_Consumption_Target_(L)': 'Subsi_WaterConsumptionTarget(L)'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                # Remove the 'Created' column if it exists
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "[dbo].[SubsidiaryTargets]"
                                                logging.info(f"Table name set to: {table_name}")
                                                existing_rows_query = f"""
                                                                            SELECT FY, Company
                                                                            FROM {table_name}
                                                                            """

                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}  # Convert rows to tuples for hashing
                                                # Step 2: Compare with DataFrame
                                                df_tuples = set(
                                                    zip(df['FY'], df['Company']))  # Convert df to a set of tuples

                                                missing_rows = existing_rows_set - df_tuples  # Find missing rows

                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")

                                                    # Step 3: Truncate the table before inserting new data
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))
                                                    logging.info(f"Table name set to: {table_name}")
                                                    insert_query = f"""
                                                                        INSERT INTO {table_name} (
                                                                                    [FY],
                                                                                    [Company],
                                                                                    [Scope1_Threshold_tCO2],
                                                                                    [Projected_Scope1t_CO2],
                                                                                    [TurnoverTarget],
                                                                                    [TargetCSR],
                                                                                    [TotalCSRBudget],
                                                                                    [Subsi_FuelConsumptionTarget(L)],
                                                                                    [Subsi_WaterConsumptionTarget(L)]

                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                            """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['FY'], row['Company'],
                                                            row['Scope1_Threshold_tCO2'],
                                                            row['Projected_Scope1t_CO2'],
                                                            row['TurnoverTarget'],
                                                            row['TargetCSR'],
                                                            row['TotalCSRBudget'],
                                                            row['Subsi_FuelConsumptionTarget(L)'],
                                                            row['Subsi_WaterConsumptionTarget(L)']

                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                IF EXISTS (
                                                                    SELECT 1
                                                                    FROM {table_name}
                                                                    WHERE [FY] = ? AND [Company] = ?
                                                                )
                                                                BEGIN
                                                                    UPDATE {table_name}
                                                                    SET
                                                                        [Scope1_Threshold_tCO2] = ?,
                                                                        [Projected_Scope1t_CO2] = ?,
                                                                        [TurnoverTarget] = ?,
                                                                        [TargetCSR] = ?,
                                                                        [TotalCSRBudget] = ?,
                                                                        [Subsi_FuelConsumptionTarget(L)] = ?,
                                                                        [Subsi_WaterConsumptionTarget(L)] = ?

                                                                WHERE [FY] = ? AND [Company] = ?;
                                                                END
                                                                ELSE
                                                                BEGIN
                                                                    INSERT INTO {table_name} (
                                                                        [FY],
                                                                        [Company],
                                                                        [Scope1_Threshold_tCO2],
                                                                        [Projected_Scope1t_CO2],
                                                                        [TurnoverTarget],
                                                                        [TargetCSR],
                                                                        [TotalCSRBudget],
                                                                        [Subsi_FuelConsumptionTarget(L)],
                                                                        [Subsi_WaterConsumptionTarget(L)]

                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                END
                                                                                                                """

                                                    logging.info(f"Beginning insertion into {table_name}.")
                                                    # Log the DataFrame columns
                                                    # print(f"Columns in DataFrame: {df.columns}")
                                                    for _, row in df.iterrows():
                                                        # Define the placeholders for this row
                                                        cursor.execute(update_insert_query, (
                                                            # For IF EXISTS condition
                                                            row['FY'], row['Company'],

                                                            # For UPDATE clause
                                                            row['Scope1_Threshold_tCO2'],
                                                            row['Projected_Scope1t_CO2'],
                                                            row['TurnoverTarget'],
                                                            row['TargetCSR'],
                                                            row['TotalCSRBudget'],
                                                            row['Subsi_FuelConsumptionTarget(L)'],
                                                            row['Subsi_WaterConsumptionTarget(L)'],

                                                            row['FY'], row['Company'],

                                                            # For INSERT INTO clause
                                                            row['FY'], row['Company'],
                                                            row['Scope1_Threshold_tCO2'],
                                                            row['Projected_Scope1t_CO2'],
                                                            row['TurnoverTarget'],
                                                            row['TargetCSR'],
                                                            row['TotalCSRBudget'],
                                                            row['Subsi_FuelConsumptionTarget(L)'],
                                                            row['Subsi_WaterConsumptionTarget(L)'],

                                                        ))
                                            conn.commit()
                                            logging.info(
                                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                            "Description": "Sheet processed successfully"}

                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}
                                    else:
                                        continue
                        for (dashboard, sheet), status in sheet_status.items():
                            insert_log_query = """
                                    INSERT INTO [dbo].[SubsidiaryOperationESGDataLog] ([CompanyName], [Dashboard], [ModifiedDate], [SheetName], [Status], [Description])
                                    VALUES (?, ?, GETDATE(), ?, ?, ?)
                                """
                            cursor.execute(insert_log_query,
                                        (subfolder, dashboard, sheet, status["Status"], status["Description"]))
                            conn.commit()

                # Process the "Dashboard Configuration Master List" workbook directly
                logging.info("Processing 'Dashboard Configuration Master List' workbook")
                config_file_name = "Dashboard%20Configuration%20Master%20List.xlsx"
                config_relative_file_url = f"/sites/Dashboard-UAT/Shared%20Documents/{config_file_name}"

                try:
                    # Download the workbook
                    decoded_configfilename = unquote(config_relative_file_url)
                    print(f'filtered_files is {decoded_configfilename}')
                    file_content = File.open_binary(ctx, decoded_configfilename)

                    logging.info(f"config file content is {file_content}")
                    uploadToBlobStorage(file_content,"config_local_copy.xlsx")
                    logging.info("upload to blob config file successfully")
                    xls = pd.ExcelFile(config_local_copy_sas_url)
                    config_sheet_names = xls.sheet_names
                    logging.info(f"Sheet names in the workbook: {config_sheet_names}")

                    # Skip the "Preface" sheet if present
                    if "Preface" in config_sheet_names:
                        config_sheet_names.remove("Preface")
                        logging.info("Ignored 'Preface' sheet.")

                    # Process each relevant sheet
                    for sheet_name in sheet_to_table_map_config.keys():
                        if sheet_name not in config_sheet_names:
                            logging.warning(
                                f"Sheet '{sheet_name}' not found in 'Dashboard Configuration Master List' workbook. Skipping.")
                            continue

                        logging.info(f"Processing sheet: {sheet_name}")

                        df = pd.read_excel(config_local_copy_sas_url, sheet_name=sheet_name, skiprows=4, header=0)
                        for col in df.columns:
                            if df[col].dtype == 'object':  # Check if the column is of string type
                                df[col] = df[col].str.strip()  # Remove leading and trailing spaces
                        df.rename(columns={'Subsidiary Name': 'SubsidiaryName'}, inplace=True)
                        df['Source'] = sheet_name

                        # Drop the 'Source' column if it exists
                        if 'Source' in df.columns:
                            df = df.drop(columns=['Source'])

                        df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^a-zA-Z0-9_]", "")

                        df.fillna(0, inplace=True)

                        # Remove the 'Created' column if it exists
                        if 'Created' in df.columns:
                            df.drop(columns=['Created'], inplace=True)

                        # Create a connection string for pyodbc
                        conn = pyodbc.connect(connection_string)
                        logging.info("Connected to SQL Server successfully.")
                        cursor = conn.cursor()

                        if sheet_name == "Subsidiary List":
                            table_name = sheet_to_table_map_config[sheet_name]
                            if_exists_query = f"""
                                    IF EXISTS (
                                        SELECT 1 
                                        FROM {table_name}
                                        WHERE SubsidiaryName = ? AND (InvestmentAccountName = ? OR InvestmentAccountName IS NULL)
                                    )
                                    BEGIN
                                        UPDATE {table_name}
                                        SET 
                                            Abbreviation = ?, 
                                            IncomeAccountName = ?
                                        WHERE SubsidiaryName = ? AND (InvestmentAccountName = ? OR InvestmentAccountName IS NULL);
                                    END
                                    ELSE
                                    BEGIN
                                        INSERT INTO {table_name} (
                                            SubsidiaryName, Abbreviation, InvestmentAccountName, IncomeAccountName
                                        )
                                        VALUES (?, ?, ?, ?);
                                    END
                                """

                            # Process each row in the DataFrame
                            for _, row in df.iterrows():
                                placeholders = (
                                    str(row['SubsidiaryName']), str(row['InvestmentAccountName']),  # For IF EXISTS
                                    str(row['Abbreviation']), str(row['IncomeAccountName']),  # For UPDATE
                                    str(row['SubsidiaryName']), str(row['InvestmentAccountName']),
                                    # For UPDATE WHERE clause
                                    str(row['SubsidiaryName']), str(row['Abbreviation']),
                                    str(row['InvestmentAccountName']),
                                    # For INSERT
                                    str(row['IncomeAccountName'])  # For INSERT values
                                )
                                try:
                                    cursor.execute(if_exists_query, placeholders)
                                except Exception as e:
                                    logging.error(f"Failed to execute query for row {row}: {e}")
                        else:
                            table_name = sheet_to_table_map_config[sheet_name]
                            insert_query = f"""
                                    IF NOT EXISTS (
                                        SELECT 1 
                                        FROM {table_name}
                                        WHERE InvestmentAccount = ?
                                    )
                                    BEGIN
                                        INSERT INTO {table_name} (InvestmentAccount)
                                        VALUES (?);
                                    END
                                """

                            # Process each row in the DataFrame
                            for _, row in df.iterrows():
                                placeholders = (row['InvestmentAccount'], row['InvestmentAccount'])
                                try:
                                    cursor.execute(insert_query, placeholders)
                                except Exception as e:
                                    logging.error(f"Failed to execute query for row {row}: {e}")

                        conn.commit()
                        logging.info(
                            f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")

                except Exception as e:
                    logging.error(f"Error processing 'Dashboard Configuration Master List' workbook: {e}")
        else:
            logging.error("Authentication failed. Please check your credentials.")
            conn.close()
    except Exception as e:
        logging.error(f"Error during SharePoint authentication: {e}")

