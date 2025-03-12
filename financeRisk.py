import azure.functions as func
import requests
import os
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

@app.route(route="FinanceRiskFunction",auth_level=func.AuthLevel.ANONYMOUS)
def FinanceRiskFunction(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request...')
    index()

    # Return a simplified response
    return func.HttpResponse(
        "This function executed successfully.",
        status_code=200
        )

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
    "Balance Sheet": "dbo.BalanceSheet",
    "RKAP Balance Sheet": "dbo.RKAPBalanceSheet",
    "Income Statement": "dbo.IncomeStatement",
    "RKAP Income Statement": "dbo.RKAPIncomeStatement",
    "Cash Flow": "dbo.CashFlow",
    "RKAP Cash Flow": "dbo.RKAPCashFlow",
    "Risk Details": "dbo.RiskDetails",
    "KRI Details": "dbo.KRIDetails",
    "Inherent Risk": "dbo.InherentRisk",
    "Residual Risk": "dbo.ResidualRisk"

}

sheet_to_table_map_subsidiary = {
    "Subsidiary Balance Sheet": "dbo.SubsidiaryBalanceSheet",
    "Subsidiary FM Balance Sheet": "dbo.SubsidiaryFMBalanceSheet",
    "Subsidiary RKAP Balance Sheet": "dbo.SubsidiaryRKAPBalanceSheet",
    "Subsidiary Income Statement": "dbo.SubsidiaryIncomeStatement",
    "Subsidiary FM Income Statement": "dbo.SubsidiaryFMIncomeStatement",
    "Subsidiary RKAP Income Statemen": "dbo.SubsidiaryRKAPIncomeStatement",
    "Subsidiary Cash Flow": "dbo.SubsidiaryCashFlow",
    "Subsidiary FM Cash Flow": "dbo.SubsidiaryFMCashFlow",
    "Subsidiary RKAP Cash Flow": "dbo.SubsidiaryRKAPCashFlow",
    "Debt Management": "dbo.SubsidiaryDebtManagement",
    "Risk Details": "dbo.SubsidiaryRiskDetails",
    "KRI Details": "dbo.SubsidiaryKRIDetails",
    "Inherent Risk": "dbo.SubsidiaryInherentRisk",
    "Residual Risk": "dbo.SubsidiaryResidualRisk"

}

# Sheet-to-table mapping for the Dashboard Configuration Master List
sheet_to_table_map_config = {
    "Subsidiary List": "dbo.SubsidiaryList",
    "Investment List": "dbo.InvestmentAccountList"
}

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



def FinanceRiskFunctionIndex():
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

            main_and_subfolders = get_nested_folders(ctx)
            all_xlsx_files = []

            conn = pyodbc.connect(connection_string)
            logging.info("Connected to SQL Server successfully.")
            cursor = conn.cursor()

            for main_folder in main_and_subfolders:
                logging.info(f"Checking folder: {main_folder}")


               # Determine the appropriate mapping based on the folder name
                if main_folder == "Parent":
                    logging.info("Processing 'Parent' folder.")
                    xlsx_files = process_subfolders(ctx, parent_path="/sites/Dashboard-UAT/Shared%20Documents/Parent")
                    sheet_to_table_map = sheet_to_table_map_client_a
                    parent_path = "/sites/Dashboard-UAT/Shared%20Documents/Parent"
                    subfolders = get_subfolders(ctx, parent_path)
                    for subfolder in subfolders:
                        subfolder_path = f"{parent_path}/{subfolder}"
                        global_subfolder = subfolder

                        xlsx_files = process_subfolders(ctx, parent_path=subfolder_path)
                        all_xlsx_files.extend(xlsx_files)
                        dashboard_folders = get_subfolders(ctx, subfolder_path)

                        sheet_status = {}
                        dashboard_file_map = {}

                        for dashboard in dashboard_folders:
                            dashboard_path = f"{subfolder_path}/{dashboard}"
                            dashboard_files = [file for file in xlsx_files if file.startswith(dashboard_path)]
                            dashboard_file_map[dashboard] = dashboard_files
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
                                if "Preface" in sheet_names:
                                    sheet_names.remove("Preface")
                                for sheet_name in sheet_names:
                                    logging.info(f"Processing sheet: {sheet_name}")
                                    if sheet_name in ["Risk Details", "KRI Details", "Inherent Risk", "Residual Risk"]:
                                        skiprows = 5
                                        header = 0
                                    elif sheet_name in ["Financial Performance", "Project Timeline", "Construction Timeline",
                                                        "Electricity Generation (monthly", "Outages & Availability (Monthly",
                                                        "Project Detail", "Project S-Curve", "Electricity Generation (Daily)",
                                                        "Coal Stockpile (Daily)",
                                                        "Project Expenses", "Electricity Generation (Annualy",
                                                        "Env - Scope 1 & 2 Emissions", "Env - Utilities",
                                                        "Social - Employee by Gender",
                                                        "Social - Employee by Age", "Social - CSR",
                                                        "Gov - Management Diversity", "Gov - Board", "Targets",
                                                        "Operation Overview"]:
                                        logging.info(f"Skipping sheet: {sheet_name}")
                                        continue
                                    elif sheet_name in ["Project S-Curve", "Electricity Generation (Daily)",
                                                        "Coal Stockpile (Daily)",
                                                        "Project Expenses"]:
                                        skiprows = 1
                                        if sheet_name in ["Project Expenses"]:
                                            header = [0, 1]
                                        else:
                                            header = 0
                                    else:
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

                                    for col in df.columns:
                                        if df[col].dtype == 'object':
                                            df[col] = df[col].str.strip()

                                    df['Company'] = unquote(global_subfolder)
                                    df['Dashboard'] = inferred_dashboard
                                    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^a-zA-Z0-9_]", "")

                                    sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success",
                                                                                    "Description": "Sheet processed successfully"}

                                    df.fillna(0, inplace=True)
                                    global year
                                    global quarter
                                    if sheet_name == "Risk Details":
                                        logging.info("Special processing for 'Risk Details'")
                                        try:
                                            df = df.drop(columns=["Unnamed:_0"])
                                            df.columns = df.columns.str.strip()

                                            df.rename(columns={"Unnamed:_1": "Year", "Unnamed:_2": "Quarter"}, inplace=True)
                                            if 'Year' in df.columns and 'Quarter' in df.columns:
                                                year = str(df['Year'].iloc[0])
                                                quarter = str(df['Quarter'].iloc[0])
                                            else:
                                                logging.warning("Year or Quarter column not found in Risk Details")
                                            required_columns = ['Year', 'Quarter', 'Risk', 'Risk_ID',
                                                                'Risk_Type', 'Inherent_Risk', 'Target_Risk', 'Residual_Risk',
                                                                'Risk_Desc', 'Risk_Cause', 'Indicator', 'Unit', 'Safe', 'Caution',
                                                                'Danger', 'Type', 'Details', 'Effectivity', 'Category',
                                                                'Descripsion',
                                                                'Plan', 'Outcome', 'Cost', 'RKAP_Program', 'Risk_Owner']

                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Quarter': 'Quarter',
                                                    'Risk': 'Risk',
                                                    'Risk_ID': 'RiskID',
                                                    'Risk_Type': 'RiskType',
                                                    'Inherent_Risk': 'InherentRisk',
                                                    'Target_Risk': 'TargetRisk',
                                                    'Residual_Risk': 'ResidualRisk',
                                                    'Risk_Desc': 'RiskDesc',
                                                    'Risk_Cause': 'RiskCause',
                                                    'Indicator': 'KRIIndicator',
                                                    'Unit': 'KRIUnit',
                                                    'Safe': 'KRIThresholdSafe',
                                                    'Caution': 'KRIThresholdCaution',
                                                    'Danger': 'KRIThresholdDanger',
                                                    'Type': 'ExistingControlType',
                                                    'Details': 'ExistingControlDetails',
                                                    'Effectivity': 'ExistingControlEffectivity',
                                                    'Category': 'RiskImpactCategory',
                                                    'Descripsion': 'RiskImpactDescripsion',
                                                    'Plan': 'PreventionPlan',
                                                    'Outcome': 'PreventionOutcome',
                                                    'Cost': 'PreventionCost',
                                                    'RKAP_Program': 'PreventionRKAPProgram',
                                                    'Risk_Owner': 'RiskOwner'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "dbo.RiskDetails"
                                                df['Year'] = year
                                                df['Quarter'] = quarter

                                                logging.info(f"Table name set to: {table_name}")
                                                df['Year'] = df['Year'].astype(str)
                                                df['Company'] = df['Company'].str.strip()
                                                current_quarter = df['Quarter'].iloc[0]
                                                current_year = df['Year'].iloc[0]
                                                existing_records_query = f"""
                                                                            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                                                            SELECT [Company], [RiskID], [Year], [Quarter] 
                                                                            FROM {table_name} WITH (NOLOCK)
                                                                            WHERE [Year] = ? AND [Quarter] = ?
                                                                        """
                                                cursor.execute(existing_records_query, (current_year, current_quarter))
                                                existing_records = set(tuple(row) for row in cursor.fetchall())
                                                all_current_records = set()
                                                for _, row in df.iterrows():
                                                    all_current_records.add(
                                                        (row['Company'], row['RiskID'], row['Year'], row['Quarter']))
                                                records_to_delete = existing_records - all_current_records

                                                if records_to_delete:
                                                    logging.info(
                                                        f"Deleting {len(records_to_delete)} obsolete records from {table_name}.")

                                                    delete_query = f"""
                                                                    DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                                    WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                    """
                                                    records_to_delete = list(records_to_delete)
                                                    cursor.executemany(delete_query, records_to_delete)
                                                    conn.commit()
                                                    logging.info("Obsolete records deleted successfully.")

                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} WITH (TABLOCKX, HOLDLOCK) (
                                                                        [Company], 
                                                                        [RiskID],       
                                                                        [Year], 
                                                                        [Quarter],
                                                                        [Risk],                                                                                                                                  
                                                                        [RiskType], 
                                                                        [InherentRisk],
                                                                        [TargetRisk], 
                                                                        [ResidualRisk],
                                                                        [RiskDesc], 
                                                                        [RiskCause],
                                                                        [KRIIndicator],
                                                                        [KRIUnit],
                                                                        [KRIThresholdSafe],
                                                                        [KRIThresholdCaution], 
                                                                        [KRIThresholdDanger],
                                                                        [ExistingControlType],
                                                                        [ExistingControlDetails],
                                                                        [ExistingControlEffectivity], 
                                                                        [RiskImpactCategory],
                                                                        [RiskImpactDescripsion],
                                                                        [PreventionPlan],
                                                                        [PreventionOutcome],
                                                                        [PreventionCost], 
                                                                        [PreventionRKAPProgram],
                                                                        [RiskOwner] 
            
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                        """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'], row['Risk'],
                                                            row['RiskType'], row['InherentRisk'], row['TargetRisk'],
                                                            row['ResidualRisk'],
                                                            row['RiskDesc'], row['RiskCause'], row['KRIIndicator'], row['KRIUnit'],
                                                            row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'], row['KRIThresholdDanger'],
                                                            row['ExistingControlType'],
                                                            row['ExistingControlDetails'], row['ExistingControlEffectivity'],
                                                            row['RiskImpactCategory'],
                                                            row['RiskImpactDescripsion'], row['PreventionPlan'],
                                                            row['PreventionOutcome'],
                                                            row['PreventionCost'],
                                                            row['PreventionRKAPProgram'], row['RiskOwner']
                                                        ))
                                                    conn.commit()
                                                    logging.info("Obsolete records deleted successfully.")
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                            IF EXISTS (
                                                                                SELECT 1
                                                                                FROM {table_name}
                                                                                WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                                )
                                                                            BEGIN
                                                                                UPDATE {table_name}
                                                                            SET 
                                                                                [Risk] = ?,                                                                
                                                                                [RiskType] = ?, 
                                                                                [InherentRisk] = ?,
                                                                                [TargetRisk] = ?, 
                                                                                [ResidualRisk] = ?,
                                                                                [RiskDesc] = ?, 
                                                                                [RiskCause] = ?,
                                                                                [KRIIndicator] = ?,
                                                                                [KRIUnit] = ?,
                                                                                [KRIThresholdSafe] = ?,
                                                                                [KRIThresholdCaution] = ?, 
                                                                                [KRIThresholdDanger] = ?,
                                                                                [ExistingControlType] = ?,
                                                                                [ExistingControlDetails] = ?,
                                                                                [ExistingControlEffectivity] = ?, 
                                                                                [RiskImpactCategory] = ?,
                                                                                [RiskImpactDescripsion] = ?,
                                                                                [PreventionPlan] = ?,
                                                                                [PreventionOutcome] = ?,
                                                                                [PreventionCost] = ?, 
                                                                                [PreventionRKAPProgram] = ?,
                                                                                [RiskOwner] = ?                                                           
            
                                                                                WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?;
                                                                            END
                                                                            ELSE
                                                                            BEGIN
                                                                                INSERT INTO {table_name} (
                                                                                [Company],
                                                                                [RiskID],    
                                                                                [Year], 
                                                                                [Quarter],
                                                                                [Risk],                                                                                                                                              
                                                                                [RiskType], 
                                                                                [InherentRisk],
                                                                                [TargetRisk], 
                                                                                [ResidualRisk],
                                                                                [RiskDesc], 
                                                                                [RiskCause],
                                                                                [KRIIndicator],
                                                                                [KRIUnit],
                                                                                [KRIThresholdSafe],
                                                                                [KRIThresholdCaution], 
                                                                                [KRIThresholdDanger],
                                                                                [ExistingControlType],
                                                                                [ExistingControlDetails],
                                                                                [ExistingControlEffectivity], 
                                                                                [RiskImpactCategory],
                                                                                [RiskImpactDescripsion],
                                                                                [PreventionPlan],
                                                                                [PreventionOutcome],
                                                                                [PreventionCost], 
                                                                                [PreventionRKAPProgram],
                                                                                [RiskOwner] 
            
                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                                                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                            END
                                                                            """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Risk'], row['RiskType'], row['InherentRisk'],
                                                            row['TargetRisk'], row['ResidualRisk'], row['RiskDesc'],
                                                            row['RiskCause'],
                                                            row['KRIIndicator'], row['KRIUnit'], row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'],
                                                            row['KRIThresholdDanger'], row['ExistingControlType'],
                                                            row['ExistingControlDetails'],
                                                            row['ExistingControlEffectivity'], row['RiskImpactCategory'],
                                                            row['RiskImpactDescripsion'],
                                                            row['PreventionPlan'], row['PreventionOutcome'], row['PreventionCost'],
                                                            row['PreventionRKAPProgram'],
                                                            row['RiskOwner'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'], row['Risk'],
                                                            row['RiskType'], row['InherentRisk'], row['TargetRisk'],
                                                            row['ResidualRisk'],
                                                            row['RiskDesc'], row['RiskCause'], row['KRIIndicator'], row['KRIUnit'],
                                                            row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'], row['KRIThresholdDanger'],
                                                            row['ExistingControlType'],
                                                            row['ExistingControlDetails'], row['ExistingControlEffectivity'],
                                                            row['RiskImpactCategory'],
                                                            row['RiskImpactDescripsion'], row['PreventionPlan'],
                                                            row['PreventionOutcome'],
                                                            row['PreventionCost'],
                                                            row['PreventionRKAPProgram'], row['RiskOwner']
                                                        ))
                                                conn.commit()
                                                logging.info(f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                                sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success","Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    elif sheet_name == "KRI Details":
                                        try:
                                            logging.info("Special processing for 'Risk Details'")
                                            df.columns = df.columns.str.strip()
                                            if year and quarter:
                                                df['Year'] = year
                                                df['Quarter'] = quarter
                                            else:
                                                logging.warning(f"Year and Quarter not found for {sheet_name}")
                                            df.rename(columns={"Unnamed:_1": "Year", "Unnamed:_2": "Quarter"}, inplace=True)
                                            required_columns = ['Year', 'Quarter', 'Risk', 'RiskID', 'RiskType', 'Inherent_Risk',
                                                                'Residual_Risk','Current_KRI', 'Status_of_KRI', 'Indicator', 'Unit',
                                                                'Safe', 'Caution', 'Danger']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Quarter': 'Quarter',
                                                    'Risk': 'Risk',
                                                    'Risk_ID': 'RiskID',
                                                    'Risk_Type': 'RiskType',
                                                    'Inherent_Risk': 'InherentRisk',
                                                    'Residual_Risk': 'ResidualRisk',
                                                    'Current_KRI': 'CurrentKRI',
                                                    'Status_of_KRI': 'StatusofKRI',
                                                    'Indicator': 'KRIIndicator',
                                                    'Unit': 'KRIUnit',
                                                    'Safe': 'KRIThresholdSafe',
                                                    'Caution': 'KRIThresholdCaution',
                                                    'Danger': 'KRIThresholdDanger'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "dbo.KRIDetails"
                                                logging.info(f"Table name set to: {table_name}")
                                                df['Year'] = df['Year'].astype(str)
                                                df['Company'] = df['Company'].str.strip()
                                                current_quarter = df['Quarter'].iloc[0]
                                                current_year = df['Year'].iloc[0]
                                                existing_records_query = f"""
                                                                            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                                                            SELECT [Company], [RiskID], [Year], [Quarter] 
                                                                            FROM {table_name} WITH (NOLOCK)
                                                                            WHERE [Year] = ? AND [Quarter] = ?
                                                                        """
                                                cursor.execute(existing_records_query, (current_year, current_quarter))
                                                existing_records = set(tuple(row) for row in cursor.fetchall())
                                                all_current_records = set()
                                                for _, row in df.iterrows():
                                                    all_current_records.add(
                                                        (row['Company'], row['RiskID'], row['Year'], row['Quarter']))
                                                records_to_delete = existing_records - all_current_records
                                                if records_to_delete:
                                                    logging.info(
                                                        f"Deleting {len(records_to_delete)} obsolete records from {table_name}.")

                                                    delete_query = f"""
                                                                DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                                    WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                    """
                                                    records_to_delete = list(records_to_delete)
                                                    cursor.executemany(delete_query, records_to_delete)
                                                    conn.commit()
                                                    logging.info("Obsolete records deleted successfully.")
                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                        [Company],                                                         
                                                                        [RiskID], 
                                                                        [Year],
                                                                        [Quarter],
                                                                        [Risk],
                                                                        [RiskType], 
                                                                        [InherentRisk],                                                                
                                                                        [ResidualRisk],
                                                                        [CurrentKRI], 
                                                                        [StatusofKRI],
                                                                        [KRIIndicator],
                                                                        [KRIUnit],
                                                                        [KRIThresholdSafe],
                                                                        [KRIThresholdCaution], 
                                                                        [KRIThresholdDanger],
                                                                        [Modified]
            
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
                                                                    """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],
                                                            row['Risk'],
                                                            row['RiskType'],
                                                            row['InherentRisk'],
                                                            row['ResidualRisk'],
                                                            row['CurrentKRI'],
                                                            row['StatusofKRI'],
                                                            row['KRIIndicator'],
                                                            row['KRIUnit'],
                                                            row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'],
                                                            row['KRIThresholdDanger'],
                                                        ))
                                                    conn.commit()
                                                    logging.info("Obsolete records deleted successfully.")
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                    IF EXISTS (
                                                                        SELECT 1 FROM {table_name}
                                                                        WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                    )
                                                                    BEGIN
                                                                        UPDATE {table_name}
                                                                        SET 
                                                                            [Risk] = ?,                                                                
                                                                            [RiskType] = ?, 
                                                                            [InherentRisk] = ?,                                                                
                                                                            [ResidualRisk] = ?,
                                                                            [CurrentKRI] = ?,
                                                                            [StatusofKRI] = ?,                                                               
                                                                            [KRIIndicator] = ?,
                                                                            [KRIUnit] = ?,
                                                                            [KRIThresholdSafe] = ?,
                                                                            [KRIThresholdCaution] = ?, 
                                                                            [KRIThresholdDanger] = ?,
                                                                            [Modified] = GETDATE()                                                     
                                                                        WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?;
                                                                    END
                                                                    ELSE
                                                                    BEGIN
                                                                        INSERT INTO {table_name} (
                                                                            [Company],
                                                                            [RiskID],    
                                                                            [Year], 
                                                                            [Quarter],                                                       
                                                                            [Risk],
                                                                            [RiskType], 
                                                                            [InherentRisk],                                                                
                                                                            [ResidualRisk],
                                                                            [CurrentKRI], 
                                                                            [StatusofKRI],
                                                                            [KRIIndicator],
                                                                            [KRIUnit],
                                                                            [KRIThresholdSafe],
                                                                            [KRIThresholdCaution], 
                                                                            [KRIThresholdDanger],
                                                                            [Modified]
                                                                        )
                                                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
                                                                    END"""
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Risk'],
                                                            row['RiskType'],
                                                            row['InherentRisk'],
                                                            row['ResidualRisk'],
                                                            row['CurrentKRI'],
                                                            row['StatusofKRI'],
                                                            row['KRIIndicator'],
                                                            row['KRIUnit'],
                                                            row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'],
                                                            row['KRIThresholdDanger'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'], row['Risk'],
                                                            row['RiskType'],
                                                            row['InherentRisk'],
                                                            row['ResidualRisk'],
                                                            row['CurrentKRI'],
                                                            row['StatusofKRI'],
                                                            row['KRIIndicator'],
                                                            row['KRIUnit'],
                                                            row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'],
                                                            row['KRIThresholdDanger']
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

                                    elif sheet_name == "Inherent Risk":
                                        try:
                                            logging.info("Special processing for 'Inherent Risk'")
                                            df.columns = df.columns.str.strip()
                                            if year and quarter:
                                                df['Year'] = year
                                                df['Quarter'] = quarter
                                            else:
                                                logging.warning(f"Year and Quarter not found for {sheet_name}")
                                            df.rename(columns={"Unnamed:_1": "Year", "Unnamed:_2": "Quarter"}, inplace=True)
                                            required_columns = ['Year', 'Quarter', 'Risk_ID', 'Value__(Rp)', 'Impact_Scale',
                                                                'Value_(No)', 'Scale', 'Risk_Exposure_Value', 'Type', 'Details']
                                            for col in required_columns:

                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Quarter': 'Quarter',
                                                    'Risk_ID': 'RiskID',
                                                    'Value__(Rp)': 'RiskImpactValue',
                                                    'Impact_Scale': 'RiskImpactScale',
                                                    'Value_(No)': 'ProbabilityValue',
                                                    'Scale': 'ProbabilityScale',
                                                    'Risk_Exposure_Value': 'RiskExposureValue',
                                                    'Type': 'RiskScaleType',
                                                    'Details': 'RiskScaleDetails'

                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "dbo.InherentRisk"
                                                logging.info(f"Table name set to: {table_name}")
                                                df['Year'] = df['Year'].astype(str)  # If 'Year' is stored as VARCHAR
                                                df['Company'] = df['Company'].str.strip()
                                                current_quarter = df['Quarter'].iloc[0]
                                                current_year = df['Year'].iloc[0]

                                                existing_records_query = f"""
                                                                            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                                                            SELECT [Company], [RiskID], [Year], [Quarter] 
                                                                            FROM {table_name} WITH (NOLOCK)
                                                                            WHERE [Year] = ? AND [Quarter] = ?
                                                                        """
                                                cursor.execute(existing_records_query, (current_year, current_quarter))
                                                existing_records = set(tuple(row) for row in cursor.fetchall())
                                                all_current_records = set()
                                                for _, row in df.iterrows():
                                                    all_current_records.add(
                                                        (row['Company'], row['RiskID'], row['Year'], row['Quarter']))
                                                records_to_delete = existing_records - all_current_records

                                                if records_to_delete:
                                                    logging.info(
                                                        f"Deleting {len(records_to_delete)} obsolete records from {table_name}.")
                                                    delete_query = f"""
                                                                        DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                                        WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                        """
                                                    records_to_delete = list(records_to_delete)
                                                    cursor.executemany(delete_query, records_to_delete)
                                                    conn.commit()
                                                    logging.info("Obsolete records deleted successfully.")
                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                    [Company],                                                          
                                                                    [RiskID], 
                                                                    [Year],
                                                                    [Quarter],
                                                                    [RiskImpactValue], 
                                                                    [RiskImpactScale],                                                                
                                                                    [ProbabilityValue],
                                                                    [ProbabilityScale],
                                                                    [RiskExposureValue],                                                               
                                                                    [RiskScaleType],
                                                                    [RiskScaleDetails]
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],
                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails'],
                                                        ))
                                                    conn.commit()  # Commit all deletions once
                                                    logging.info("Obsolete records deleted successfully.")
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                        IF EXISTS (
                                                                            SELECT 1 FROM {table_name}
                                                                            WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                        )
                                                                        BEGIN
                                                                            UPDATE {table_name}
                                                                            SET 
                                                                                [RiskImpactValue] = ?, 
                                                                                [RiskImpactScale] = ?,                                                                
                                                                                [ProbabilityValue] = ?,
                                                                                [ProbabilityScale] = ?,
                                                                                [RiskExposureValue] = ?,                                                               
                                                                                [RiskScaleType] = ?,
                                                                                [RiskScaleDetails] = ?                                                     
                                                                            WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?;
                                                                        END
                                                                        ELSE
                                                                        BEGIN
                                                                            INSERT INTO {table_name} (
                                                                            [Company],                                                          
                                                                            [RiskID], 
                                                                            [Year],
                                                                            [Quarter],
                                                                            [RiskImpactValue], 
                                                                            [RiskImpactScale],                                                                
                                                                            [ProbabilityValue],
                                                                            [ProbabilityScale],
                                                                            [RiskExposureValue],                                                               
                                                                            [RiskScaleType],
                                                                            [RiskScaleDetails]
                                                                        )
                                                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                        END"""

                                                    logging.info("Beginning insertion into Risk Details table.")
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],
                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails'],
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

                                    elif sheet_name == "Residual Risk":
                                        try:
                                            logging.info("Special processing for 'Residual Risk'")
                                            df.columns = df.columns.str.strip()
                                            if year and quarter:
                                                df['Year'] = year
                                                df['Quarter'] = quarter
                                            else:
                                                logging.warning(f"Year and Quarter not found for {sheet_name}")
                                            required_columns = ['Risk_ID', 'Value__(Rp)', 'Impact_Scale',
                                                                'Value_(No)', 'Scale', 'Risk_Exposure_Value', 'Type', 'Details']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Risk_ID': 'RiskID',
                                                    'Value__(Rp)': 'RiskImpactValue',
                                                    'Impact_Scale': 'RiskImpactScale',
                                                    'Value_(No)': 'ProbabilityValue',
                                                    'Scale': 'ProbabilityScale',
                                                    'Risk_Exposure_Value': 'RiskExposureValue',
                                                    'Type': 'RiskScaleType',
                                                    'Details': 'RiskScaleDetails'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = "dbo.ResidualRisk"
                                                logging.info(f"Table name set to: {table_name}")

                                                logging.info(f"Table name set to: {table_name}")
                                                df['Year'] = df['Year'].astype(str)
                                                df['Company'] = df['Company'].str.strip()
                                                current_quarter = df['Quarter'].iloc[0]
                                                current_year = df['Year'].iloc[0]

                                                existing_records_query = f"""
                                                                                SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                                                                SELECT [Company], [RiskID], [Year], [Quarter] 
                                                                                FROM {table_name} WITH (NOLOCK)
                                                                                WHERE [Year] = ? AND [Quarter] = ?
                                                                            """
                                                cursor.execute(existing_records_query, (current_year, current_quarter))
                                                existing_records = set(tuple(row) for row in cursor.fetchall())
                                                all_current_records = set()
                                                for _, row in df.iterrows():
                                                    all_current_records.add(
                                                        (row['Company'], row['RiskID'], row['Year'], row['Quarter']))
                                                records_to_delete = existing_records - all_current_records
                                                if records_to_delete:
                                                    logging.info(
                                                        f"Deleting {len(records_to_delete)} obsolete records from {table_name}.")
                                                    delete_query = f"""
                                                                    DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                                        WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                        """
                                                    records_to_delete = list(records_to_delete)
                                                    cursor.executemany(delete_query, records_to_delete)
                                                    conn.commit()
                                                    logging.info("Obsolete records deleted successfully.")
                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                        [Company],                                                         
                                                                        [RiskID], 
                                                                        [Year],
                                                                        [Quarter],
                                                                        [RiskImpactValue], 
                                                                        [RiskImpactScale],                                                                
                                                                        [ProbabilityValue],
                                                                        [ProbabilityScale],
                                                                        [RiskExposureValue],                                                               
                                                                        [RiskScaleType],
                                                                        [RiskScaleDetails]
            
                                                                    )
                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                    """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],
                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails'],
                                                        ))
                                                    conn.commit()
                                                    logging.info("Obsolete records deleted successfully.")
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                        IF EXISTS (
                                                            SELECT 1 FROM {table_name}
                                                            WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                        )
                                                        BEGIN
                                                            UPDATE {table_name}
                                                            SET 
                                                                [RiskImpactValue] = ?, 
                                                                [RiskImpactScale] = ?,                                                                
                                                                [ProbabilityValue] = ?,
                                                                [ProbabilityScale] = ?,
                                                                [RiskExposureValue] = ?,                                                               
                                                                [RiskScaleType] = ?,
                                                                [RiskScaleDetails] = ?                                                    
                                                            WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?;
                                                        END
                                                        ELSE
                                                        BEGIN
                                                            INSERT INTO {table_name} (
                                                                [Company],
                                                                [RiskID],    
                                                                [Year], 
                                                                [Quarter],                                                       
                                                                [RiskImpactValue], 
                                                                [RiskImpactScale],                                                                
                                                                [ProbabilityValue],
                                                                [ProbabilityScale],
                                                                [RiskExposureValue],                                                               
                                                                [RiskScaleType],
                                                                [RiskScaleDetails]
                                                            )
                                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                        END"""

                                                    logging.info("Beginning insertion into Risk Details table.")
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],
                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails']
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

                                    else:
                                        try:
                                            required_columns = [
                                                'Account', 'Year', 'January', 'February', 'March', 'April', 'May', 'June', 'July',
                                                'August', 'September', 'October', 'November', 'December']

                                            for col in required_columns:

                                                column_mapping = {
                                                    'Account': 'Account',
                                                    'Year': 'Year',
                                                    'January': 'January',
                                                    'February': 'February',
                                                    'March': 'March',
                                                    'April': 'April',
                                                    'May': 'May',
                                                    'June': 'June',
                                                    'July': 'July',
                                                    'August': 'August',
                                                    'September': 'September',
                                                    'October': 'October',
                                                    'November': 'November',
                                                    'December': 'December'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)

                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")
                                                table_name = sheet_to_table_map[sheet_name]
                                                logging.info(f"Table name set to: {table_name}")
                                                existing_rows_query = f"""
                                                                    SELECT Account, Year, Company
                                                                    FROM {table_name}
                                                                    """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in
                                                                    rows}
                                                df_tuples = set(
                                                    zip(df['Account'], df['Year'], df['Company']))
                                                missing_rows = df_tuples - existing_rows_set
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")
                                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                                    cursor.execute(truncate_query)
                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                    Account, Year, Company, January, February, March, April, May, 
                                                                    June, July, August, September, October, November, December
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                    """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Account'], row['Year'], row['Company'],  # For INSERT
                                                            row['January'], row['February'], row['March'], row['April'], row['May'],
                                                            row['June'], row['July'], row['August'], row['September'],
                                                            row['October'], row['November'], row['December']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                IF EXISTS (
                                                                    SELECT 1 
                                                                    FROM {table_name}
                                                                    WHERE Account = ? AND Year = ? AND Company = ?
                                                                )
                                                                BEGIN
                                                                    UPDATE {table_name}
                                                                    SET 
                                                                        January = ?, February = ?, March = ?, April = ?, May = ?, June = ?, 
                                                                        July = ?, August = ?, September = ?, October = ?, November = ?, December = ?
                                                                    WHERE Account = ? AND Year = ? AND Company = ?;
                                                                END
                                                                ELSE
                                                                BEGIN
                                                                    INSERT INTO {table_name} (
                                                                        Account, Year, Company, January, February, March, April, May, 
                                                                        June, July, August, September, October, November, December
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                END
                                                                """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            row['Account'], row['Year'], row['Company'],  # For IF EXISTS
                                                            row['January'], row['February'], row['March'], row['April'], row['May'],
                                                            row['June'], row['July'], row['August'], row['September'],
                                                            row['October'], row['November'], row['December'],  # For UPDATE
                                                            row['Account'], row['Year'], row['Company'],  # For UPDATE WHERE clause
                                                            row['Account'], row['Year'], row['Company'],  # For INSERT
                                                            row['January'], row['February'], row['March'], row['April'], row['May'],
                                                            row['June'], row['July'], row['August'], row['September'],
                                                            row['October'], row['November'], row['December']  # For INSERT values
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

                        for (dashboard, sheet), status in sheet_status.items():
                            insert_log_query = """
                                    INSERT INTO [dbo].[FinanceRiskDataLog] ([CompanyName], [ModifiedDate], [Dashboard], [SheetName], [Status], [Description])
                                    VALUES (?, GETDATE(), ?, ?, ?, ?)
                                """
                            cursor.execute(insert_log_query,
                                        (subfolder, dashboard, sheet, status["Status"], status["Description"]))
                            conn.commit()


                if main_folder == "Subsidiary":
                    logging.info("Processing 'Subsidiary' folder.")
                    xlsx_files = process_subfolders(ctx, parent_path="/sites/Dashboard-UAT/Shared%20Documents/Subsidiary")
                    sheet_to_table_map = sheet_to_table_map_subsidiary
                    parent_path = "/sites/Dashboard-UAT/Shared%20Documents/Subsidiary"
                    subfolders = get_subfolders(ctx, parent_path)
                    logging.info(f"Found subfolders: {subfolders}")
                    for subfolder in subfolders:
                        subfolder_path = f"{parent_path}/{subfolder}"
                        logging.info(f"Processing subfolder: {subfolder}")
                        global_subfolder = subfolder

                        xlsx_files = process_subfolders(ctx, parent_path=subfolder_path)
                        all_xlsx_files.extend(xlsx_files)
                        dashboard_folders = get_subfolders(ctx, subfolder_path)

                        sheet_status = {}
                        dashboard_file_map = {}
                        for dashboard in dashboard_folders:
                            dashboard_path = f"{subfolder_path}/{dashboard}"
                            dashboard_files = [file for file in xlsx_files if file.startswith(dashboard_path)]
                            dashboard_file_map[dashboard] = dashboard_files

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

                                if "Preface" in sheet_names:
                                    sheet_names.remove("Preface")

                                for sheet_name in sheet_names:
                                    logging.info(f"Processing sheet: {sheet_name}")
                                    if sheet_name in ["Risk Details", "KRI Details", "Inherent Risk", "Residual Risk"]:
                                        skiprows = 5
                                        header = 0
                                    elif sheet_name in ["Financial Performance", "Project Timeline", "Construction Timeline",
                                                        "Electricity Generation (monthly", "Outages & Availability (Monthly",
                                                        "Project Detail", "Project S-Curve", "Electricity Generation (Daily)",
                                                        "Coal Stockpile (Daily)",
                                                        "Project Expenses", "Electricity Generation (Annualy",
                                                        "Env - Scope 1 & 2 Emissions", "Env - Utilities",
                                                        "Social - Employee by Gender",
                                                        "Social - Employee by Age", "Social - CSR",
                                                        "Gov - Management Diversity", "Gov - Board", "Targets",
                                                        "Operation Overview"]:
                                        logging.info(f"Skipping sheet: {sheet_name}")
                                        continue
                                    elif sheet_name in ["Project S-Curve", "Electricity Generation (Daily)",
                                                        "Coal Stockpile (Daily)",
                                                        "Project Expenses"]:
                                        skiprows = 1
                                        if sheet_name in ["Project Expenses"]:
                                            header = [0, 1]
                                        else:
                                            header = 0
                                    else:
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

                                    for col in df.columns:
                                        if df[col].dtype == 'object':
                                            df[col] = df[col].str.strip()
                                    df['Company'] = unquote(global_subfolder)
                                    df['Dashboard'] = inferred_dashboard
                                    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^a-zA-Z0-9_]", "")
                                    df.fillna(0, inplace=True)
                                    if sheet_name == "Debt Management":
                                        try:
                                            company_name = df['Company'].iloc[0]
                                            logging.info("Special processing for 'Debt Management' sheet.")

                                            required_columns = [
                                                'Account', 'Year', 'Loan', 'Type', 'Start Date', 'Due Date',
                                                'Original Principal Value (USD)', 'PaymentCategory', 'NextPayment Date',
                                                'January', 'February', 'March', 'April', 'May', 'June', 'July',
                                                'August', 'September', 'October', 'November', 'December'
                                            ]
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Account': 'Account',
                                                    'Year': 'Year',
                                                    'Loan': 'Loan',
                                                    'Type': 'Type',
                                                    'Start Date': 'Start_Date',
                                                    'Due Date': 'Due_Date',
                                                    'Original Principal Value (USD)': 'Original_Principal_Value_(USD)',
                                                    'PaymentCategory': 'Payment_Category',
                                                    'Next Payment Date': 'Next_Payment_Date',
                                                    'January': 'January',
                                                    'February': 'February',
                                                    'March': 'March',
                                                    'April': 'April',
                                                    'May': 'May',
                                                    'June': 'June',
                                                    'July': 'July',
                                                    'August': 'August',
                                                    'September': 'September',
                                                    'October': 'October',
                                                    'November': 'November',
                                                    'December': 'December'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")

                                                table_name = 'dbo.SubsidiaryDebtManagement'
                                                existing_rows_query = f"""
                                                                        SELECT Account, Year, Company, Payment_Category 
                                                                        FROM {table_name}
                                                                    """
                                                cursor.execute(existing_rows_query)
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in rows}
                                                df_tuples = set(zip(df['Account'], df['Year'], df['Company'],
                                                                    df['Payment_Category']))
                                                missing_rows = df_tuples - existing_rows_set
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Company = ?;"
                                                    cursor.execute(truncate_query, (company_name,))
                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                [Account], [Year], [Company], [January], [February], [March], [April], 
                                                                [May], 
                                                                [June], [July], [August], [September], [October], [November], [December], 
                                                                [Loan], [Type],
                                                                [Start_Date], [Due_Date], [Original_Principal_Value_(USD)],
                                                                [Payment_Category],
                                                                [Next_Payment_Date]
                                                                )
                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Account'], row['Year'], row['Company'],
                                                            row['January'], row['February'], row['March'], row['April'], row['May'],
                                                            row['June'], row['July'], row['August'], row['September'],
                                                            row['October'], row['November'], row['December'],
                                                            row['Loan'], row['Type'], row['Start_Date'], row['Due_Date'],
                                                            row['Original_Principal_Value_(USD)'], row['Payment_Category'],
                                                            row['Next_Payment_Date']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                        IF EXISTS (
                                                                            SELECT 1 
                                                                            FROM {table_name}
                                                                            WHERE [Account] = ? AND [Year] = ? AND [Company] = ? AND [Payment_Category] = ?
                                                                        )
                                                                        BEGIN
                                                                            UPDATE {table_name}
                                                                            SET 
                                                                                [Loan] = ?, [Type] = ?, [Start_Date] = ?, [Due_Date] = ?, 
                                                                                [Original_Principal_Value_(USD)] = ?, 
                                                                                [Next_Payment_Date] = ?,
                                                                                [January] = ?, [February] = ?, [March] = ?, [April] = ?, [May] = ?,
                                                                                [June] = ?, [July] = ?, [August] = ?, [September] = ?, [October] = ?,
                                                                                [November] = ?, [December] = ?
                                                                            WHERE [Account] = ? AND [Year] = ? AND [Company] = ? AND [Payment_Category] = ?;
                                                                        END
                                                                        ELSE
                                                                        BEGIN
                                                                            INSERT INTO {table_name} (
                                                                                [Account], [Year], [Company], [January], [February], [March], [April], 
                                                                                [May], 
                                                                                [June], [July], [August], [September], [October], [November], [December], 
                                                                                [Loan], [Type],
                                                                                [Start_Date], [Due_Date], [Original_Principal_Value_(USD)],
                                                                                [Payment_Category],
                                                                                [Next_Payment_Date]
                                                                            )
                                                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                        END 
                                                                        """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            row['Account'], row['Year'], row['Company'], row['Payment_Category'],

                                                            row['Loan'], row['Type'], row['Start_Date'], row['Due_Date'],
                                                            row['Original_Principal_Value_(USD)'],
                                                            row['Next_Payment_Date'],
                                                            row['January'], row['February'], row['March'], row['April'], row['May'],
                                                            row['June'],
                                                            row['July'], row['August'], row['September'], row['October'],
                                                            row['November'],
                                                            row['December'],

                                                            row['Account'], row['Year'], row['Company'], row['Payment_Category'],

                                                            row['Account'], row['Year'], row['Company'],
                                                            row['January'], row['February'], row['March'], row['April'], row['May'],
                                                            row['June'], row['July'], row['August'], row['September'],
                                                            row['October'], row['November'], row['December'],
                                                            row['Loan'], row['Type'], row['Start_Date'], row['Due_Date'],
                                                            row['Original_Principal_Value_(USD)'], row['Payment_Category'],
                                                            row['Next_Payment_Date']
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

                                    elif sheet_name == "Risk Details":
                                        try:
                                            logging.info("Special processing for 'Subsidiary Risk Details'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            df.columns = df.columns.str.strip()
                                            df.rename(columns={"Unnamed:_1": "Year", "Unnamed:_2": "Quarter"}, inplace=True)
                                            if 'Year' in df.columns and 'Quarter' in df.columns:
                                                year = str(df['Year'].iloc[0])
                                                quarter = str(df['Quarter'].iloc[0])
                                            else:
                                                logging.warning("Year or Quarter column not found in Risk Details")
                                            required_columns = ['Year', 'Quarter', 'Risk', 'Risk_ID',
                                                                'Risk_Type', 'Inherent_Risk', 'Target_Risk', 'Residual_Risk',
                                                                'Risk_Desc', 'Risk_Cause', 'Indicator', 'Unit', 'Safe', 'Caution',
                                                                'Danger', 'Type', 'Details', 'Effectivity', 'Category',
                                                                'Descripsion',
                                                                'Plan', 'Outcome', 'Cost', 'RKAP_Program', 'Risk_Owner']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Quarter': 'Quarter',
                                                    'Risk': 'Risk',
                                                    'Risk_ID': 'RiskID',
                                                    'Risk_Type': 'RiskType',
                                                    'Inherent_Risk': 'InherentRisk',
                                                    'Target_Risk': 'TargetRisk',
                                                    'Residual_Risk': 'ResidualRisk',
                                                    'Risk_Desc': 'RiskDesc',
                                                    'Risk_Cause': 'RiskCause',
                                                    'Indicator': 'KRIIndicator',
                                                    'Unit': 'KRIUnit',
                                                    'Safe': 'KRIThresholdSafe',
                                                    'Caution': 'KRIThresholdCaution',
                                                    'Danger': 'KRIThresholdDanger',
                                                    'Type': 'ExistingControlType',
                                                    'Details': 'ExistingControlDetails',
                                                    'Effectivity': 'ExistingControlEffectivity',
                                                    'Category': 'RiskImpactCategory',
                                                    'Descripsion': 'RiskImpactDescripsion',
                                                    'Plan': 'PreventionPlan',
                                                    'Outcome': 'PreventionOutcome',
                                                    'Cost': 'PreventionCost',
                                                    'RKAP_Program': 'PreventionRKAPProgram',
                                                    'Risk_Owner': 'RiskOwner'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")
                                                table_name = "[dbo].[SubsidiaryRiskDetails]"
                                                logging.info(f"Table name set to: {table_name}")
                                                df['Year'] = df['Year'].astype(str)
                                                df['Company'] = df['Company'].str.strip().str.lower()
                                                unique_company_quarters = df[['Company', 'Year', 'Quarter']].drop_duplicates()
                                                for _, cq in unique_company_quarters.iterrows():
                                                    company = cq['Company']
                                                    year = cq['Year']
                                                    quarter = cq['Quarter']
                                                    existing_records_query = f"""
                                                                                SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                                                                SELECT [Company], [RiskID], [Year], [Quarter]
                                                                                FROM {table_name} WITH (NOLOCK)
                                                                                WHERE [Company]  = ? AND [Year] = ? AND [Quarter] = ?
                                                                                """
                                                    cursor.execute(existing_records_query, (company, year, quarter))
                                                    existing_records = set(tuple(row) for row in cursor.fetchall())
                                                    company_df = df[(df['Company'] == company) & (df['Year'] == year) & (
                                                            df['Quarter'] == quarter)]
                                                    all_current_records = set()
                                                    for _, row in company_df.iterrows():
                                                        all_current_records.add(
                                                            (row['Company'], row['RiskID'], row['Year'], row['Quarter']))
                                                    records_to_delete = existing_records - all_current_records
                                                    if records_to_delete:
                                                        logging.info(
                                                            f"Deleting {len(records_to_delete)} obsolete records for {company} in {year} Q{quarter}.")
                                                        delete_query = f"""
                                                                        DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                                        WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                            """
                                                        cursor.executemany(delete_query, list(records_to_delete))
                                                        conn.commit()
                                                        logging.info("Obsolete records deleted successfully.")
                                                        insert_query = f"""
                                                                        INSERT INTO {table_name} WITH (TABLOCKX, HOLDLOCK) (
                                                                            [Company],
                                                                            [RiskID],
                                                                            [Year],
                                                                            [Quarter],
                                                                            [Risk],
                                                                            [RiskType],
                                                                            [InherentRisk],
                                                                            [TargetRisk],
                                                                            [ResidualRisk],
                                                                            [RiskDesc],
                                                                            [RiskCause],
                                                                            [KRIIndicator],
                                                                            [KRIUnit],
                                                                            [KRIThresholdSafe],
                                                                            [KRIThresholdCaution],
                                                                            [KRIThresholdDanger],
                                                                            [ExistingControlType],
                                                                            [ExistingControlDetails],
                                                                            [ExistingControlEffectivity],
                                                                            [RiskImpactCategory],
                                                                            [RiskImpactDescripsion],
                                                                            [PreventionPlan],
                                                                            [PreventionOutcome],
                                                                            [PreventionCost],
                                                                            [PreventionRKAPProgram],
                                                                            [RiskOwner]
                                                                        )
                                                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                        """

                                                        for _, row in df.iterrows():
                                                            cursor.execute(insert_query, (
                                                                row['Company'], row['RiskID'], row['Year'], row['Quarter'],
                                                                row['Risk'],
                                                                row['RiskType'], row['InherentRisk'], row['TargetRisk'],
                                                                row['ResidualRisk'],
                                                                row['RiskDesc'], row['RiskCause'], row['KRIIndicator'],
                                                                row['KRIUnit'],
                                                                row['KRIThresholdSafe'],
                                                                row['KRIThresholdCaution'], row['KRIThresholdDanger'],
                                                                row['ExistingControlType'],
                                                                row['ExistingControlDetails'], row['ExistingControlEffectivity'],
                                                                row['RiskImpactCategory'],
                                                                row['RiskImpactDescripsion'], row['PreventionPlan'],
                                                                row['PreventionOutcome'],
                                                                row['PreventionCost'],
                                                                row['PreventionRKAPProgram'], row['RiskOwner']
                                                            ))
                                                        conn.commit()
                                                        logging.info("Obsolete records deleted successfully.")
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                            IF EXISTS (
                                                                                    SELECT 1
                                                                                    FROM {table_name}
                                                                                    WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                                )
                                                                                BEGIN
                                                                                    UPDATE {table_name}
                                                                                    SET
                                                                                        [Risk] = ?,
                                                                                        [RiskType] = ?,
                                                                                        [InherentRisk] = ?,
                                                                                        [TargetRisk] = ?,
                                                                                        [ResidualRisk] = ?,
                                                                                        [RiskDesc] = ?,
                                                                                        [RiskCause] = ?,
                                                                                        [KRIIndicator] = ?,
                                                                                        [KRIUnit] = ?,
                                                                                        [KRIThresholdSafe] = ?,
                                                                                        [KRIThresholdCaution] = ?,
                                                                                        [KRIThresholdDanger] = ?,
                                                                                        [ExistingControlType] = ?,
                                                                                        [ExistingControlDetails] = ?,
                                                                                        [ExistingControlEffectivity] = ?,
                                                                                        [RiskImpactCategory] = ?,
                                                                                        [RiskImpactDescripsion] = ?,
                                                                                        [PreventionPlan] = ?,
                                                                                        [PreventionOutcome] = ?,
                                                                                        [PreventionCost] = ?,
                                                                                        [PreventionRKAPProgram] = ?,
                                                                                        [RiskOwner] = ?
                                                                                    WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?;
                                                                                END
                                                                                ELSE
                                                                                BEGIN
                                                                                    INSERT INTO {table_name} (
                                                                                        [Company],
                                                                                        [RiskID],
                                                                                        [Year],
                                                                                        [Quarter],
                                                                                        [Risk],
                                                                                        [RiskType],
                                                                                        [InherentRisk],
                                                                                        [TargetRisk],
                                                                                        [ResidualRisk],
                                                                                        [RiskDesc],
                                                                                        [RiskCause],
                                                                                        [KRIIndicator],
                                                                                        [KRIUnit],
                                                                                        [KRIThresholdSafe],
                                                                                        [KRIThresholdCaution],
                                                                                        [KRIThresholdDanger],
                                                                                        [ExistingControlType],
                                                                                        [ExistingControlDetails],
                                                                                        [ExistingControlEffectivity],
                                                                                        [RiskImpactCategory],
                                                                                        [RiskImpactDescripsion],
                                                                                        [PreventionPlan],
                                                                                        [PreventionOutcome],
                                                                                        [PreventionCost],
                                                                                        [PreventionRKAPProgram],
                                                                                        [RiskOwner]
                                                                                    )
                                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                                END
                                                                            """
                                                    logging.info("Beginning insertion into Risk Details table.")
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Risk'], row['RiskType'], row['InherentRisk'],
                                                            row['TargetRisk'], row['ResidualRisk'], row['RiskDesc'],
                                                            row['RiskCause'],
                                                            row['KRIIndicator'], row['KRIUnit'], row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'],
                                                            row['KRIThresholdDanger'], row['ExistingControlType'],
                                                            row['ExistingControlDetails'],
                                                            row['ExistingControlEffectivity'], row['RiskImpactCategory'],
                                                            row['RiskImpactDescripsion'],
                                                            row['PreventionPlan'], row['PreventionOutcome'], row['PreventionCost'],
                                                            row['PreventionRKAPProgram'],
                                                            row['RiskOwner'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'], row['Risk'],
                                                            row['RiskType'], row['InherentRisk'], row['TargetRisk'],
                                                            row['ResidualRisk'],
                                                            row['RiskDesc'], row['RiskCause'], row['KRIIndicator'], row['KRIUnit'],
                                                            row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'], row['KRIThresholdDanger'],
                                                            row['ExistingControlType'],
                                                            row['ExistingControlDetails'], row['ExistingControlEffectivity'],
                                                            row['RiskImpactCategory'],
                                                            row['RiskImpactDescripsion'], row['PreventionPlan'],
                                                            row['PreventionOutcome'],
                                                            row['PreventionCost'],
                                                            row['PreventionRKAPProgram'], row['RiskOwner']
                                                        ))
                                                conn.commit()
                                                logging.info(
                                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                                sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success", "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail",
                                                                                            "Description": str(e)}

                                    elif sheet_name == "KRI Details":
                                        try:
                                            logging.info("Special processing for 'Subsidiary KRI Details'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            df.columns = df.columns.str.strip()
                                            if year and quarter:
                                                df['Year'] = year
                                                df['Quarter'] = quarter
                                            else:
                                                logging.warning(f"Year and Quarter not found for {sheet_name}")
                                            required_columns = ['Year', 'Quarter', 'Risk', 'RiskID', 'RiskType', 'Inherent_Risk',
                                                                'Residual_Risk',
                                                                'Current_KRI', 'Status_of_KRI', 'Indicator', 'Unit',
                                                                'Safe', 'Caution', 'Danger']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Quarter': 'Quarter',
                                                    'Risk': 'Risk',
                                                    'Risk_ID': 'RiskID',
                                                    'Risk_Type': 'RiskType',
                                                    'Inherent_Risk': 'InherentRisk',
                                                    'Residual_Risk': 'ResidualRisk',
                                                    'Current_KRI': 'CurrentKRI',
                                                    'Status_of_KRI': 'StatusofKRI',
                                                    'Indicator': 'KRIIndicator',
                                                    'Unit': 'KRIUnit',
                                                    'Safe': 'KRIThresholdSafe',
                                                    'Caution': 'KRIThresholdCaution',
                                                    'Danger': 'KRIThresholdDanger'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")
                                                table_name = "[dbo].[SubsidiaryKRIDetails]"
                                                logging.info(f"Table name set to: {table_name}")
                                                df['Company'] = df['Company'].str.strip().str.lower()
                                                unique_company_quarters = df[['Company', 'Year', 'Quarter']].drop_duplicates()
                                                for _, cq in unique_company_quarters.iterrows():
                                                    company = cq['Company']
                                                    year = cq['Year']
                                                    quarter = cq['Quarter']
                                                    existing_records_query = f"""
                                                                                SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                                                                SELECT [Company], [RiskID], [Year], [Quarter]
                                                                                FROM {table_name} WITH (NOLOCK)
                                                                                WHERE [Company]  = ? AND [Year] = ? AND [Quarter] = ?
                                                                            """
                                                    cursor.execute(existing_records_query, (company, year, quarter))
                                                    existing_records = set(tuple(row) for row in cursor.fetchall())
                                                    company_df = df[(df['Company'] == company) & (df['Year'] == year) & (
                                                            df['Quarter'] == quarter)]
                                                    all_current_records = set()
                                                    for _, row in company_df.iterrows():
                                                        all_current_records.add(
                                                            (row['Company'], row['RiskID'], row['Year'], row['Quarter']))
                                                    records_to_delete = existing_records - all_current_records
                                                    if records_to_delete:
                                                        logging.info(
                                                            f"Deleting {len(records_to_delete)} obsolete records for {company} in {year} Q{quarter}.")
                                                        delete_query = f"""
                                                                        DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                                        WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                        """
                                                        cursor.executemany(delete_query, list(records_to_delete))
                                                        conn.commit()
                                                        logging.info("Obsolete records deleted successfully.")
                                                        insert_query = f"""
                                                                            INSERT INTO {table_name} (
                                                                                [Company],
                                                                                [RiskID],
                                                                                [Year],
                                                                                [Quarter],
                                                                                [Risk],
                                                                                [RiskType],
                                                                                [InherentRisk],
                                                                                [ResidualRisk],
                                                                                [CurrentKRI],
                                                                                [StatusofKRI],
                                                                                [KRIIndicator],
                                                                                [KRIUnit],
                                                                                [KRIThresholdSafe],
                                                                                [KRIThresholdCaution],
                                                                                [KRIThresholdDanger],
                                                                                [Modified]
                                                                            )
                                                                            VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
                                                                            END
                                                                        """
                                                        for _, row in df.iterrows():
                                                            placeholders = (
                                                                row['Company'],
                                                                row['RiskID'],
                                                                row['Year'], row['Quarter'],
                                                                row['Risk'],
                                                                row['RiskType'],
                                                                row['InherentRisk'],
                                                                row['ResidualRisk'],
                                                                row['CurrentKRI'],
                                                                row['StatusofKRI'],
                                                                row['KRIIndicator'],
                                                                row['KRIUnit'],
                                                                row['KRIThresholdSafe'],
                                                                row['KRIThresholdCaution'],
                                                                row['KRIThresholdDanger']
                                                            )
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                        IF EXISTS (
                                                                                SELECT 1
                                                                                FROM {table_name}
                                                                                WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                            )
                                                                            BEGIN
                                                                                UPDATE {table_name}
                                                                                SET
                                                                                    [Risk] = ?,
                                                                                    [RiskType] = ?,
                                                                                    [InherentRisk] = ?,
                                                                                    [ResidualRisk] = ?,
                                                                                    [CurrentKRI] = ?,
                                                                                    [StatusofKRI] = ?,
                                                                                    [KRIIndicator] = ?,
                                                                                    [KRIUnit] = ?,
                                                                                    [KRIThresholdSafe] = ?,
                                                                                    [KRIThresholdCaution] = ?,
                                                                                    [KRIThresholdDanger] = ?,
                                                                                    [Modified] = GETDATE()
                                                                                WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                            END
                                                                            ELSE
                                                                            BEGIN
                                                                                INSERT INTO {table_name} (
                                                                                    [Company],
                                                                                    [RiskID],
                                                                                    [Year],
                                                                                    [Quarter],
                                                                                    [Risk],
                                                                                    [RiskType],
                                                                                    [InherentRisk],
                                                                                    [ResidualRisk],
                                                                                    [CurrentKRI],
                                                                                    [StatusofKRI],
                                                                                    [KRIIndicator],
                                                                                    [KRIUnit],
                                                                                    [KRIThresholdSafe],
                                                                                    [KRIThresholdCaution],
                                                                                    [KRIThresholdDanger],
                                                                                    [Modified]
                                                                                )
                                                                                VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
                                                                            END
                                                                            """
                                                    logging.info("Beginning insertion into KRI Details table.")
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Risk'],
                                                            row['RiskType'],
                                                            row['InherentRisk'],
                                                            row['ResidualRisk'],
                                                            row['CurrentKRI'],
                                                            row['StatusofKRI'],
                                                            row['KRIIndicator'],
                                                            row['KRIUnit'],
                                                            row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'],
                                                            row['KRIThresholdDanger'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Company'],
                                                            row['RiskID'],
                                                            row['Year'], row['Quarter'], row['Risk'],
                                                            row['RiskType'],
                                                            row['InherentRisk'],
                                                            row['ResidualRisk'],
                                                            row['CurrentKRI'],
                                                            row['StatusofKRI'],
                                                            row['KRIIndicator'],
                                                            row['KRIUnit'],
                                                            row['KRIThresholdSafe'],
                                                            row['KRIThresholdCaution'],
                                                            row['KRIThresholdDanger']
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

                                    elif sheet_name == "Inherent Risk":
                                        try:
                                            logging.info("Special processing for 'Subsidiary Inherent Risk'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            df.columns = df.columns.str.strip()
                                            if year and quarter:
                                                df['Year'] = year
                                                df['Quarter'] = quarter
                                            else:
                                                logging.warning(f"Year and Quarter not found for {sheet_name}")
                                            required_columns = ['Year', 'Quarter', 'Risk_ID', 'Value__(Rp)', 'Impact_Scale',
                                                                'Value_(No)', 'Scale', 'Risk_Exposure_Value', 'Type', 'Details']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Quarter': 'Quarter',
                                                    'Risk_ID': 'RiskID',
                                                    'Value__(Rp)': 'RiskImpactValue',
                                                    'Impact_Scale': 'RiskImpactScale',
                                                    'Value_(No)': 'ProbabilityValue',
                                                    'Scale': 'ProbabilityScale',
                                                    'Risk_Exposure_Value': 'RiskExposureValue',
                                                    'Type': 'RiskScaleType',
                                                    'Details': 'RiskScaleDetails'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")
                                                table_name = "[dbo].[SubsidiaryInherentRisk]"
                                                logging.info(f"Table name set to: {table_name}")
                                                df['Company'] = df['Company'].str.strip().str.lower()
                                                unique_company_quarters = df[['Company', 'Year', 'Quarter']].drop_duplicates()
                                                for _, cq in unique_company_quarters.iterrows():
                                                    company = cq['Company']
                                                    year = cq['Year']
                                                    quarter = cq['Quarter']
                                                    existing_records_query = f"""
                                                                                SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                                                                SELECT [Company], [RiskID], [Year], [Quarter]
                                                                                FROM {table_name} WITH (NOLOCK)
                                                                                WHERE [Company]  = ? AND [Year] = ? AND [Quarter] = ?
                                                                            """
                                                    cursor.execute(existing_records_query, (company, year, quarter))
                                                    existing_records = set(tuple(row) for row in cursor.fetchall())
                                                    company_df = df[(df['Company'] == company) & (df['Year'] == year) & (
                                                            df['Quarter'] == quarter)]
                                                    all_current_records = set()
                                                    for _, row in company_df.iterrows():
                                                        all_current_records.add(
                                                            (row['Company'], row['RiskID'], row['Year'], row['Quarter']))
                                                    records_to_delete = existing_records - all_current_records
                                                    if records_to_delete:
                                                        logging.info(
                                                            f"Deleting {len(records_to_delete)} obsolete records for {company} in {year} Q{quarter}.")
                                                        delete_query = f"""
                                                                        DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                                        WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                    """
                                                        cursor.executemany(delete_query, list(records_to_delete))
                                                        conn.commit()
                                                        logging.info("Obsolete records deleted successfully.")
                                                        insert_query = f"""
                                                                        INSERT INTO {table_name} (
                                                                            [Company],
                                                                            [RiskID],
                                                                            [Year],
                                                                            [Quarter],
                                                                            [RiskImpactValue],
                                                                            [RiskImpactScale],
                                                                            [ProbabilityValue],
                                                                            [ProbabilityScale],
                                                                            [RiskExposureValue],
                                                                            [RiskScaleType],
                                                                            [RiskScaleDetails]
                                                                        )
                                                                        VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                        END
                                                                        """
                                                        for _, row in df.iterrows():
                                                            placeholders = (

                                                                row['Company'],
                                                                row['RiskID'],
                                                                row['Year'], row['Quarter'],
                                                                row['RiskImpactValue'],
                                                                row['RiskImpactScale'],
                                                                row['ProbabilityValue'],
                                                                row['ProbabilityScale'],
                                                                row['RiskExposureValue'],
                                                                row['RiskScaleType'],
                                                                row['RiskScaleDetails']
                                                            )
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                            IF EXISTS (
                                                                                    SELECT 1
                                                                                    FROM {table_name}
                                                                                    WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                                )
                                                                                BEGIN
                                                                                    UPDATE {table_name}
                                                                                    SET
                                                                                        [RiskImpactValue] = ?,
                                                                                        [RiskImpactScale] = ?,
                                                                                        [ProbabilityValue] = ?,
                                                                                        [ProbabilityScale] = ?,
                                                                                        [RiskExposureValue] = ?,
                                                                                        [RiskScaleType] = ?,
                                                                                        [RiskScaleDetails] = ?
                                                                                    WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                                END
                                                                                ELSE
                                                                                BEGIN
                                                                                    INSERT INTO {table_name} (
                                                                                        [Company],
                                                                                        [RiskID],
                                                                                        [Year],
                                                                                        [Quarter],
                                                                                        [RiskImpactValue],
                                                                                        [RiskImpactScale],
                                                                                        [ProbabilityValue],
                                                                                        [ProbabilityScale],
                                                                                        [RiskExposureValue],
                                                                                        [RiskScaleType],
                                                                                        [RiskScaleDetails]
                                                                                    )
                                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?,?);
                                                                                END
                                                                                """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],
                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Company'],
                                                            row['RiskID'],
                                                            row['Year'], row['Quarter'],
                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails']
                                                        ))
                                                conn.commit()
                                                logging.info(
                                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                                sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success", "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail","Description": str(e)}

                                    elif sheet_name == "Residual Risk":
                                        try:
                                            logging.info("Special processing for Subsidiary Residual Risk'")
                                            df = df.drop(columns=["Unnamed:_0"])
                                            df.columns = df.columns.str.strip()
                                            if year and quarter:
                                                df['Year'] = year
                                                df['Quarter'] = quarter
                                            else:
                                                logging.warning(f"Year and Quarter not found for {sheet_name}")
                                            required_columns = ['Year', 'Quarter', 'Risk_ID', 'Value__(Rp)', 'Impact_Scale',
                                                                'Value_(No)', 'Scale', 'Risk_Exposure_Value', 'Type', 'Details']
                                            for col in required_columns:
                                                column_mapping = {
                                                    'Year': 'Year',
                                                    'Quarter': 'Quarter',
                                                    'Risk_ID': 'RiskID',
                                                    'Value__(Rp)': 'RiskImpactValue',
                                                    'Impact_Scale': 'RiskImpactScale',
                                                    'Value_(No)': 'ProbabilityValue',
                                                    'Scale': 'ProbabilityScale',
                                                    'Risk_Exposure_Value': 'RiskExposureValue',
                                                    'Type': 'RiskScaleType',
                                                    'Details': 'RiskScaleDetails'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")
                                                table_name = "[dbo].[SubsidiaryResidualRisk]"
                                                logging.info(f"Table name set to: {table_name}")
                                                df['Company'] = df['Company'].str.strip().str.lower()
                                                unique_company_quarters = df[['Company', 'Year', 'Quarter']].drop_duplicates()
                                                for _, cq in unique_company_quarters.iterrows():
                                                    company = cq['Company']
                                                    year = cq['Year']
                                                    quarter = cq['Quarter']
                                                    existing_records_query = f"""
                                                                                SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                                                                SELECT [Company], [RiskID], [Year], [Quarter]
                                                                                FROM {table_name} WITH (NOLOCK)
                                                                                WHERE [Company]  = ? AND [Year] = ? AND [Quarter] = ?
                                                                                """
                                                    cursor.execute(existing_records_query, (company, year, quarter))
                                                    existing_records = set(tuple(row) for row in cursor.fetchall())
                                                    company_df = df[(df['Company'] == company) & (df['Year'] == year) & (
                                                            df['Quarter'] == quarter)]
                                                    all_current_records = set()
                                                    for _, row in company_df.iterrows():
                                                        all_current_records.add(
                                                            (row['Company'], row['RiskID'], row['Year'], row['Quarter']))
                                                    records_to_delete = existing_records - all_current_records
                                                    if records_to_delete:
                                                        logging.info(
                                                            f"Deleting {len(records_to_delete)} obsolete records for {company} in {year} Q{quarter}.")
                                                        delete_query = f"""
                                                                        DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                                        WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                        """
                                                        cursor.executemany(delete_query, list(records_to_delete))
                                                        conn.commit()
                                                        logging.info("Obsolete records deleted successfully.")
                                                        insert_query = f"""
                                                                            INSERT INTO {table_name} (
                                                                                [Company],
                                                                                [RiskID],
                                                                                [Year],
                                                                                [Quarter],
                                                                                [RiskImpactValue],
                                                                                [RiskImpactScale],
                                                                                [ProbabilityValue],
                                                                                [ProbabilityScale],
                                                                                [RiskExposureValue],
                                                                                [RiskScaleType],
                                                                                [RiskScaleDetails]
                                                                            )
                                                                            VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                                    END
                                                                                """
                                                        for _, row in df.iterrows():
                                                            placeholders = (
                                                                row['Company'],
                                                                row['RiskID'],
                                                                row['Year'], row['Quarter'],
                                                                row['RiskImpactValue'],
                                                                row['RiskImpactScale'],
                                                                row['ProbabilityValue'],
                                                                row['ProbabilityScale'],
                                                                row['RiskExposureValue'],
                                                                row['RiskScaleType'],
                                                                row['RiskScaleDetails']
                                                            )
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                            IF EXISTS (
                                                                                    SELECT 1
                                                                                    FROM {table_name}
                                                                                    WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                                )
                                                                                BEGIN
                                                                                    UPDATE {table_name}
                                                                                    SET
                                                                                        [RiskImpactValue] = ?,
                                                                                        [RiskImpactScale] = ?,
                                                                                        [ProbabilityValue] = ?,
                                                                                        [ProbabilityScale] = ?,
                                                                                        [RiskExposureValue] = ?,
                                                                                        [RiskScaleType] = ?,
                                                                                        [RiskScaleDetails] = ?
                                                                                    WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                                                                END
                                                                                ELSE
                                                                                BEGIN
                                                                                    INSERT INTO {table_name} (
                                                                                        [Company],
                                                                                        [RiskID],
                                                                                        [Year],
                                                                                        [Quarter],
                                                                                        [RiskImpactValue],
                                                                                        [RiskImpactScale],
                                                                                        [ProbabilityValue],
                                                                                        [ProbabilityScale],
                                                                                        [RiskExposureValue],
                                                                                        [RiskScaleType],
                                                                                        [RiskScaleDetails]
                                                                                    )
                                                                                    VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                                END
                                                                                """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],
                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails'],

                                                            row['Company'], row['RiskID'], row['Year'], row['Quarter'],

                                                            row['Company'],
                                                            row['RiskID'],
                                                            row['Year'], row['Quarter'],
                                                            row['RiskImpactValue'],
                                                            row['RiskImpactScale'],
                                                            row['ProbabilityValue'],
                                                            row['ProbabilityScale'],
                                                            row['RiskExposureValue'],
                                                            row['RiskScaleType'],
                                                            row['RiskScaleDetails']
                                                        ))
                                                conn.commit()
                                                logging.info(
                                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                                sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success", "Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                                    else:
                                        try:
                                            company_name = unquote(global_subfolder)
                                            required_columns = [
                                                'Account', 'Year', 'January', 'February', 'March', 'April', 'May', 'June', 'July',
                                                'August', 'September', 'October', 'November', 'December']
                                            for col in required_columns:

                                                column_mapping = {
                                                    'Account': 'Account',
                                                    'Year': 'Year',
                                                    'January': 'January',
                                                    'February': 'February',
                                                    'March': 'March',
                                                    'April': 'April',
                                                    'May': 'May',
                                                    'June': 'June',
                                                    'July': 'July',
                                                    'August': 'August',
                                                    'September': 'September',
                                                    'October': 'October',
                                                    'November': 'November',
                                                    'December': 'December'
                                                }
                                                df.rename(columns=column_mapping, inplace=True)
                                                if 'Created' in df.columns:
                                                    df.drop(columns=['Created'], inplace=True)
                                                    logging.info(f"'Created' column removed.")
                                                table_name = sheet_to_table_map[sheet_name]
                                                logging.info(f"Table name set to: {table_name}")
                                                existing_rows_query = f"""
                                                                        SELECT Account, Year, Company
                                                                        FROM {table_name} WHERE Company = ?
                                                                    """
                                                cursor.execute(existing_rows_query, (company_name,))
                                                rows = cursor.fetchall()
                                                existing_rows_set = {tuple(row) for row in rows}
                                                df_tuples = set(
                                                    zip(df['Account'], df['Year'], df['Company']))
                                                missing_rows = existing_rows_set - df_tuples
                                                if missing_rows:
                                                    logging.info("Missing rows detected. Performing TRUNCATE + INSERT.")
                                                    truncate_query = f"DELETE FROM {table_name} WHERE Account = ? AND Year = ? AND Company = ?"
                                                    for row in missing_rows:
                                                        cursor.execute(truncate_query, row)
                                                    insert_query = f"""
                                                                    INSERT INTO {table_name} (
                                                                    Account, Year, Company, January, February, March, April, May, 
                                                                    June, July, August, September, October, November, December
                                                                    )
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                    """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(insert_query, (
                                                            row['Account'], row['Year'], row['Company'],  # For INSERT
                                                            row['January'], row['February'], row['March'], row['April'], row['May'],
                                                            row['June'], row['July'], row['August'], row['September'],
                                                            row['October'], row['November'], row['December']
                                                        ))
                                                else:
                                                    logging.info("Rows exist. Performing UPDATE or INSERT.")
                                                    update_insert_query = f"""
                                                                            IF EXISTS (
                                                                                SELECT 1 
                                                                                FROM {table_name}
                                                                                WHERE Account = ? AND Year = ? AND Company = ?
                                                                            )
                                                                            BEGIN
                                                                                UPDATE {table_name}
                                                                                SET 
                                                                                    January = ?, February = ?, March = ?, April = ?, May = ?, June = ?, 
                                                                                    July = ?, August = ?, September = ?, October = ?, November = ?, December = ?
                                                                                WHERE Account = ? AND Year = ? AND Company = ?;
                                                                            END
                                                                            ELSE
                                                                            BEGIN
                                                                                INSERT INTO {table_name} (
                                                                                    Account, Year, Company, January, February, March, April, May, 
                                                                                    June, July, August, September, October, November, December
                                                                                )
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                                                            END
                                                                        """
                                                    for _, row in df.iterrows():
                                                        cursor.execute(update_insert_query, (
                                                            row['Account'], row['Year'], row['Company'],
                                                            row['January'], row['February'], row['March'], row['April'], row['May'],
                                                            row['June'], row['July'], row['August'], row['September'],
                                                            row['October'], row['November'], row['December'],
                                                            row['Account'], row['Year'], row['Company'],
                                                            row['Account'], row['Year'], row['Company'],
                                                            row['January'], row['February'], row['March'], row['April'], row['May'],
                                                            row['June'], row['July'], row['August'], row['September'],
                                                            row['October'], row['November'], row['December']
                                                        ))
                                                conn.commit()
                                                logging.info(
                                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                                                sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Success","Description": "Sheet processed successfully"}
                                        except Exception as e:
                                            logging.error(f"Error occurred while processing '{sheet_name}': {str(e)}")
                                            sheet_status[(inferred_dashboard, sheet_name)] = {"Status": "Fail", "Description": str(e)}

                        for (dashboard, sheet), status in sheet_status.items():
                            insert_log_query = """
                                            INSERT INTO [dbo].[SubsidiaryFinanceRiskDataLog] ([CompanyName], [ModifiedDate], [Dashboard], [SheetName], [Status], [Description])
                                            VALUES (?, GETDATE(), ?, ?, ?, ?)
                                            """
                            cursor.execute(insert_log_query,
                                        (subfolder, dashboard, sheet, status["Status"],
                                            status["Description"]))
                            conn.commit()

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

                    if "Preface" in config_sheet_names:
                        config_sheet_names.remove("Preface")
                        logging.info("Ignored 'Preface' sheet.")

                    for sheet_name in sheet_to_table_map_config.keys():
                        if sheet_name not in config_sheet_names:
                            logging.warning(
                                f"Sheet '{sheet_name}' not found in 'Dashboard Configuration Master List' workbook. Skipping.")
                            continue
                        logging.info(f"Processing sheet: {sheet_name}")
                        df = pd.read_excel(config_local_copy_sas_url, sheet_name=sheet_name, skiprows=4, header=0)
                        for col in df.columns:
                            if df[col].dtype == 'object':
                                df[col] = df[col].str.strip()
                        df.rename(columns={'Subsidiary Name': 'SubsidiaryName'}, inplace=True)
                        df['Source'] = sheet_name

                        if 'Source' in df.columns:
                            df = df.drop(columns=['Source'])
                        df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^a-zA-Z0-9_]", "")

                        df.fillna(0, inplace=True)

                        if 'Created' in df.columns:
                            df.drop(columns=['Created'], inplace=True)

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
                            for _, row in df.iterrows():
                                placeholders = (
                                    str(row['SubsidiaryName']), str(row['InvestmentAccountName']),
                                    str(row['Abbreviation']), str(row['IncomeAccountName']),
                                    str(row['SubsidiaryName']), str(row['InvestmentAccountName']),
                                    str(row['SubsidiaryName']), str(row['Abbreviation']), str(row['InvestmentAccountName']),
                                    str(row['IncomeAccountName'])
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
                            for _, row in df.iterrows():
                                placeholders = (row['InvestmentAccount'], row['InvestmentAccount'])
                                try:
                                    cursor.execute(insert_query, placeholders)
                                except Exception as e:
                                    logging.error(f"Failed to execute query for row {row}: {e}")

                        conn.commit()
                        logging.info(f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully.")
                except Exception as e:
                    logging.error(f"Error processing 'Dashboard Configuration Master List' workbook: {e}")
        else:
            logging.error("Authentication failed. Please check your credentials.")
        
        conn.close()
    except Exception as e:
        logging.error(f"Error during SharePoint authentication: {e}")