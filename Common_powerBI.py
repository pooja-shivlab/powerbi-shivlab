import os
import re
import logging
import pandas as pd
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import pyodbc
from urllib.parse import unquote
from dotenv import load_dotenv
import hashlib
import urllib.parse
from datetime import datetime
import warnings


logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")

logging.debug("This is a test debug log.")

warnings.simplefilter(action="ignore", category=FutureWarning)

load_dotenv()

sql_server_host = os.getenv("SQL_SERVER_HOST")
sql_server_port = os.getenv("SQL_SERVER_PORT")
sql_server_user = os.getenv("SQL_SERVER_USER")
sql_server_password = os.getenv("SQL_SERVER_PASSWORD")
sql_database = os.getenv("SQL_DATABASE")

site_url = os.getenv("SHAREPOINT_URL")
username = os.getenv("SHAREPOINT_USERNAME")
password = os.getenv("SHAREPOINT_PASSWORD")

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
    "Residual Risk": "dbo.ResidualRisk",
    # --------------------operational-----------
    "Operation Overview": "dbo.OperationOverview",
    "Env - Scope 1 & 2 Emissions": "dbo.Env-Scope1&2Emissions",
    "Env - Scope 2 Electricity": "dbo.Env-Scope2Electricity",
    "Env - Utilities": "dbo.Env-Utilities",
    "Social - Employee by Gender": "dbo.Social-EmployeeByGender",
    "Social - Employee by Age": "dbo.Social-EmployeeByAge",
    "Social - CSR": "dbo.Social-CSR",
    "Gov - Management Diversity": "dbo.Gov-ManagementDiversity",
    "Gov - Board": "dbo.Gov-Board",
    "Targets": "dbo.Targets",
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
    "Residual Risk": "dbo.SubsidiaryResidualRisk",
    # -------------operational-------------------
    "Financial Performance": "dbo.OP_FinancialPerformance",
    "Project Timeline": "dbo.OP_ProjectTimeline",
    "Construction Timeline": "dbo.OP_ConstructionTimeline",
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
    "Targets": "dbo.SubsidiaryTargets",
}

sheet_to_table_map_config = {
    "Subsidiary List": "dbo.SubsidiaryList",
    "Investment List": "dbo.InvestmentAccountList",
}


def escape_special_characters(text):
    # Ensure text is a string before applying the regex
    if not isinstance(text, str):
        text = str(text)  # Convert non-string values to strings
    return re.sub(r"([\\\'\"%_])", r"\\\1", text)


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

    folder_collection = ctx.web.get_folder_by_server_relative_url(parent_path).folders
    ctx.load(folder_collection)
    ctx.execute_query()

    file_collection = ctx.web.get_folder_by_server_relative_url(parent_path).files
    ctx.load(file_collection)
    ctx.execute_query()

    for file in file_collection:
        file_name = file.properties["Name"]
        if file_name.endswith(".xlsx") and file_name not in []:
            xlsx_files.append(file.properties["ServerRelativeUrl"])

    for folder in folder_collection:
        folder_name = folder.properties["Name"]
        subfolder_path = f"{parent_path}/{folder_name}"

        xlsx_files += process_subfolders(ctx, subfolder_path)

    return xlsx_files


def get_subfolders(ctx, parent_path):
    """
    Retrieves the subfolders under a given folder path.
    """
    try:
        folder = ctx.web.get_folder_by_server_relative_url(parent_path)
        folder.expand(["Folders"]).get().execute_query()
        subfolders = [subfolder.name for subfolder in folder.folders]
        logging.info(f"Subfolders in '{parent_path}': {subfolders}")
        return subfolders
    except Exception as e:
        logging.error(f"Error retrieving subfolders from '{parent_path}': {e}")
        return []


connection_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={sql_server_host},{sql_server_port};"
    f"Database={sql_database};"
    f"UID={sql_server_user};"
    f"PWD={sql_server_password};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=yes;"
)
global_subfolder = ""
try:
    auth_context = AuthenticationContext(site_url)
    if auth_context.acquire_token_for_user(username, password):
        logging.info("Authentication successful.")
        ctx = ClientContext(site_url, auth_context)

        main_and_subfolders = get_nested_folders(ctx)
        all_xlsx_files = []
        conn = pyodbc.connect(connection_string)
        logging.info("Connected to SQL Server successfully.")
        cursor = conn.cursor()
        main_folder_list = []
        for main_folder in main_and_subfolders:
            logging.info(f"Checking folder: {main_folder}")
            main_folder_list.append(main_folder)

except Exception as e:
    logging.error(f"Error during SharePoint authentication: {e}")
