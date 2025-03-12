
from Common_powerBI import *

def SubsidiaryFinanceFunctionIndex():
    if "Subsidiary" in main_folder_list:
        logging.info("Processing 'Subsidiary Finance Risk' folder.")
        xlsx_files = process_subfolders(
            ctx, parent_path="/sites/Dashboard-UAT/Shared%20Documents/Subsidiary"
        )
        sheet_to_table_map = sheet_to_table_map_subsidiary
        parent_path = "/sites/Dashboard-UAT/Shared%20Documents/Subsidiary"
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
                dashboard_files = [
                    file for file in xlsx_files if file.startswith(dashboard_path)
                ]
                dashboard_file_map[dashboard] = dashboard_files

                for file in xlsx_files:
                    print(f'file is {file}')
                    file_content = File.open_binary(ctx, file)
                    logging.info(f"file content is {file_content}")

                    # Load the workbook to inspect sheet names
                    uploadToBlobStorage(file_content, "local_copy.xlsx")
                    logging.info("upload to blob successfully")
                    xls = pd.ExcelFile(local_copy_sas_url)
                    sheet_names = xls.sheet_names
                    if "Preface" in sheet_names:
                        sheet_names.remove("Preface")

                    for sheet_name in sheet_names:
                        if sheet_name in [
                            "Financial Performance",
                            "Project Timeline",
                            "Construction Timeline",
                            "Electricity Generation (monthly",
                            "Outages & Availability (Monthly",
                            "Project Detail",
                            "Electricity Generation (Daily)",
                            "Coal Stockpile (Daily)",
                            "Project Expenses",
                            "Electricity Generation (Annualy",
                            "Env - Scope 1 & 2 Emissions",
                            "Env - Utilities",
                            "Social - Employee by Gender",
                            "Social - Employee by Age",
                            "Social - CSR",
                            "Gov - Management Diversity",
                            "Gov - Board",
                            "Targets",
                            "Risk Details",
                            "KRI Details",
                            "Inherent Risk",
                            "Residual Risk"
                        ]:
                            continue
                        elif sheet_name in [
                            "Electricity Generation (Daily)",
                            "Coal Stockpile (Daily)",
                            "Project Expenses",
                        ]:
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
                        df = pd.read_excel(
                             local_copy_sas_url,
                            sheet_name=sheet_name,
                            skiprows=skiprows,
                            header=header,
                        )

                        for col in df.columns:
                            if df[col].dtype == "object":
                                df[col] = df[col].str.strip()
                        df["Company"] = unquote(global_subfolder)
                        df["Dashboard"] = inferred_dashboard
                        df.columns = (
                            df.columns.str.strip()
                            .str.replace(" ", "_")
                            .str.replace(r"[^a-zA-Z0-9_]", "")
                        )
                        df.fillna(0, inplace=True)
                        if sheet_name == "Debt Management":
                            try:
                                company_name = df["Company"].iloc[0]

                                required_columns = [
                                    "Account",
                                    "Year",
                                    "Loan",
                                    "Type",
                                    "Start Date",
                                    "Due Date",
                                    "Original Principal Value (USD)",
                                    "PaymentCategory",
                                    "NextPayment Date",
                                    "January",
                                    "February",
                                    "March",
                                    "April",
                                    "May",
                                    "June",
                                    "July",
                                    "August",
                                    "September",
                                    "October",
                                    "November",
                                    "December",
                                ]
                                for col in required_columns:
                                    column_mapping = {
                                        "Account": "Account",
                                        "Year": "Year",
                                        "Loan": "Loan",
                                        "Type": "Type",
                                        "Start Date": "Start_Date",
                                        "Due Date": "Due_Date",
                                        "Original Principal Value (USD)": "Original_Principal_Value_(USD)",
                                        "PaymentCategory": "Payment_Category",
                                        "Next Payment Date": "Next_Payment_Date",
                                        "January": "January",
                                        "February": "February",
                                        "March": "March",
                                        "April": "April",
                                        "May": "May",
                                        "June": "June",
                                        "July": "July",
                                        "August": "August",
                                        "September": "September",
                                        "October": "October",
                                        "November": "November",
                                        "December": "December",
                                    }
                                    df.rename(columns=column_mapping, inplace=True)
                                    if "Created" in df.columns:
                                        df.drop(columns=["Created"], inplace=True)

                                    table_name = "dbo.SubsidiaryDebtManagement"
                                    existing_rows_query = f"""
                                                          SELECT Account, Year, Company, Payment_Category 
                                                           FROM {table_name}
                                                         """
                                    cursor.execute(existing_rows_query)
                                    rows = cursor.fetchall()
                                    existing_rows_set = {tuple(row) for row in rows}
                                    df_tuples = set(
                                        zip(
                                            df["Account"],
                                            df["Year"],
                                            df["Company"],
                                            df["Payment_Category"],
                                        )
                                    )
                                    missing_rows = df_tuples - existing_rows_set
                                    if missing_rows:
                                        truncate_query = (
                                            f"DELETE FROM {table_name} WHERE Company = ?;"
                                        )
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
                                            cursor.execute(
                                                insert_query,
                                                (
                                                    row["Account"],
                                                    row["Year"],
                                                    row["Company"],
                                                    row["January"],
                                                    row["February"],
                                                    row["March"],
                                                    row["April"],
                                                    row["May"],
                                                    row["June"],
                                                    row["July"],
                                                    row["August"],
                                                    row["September"],
                                                    row["October"],
                                                    row["November"],
                                                    row["December"],
                                                    row["Loan"],
                                                    row["Type"],
                                                    row["Start_Date"],
                                                    row["Due_Date"],
                                                    row["Original_Principal_Value_(USD)"],
                                                    row["Payment_Category"],
                                                    row["Next_Payment_Date"],
                                                ),
                                            )
                                    else:
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
                                            cursor.execute(
                                                update_insert_query,
                                                (
                                                    row["Account"],
                                                    row["Year"],
                                                    row["Company"],
                                                    row["Payment_Category"],
                                                    row["Loan"],
                                                    row["Type"],
                                                    row["Start_Date"],
                                                    row["Due_Date"],
                                                    row["Original_Principal_Value_(USD)"],
                                                    row["Next_Payment_Date"],
                                                    row["January"],
                                                    row["February"],
                                                    row["March"],
                                                    row["April"],
                                                    row["May"],
                                                    row["June"],
                                                    row["July"],
                                                    row["August"],
                                                    row["September"],
                                                    row["October"],
                                                    row["November"],
                                                    row["December"],
                                                    row["Account"],
                                                    row["Year"],
                                                    row["Company"],
                                                    row["Payment_Category"],
                                                    row["Account"],
                                                    row["Year"],
                                                    row["Company"],
                                                    row["January"],
                                                    row["February"],
                                                    row["March"],
                                                    row["April"],
                                                    row["May"],
                                                    row["June"],
                                                    row["July"],
                                                    row["August"],
                                                    row["September"],
                                                    row["October"],
                                                    row["November"],
                                                    row["December"],
                                                    row["Loan"],
                                                    row["Type"],
                                                    row["Start_Date"],
                                                    row["Due_Date"],
                                                    row["Original_Principal_Value_(USD)"],
                                                    row["Payment_Category"],
                                                    row["Next_Payment_Date"],
                                                ),
                                            )
                                conn.commit()
                                sheet_status[(inferred_dashboard, sheet_name)] = {
                                    "Status": "Success",
                                    "Description": "Sheet processed successfully",
                                }
                            except Exception as e:
                                logging.error(
                                    f"Error occurred while processing '{sheet_name}': {str(e)}"
                                )
                                sheet_status[(inferred_dashboard, sheet_name)] = {
                                    "Status": "Fail",
                                    "Description": str(e),
                                }
                        else:
                            try:
                                company_name = unquote(global_subfolder)
                                required_columns = [
                                    "Account",
                                    "Year",
                                    "January",
                                    "February",
                                    "March",
                                    "April",
                                    "May",
                                    "June",
                                    "July",
                                    "August",
                                    "September",
                                    "October",
                                    "November",
                                    "December",
                                ]
                                for col in required_columns:

                                    column_mapping = {
                                        "Account": "Account",
                                        "Year": "Year",
                                        "January": "January",
                                        "February": "February",
                                        "March": "March",
                                        "April": "April",
                                        "May": "May",
                                        "June": "June",
                                        "July": "July",
                                        "August": "August",
                                        "September": "September",
                                        "October": "October",
                                        "November": "November",
                                        "December": "December",
                                    }
                                    df.rename(columns=column_mapping, inplace=True)
                                    if "Created" in df.columns:
                                        df.drop(columns=["Created"], inplace=True)
                                    table_name = sheet_to_table_map[sheet_name]
                                    existing_rows_query = f"""
                                          SELECT Account, Year, Company
                                          FROM {table_name} WHERE Company = ?
                                      """
                                    cursor.execute(existing_rows_query, (company_name,))
                                    rows = cursor.fetchall()
                                    existing_rows_set = {tuple(row) for row in rows}
                                    df_tuples = set(
                                        zip(df["Account"], df["Year"], df["Company"])
                                    )
                                    missing_rows = existing_rows_set - df_tuples
                                    if missing_rows:
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
                                            cursor.execute(
                                                insert_query,
                                                (
                                                    row["Account"],
                                                    row["Year"],
                                                    row["Company"],
                                                    row["January"],
                                                    row["February"],
                                                    row["March"],
                                                    row["April"],
                                                    row["May"],
                                                    row["June"],
                                                    row["July"],
                                                    row["August"],
                                                    row["September"],
                                                    row["October"],
                                                    row["November"],
                                                    row["December"],
                                                ),
                                            )
                                    else:
                                        logging.info(
                                            "Rows exist. Performing UPDATE or INSERT."
                                        )
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
                                            cursor.execute(
                                                update_insert_query,
                                                (
                                                    row["Account"],
                                                    row["Year"],
                                                    row["Company"],
                                                    row["January"],
                                                    row["February"],
                                                    row["March"],
                                                    row["April"],
                                                    row["May"],
                                                    row["June"],
                                                    row["July"],
                                                    row["August"],
                                                    row["September"],
                                                    row["October"],
                                                    row["November"],
                                                    row["December"],
                                                    row["Account"],
                                                    row["Year"],
                                                    row["Company"],
                                                    row["Account"],
                                                    row["Year"],
                                                    row["Company"],
                                                    row["January"],
                                                    row["February"],
                                                    row["March"],
                                                    row["April"],
                                                    row["May"],
                                                    row["June"],
                                                    row["July"],
                                                    row["August"],
                                                    row["September"],
                                                    row["October"],
                                                    row["November"],
                                                    row["December"],
                                                ),
                                            )
                                    conn.commit()
                                    sheet_status[(inferred_dashboard, sheet_name)] = {
                                        "Status": "Success",
                                        "Description": "Sheet processed successfully",
                                    }
                            except Exception as e:
                                logging.error(
                                    f"Error occurred while processing '{sheet_name}': {str(e)}"
                                )
                                sheet_status[(inferred_dashboard, sheet_name)] = {
                                    "Status": "Fail",
                                    "Description": str(e),
                                }

            for (dashboard, sheet), status in sheet_status.items():
                insert_log_query = """
                      INSERT INTO [dbo].[SubsidiaryFinanceRiskDataLog] ([CompanyName], [ModifiedDate], [Dashboard], [SheetName], [Status], [Description])
                      VALUES (?, GETDATE(), ?, ?, ?, ?)
                      """
                cursor.execute(
                    insert_log_query,
                    (subfolder, dashboard, sheet, status["Status"], status["Description"]),
                )
                conn.commit()