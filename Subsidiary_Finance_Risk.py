from Common_powerBI import *


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
                target_file = ctx.web.get_file_by_server_relative_url(file)
                with open("local_copy.xlsx", "wb") as local_file:
                    target_file.download(local_file).execute_query()
                xls = pd.ExcelFile("local_copy.xlsx")
                sheet_names = xls.sheet_names
                if "Preface" in sheet_names:
                    sheet_names.remove("Preface")

                for sheet_name in sheet_names:
                    if sheet_name in [
                        "Risk Details",
                        "KRI Details",
                        "Inherent Risk",
                        "Residual Risk",
                    ]:
                        skiprows = 5
                        header = 0
                    elif sheet_name in [
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
                        "Operation Overview",
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
                        "local_copy.xlsx",
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

                    elif sheet_name == "Risk Details":
                        try:
                            logging.info(
                                "Special processing for 'Subsidiary Risk Details'"
                            )
                            df = df.drop(columns=["Unnamed:_0"])
                            df.columns = df.columns.str.strip()
                            df.rename(
                                columns={"Unnamed:_1": "Year", "Unnamed:_2": "Quarter"},
                                inplace=True,
                            )
                            if "Year" in df.columns and "Quarter" in df.columns:
                                year = str(df["Year"].iloc[0])
                                quarter = str(df["Quarter"].iloc[0])
                            else:
                                logging.warning(
                                    "Year or Quarter column not found in Risk Details"
                                )
                            required_columns = [
                                "Year",
                                "Quarter",
                                "Risk",
                                "Risk_ID",
                                "Risk_Type",
                                "Inherent_Risk",
                                "Target_Risk",
                                "Residual_Risk",
                                "Risk_Desc",
                                "Risk_Cause",
                                "Indicator",
                                "Unit",
                                "Safe",
                                "Caution",
                                "Danger",
                                "Type",
                                "Details",
                                "Effectivity",
                                "Category",
                                "Descripsion",
                                "Plan",
                                "Outcome",
                                "Cost",
                                "RKAP_Program",
                                "Risk_Owner",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Year": "Year",
                                    "Quarter": "Quarter",
                                    "Risk": "Risk",
                                    "Risk_ID": "RiskID",
                                    "Risk_Type": "RiskType",
                                    "Inherent_Risk": "InherentRisk",
                                    "Target_Risk": "TargetRisk",
                                    "Residual_Risk": "ResidualRisk",
                                    "Risk_Desc": "RiskDesc",
                                    "Risk_Cause": "RiskCause",
                                    "Indicator": "KRIIndicator",
                                    "Unit": "KRIUnit",
                                    "Safe": "KRIThresholdSafe",
                                    "Caution": "KRIThresholdCaution",
                                    "Danger": "KRIThresholdDanger",
                                    "Type": "ExistingControlType",
                                    "Details": "ExistingControlDetails",
                                    "Effectivity": "ExistingControlEffectivity",
                                    "Category": "RiskImpactCategory",
                                    "Descripsion": "RiskImpactDescripsion",
                                    "Plan": "PreventionPlan",
                                    "Outcome": "PreventionOutcome",
                                    "Cost": "PreventionCost",
                                    "RKAP_Program": "PreventionRKAPProgram",
                                    "Risk_Owner": "RiskOwner",
                                }
                                df.rename(columns=column_mapping, inplace=True)
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                table_name = "[dbo].[SubsidiaryRiskDetails]"
                                df["Year"] = df["Year"].astype(str)
                                df["Company"] = df["Company"].str.strip().str.lower()
                                unique_company_quarters = df[
                                    ["Company", "Year", "Quarter"]
                                ].drop_duplicates()
                                for _, cq in unique_company_quarters.iterrows():
                                    company = cq["Company"]
                                    year = cq["Year"]
                                    quarter = cq["Quarter"]
                                    existing_records_query = f"""
                                       SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                       SELECT [Company], [RiskID], [Year], [Quarter]
                                       FROM {table_name} WITH (NOLOCK)
                                       WHERE [Company]  = ? AND [Year] = ? AND [Quarter] = ?
                                       """
                                    cursor.execute(
                                        existing_records_query, (company, year, quarter)
                                    )
                                    existing_records = set(
                                        tuple(row) for row in cursor.fetchall()
                                    )
                                    company_df = df[
                                        (df["Company"] == company)
                                        & (df["Year"] == year)
                                        & (df["Quarter"] == quarter)
                                    ]
                                    all_current_records = set()
                                    for _, row in company_df.iterrows():
                                        all_current_records.add(
                                            (
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                            )
                                        )
                                    records_to_delete = (
                                        existing_records - all_current_records
                                    )
                                    if records_to_delete:
                                        delete_query = f"""
                                           DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                           WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                        """
                                        cursor.executemany(
                                            delete_query, list(records_to_delete)
                                        )
                                        conn.commit()
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
                                            cursor.execute(
                                                insert_query,
                                                (
                                                    row["Company"],
                                                    row["RiskID"],
                                                    row["Year"],
                                                    row["Quarter"],
                                                    row["Risk"],
                                                    row["RiskType"],
                                                    row["InherentRisk"],
                                                    row["TargetRisk"],
                                                    row["ResidualRisk"],
                                                    row["RiskDesc"],
                                                    row["RiskCause"],
                                                    row["KRIIndicator"],
                                                    row["KRIUnit"],
                                                    row["KRIThresholdSafe"],
                                                    row["KRIThresholdCaution"],
                                                    row["KRIThresholdDanger"],
                                                    row["ExistingControlType"],
                                                    row["ExistingControlDetails"],
                                                    row["ExistingControlEffectivity"],
                                                    row["RiskImpactCategory"],
                                                    row["RiskImpactDescripsion"],
                                                    row["PreventionPlan"],
                                                    row["PreventionOutcome"],
                                                    row["PreventionCost"],
                                                    row["PreventionRKAPProgram"],
                                                    row["RiskOwner"],
                                                ),
                                            )
                                        conn.commit()
                                        logging.info(
                                            "Obsolete records deleted successfully."
                                        )
                                else:
                                    logging.info(
                                        "Rows exist. Performing UPDATE or INSERT."
                                    )
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
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["Risk"],
                                                row["RiskType"],
                                                row["InherentRisk"],
                                                row["TargetRisk"],
                                                row["ResidualRisk"],
                                                row["RiskDesc"],
                                                row["RiskCause"],
                                                row["KRIIndicator"],
                                                row["KRIUnit"],
                                                row["KRIThresholdSafe"],
                                                row["KRIThresholdCaution"],
                                                row["KRIThresholdDanger"],
                                                row["ExistingControlType"],
                                                row["ExistingControlDetails"],
                                                row["ExistingControlEffectivity"],
                                                row["RiskImpactCategory"],
                                                row["RiskImpactDescripsion"],
                                                row["PreventionPlan"],
                                                row["PreventionOutcome"],
                                                row["PreventionCost"],
                                                row["PreventionRKAPProgram"],
                                                row["RiskOwner"],
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["Risk"],
                                                row["RiskType"],
                                                row["InherentRisk"],
                                                row["TargetRisk"],
                                                row["ResidualRisk"],
                                                row["RiskDesc"],
                                                row["RiskCause"],
                                                row["KRIIndicator"],
                                                row["KRIUnit"],
                                                row["KRIThresholdSafe"],
                                                row["KRIThresholdCaution"],
                                                row["KRIThresholdDanger"],
                                                row["ExistingControlType"],
                                                row["ExistingControlDetails"],
                                                row["ExistingControlEffectivity"],
                                                row["RiskImpactCategory"],
                                                row["RiskImpactDescripsion"],
                                                row["PreventionPlan"],
                                                row["PreventionOutcome"],
                                                row["PreventionCost"],
                                                row["PreventionRKAPProgram"],
                                                row["RiskOwner"],
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

                    elif sheet_name == "KRI Details":
                        try:
                            logging.info(
                                "Special processing for 'Subsidiary KRI Details'"
                            )
                            df = df.drop(columns=["Unnamed:_0"])
                            df.columns = df.columns.str.strip()
                            if year and quarter:
                                df["Year"], df["Quarter"] = year, quarter
                            else:
                                logging.warning(
                                    f"Year and Quarter not found for {sheet_name}"
                                )
                            required_columns = [
                                "Year",
                                "Quarter",
                                "Risk",
                                "RiskID",
                                "RiskType",
                                "Inherent_Risk",
                                "Residual_Risk",
                                "Current_KRI",
                                "Status_of_KRI",
                                "Indicator",
                                "Unit",
                                "Safe",
                                "Caution",
                                "Danger",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Year": "Year",
                                    "Quarter": "Quarter",
                                    "Risk": "Risk",
                                    "Risk_ID": "RiskID",
                                    "Risk_Type": "RiskType",
                                    "Inherent_Risk": "InherentRisk",
                                    "Residual_Risk": "ResidualRisk",
                                    "Current_KRI": "CurrentKRI",
                                    "Status_of_KRI": "StatusofKRI",
                                    "Indicator": "KRIIndicator",
                                    "Unit": "KRIUnit",
                                    "Safe": "KRIThresholdSafe",
                                    "Caution": "KRIThresholdCaution",
                                    "Danger": "KRIThresholdDanger",
                                }
                                df.rename(columns=column_mapping, inplace=True)
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                table_name = "[dbo].[SubsidiaryKRIDetails]"
                                df["Company"] = df["Company"].str.strip().str.lower()
                                unique_company_quarters = df[
                                    ["Company", "Year", "Quarter"]
                                ].drop_duplicates()
                                for _, cq in unique_company_quarters.iterrows():
                                    company = cq["Company"]
                                    year = cq["Year"]
                                    quarter = cq["Quarter"]
                                    existing_records_query = f"""
                                       SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                       SELECT [Company], [RiskID], [Year], [Quarter]
                                       FROM {table_name} WITH (NOLOCK)
                                       WHERE [Company]  = ? AND [Year] = ? AND [Quarter] = ?
                                      """
                                    cursor.execute(
                                        existing_records_query, (company, year, quarter)
                                    )
                                    existing_records = set(
                                        tuple(row) for row in cursor.fetchall()
                                    )
                                    company_df = df[
                                        (df["Company"] == company)
                                        & (df["Year"] == year)
                                        & (df["Quarter"] == quarter)
                                    ]
                                    all_current_records = set()
                                    for _, row in company_df.iterrows():
                                        all_current_records.add(
                                            (
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                            )
                                        )
                                    records_to_delete = (
                                        existing_records - all_current_records
                                    )
                                    if records_to_delete:
                                        delete_query = f"""
                                           DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                           WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                           """
                                        cursor.executemany(
                                            delete_query, list(records_to_delete)
                                        )
                                        conn.commit()
                                        logging.info(
                                            "Obsolete records deleted successfully."
                                        )
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
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["Risk"],
                                                row["RiskType"],
                                                row["InherentRisk"],
                                                row["ResidualRisk"],
                                                row["CurrentKRI"],
                                                row["StatusofKRI"],
                                                row["KRIIndicator"],
                                                row["KRIUnit"],
                                                row["KRIThresholdSafe"],
                                                row["KRIThresholdCaution"],
                                                row["KRIThresholdDanger"],
                                            )
                                else:
                                    logging.info(
                                        "Rows exist. Performing UPDATE or INSERT."
                                    )
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
                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["Risk"],
                                                row["RiskType"],
                                                row["InherentRisk"],
                                                row["ResidualRisk"],
                                                row["CurrentKRI"],
                                                row["StatusofKRI"],
                                                row["KRIIndicator"],
                                                row["KRIUnit"],
                                                row["KRIThresholdSafe"],
                                                row["KRIThresholdCaution"],
                                                row["KRIThresholdDanger"],
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["Risk"],
                                                row["RiskType"],
                                                row["InherentRisk"],
                                                row["ResidualRisk"],
                                                row["CurrentKRI"],
                                                row["StatusofKRI"],
                                                row["KRIIndicator"],
                                                row["KRIUnit"],
                                                row["KRIThresholdSafe"],
                                                row["KRIThresholdCaution"],
                                                row["KRIThresholdDanger"],
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

                    elif sheet_name == "Inherent Risk":
                        try:
                            logging.info(
                                "Special processing for 'Subsidiary Inherent Risk'"
                            )
                            df = df.drop(columns=["Unnamed:_0"])
                            df.columns = df.columns.str.strip()
                            if year and quarter:
                                df["Year"], df["Quarter"] = year, quarter
                            else:
                                logging.warning(
                                    f"Year and Quarter not found for {sheet_name}"
                                )
                            required_columns = [
                                "Year",
                                "Quarter",
                                "Risk_ID",
                                "Value__(Rp)",
                                "Impact_Scale",
                                "Value_(No)",
                                "Scale",
                                "Risk_Exposure_Value",
                                "Type",
                                "Details",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Year": "Year",
                                    "Quarter": "Quarter",
                                    "Risk_ID": "RiskID",
                                    "Value__(Rp)": "RiskImpactValue",
                                    "Impact_Scale": "RiskImpactScale",
                                    "Value_(No)": "ProbabilityValue",
                                    "Scale": "ProbabilityScale",
                                    "Risk_Exposure_Value": "RiskExposureValue",
                                    "Type": "RiskScaleType",
                                    "Details": "RiskScaleDetails",
                                }
                                df.rename(columns=column_mapping, inplace=True)
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                table_name = "[dbo].[SubsidiaryInherentRisk]"
                                df["Company"] = df["Company"].str.strip().str.lower()
                                unique_company_quarters = df[
                                    ["Company", "Year", "Quarter"]
                                ].drop_duplicates()
                                for _, cq in unique_company_quarters.iterrows():
                                    company = cq["Company"]
                                    year = cq["Year"]
                                    quarter = cq["Quarter"]
                                    existing_records_query = f"""
                                       SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                       SELECT [Company], [RiskID], [Year], [Quarter]
                                       FROM {table_name} WITH (NOLOCK)
                                       WHERE [Company]  = ? AND [Year] = ? AND [Quarter] = ?
                                     """
                                    cursor.execute(
                                        existing_records_query, (company, year, quarter)
                                    )
                                    existing_records = set(
                                        tuple(row) for row in cursor.fetchall()
                                    )
                                    company_df = df[
                                        (df["Company"] == company)
                                        & (df["Year"] == year)
                                        & (df["Quarter"] == quarter)
                                    ]
                                    all_current_records = set()
                                    for _, row in company_df.iterrows():
                                        all_current_records.add(
                                            (
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                            )
                                        )
                                    records_to_delete = (
                                        existing_records - all_current_records
                                    )
                                    if records_to_delete:
                                        delete_query = f"""
                                           DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                           WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                           """
                                        cursor.executemany(
                                            delete_query, list(records_to_delete)
                                        )
                                        conn.commit()
                                        logging.info(
                                            "Obsolete records deleted successfully."
                                        )
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
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["RiskImpactValue"],
                                                row["RiskImpactScale"],
                                                row["ProbabilityValue"],
                                                row["ProbabilityScale"],
                                                row["RiskExposureValue"],
                                                row["RiskScaleType"],
                                                row["RiskScaleDetails"],
                                            )
                                else:
                                    logging.info(
                                        "Rows exist. Performing UPDATE or INSERT."
                                    )
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
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["RiskImpactValue"],
                                                row["RiskImpactScale"],
                                                row["ProbabilityValue"],
                                                row["ProbabilityScale"],
                                                row["RiskExposureValue"],
                                                row["RiskScaleType"],
                                                row["RiskScaleDetails"],
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["RiskImpactValue"],
                                                row["RiskImpactScale"],
                                                row["ProbabilityValue"],
                                                row["ProbabilityScale"],
                                                row["RiskExposureValue"],
                                                row["RiskScaleType"],
                                                row["RiskScaleDetails"],
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

                    elif sheet_name == "Residual Risk":
                        try:
                            logging.info(
                                "Special processing for Subsidiary Residual Risk'"
                            )
                            df = df.drop(columns=["Unnamed:_0"])
                            df.columns = df.columns.str.strip()
                            if year and quarter:
                                df["Year"], df["Quarter"] = year, quarter
                            else:
                                logging.warning(
                                    f"Year and Quarter not found for {sheet_name}"
                                )
                            required_columns = [
                                "Year",
                                "Quarter",
                                "Risk_ID",
                                "Value__(Rp)",
                                "Impact_Scale",
                                "Value_(No)",
                                "Scale",
                                "Risk_Exposure_Value",
                                "Type",
                                "Details",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Year": "Year",
                                    "Quarter": "Quarter",
                                    "Risk_ID": "RiskID",
                                    "Value__(Rp)": "RiskImpactValue",
                                    "Impact_Scale": "RiskImpactScale",
                                    "Value_(No)": "ProbabilityValue",
                                    "Scale": "ProbabilityScale",
                                    "Risk_Exposure_Value": "RiskExposureValue",
                                    "Type": "RiskScaleType",
                                    "Details": "RiskScaleDetails",
                                }
                                df.rename(columns=column_mapping, inplace=True)
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                table_name = "[dbo].[SubsidiaryResidualRisk]"
                                df["Company"] = df["Company"].str.strip().str.lower()
                                unique_company_quarters = df[
                                    ["Company", "Year", "Quarter"]
                                ].drop_duplicates()
                                for _, cq in unique_company_quarters.iterrows():
                                    company = cq["Company"]
                                    year = cq["Year"]
                                    quarter = cq["Quarter"]
                                    existing_records_query = f"""
                                       SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                                       SELECT [Company], [RiskID], [Year], [Quarter]
                                       FROM {table_name} WITH (NOLOCK)
                                       WHERE [Company]  = ? AND [Year] = ? AND [Quarter] = ?
                                       """
                                    cursor.execute(
                                        existing_records_query, (company, year, quarter)
                                    )
                                    existing_records = set(
                                        tuple(row) for row in cursor.fetchall()
                                    )
                                    company_df = df[
                                        (df["Company"] == company)
                                        & (df["Year"] == year)
                                        & (df["Quarter"] == quarter)
                                    ]
                                    all_current_records = set()
                                    for _, row in company_df.iterrows():
                                        all_current_records.add(
                                            (
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                            )
                                        )
                                    records_to_delete = (
                                        existing_records - all_current_records
                                    )
                                    if records_to_delete:
                                        delete_query = f"""
                                           DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                           WHERE [Company] = ? AND [RiskID] = ? AND [Year] = ? AND [Quarter] = ?
                                           """
                                        cursor.executemany(
                                            delete_query, list(records_to_delete)
                                        )
                                        conn.commit()
                                        logging.info(
                                            "Obsolete records deleted successfully."
                                        )
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
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["RiskImpactValue"],
                                                row["RiskImpactScale"],
                                                row["ProbabilityValue"],
                                                row["ProbabilityScale"],
                                                row["RiskExposureValue"],
                                                row["RiskScaleType"],
                                                row["RiskScaleDetails"],
                                            )
                                else:
                                    logging.info(
                                        "Rows exist. Performing UPDATE or INSERT."
                                    )
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
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["RiskImpactValue"],
                                                row["RiskImpactScale"],
                                                row["ProbabilityValue"],
                                                row["ProbabilityScale"],
                                                row["RiskExposureValue"],
                                                row["RiskScaleType"],
                                                row["RiskScaleDetails"],
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["Company"],
                                                row["RiskID"],
                                                row["Year"],
                                                row["Quarter"],
                                                row["RiskImpactValue"],
                                                row["RiskImpactScale"],
                                                row["ProbabilityValue"],
                                                row["ProbabilityScale"],
                                                row["RiskExposureValue"],
                                                row["RiskScaleType"],
                                                row["RiskScaleDetails"],
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
                                                row["Company"],  # For INSERT
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
