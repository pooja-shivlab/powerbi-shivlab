from Common_powerBI import *
from Parent_Operational_ESG import *

if "Subsidiary" in main_folder_list:
    logging.info("Processing 'Subsidiary' folder.")
    xlsx_files = process_subfolders(
        ctx, parent_path="/sites/Dashboard-UAT/Shared%20Documents/Subsidiary"
    )
    all_xlsx_files.extend(xlsx_files)
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

                logging.info(f"Sheet names in the workbook: {sheet_names}")
                if "Preface" in sheet_names:
                    sheet_names.remove("Preface")

                for sheet_name in sheet_names:

                    if sheet_name in [
                        "Financial Performance",
                        "Project Timeline",
                        "Construction Timeline",
                    ]:
                        skiprows = 3
                        header = 0
                    elif sheet_name in [
                        "Electricity Generation (monthly",
                        "Outages & Availability (Monthly",
                        "Project Detail",
                    ]:
                        skiprows = 2
                        header = 0
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
                    elif sheet_name == "Electricity Generation (Annualy":
                        skiprows = 0
                        header = 0
                    elif sheet_name in [
                        "Risk Details",
                        "KRI Details",
                        "Inherent Risk",
                        "Residual Risk",
                        "Balance Sheet",
                        "RKAP Balance Sheet",
                        "Income Statement",
                        "RKAP Income Statement",
                        "Cash Flow",
                        "RKAP Cash Flow",
                        "RKAP Cash Flow",
                        "Subsidiary Balance Sheet",
                        "Subsidiary FM Balance Sheet",
                        "Subsidiary RKAP Balance Sheet",
                        "Subsidiary Income Statement",
                        "Subsidiary FM Income Statement",
                        "Subsidiary RKAP Income Statemen",
                        "Subsidiary Cash Flow",
                        "Subsidiary FM Cash Flow",
                        "Subsidiary RKAP Cash Flow",
                        "Debt Management",
                    ]:


                        continue
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
                    df["Company"] = subfolder
                    df["Dashboard"] = inferred_dashboard

                    if sheet_name in ["Project Expenses"]:
                        if isinstance(df.columns, pd.MultiIndex):

                            df.columns = [
                                " ".join(col).strip() for col in df.columns.values
                            ]

                    for col in df.columns:
                        if df[col].dtype == "object":
                            df[col] = df[col].str.strip()

                    df["Company"] = subfolder
                    df.columns = (
                        df.columns.str.strip()
                        .str.replace(" ", "_")
                        .str.replace(r"[^a-zA-Z0-9_]", "")
                    )

                    df.fillna(0, inplace=True)

                    if sheet_name == "Financial Performance":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Financial Performance' sheet."
                            )

                            required_columns = [
                                "Date",
                                "Penalty_Cost_(IDR)",
                                "Remarks",
                                "Notes",
                            ]

                            for col in required_columns:
                                column_mapping = {
                                    "Date": "Date",
                                    "Penalty_Cost_(IDR)": "PenaltyCost(IDR)",
                                    "Remarks": "Remarks",
                                    "Notes": "Notes",
                                }
                                df.rename(columns=column_mapping, inplace=True)
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "dbo.OP_FinancialPerformance"
                                existing_rows_query = f"""
                                                    SELECT Date, Remarks ,Company
                                                    FROM {table_name}
                                                """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(
                                    zip(df["Date"], df["Remarks"], df["Company"])
                                )

                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
                                    cursor.execute(truncate_query, (company_name,))

                                    insert_query = f"""
                                                        INSERT INTO {table_name} (
                                                            [PenaltyCost(IDR)], [Notes], [Company], [Date], 
                                                            [Remarks]
                                                        )
                                                        VALUES (?, ?, ?, ?, ?)
                                                    """
                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["PenaltyCost(IDR)"],
                                                row["Notes"],
                                                row["Company"],
                                                row["Date"],
                                                row["Remarks"],
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



                                    for _, row in df.iterrows():
                                        placeholders = (
                                            row["Date"],
                                            row["Remarks"],
                                            row["Company"],
                                            row["PenaltyCost(IDR)"],
                                            row["Notes"],
                                            row["Date"],
                                            row["Remarks"],
                                            row["Company"],
                                            row["PenaltyCost(IDR)"],
                                            row["Notes"],
                                            row["Company"],
                                            row["Date"],
                                            row["Remarks"],
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

                    elif sheet_name == "Project Timeline":
                        try:
                            logging.info(
                                "Special processing for 'Project Timeline' sheet."
                            )
                            company_name = df["Company"].iloc[0]
                            required_columns = [
                                "Phase",
                                "Stage",
                                "Planned_Completion_Date",
                                "Actual_Completion_Date",
                                "Status",
                                "Progression",
                            ]

                            column_mapping = {
                                "Phase": "Phase",
                                "Stage": "Stage",
                                "Planned_Completion_Date": "PlannedCompletionDate",
                                "Actual_Completion_Date": "ActualCompletionDate",
                                "Status": "Status",
                                "Progression": "Progression",
                            }
                            df.rename(columns=column_mapping, inplace=True)
                            df["PlannedCompletionDate"] = pd.to_datetime(
                                df["PlannedCompletionDate"], errors="coerce"
                            )
                            df["ActualCompletionDate"] = pd.to_datetime(
                                df["ActualCompletionDate"], errors="coerce"
                            )

                            if "Created" in df.columns:
                                df.drop(columns=["Created"], inplace=True)

                            table_name = "[dbo].[OP_ProjectTimeline]"
                            existing_rows_query = f"""
                                                                 SELECT Company, Stage ,Status
                                                                 FROM {table_name}
                                                             """
                            cursor.execute(existing_rows_query)
                            rows = cursor.fetchall()
                            existing_rows_set = {tuple(row) for row in rows}

                            df_tuples = set(
                                zip(df["Company"], df["Stage"], df["Status"])
                            )

                            missing_rows = df_tuples - existing_rows_set
                            if missing_rows:


                                truncate_query = (
                                    f"DELETE FROM {table_name} WHERE Company = ?;"
                                )
                                cursor.execute(truncate_query, (company_name,))

                                insert_query = f"""
                                    INSERT INTO {table_name} (
                                            [Company], [Phase], [Stage], [PlannedCompletionDate], [ActualCompletionDate],
                                            [Status], [Progression]                                             
                                        )
                                        VALUES (?, ?, ?, ?, ?, ?, ?)
                                                           """

                                for _, row in df.iterrows():
                                    cursor.execute(
                                        insert_query,
                                        (
                                            row.get("Company", None),
                                            row.get("Phase", None),
                                            row.get("Stage", None),
                                            row.get("PlannedCompletionDate", None),
                                            row.get("ActualCompletionDate", None),
                                            row.get("Status", None),
                                            row.get("Progression", None),
                                        ),
                                    )
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


                                for _, row in df.iterrows():
                                    cursor.execute(
                                        update_insert_query,
                                        (
                                            row.get("Company", None),
                                            row.get("Stage", None),
                                            row.get("Status", None),
                                            row.get("PlannedCompletionDate", None),
                                            row.get("ActualCompletionDate", None),
                                            row.get("Progression", None),
                                            row.get("Phase", None),
                                            row.get("Company", None),
                                            row.get("Stage", None),
                                            row.get("Status", None),
                                            row.get("Company", None),
                                            row.get("Phase", None),
                                            row.get("Stage", None),
                                            row.get("PlannedCompletionDate", None),
                                            row.get("ActualCompletionDate", None),
                                            row.get("Status", None),
                                            row.get("Progression", None),
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

                    elif sheet_name == "Project Detail":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Project Detail' sheet."
                            )
                            df.fillna(0, inplace=True)

                            required_columns = [
                                "Project_Duration_(Days)",
                                "Construction_Duration_(Days)",
                                "Currency",
                                "Construction",
                                "Others",
                                "Total",
                                "Currency.1",
                                "Construction.1",
                                "Others.1",
                                "Total.1",
                                "%_Construction",
                                "%_Others",
                                "%_total",
                            ]

                            for col in required_columns:
                                column_mapping = {
                                    "Project_Duration_(Days)": "ProjectDuration_Days",
                                    "Construction_Duration_(Days)": "ConstructionDuration",
                                    "Currency": "Budget_Currency",
                                    "Construction": "Budget_Construction",
                                    "Others": "Budget_Other",
                                    "Total": "Budget_Total",
                                    "Currency.1": "Actual_Currency",
                                    "Construction.1": "Actual_Construction",
                                    "Others.1": "Actual_Other",
                                    "Total.1": "Actual_Total",
                                    "%_Construction": "ConstructionPercentage",
                                    "%_Others": "OtherPercentage",
                                    "%_total": "TotalPercentage",
                                }

                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                project_list_df = pd.DataFrame(
                                    project_list, columns=["Subsidiary_Name", "Project"]
                                )

                                matching_projects = project_list_df[
                                    project_list_df["Subsidiary_Name"] == company_name
                                ]["Project"].tolist()

                                if matching_projects:

                                    df["Project"] = ", ".join(matching_projects)


                                current_project = df["Project"].iloc[0]
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
                                        (row["Company"], row["Project"])
                                    )

                                missing_rows = existing_rows_set - all_current_records
                                if missing_rows:

                                    truncate_query = f"""
                                                    DELETE FROM {table_name} WITH (ROWLOCK, UPDLOCK)
                                                    WHERE [Company] = ? AND [Project] = ?
                                            """
                                    missing_rows = list(missing_rows)
                                    cursor.executemany(truncate_query, missing_rows)
                                    conn.commit()


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
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["Company"],
                                                row["Project"],
                                                row["ProjectDuration_Days"],
                                                row["ConstructionDuration"],
                                                row["Budget_Currency"],
                                                row["Budget_Construction"],
                                                row["Budget_Other"],
                                                row["Budget_Total"],
                                                row["Actual_Currency"],
                                                row["Actual_Construction"],
                                                row["Actual_Other"],
                                                row["Actual_Total"],
                                                row["ConstructionPercentage"],
                                                row["OtherPercentage"],
                                                row["TotalPercentage"],
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

                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["Project"],
                                                row["ProjectDuration_Days"],
                                                row["ConstructionDuration"],
                                                row["Budget_Currency"],
                                                row["Budget_Construction"],
                                                row["Budget_Other"],
                                                row["Budget_Total"],
                                                row["Actual_Currency"],
                                                row["Actual_Construction"],
                                                row["Actual_Other"],
                                                row["Actual_Total"],
                                                row["ConstructionPercentage"],
                                                row["OtherPercentage"],
                                                row["TotalPercentage"],
                                                row["Company"],
                                                row["Project"],
                                                row["Company"],
                                                row["Project"],
                                                row["ProjectDuration_Days"],
                                                row["ConstructionDuration"],
                                                row["Budget_Currency"],
                                                row["Budget_Construction"],
                                                row["Budget_Other"],
                                                row["Budget_Total"],
                                                row["Actual_Currency"],
                                                row["Actual_Construction"],
                                                row["Actual_Other"],
                                                row["Actual_Total"],
                                                row["ConstructionPercentage"],
                                                row["OtherPercentage"],
                                                row["TotalPercentage"],
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

                    elif sheet_name == "Project Expenses":
                        try:
                            company_name = df["Company"].iloc[0]

                            logging.info(
                                "Special processing for 'Project Expenses' sheet."
                            )
                            df.columns = df.columns.str.replace(" ", "_").str.replace(
                                r"[^a-zA-Z0-9_]", ""
                            )
                            df["Actual_Construction"] = pd.to_numeric(
                                df["Actual_Construction"], errors="coerce"
                            )
                            df["Actual_Others"] = pd.to_numeric(
                                df["Actual_Others"], errors="coerce"
                            )
                            df["Actual_Total"] = pd.to_numeric(
                                df["Actual_Total"], errors="coerce"
                            )
                            df.fillna(0, inplace=True)

                            required_columns = [
                                "Date_Unnamed:_0_level_1",
                                "Actual_Currency",
                                "Actual_Construction",
                                "Actual_Others",
                                "Actual_Total",
                            ]

                            for col in required_columns:
                                column_mapping = {
                                    "Date_Unnamed:_0_level_1": "Date",
                                    "Actual_Currency": "Currency",
                                    "Actual_Construction": "Construction",
                                    "Actual_Others": "Other",
                                    "Actual_Total": "Total",
                                }

                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)


                                table_name = "dbo.OP_ProjectExpenses"
                                existing_rows_query = f"""
                                                                     SELECT Date, Company
                                                                     FROM {table_name}
                                                                 """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Date"], df["Company"]))

                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
                                    cursor.execute(truncate_query, (company_name,))

                                    insert_query = f"""
                                                               INSERT INTO {table_name} (
                                                                       [Date], [Company], [Currency], [Construction], [Other], [Total]
                                                                   )
                                                                   VALUES (?, ?, ?, ?, ?, ?)
                                                          """
                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row.get("Date", None),
                                                row.get("Company", None),
                                                row.get("Currency", None),
                                                row.get("Construction", None),
                                                row.get("Other", None),
                                                row.get("Total", None),
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

                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Date"],
                                                row["Company"],
                                                row["Currency"],
                                                row["Construction"],
                                                row["Other"],
                                                row["Total"],
                                                row["Date"],
                                                row["Company"],
                                                row["Date"],
                                                row["Company"],
                                                row["Currency"],
                                                row["Construction"],
                                                row["Other"],
                                                row["Total"],
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

                    elif sheet_name == "Construction Timeline":
                        try:
                            company_name = df["Company"].iloc[0]
                            df.columns = df.columns.str.replace(" ", "_").str.replace(
                                r"[^a-zA-Z0-9_]", ""
                            )
                            required_columns = [
                                "Milestone",
                                "Planned",
                                "Actual",
                                "Planned.1",
                                "Forecasted",
                                "Unnamed:_5",
                                "Unnamed:_6",
                                "Unnamed:_7",
                            ]

                            df["Planned"] = pd.to_datetime(
                                df["Planned"], errors="coerce"
                            )
                            df["Actual"] = pd.to_datetime(df["Actual"], errors="coerce")
                            df["Planned.1"] = pd.to_datetime(
                                df["Planned.1"], errors="coerce"
                            )
                            df["Forecasted"] = pd.to_datetime(
                                df["Forecasted"], errors="coerce"
                            )

                            df["Unnamed:_5"] = df["Unnamed:_5"].fillna(0.0)
                            df["Unnamed:_6"] = df["Unnamed:_6"].fillna(0)

                            df["Planned"] = df["Planned"].fillna(method="ffill")
                            df["Actual"] = df["Actual"].fillna(method="ffill")
                            df["Planned.1"] = df["Planned.1"].fillna(method="bfill")
                            df["Forecasted"] = df["Forecasted"].fillna(method="bfill")

                            if df["Planned"].isna().any():
                                print(
                                    "Warning: Some 'PlannedStartDate' values could not be converted."
                                )
                            if df["Actual"].isna().any():
                                print(
                                    "Warning: Some 'ActualStartDate' values could not be converted."
                                )
                            if df["Planned.1"].isna().any():
                                print(
                                    "Warning: Some 'PlannedEndDate' values could not be converted."
                                )
                            if df["Forecasted"].isna().any():
                                print(
                                    "Warning: Some 'ForecastedEndDate' values could not be converted."
                                )
                            for col in required_columns:
                                column_mapping = {
                                    "Milestone": "Milestone",
                                    "Planned": "PlannedStartDate",
                                    "Actual": "ActualStartDate",
                                    "Planned.1": "PlannedEndDate",
                                    "Forecasted": "ForecastedEndDate",
                                    "Unnamed:_5": "Duration",
                                    "Unnamed:_6": "Completion",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "dbo.OP_ConstructionTimeline"
                                existing_rows_query = f"""
                                                                     SELECT Milestone, Company
                                                                     FROM {table_name}
                                                                 """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(
                                    zip(
                                        df["Milestone"],
                                        df["Company"],
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
                                                                          [Milestone], [PlannedStartDate], [ActualStartDate],
                                                                           [PlannedEndDate], [ForecastedEndDate], [Duration],
                                                                           [Completion], [Company]
                                                                      )
                                                                      VALUES (?, ?, ?, ?, ?, ?, ?,?)
                                                               """

                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["Milestone"],
                                                row["PlannedStartDate"],
                                                row["ActualStartDate"],
                                                row["PlannedEndDate"],
                                                row["ForecastedEndDate"],
                                                row["Duration"],
                                                row["Completion"],
                                                row["Company"],
                                            ),
                                        )
                                else:
                                    logging.info(
                                        "Rows exist. Performing UPDATE or INSERT."
                                    )

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

                                    for _, row in df.iterrows():
                                        try:
                                            row["PlannedStartDate"] = (
                                                row["PlannedStartDate"].to_pydatetime()
                                                if row["PlannedStartDate"]
                                                else None
                                            )
                                            row["ActualStartDate"] = (
                                                row["ActualStartDate"].to_pydatetime()
                                                if row["ActualStartDate"]
                                                else None
                                            )
                                            row["PlannedEndDate"] = (
                                                row["PlannedEndDate"].to_pydatetime()
                                                if row["PlannedEndDate"]
                                                else None
                                            )
                                            row["ForecastedEndDate"] = (
                                                row["ForecastedEndDate"].to_pydatetime()
                                                if row["ForecastedEndDate"]
                                                else None
                                            )

                                            row["Duration"] = (
                                                int(row["Duration"])
                                                if row["Duration"] is not None
                                                else 0
                                            )
                                            row["Completion"] = (
                                                float(row["Completion"])
                                                if row["Completion"] is not None
                                                else 0.0
                                            )

                                            placeholders = (
                                                row["Milestone"],
                                                row["Company"],
                                                row["PlannedStartDate"],
                                                row["ActualStartDate"],
                                                row["PlannedEndDate"],
                                                row["ForecastedEndDate"],
                                                row["Duration"],
                                                row["Completion"],
                                                row["Milestone"],
                                                row["Company"],
                                                row["Milestone"],
                                                row["PlannedStartDate"],
                                                row["ActualStartDate"],
                                                row["PlannedEndDate"],
                                                row["ForecastedEndDate"],
                                                row["Duration"],
                                                row["Completion"],
                                                row["Company"],
                                            )
                                            cursor.execute(
                                                if_exists_query, placeholders
                                            )

                                        except Exception as e:
                                            logging.error(
                                                f"Error converting row data: {row.to_dict()} -> {e}"
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

                    elif sheet_name == "Electricity Generation (Annualy":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Electricity Generation (Annualy)' sheet."
                            )

                            required_columns = [
                                "Year",
                                "Contracted Electricity Delivered (MWh)",
                                "Tariff/MWh (IDR)",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Year": "Year",
                                    "Contracted_Electricity_Delivered_(MWh)": "ContractedElectricityDelivered",
                                    "Tariff/MWh_(IDR)": "Tariff_MWh",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                df["ContractedElectricityDelivered"] = df[
                                    "ContractedElectricityDelivered"
                                ].fillna(0)
                                df["Tariff_MWh"] = df["Tariff_MWh"].fillna(0)

                                table_name = "dbo.OP_AnnualyElectricityGeneration"
                                existing_rows_query = f"""
                                                                     SELECT Year, Company
                                                                     FROM {table_name}
                                                                                 """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Year"], df["Company"]))

                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:

                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
                                    cursor.execute(truncate_query, (company_name,))

                                    insert_query = f"""
                                                                   INSERT INTO {table_name} (
                                                                       [Year], [Company], [ContractedElectricityDelivered], [Tariff_MWh]

                                                                   )
                                                                   VALUES (?, ?, ?, ?)
                                                                                               """

                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["Year"],
                                                row["Company"],
                                                row["ContractedElectricityDelivered"],
                                                row["Tariff_MWh"],
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


                                    df = df.sort_values(by=["Company"])

                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Year"],
                                                row["Company"],
                                                row["ContractedElectricityDelivered"],
                                                row["Tariff_MWh"],
                                                row["Year"],
                                                row["Company"],
                                                row["Year"],
                                                row["Company"],
                                                row["ContractedElectricityDelivered"],
                                                row["Tariff_MWh"],
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

                    elif sheet_name == "Electricity Generation (monthly":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Electricity Generation (Monthly)' sheet."
                            )

                            df = df.drop(columns=["Unnamed:_0"])

                            required_columns = [
                                "Year",
                                "Recorded Electricity Delivered (MWh)",
                                "Cummulative Electricity Delivered (MWh)"
                                "Settled Electricity Delivered (MWh)",
                                "Settlement Variance",
                                "CF (%)",
                                "AF (%)",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Year": "Year",
                                    "Recorded_Electricity_Delivered_(MWh)": "RecordedElectricityDelivered",
                                    "Cummulative_Electricity_Delivered_(MWh)": "CummulativeElectricityDelivered",
                                    "Settled_Electricity_Delivered_(MWh)": "SettledElectricityDelivered",
                                    "Settlement_Variance": "SettlementVariance",
                                    "CF_(%)": "CFPercentage",
                                    "AF_(%)": "AFPercentage",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "dbo.OP_MonthlyElectricityGeneration"
                                existing_rows_query = f"""
                                                                         SELECT Company, Month
                                                                         FROM {table_name}
                                                                     """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Company"], df["Month"]))

                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
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
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row.get("Company", None),
                                                row.get("Year", None),
                                                row.get(
                                                    "RecordedElectricityDelivered", None
                                                ),
                                                row.get(
                                                    "CummulativeElectricityDelivered",
                                                    None,
                                                ),
                                                row.get(
                                                    "SettledElectricityDelivered", None
                                                ),
                                                row.get("SettlementVariance", None),
                                                row.get("CFPercentage", None),
                                                row.get("AFPercentage", None),
                                                row.get("Month", None),
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



                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["Month"],
                                                row["RecordedElectricityDelivered"],
                                                row["CummulativeElectricityDelivered"],
                                                row["SettledElectricityDelivered"],
                                                row["SettlementVariance"],
                                                row["CFPercentage"],
                                                row["AFPercentage"],
                                                row["Company"],
                                                row["Month"],
                                                row["Company"],
                                                row["Year"],
                                                row["RecordedElectricityDelivered"],
                                                row["CummulativeElectricityDelivered"],
                                                row["SettledElectricityDelivered"],
                                                row["SettlementVariance"],
                                                row["CFPercentage"],
                                                row["AFPercentage"],
                                                row["Month"],
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

                    elif sheet_name == "Electricity Generation (Daily)":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Electricity Generation (Daily)' sheet."
                            )
                            df = df.drop(columns=["Unnamed:_0"])
                            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

                            required_columns = [
                                "Date",
                                "Daily_Electricity_Generated",
                                "Cummulative_Electricity_Delivered_(MWh)"
                                "Progress_Bar",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Date": "Date",
                                    "Daily_Electricity_Generated": "DailyElectricityGenerated",
                                    "Cummulative_Electricity_Delivered_(MWh)": "CummulativeElectricityDelivered",
                                    "Progress_bar": "ProgressBar",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "OP_DailyElectricityGeneration"

                                existing_rows_query = f"""
                                                                    SELECT Date, Company
                                                                    FROM {table_name}
                                                                """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Date"], df["Company"]))

                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
                                    cursor.execute(truncate_query, (company_name,))

                                    insert_query = f"""
                                                               INSERT INTO {table_name} (
                                                                               [Date], [Company], [DailyElectricityGenerated],
                                                                               [CummulativeElectricityDelivered], [ProgressBar]

                                                               )     VALUES (?, ?, ?, ?, ?)
                                                          """
                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row.get("Date", None),
                                                row.get("Company", None),
                                                row.get(
                                                    "DailyElectricityGenerated", None
                                                ),
                                                row.get(
                                                    "CummulativeElectricityDelivered",
                                                    None,
                                                ),
                                                row.get("ProgressBar", None),
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


                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Date"],
                                                row["Company"],
                                                row["DailyElectricityGenerated"],
                                                row["CummulativeElectricityDelivered"],
                                                row["ProgressBar"],
                                                row["Date"],
                                                row["Company"],
                                                row["Date"],
                                                row["Company"],
                                                row["DailyElectricityGenerated"],
                                                row["CummulativeElectricityDelivered"],
                                                row["ProgressBar"],
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

                    elif sheet_name == "Coal Stockpile (Daily)":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Coal Stockpile (Daily)' sheet."
                            )
                            df = df.drop(columns=["Unnamed:_0"])

                            required_columns = [
                                "Date",
                                "Coal_Stockpile_(Days)",
                                "Mandated_Days_of_Coal_Stockpile",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Date": "Date",
                                    "Coal_Stockpile_(Days)": "CoalStockpileDays",
                                    "Mandated_Days_of_Coal_Stockpile": "MandatedDaysOfCoalStockpile",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "dbo.OP_CoalStockpileDaily"
                                existing_rows_query = f"""
                                                                        SELECT Company, Date
                                                                        FROM {table_name}
                                                                    """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Company"], df["Date"]))

                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
                                    cursor.execute(truncate_query, (company_name,))

                                    insert_query = f"""
                                                                            INSERT INTO {table_name} (
                                                                               [Company], [Date], [CoalStockpileDays],
                                                                               [MandatedDaysOfCoalStockpile]
                                                                           )
                                                                           VALUES (?, ?, ?, ?)
                                                                      """

                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row.get("Company", None),
                                                row.get("Date", None),
                                                row.get("CoalStockpileDays", None),
                                                row.get(
                                                    "MandatedDaysOfCoalStockpile", None
                                                ),
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


                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["Date"],
                                                row["CoalStockpileDays"],
                                                row["MandatedDaysOfCoalStockpile"],
                                                row["Company"],
                                                row["Date"],
                                                row["Company"],
                                                row["Date"],
                                                row["CoalStockpileDays"],
                                                row["MandatedDaysOfCoalStockpile"],
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

                    elif sheet_name == "Outages & Availability (Monthly":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Outages & Availability (Monthly' sheet."
                            )
                            df = df.drop(columns=["Unnamed:_0"])
                            required_columns = [
                                "Year",
                                "Month",
                                "Maintenance_Outage_(hours)",
                                "%",
                                "Scheduled_Outage_(hours)",
                                "%.1",
                                "Forced_Outage_(hours)",
                                "%.2",
                                "Actual_Outage_Hours",
                                "Permitted_Outage_Hours",
                                "Availability_Percentage",
                                "Required_Availability_Percentage",
                            ]

                            for col in required_columns:
                                column_mapping = {
                                    "Year": "Year",
                                    "Month": "Month",
                                    "Maintenance_Outage_(hours)": "MaintenanceOutageHours",
                                    "%": "MaintenanceOutagePercentage",
                                    "Scheduled_Outage_(hours)": "ScheduledOutageHours",
                                    "%.1": "ScheduledOutagePercentage",
                                    "Forced_Outage_(hours)": "ForcedOutageHours",
                                    "%.2": "ForcedOutagePercentage",
                                    "Actual_Outage_Hours": "ActualOutageHours",
                                    "Permitted_Outage_Hours": "PermittedOutageHours",
                                    "Availability_Percentage": "AvailabilityPercentage",
                                    "Required_Availability_Percentage": "RequiredAvailabilityPercentage",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "dbo.OP_MonthlyOutagesAndAvailability"
                                existing_rows_query = f"""
                                                                         SELECT Company, Month
                                                                         FROM {table_name}
                                                                     """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Company"], df["Month"]))
                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:

                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
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
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row.get("Company", None),
                                                row.get("Month", None),
                                                row.get("Year", None),
                                                row.get("MaintenanceOutageHours", None),
                                                row.get(
                                                    "MaintenanceOutagePercentage", None
                                                ),
                                                row.get("ScheduledOutageHours", None),
                                                row.get(
                                                    "ScheduledOutagePercentage", None
                                                ),
                                                row.get("ForcedOutageHours", None),
                                                row.get("ForcedOutagePercentage", None),
                                                row.get("ActualOutageHours", None),
                                                row.get("PermittedOutageHours", None),
                                                row.get("AvailabilityPercentage", None),
                                                row.get(
                                                    "RequiredAvailabilityPercentage",
                                                    None,
                                                ),
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



                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["Month"],
                                                row["Year"],
                                                row["MaintenanceOutageHours"],
                                                row["MaintenanceOutagePercentage"],
                                                row["ScheduledOutageHours"],
                                                row["ScheduledOutagePercentage"],
                                                row["ForcedOutageHours"],
                                                row["ForcedOutagePercentage"],
                                                row["ActualOutageHours"],
                                                row["PermittedOutageHours"],
                                                row["AvailabilityPercentage"],
                                                row["RequiredAvailabilityPercentage"],
                                                row["Company"],
                                                row["Month"],
                                                row["Company"],
                                                row["Month"],
                                                row["Year"],
                                                row["MaintenanceOutageHours"],
                                                row["MaintenanceOutagePercentage"],
                                                row["ScheduledOutageHours"],
                                                row["ScheduledOutagePercentage"],
                                                row["ForcedOutageHours"],
                                                row["ForcedOutagePercentage"],
                                                row["ActualOutageHours"],
                                                row["PermittedOutageHours"],
                                                row["AvailabilityPercentage"],
                                                row["RequiredAvailabilityPercentage"],
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

                    elif sheet_name == "Env - Scope 1 & 2 Emissions":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Env - Scope 1 & 2 Emissions'"
                            )

                            df = df.drop(columns=["Unnamed:_0"])
                            df.columns = df.columns.str.strip()
                            required_columns = [
                                "Month",
                                "Scope_1_tCO2e",
                                "Scope_2_tCO2e",
                                "Total_Scope_1_&_2",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Month": "Month",
                                    "Scope_1_tCO2e": "Scope1_tCO2e",
                                    "Scope_2_tCO2e": "Scope2_tCO2e",
                                    "Total_Scope_1_&_2": "Total_Scope1&2",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "[dbo].[SubsidiaryEnv-Scope1&2Emissions]"
                                existing_rows_query = f"""
                                                                                     SELECT Company, Month
                                                                                     FROM {table_name}
                                                                                 """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Company"], df["Month"]))

                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
                                    cursor.execute(truncate_query, (company_name,))

                                    insert_query = f"""
                                                                     INSERT INTO {table_name} (
                                                                       [Company], [Month], [Scope1_tCO2e], [Scope2_tCO2e], [Total_Scope1&2]
                                                                   )
                                                                   VALUES (?, ?, ?, ?, ?)
                                                                                """
                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["Company"],
                                                row["Month"],
                                                row["Scope1_tCO2e"],
                                                row["Scope2_tCO2e"],
                                                row["Total_Scope1&2"],
                                            ),
                                        )
                                else:
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



                                    for _, row in df.iterrows():

                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["Month"],
                                                row["Scope1_tCO2e"],
                                                row["Scope2_tCO2e"],
                                                row["Total_Scope1&2"],
                                                row["Company"],
                                                row["Month"],
                                                row["Company"],
                                                row["Month"],
                                                row["Scope1_tCO2e"],
                                                row["Scope2_tCO2e"],
                                                row["Total_Scope1&2"],
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

                    elif sheet_name == "Env - Utilities":
                        try:
                            logging.info("Special processing for 'Env-Utilities'")
                            company_name = df["Company"].iloc[0]
                            df = df.drop(columns=["Unnamed:_0"])

                            required_columns = [
                                "Month",
                                "Subsi_Electricity_Usage_(Wh)",
                                "Subsi_Actual_Water_Consumption_(m3)",
                                "Subsi_Actual_Fuel_Consumption_(L)",
                            ]

                            for col in required_columns:
                                column_mapping = {
                                    "Month": "Month",
                                    "Subsi_Electricity_Usage_(Wh)": "Subsi_ElectricityUsage(Wh)",
                                    "Subsi_Actual_Water_Consumption_(m3)": "Subsi_ActualWaterConsumption(m3)",
                                    "Subsi_Actual_Fuel_Consumption_(L)": "Subsi_ActualFuelConsumption(L)",
                                }
                                df.rename(columns=column_mapping, inplace=True)
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "[dbo].[SubsidiaryEnv-Utilities]"
                                existing_rows_query = f"""
                                                                    SELECT Company, Month
                                                                     FROM {table_name}
                                                                     """

                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Company"], df["Month"]))

                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
                                    cursor.execute(truncate_query, (company_name,))

                                    insert_query = f"""
                                                      INSERT INTO {table_name} ([Company], [Month], [Subsi_ElectricityUsage(Wh)], [Subsi_ActualWaterConsumption(m3)], [Subsi_ActualFuelConsumption(L)])

                                                                                   VALUES (?, ?, ?, ?, ?)
                                                              """

                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["Company"],
                                                row["Month"],
                                                row["Subsi_ElectricityUsage(Wh)"],
                                                row["Subsi_ActualWaterConsumption(m3)"],
                                                row["Subsi_ActualFuelConsumption(L)"],
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

                                    for _, row in df.iterrows():

                                        placeholders = (
                                            row["Company"],
                                            row["Month"],
                                            row["Subsi_ElectricityUsage(Wh)"],
                                            row["Subsi_ActualWaterConsumption(m3)"],
                                            row["Subsi_ActualFuelConsumption(L)"],
                                            row["Company"],
                                            row["Month"],
                                            row["Company"],
                                            row["Month"],
                                            row["Subsi_ElectricityUsage(Wh)"],
                                            row["Subsi_ActualWaterConsumption(m3)"],
                                            row["Subsi_ActualFuelConsumption(L)"],
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

                    elif sheet_name == "Social - Employee by Gender":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Social - Employee by Gender'"
                            )
                            df = df.drop(columns=["Unnamed:_0"])
                            df.columns = df.columns.str.strip()
                            required_columns = [
                                "Month",
                                "Total_Male",
                                "Total_Female",
                                "New_Hire_Male",
                                "New_Hire_Female",
                                "Turnover_Male",
                                "Turnover_Female",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Month": "Month",
                                    "Total_Male": "Total_Male",
                                    "Total_Female": "Total_Female",
                                    "New_Hire_Male": "NewHire_Male",
                                    "New_Hire_Female": "NewHire_Female",
                                    "Turnover_Male": "Turnover_Male",
                                    "Turnover_Female": "Turnover_Female",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "[dbo].[SubsidiarySocial-EmployeeByGender]"


                                existing_rows_query = f"""
                                                                SELECT Company, Month
                                                                 FROM {table_name}
                                                                 """

                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Company"], df["Month"]))
                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
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
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["Company"],
                                                row["Month"],
                                                row["Total_Male"],
                                                row["Total_Female"],
                                                row["NewHire_Male"],
                                                row["NewHire_Female"],
                                                row["Turnover_Male"],
                                                row["Turnover_Female"],
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


                                    for _, row in df.iterrows():

                                        placeholders = (
                                            row["Company"],
                                            row["Month"],
                                            row["Total_Male"],
                                            row["Total_Female"],
                                            row["NewHire_Male"],
                                            row["NewHire_Female"],
                                            row["Turnover_Male"],
                                            row["Turnover_Female"],
                                            row["Company"],
                                            row["Month"],
                                            row["Company"],
                                            row["Month"],
                                            row["Total_Male"],
                                            row["Total_Female"],
                                            row["NewHire_Male"],
                                            row["NewHire_Female"],
                                            row["Turnover_Male"],
                                            row["Turnover_Female"],
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

                    elif sheet_name == "Social - Employee by Age":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Social - Employee by Age'"
                            )

                            df = df.drop(columns=["Unnamed:_0"])

                            required_columns = [
                                "Month",
                                "Total_<35",
                                "Total_35-50",
                                "Total_51-63",
                                "Total_>63",
                                "New_Hire_<35",
                                "New_Hire_35-50",
                                "New_Hire_51-63",
                                "New_Hire_>63",
                                "Turnover_<35",
                                "Turnover_35-50",
                                "Turnover_51-63",
                                "Turnover_>63",
                            ]

                            for col in required_columns:

                                column_mapping = {
                                    "Month": "Month",
                                    "Total_<35": "Total_<35",
                                    "Total_35-50": "Total_35-50",
                                    "Total_51-63": "Total_51-63",
                                    "Total_>63": "Total_>63",
                                    "New_Hire_<35": "New Hire_<35",
                                    "New_Hire_35-50": "New Hire_35-50",
                                    "New_Hire_51-63": "New Hire_51-63",
                                    "New_Hire_>63": "New Hire_>63",
                                    "Turnover_<35": "Turnover_<35",
                                    "Turnover_35-50": "Turnover_35-50",
                                    "Turnover_51-63": "Turnover_51-63",
                                    "Turnover_>63": "Turnover_>63",
                                }

                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)


                                table_name = "[dbo].[SubsidiarySocial-EmployeeByAge]"



                                existing_rows_query = f"""

                                                                   SELECT Company, Month

                                                                    FROM {table_name}

                                                                    """

                                cursor.execute(existing_rows_query)

                                rows = cursor.fetchall()

                                existing_rows_set = {tuple(row) for row in rows}
                                df_tuples = set(zip(df["Company"], df["Month"]))

                                missing_rows = df_tuples - existing_rows_set

                                if missing_rows:



                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
                                    cursor.execute(truncate_query, (company_name,))

                                    insert_query = f"""

                                                                  INSERT INTO {table_name} ([Company], [Month], [Total_<35], [Total_35-50], [Total_51-63],  [Total_>63],

                                                                                              [New Hire_<35], [New Hire_35-50], [New Hire_51-63],[New Hire_>63], [Turnover_<35],

                                                                                               [Turnover_35-50],[Turnover_51-63], [Turnover_>63])

                                                                                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                                                              """

                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["Company"],
                                                row["Month"],
                                                row["Total_<35"],
                                                row["Total_35-50"],
                                                row["Total_51-63"],
                                                row["Total_>63"],
                                                row["New Hire_<35"],
                                                row["New Hire_35-50"],
                                                row["New Hire_51-63"],
                                                row["New Hire_>63"],
                                                row["Turnover_<35"],
                                                row["Turnover_35-50"],
                                                row["Turnover_51-63"],
                                                row["Turnover_>63"],
                                            ),
                                        )

                                else:


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

                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["Company"],
                                                row["Month"],
                                                row["Total_<35"],
                                                row["Total_35-50"],
                                                row["Total_51-63"],
                                                row["Total_>63"],
                                                row["New Hire_<35"],
                                                row["New Hire_35-50"],
                                                row["New Hire_51-63"],
                                                row["New Hire_>63"],
                                                row["Turnover_<35"],
                                                row["Turnover_35-50"],
                                                row["Turnover_51-63"],
                                                row["Turnover_>63"],
                                                row["Company"],
                                                row["Month"],
                                                row["Company"],
                                                row["Month"],
                                                row["Total_<35"],
                                                row["Total_35-50"],
                                                row["Total_51-63"],
                                                row["Total_>63"],
                                                row["New Hire_<35"],
                                                row["New Hire_35-50"],
                                                row["New Hire_51-63"],
                                                row["New Hire_>63"],
                                                row["Turnover_<35"],
                                                row["Turnover_35-50"],
                                                row["Turnover_51-63"],
                                                row["Turnover_>63"],
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

                    elif sheet_name == "Social - CSR":
                        try:
                            logging.info("Special processing for 'Social - CSR'")
                            company_name = df["Company"].iloc[0]

                            df = df.drop(columns=["Unnamed:_0", "Unnamed:_5"])

                            df.columns = df.columns.str.strip()

                            column_mapping = {
                                "Month": "Month",
                                "CSR__Project_Name": "CSR_ProjectName",
                                "CSR_Value": "CSR_Value",
                                "CSR_Disbursed": "CSR_Disbursement",
                            }
                            df.rename(columns=column_mapping, inplace=True)

                            if "Created" in df.columns:
                                df.drop(columns=["Created"], inplace=True)


                            table_name = "[dbo].[SubsidiarySocial-CSR]"

                            existing_rows_query = f"""
                                                                    SELECT Company, Month
                                                                     FROM {table_name}
                                                                 """

                            cursor.execute(existing_rows_query)
                            rows = cursor.fetchall()
                            existing_rows_set = {tuple(row) for row in rows}

                            df_tuples = set(zip(df["Company"], df["Month"]))

                            missing_rows = df_tuples - existing_rows_set

                            if missing_rows:


                                truncate_query = (
                                    f"DELETE FROM {table_name} WHERE Company = ?;"
                                )
                                cursor.execute(truncate_query, (company_name,))

                                insert_query = f"""
                                                         INSERT INTO {table_name} (Company, Month, CSR_ProjectName, CSR_Value, CSR_Disbursement)
                                                         VALUES (?, ?, ?, ?, ?)
                                                     """

                                for _, row in df.iterrows():
                                    cursor.execute(
                                        insert_query,
                                        (
                                            row["Company"],
                                            row["Month"],
                                            row["CSR_ProjectName"],
                                            row["CSR_Value"],
                                            row["CSR_Disbursement"],
                                        ),
                                    )
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
                                    cursor.execute(
                                        update_insert_query,
                                        (
                                            row["Company"],
                                            row["Month"],
                                            row["CSR_ProjectName"],
                                            row["CSR_Value"],
                                            row["CSR_Disbursement"],
                                            row["Company"],
                                            row["Month"],
                                            row["Company"],
                                            row["Month"],
                                            row["CSR_ProjectName"],
                                            row["CSR_Value"],
                                            row["CSR_Disbursement"],
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

                    elif sheet_name == "Gov - Management Diversity":
                        try:
                            company_name = df["Company"].iloc[0]
                            logging.info(
                                "Special processing for 'Gov - Management Diversity'"
                            )

                            df = df.drop(columns=["Unnamed:_0"])

                            df.columns = df.columns.str.strip()
                            required_columns = [
                                "Month",
                                "Senior_Male",
                                "Senior_Female",
                                "Middle_Male",
                                "Middle_Female",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Month": "Month",
                                    "Senior_Male": "Senior_Male",
                                    "Senior_Female": "Senior_Female",
                                    "Middle_Male": "Middle_Male",
                                    "Middle_Female": "Middle_Female",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "[dbo].[SubsidiaryGov-ManagementDiversity]"


                                existing_rows_query = f"""
                                                                                                SELECT Company, Month
                                                                                                 FROM {table_name}
                                                                                                 """

                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(zip(df["Company"], df["Month"]))
                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
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
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["Company"],
                                                row["Month"],
                                                row["Senior_Male"],
                                                row["Senior_Female"],
                                                row["Middle_Male"],
                                                row["Middle_Female"],
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



                                for _, row in df.iterrows():

                                    placeholders = (
                                        row["Company"],
                                        row["Month"],
                                        row["Senior_Male"],
                                        row["Senior_Female"],
                                        row["Middle_Male"],
                                        row["Middle_Female"],
                                        row["Company"],
                                        row["Month"],
                                        row["Company"],
                                        row["Month"],
                                        row["Senior_Male"],
                                        row["Senior_Female"],
                                        row["Middle_Male"],
                                        row["Middle_Female"],
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

                    elif sheet_name == "Gov - Board":
                        try:
                            logging.info("Special processing for 'Gov - Board'")
                            company_name = df["Company"].iloc[0]
                            df = df.drop(columns=["Unnamed:_0"])
                            df.columns = df.columns.str.strip()

                            required_columns = [
                                "Year",
                                "Name",
                                "Gender",
                                "Types",
                                "Executive/non-executive",
                                "Independence_(yes/no)",
                                "Board_Independence_Percentage",
                                "Start_Date",
                                "End_Date",
                                "Remaining_Period",
                                "Tenure_years",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "Year": "Year",
                                    "Name": "Name",
                                    "Gender": "Gender",
                                    "Types": "Types",
                                    "Executive/non-executive": "Executive/Non-Executive",
                                    "Independence_(yes/no)": "Independence",
                                    "Board_Independence_Percentage": "BoardIndependencePercentage",
                                    "Start_Date": "StartDate",
                                    "End_Date": "EndDate",
                                    "Remaining_Period": "RemainingPeriod",
                                    "Tenure_years": "TenureYears",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                if "FY" in df.columns:
                                    try:
                                        df["Year"] = df["Year"].astype(int)

                                    except ValueError as ve:
                                        logging.error(
                                            f"Failed to convert 'FY' column to integers: {ve}"
                                        )
                                        raise

                                df["BoardIndependencePercentage"] = pd.to_numeric(
                                    df["BoardIndependencePercentage"], errors="coerce"
                                )
                                df["StartDate"] = pd.to_datetime(
                                    df["StartDate"], errors="coerce"
                                )
                                df["EndDate"] = pd.to_datetime(
                                    df["EndDate"], errors="coerce"
                                )


                                table_name = "[dbo].[SubsidiaryGov-Board]"
                                existing_rows_query = f"""
                                                                      SELECT Company, Name, Year
                                                                       FROM {table_name}
                                                                       """

                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                df_tuples = set(
                                    zip(df["Company"], df["Name"], df["Year"])
                                )
                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
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
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["Company"],
                                                row["Name"],
                                                row["Year"],
                                                row["Gender"],
                                                row["Types"],
                                                row["Executive/Non-Executive"],
                                                row["Independence"],
                                                row["BoardIndependencePercentage"],
                                                row["StartDate"],
                                                row["EndDate"],
                                                row["RemainingPeriod"],
                                                row["TenureYears"],
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

                    elif sheet_name == "Targets":
                        try:

                            company_name = df["Company"].iloc[0]
                            df = df.drop(columns=["Unnamed:_0"])
                            df.columns = df.columns.str.strip()
                            required_columns = [
                                "FY",
                                "Scope_1_Threshold_(tCO2)",
                                "Projected_Scope_1__(tCO2e)",
                                "Turnover_Target",
                                "Target_CSR_%",
                                "Total_CSR_Budget_($)",
                                "Subsi_Fuel_Consumption_Target_(L)",
                                "Subsi_Water_Consumption_Target_(L)",
                            ]
                            for col in required_columns:
                                column_mapping = {
                                    "FY": "FY",
                                    "Scope_1_Threshold_(tCO2)": "Scope1_Threshold_tCO2",
                                    "Projected_Scope_1_(tCO2)": "Projected_Scope1t_CO2",
                                    "Turnover_Target": "TurnoverTarget",
                                    "Target_CSR_%": "TargetCSR",
                                    "Total_CSR_Budget_($)": "TotalCSRBudget",
                                    "Subsi_Fuel_Consumption_Target_(L)": "Subsi_FuelConsumptionTarget(L)",
                                    "Subsi_Water_Consumption_Target_(L)": "Subsi_WaterConsumptionTarget(L)",
                                }
                                df.rename(columns=column_mapping, inplace=True)

                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)

                                table_name = "[dbo].[SubsidiaryTargets]"
                                existing_rows_query = f"""
                                                                        SELECT FY, Company
                                                                         FROM {table_name}
                                                                       """

                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}
                                df_tuples = set(zip(df["FY"], df["Company"]))
                                missing_rows = existing_rows_set - df_tuples

                                if missing_rows:


                                    truncate_query = (
                                        f"DELETE FROM {table_name} WHERE Company = ?;"
                                    )
                                    cursor.execute(truncate_query, (company_name,))
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
                                        cursor.execute(
                                            insert_query,
                                            (
                                                row["FY"],
                                                row["Company"],
                                                row["Scope1_Threshold_tCO2"],
                                                row["Projected_Scope1t_CO2"],
                                                row["TurnoverTarget"],
                                                row["TargetCSR"],
                                                row["TotalCSRBudget"],
                                                row["Subsi_FuelConsumptionTarget(L)"],
                                                row["Subsi_WaterConsumptionTarget(L)"],
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



                                    for _, row in df.iterrows():

                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                row["FY"],
                                                row["Company"],
                                                row["Scope1_Threshold_tCO2"],
                                                row["Projected_Scope1t_CO2"],
                                                row["TurnoverTarget"],
                                                row["TargetCSR"],
                                                row["TotalCSRBudget"],
                                                row["Subsi_FuelConsumptionTarget(L)"],
                                                row["Subsi_WaterConsumptionTarget(L)"],
                                                row["FY"],
                                                row["Company"],
                                                row["FY"],
                                                row["Company"],
                                                row["Scope1_Threshold_tCO2"],
                                                row["Projected_Scope1t_CO2"],
                                                row["TurnoverTarget"],
                                                row["TargetCSR"],
                                                row["TotalCSRBudget"],
                                                row["Subsi_FuelConsumptionTarget(L)"],
                                                row["Subsi_WaterConsumptionTarget(L)"],
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
                        continue
        for (dashboard, sheet), status in sheet_status.items():
            insert_log_query = """
                               INSERT INTO [dbo].[SubsidiaryOperationESGDataLog] ([CompanyName], [Dashboard], [ModifiedDate], [SheetName], [Status], [Description])
                               VALUES (?, ?, GETDATE(), ?, ?, ?)
                           """
            cursor.execute(
                insert_log_query,
                (subfolder, dashboard, sheet, status["Status"], status["Description"]),
            )
            conn.commit()
