from Common_powerBI import *
def ParentSubsidiaryOPFunctionIndex():
    try:
        if "Parent" in main_folder_list:
            logging.info("Processing Parent Operational ESG folder.")

            xlsx_files = process_subfolders(
                ctx, parent_path="/sites/Dashboard-UAT/Shared%20Documents/Parent"
            )
            all_xlsx_files.extend(xlsx_files)
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
                                "Env - Scope 1 & 2 Emissions",
                                "Env - Scope 2 Electricity",
                                "Env - Utilities",
                                "Social - Employee by Gender",
                                "Social - Employee by Age",
                                "Social - CSR",
                                "Gov - Management Diversity",
                                "Gov - Board",
                                "Targets"
                            ]:
                                continue
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
                            df = pd.read_excel(
                                 local_copy_sas_url,
                                sheet_name=sheet_name,
                                skiprows=skiprows,
                                header=header,
                            )

                            df["Company"] = subfolder
                            df["Dashboard"] = inferred_dashboard
                            df.columns = (
                                df.columns.str.strip()
                                .str.replace(" ", "_")
                                .str.replace(r"[^a-zA-Z0-9_]", "")
                            )

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

                            if sheet_name == "Operation Overview":
                                logging.info(
                                    f"Processing sheet: {sheet_name} from Dashboard: {dashboard}"
                                )
                                try:
                                    name_table = "dbo.OperationOverview"
                                    required_columns = [
                                        "Subsidiary_Name",
                                        "Project",
                                        "Type_(Coal/Hydro/Solar)",
                                        "Stage",
                                        "COD_Date",
                                        "NDC_(MW)",
                                        "Latitude",
                                        "Langitude",
                                    ]
                                    for col in required_columns:
                                        column_mapping = {
                                            "Subsidiary_Name": "Subsidiary_Name",
                                            "Project": "Project",
                                            "Type_(Coal/Hydro/Solar)": "Type_(Coal/Hydro/Solar)",
                                            "Stage": "Stage",
                                            "COD_Date": "COD_Date",
                                            "NDC_(MW)": "NDC_(MW)",
                                            "Latitude": "Latitude",
                                            "Langitude": "Langitude",
                                        }
                                        df.rename(columns=column_mapping, inplace=True)

                                        if "Created" in df.columns:
                                            df.drop(columns=["Created"], inplace=True)

                                        table_name = "[dbo].[OperationOverview]"
                                        df["Subsidiary_Name"] = df["Subsidiary_Name"].apply(
                                            lambda x: str(x) if not isinstance(x, str) else x
                                        )
                                        df["Subsidiary_Name"] = df["Subsidiary_Name"].apply(
                                            escape_special_characters
                                        )

                                        df["Project"] = df["Project"].apply(
                                            lambda x: str(x) if not isinstance(x, str) else x
                                        )

                                        df["Project"] = df["Project"].apply(
                                            escape_special_characters
                                        )

                                        df["Subsidiary_Name"] = (
                                            df["Subsidiary_Name"]
                                            .astype(str)
                                            .apply(escape_special_characters)
                                        )
                                        df["Project"] = (
                                            df["Project"]
                                            .astype(str)
                                            .apply(escape_special_characters)
                                        )
                                        df["COD_Date"] = pd.to_datetime(
                                            df["COD_Date"], errors="coerce"
                                        ).dt.date
                                        df["NDC_(MW)"] = (
                                            df["NDC_(MW)"]
                                            .apply(pd.to_numeric, errors="coerce")
                                            .round(2)
                                        )
                                        df["Latitude"] = pd.to_numeric(
                                            df["Latitude"], errors="coerce"
                                        ).round(6)
                                        df["Langitude"] = pd.to_numeric(
                                            df["Langitude"], errors="coerce"
                                        ).round(6)
                                        df.dropna(
                                            subset=["Latitude", "Langitude", "NDC_(MW)"],
                                            inplace=True,
                                        )
                                        df["Latitude"].fillna(0, inplace=True)
                                        df["Langitude"].fillna(0, inplace=True)
                                        df["NDC_(MW)"].fillna(0, inplace=True)

                                        global project_list
                                        if (
                                            "Project" in df.columns
                                            and "Subsidiary_Name" in df.columns
                                        ):
                                            project_list = (
                                                df[["Subsidiary_Name", "Project"]]
                                                .drop_duplicates()
                                                .values.tolist()
                                            )

                                        existing_rows_query = f"""
                                                                               SELECT Subsidiary_Name, Project 
                                                                               FROM {table_name}
                                                                           """
                                        cursor.execute(existing_rows_query)
                                        rows = cursor.fetchall()
                                        existing_rows_set = {tuple(row) for row in rows}

                                        df_tuples = set(
                                            zip(df["Subsidiary_Name"], df["Project"])
                                        )

                                        missing_rows = df_tuples - existing_rows_set

                                        if missing_rows:

                                            truncate_query = f"TRUNCATE TABLE {table_name};"
                                            cursor.execute(truncate_query)

                                            insert_query = f"""
                                                               INSERT INTO {table_name} (Subsidiary_Name, Project, [Type_(Coal/Hydro/Solar)], Stage, COD_Date, [NDC_(MW)], Latitude, Langitude, Company)
                                                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                           """
                                            for _, row in df.iterrows():
                                                cursor.execute(
                                                    insert_query,
                                                    (
                                                        row["Subsidiary_Name"],
                                                        row["Project"],
                                                        row["Type_(Coal/Hydro/Solar)"],
                                                        row["Stage"],
                                                        row["COD_Date"],
                                                        row["NDC_(MW)"],
                                                        row["Latitude"],
                                                        row["Langitude"],
                                                        row["Company"],
                                                    ),
                                                )
                                        else:
                                            logging.info(
                                                "Rows exist. Performing UPDATE or INSERT."
                                            )
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
                                                cursor.execute(
                                                    update_insert_query,
                                                    (
                                                        row["Subsidiary_Name"],
                                                        row["Project"],
                                                        row["Type_(Coal/Hydro/Solar)"],
                                                        row["Stage"],
                                                        row["COD_Date"],
                                                        row["NDC_(MW)"],
                                                        row["Latitude"],
                                                        row["Langitude"],
                                                        row["Company"],
                                                        row["Subsidiary_Name"],
                                                        row["Project"],
                                                        row["Subsidiary_Name"],
                                                        row["Project"],
                                                        row["Type_(Coal/Hydro/Solar)"],
                                                        row["Stage"],
                                                        row["COD_Date"],
                                                        row["NDC_(MW)"],
                                                        row["Latitude"],
                                                        row["Langitude"],
                                                        row["Company"],
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
                            INSERT INTO [dbo].[OperationESGDataLog] ([CompanyName], [Dashboard], [ModifiedDate], [SheetName], [Status], [Description])
                            VALUES (?, ?, GETDATE(), ?, ?, ?)
                        """
                    cursor.execute(
                        insert_log_query,
                        (subfolder, dashboard, sheet, status["Status"], status["Description"]),
                    )
                    conn.commit()
    except Exception as e:
        print("Parent Exception",e)


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
                    print(f'file is {file}')
                    file_content = File.open_binary(ctx, file)
                    logging.info(f"file content is {file_content}")

                    # Load the workbook to inspect sheet names
                    uploadToBlobStorage(file_content, "local_copy.xlsx")
                    logging.info("upload to blob successfully")
                    xls = pd.ExcelFile(local_copy_sas_url)
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
                            local_copy_sas_url,
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
