from Common_powerBI import *

def ParentESGFunctionIndex():
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

                    # target_file = ctx.web.get_file_by_server_relative_url(file)
                    # with open("local_copy.xlsx", "wb") as local_file:
                    #     target_file.download(local_file).execute_query()
                    #
                    # xls = pd.ExcelFile("local_copy.xlsx")
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
                            "Debt Management",
                            "Operation Overview",
                            "Financial Performance",
                            "Project Timeline",
                            "Construction Timeline",
                            "Project Detail",
                            "Project Expenses",
                            "Electricity Generation (Annualy",
                            "Electricity Generation (monthly",
                            "Electricity Generation (Daily)",
                            "Outages & Availability (Monthly",
                            "Coal Stockpile (Daily)"

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

                        if sheet_name == "Env - Scope 1 & 2 Emissions":
                            try:
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

                                    table_name = "[dbo].[Env-Scope1&2Emissions]"
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

                                        truncate_query = f"TRUNCATE TABLE {table_name};"
                                        cursor.execute(truncate_query)

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
                                df = df.drop(columns=["Unnamed:_0"])

                                required_columns = [
                                    "Month",
                                    "IPRen_Electricity_Usage_(Wh)",
                                    "IPRen_Actual_Water_Consumption_(m3)",
                                    "IPRen_Actual_Fuel_Consumption_(L)",
                                ]
                                for col in required_columns:
                                    column_mapping = {
                                        "Month": "Month",
                                        "IPRen_Electricity_Usage_(Wh)": "IPRen_ElectricityUsage(Wh)",
                                        "IPRen_Actual_Water_Consumption_(m3)": "IPRen_ActualWaterConsumption(m3)",
                                        "IPRen_Actual_Fuel_Consumption_(L)": "IPRen_ActualFuelConsumption(L)",
                                    }
                                    df.rename(columns=column_mapping, inplace=True)

                                    if "Created" in df.columns:
                                        df.drop(columns=["Created"], inplace=True)

                                    table_name = "[dbo].[Env-Utilities]"
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

                                        truncate_query = f"TRUNCATE TABLE {table_name};"
                                        cursor.execute(truncate_query)
                                        insert_query = f"""
                            
                                                            INSERT INTO {table_name} ([Company], [Month], [IPRen_ElectricityUsage(Wh)], [IPRen_ActualWaterConsumption(m3)], [IPRen_ActualFuelConsumption(L)])
                                                                                VALUES (?, ?, ?, ?, ?)
                                                        """

                                        for _, row in df.iterrows():
                                            cursor.execute(
                                                insert_query,
                                                (
                                                    row["Company"],
                                                    row["Month"],
                                                    row["IPRen_ElectricityUsage(Wh)"],
                                                    row["IPRen_ActualWaterConsumption(m3)"],
                                                    row["IPRen_ActualFuelConsumption(L)"],
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

                                        for _, row in df.iterrows():
                                            placeholders = (
                                                row["Company"],
                                                row["Month"],
                                                row["IPRen_ElectricityUsage(Wh)"],
                                                row["IPRen_ActualWaterConsumption(m3)"],
                                                row["IPRen_ActualFuelConsumption(L)"],
                                                row["Company"],
                                                row["Month"],
                                                row["Company"],
                                                row["Month"],
                                                row["IPRen_ElectricityUsage(Wh)"],
                                                row["IPRen_ActualWaterConsumption(m3)"],
                                                row["IPRen_ActualFuelConsumption(L)"],
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

                                    table_name = "[dbo].[Social-EmployeeByGender]"

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

                                    table_name = "[dbo].[Social-EmployeeByAge]"

                                    existing_rows_query = f"""
                                                                    SELECT Company, Month FROM {table_name}
                                                                """

                                    cursor.execute(existing_rows_query)
                                    rows = cursor.fetchall()
                                    existing_rows_set = {tuple(row) for row in rows}

                                    df_tuples = set(zip(df["Company"], df["Month"]))
                                    missing_rows = df_tuples - existing_rows_set
                                    if missing_rows:

                                        truncate_query = f"TRUNCATE TABLE {table_name};"
                                        cursor.execute(truncate_query)
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
                                            placeholders = (
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

                                table_name = "[dbo].[Social-CSR]"

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

                                    truncate_query = f"TRUNCATE TABLE {table_name};"
                                    cursor.execute(truncate_query)

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

                                    table_name = "[dbo].[Gov-ManagementDiversity]"

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
                                df = df.drop(columns=["Unnamed:_0"])

                                df.columns = df.columns.str.strip()
                                required_columns = [
                                    "Year",
                                    "Name",
                                    "Gender",
                                    "Types",
                                    "Executive/non-executive",
                                    "Independence_(yes/no)",
                                    "Board_Independence_Percentage_(%)",
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
                                        "Board_Independence_Percentage_(%)": "BoardIndependencePercentage",
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

                                    table_name = "[dbo].[Gov-Board]"

                                    existing_rows_query = f"""
                                                                SELECT Name, Company, Year
                                                                FROM {table_name}
                                                                """

                                    cursor.execute(existing_rows_query)
                                    rows = cursor.fetchall()
                                    existing_rows_set = {tuple(row) for row in rows}
                                    df_tuples = set(
                                        zip(df["Name"], df["Company"], df["Year"])
                                    )
                                    missing_rows = existing_rows_set - df_tuples

                                    if missing_rows:

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
                                            cursor.execute(
                                                insert_query,
                                                (
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
                                                    row["Company"],
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

                                        for _, row in df.iterrows():
                                            cursor.execute(
                                                update_insert_query,
                                                (
                                                    row["Name"],
                                                    row["Company"],
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
                                                    row["Name"],
                                                    row["Company"],
                                                    row["Year"],
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

                        elif sheet_name == "Targets":
                            try:
                                logging.info("Special processing for 'Targets'")

                                df = df.drop(columns=["Unnamed:_0"])
                                print("DF", df.columns)

                                df.columns = df.columns.str.strip()
                                required_columns = [
                                    "FY",
                                    "Scope_1_Threshold_(tCO2e)",
                                    "Projected_Scope_1__(tCO2e)",
                                    "Turnover_Target",
                                    "Target_CSR_%",
                                    "Total_CSR_Budget_(IDR)",
                                    "IPRen_Fuel_Consumption_Target_(L)",
                                    "IPRen_Water_Consumption_Target_(L)",
                                ]
                                for col in required_columns:

                                    column_mapping = {
                                        "FY": "FY",
                                        "Scope_1_Threshold_(tCO2e)": "Scope1_Threshold_tCO2",
                                        "Projected_Scope_1__(tCO2e)": "Projected_Scope1t_CO2",
                                        "Turnover_Target": "TurnoverTarget",
                                        "Target_CSR_%": "TargetCSR",
                                        "Total_CSR_Budget_(IDR)": "TotalCSRBudget",
                                        "IPRen_Fuel_Consumption_Target_(L)": "IPRen_FuelConsumptionTarget(L)",
                                        "IPRen_Water_Consumption_Target_(L)": "IPRen_WaterConsumptionTarget(L)",
                                    }
                                    df.rename(columns=column_mapping, inplace=True)

                                    if "Created" in df.columns:
                                        df.drop(columns=["Created"], inplace=True)

                                    table_name = "[dbo].[Targets]"

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

                                        truncate_query = f"TRUNCATE TABLE {table_name};"
                                        cursor.execute(truncate_query)

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
                                                    row["IPRen_FuelConsumptionTarget(L)"],
                                                    row["IPRen_WaterConsumptionTarget(L)"],
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
                                                    row["IPRen_FuelConsumptionTarget(L)"],
                                                    row["IPRen_WaterConsumptionTarget(L)"],
                                                    row["FY"],
                                                    row["Company"],
                                                    row["FY"],
                                                    row["Company"],
                                                    row["Scope1_Threshold_tCO2"],
                                                    row["Projected_Scope1t_CO2"],
                                                    row["TurnoverTarget"],
                                                    row["TargetCSR"],
                                                    row["TotalCSRBudget"],
                                                    row["IPRen_FuelConsumptionTarget(L)"],
                                                    row["IPRen_WaterConsumptionTarget(L)"],
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

