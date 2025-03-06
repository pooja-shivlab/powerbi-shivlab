from Common_powerBI import *

if "Parent" in main_folder_list:
    logging.info("Processing 'Parent' folder.")
    # Process only the "Parent" folder and its subfolders
    xlsx_files = process_subfolders(
        ctx, parent_path="/sites/Dashboard-UAT/Shared%20Documents/Parent"
    )
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
        all_xlsx_files.extend(
            xlsx_files
        )  # Ensure files are mapped to correct subfolder

        dashboard_folders = get_subfolders(ctx, subfolder_path)

        sheet_status = {}
        dashboard_file_map = {}
        for dashboard in dashboard_folders:
            dashboard_path = f"{subfolder_path}/{dashboard}"
            dashboard_files = [
                file for file in xlsx_files if file.startswith(dashboard_path)
            ]
            dashboard_file_map[dashboard] = dashboard_files

            # Process all files in the current folder
            for file in xlsx_files:
                # Download the file locally
                target_file = ctx.web.get_file_by_server_relative_url(file)
                with open("local_copy.xlsx", "wb") as local_file:
                    target_file.download(local_file).execute_query()

                # Load the workbook to inspect sheet names
                xls = pd.ExcelFile("local_copy.xlsx")
                sheet_names = xls.sheet_names

                logging.info(f"Sheet names in the workbook: {sheet_names}")
                # Skip the "Preface" sheet if present
                if "Preface" in sheet_names:
                    sheet_names.remove("Preface")

                for sheet_name in sheet_names:
                    logging.info(f"Processing sheet: {sheet_name}")

                    if sheet_name in [
                        "Financial Performance",
                        "Project Timeline",
                        "Construction Timeline",
                    ]:
                        skiprows = 3
                        header = 0  # First row after skipping rows becomes header
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
                            header = [0, 1]  # Combined header from rows 3 and 4
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
                    df = pd.read_excel(
                        "local_copy.xlsx",
                        sheet_name=sheet_name,
                        skiprows=skiprows,
                        header=header,
                    )

                    # Step 6: Add inferred dashboard and company to DataFrame
                    df["Company"] = subfolder
                    df["Dashboard"] = inferred_dashboard
                    df.columns = (
                        df.columns.str.strip()
                        .str.replace(" ", "_")
                        .str.replace(r"[^a-zA-Z0-9_]", "")
                    )

                    # Step 7: Log success
                    sheet_status[(inferred_dashboard, sheet_name)] = {
                        "Status": "Success",
                        "Description": "Sheet processed successfully",
                    }

                    if sheet_name in ["Project Expenses"]:
                        # Check if the sheet requires flattening
                        if isinstance(df.columns, pd.MultiIndex):
                            # Flatten MultiIndex for specific sheets only
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

                    # Replace NaN values with 0 for numeric columns
                    df.fillna(0, inplace=True)

                    if sheet_name == "Operation Overview":
                        logging.info(
                            f"Processing sheet: {sheet_name} from Dashboard: {dashboard}"
                        )
                        try:
                            logging.info(
                                "Special processing for 'Operation Overview' sheet."
                            )
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
                                # Remove the 'Created' column if it exists
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                    logging.info(f"'Created' column removed.")

                                # Insert data into the defined table
                                table_name = "[dbo].[OperationOverview]"
                                df["Subsidiary_Name"] = df["Subsidiary_Name"].apply(
                                    lambda x: str(x) if not isinstance(x, str) else x
                                )
                                df["Subsidiary_Name"] = df["Subsidiary_Name"].apply(
                                    escape_special_characters
                                )

                                # Ensure the Project column is treated as a string
                                df["Project"] = df["Project"].apply(
                                    lambda x: str(x) if not isinstance(x, str) else x
                                )
                                # Before inserting the 'Project' column data, apply the escaping
                                df["Project"] = df["Project"].apply(
                                    escape_special_characters
                                )
                                # Insert data into the defined table
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

                                # Step 1: Check if the sheet has rows that are NOT in the database
                                existing_rows_query = f"""
                                                                       SELECT Subsidiary_Name, Project 
                                                                       FROM {table_name}
                                                                   """
                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                # Step 2: Compare with DataFrame
                                df_tuples = set(
                                    zip(df["Subsidiary_Name"], df["Project"])
                                )

                                missing_rows = df_tuples - existing_rows_set

                                if missing_rows:
                                    logging.info(
                                        "Missing rows detected. Performing TRUNCATE + INSERT."
                                    )

                                    # Step 3: Truncate the table before inserting new data
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
                            logging.info(
                                "Data successfully processed and committed for 'Operation Overview' sheet."
                            )
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
                            logging.info(
                                "Special processing for 'Env - Scope 1 & 2 Emissions'"
                            )
                            df = df.drop(columns=["Unnamed:_0"])
                            # Clean the column names to remove leading/trailing spaces
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

                                # Remove the 'Created' column if it exists
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
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
                                df_tuples = set(zip(df["Company"], df["Month"]))

                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:
                                    logging.info(
                                        "Missing rows detected. Performing TRUNCATE + INSERT."
                                    )

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

                                    logging.info(
                                        "Beginning insertion into Env-Scope1&2Emissions table."
                                    )
                                    for _, row in df.iterrows():
                                        # Define the placeholders for this row
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                # For IF EXISTS condition
                                                row["Company"],
                                                row["Month"],
                                                # For UPDATE clause
                                                row["Scope1_tCO2e"],
                                                row["Scope2_tCO2e"],
                                                row["Total_Scope1&2"],
                                                row["Company"],
                                                row["Month"],
                                                # For INSERT INTO clause
                                                row["Company"],
                                                row["Month"],
                                                row["Scope1_tCO2e"],
                                                row["Scope2_tCO2e"],
                                                row["Total_Scope1&2"],
                                            ),
                                        )
                                conn.commit()
                                logging.info(
                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully."
                                )
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
                                    zip(df["Company"], df["Month"])
                                )  # Convert df to a set of tuples
                                missing_rows = (
                                    df_tuples - existing_rows_set
                                )  # Find missing rows
                                if missing_rows:
                                    logging.info(
                                        "Missing rows detected. Performing TRUNCATE + INSERT."
                                    )
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

                                    logging.info(
                                        "Beginning insertion into ENV-Utilites table."
                                    )

                                    for _, row in df.iterrows():
                                        # Define the placeholders for this row
                                        placeholders = (
                                            # For IF EXISTS condition
                                            row["Company"],
                                            row["Month"],
                                            # For UPDATE clause
                                            row["IPRen_ElectricityUsage(Wh)"],
                                            row["IPRen_ActualWaterConsumption(m3)"],
                                            row["IPRen_ActualFuelConsumption(L)"],
                                            row["Company"],
                                            row["Month"],
                                            # For INSERT INTO clause
                                            row["Company"],
                                            row["Month"],
                                            row["IPRen_ElectricityUsage(Wh)"],
                                            row["IPRen_ActualWaterConsumption(m3)"],
                                            row["IPRen_ActualFuelConsumption(L)"],
                                        )

                                conn.commit()
                            logging.info(
                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully."
                            )
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

                                # Remove the 'Created' column if it exists
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                    logging.info(f"'Created' column removed.")

                                table_name = "[dbo].[Social-EmployeeByGender]"
                                logging.info(f"Table name set to: {table_name}")

                                existing_rows_query = f"""
                                                             SELECT Company, Month
                                                              FROM {table_name}
                                                              """

                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                # Step 2: Compare with DataFrame
                                df_tuples = set(zip(df["Company"], df["Month"]))

                                missing_rows = (
                                    df_tuples - existing_rows_set
                                )  # Find missing rows
                                if missing_rows:
                                    logging.info(
                                        "Missing rows detected. Performing TRUNCATE + INSERT."
                                    )
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

                                    logging.info(
                                        "Beginning insertion into Social-EmployeeByGender table."
                                    )
                                    for _, row in df.iterrows():
                                        # Define the placeholders for this row
                                        placeholders = (
                                            # For IF EXISTS condition
                                            row["Company"],
                                            row["Month"],
                                            # For UPDATE clause
                                            row["Total_Male"],
                                            row["Total_Female"],
                                            row["NewHire_Male"],
                                            row["NewHire_Female"],
                                            row["Turnover_Male"],
                                            row["Turnover_Female"],
                                            row["Company"],
                                            row["Month"],
                                            # For INSERT INTO clause
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
                                logging.info(
                                    f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully."
                                )
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
                                df_tuples = set(zip(df["Company"], df["Month"]))
                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:
                                    logging.info(
                                        "Missing rows detected. Performing TRUNCATE + INSERT."
                                    )
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
                                                row["Month"],  # For UPDATE clause
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

                                    logging.info(
                                        "Beginning insertion into Social-EmployeeByAge table."
                                    )
                                    for _, row in df.iterrows():
                                        placeholders = (
                                            # For IF EXISTS condition
                                            row["Company"],
                                            row["Month"],
                                            # For UPDATE clause
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
                                            # For INSERT INTO clause
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
                            logging.info(
                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully."
                            )
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
                            # Drop unnecessary columns
                            df = df.drop(columns=["Unnamed:_0", "Unnamed:_5"])
                            df.columns = df.columns.str.strip()

                            # Define required columns and rename
                            column_mapping = {
                                "Month": "Month",
                                "CSR__Project_Name": "CSR_ProjectName",
                                "CSR_Value": "CSR_Value",
                                "CSR_Disbursed": "CSR_Disbursement",
                            }
                            df.rename(columns=column_mapping, inplace=True)

                            # Remove the 'Created' column if it exists
                            if "Created" in df.columns:
                                df.drop(columns=["Created"], inplace=True)
                                logging.info(f"'Created' column removed.")

                            table_name = "[dbo].[Social-CSR]"

                            # Step 1: Check if the sheet has rows that are NOT in the database
                            existing_rows_query = f"""
                                                              SELECT Company, Month
                                                               FROM {table_name}
                                                           """

                            cursor.execute(existing_rows_query)
                            rows = cursor.fetchall()
                            existing_rows_set = {
                                tuple(row) for row in rows
                            }  # Convert rows to tuples for hashing

                            # Step 2: Compare with DataFrame
                            df_tuples = set(
                                zip(df["Company"], df["Month"])
                            )  # Convert df to a set of tuples

                            missing_rows = (
                                df_tuples - existing_rows_set
                            )  # Find missing rows

                            if missing_rows:
                                logging.info(
                                    "Missing rows detected. Performing TRUNCATE + INSERT."
                                )

                                # Step 3: Truncate the table before inserting new data
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

                            # Commit the transaction
                            conn.commit()
                            logging.info(
                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully."
                            )
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
                            # Clean the column names to remove leading/trailing spaces
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

                                # Remove the 'Created' column if it exists
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                    logging.info(f"'Created' column removed.")

                                table_name = "[dbo].[Gov-ManagementDiversity]"
                                logging.info(f"Table name set to: {table_name}")

                                existing_rows_query = f"""
                                                     SELECT Company, Month
                                                      FROM {table_name}
                                                      """

                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {tuple(row) for row in rows}

                                # Step 2: Compare with DataFrame
                                df_tuples = set(zip(df["Company"], df["Month"]))
                                missing_rows = df_tuples - existing_rows_set
                                if missing_rows:
                                    logging.info(
                                        "Missing rows detected. Performing TRUNCATE + INSERT."
                                    )

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

                                logging.info(
                                    "Beginning insertion into Gov-ManagementDiversity table."
                                )
                                for _, row in df.iterrows():
                                    placeholders = (
                                        # For IF EXISTS condition
                                        row["Company"],
                                        row["Month"],
                                        # For UPDATE clause
                                        row["Senior_Male"],
                                        row["Senior_Female"],
                                        row["Middle_Male"],
                                        row["Middle_Female"],
                                        row["Company"],
                                        row["Month"],
                                        # For INSERT INTO clause
                                        row["Company"],
                                        row["Month"],
                                        row["Senior_Male"],
                                        row["Senior_Female"],
                                        row["Middle_Male"],
                                        row["Middle_Female"],
                                    )
                            conn.commit()
                            logging.info(
                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully."
                            )
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
                            # Clean the column names to remove leading/trailing spaces
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

                                # Remove the 'Created' column if it exists
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                    logging.info(f"'Created' column removed.")
                                if "FY" in df.columns:
                                    try:
                                        df["Year"] = df["Year"].astype(int)
                                        logging.info(
                                            "Converted 'Year' column to integers."
                                        )
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
                                logging.info(
                                    f"main_folder: {main_folder}, sheet_name: {sheet_name}"
                                )

                                table_name = "[dbo].[Gov-Board]"
                                logging.info(f"Table name set to: {table_name}")
                                existing_rows_query = f"""
                                                         SELECT Name, Company, Year
                                                          FROM {table_name}
                                                        """

                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {
                                    tuple(row) for row in rows
                                }  # Convert rows to tuples for hashing
                                # Step 2: Compare with DataFrame
                                df_tuples = set(
                                    zip(df["Name"], df["Company"], df["Year"])
                                )
                                missing_rows = existing_rows_set - df_tuples
                                logging.info(f"Table name set to: {table_name}")
                                if missing_rows:
                                    logging.info(
                                        "Missing rows detected. Performing TRUNCATE + INSERT."
                                    )

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

                                    logging.info(
                                        "Beginning insertion into Gov-Board table."
                                    )

                                    # Log the DataFrame columns
                                    # print(f"Columns in DataFrame: {df.columns}")
                                    for _, row in df.iterrows():
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                # For IF EXISTS condition
                                                row["Name"],
                                                row["Company"],
                                                row["Year"],
                                                # For UPDATE clause
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
                                                # For INSERT INTO clause
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
                            logging.info(
                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully."
                            )
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
                            # Clean the column names to remove leading/trailing spaces
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
                                # if col not in df.columns:
                                #     logging.error(
                                #         f"Missing required column '{col}' in GOV-BOARD")
                                #     continue

                                # Rename columns to match the database schema if necessary
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

                                # Remove the 'Created' column if it exists
                                if "Created" in df.columns:
                                    df.drop(columns=["Created"], inplace=True)
                                    logging.info(f"'Created' column removed.")

                                table_name = "[dbo].[Targets]"
                                logging.info(f"Table name set to: {table_name}")
                                existing_rows_query = f"""
                                                                                             SELECT FY, Company
                                                                                              FROM {table_name}
                                                                                            """

                                cursor.execute(existing_rows_query)
                                rows = cursor.fetchall()
                                existing_rows_set = {
                                    tuple(row) for row in rows
                                }  # Convert rows to tuples for hashing
                                # Step 2: Compare with DataFrame
                                df_tuples = set(
                                    zip(df["FY"], df["Company"])
                                )  # Convert df to a set of tuples

                                missing_rows = (
                                    existing_rows_set - df_tuples
                                )  # Find missing rows

                                if missing_rows:
                                    logging.info(
                                        "Missing rows detected. Performing TRUNCATE + INSERT."
                                    )

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

                                    logging.info(
                                        f"Beginning insertion into {table_name}."
                                    )
                                    # Log the DataFrame columns
                                    # print(f"Columns in DataFrame: {df.columns}")
                                    for _, row in df.iterrows():
                                        # Define the placeholders for this row
                                        cursor.execute(
                                            update_insert_query,
                                            (
                                                # For IF EXISTS condition
                                                row["FY"],
                                                row["Company"],
                                                # For UPDATE clause
                                                row["Scope1_Threshold_tCO2"],
                                                row["Projected_Scope1t_CO2"],
                                                row["TurnoverTarget"],
                                                row["TargetCSR"],
                                                row["TotalCSRBudget"],
                                                row["IPRen_FuelConsumptionTarget(L)"],
                                                row["IPRen_WaterConsumptionTarget(L)"],
                                                row["FY"],
                                                row["Company"],
                                                # For INSERT INTO clause
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
                            logging.info(
                                f"Data from sheet '{sheet_name}' inserted into table '{table_name}' successfully."
                            )
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
                    INSERT INTO [dbo].[OperationESGDataLog] ([CompanyName], [Dashboard], [ModifiedDate], [SheetName], [Status], [Description])
                    VALUES (?, ?, GETDATE(), ?, ?, ?)
                """
            cursor.execute(
                insert_log_query,
                (subfolder, dashboard, sheet, status["Status"], status["Description"]),
            )
            conn.commit()
