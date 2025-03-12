from Common_powerBI import *

def MasterConfigurationFunctionIndex():
    logging.info("Processing 'Dashboard Configuration Master List' workbook")
    config_file_name = "Dashboard%20Configuration%20Master%20List.xlsx"
    config_relative_file_url = f"/sites/Dashboard-UAT/Shared%20Documents/{config_file_name}"
    try:
        decoded_configfilename = unquote(config_relative_file_url)
        print(f'filtered_files is {decoded_configfilename}')
        file_content = File.open_binary(ctx, decoded_configfilename)

        logging.info(f"config file content is {file_content}")
        uploadToBlobStorage(file_content, "config_local_copy.xlsx")
        logging.info("upload to blob config file successfully")
        xls = pd.ExcelFile(config_local_copy_sas_url)
        config_sheet_names = xls.sheet_names
        logging.info(f"Sheet names in the workbook: {config_sheet_names}")

        if "Preface" in config_sheet_names:
            config_sheet_names.remove("Preface")

        for sheet_name in sheet_to_table_map_config.keys():
            if sheet_name not in config_sheet_names:
                logging.warning(
                    f"Sheet '{sheet_name}' not found in 'Dashboard Configuration Master List' workbook. Skipping."
                )
                continue
            df = pd.read_excel(
                config_local_copy_sas_url, sheet_name=sheet_name, skiprows=4, header=0
            )
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].str.strip()
            df.rename(columns={"Subsidiary Name": "SubsidiaryName"}, inplace=True)
            df["Source"] = sheet_name

            if "Source" in df.columns:
                df = df.drop(columns=["Source"])
            df.columns = (
                df.columns.str.strip()
                .str.replace(" ", "_")
                .str.replace(r"[^a-zA-Z0-9_]", "")
            )

            df.fillna(0, inplace=True)

            if "Created" in df.columns:
                df.drop(columns=["Created"], inplace=True)

            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()

            if sheet_name == "Subsidiary List":
                logging.info("Special processing for Subsidiary list")
                table_name = sheet_to_table_map_config[sheet_name]

                # Fetch existing records from SQL
                select_query = f"SELECT SubsidiaryName, InvestmentAccountName FROM {table_name}"
                cursor.execute(select_query)

                # Convert each row into a tuple explicitly
                existing_rows_set = set(tuple(row) for row in cursor.fetchall())

                # Create a set of new records from the Excel sheet
                df_tuples = set(df[['SubsidiaryName', 'InvestmentAccountName']].apply(tuple, axis=1))

                # Identify missing rows (exist in SQL but not in the new sheet)
                missing_rows = existing_rows_set - df_tuples

                if missing_rows:
                    logging.info(f"Missing rows detected. Truncating and reinserting into {table_name}.")

                    # Truncate the table before reinserting all rows
                    truncate_query = f"TRUNCATE TABLE {table_name};"
                    cursor.execute(truncate_query)

                # Now proceed with the insert/update logic
                if sheet_name == "Subsidiary List":
                    logging.info("Special processing for Subsidiary list")

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
                            str(row["SubsidiaryName"]),
                            str(row["InvestmentAccountName"]),
                            str(row["Abbreviation"]),
                            str(row["IncomeAccountName"]),
                            str(row["SubsidiaryName"]),
                            str(row["InvestmentAccountName"]),
                            str(row["SubsidiaryName"]),
                            str(row["Abbreviation"]),
                            str(row["InvestmentAccountName"]),
                            str(row["IncomeAccountName"]),
                        )
                        try:
                            cursor.execute(if_exists_query, placeholders)
                        except Exception as e:
                            logging.error(f"Failed to execute query for row {row}: {e}")

                else:
                    logging.info("Processing other tables")

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
                        placeholders = (row["InvestmentAccount"], row["InvestmentAccount"])
                        try:
                            cursor.execute(insert_query, placeholders)
                        except Exception as e:
                            logging.error(f"Failed to execute query for row {row}: {e}")

                # Commit changes and close connection
                conn.commit()
                conn.close()
    except Exception as e:
        logging.error(
            f"Error processing 'Dashboard Configuration Master List' workbook: {e}"
        )
