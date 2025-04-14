import pyodbc
import logging
import shutil
import calendar
import logging
import os
import re
import pandas as pd
import pyodbc
import logging
import shutil
import calendar
import os
from datetime import datetime, date
import json

# Set up logging
logging.basicConfig(filename='import_log.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

# Get the Azure SQL Database connection details from environment variables
server = os.getenv('AZURE_SQL_SERVER')
database = os.getenv('AZURE_SQL_DATABASE')
# username = os.getenv('AZURE_SQL_USERNAME')
# password = os.getenv('AZURE_SQL_PASSWORD')
username = os.getenv('AZURE_CLIENT_ID')
password = os.getenv('AZURE_SECRET')
driver = '{ODBC Driver 18 for SQL Server}'

# Set up logging with the current date as the log file name
log_file_name = datetime.now().strftime('%Y-%m-%d') + '.log'
logging.basicConfig(filename=log_file_name, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def push_billing_account_cost_csv_to_sql(file_path, conn):
    try:
        logging.info(f"Starting to process file: {file_path}")
        print(f"Starting to process file: {file_path}")
        
        # Start a transaction
        cursor = conn.cursor()
        # cursor.execute("BEGIN TRANSACTION")
        # logging.info("Transaction started.")

        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        logging.info(f"CSV file {file_path} read successfully.")
        print(f"CSV file {file_path} read successfully.")

        # Extract month and year from the file path
        file_parts = file_path.split(os.sep)
        year = file_parts[-3]  # Assuming the year is the third last folder
        month_name = file_parts[-2]  # Assuming the month is the second last folder

        # Replace NaN with an empty string
        df['SubscriptionName'] = df['SubscriptionName'].fillna('')
        df['SubscriptionId'] = df['SubscriptionId'].fillna('')
        
        # Add Month, Year, and Date columns to the DataFrame
        df['Month'] = month_name
        df['Year'] = year
        month_number = list(calendar.month_abbr).index(month_name.capitalize())
        first_date_of_given_month = date(date.today().year, month_number, 1)
        df['Date'] = first_date_of_given_month      

        # Rename columns to match the database table schema
        df.rename(columns={
            'SubscriptionName': 'SubscriptionName',
            'SubscriptionId': 'SubscriptionId',
            'TotalCost (Including Other Azure Resources)': 'TotalCost'
        }, inplace=True)

        # # Debugging: Print the first few rows of the DataFrame
        # print(df.head())

        # Insert data into the SQL table in blocks of 5000 rows
        row_count = 0
        for index, row in df.iterrows():
            # # Print the row for logging
            # print(f"Inserting row {index} : {row}")

            # Prepare the query and parameters
            query = """
                INSERT INTO BillingAccountCost (SubscriptionName, SubscriptionId, TotalCost, Month, Year, Date)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            params = (row['SubscriptionName'], row['SubscriptionId'], row['TotalCost'], row['Month'], row['Year'], row['Date'])

            # # Print and log the query and parameters for debugging
            # print(f"Executing query: {query}")
            # print(f"With parameters: {params}")
            # logging.debug(f"Executing query: {query}")
            # logging.debug(f"With parameters: {params}")

            try:
                # Execute the query
                cursor.execute(query, params)
                row_count += 1
                if row_count % 5000 == 0:
                    conn.commit()
                    logging.info(f"Committed {row_count} rows")
                    print(f"Committed {row_count} rows")
            except Exception as sql_error:
                logging.error(f"SQL error while inserting row {index}: {sql_error}")
                print(f"SQL error while inserting row {index}: {sql_error}")
                raise        

        # Commit any remaining rows
        if row_count % 5000 != 0:
            conn.commit()
            logging.info(f"Committed {row_count % 5000} rows")
            print(f"Committed {row_count % 5000} rows")

        logging.info("Transaction committed successfully.")
        print("Transaction committed successfully.")
        logging.info(f"Data from {file_path} successfully inserted into BillingAccountCost table.")
        print(f"Data from {file_path} successfully inserted into BillingAccountCost table.")
    except Exception as e:
        # Rollback the transaction in case of an error
        conn.rollback()
        logging.error(f"Error processing file {file_path}: {e}")
        print(f"Error processing file {file_path}: {e}")
        raise

def push_subscription_cost_csv_to_sql(file_path, conn):
    try:
        logging.info(f"Starting to process file: {file_path}")
        print(f"Starting to process file: {file_path}")
        
        # Start a transaction
        cursor = conn.cursor()

        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        logging.info(f"CSV file {file_path} read successfully.")
        print(f"CSV file {file_path} read successfully.")

        # Extract month and year from the file path
        file_parts = file_path.split(os.sep)
        year = file_parts[-3]  # Assuming the year is the third last folder
        month_name = file_parts[-2]  # Assuming the month is the second last folder

        # Replace NaN with an empty string
        df['SubscriptionName'] = df['SubscriptionName'].fillna('')
        df['SubscriptionId'] = df['SubscriptionId'].fillna('')
        
        # Add Month, Year, and Date columns to the DataFrame
        df['Month'] = month_name
        df['Year'] = year
        month_number = list(calendar.month_abbr).index(month_name.capitalize())
        first_date_of_given_month = date(date.today().year, month_number, 1)
        df['Date'] = first_date_of_given_month

        # Rename columns to match the database table schema
        df.rename(columns={
            'SubscriptionName': 'SubscriptionName',
            'SubscriptionId': 'SubscriptionId',
            'TotalCost': 'AzureCost',
            'ResourceCount': 'ResourceCount'
        }, inplace=True)

        # # Debugging: Print the first few rows of the DataFrame
        # print(df.head())

        # Insert data into the SQL table in blocks of 5000 rows
        row_count = 0
        for index, row in df.iterrows():
            # # Print the row for logging
            # print(f"Inserting row {index} : {row}")

            # Prepare the query and parameters
            query = """
                INSERT INTO SubscriptionCost (SubscriptionName, SubscriptionId, AzureCost, ResourceCount, Month, Year, Date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            params = (row['SubscriptionName'], row['SubscriptionId'], row['AzureCost'], row['ResourceCount'], row['Month'], row['Year'], row['Date'])

            # # Print and log the query and parameters for debugging
            # print(f"Executing query: {query}")
            # print(f"With parameters: {params}")
            # logging.debug(f"Executing query: {query}")
            # logging.debug(f"With parameters: {params}")

            try:
                # Execute the query
                cursor.execute(query, params)
                row_count += 1
                if row_count % 5000 == 0:
                    conn.commit()
                    logging.info(f"Committed {row_count} rows")
                    print(f"Committed {row_count} rows")
            except Exception as sql_error:
                logging.error(f"SQL error while inserting row {index}: {sql_error}")
                print(f"SQL error while inserting row {index}: {sql_error}")
                raise        

        # Commit any remaining rows
        if row_count % 5000 != 0:
            conn.commit()
            logging.info(f"Committed {row_count % 5000} rows")
            print(f"Committed {row_count % 5000} rows")

        logging.info("Transaction committed successfully.")
        print("Transaction committed successfully.")
        logging.info(f"Data from {file_path} successfully inserted into SubscriptionCost table.")
        print(f"Data from {file_path} successfully inserted into SubscriptionCost table.")
    except Exception as e:
        # Rollback the transaction in case of an error
        conn.rollback()
        logging.error(f"Error processing file {file_path}: {e}")
        print(f"Error processing file {file_path}: {e}")
        raise

# Save the last processed file and row in the ProcessCheckpoint table after each commit
def save_checkpoint(conn, file_path, last_row):
    try:
        cursor = conn.cursor()
        query = """
            INSERT INTO ProcessCheckpoint (LastProcessedFile, LastProcessedRow)
            VALUES (?, ?)
        """
        cursor.execute(query, (file_path, last_row))
        conn.commit()
        logging.info(f"Checkpoint saved: File={file_path}, Row={last_row}")
        print(f"Checkpoint saved: File={file_path}, Row={last_row}")
    except Exception as e:
        logging.error(f"Error saving checkpoint: {e}")
        print(f"Error saving checkpoint: {e}")
        raise

# Retrieve the last processed file and row from the ProcessCheckpoint table when the script starts
def get_last_checkpoint(conn):
    try:
        cursor = conn.cursor()
        query = """
            SELECT TOP 1 LastProcessedFile, LastProcessedRow
            FROM ProcessCheckpoint
            ORDER BY Id DESC
        """
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return result[0], result[1]  # File path, last processed row
        return None, 0  # No checkpoint found
    except Exception as e:
        logging.error(f"Error retrieving checkpoint: {e}")
        raise        

def extract_subscription_name(full_name):
    # Extract the subscription name without the ID in parentheses
    # Find the index of the last opening parenthesis
    last_opening_parenthesis = full_name.rfind('(')

    # If an opening parenthesis is found, return the substring before it
    if last_opening_parenthesis != -1:
        return full_name[:last_opening_parenthesis].strip()
    
    # If no opening parenthesis is found, return the full name
    return full_name

def push_azure_resource_cost_json_to_sql(json_file_path, conn, start_row):
    try:
        # Read the JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        logging.info(f"JSON file {json_file_path} read successfully.")
        print(f"JSON file {json_file_path} read successfully.")

        # Prepare the SQL query
        query = """
            INSERT INTO AzureResourceCost (
                SubscriptionName, SubscriptionId, ResourceGroup, ResourceName, ResourceID, ConsumedService, MeterCategory, 
                MeterSubcategory, Location, BillingMonth, CostCenter, Cost
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Insert each record into the database
        cursor = conn.cursor()
        # If start_row is greater than 0, it means we are resuming from a checkpoint
        if start_row > 0:
            #
            row_count = start_row
        else:
            row_count = 0
        for index, record in enumerate(data):
            if index < start_row:
                continue  # Skip already processed rows          
            try:
                # Extract fields from the JSON record
                cost = record[0]
                subscription_name = extract_subscription_name(record[1])
                subscription_id = record[1].split('(')[-1].strip(')')
                resource_group = record[2]
                resource_name = record[3].split('/')[-1]
                resource_id = record[3]
                resource_type = record[4]
                meter_category = record[6]
                meter_subcategory = record[5]
                location = record[7]
                billing_month = record[8]
                cost_center = record[10]

                # Prepare the parameters
                params = (
                    subscription_name, subscription_id, resource_group, resource_name, resource_id, resource_type, meter_category,
                    meter_subcategory, location, billing_month, cost_center, str(cost)
                )

                # Execute the query
                cursor.execute(query, params)
                row_count += 1

                # Commit in batches of 5000 rows
                if row_count % 5000 == 0:
                    conn.commit()
                    logging.info(f"Committed {row_count} rows.")
                    print(f"Committed {row_count} rows.")

                    # Save the checkpoint
                    save_checkpoint(conn, json_file_path, row_count)
                    
            except Exception as sql_error:
                logging.error(f"SQL error while inserting record: {sql_error}")
                print(f"SQL error while inserting record: {sql_error}")
                raise

        # Commit any remaining rows
        if row_count % 5000 != 0:
            conn.commit()
            logging.info(f"Committed {row_count % 5000} rows.")
            print(f"Committed {row_count % 5000} rows.")

            # Save the checkpoint
            save_checkpoint(conn, json_file_path, row_count)
        
        logging.info(f"Data from {json_file_path} successfully inserted into AzureResourceCost table.")
        print(f"Data from {json_file_path} successfully inserted into AzureResourceCost table.")
        logging.info(f"Total rows inserted: {row_count}")
        print(f"Total rows inserted: {row_count}")

        return row_count
    except Exception as e:
        logging.error(f"Error processing file {json_file_path}: {e}")
        print(f"Error processing file {json_file_path}: {e}")
        raise    

def main():
    try:
        # Establish the connection       
        # conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}')
        conn = pyodbc.connect(f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password};;Authentication=ActiveDirectoryServicePrincipal;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
        # cursor = conn.cursor()
        logging.info("Connection to Azure SQL Database established successfully.")
        print("Connection to Azure SQL Database established successfully.")
    
        # Loop through the folders: output/year/month
        base_folder = 'output'
        processed_folder = 'processed'

        # Ensure the processed folder exists
        if not os.path.exists(processed_folder):
            os.makedirs(processed_folder)

        # Resume from the last checkpoint 
        last_file, last_row = get_last_checkpoint(conn)
        logging.info(f"Resuming from checkpoint: File={last_file}, Row={last_row}")
        print(f"Resuming from checkpoint: File={last_file}, Row={last_row}")

        # Create a mapping of month abbreviations to their order
        month_order = {month: index for index, month in enumerate(calendar.month_abbr) if month}

        for year_folder in sorted(os.listdir(base_folder)):  # Sort year folders
            year_path = os.path.join(base_folder, year_folder)
            if os.path.isdir(year_path):  # Check if it's a directory
                # Sort month folders based on the month_order mapping
                for month_folder in sorted(os.listdir(year_path), key=lambda m: month_order.get(m[:3], 0)):
                    month_path = os.path.join(year_path, month_folder)
                    if os.path.isdir(month_path):  # Check if it's a directory
                        for file in sorted(os.listdir(month_path)):  # Sort files
                            file_path = os.path.join(month_path, file)
                            
                            # # Process only files that start with 'billing_account_cost_summary'
                            # if os.path.isfile(file_path):
                            #     try:
                            #         if file.startswith('billing_account_cost_summary'):
                            #             # Process the file (you can add your processing logic here)
                            #             logging.info(f"Processing file: {file_path}")
                            #             print(f"Processing file: {file_path}")

                            #             push_billing_account_cost_csv_to_sql(file_path, conn)
                                        
                            #             # # Move the processed file to the processed folder
                            #             # processed_file_path = os.path.join(processed_folder, file)
                            #             # shutil.move(file_path, processed_file_path)
                            #             logging.info(f"Moved file {file} to {processed_folder}")
                            #             print(f"Moved file {file} to {processed_folder}")
                            #     except Exception as e:
                            #         logging.error(f"Error processing file {file_path}: {e}")
                            #         raise

                            # # Process only files that start with 'subscription_cost_summary'
                            # if file.startswith('subscription_cost_summary'):
                            #     try:                                    
                            #         # Process the file (you can add your processing logic here)
                            #         logging.info(f"Processing file: {file_path}")
                            #         print(f"Processing file: {file_path}")

                            #         push_subscription_cost_csv_to_sql(file_path, conn)
                                    
                            #         # # Move the processed file to the processed folder
                            #         # processed_file_path = os.path.join(processed_folder, file)
                            #         # shutil.move(file_path, processed_file_path)
                            #         logging.info(f"Moved file {file} to {processed_folder}")
                            #         print(f"Moved file {file} to {processed_folder}")
                            #     except Exception as e:
                            #         logging.error(f"Error processing file {file_path}: {e}")
                            #         raise     

                            # Process only files that start with 'azure_cost_data'
                            if file.startswith('azure_cost_data'):
                                # Skip files that have already been processed
                                if last_file and os.path.abspath(file_path) < os.path.abspath(last_file):
                                    continue

                                try:                                    
                                    # Process the file (you can add your processing logic here)
                                    logging.info(f"Processing file: {file_path}")
                                    print(f"Processing file: {file_path}")

                                    # push_azure_resource_cost_json_to_sql(file_path, conn)

                                    # Process the file & save the checkpoint
                                    row_count = push_azure_resource_cost_json_to_sql(file_path, conn, last_row if file_path == last_file else 0)

                                    last_row = 0  # Reset last_row for the next file
                                    
                                    # # Move the processed file to the processed folder
                                    # processed_file_path = os.path.join(processed_folder, file)
                                    # shutil.move(file_path, processed_file_path)
                                    logging.info(f"Moved file {file} to {processed_folder}")
                                    print(f"Moved file {file} to {processed_folder}")
                                except Exception as e:
                                    logging.error(f"Error processing file {file_path}: {e}")
                                    raise                                                         
        
        # Close the database connection
        conn.close()        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()