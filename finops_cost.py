import os
import requests
import csv
from datetime import datetime, timedelta
import calendar
import json
import time
import re
# Import custom modules
import logger

# Define the necessary variables
tenant_id = os.getenv('TENANT_ID')
client_id = os.getenv('FINOPS_AZURE_CLIENT_ID')
client_secret = os.getenv('FINOPS_AZURE_CLIENT_SECRET')
billing_account = os.getenv('BILLING_ACCOUNT')

# Specify the month for which you want to get the total cost
output_dir = 'output'
# File containing the total cost (including Azure marketplace, reservation cost) summary of subscriptions
billing_account_cost_summary_csv_file = 'billing_account_cost_summary'
# File containing the cost of Azure services (doesn't include Azure marketplace, reservation cost) in a subscription  
subscription_cost_summary_csv_file = "subscription_cost_summary"

next_link_file = 'next_link.txt'

# Function to obtain a new OAuth 2.0 token
def get_access_token(tenant_id, client_id, client_secret):
    token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://management.azure.com/'
    }
    token_r = requests.post(token_url, data=token_data)
    return token_r.json().get('access_token')

# Function to sanitize file names by removing invalid characters
def sanitize_filename(name):
    return re.sub(r'[\/:*?"<>|]', '_', name)

# Function to query cost data for each subscription in a billing account
def query_cost_by_subscription_in_billing_account(billing_account, access_token, start_date, end_date):
    cost_management_url = f'https://management.azure.com/providers/Microsoft.Billing/billingAccounts/{billing_account}/providers/Microsoft.CostManagement/query?api-version=2021-10-01'

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Define the request body to group by SubscriptionId
    grouping = [
        {"type": "Dimension", "name": "SubscriptionId"},
        {"type": "Dimension", "name": "SubscriptionName"},
    ]

    body = {
        "type": "ActualCost",
        "dataSet": {
            "granularity": "None",
            "aggregation": {
                "totalCost": {
                    "name": "Cost",
                    "function": "Sum"
                },
            },
            "sorting": [
            {
                "direction": "descending",
                "name": "Cost"
            }
            ],
            "grouping": grouping
        },
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date,
            "to": end_date
        }
    }

    response = requests.post(cost_management_url, headers=headers, json=body)
    # response.raise_for_status()
    return response.json()

# Function to make the request with pagination and retry logic
def get_cost_data_with_pagination_retries(year, month, subscription_id, access_token, log_file, json_file, next_link_file, max_retries=10, backoff_factor=1):
    cost_data = []
    # Define the API endpoint with subscription scope
    cost_management_url = f'https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2021-10-01'

    # Calculate the start and end dates for the specified month
    start_date = datetime(year, month, 1).strftime('%Y-%m-%d')
    end_date = datetime(year, month, calendar.monthrange(year, month)[1]).strftime('%Y-%m-%d')

    # Set up the headers
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'ClientType': 'CPS-Dashboard',
        'X-Ms-Command-Name': 'CostAnalysis' # Added header due to error 429 https://learn.microsoft.com/en-us/answers/questions/1340993/exception-429-too-many-requests-for-azure-cost-man
    }

    # Define the request body to include specific tag keys
    tag_keys = ['costcenter']  # Example tag keys

    grouping = [
        {"type": "Dimension", "name": "SubscriptionName"},
        {"type": "Dimension", "name": "ResourceGroup"},
        {"type": "Dimension", "name": "ResourceId"},
        {"type": "Dimension", "name": "ConsumedService"},    
        {"type": "Dimension", "name": "MeterSubcategory"},  
        {"type": "Dimension", "name": "MeterCategory"},  
        {"type": "Dimension", "name": "ResourceLocation"},  
        {"type": "Dimension", "name": "BillingMonth"},        
    ]

    # Add tag keys to the grouping
    for tag_key in tag_keys:
        grouping.append({"type": "TagKey", "name": tag_key})

    # Define the request body to include resource and tag details
    body = {
        "type": "Usage",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date,
            "to": end_date
        },
        "dataset": {
            "granularity": "None",
            "aggregation": {
                "totalCost": {
                    "name": "PreTaxCost",
                    "function": "Sum"
                }
            },
            "grouping": grouping
        }
    }

    def make_request(url, body):
        for attempt in range(max_retries):
            response = requests.post(url, headers=headers, json=body)
            if response.status_code == 429:
                # Too many requests, apply exponential backoff
                # wait_time = backoff_factor * (1 ** attempt)
                wait_time = 20
                print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                # Print specific headers related to rate limiting
                rate_limit_headers = [
                    'x-ms-ratelimit-microsoft.costmanagement-qpu-retry-after',
                    'x-ms-ratelimit-microsoft.costmanagement-entity-retry-after',
                    'x-ms-ratelimit-microsoft.costmanagement-tenant-retry-after',
                    'x-ms-ratelimit-microsoft.costmanagement-client-retry-after'
                ]
                for header in rate_limit_headers:
                    if header in response.headers:
                        print(f"{header}: {response.headers[header]}")
                with open(log_file, 'a') as log:
                    log.write(f"Rate limit exceeded. Retrying in {wait_time} seconds...\n")
                time.sleep(wait_time)
            elif response.status_code == 401 and "ExpiredAuthenticationToken" in response.text:
                # Refresh the access token if expired
                headers['Authorization'] = f'Bearer {get_access_token()}'
                print("Access token expired. Obtained new token")
                logger.log_note("Access token expired. Obtained new token")
                with open(log_file, 'a') as log:
                    log.write("Access token expired. Obtained new token.\n")
            else:
                return response
        raise Exception("Max retries exceeded")

    # Initial request to check if pagination is needed
    response = make_request(cost_management_url, body)
    if response.status_code == 200:
        data = response.json()
        cost_data.extend(data['properties']['rows'])
        next_link = data.get('properties', {}).get('nextLink')

        # Write the data to JSON file incrementally
        with open(json_file, 'w') as f:
            json.dump(cost_data, f, indent=4)

        # If there's a next link, continue with pagination
        while next_link:
            response = make_request(next_link, body)
            if response.status_code == 200:
                data = response.json()
                cost_data.extend(data['properties']['rows'])
                print(f"Processed {len(cost_data)} resources so far...")
                with open(log_file, 'a') as log:
                    log.write(f"Processed {len(cost_data)} resources so far...\n")

                # Write the data to JSON file incrementally
                with open(json_file, 'w') as f:
                    json.dump(cost_data, f, indent=4)

                next_link = data.get('properties', {}).get('nextLink')

                # Save the nextLink to a file
                with open(next_link_file, 'w') as file:
                    if next_link:
                        file.write(next_link)
                    else:
                        file.write('')
            else:
                print(f"Failed to retrieve cost data. Status Code: {response.status_code}")
                logger.log_note(f"Failed to retrieve cost data. Status Code: {response.status_code}")
                with open(log_file, 'a') as log:
                    log.write(f"Failed to retrieve cost data. Status Code: {response.status_code}\n")
                    log.write(response.text + "\n")
    else:
        print(f"Failed to retrieve cost data. Status Code: {response.status_code}")
        logger.log_note(f"Failed to retrieve cost data. Status Code: {response.status_code}")
        with open(log_file, 'a') as log:
            log.write(f"Failed to retrieve cost data. Status Code: {response.status_code}\n")
            log.write(response.text + "\n")

    # Clean up the nextLink file after successful completion
    if os.path.exists(next_link_file):
        os.remove(next_link_file)

    return cost_data

# # Function to write cost data to CSV
# def write_cost_data_to_csv(json_file, csv_file):
#     with open(json_file, 'r') as f:
#         cost_data = json.load(f)

#     with open(csv_file, 'w', newline='') as csvfile:
#         fieldnames = ['SubscriptionName', 'ResourceGroup', 'ResourceID', 'ConsumedService', 'MeterSubcategory', 'MeterCategory', 'ResourceLocation', 'TotalCost', 'CostCenter', 'Tags', 'BillingMonth']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         # Write the header
#         writer.writeheader()
#         # Write the data        
#         for item in cost_data:
#             row = {
#                 'SubscriptionName' : item[1],
#                 'ResourceGroup': item[2],
#                 'ResourceID': item[3],
#                 'ConsumedService': item[4], 
#                 'MeterSubcategory': item[5], 
#                 'MeterCategory': item[6], 
#                 'ResourceLocation': item[7],
#                 'TotalCost': f"{item[0]:.2f}", # truncate to 2 decimal places  
#                 'CostCenter': item[11], 
#                 'Tags': item[9], 
#                 'BillingMonth': item[8]
#             }
#             writer.writerow(row)  

def extract_subscription_name(full_name):
    # # Extract the subscription name without the ID in parentheses
    # if '(' in full_name:
    #     return full_name.split('(')[0].strip()
    # return full_name

    # Find the index of the last opening parenthesis
    last_open_bracket_index = full_name.rfind('(')
    
    # If an opening parenthesis is found, return the substring up to that index
    if last_open_bracket_index != -1:
        return full_name[:last_open_bracket_index].strip()
    
    # If no opening parenthesis is found, return the original string
    return full_name

def write_monthly_summary_billing_account_to_csv(data, csv_file):
    # Define the CSV headers
    headers = ['SubscriptionName', 'SubscriptionId', 'TotalCost (Including Other Azure Resources)', 'Curency']

    # Write data to CSV
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for row in data:            
            subscription_name = extract_subscription_name(row[2])
            subscription_id = row[1]
            total_cost = f"{row[0]:.2f}" # truncate to 2 decimal places 
            currency = row[3]
            writer.writerow([subscription_name, subscription_id, total_cost, currency])             


def process_monthly_costs(year, month): 
    try:
        # Log the start of the process    
        logger.log_note('*** Job initiated at ' + str(datetime.today()) + ' ***')        
        # Check if the client secret is available
        if client_secret is None:
            raise ValueError("The environment variable 'AZURE_CLIENT_SECRET' is not set.")
        
        print("Process initiated..")
        logger.log_note("Process initiated..")
        access_token = get_access_token(tenant_id, client_id, client_secret)
        print("Access token retrieved.")
        logger.log_note("Access token retrieved.")

        # Calculate the start and end dates for the specified month
        start_date = datetime(year, month, 1).strftime('%Y-%m-%d')
        end_date = datetime(year, month, calendar.monthrange(year, month)[1]).strftime('%Y-%m-%d')
        month_name = datetime(year, month, 1).strftime('%b')

        # Ensure the output directory exists  
        output_dir = os.path.join('output', str(year), month_name)      
        os.makedirs(output_dir, exist_ok=True)    

        # Get the summary of the cost for the billing account at the subscription level
        # This will have the cost including "Other Azure Resources" 
        print(f"Getting the total cost of each subscription for the year: {year} month: {month_name}")
        logger.log_note(f"Getting the total cost of each subscription for the year: {year} month: {month_name}")
        monthly_summary_billing_account = query_cost_by_subscription_in_billing_account(billing_account, access_token, start_date, end_date)

        # Write the json data to CSV file
        monthly_summary_billing_account_data = []
        monthly_summary_billing_account_data.extend(monthly_summary_billing_account['properties']['rows'])

        # Prepare a billing_account_summary_csv file with month & year containing the Subscription cost for the enrollement account
        billing_account_summary_csv = os.path.join(output_dir, f'{billing_account_cost_summary_csv_file}_{month_name}_{year}.csv')
        file_exists = os.path.isfile(billing_account_summary_csv)        
        # Write the summary of the cost for the billing account at the subscription level to CSV
        print(f"Writing the summary of the cost for the billing account at the subscription level to CSV: {billing_account_summary_csv}")
        logger.log_note(f"Writing the summary of the cost for the billing account at the subscription level to CSV: {billing_account_summary_csv}")        
        write_monthly_summary_billing_account_to_csv(monthly_summary_billing_account_data, billing_account_summary_csv)        
          
        # Prepare subscription cost summary file name with month, year. This cost summary contains the subscription cost only of Azure services & excludes the "Other Azure Services" like Marketplace, Reservations, etc
        subscription_cost_summary_csv = os.path.join(output_dir,f'{subscription_cost_summary_csv_file}_{month_name}_{year}.csv')
        file_exists = os.path.isfile(subscription_cost_summary_csv)
        
        # Loop through each of the subscriptions in billing_account_summary_csv
        print(f"Looping through each of the subscriptions in {billing_account_summary_csv}")
        logger.log_note(f"Looping through each of the subscriptions in {billing_account_summary_csv}")
        # Prepare a summary CSV file containing the Subscription cost for Azure services (without including the "Other Azure Services" like Marketplace, Reservations, etc)
        print(f"Writing the summary CSV file containing the Subscription cost for Azure services (without including the 'Other Azure Services' like Marketplace, Reservations, etc) to CSV: {subscription_cost_summary_csv_file}_{month_name}_{year}.csv")
        logger.log_note(f"Writing the summary CSV file containing the Subscription cost for Azure services (without including the 'Other Azure Services' like Marketplace, Reservations, etc) to CSV: {subscription_cost_summary_csv_file}_{month_name}_{year}.csv")            
        with open(subscription_cost_summary_csv, 'a', newline='') as summary_file:
            summary_writer = csv.writer(summary_file)
            # Write header only if the file doesn't exist
            if not file_exists:
                summary_writer.writerow(['SubscriptionId', 'SubscriptionName', 'TotalCost', 'ResourceCount'])   

            # Read the subscription list from billing_account_summary_csv CSV file
            with open(billing_account_summary_csv, 'r') as csvfile:
                reader = csv.DictReader(csvfile)      
                for row in reader:
                    subscription_name = row['SubscriptionName']
                    subscription_id = row['SubscriptionId']
                    subscription_cost = float(row["TotalCost (Including Other Azure Resources)"])

                    # Check if the SubscriptionId is empty or null
                    if subscription_id and subscription_cost > 1:
                        # Sanitize the subscription name for file names
                        subscription_name = sanitize_filename(subscription_name)

                        # Prepare file names with month, year, and subscription name
                        json_file = os.path.join(output_dir, f'azure_cost_data_{subscription_name}_{month_name}{year}.json')
                        # csv_file = os.path.join(output_dir, f'azure_cost_data_{subscription_name}_{month_name}{year}.csv')
                        log_file = os.path.join(output_dir, f'process_log_{subscription_name}.txt')
                        next_link_file = os.path.join(output_dir, f'next_link_{subscription_name}.txt')

                        print(f"Retrieving all cost data of the month {month_name}, {year} for subscription {subscription_name} to json file {json_file}.")
                        logger.log_note(f"Retrieving all cost data of the month {month_name}, {year} for subscription {subscription_name} to json file {json_file}.")
                        # Retrieve all cost data with retries
                        all_cost_data = get_cost_data_with_pagination_retries(year, month, subscription_id, access_token, log_file, json_file, next_link_file)

                        # Output the total number of records & total cost for the subscription
                        print(f"Total number of cost records in subscription {subscription_name}: {len(all_cost_data)}") 
                        logger.log_note(f"Total number of cost records in subscription {subscription_name}: {len(all_cost_data)}") 
                        # Calculate total cost
                        total_cost = sum(float(entry[0]) for entry in all_cost_data)  
                        print(f"Total cost of subscription {subscription_name}: {total_cost:.2f}") 
                        logger.log_note(f"Total cost of subscription {subscription_name}: {total_cost:.2f}") 
                        # Output the completion of cost data into json file
                        print(f"Cost data for subscription {subscription_name} has been written to {json_file}")
                        logger.log_note(f"Cost data for subscription {subscription_name} has been written to {json_file}")
                        # Output the completion of the cost data for the subscription
                        print(f"Cost data retrieval completed successfully for subscription {subscription_name}")
                        logger.log_note(f"Cost data retrieval completed successfully for subscription {subscription_name}")
                        # Log the details into subscription log file
                        with open(log_file, 'a') as log:
                            log.write(f"Total number of cost records in subscription {subscription_name}: {len(all_cost_data)}\n")
                            log.write(f"Total cost of subscription {subscription_name}: {total_cost:.2f}\n")                                          
                            log.write(f"Cost data has been written to {json_file}\n")                              
                            log.write(f"Cost data retrieval completed successfully for subscription {subscription_name}.\n")

                        # Write summary data to CSV
                        summary_writer.writerow([subscription_id, subscription_name, f"{total_cost:.2f}", len(all_cost_data)]) 
                        # # Write the JSON data to CSV
                        # print(f"Write cost data to csv file {csv_file}..")
                        # write_cost_data_to_csv(json_file, csv_file)
                        # # Log CSV completion
                        # print(f"Cost data has been written to {csv_file}")
                        # with open(log_file, 'a') as log:
                        #     log.write(f"Cost data has been written to {csv_file}\n")                           

                    else:
                        # Don't process subscription which has cost less than 1$ but write them in the subscrption_cost_summary_ file
                        # Write the empty SubscriptionId data to CSV as it contains some negative charges as shown in the Azure portal
                        summary_writer.writerow([subscription_id, subscription_name, f"{total_cost:.2f}", len(all_cost_data)])                          

                # Output the summary of processing of cost data for all subscriptions  
                print(f"The summary of cost data for all subscriptions is for {month_name}, {year} is available at: {subscription_cost_summary_csv}")     

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        logger.log_note(f"HTTP error occurred: {http_err}")
        raise  # Re-raise the exception to stop processing
    except Exception as err:
        print(f"An error occurred: {err}")
        logger.log_note(f"An error occurred: {err}")
        raise  # Re-raise the exception to stop processing

# Main function
def main():
    # Loop through months 1 to 12 for the given year
    year = 2025
    for month in range(1, 4):
        process_monthly_costs(year, month)    

if __name__ == "__main__":
    main()