import time
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
from jugaad_data.nse import bhavcopy_fo_save
import logging

# Configure logging
logging.basicConfig(filename='data_update.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Function to validate if a given date is not a weekend (Saturday or Sunday)
def is_weekday(input_date):
    return input_date.weekday() < 5  # Monday to Friday (0 to 4)

# Function to generate a date range
def date_range(start_date, end_date):
    delta = timedelta(days=1)
    while start_date <= end_date:
        yield start_date
        start_date += delta

# Function to download data for a specific date
def download_data_for_date(target_date):
    # Generate the file name based on the date and format
    file_name = f"fo{target_date.strftime('%d%b%Y')}bhav.csv"
    file_path = rf"C:\Users\anand\OneDrive\Documents\BHAV COPY\{file_name}"

    # Call bhavcopy_fo_save to fetch data for the desired date
    save_directory = rf"C:\Users\anand\OneDrive\Documents\BHAV COPY"
    try:
        bhavcopy_fo_save(target_date, save_directory)
        print(f"Bhavcopy for {target_date} saved to {save_directory}")
    except Exception as e:
        print(f"An error occurred while saving Bhav copy data: {str(e)}")
        logging.error(f"Error while saving Bhav copy data: {str(e)}")

    # Wait for 5 seconds to ensure the file is downloaded
    time.sleep(5)

    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"An error occurred while processing the CSV file: {str(e)}")
        logging.error(f"Error while processing the CSV file: {str(e)}")
        return None

# Input for the date range
while True:
    try:
        start_date_str = input("Enter the start date (YYYY-MM-DD): ")
        end_date_str = input("Enter the end date (YYYY-MM-DD): ")
        
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str)

        if start_date <= end_date:
            break
        else:
            print("End date should be greater than or equal to the start date.")
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")

# Define the MySQL database connection parameters
db_username = 'root'
db_password = '#Trader1429'
db_host = 'localhost'
new_database_name = 'FnO_Data'  # Update the database name

# Create the MySQL database connection URL with the new database name
database_url = f"mysql+pymysql://{db_username}:{db_password}@{db_host}/{new_database_name}"

# Create a list to store the dataframes for each date
dataframes = []

# Process data for dates within the specified range
for single_date in date_range(start_date, end_date):
    if is_weekday(single_date):
        df = download_data_for_date(single_date)
        if df is not None:
            dataframes.append(df)

# Check if start and end dates are the same
if start_date == end_date and is_weekday(start_date):
    df = download_data_for_date(start_date)
    if df is not None:
        dataframes.append(df)

# Continue with your code to merge and update the data in the database

if dataframes:
    # Concatenate dataframes into one
    combined_data = pd.concat(dataframes, ignore_index=True)

    try:
        # Create a database connection
        engine = create_engine(database_url)

        # Fetch existing data from the database
        query = "SELECT * FROM options_data"
        existing_data = pd.read_sql(query, con=engine)

        # Append the new data to the existing data
        combined_data = pd.concat([existing_data, combined_data], ignore_index=True)

        # Replace the data in the database with the combined data
        combined_data.to_sql('options_data', con=engine, if_exists='replace', index=False)

        # Close the database connection
        engine.dispose()

        print("Data has been successfully updated in the database.")
        logging.info("Data updated in the database.")
    except Exception as e:
        print(f"An error occurred while updating the database: {str(e)}")
        logging.error(f"Error while updating the database: {str(e)}")
