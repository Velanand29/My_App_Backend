from flask import Flask, request, jsonify
from sqlalchemy import create_engine
import pandas as pd
from datetime import date, timedelta
from jugaad_data.nse import bhavcopy_fo_save
import time

app = Flask(__name__)

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
    # Update with your file path
    file_name = f"fo{target_date.strftime('%d%b%Y')}bhav.csv"
    file_path = f"C:\\Users\\anand\\OneDrive\\Documents\\BHAV COPY\\{file_name}"

    # Call bhavcopy_fo_save to fetch data for the desired date
    save_directory = f"C:\\Users\\anand\\OneDrive\\Documents\\BHAV COPY"
    try:
        bhavcopy_fo_save(target_date, save_directory)
        return file_path  # Return the file path
    except Exception as e:
        return str(e)

# Define an API route for data upload
@app.route('/api/upload-data', methods=['POST'])
def upload_data():
    try:
        # Get the start date and end date from the request
        start_date_str = request.form.get('start_date')
        end_date_str = request.form.get('end_date')

        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str)

        # Validate the date range
        if start_date > end_date:
            return jsonify({'message': 'End date should be greater than or equal to the start date'}), 400

        # Define MySQL database connection parameters
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
                file_path = download_data_for_date(single_date)
                if file_path:
                    df = pd.read_csv(file_path)
                    dataframes.append(df)

        # Check if start and end dates are the same
        if start_date == end_date and is_weekday(start_date):
            file_path = download_data_for_date(start_date)
            if file_path:
                df = pd.read_csv(file_path)
                dataframes.append(df)

        # Continue with your code to merge and update the data in the database
        if dataframes:
            combined_data = pd.concat(dataframes, ignore_index=True)
            engine = create_engine(database_url)
            query = "SELECT * FROM options_data"
            existing_data = pd.read_sql(query, con=engine)
            combined_data = pd.concat([existing_data, combined_data], ignore_index=True)
            combined_data.to_sql('options_data', con=engine, if_exists='replace', index=False)
            engine.dispose()
            return jsonify({'message': 'Data has been successfully updated in the database'}), 200

        return jsonify({'message': 'No data to update'}), 200

    except Exception as e:
        return jsonify({'message': f'An error occurred: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True)
