import csv

import pandas as pd
import requests
import boto3
from sqlalchemy import MetaData, Table, select
import warnings
import tabula
import jpype
from io import StringIO


# Filter out the FutureWarning from tabula
warnings.filterwarnings("ignore", message="errors='ignore' is deprecated", category=FutureWarning)


class DataExtractor:
    def __init__(self, database_connector):
        self.database_connector = database_connector

    def read_rds_table(self, table_name):
        """Read data from the specified table in the RDS database."""
        try:
            engine = self.database_connector.init_db_engine()
            if not engine:
                print("Error initializing database engine.")
                return None

            with engine.connect() as connection:
                metadata = MetaData()
                metadata.reflect(bind=engine)
                table = Table(table_name, metadata, autoload=True)
                columns = table.columns
                query = select(columns)
                result = connection.execute(query)
                data = result.fetchall()

                # convert data from db into pandas Dataframe
                df = pd.DataFrame(data, columns=[c.name for c in columns])
                return df
        except Exception as e:
            print(f"Error reading data from table `{table_name}`: {e}")
            return None

    @staticmethod
    def retrieve_pdf_data(pdf_url_link):
        try:
            # Extract tables from PDF into single DataFrame
            pdf_df = tabula.read_pdf(pdf_url_link, pages='all', multiple_tables=True)
            combined_pdf_df = pd.concat(pdf_df)

            return combined_pdf_df
        except Exception as e:
            print(f"Error retrieving data from PDF: {e}")
            return None

    @staticmethod
    def extract_from_csv(file_path):
        """
        Extracts data from a CSV file.

        Args:
            file_path (str): The path to the CSV file.

        Returns:
            list: A list of dictionaries representing the extracted data.
        """
        data = []
        with open(file_path, 'r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                data.append(row)
        return data

    @staticmethod
    def extract_from_api(url):
        """
        Extracts data from an API.

        Args:
            url (str): The URL of the API.

        Returns:
            list: A list of dictionaries representing the extracted data.
        """
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to fetch data from API")
            return []

    @staticmethod
    def list_number_of_stores(api_url, headers):
        """
        Extracts the number of stores from the API.

        Args:
            api_url (str): The URL endpoint for retrieving the number of stores.
            headers (dict): The header dictionary containing the API key.

        Returns:
            int: The number of stores.
        """
        try:
            response = requests.get(api_url, headers=headers)
            if response.status_code == 200:
                return response.json()['number_stores']
            else:
                print("Failed to retrieve number of stores. Status code:", response.status_code)
                return None
        except Exception as e:
            print("Error retrieving number of stores:", e)
            return None

    @staticmethod
    def retrieve_stores_data(api_url, headers, store_start_num, store_end_num):
        """
        Retrieve store data for a range of store numbers from the API and save them in a pandas dataframe

        Args:
            api_url (str): The URL endpoint for retrieving store data. It should contain a
                placeholder(store_number) for the store number.
            headers (dict): The header dictionary containing the API key.
            store_start_num (int): The store starting number.
            store_end_num (int): The store ending number.

        Returns:
            pd.DataFrame: DataFrame containing store data.
        """
        store_data_list = []
        try:
            for store_num in range(store_start_num, store_end_num):
                url = api_url.format(store_number=store_num)
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    store_data = response.json()
                    store_data_list.append(store_data)
                else:
                    print(f"Failed to retrieve data for store number {store_num}. Status code:", response.status_code)
            store_data_df = pd.DataFrame(store_data_list)
            return store_data_df
        except Exception as e:
            print("Error retrieving store data:", e)
            return None

    @staticmethod
    def extract_from_s3(s3_address):
        """
        Extracts data from an S3 bucket.

        Args:
            s3_address (str): The S3 bucket url.

        Returns:
            pd.DataFrame: DataFrame containing store data.
        """
        s3 = boto3.client('s3')

        # Extract bucket and key from s3 address
        bucket_name, object_key = s3_address.split('s3://')[1].split('/', 1)
        try:
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            s3_csv_data = response['Body'].read().decode('utf-8')

            # Convert data to CSV
            s3_df = pd.read_csv(StringIO(s3_csv_data))
            return s3_df
        except Exception as e:
            print(f"Failed to fetch data from S3: {e}")
            return []


if __name__ == "__main__":
    extractor = DataExtractor()
    csv_data = extractor.extract_from_csv('example.csv')
    api_data = extractor.extract_from_api('https://api.example.com/data')
    s3_data = extractor.extract_from_s3('example-bucket', 'example-object.json')
