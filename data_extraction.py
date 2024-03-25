import csv

import pandas as pd
import requests
import boto3
from sqlalchemy import MetaData, Table, select
import warnings
import tabula
import jpype


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
    def extract_from_s3(bucket_name, object_key):
        """
        Extracts data from an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (str): The key of the object in the S3 bucket.

        Returns:
            list: A list of dictionaries representing the extracted data.
        """
        s3 = boto3.client('s3')
        try:
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            data = response['Body'].read().decode('utf-8')
            # Process the data as needed (e.g., parse JSON)
            return data
        except Exception as e:
            print("Failed to fetch data from S3: ", e)
            return []


if __name__ == "__main__":
    extractor = DataExtractor()
    csv_data = extractor.extract_from_csv('example.csv')
    api_data = extractor.extract_from_api('https://api.example.com/data')
    s3_data = extractor.extract_from_s3('example-bucket', 'example-object.json')
