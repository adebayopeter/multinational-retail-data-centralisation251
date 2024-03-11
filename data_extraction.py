import csv
import requests
import boto3


class DataExtractor:
    def __init__(self):
        pass

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
