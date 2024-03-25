import pandas as pd
import numpy as np


class DataCleaning:
    @staticmethod
    def clean_user_data(data_df):
        """
        Clean the user data DataFrame.

        Args:
            data_df (pd.DataFrame): DataFrame containing user data.

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Calculate columns with less than 10% non-null values
        threshold = int(len(data_df) * 0.1)

        # Drop columns with less than 10% non-null values
        data_df = data_df.dropna(axis=1, thresh=threshold)

        # Drop rows with NULL values
        # data_df.dropna(inplace=True)

        # Convert date columns to datatime format
        date_columns = ['opening_date']
        for col in date_columns:
            if col in data_df.columns:
                data_df.loc[:, col] = pd.to_datetime(data_df[col], errors='coerce', format='%Y-%m-%d')
                data_df = data_df.dropna(subset=[col])

        # Convert numeric columns saved as objects to numeric
        numeric_columns = ['longitude', 'staff_numbers', 'latitude']
        for col in numeric_columns:
            if col in data_df.columns:
                data_df.loc[:, col] = pd.to_numeric(data_df[col], errors='coerce')
                data_df = data_df.dropna(subset=[col])

        return data_df

    @staticmethod
    def clean_card_data(card_data_df):
        """
        Clean the card data DataFrame.

        Args:
            card_data_df (pd.DataFrame): DataFrame containing card data.

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Drop rows with all NULL values
        card_data_df = card_data_df.dropna(how='all')

        # Drop rows with non-numeric card numbers
        card_data_df = card_data_df[pd.to_numeric(card_data_df['card_number'], errors='coerce').notnull()]

        # Convert 'expiry_date' column to datetime data type
        # card_data_df['expiry_date'] = pd.to_datetime(card_data_df['expiry_date'], format='%m/%y')

        # Convert 'date_payment_confirmed' column to datetime data type
        card_data_df['date_payment_confirmed'] = pd.to_datetime(
            card_data_df['date_payment_confirmed'], errors='coerce', format='%Y-%m-%d')

        # Drop all rows with NULL values in 'expiry_date' or 'date_payment_confirmed' columns
        card_data_df = card_data_df.dropna(subset=['expiry_date', 'date_payment_confirmed'])

        # Convert `card_provider` column into string data type
        card_data_df['card_provider'] = card_data_df['card_provider'].str.strip()
        card_data_df['card_provider'] = card_data_df['card_provider'].astype("string")

        # Convert `card_number` column into integer data type
        card_data_df['card_number'] = card_data_df['card_number'].astype(str).str.rstrip('.0')
        card_data_df['card_number'] = pd.to_numeric(card_data_df['card_number'], errors='coerce', downcast='integer')

        # Reset the index
        card_data_df = card_data_df.reset_index(drop=True)

        return card_data_df

    @staticmethod
    def clean_csv(file_path):
        """Clean data from a CSV file."""
        try:
            df = pd.read_csv(file_path)
            # perform data cleaning operations
            return df
        except Exception as e:
            print("Error cleaning CSV data: ", e)
            return None

    @staticmethod
    def clean_api_data(url):
        """Clean data from an API."""
        # Implement data cleaning for API data
        pass

    @staticmethod
    def clean_s3_data(bucket_name, object_key):
        """Clean data from an S3 bucket."""
        # Implement data cleaning for S3 data
        pass


if __name__ == "__main__":
    cleaned_data = DataCleaning.clean_csv('example.csv')
    if cleaned_data is not None:
        print("CSV data cleaned successfully")
    else:
        print("Failed to clean CSV data")
