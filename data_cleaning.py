import pandas as pd
import numpy as np
import re


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
    def clean_store_data(store_data_df):
        """
        Clean the store data DataFrame.

        Args:
            store_data_df (pd.DataFrame): DataFrame containing store data.

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Drop rows with all NULL values
        store_data_df = store_data_df.dropna(how='all')

        # Drop rows with non-numeric longitude
        store_data_df = store_data_df[pd.to_numeric(store_data_df['longitude'], errors='coerce').notnull()]

        # Drop rows with non-numeric latitude
        store_data_df = store_data_df[pd.to_numeric(store_data_df['latitude'], errors='coerce').notnull()]

        # Convert 'opening_date' column to datetime data type
        store_data_df['opening_date'] = pd.to_datetime(
            store_data_df['opening_date'], errors='coerce', format='%Y-%m-%d')

        # Drop all rows with NULL values in 'opening_date' column
        store_data_df = store_data_df.dropna(subset=['opening_date'])

        # Convert `staff_numbers` column into integer data type
        store_data_df['staff_numbers'] = pd.to_numeric(store_data_df['staff_numbers'],
                                                       errors='coerce', downcast='integer')

        # Replace NULL values with 0
        store_data_df['staff_numbers'] = store_data_df['staff_numbers'].fillna(0)

        # Drop `lat` column
        store_data_df.drop(columns=['lat'], inplace=True)

        # Convert `longitude` column into integer data type
        store_data_df['longitude'] = pd.to_numeric(store_data_df['longitude'], errors='coerce')

        # Convert `latitude` column into integer data type
        store_data_df['latitude'] = pd.to_numeric(store_data_df['latitude'], errors='coerce')

        # Replace `eeAmerica` with `America`
        store_data_df['continent'] = store_data_df['continent'].replace('eeAmerica', 'America')

        # Replace `eeEurope` with `Europe`
        store_data_df['continent'] = store_data_df['continent'].replace('eeEurope', 'Europe')

        # Convert columns into string data type
        string_columns = ['address', 'locality', 'store_code', 'store_type', 'country_code', 'continent']
        # Iterate over each column and apply string operations
        for col in string_columns:
            if col in store_data_df.columns:
                store_data_df[col] = store_data_df[col].str.strip()
                store_data_df[col] = store_data_df[col].astype("string")

        # Reset/rearrange index column
        # store_data_df.set_index('index', inplace=True)
        # store_data_df.reset_index(drop=False, inplace=True)
        store_data_df.reset_index(drop=True, inplace=True)
        store_data_df.index += 1

        return store_data_df

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
    def convert_product_weights(s3_csv_data_df):
        # Make a copy of the DataFrame to avoiding modifying the original
        s3_csv_data_df = s3_csv_data_df.copy()

        # Func to convert ml to kg
        def ml_to_kg(ml_value):
            # if 1ml = 1g, ml to kg dividing by 1000
            return ml_value / 1000

        # Func to extract numeric values from strings
        def extract_numeric_value(weight_string):
            numeric_value = None
            if isinstance(weight_string, str):
                # Check for patterns like `12 x 100g`
                match = re.search(r'(\d+)\s*x\s*(\d+(\.\d+)?)\s*(\w+)', weight_string)
                if match:
                    qty = float(match.group(1))
                    unit_weight = float(match.group(2))
                    unit = match.group(4)
                    # Check for units and convert to kg appropriately
                    if 'kg' == unit.lower():
                        numeric_value = qty * unit_weight
                    elif 'g' == unit.lower():
                        numeric_value = (qty * unit_weight) / 1000
                    elif 'ml' == unit.lower():
                        numeric_value = (qty * unit_weight) / 1000
                        numeric_value *= 0.001
                else:
                    # Extract numeric value from the string
                    match = re.search(r'(\d+(\.\d+)?)', weight_string)
                    if match:
                        numeric_value = float(match.group(1))
                        # Check for units and convert to kg appropriately
                        if 'kg' in weight_string.lower():
                            numeric_value *= 1.0
                        elif 'g' in weight_string.lower():
                            numeric_value *= 0.001
                        elif 'ml' in weight_string.lower():
                            numeric_value *= 0.001
                            numeric_value *= 0.001
            return numeric_value

        s3_csv_data_df['weight'] = s3_csv_data_df['weight'].apply(lambda x: extract_numeric_value(x))

        # Remove rows with non-numeric or NaN values in weight column
        s3_csv_data_df = s3_csv_data_df[s3_csv_data_df['weight'].notnull()]

        return s3_csv_data_df

    @staticmethod
    def clean_products_data(s3_csv_data_df):
        # Make a copy of the DataFrame to avoiding modifying the original
        s3_csv_data_df = s3_csv_data_df.copy()

        # Remove `pound` sign from product_price
        s3_csv_data_df['product_price'] = s3_csv_data_df['product_price'].str.replace('£', '')

        # Remove rows with empty or non-numeric values in `product_price` column
        s3_csv_data_df = s3_csv_data_df[pd.to_numeric(s3_csv_data_df['product_price'], errors='coerce').notnull()]

        # Convert `product_price` to float
        s3_csv_data_df['product_price'] = pd.to_numeric(s3_csv_data_df['product_price'], errors='coerce')

        # Convert `EAN` column to integer
        s3_csv_data_df['EAN'] = pd.to_numeric(s3_csv_data_df['EAN'], errors='coerce', downcast='integer')

        # Convert 'date_added' column to datetime data type
        s3_csv_data_df['date_added'] = pd.to_datetime(
            s3_csv_data_df['date_added'], errors='coerce', format='%Y-%m-%d')

        # Remove rows with empty or NULL values in `date_added` column
        s3_csv_data_df = s3_csv_data_df.dropna(subset=['date_added'])

        # Convert columns into string data type
        string_columns = ['product_name', 'category', 'uuid', 'removed', 'product_code']
        # Iterate over each column and apply string operations
        for col in string_columns:
            if col in s3_csv_data_df.columns:
                s3_csv_data_df[col] = s3_csv_data_df[col].str.strip()
                s3_csv_data_df[col] = s3_csv_data_df[col].astype("string")

        return s3_csv_data_df

    @staticmethod
    def clean_orders_data(order_table_df):
        # Drop `first_name`, `last_name`, and `1` column
        order_table_df = order_table_df.drop(columns=['first_name', 'last_name', '1'])

        # Convert `card_number` column to integer
        order_table_df['card_number'] = pd.to_numeric(order_table_df['card_number'], errors='coerce',
                                                      downcast='integer')

        # Convert `product_quantity` column to integer
        order_table_df['product_quantity'] = pd.to_numeric(order_table_df['product_quantity'], errors='coerce',
                                                           downcast='integer')

        # Drop empty or NULL rows in `card_number` and `product_quantity` columns
        order_table_df = order_table_df.dropna(subset=['card_number', 'product_quantity'])

        # Convert columns into string data type
        string_columns = ['date_uuid', 'user_uuid', 'store_code', 'product_code']
        for col in string_columns:
            if col in order_table_df.columns:
                order_table_df[col] = order_table_df[col].str.strip()
                order_table_df[col] = order_table_df[col].astype("string")

        return order_table_df

    @staticmethod
    def clean_json_data(json_data_df):
        # Make a copy of the DataFrame to avoiding modifying the original
        json_data_df = json_data_df.copy()

        # Convert `time_period`, `date_uuid` columns to string
        string_columns = ['time_period', 'date_uuid']
        for col in string_columns:
            if col in json_data_df.columns:
                json_data_df[col] = json_data_df[col].str.strip()
                json_data_df[col] = json_data_df[col].astype("string")

        # Drop empty or NULL rows in `time_period`, `date_uuid` columns
        json_data_df = json_data_df.dropna(subset=string_columns)

        # Convert `month`, `year` and `day` columns to integer
        json_data_df['year'] = pd.to_numeric(json_data_df['year'], errors='coerce', downcast='integer')

        # Drop empty or NULL rows in `month`, `year` and `day` columns
        json_data_df = json_data_df.dropna(subset=['year'])

        # Ensure year column only as integer data type
        json_data_df['year'] = json_data_df['year'].astype(int)

        json_data_df['month'] = pd.to_numeric(json_data_df['month'], errors='coerce', downcast='integer')
        json_data_df['day'] = pd.to_numeric(json_data_df['day'], errors='coerce', downcast='integer')

        json_data_df['timestamp'] = pd.to_datetime(json_data_df['timestamp'], format='%H:%M:%S')

        # Drop empty or NULL rows in `timestamp` column
        json_data_df = json_data_df.dropna(subset=['timestamp'])

        return json_data_df

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
