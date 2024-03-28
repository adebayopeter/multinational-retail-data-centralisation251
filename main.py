import database_utils as db
import data_extraction as de
import data_cleaning as dc

db_connector = db.DatabaseConnector()
extractor = de.DataExtractor(db_connector)
cleaner = dc.DataCleaning()

api_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
api_url_2 = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
store_end_number = extractor.list_number_of_stores(api_url, headers)
stores_data_df = extractor.retrieve_stores_data(api_url_2, headers, 0, store_end_number)
print(stores_data_df.head())
print(stores_data_df.shape)
print(stores_data_df.info())
print(stores_data_df.columns)
stores_data_df.to_csv('store_data.csv')
clean_stores_data_df = cleaner.clean_store_data(stores_data_df)
clean_stores_data_df.to_csv('clean_store_data.csv')
print(clean_stores_data_df.info())

db_connector2 = db.DatabaseConnector('db_creds_local.yaml')
# pdf_df = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# print(pdf_df.head())
# print(pdf_df.shape)
# print(pdf_df.info())
# print(pdf_df.columns)
# pdf_df.to_csv('pdf.csv')
# cleaned_pdf = cleaner.clean_card_data(pdf_df)
# print(cleaned_pdf.head())
# print(cleaned_pdf['expiry_date'].head())
# print(cleaned_pdf.info())
# print(cleaned_pdf.shape)
# cleaned_pdf.to_csv('cleaned_pdf.csv')
# print(cleaned_pdf['card_provider'].unique())
# success = db_connector2.upload_to_db(cleaned_pdf, 'dim_card_details')

# tables = db_connector.list_db_tables()

# print(f"Tables in the database: {tables}")
# table_name = tables[0]  # choose the first table
# data_df = extractor.read_rds_table(table_name)
# cleaned_data_df = cleaner.clean_user_data(data_df)
# success = db_connector2.upload_to_db(cleaned_data_df, 'dim_users')
success = db_connector2.upload_to_db(clean_stores_data_df, 'dim_store_details')


if success:
    print(f"Data uploaded to the database successfully")
else:
    print(f"Failed to upload data to database")
    

# print(data_df.dtypes)
# print(data_df.head())
# data_df.to_csv('extract.csv')

# print(cleaned_data_df.dtypes)
# print(cleaned_data_df.head())
# print(cleaned_data_df.info())
# cleaned_data_df.to_csv('extract_2.csv')


