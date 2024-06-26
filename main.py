import database_utils as db
import data_extraction as de
import data_cleaning as dc

db_connector = db.DatabaseConnector()
extractor = de.DataExtractor(db_connector)
cleaner = dc.DataCleaning()

############
# Milestone 2: Task 3
############
# tables = db_connector.list_db_tables()
# print(f"Tables in the database: {tables}")
# table_name = tables[1]  # choose the first table
# user_data_df = extractor.read_rds_table(table_name)
# print(user_data_df.info())
# user_data_df.to_csv('csv/user_data.csv')
# cleaned_user_data_df = cleaner.clean_user_data(user_data_df)
# print(cleaned_user_data_df.info())
# cleaned_user_data_df.to_csv('csv/cleaned_user_data.csv')
# db_connector2 = db.DatabaseConnector('db_creds_local.yaml')
# success = db_connector2.upload_to_db(cleaned_user_data_df, 'dim_users')
# if success:
#     print(f"Data uploaded to the database successfully")
# else:
#     print(f"Failed to upload data to database")


############
# Milestone 2: Task 4
############
# db_connector2 = db.DatabaseConnector('db_creds_local.yaml')
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
# if success:
#     print(f"Data uploaded to the database successfully")
# else:
#     print(f"Failed to upload data to database")


############
# Milestone 2: Task 5
############
# api_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
# api_url_2 = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
# headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
# store_end_number = extractor.list_number_of_stores(api_url, headers)
# stores_data_df = extractor.retrieve_stores_data(api_url_2, headers, 0, store_end_number)
# print(stores_data_df.head())
# print(stores_data_df.shape)
# print(stores_data_df.info())
# print(stores_data_df.columns)
# stores_data_df.to_csv('csv/store_data.csv')
# clean_stores_data_df = cleaner.clean_store_data(stores_data_df)
# clean_stores_data_df.to_csv('csv/clean_store_data.csv')
# print(clean_stores_data_df.info())
# db_connector2 = db.DatabaseConnector('db_creds_local.yaml')
# success = db_connector2.upload_to_db(clean_stores_data_df, 'dim_store_details')
# if success:
#     print(f"Data uploaded to the database successfully")
# else:
#     print(f"Failed to upload data to database")


############
# Milestone 2: Task 6
############
# s3_csv_data_df = extractor.extract_from_s3('s3://data-handling-public/products.csv')
# print(s3_csv_data_df.info())
# s3_csv_data_df.to_csv('s3_csv_data.csv')
# cleaned_s3_csv_data_df_2 = cleaner.convert_product_weights(s3_csv_data_df)
# cleaned_s3_csv_data_df = cleaner.clean_products_data(cleaned_s3_csv_data_df_2)
# cleaned_s3_csv_data_df.to_csv('cleaned_s3_csv_data.csv')
# print(cleaned_s3_csv_data_df.info())
# db_connector2 = db.DatabaseConnector('db_creds_local.yaml')
# success = db_connector2.upload_to_db(cleaned_s3_csv_data_df, 'dim_products')
# if success:
#     print(f"Data uploaded to the database successfully")
# else:
#     print(f"Failed to upload data to database")


############
# Milestone 2: Task 7
############
# table_names = db_connector.list_db_tables()
# print(table_names)
# order_table = table_names[2]
# orders_table_df = extractor.read_rds_table(order_table)
# print(orders_table_df.info())
# print(orders_table_df.head())
# print(orders_table_df.shape)
# orders_table_df.to_csv('orders_table_data.csv')
# cleaned_orders_table_df = cleaner.clean_orders_data(orders_table_df)
# cleaned_orders_table_df.to_csv('cleaned_orders_table.csv')
# print(cleaned_orders_table_df.info())
# db_connector2 = db.DatabaseConnector('db_creds_local.yaml')
# success = db_connector2.upload_to_db(cleaned_orders_table_df, 'orders_table')
# if success:
#     print(f"Data uploaded to the database successfully")
# else:
#     print(f"Failed to upload data to database")


############
# Milestone 1: Task 8
############
# json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
# json_data_df = extractor.extract_from_json(json_url)
# print(json_data_df.info())
# json_data_df.to_csv('csv/json_data.csv')
# cleaned_json_data_df = cleaner.clean_json_data(json_data_df)
# print(cleaned_json_data_df.info())
# cleaned_json_data_df.to_csv('csv/cleaned_json_data.csv')
# db_connector2 = db.DatabaseConnector('db_creds_local.yaml')
# success = db_connector2.upload_to_db(cleaned_json_data_df, 'dim_date_times')
# if success:
#     print(f"Data uploaded to the database successfully")
# else:
#     print(f"Failed to upload data to database")
