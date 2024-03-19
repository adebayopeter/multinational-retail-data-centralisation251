import database_utils as db
import data_extraction as de
import data_cleaning as dc

db_connector = db.DatabaseConnector()
extractor = de.DataExtractor(db_connector)
cleaner = dc.DataCleaning()
db_connector2 = db.DatabaseConnector('db_creds_local.yaml')

tables = db_connector.list_db_tables()
print(f"Tables in the database: {tables}")

if tables:
    table_name = tables[0]  # choose the first table
    data_df = extractor.read_rds_table(table_name)
    cleaned_data_df = cleaner.clean_user_data(data_df)
    success = db_connector2.upload_to_db(cleaned_data_df, 'dim_users')

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


