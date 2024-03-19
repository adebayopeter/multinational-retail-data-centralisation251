import yaml
from sqlalchemy import create_engine, MetaData, Table


class DatabaseConnector:

    def __init__(self, creds_file='db_creds.yaml'):
        """Load database credentials from the YAML file."""
        self.creds_file = creds_file
        self.db_param = self.read_db_creds(self.creds_file)

    def read_db_creds(self, creds_file):
        """Load database credentials from the YAML file."""
        self.creds_file = creds_file
        try:
            with open(self.creds_file, 'r') as file:
                db_param = yaml.safe_load(file)
        except FileNotFoundError:
            print("Credentials file not found")
        except KeyError as e:
            print("Missing key in credentials file: ", e)
        return db_param

    def init_db_engine(self):
        if not self.db_param:
            return None

        db_uri = f"postgresql://{self.db_param['RDS_USER']}:{self.db_param['RDS_PASSWORD']}@{self.db_param['RDS_HOST']}:{self.db_param['RDS_PORT']}/{self.db_param['RDS_DATABASE']}"
        engine = create_engine(db_uri)
        return engine

    def list_db_tables(self):
        """List all tables in the database"""
        try:
            engine = self.init_db_engine()
            if not engine:
                print("Error connecting to database engine.")
                return []

            metadata = MetaData()
            metadata.reflect(bind=engine)
            tables = list(metadata.tables.keys())
            return tables
        except Exception as e:
            print("Error here: ", e)

    def upload_to_db(self, df, table_name):
        """Upload a dataframe to a specified table in the database"""
        try:
            engine = self.init_db_engine()
            if not engine:
                print("Error initializing database engine")
                return False

            # Create a SQLAlchemy Table object for the specified table
            metadata = MetaData()
            metadata.reflect(bind=engine)
            table = Table(table_name, metadata)

            # Upload the dataframe to the database table
            with engine.connect() as connection:
                df.to_sql(table_name, connection, if_exists='replace', index=False)

            print(f"Data uploaded successfully to table `{table_name}`")
            return True
        except Exception as e:
            print(f"Error uploading data to table `{table_name}` : {e}")
            return False


if __name__ == "__main__":
    db_connector = DatabaseConnector()
    db_engine = db_connector.init_db_engine()

