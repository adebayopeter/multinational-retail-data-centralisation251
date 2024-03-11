import psycopg2


class DatabaseConnector:

    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def connect(self):
        """Establishes a connection to the database."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print("Connected to the database")
        except Exception as e:
            print("Failed to connect to the database:", e)

    def disconnect(self):
        """Closes the database connection."""
        if self.conn is not None:
            self.conn.close()
            print("Disconnect from the database")


    def execute_query(self, query):
        """Executes a SQL query on the connected database."""
        if self.conn is not None:
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                self.conn.commit()
                print("Query executed successfully")
            except Exception as e:
                print("Failed to execute query: ", e)
        else:
            print("Not connected to the database")


if __name__ == "__main__":
    db_connector = DatabaseConnector(
        dbname='',
        user='',
        password='',
        host='',
        port=''
    )

    # Connect to the database
    db_connector.connect()

    # Query
    create_table_query = """
    CREATE TABLE IF NOT EXISTS example_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        age INT
    );
    """

    # Execute query
    db_connector.execute_query(create_table_query)

    # disconnect from the database
    db_connector.disconnect()
    