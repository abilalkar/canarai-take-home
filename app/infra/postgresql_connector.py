import psycopg2

class PostgresConnector:
    def __init__(self, database, user, password, host, port=5432):
        # Initialize connection details
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def fetch_data(self, query):
        # Fetch data from the database
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as error:
            print(f"Error fetching data: {error}")
            raise

    def connect(self):
        # Establish connection to the database
        self.connection = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor = self.connection.cursor()

    def execute(self, query, params=None):
        # Execute a query (insert, update, delete)
        if not self.connection:
            raise Exception("Connection not established.")
        self.cursor.execute(query, params)
        self.connection.commit()

    def close(self):
        # Close the cursor and connection
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()