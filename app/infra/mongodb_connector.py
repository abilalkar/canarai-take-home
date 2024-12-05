from pymongo import MongoClient

class MongoDBConnector:
    def __init__(self, host, port, database, username, password):
        # Initialize MongoDB connection parameters
        self.host = host
        self.port = port
        self.database_name = database
        self.username = username
        self.password = password
        self.client = None
        self.db = None

    def connect(self):
        # Connect to MongoDB server, authenticate if credentials are provided
        try:
            if self.username and self.password:
                self.client = MongoClient(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password
                )
            else:
                self.client = MongoClient(host=self.host, port=self.port)

            if self.database_name:
                self.db = self.client[self.database_name]
                print(f"Connected to MongoDB database '{self.database_name}'")
            else:
                print("Connected to MongoDB server without specifying a database.")
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            raise

    def insert_one(self, collection, document):
        # Insert a single document into the specified collection
        try:
            result = self.db[collection].insert_one(document)
            return result.inserted_id
        except Exception as e:
            print(f"Error inserting document: {e}")
            raise

    def fetch_all(self, collection, query={}):
        # Fetch all documents from a collection based on a query
        try:
            return list(self.db[collection].find(query))
        except Exception as e:
            print(f"Error fetching documents: {e}")
            raise

    def close(self):
        # Close the MongoDB connection
        if self.client:
            self.client.close()
            print("MongoDB connection closed.")