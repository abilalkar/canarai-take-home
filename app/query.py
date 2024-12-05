import psycopg2
import csv
import os

from infra.postgresql_connector import PostgresConnector
from infra.mongodb_connector import MongoDBConnector

# Class to handle CSV writing operations
class ToCSV:
    def __init__(self, host, port, user, password, database):
        # Initialize connection parameters for the database
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    @staticmethod
    def write_to_csv(data, csv_filename, headers=None):
        # Writes data to a CSV file. If headers are provided, they will be written as the first row.
        try:
            with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                if headers:
                    writer.writerow(headers)  # Write headers if provided
                writer.writerows(data)  # Write the data rows
            print(f"Data successfully written to {csv_filename}")
        except Exception as error:
            print(f"Error writing to CSV: {error}")
            raise

if __name__ == "__main__":
    # PostgreSQL configuration
    postgre_config = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': int(os.getenv('POSTGRES_PORT')),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': os.getenv('POSTGRES_DB'),
    }

    # Initialize PostgreSQL connection
    postgre = PostgresConnector(**postgre_config)
    postgre_query = "SELECT * FROM raw_table;"  # Sample query

    try:
        # Fetch data from PostgreSQL
        postgre.connect()
        postgre_data = postgre.fetch_data(postgre_query)
        
        if postgre_data:
            headers = [desc[0] for desc in postgre.cursor.description]  # Get column names as headers
            ToCSV.write_to_csv(postgre_data, 'postgre_processed_data.csv', headers=headers)
        else:
            print("No data found in PostgreSQL.")
    except psycopg2.Error as e:
        print(f"PostgreSQL database error: {e}")
    except Exception as e:
        print(f"Unexpected error during PostgreSQL processing: {e}")
    finally:
        postgre.close()  # Close PostgreSQL connection

    # MongoDB configuration
    mongo_config = {
        'host': os.getenv('MONGO_HOST'),
        'port': int(os.getenv('MONGO_PORT')),
        'database': os.getenv('MONGO_DB'),
        'username': os.getenv('MONGO_USER'),
        'password': os.getenv('MONGO_PASSWORD'),
    }

    # Initialize MongoDB connection
    mongo = MongoDBConnector(**mongo_config)
    mongo_collection_name = "raw_collection"  # Collection name in MongoDB

    try:
        # Fetch data from MongoDB
        mongo.connect()
        mongo_data = mongo.fetch_all(mongo_collection_name)

        if mongo_data:
            headers = mongo_data[0].keys()  # Get keys from the first document as headers
            rows = [list(doc.values()) for doc in mongo_data]  # Extract values as rows
            ToCSV.write_to_csv(rows, "processed_mongodb_data.csv", headers=headers)
        else:
            print("No data found in MongoDB.")
    except Exception as e:
        print(f"Unexpected error during MongoDB processing: {e}")
    finally:
        mongo.close()  # Close MongoDB connection