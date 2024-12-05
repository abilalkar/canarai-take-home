import redis

class RedisConnector:
    def __init__(self, host, port=6379, db=0):
        # Initialize connection parameters
        self.host = host
        self.port = port
        self.db = db
        self.connection = None

    def connect(self):
        # Establish connection to the Redis server
        self.connection = redis.Redis(host=self.host, port=self.port, db=self.db)

    def exists(self, key):
        # Check if the key exists in Redis
        return self.connection.exists(key)

    def set(self, key, value):
        # Set a value for a key in Redis
        self.connection.set(key, value)

    def close(self):
        # Close the Redis connection
        if self.connection:
            self.connection.close()