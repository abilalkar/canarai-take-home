# Scrapy Project with PostgreSQL, MongoDB and Redis Integration

## Project Overview
A Scrapy project that crawls job data and stores it in PostgreSQL, MongoDB, and Redis for caching.
## Setup Instructions
1. **Clone the Repository**:
```
git clone https://github.com/abilalkar/canarai-take-home.git
cd canarai-take-home/app
```
2. **Build and Start Containers**:
```
docker-compose up --build
```

This starts the PostgreSQL, MongoDB, and Redis containers. Scrapy runs automatically and generates resulting CSV files.
The following CSV files will be created in the root folder:
- postgre_processed_data.csv: Data from PostgreSQL.
- processed_mongodb_data.csv: Data from MongoDB.

## Accessing Container Data
PostgreSQL:
1.	Enter the PostgreSQL container:
```
docker exec -it postgres_service psql -U postgres
```
2.	Connect to the database:
```
\c scrapydb
```
3.	Query the table:
```
SELECT * FROM raw_table;
```

MongoDB:
1.	Enter the MongoDB container:
```
docker exec -it mongodb_service mongosh --username mongo_user --password mongo_password
```
2.	Switch to the database:
```
use scrapydb
```
3.	Query data:
```
db.raw_collection.find()
```

Redis:
1.	Enter the Redis container:
```
docker exec -it redis_service redis-cli
```
2.	Check cached keys:
```
KEYS *
```

## Additional folder explanation
- **`data/` Folder**: Located in the root directory, this folder is designated for storing the two JSON data files used as input for the project.  

