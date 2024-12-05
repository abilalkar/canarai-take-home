import json
import os
import psycopg2
from itemadapter import ItemAdapter
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent.parent / 'infra'))
from postgresql_connector import PostgresConnector
from redis_connector import RedisConnector
from mongodb_connector import MongoDBConnector

class JobsProjectPipeline:
    def __init__(self, postgres_settings, redis_settings, mongo_settings):
        # Initialize the pipeline with settings for PostgreSQL, Redis, and MongoDB
        self.postgres_settings = postgres_settings
        self.redis_settings = redis_settings
        self.mongo_settings = mongo_settings

    def __del__(self):
        # Close all database connections when the pipeline object is deleted
        self.postgresql.close()
        self.redis.close()
        self.mongodb.close()

    def create_table_if_not_exists(self):
        # Create the 'raw_table' in PostgreSQL if it doesn't already exist.
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS raw_table (
            slug text,
            language text,
            languages jsonb,
            req_id VARCHAR(255) PRIMARY KEY,
            title text,
            description text,
            street_address text,
            city text,
            state text,
            country_code text,
            postal_code text,
            location_type text,
            latitude double precision,
            longitude double precision,
            categories jsonb,
            tags jsonb,
            tags5 jsonb,
            tags6 jsonb,
            brand text,
            promotion_value bigint,
            salary_currency text,
            salary_value bigint,
            salary_min_value bigint,
            salary_max_value bigint,
            benefits jsonb,
            employment_type text,
            hiring_organization text,
            source text,
            apply_url text,
            internal boolean,
            searchable boolean,
            applyable boolean,
            li_easy_applyable boolean,
            ats_code text,
            update_date text,
            create_date text,
            category jsonb,
            full_location text,
            short_location text
        )
        '''
        self.postgresql.execute(create_table_query)

    @classmethod
    def from_crawler(cls, crawler):
        # Initialize pipeline settings from Scrapy's settings
        postgres_settings = {
            'host': os.getenv('POSTGRES_HOST'),  
            'port': int(os.getenv('POSTGRES_PORT')),  
            'user': os.getenv('POSTGRES_USER'),  
            'password': os.getenv('POSTGRES_PASSWORD'),  
            'database': os.getenv('POSTGRES_DB')
        }
        redis_settings = {
            'host': os.getenv('REDIS_HOST'),  
            'port': int(os.getenv('REDIS_PORT')),  
            'db': os.getenv('REDIS_DB'),
        }
        mongo_settings = {
            'host': os.getenv('MONGO_HOST'),
            'port': int(os.getenv('MONGO_PORT')),
            'database': os.getenv('MONGO_DB'),
            'username': os.getenv('MONGO_USER'),
            'password': os.getenv('MONGO_PASSWORD')
        }
        return cls(postgres_settings, redis_settings, mongo_settings)

    def open_spider(self, spider):
        # Open connections to PostgreSQL, MongoDB, and Redis when the spider starts
        try:
            self.postgresql = PostgresConnector(**self.postgres_settings)
            self.postgresql.connect()

            self.create_table_if_not_exists()  # Ensure the PostgreSQL table exists

            self.mongodb = MongoDBConnector(**self.mongo_settings)
            self.mongodb.connect()

            self.redis = RedisConnector(**self.redis_settings)
            self.redis.connect()

        except psycopg2.OperationalError as e:
            spider.logger.error(f"PostgreSQL connection error: {e}")
            raise Exception("Failed to connect to PostgreSQL.") from e
        except Exception as e:
            spider.logger.error(f"Error connecting to services: {e}")
            raise Exception("Failed to initialize connections.") from e

    def close_spider(self, spider):
        # Close all database connections when the spider finishes.
        try:
            self.postgresql.close()
            self.mongodb.close()
            self.redis.close()
        except Exception as e:
            spider.logger.error(f"Error closing connections: {e}")
            raise

    def process_item(self, item, spider):
        # Process each scraped item, storing it in PostgreSQL, MongoDB, and Redis.
        adapter = ItemAdapter(item)

        red_id = adapter.get('req_id')

        # Check if the job is already processed (cached in Redis)
        if self.redis.exists(red_id):
            spider.logger.info(f"Duplicate job {red_id} found in cache. Skipping.")
            return item

        # Insert job data into PostgreSQL
        try:
            query = '''
                INSERT INTO "raw_table" (
                    "slug", "language", "languages", "req_id", "title", "description", 
                    "street_address", "city", "state", "country_code", "postal_code", 
                    "location_type", "latitude", "longitude", "categories", "tags", "tags5", 
                    "tags6", "brand", "promotion_value", "salary_currency", "salary_value", 
                    "salary_min_value", "salary_max_value", "benefits", "employment_type", 
                    "hiring_organization", "source", "apply_url", "internal", "searchable", 
                    "applyable", "li_easy_applyable", "ats_code", "update_date", "create_date", 
                    "category", "full_location", "short_location"
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            '''
            data = (
                item.get('slug'),
                item.get('language'),
                json.dumps(item.get('languages', [])),  
                item.get('req_id'),
                item.get('title'),
                item.get('description'),
                item.get('street_address'),
                item.get('city'),
                item.get('state'),
                item.get('country_code'),
                item.get('postal_code'),
                item.get('location_type'),
                item.get('latitude'),
                item.get('longitude'),
                json.dumps(item.get('categories', [])),
                json.dumps(item.get('tags', [])),
                json.dumps(item.get('tags5', [])),
                json.dumps(item.get('tags6', [])),
                item.get('brand'),
                item.get('promotion_value'),
                item.get('salary_currency'),
                item.get('salary_value'),
                item.get('salary_min_value'),
                item.get('salary_max_value'),
                json.dumps(item.get('benefits', [])),
                item.get('employment_type'),
                item.get('hiring_organization'),
                item.get('source'),
                item.get('apply_url'),
                item.get('internal'),
                item.get('searchable'),
                item.get('applyable'),
                item.get('li_easy_applyable'),
                item.get('ats_code'),
                item.get('update_date'),
                item.get('create_date'),
                json.dumps(item.get('category', [])),
                item.get('full_location'),
                item.get('short_location')
            )

            self.postgresql.execute(query, data)
            spider.logger.info(f"Successfully inserted job {red_id} into PostgreSQL.")
        except Exception as e:
            spider.logger.error(f"Error inserting job {red_id} into PostgreSQL. Query: {query}, Data: {data}. Error: {e}")
            return None
        
        # Insert job data into MongoDB
        try:
            document = {
                'slug': adapter.get('slug'),
                'language': adapter.get('language'),
                'languages': adapter.get('languages', []),
                'req_id': adapter.get('req_id'),
                'title': adapter.get('title'),
                'description': adapter.get('description'),
                'street_address': adapter.get('street_address'),
                'city': adapter.get('city'),
                'state': adapter.get('state'),
                'country_code': adapter.get('country_code'),
                'postal_code': adapter.get('postal_code'),
                'location_type': adapter.get('location_type'),
                'latitude': adapter.get('latitude'),
                'longitude': adapter.get('longitude'),
                'categories': adapter.get('categories', []),
                'tags': adapter.get('tags', []),
                'tags5': adapter.get('tags5', []),
                'tags6': adapter.get('tags6', []),
                'brand': adapter.get('brand'),
                'promotion_value': adapter.get('promotion_value'),
                'salary_currency': adapter.get('salary_currency'),
                'salary_value': adapter.get('salary_value'),
                'salary_min_value': adapter.get('salary_min_value'),
                'salary_max_value': adapter.get('salary_max_value'),
                'benefits': adapter.get('benefits', []),
                'employment_type': adapter.get('employment_type'),
                'hiring_organization': adapter.get('hiring_organization'),
                'source': adapter.get('source'),
                'apply_url': adapter.get('apply_url'),
                'internal': adapter.get('internal'),
                'searchable': adapter.get('searchable'),
                'applyable': adapter.get('applyable'),
                'li_easy_applyable': adapter.get('li_easy_applyable'),
                'ats_code': adapter.get('ats_code'),
                'update_date': adapter.get('update_date'),
                'create_date': adapter.get('create_date'),
                'category': adapter.get('category', []),
                'full_location': adapter.get('full_location'),
                'short_location': adapter.get('short_location')
            }

            # Insert the document into MongoDB
            self.mongodb.insert_one('raw_collection', document)
            spider.logger.info(f"Successfully inserted job {red_id} into MongoDB.")
        except Exception as e:
            spider.logger.error(f"Error inserting job {red_id} into MongoDB: {e}")
            return None

        try:
            # Cache the job in Redis
            self.redis.set(red_id, 1)
            spider.logger.info(f"Job {red_id} cached in Redis.")
        except Exception as e:
            spider.logger.error(f"Error caching job {red_id} in Redis: {e}")

        return item