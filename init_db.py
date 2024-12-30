from astrapy import DataAPIClient
from dotenv import load_dotenv
import pandas as pd
import os
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()

# Initialize Astra DB Client
ASTRA_TOKEN = os.getenv('ASTRA_TOKEN')
ASTRA_DB_ENDPOINT = os.getenv('ASTRA_DB_ENDPOINT')

client = DataAPIClient(ASTRA_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_ENDPOINT)

logger.info(f"Connected to Astra DB: {db.name}")

# Load Data
data = pd.read_csv('social_media_engagement_data.csv')

# Define Table and Insert Function
TABLE_NAME = 'engagement'

def create_table():
    try:
        if TABLE_NAME not in db.list_collection_names():
            db.create_table(
                name=TABLE_NAME,
                primary_keys=['post_type', 'total_engagement'],
                columns=[
                    {'name': 'post_type', 'type': 'text'},
                    {'name': 'likes', 'type': 'int'},
                    {'name': 'comments', 'type': 'int'},
                    {'name': 'shares', 'type': 'int'},
                    {'name': 'total_engagement', 'type': 'int'}
                ]
            )
            logger.info(f"Created table '{TABLE_NAME}'.")
        else:
            logger.info(f"Table '{TABLE_NAME}' already exists.")
    except Exception as e:
        logger.error(f"Error creating table '{TABLE_NAME}': {e}")
        exit(1)

def insert_data():
    try:
        records = []
        for _, row in data.iterrows():
            record = {
                'post_type': row['Post Type'],
                'likes': int(row['Likes']),
                'comments': int(row['Comments']),
                'shares': int(row['Shares']),
                'total_engagement': int(row['Total Engagement'])
            }
            records.append(record)
        
        db.insert_many(TABLE_NAME, records)
        logger.info(f"Inserted {len(records)} records into '{TABLE_NAME}' table.")
    except Exception as e:
        logger.error(f"Error inserting data into '{TABLE_NAME}': {e}")

if __name__ == '__main__':
    create_table()
    insert_data()