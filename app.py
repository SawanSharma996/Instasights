from flask import Flask, request, jsonify
import pandas as pd
from dotenv import load_dotenv
import os
import logging
import uuid

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Application started")

# Load environment variables from .env file
load_dotenv()

# Initialize the Astra DB client using DataStax Driver
ASTRA_DB_CONFIG = {
    'secure_connect_bundle': os.getenv('SECURE_CONNECT_BUNDLE'),
    'username': 'token',  # Use 'token' as the username
    'password': os.getenv('ASTRA_PASSWORD')  # Your generated Database Token
}

# Setup authentication provider
auth_provider = PlainTextAuthProvider(
    ASTRA_DB_CONFIG['username'],
    ASTRA_DB_CONFIG['password']
)

# Initialize cluster connection
try:
    cluster = Cluster(
        cloud={'secure_connect_bundle': ASTRA_DB_CONFIG['secure_connect_bundle']},
        auth_provider=auth_provider
    )
    session = cluster.connect()
    logger.info("Connected to Astra DB")
except Exception as e:
    logger.error(f"Failed to connect to Astra DB: {e}")
    exit(1)

# Define your keyspace
KEYSPACE = os.getenv('KEYSPACE')  # Ensure this is set in your .env

# Connect to the specified keyspace
try:
    session.set_keyspace(KEYSPACE)
    logger.info(f"Using keyspace: {KEYSPACE}")
except Exception as e:
    logger.error(f"Error setting keyspace: {e}")
    exit(1)

# Function to create table if it doesn't exist
def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS engagement (
        id uuid PRIMARY KEY,
        post_type text,
        likes int,
        comments int,
        shares int,
        total_engagement int
    )
    """
    try:
        session.execute(create_table_query)
        logger.info("Table 'engagement' is ready with UUID as PRIMARY KEY.")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        exit(1)

# Load data from CSV
try:
    data = pd.read_csv('social_media_engagement_data.csv')
    logger.info("Successfully loaded social_media_engagement_data.csv")
except Exception as e:
    logger.error(f"Error loading CSV file: {e}")
    exit(1)

# Function to insert data into the table
def insert_data():
    create_table()  # Ensure table exists

    insert_query = session.prepare("""
    INSERT INTO engagement (id, post_type, likes, comments, shares, total_engagement)
    VALUES (?, ?, ?, ?, ?, ?)
    """)

    batch = BatchStatement()
    for index, row in data.iterrows():
        record = (
            uuid.uuid4(),  # Generate a unique UUID for each row
            row['Post Type'],
            int(row['Likes']),
            int(row['Comments']),
            int(row['Shares']),
            int(row['Total Engagement'])
        )
        batch.add(insert_query, record)
        
        # Execute the batch every 100 records
        if (index + 1) % 100 == 0:
            try:
                session.execute(batch)
                logger.info(f"Inserted {index + 1} records.")
                batch.clear()
            except Exception as e:
                logger.error(f"Error inserting batch at index {index + 1}: {e}")

    # Execute any remaining records
    try:
        session.execute(batch)
        logger.info("Data insertion completed.")
    except Exception as e:
        logger.error(f"Error inserting final batch: {e}")

# Flask app factory function
def create_app():
    app = Flask(__name__)

    # Insert data during app initialization
    insert_data()

    # Route to analyze post performance
    @app.route('/analyze', methods=['GET'])
    def analyze():
        post_type = request.args.get('post_type')
        if not post_type:
            return jsonify({"error": "Please provide a 'post_type' parameter."}), 400

        try:
            select_query = session.prepare("""
            SELECT likes, comments, shares, total_engagement FROM engagement WHERE post_type = ?
            """)

            rows = session.execute(select_query, (post_type,))

            likes = [row.likes for row in rows]
            comments = [row.comments for row in rows]
            shares = [row.shares for row in rows]
            total_engagement = [row.total_engagement for row in rows]

            return jsonify({
                'post_type': post_type,
                'avg_likes': sum(likes) / len(likes) if likes else 0,
                'avg_comments': sum(comments) / len(comments) if comments else 0,
                'avg_shares': sum(shares) / len(shares) if shares else 0,
                'avg_total_engagement': sum(total_engagement) / len(total_engagement) if total_engagement else 0
            })

        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            return jsonify({"error": str(e)}), 500

    # Route to fetch all post types
    @app.route('/post_types', methods=['GET'])
    def get_post_types():
        try:
            select_query = session.prepare("""
            SELECT DISTINCT post_type FROM engagement
            """)

            rows = session.execute(select_query)

            post_types = list(set(row.post_type for row in rows))
            return jsonify({"post_types": post_types})
        except Exception as e:
            logger.error(f"Error fetching post types: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route('/')
    def index():
        return "Social Media Performance Analysis API"

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, use_reloader=False)