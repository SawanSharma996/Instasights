from flask import Flask, request, jsonify
import pandas as pd
from astrapy import DataAPIClient
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Application started")

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Initialize the Astra DB client
ASTRA_TOKEN = os.getenv('ASTRA_TOKEN')
ASTRA_DB_ENDPOINT = os.getenv('ASTRA_DB_ENDPOINT')

client = DataAPIClient(ASTRA_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_ENDPOINT)

print(f"Connected to Astra DB: {db.name}")

data = pd.read_csv('social_media_engagement_data.csv')

# Insert data into the table
def insert_data():
    # Check if the collection/table exists; if not, create it
    if 'engagement' not in db.list_collection_names():
        db.create_table('engagement', primary_keys=['post_type', 'total_engagement'])
        print("Created table 'engagement'")
    else:
        print("Table 'engagement' already exists")

    # Insert data
    for index, row in data.iterrows():
        record = {
            'post_type': row['Post Type'],
            'likes': int(row['Likes']),
            'comments': int(row['Comments']),
            'shares': int(row['Shares']),
            'total_engagement': int(row['Total Engagement'])
        }
        db.insert('engagement', record)
    print("Data insertion completed")

# Route to analyze post performance
@app.route('/analyze', methods=['GET'])
def analyze():
    post_type = request.args.get('post_type')
    if not post_type:
        return jsonify({"error": "Please provide a 'post_type' parameter."}), 400

    try:
        # Query to calculate average metrics for the given post_type
        query = {
            "select": ["AVG(likes) AS avg_likes", "AVG(comments) AS avg_comments",
                       "AVG(shares) AS avg_shares", "AVG(total_engagement) AS avg_total_engagement"],
            "from": "engagement",
            "where": {"post_type": post_type}
        }

        result = db.execute_query(query)
        if result:
            avg_metrics = result[0]
            return jsonify({
                'post_type': post_type,
                'avg_likes': avg_metrics['avg_likes'],
                'avg_comments': avg_metrics['avg_comments'],
                'avg_shares': avg_metrics['avg_shares'],
                'avg_total_engagement': avg_metrics['avg_total_engagement']
            })
        else:
            return jsonify({"error": "Post type not found."}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to fetch all post types
@app.route('/post_types', methods=['GET'])
def get_post_types():
    try:
        query = {
            "select": ["DISTINCT post_type"],
            "from": "engagement"
        }
        results = db.execute_query(query)
        post_types = [record['post_type'] for record in results]
        return jsonify({"post_types": post_types})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def index():
    return "Social Media Performance Analysis API"

# Initialize data insertion before the first request
@app.before_first_request
def initialize():
    insert_data()

if __name__ == '__main__':
    app.run(debug=True)