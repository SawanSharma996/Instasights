from flask import Flask, request, jsonify
from dotenv import load_dotenv
import os
import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

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

# Flask app factory function
def create_app():
    app = Flask(__name__)

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