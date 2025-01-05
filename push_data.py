import pandas as pd
from dotenv import load_dotenv
import os
import logging
import uuid

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

# LangChain (for vector search)
from langchain_openai import OpenAIEmbeddings
from langchain_astradb import AstraDBVectorStore

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv()

# Astra DB connection configuration
ASTRA_DB_CONFIG = {
    'secure_connect_bundle': os.getenv('SECURE_CONNECT_BUNDLE'),
    'username': 'token',  # 'token' as the username
    'password': os.getenv('ASTRA_PASSWORD'),
    'api_endpoint': os.getenv('ASTRA_DB_API_ENDPOINT')
}

# Optional embedding API key for vector search
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Connect to Astra DB
auth_provider = PlainTextAuthProvider(
    username=ASTRA_DB_CONFIG['username'],
    password=ASTRA_DB_CONFIG['password']
)
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

# Keyspace
KEYSPACE = os.getenv('KEYSPACE') or "default_keyspace"
try:
    session.set_keyspace(KEYSPACE)
    logger.info(f"Using keyspace: {KEYSPACE}")
except Exception as e:
    logger.error(f"Error setting keyspace: {e}")
    exit(1)

# Initialize Vector Store
vector_store = None
if OPENAI_API_KEY:
    try:
        embedding = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
        vector_store = AstraDBVectorStore(
            embedding=embedding,
            collection_name="engagement_store",
            token=os.getenv('ASTRA_DB_APPLICATION_TOKEN'),
            api_endpoint=ASTRA_DB_CONFIG['api_endpoint']
        )
        logger.info("Astra Vector Store initialized with OpenAI embeddings.")
    except Exception as e:
        logger.warning(f"Astra Vector Store not configured properly: {e}")

# Load dataset
try:
    data = pd.read_csv('social_media_engagement_data.csv')
    logger.info("Successfully loaded social_media_engagement_data.csv")
except Exception as e:
    logger.error(f"Error loading CSV file: {e}")
    exit(1)

# Insert data into vector store
def insert_data():
    if not vector_store:
        logger.error("Vector store is not initialized. Exiting data insertion.")
        return

    batch_size = 100
    records_to_vectorize = []

    for index, row in data.iterrows():
        try:
            desc = row['post_type']
            metadata = {
                "content": desc,
                "id": str(uuid.uuid4()),
                "likes": int(row['likes']),
                "comments": int(row['comments']),
                "shares": int(row['shares']),
                "total_engagement": int(row['total_engagement'])
            }
            records_to_vectorize.append({"text": desc, "metadata": metadata})

            if (index + 1) % batch_size == 0:
                _flush_to_vector_store(records_to_vectorize)
                logger.info(f"Vectorized and stored records up to index {index + 1}.")
                records_to_vectorize = []
        except Exception as e:
            logger.error(f"Error processing row {index + 1}: {e}")

    if records_to_vectorize:
        _flush_to_vector_store(records_to_vectorize)
        logger.info("Vectorized and stored remaining 'post_type' fields.")

def _flush_to_vector_store(records):
    texts = [r['text'] for r in records]
    metadatas = [r['metadata'] for r in records]
    try:
        vector_store.add_texts(texts=texts, metadatas=metadatas)
    except Exception as ve:
        logger.error(f"Error vectorizing records: {ve}")

# analysis function
def analyze_post_type(post_type: str, top_k=5):
    if not vector_store:
        logger.error("Vector store not initialized.")
        return []
    try:
        results = vector_store.similarity_search(post_type, k=top_k)
        return [
            {
                "id": r.metadata["id"],
                "post_type": r.metadata["content"],
                "likes": r.metadata["likes"],
                "comments": r.metadata["comments"],
                "shares": r.metadata["shares"],
                "total_engagement": r.metadata["total_engagement"]
            }
            for r in results
        ]
    except Exception as e:
        logger.error(f"Error performing similarity search: {e}")
        return []

if __name__ == "__main__":
    insert_data()
    # Example: analyze "carousel" posts
    similar_posts = analyze_post_type("carousel", top_k=3)
    logger.info(f"Similar 'carousel' posts: {similar_posts}")