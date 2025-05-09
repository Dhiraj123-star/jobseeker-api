from elasticsearch import Elasticsearch
from elasticsearch import exceptions
from app.models import Job
import uuid
import time
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

es = None
INDEX_NAME = "jobs"

def connect_elasticsearch():
    global es
    elastic_url = os.getenv("ELASTIC_URL", "http://elasticsearch:9200")
    logger.info(f"Starting connection attempts to Elasticsearch at {elastic_url}")
    
    for i in range(90):  # Increased to 90 attempts (15 minutes total)
        try:
            es = Elasticsearch(
                hosts=[elastic_url],
                request_timeout=30,
                verify_certs=False
            )
            # Explicitly check cluster health
            health = es.cluster.health(wait_for_status="yellow", request_timeout=10)
            if es.ping():
                logger.info(f"Connected to Elasticsearch. Cluster health: {health['status']}")
                break
            else:
                logger.warning(f"Attempt {i+1}: Ping failed, retrying...")
        except exceptions.ConnectionError as e:
            logger.error(f"Attempt {i+1}: Elasticsearch connection failed: {str(e)}")
            if "connection refused" in str(e).lower():
                logger.info("Connection refused, likely Elasticsearch not fully started.")
            elif "timeout" in str(e).lower():
                logger.info("Connection timed out, retrying...")
        except exceptions.ApiError as e:
            logger.error(f"Attempt {i+1}: Elasticsearch API error: {str(e)}")
        except Exception as e:
            logger.error(f"Attempt {i+1}: Unexpected error: {str(e)}")
        time.sleep(10)  # Keep 10-second intervals
    else:
        logger.error("Elasticsearch is not reachable after 90 attempts")
        raise Exception("Elasticsearch is not reachable after 90 attempts.")
    
    logger.info("Elasticsearch connection process completed")

def create_index():
    try:
        if not es.indices.exists(index=INDEX_NAME):
            logger.info(f"Creating index {INDEX_NAME}")
            es.indices.create(index=INDEX_NAME)
        else:
            logger.info(f"Index {INDEX_NAME} already exists")
    except exceptions.ApiError as e:
        logger.error(f"Failed to create index {INDEX_NAME}: {str(e)}")
        raise
    except exceptions.ConnectionError as e:
        logger.error(f"Failed to create index {INDEX_NAME}: Connection error: {str(e)}")
        raise

def index_job(job: Job):
    try:
        doc_id = str(uuid.uuid4())
        res = es.index(index=INDEX_NAME, id=doc_id, document=job.dict())
        logger.info(f"Indexed job with ID {doc_id}")
        return {"message": "Job indexed", "id": doc_id}
    except exceptions.ApiError as e:
        logger.error(f"Failed to index job: {str(e)}")
        raise
    except exceptions.ConnectionError as e:
        logger.error(f"Failed to index job: Connection error: {str(e)}")
        raise

def search_jobs(query: str):
    try:
        logger.info(f"Searching jobs with query: {query}")
        res = es.search(index=INDEX_NAME, query={
            "multi_match": {
                "query": query,
                "fields": [
                    "title",
                    "description",
                    "tags",
                    "location",
                    "company"
                ]
            }
        })
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except exceptions.ApiError as e:
        logger.error(f"Failed to search jobs: {str(e)}")
        raise
    except exceptions.ConnectionError as e:
        logger.error(f"Failed to search jobs: Connection error: {str(e)}")
        raise