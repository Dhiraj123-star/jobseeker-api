from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.elastic_utils import connect_elasticsearch, create_index, index_job, search_jobs
from app.models import Job
import logging
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application lifespan")
    try:
        logger.info("Waiting for Elasticsearch to be ready")
        connect_elasticsearch()
        logger.info("Elasticsearch connection established successfully")
        logger.info("Attempting to create index")
        create_index()
        logger.info("Index creation completed successfully")
    except Exception as e:
        logger.error(f"Failed to initialize application: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        raise
    yield
    logger.info("Shutting down application")

app = FastAPI(lifespan=lifespan, startup_timeout=900)  # Increased to 15 minutes

@app.post("/jobs")
def add_job(job: Job):
    logger.info(f"Adding job: {job.title}")
    return index_job(job)

@app.get("/search")
def search(q: str):
    logger.info(f"Searching with query: {q}")
    return search_jobs(q)