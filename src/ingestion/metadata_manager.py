"""
MongoDB metadata manager for tracking jobs and pipeline state.
"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

logger = logging.getLogger(__name__)


class MetadataManager:
    """Manages job metadata and pipeline state in MongoDB."""
    
    def __init__(self, mongo_uri: str, db_name: str):
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            # Verify connection
            self.client.admin.command('ping')
            self.db = self.client[db_name]
            logger.info(f"Connected to MongoDB: {db_name}")
        except ServerSelectionTimeoutError:
            logger.error(f"Could not connect to MongoDB at {mongo_uri}")
            raise
    
    def record_job(self, job_name: str, status: str, records_processed: int, error_message: str = None):
        """Record job execution in MongoDB."""
        try:
            job_doc = {
                "job_name": job_name,
                "status": status,
                "records_processed": records_processed,
                "started_at": datetime.now(),
                "error_message": error_message,
                "created_at": datetime.now()
            }
            
            result = self.db.jobs.insert_one(job_doc)
            logger.info(f"Recorded job {job_name} with status {status} (ID: {result.inserted_id})")
            
        except Exception as e:
            logger.error(f"Error recording job: {e}")
            raise
    
    def get_last_cursor(self, cursor_name: str) -> Optional[datetime]:
        """Get last successful fetch timestamp."""
        try:
            state = self.db.pipeline_state.find_one({"_id": cursor_name})
            if state:
                cursor = state.get("last_cursor")
                logger.info(f"Retrieved cursor {cursor_name}: {cursor}")
                return cursor
            logger.info(f"No cursor found for {cursor_name}, will use default")
            return None
        except Exception as e:
            logger.error(f"Error getting cursor: {e}")
            return None
    
    def update_cursor(self, cursor_name: str, timestamp: datetime):
        """Update last successful fetch timestamp."""
        try:
            self.db.pipeline_state.update_one(
                {"_id": cursor_name},
                {
                    "$set": {
                        "last_cursor": timestamp,
                        "last_update": datetime.now()
                    }
                },
                upsert=True
            )
            logger.info(f"Updated cursor {cursor_name} to {timestamp}")
        except Exception as e:
            logger.error(f"Error updating cursor: {e}")
            raise
    
    def record_data_quality(self, table_name: str, metrics: Dict[str, Any]):
        """Record data quality metrics."""
        try:
            quality_doc = {
                "table_name": table_name,
                "run_date": datetime.now(),
                **metrics
            }
            
            self.db.data_quality.insert_one(quality_doc)
            logger.info(f"Recorded data quality metrics for {table_name}")
            
        except Exception as e:
            logger.error(f"Error recording data quality: {e}")
            raise
    
    def get_latest_jobs(self, limit: int = 10) -> list:
        """Get latest job records."""
        try:
            jobs = list(self.db.jobs.find().sort("_id", -1).limit(limit))
            return jobs
        except Exception as e:
            logger.error(f"Error retrieving jobs: {e}")
            return []
