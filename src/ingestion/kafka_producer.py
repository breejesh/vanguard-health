"""
Updated ingestion producer: Creates fetch tasks for Kafka consumers.

Logic:
1. Check MongoDB for last fetch time
2. If found: use it. If not: use (now - 30 days)
3. Create 6 fetch tasks (one per resource type)
4. Send to Kafka topic
5. Workers consume and actually fetch data

This DECOUPLES task creation from data fetching.
"""
import sys
import logging
import os
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.common.config import get_config
from src.common.logger import setup_logger
from src.ingestion.metadata_manager import MetadataManager

logger = setup_logger(__name__)
config = get_config()

FETCH_LOOKBACK_DAYS = int(os.getenv("FETCH_LOOKBACK_DAYS", "30"))  # Default 30 days


class KafkaTaskProducer:
    """Produces fetch tasks to Kafka - does NOT fetch data itself."""
    
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            max_in_flight_requests_per_connection=5
        )
        logger.info(f"Kafka producer initialized: {bootstrap_servers}")
    
    def produce_fetch_tasks(self, resource_types: list, since_datetime: datetime) -> int:
        """
        Create fetch tasks based on timestamp - actual fetching done by workers.
        
        Each task tells a worker:
        - What resource type to fetch
        - Starting from what timestamp
        - Starting from what page
        """
        task_count = 0
        task_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for resource_type in resource_types:
            task = {
                "id": f"{resource_type.lower()}-{task_id}",
                "resource_type": resource_type,
                "since_timestamp": since_datetime.isoformat(),  # Workers will use this for API query
                "start_page": 1,
                "task_created_at": datetime.now().isoformat(),
                "status": "pending",
                "batch_id": task_id  # Link all 6 tasks together
            }
            
            try:
                future = self.producer.send('fhir-fetch-tasks', value=task)
                record_metadata = future.get(timeout=10)
                
                logger.info(
                    f"Task produced: {resource_type} (since {since_datetime.date()}) "
                    f"→ partition {record_metadata.partition}, offset {record_metadata.offset}"
                )
                task_count += 1
                
            except KafkaError as e:
                logger.error(f"Failed to produce task for {resource_type}: {e}")
        
        self.producer.flush()
        return task_count
    
    def close(self):
        self.producer.close()


def main():
    """
    Producer CronJob: Creates fetch tasks based on MongoDB metadata.
    
    Flow:
    1. Get last_fetch_time from MongoDB (if exists)
    2. If not found: use (now - FETCH_LOOKBACK_DAYS) as fallback
    3. Create 6 fetch tasks and send to Kafka
    4. Record batch_id in MongoDB for tracking
    5. Exit (workers will consume and fetch)
    """
    metadata_manager = None
    producer = None
    
    try:
        # Initialize Kafka producer
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        producer = KafkaTaskProducer(kafka_servers)
        logger.info("Kafka producer initialized")
        
        # Try to connect to MongoDB for metadata
        try:
            metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)
            logger.info("Connected to MongoDB")
        except Exception as e:
            logger.warning(f"Could not connect to MongoDB: {e}. Using fallback timestamp.")
            metadata_manager = None
        
        # ═══════════════════════════════════════════════════════════
        # DETERMINE FETCH START TIME
        # ═══════════════════════════════════════════════════════════
        
        if metadata_manager:
            try:
                # Get last successful fetch time from MongoDB
                last_fetch = metadata_manager.get_last_cursor("fhir_fetch")
                if last_fetch:
                    since_timestamp = last_fetch
                    logger.info(f"Found last fetch time in MongoDB: {since_timestamp}")
                else:
                    # First run: default to 30 days ago
                    since_timestamp = datetime.now() - timedelta(days=FETCH_LOOKBACK_DAYS)
                    logger.info(f"No prev fetch found. Using default: {FETCH_LOOKBACK_DAYS} days ago → {since_timestamp}")
            except Exception as e:
                logger.warning(f"Error reading MongoDB cursor: {e}. Using fallback.")
                since_timestamp = datetime.now() - timedelta(days=FETCH_LOOKBACK_DAYS)
        else:
            # MongoDB unavailable: use default lookback
            since_timestamp = datetime.now() - timedelta(days=FETCH_LOOKBACK_DAYS)
            logger.info(f"Using default lookback: {FETCH_LOOKBACK_DAYS} days ago → {since_timestamp}")
        
        # ═══════════════════════════════════════════════════════════
        # CREATE AND SEND FETCH TASKS TO KAFKA
        # ═══════════════════════════════════════════════════════════
        
        resource_types = ["Patient", "Encounter", "Observation", "Condition", "Medication", "MedicationRequest"]
        task_count = producer.produce_fetch_tasks(resource_types, since_timestamp)
        
        logger.info(f"✓ Created {task_count} fetch tasks (batch starting from {since_timestamp.date()})")
        
        # ═══════════════════════════════════════════════════════════
        # RECORD IN MONGODB FOR TRACKING
        # ═══════════════════════════════════════════════════════════
        
        if metadata_manager:
            try:
                # Record this batch run
                metadata_manager.record_job(
                    "ingestion_producer",
                    "completed",
                    task_count,
                    f"Created {task_count} fetch tasks"
                )
                logger.info("✓ Recorded batch in MongoDB")
            except Exception as e:
                logger.warning(f"Could not record batch in MongoDB: {e}")
        
        logger.info("Producer job completed. Workers will now consume and fetch data.")
        return 0
        
    except Exception as e:
        logger.error(f"Producer error: {e}", exc_info=True)
        return 1
        
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed")


if __name__ == "__main__":
    sys.exit(main())
