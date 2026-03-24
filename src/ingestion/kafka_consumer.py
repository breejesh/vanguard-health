"""
Kafka consumer worker - fetches FHIR data based on Kafka messages.
Runs as a deployment (can scale to N replicas for parallel IO).
"""
import os
import sys
import time
import logging
import json
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from src.common.config import get_config
from src.common.logger import setup_logger
from src.ingestion.fhir_fetcher import FHIRFetcher
from src.ingestion.hudi_writer import HudiWriter
from src.ingestion.metadata_manager import MetadataManager

logger = setup_logger(__name__)
config = get_config()


class FetchWorker:
    """Worker that consumes fetch tasks from Kafka and fetches data."""
    
    def __init__(self):
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.consumer = self._connect_kafka_with_retry(kafka_servers)
        
        self.fetcher = FHIRFetcher(config.SYNTHEA_API_URL, os.getenv("SYNTHEA_API_KEY"))
        self.writer = HudiWriter(config.BRONZE_PATH)
        self.metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)
        
        # Get worker ID from hostname
        self.worker_id = os.getenv("HOSTNAME", "unknown")
        logger.info(f"Worker {self.worker_id} initialized")
    
    def _connect_kafka_with_retry(self, kafka_servers, max_retries=30, initial_backoff=2):
        """Connect to Kafka with exponential backoff retry logic."""
        backoff = initial_backoff
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt + 1}/{max_retries} to connect to Kafka at {kafka_servers}")
                consumer = KafkaConsumer(
                    'fhir-fetch-tasks',
                    bootstrap_servers=kafka_servers.split(','),
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id='fhir-workers',
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    max_poll_records=1,  # Process one task at a time
                    request_timeout_ms=15000,  # Must be larger than session timeout
                    session_timeout_ms=10000
                )
                logger.info("Successfully connected to Kafka!")
                return consumer
            except (NoBrokersAvailable, KafkaError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Failed to connect to Kafka (attempt {attempt + 1}): {e}. Retrying in {backoff}s...")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 30)  # Cap backoff at 30 seconds
                else:
                    logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                    raise
        

    def process_task(self, task: dict) -> bool:
        """Process a single fetch task."""
        try:
            task_id = task.get("id")
            resource_type = task.get("resource_type")
            since_timestamp = task.get("since_timestamp")  # Comes from producer
            
            logger.info(f"[{self.worker_id}] Processing task {task_id}: {resource_type}")
            
            # Fetch data for this resource type
            from datetime import datetime as dt
            since_dt = dt.fromisoformat(since_timestamp.replace('Z', '+00:00'))
            resources = self.fetcher._fetch_resource_type(resource_type, since_dt)
            
            if resources:
                # Write to Bronze
                self.writer.write_to_bronze(resources)
                
                # Record completion
                self.metadata_manager.record_job(
                    f"fetch_worker_{resource_type}",
                    "completed",
                    len(resources)
                )
                
                logger.info(f"[{self.worker_id}] Completed {resource_type}: {len(resources)} records")
                return True
            else:
                logger.warning(f"[{self.worker_id}] No records for {resource_type}")
                return True
                
        except Exception as e:
            logger.error(f"[{self.worker_id}] Error processing task: {e}", exc_info=True)
            self.metadata_manager.record_job(
                f"fetch_worker_{resource_type}",
                "failed",
                0,
                str(e)
            )
            return False
    
    def run(self):
        """Run the worker - consume and process tasks."""
        logger.info(f"[{self.worker_id}] Starting Kafka consumer loop")
        
        try:
            for message in self.consumer:
                task = message.value
                logger.info(f"[{self.worker_id}] Received task: {task['id']}")
                
                success = self.process_task(task)
                
                if success:
                    logger.info(f"[{self.worker_id}] Task completed: {task['id']}")
                else:
                    logger.warning(f"[{self.worker_id}] Task failed: {task['id']}")
                    
        except KeyboardInterrupt:
            logger.info(f"[{self.worker_id}] Shutting down gracefully...")
        except Exception as e:
            logger.error(f"[{self.worker_id}] Consumer error: {e}", exc_info=True)
        finally:
            self.consumer.close()


def main():
    """Start the fetch worker."""
    worker = FetchWorker()
    worker.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
