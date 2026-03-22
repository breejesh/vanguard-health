"""
Kafka consumer worker - fetches FHIR data based on Kafka messages.
Runs as a deployment (can scale to N replicas for parallel IO).
"""
import os
import sys
import logging
import json
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

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
        self.consumer = KafkaConsumer(
            'fhir-fetch-tasks',
            bootstrap_servers=kafka_servers.split(','),
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='fhir-workers',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=1  # Process one task at a time
        )
        
        self.fetcher = FHIRFetcher(config.SYNTHEA_API_URL, os.getenv("SYNTHEA_API_KEY"))
        self.writer = HudiWriter(config.BRONZE_PATH)
        self.metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)
        
        # Get worker ID from hostname
        self.worker_id = os.getenv("HOSTNAME", "unknown")
        logger.info(f"Worker {self.worker_id} initialized")
    
    def process_task(self, task: dict) -> bool:
        """Process a single fetch task."""
        try:
            task_id = task.get("id")
            resource_type = task.get("resource_type")
            last_modified = task.get("last_modified")
            
            logger.info(f"[{self.worker_id}] Processing task {task_id}: {resource_type}")
            
            # Fetch data for this resource type
            from datetime import datetime as dt
            last_mod_dt = dt.fromisoformat(last_modified.replace('Z', '+00:00'))
            resources = self.fetcher._fetch_resource_type(resource_type, last_mod_dt)
            
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
