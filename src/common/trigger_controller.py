"""
Pipeline Trigger Controller - runs as CronJob every 2 minutes
Checks if main pipeline should run based on:
- Last successful run time (must be >= 10 minutes ago)
- No jobs currently running
"""
import sys
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from pymongo import MongoClient
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Add src to path
sys.path.insert(0, '/app')

from src.common.logger import setup_logger
from src.common.config import get_config
from src.common.orchestrator import JobOrchestrator

logger = setup_logger(__name__)
config_obj = get_config()


class PipelineTriggerController:
    """Controls when to trigger the main data pipeline."""
    
    # Configuration
    MIN_INTERVAL_MINUTES = 10  # Minimum time between orchestrator runs
    
    def __init__(self):
        """Initialize MongoDB and K8s clients."""
        self.mongo_client = MongoClient(config_obj.MONGODB_URI, serverSelectionTimeoutMS=5000)
        self.db = self.mongo_client[config_obj.MONGODB_DB]
        self.orchestrator = JobOrchestrator()
        self.namespace = "vanguard-health"
        
    def get_last_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful pipeline run from MongoDB."""
        try:
            state = self.db.pipeline_state.find_one({"_id": "main_pipeline"})
            if state:
                last_run = state.get("last_run_time")
                if last_run:
                    logger.info(f"Last pipeline run: {last_run}")
                    return last_run
            logger.info("No previous pipeline run found in metadata")
            return None
        except Exception as e:
            logger.error(f"Error getting last run time: {e}")
            return None
    
    def set_last_run_time(self, timestamp: datetime):
        """Update last successful pipeline run timestamp."""
        try:
            # Ensure timestamp is UTC
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            
            self.db.pipeline_state.update_one(
                {"_id": "main_pipeline"},
                {
                    "$set": {
                        "last_run_time": timestamp,
                        "last_update": datetime.now(timezone.utc),
                        "status": "in_progress"
                    }
                },
                upsert=True
            )
            logger.info(f"Updated last run time to {timestamp}")
        except Exception as e:
            logger.error(f"Error updating last run time: {e}")
    
    def mark_pipeline_complete(self, success: bool = True):
        """Mark pipeline as complete in MongoDB."""
        try:
            self.db.pipeline_state.update_one(
                {"_id": "main_pipeline"},
                {
                    "$set": {
                        "last_completed_time": datetime.now(timezone.utc),
                        "status": "succeeded" if success else "failed",
                        "last_update": datetime.now(timezone.utc)
                    }
                },
                upsert=True
            )
            logger.info(f"Pipeline marked as {'succeeded' if success else 'failed'}")
        except Exception as e:
            logger.error(f"Error marking pipeline: {e}")
    
    def is_pipeline_running(self) -> bool:
        """Check if main pipeline jobs are currently running."""
        try:
            batch_v1 = client.BatchV1Api()
            
            # Check for running ingestion, bronze-to-silver, or silver-to-gold jobs
            jobs = batch_v1.list_namespaced_job(self.namespace)
            
            pipeline_job_names = ["ingestion-main", "bronze-to-silver-main", "silver-to-gold-main"]
            
            for job in jobs.items:
                if job.metadata.name in pipeline_job_names:
                    # Check if job is active
                    if job.status.active and job.status.active > 0:
                        logger.info(f"Found active job: {job.metadata.name}")
                        return True
            
            logger.info("No active pipeline jobs found")
            return False
        
        except Exception as e:
            logger.error(f"Error checking pipeline status: {e}")
            return True  # Fail safe - don't trigger if we can't check
    
    def should_trigger_pipeline(self) -> tuple[bool, str]:
        """Determine if pipeline should be triggered based on:
        - Last run time (must be >= 10 minutes ago)
        - No jobs currently running
        
        Returns (should_trigger, reason)
        """
        logger.info("\n" + "=" * 70)
        logger.info("PIPELINE TRIGGER CHECK")
        logger.info("=" * 70)
        
        # Check if pipeline is already running
        if self.is_pipeline_running():
            return False, "Pipeline already running - skipping"
        
        # Check last run time (use UTC for proper comparison with MongoDB timestamps)
        last_run = self.get_last_run_time()
        now = datetime.now(timezone.utc)
        
        if last_run is None:
            return True, f"No previous runs found - triggering first run"
        
        # Ensure last_run is timezone-aware (MongoDB returns aware datetimes)
        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=timezone.utc)
        
        elapsed_minutes = (now - last_run).total_seconds() / 60

        if elapsed_minutes >= self.MIN_INTERVAL_MINUTES:
            return True, f"Last run was {elapsed_minutes:.1f} minutes ago (>= {self.MIN_INTERVAL_MINUTES}m) - triggering"
        else:
            return False, f"Last run was {elapsed_minutes:.1f} minutes ago (< {self.MIN_INTERVAL_MINUTES}m) - skipping"
    
    def trigger_pipeline(self) -> bool:
        """Trigger the main pipeline orchestration job."""
        try:
            logger.info("\nTriggering main pipeline orchestrator job")
            orchestrator_image = os.getenv("PIPELINE_ORCHESTRATOR_IMAGE", "vanguard/pipeline-orchestrator:latest")

            batch_v1 = client.BatchV1Api()

            orchestrator_job = {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": f"pipeline-orchestrator-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                    "namespace": self.namespace
                },
                "spec": {
                    "ttlSecondsAfterFinished": 86400,
                    "backoffLimit": 1,
                    "activeDeadlineSeconds": 14400,  # 4 hour max
                    "template": {
                        "metadata": {
                            "labels": {"app": "pipeline-orchestrator"}
                        },
                        "spec": {
                            "serviceAccountName": "vanguard-sa",
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "name": "orchestrator",
                                    "image": orchestrator_image,
                                    "imagePullPolicy": "IfNotPresent",
                                    "env": [
                                        {
                                            "name": "TRIGGER_MODE",
                                            "value": "true"
                                        }
                                    ],
                                    "envFrom": [
                                        {"configMapRef": {"name": "app-config"}},
                                        {"secretRef": {"name": "app-secrets"}}
                                    ],
                                    "volumeMounts": [
                                        {"name": "data", "mountPath": "/mnt/data"},
                                        {"name": "logs", "mountPath": "/app/logs"}
                                    ],
                                    "resources": {
                                        "requests": {"memory": "1Gi", "cpu": "0.5"},
                                        "limits": {"memory": "2Gi", "cpu": "1"}
                                    }
                                }
                            ],
                                "volumes": [
                                {
                                    "name": "data",
                                    "persistentVolumeClaim": {"claimName": "dev-data-pvc"}
                                },
                                {"name": "logs", "emptyDir": {}}
                            ]
                        }
                    }
                }
            }

            response = batch_v1.create_namespaced_job(
                namespace=self.namespace,
                body=orchestrator_job
            )

            logger.info(f"Using orchestrator image: {orchestrator_image}")
            logger.info(f"Pipeline orchestration job submitted: {response.metadata.name}")
            self.set_last_run_time(datetime.now(timezone.utc))
            return True
        
        except Exception as e:
            logger.error(f"Failed to trigger pipeline: {e}")
            return False
    
    def run(self):
        """Main entry point - check and potentially trigger pipeline."""
        try:
            should_trigger, reason = self.should_trigger_pipeline()
            
            if should_trigger:
                logger.warning(f"✓ {reason}")
                success = self.trigger_pipeline()
                if success:
                    logger.info("✓ Pipeline triggered successfully")
                else:
                    logger.error("✗ Failed to trigger pipeline")
            else:
                logger.info(f"✗ {reason}")
            
            logger.info("=" * 70 + "\n")
        
        except Exception as e:
            logger.error(f"Trigger controller failed: {e}", exc_info=True)


def main():
    """Entry point."""
    controller = PipelineTriggerController()
    controller.run()


if __name__ == "__main__":
    main()
