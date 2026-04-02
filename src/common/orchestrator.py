"""
Kubernetes Job Orchestrator - manages job dependencies and execution flow.
Triggers jobs sequentially: bronze -> silver -> gold
"""
import logging
import time
from datetime import datetime, timedelta
from typing import Optional
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class JobOrchestrator:
    """Orchestrates sequential job execution with dependency management."""
    
    def __init__(self, namespace: str = "vanguard-health"):
        """Initialize K8s client."""
        try:
            # Try in-cluster config first (when running in K8s)
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except config.ConfigException:
            # Fall back to local config if not in K8s
            config.load_kube_config()
            logger.info("Loaded local Kubernetes config")
        
        self.batch_v1 = client.BatchV1Api()
        self.v1 = client.CoreV1Api()
        self.namespace = namespace
    
    def create_job(self, job_manifest: dict) -> Optional[str]:
        """Create a Kubernetes Job and return job name."""
        try:
            job_name = job_manifest["metadata"]["name"]
            logger.info(f"Creating job: {job_name}")
            
            response = self.batch_v1.create_namespaced_job(
                namespace=self.namespace,
                body=job_manifest
            )
            logger.info(f"Job created successfully: {job_name}")
            return job_name
        except ApiException as e:
            # If the Job already exists (409), log and return the existing name so
            # the orchestrator can wait for it instead of failing outright.
            if e.status == 409:
                logger.warning(f"Job {job_name} already exists (409). Proceeding to wait for completion.")
                return job_name
            logger.error(f"Failed to create job: {e}")
            raise
    
    def get_job_status(self, job_name: str) -> dict:
        """Get job status (PENDING, RUNNING, SUCCEEDED, FAILED)."""
        try:
            job = self.batch_v1.read_namespaced_job(job_name, self.namespace)
            conditions = {condition.type: condition.status for condition in (job.status.conditions or [])}
            
            status = {
                "name": job_name,
                "active": job.status.active or 0,
                "succeeded": job.status.succeeded or 0,
                "failed": job.status.failed or 0,
                "start_time": job.status.start_time,
                "completion_time": job.status.completion_time,
            }
            
            # Determine overall status
            if conditions.get("Complete") == "True" or (job.status.succeeded and job.status.succeeded > 0):
                status["state"] = "SUCCEEDED"
            elif conditions.get("Failed") == "True":
                status["state"] = "FAILED"
            elif (job.status.active and job.status.active > 0) or (job.status.failed and job.status.failed > 0):
                status["state"] = "RUNNING"
            else:
                status["state"] = "PENDING"
            
            return status
        except ApiException as e:
            if e.status == 404:
                return {"name": job_name, "state": "NOT_FOUND"}
            logger.error(f"Error getting job status: {e}")
            raise
    
    def wait_for_job(self, job_name: str, timeout_seconds: int = 3600, check_interval: int = 10) -> bool:
        """Wait for job to complete, return True if succeeded."""
        logger.info(f"Waiting for job {job_name} to complete (timeout: {timeout_seconds}s)")
        
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status = self.get_job_status(job_name)
            
            if status["state"] == "SUCCEEDED":
                logger.info(f"Job {job_name} SUCCEEDED")
                return True
            elif status["state"] == "FAILED":
                logger.error(f"Job {job_name} FAILED")
                # Get pod logs for debugging
                self.get_job_logs(job_name)
                return False
            elif status["state"] == "RUNNING":
                logger.info(f"Job {job_name} still running... (elapsed: {int(time.time() - start_time)}s)")
            
            time.sleep(check_interval)
        
        logger.error(f"Job {job_name} timeout after {timeout_seconds}s")
        return False
    
    def get_job_logs(self, job_name: str, lines: int = 100):
        """Retrieve and log job pod logs."""
        try:
            # Get pods for this job
            pods = self.v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"job-name={job_name}"
            )
            
            for pod in pods.items:
                logger.info(f"\n--- Logs from pod {pod.metadata.name} ---")
                try:
                    log = self.v1.read_namespaced_pod_log(
                        name=pod.metadata.name,
                        namespace=self.namespace,
                        tail_lines=lines
                    )
                    logger.info(log)
                except Exception as e:
                    logger.warning(f"Could not read logs: {e}")
        except Exception as e:
            logger.warning(f"Error retrieving logs: {e}")
    
    def cleanup_job(self, job_name: str, delete_pods: bool = True):
        """Clean up completed job and optionally its pods."""
        try:
            propagation_policy = "Background" if delete_pods else "Orphan"
            self.batch_v1.delete_namespaced_job(
                name=job_name,
                namespace=self.namespace,
                propagation_policy=propagation_policy
            )
            logger.info(f"Deleted job {job_name}")
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"Error deleting job: {e}")
    
    def orchestrate_pipeline(self,
                            fetcher_job_manifest: dict,
                            bronze_job_manifest: dict,
                            silver_job_manifest: dict,
                            gold_job_manifest: dict,
                            firebase_job_manifest: dict = None,
                            cleanup_on_complete: bool = True) -> bool:
        """Run full pipeline: fetcher -> bronze -> silver -> gold -> firebase.

        All manifests are full Kubernetes Job manifests. Any manifest may be None
        to skip that stage.
        """
        logger.info("=" * 60)
        logger.info("Starting pipeline orchestration")
        logger.info("=" * 60)
        
        try:
            # Stage 1: Fetcher (optional)
            if fetcher_job_manifest:
                fetcher_name = fetcher_job_manifest["metadata"]["name"]
                logger.info(f"\n[STAGE 1/5] Triggering Fetcher job: {fetcher_name}")
                self.create_job(fetcher_job_manifest)
                if not self.wait_for_job(fetcher_name, timeout_seconds=3600):
                    logger.error("Fetcher job failed, aborting pipeline")
                    return False
                logger.info("✓ Fetcher job completed successfully")
            else:
                logger.info("\n[STAGE 1/5] Skipping Fetcher (manifest not provided)")

            # Stage 2: Bronze ingestion (optional)
            if bronze_job_manifest:
                bronze_name = bronze_job_manifest["metadata"]["name"]
                logger.info(f"\n[STAGE 2/5] Triggering Bronze ingestion: {bronze_name}")
                self.create_job(bronze_job_manifest)
                if not self.wait_for_job(bronze_name, timeout_seconds=3600):
                    logger.error("Bronze job failed, aborting pipeline")
                    return False
                logger.info(f"✓ Bronze job completed successfully")
            else:
                logger.info("\n[STAGE 2/5] Skipping Bronze ingestion (manifest not provided)")

            # Stage 3: Bronze to Silver transformation
            if silver_job_manifest:
                silver_name = silver_job_manifest["metadata"]["name"]
                logger.info(f"\n[STAGE 3/5] Triggering Bronze-to-Silver transformation: {silver_name}")
                self.create_job(silver_job_manifest)
                if not self.wait_for_job(silver_name, timeout_seconds=7200):
                    logger.error("Silver job failed, aborting pipeline")
                    return False
                logger.info(f"✓ Silver job completed successfully")
            else:
                logger.info("\n[STAGE 3/5] Skipping Silver transformation (manifest not provided)")

            # Stage 4: Silver to Gold transformation
            if gold_job_manifest:
                gold_name = gold_job_manifest["metadata"]["name"]
                logger.info(f"\n[STAGE 4/5] Triggering Silver-to-Gold transformation: {gold_name}")
                self.create_job(gold_job_manifest)
                if not self.wait_for_job(gold_name, timeout_seconds=7200):
                    logger.error("Gold job failed")
                logger.info(f"✓ Gold job completed successfully")
            else:
                logger.info("\n[STAGE 4/5] Skipping Gold transformation (manifest not provided)")

            # Stage 5: Firebase push (optional)
            if firebase_job_manifest:
                fb_name = firebase_job_manifest["metadata"]["name"]
                logger.info(f"\n[STAGE 5/5] Triggering Firebase push job: {fb_name}")
                self.create_job(firebase_job_manifest)
                if not self.wait_for_job(fb_name, timeout_seconds=1800):
                    logger.error("Firebase push job failed")
                    # Do not abort pipeline; log and continue
                logger.info("✓ Firebase push completed (or finished with errors logged)")
            else:
                logger.info("\n[STAGE 5/5] Skipping Firebase push (manifest not provided)")
            
            logger.info("\n" + "=" * 60)
            logger.info("Pipeline orchestration completed successfully!")
            logger.info("=" * 60)
            
            # Cleanup
            if cleanup_on_complete:
                logger.info("\nCleaning up job resources...")
                self.cleanup_job(bronze_name)
                self.cleanup_job(silver_name)
                self.cleanup_job(gold_name)
            
            return True
        
        except Exception as e:
            logger.error(f"Pipeline orchestration failed: {e}")
            return False
