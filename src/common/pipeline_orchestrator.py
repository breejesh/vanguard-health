"""
Main Pipeline Orchestrator - coordinates the full ETL pipeline
Entry point for pipeline orchestration job
"""
import sys
import os
import logging
from datetime import datetime

# Add src to path
sys.path.insert(0, '/app')

from src.common.logger import setup_logger
from src.common.config import get_config
from src.common.orchestrator import JobOrchestrator

logger = setup_logger(__name__)
config = get_config()


def get_ingestion_job_manifest():
    """Get K8s Job manifest for FHIR ingestion (Bronze population)."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "ingestion-main",
            "namespace": "vanguard-health"
        },
        "spec": {
            "ttlSecondsAfterFinished": 86400,
            "backoffLimit": 2,
            "activeDeadlineSeconds": 3600,
            "template": {
                "metadata": {
                    "labels": {"app": "fhir-ingestion", "stage": "bronze"}
                },
                "spec": {
                    "serviceAccountName": "vanguard-sa",
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "ingestion",
                            "image": "vanguard/ingestion:v1.0.0",
                            "imagePullPolicy": "IfNotPresent",
                            "envFrom": [
                                {"configMapRef": {"name": "app-config"}},
                                {"secretRef": {"name": "app-secrets"}}
                            ],
                            "env": [
                                {
                                    "name": "JOB_STAGE",
                                    "value": "bronze_ingestion"
                                },
                                {
                                    "name": "LOG_LEVEL",
                                    "value": "INFO"
                                }
                            ],
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/mnt/data"},
                                {"name": "logs", "mountPath": "/app/logs"}
                            ],
                            "resources": {
                                "requests": {"memory": "2Gi", "cpu": "1"},
                                "limits": {"memory": "4Gi", "cpu": "2"}
                            }
                        }
                    ],
                    "volumes": [
                        {
                            "name": "data",
                            "persistentVolumeClaim": {"claimName": "data-pvc"}
                        },
                        {"name": "logs", "emptyDir": {}}
                    ]
                }
            }
        }
    }


def get_bronze_to_silver_job_manifest():
    """Get K8s Job manifest for Bronze-to-Silver transformation."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "bronze-to-silver-main",
            "namespace": "vanguard-health"
        },
        "spec": {
            "ttlSecondsAfterFinished": 86400,
            "backoffLimit": 3,
            "activeDeadlineSeconds": 7200,  # 2 hours for Spark job
            "template": {
                "metadata": {
                    "labels": {"app": "bronze-to-silver", "stage": "silver"}
                },
                "spec": {
                    "serviceAccountName": "vanguard-sa",
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "spark-driver",
                            "image": "vanguard/spark-jobs:v1.0.0",
                            "imagePullPolicy": "IfNotPresent",
                            "command": [
                                "spark-submit",
                                "--master", "local[4]",
                                "--driver-memory", "2g",
                                "--executor-memory", "2g",
                                "--conf", "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
                                "--conf", "spark.eventLog.enabled=true",
                                "--conf", "spark.eventLog.dir=/app/logs/spark-events",
                                "src/spark_jobs/bronze_to_silver.py"
                            ],
                            "envFrom": [
                                {"configMapRef": {"name": "app-config"}},
                                {"secretRef": {"name": "app-secrets"}}
                            ],
                            "env": [
                                {"name": "JOB_STAGE", "value": "silver_transform"},
                                {"name": "LOG_LEVEL", "value": "INFO"}
                            ],
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/mnt/data"},
                                {"name": "logs", "mountPath": "/app/logs"}
                            ],
                            "resources": {
                                "requests": {"memory": "4Gi", "cpu": "2"},
                                "limits": {"memory": "8Gi", "cpu": "4"}
                            }
                        }
                    ],
                    "volumes": [
                        {
                            "name": "data",
                            "persistentVolumeClaim": {"claimName": "data-pvc"}
                        },
                        {"name": "logs", "emptyDir": {}}
                    ]
                }
            }
        }
    }


def get_silver_to_gold_job_manifest():
    """Get K8s Job manifest for Silver-to-Gold transformation."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "silver-to-gold-main",
            "namespace": "vanguard-health"
        },
        "spec": {
            "ttlSecondsAfterFinished": 86400,
            "backoffLimit": 2,
            "activeDeadlineSeconds": 7200,  # 2 hours
            "template": {
                "metadata": {
                    "labels": {"app": "silver-to-gold", "stage": "gold"}
                },
                "spec": {
                    "serviceAccountName": "vanguard-sa",
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "spark-driver",
                            "image": "vanguard/spark-jobs:v1.0.0",
                            "imagePullPolicy": "IfNotPresent",
                            "command": [
                                "spark-submit",
                                "--master", "local[4]",
                                "--driver-memory", "2g",
                                "--executor-memory", "2g",
                                "--conf", "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
                                "--conf", "spark.eventLog.enabled=true",
                                "--conf", "spark.eventLog.dir=/app/logs/spark-events",
                                "src/spark_jobs/silver_to_gold.py"
                            ],
                            "envFrom": [
                                {"configMapRef": {"name": "app-config"}},
                                {"secretRef": {"name": "app-secrets"}}
                            ],
                            "env": [
                                {"name": "JOB_STAGE", "value": "gold_transform"},
                                {"name": "LOG_LEVEL", "value": "INFO"}
                            ],
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/mnt/data"},
                                {"name": "logs", "mountPath": "/app/logs"}
                            ],
                            "resources": {
                                "requests": {"memory": "4Gi", "cpu": "2"},
                                "limits": {"memory": "8Gi", "cpu": "4"}
                            }
                        }
                    ],
                    "volumes": [
                        {
                            "name": "data",
                            "persistentVolumeClaim": {"claimName": "data-pvc"}
                        },
                        {"name": "logs", "emptyDir": {}}
                    ]
                }
            }
        }
    }


def main():
    """Main orchestrator entry point."""
    logger.info("=" * 70)
    logger.info(f"Pipeline Orchestrator Started - {datetime.now().isoformat()}")
    logger.info("=" * 70)
    
    try:
        # Initialize orchestrator
        orchestrator = JobOrchestrator(namespace="vanguard-health")
        
        # Get job manifests
        bronze_job = get_ingestion_job_manifest()
        silver_job = get_bronze_to_silver_job_manifest()
        gold_job = get_silver_to_gold_job_manifest()
        
        # Run orchestration
        success = orchestrator.orchestrate_pipeline(
            bronze_job_manifest=bronze_job,
            silver_job_manifest=silver_job,
            gold_job_manifest=gold_job,
            cleanup_on_complete=True
        )
        
        if success:
            logger.info("\n✓ PIPELINE COMPLETED SUCCESSFULLY")
            exit(0)
        else:
            logger.error("\n✗ PIPELINE FAILED")
            exit(1)
    
    except Exception as e:
        logger.error(f"Fatal orchestrator error: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
