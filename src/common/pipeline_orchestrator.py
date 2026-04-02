"""
Main Pipeline Orchestrator - coordinates the full ETL pipeline
Entry point for pipeline orchestration job
"""
import sys
import os
import logging
import shlex
import uuid
from datetime import datetime

# Add src to path
sys.path.insert(0, '/app')

from src.common.logger import setup_logger
from src.common.config import get_config
from src.common.orchestrator import JobOrchestrator
from src.ingestion.metadata_manager import MetadataManager

logger = setup_logger(__name__)
config = get_config()
SPARK_EVENT_LOG_DIR = "/app/logs/spark-events"


def build_spark_submit_command(script_path: str) -> list[str]:
    """Build a shell command that prepares Spark's event-log directory first."""
    spark_submit_args = [
        "spark-submit",
        "--master", "local[4]",
        "--driver-memory", "2g",
        "--executor-memory", "2g",
        "--conf", "spark.eventLog.enabled=true",
        "--conf", f"spark.eventLog.dir={SPARK_EVENT_LOG_DIR}",
        script_path,
    ]

    return [
        "/bin/sh",
        "-c",
        "mkdir -p {log_dir} && exec {command}".format(
            log_dir=shlex.quote(SPARK_EVENT_LOG_DIR),
            command=" ".join(shlex.quote(arg) for arg in spark_submit_args),
        ),
    ]


def inject_job_env(job_manifest: dict, extra_env: dict) -> dict:
    """Inject or overwrite env vars on the first container in a job manifest."""
    if not job_manifest or not extra_env:
        return job_manifest

    container = job_manifest["spec"]["template"]["spec"]["containers"][0]
    env_list = container.setdefault("env", [])

    for name, value in extra_env.items():
        updated = False
        for item in env_list:
            if item.get("name") == name:
                item["value"] = value
                updated = True
                break
        if not updated:
            env_list.append({"name": name, "value": value})

    return job_manifest


def get_ingestion_job_manifest(run_suffix: str = ""):
    """Get K8s Job manifest for FHIR ingestion (Bronze population)."""
    job_name = "ingestion-main" if not run_suffix else f"ingestion-main-{run_suffix}"
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
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
                            "image": "vanguard/ingestion:latest",
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
                            "env": [
                                {"name": "JOB_STAGE", "value": "bronze_ingestion"},
                                {"name": "LOG_LEVEL", "value": "INFO"},
                                {"name": "PYTHONUNBUFFERED", "value": "1"},
                                {"name": "PYTHONPATH", "value": "/app"}
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
                            "persistentVolumeClaim": {"claimName": "dev-data-pvc"}
                        },
                        {"name": "logs", "emptyDir": {}}
                    ]
                }
            }
        }
    }


def get_bronze_to_silver_job_manifest(run_suffix: str = ""):
    """Get K8s Job manifest for Bronze-to-Silver transformation."""
    job_name = "bronze-to-silver-main" if not run_suffix else f"bronze-to-silver-main-{run_suffix}"
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
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
                            "image": "vanguard/spark-jobs:latest",
                            "imagePullPolicy": "IfNotPresent",
                            "command": build_spark_submit_command("src/spark_jobs/bronze_to_silver.py"),
                            "envFrom": [
                                {"configMapRef": {"name": "app-config"}},
                                {"secretRef": {"name": "app-secrets"}}
                            ],
                            "env": [
                                {"name": "JOB_STAGE", "value": "silver_transform"},
                                {"name": "LOG_LEVEL", "value": "INFO"},
                                {"name": "PYTHONUNBUFFERED", "value": "1"},
                                {"name": "PYTHONPATH", "value": "/app"}
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
                            "persistentVolumeClaim": {"claimName": "dev-data-pvc"}
                        },
                        {"name": "logs", "emptyDir": {}}
                    ]
                }
            }
        }
    }


def get_silver_to_gold_job_manifest(run_suffix: str = ""):
    """Get K8s Job manifest for Silver-to-Gold transformation."""
    job_name = "silver-to-gold-main" if not run_suffix else f"silver-to-gold-main-{run_suffix}"
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
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
                            "image": "vanguard/spark-jobs:latest",
                            "imagePullPolicy": "IfNotPresent",
                            "command": build_spark_submit_command("src/spark_jobs/silver_to_gold.py"),
                            "envFrom": [
                                {"configMapRef": {"name": "app-config"}},
                                {"secretRef": {"name": "app-secrets"}}
                            ],
                            "env": [
                                {"name": "JOB_STAGE", "value": "gold_transform"},
                                {"name": "LOG_LEVEL", "value": "INFO"},
                                {"name": "PYTHONUNBUFFERED", "value": "1"},
                                {"name": "PYTHONPATH", "value": "/app"}
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
                            "persistentVolumeClaim": {"claimName": "dev-data-pvc"}
                        },
                        {"name": "logs", "emptyDir": {}}
                    ]
                }
            }
        }
    }


def get_fetcher_job_manifest(run_suffix: str = ""):
    """Job manifest to run the fetcher as the first stage."""
    job_name = "ingestion-fetcher-main" if not run_suffix else f"ingestion-fetcher-main-{run_suffix}"
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": "vanguard-health"
        },
        "spec": {
            "ttlSecondsAfterFinished": 86400,
            "backoffLimit": 1,
            "activeDeadlineSeconds": 3600,
            "template": {
                "metadata": {"labels": {"app": "ingestion-fetcher", "stage": "fetch"}},
                "spec": {
                    "serviceAccountName": "vanguard-sa",
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "fetcher",
                            "image": "vanguard/ingestion:latest",
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["python", "-m", "src.ingestion.fetcher_job"],
                            "envFrom": [
                                {"configMapRef": {"name": "app-config"}},
                                {"secretRef": {"name": "app-secrets"}}
                            ],
                            "env": [
                                {"name": "LOG_LEVEL", "value": "INFO"},
                                {"name": "PYTHONUNBUFFERED", "value": "1"},
                                {"name": "PYTHONPATH", "value": "/app"}
                            ],
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/mnt/data"},
                                {"name": "logs", "mountPath": "/app/logs"}
                            ],
                            "resources": {"requests": {"memory": "1Gi", "cpu": "500m"}, "limits": {"memory": "2Gi", "cpu": "1"}}
                        }
                    ],
                    "volumes": [
                        {"name": "data", "persistentVolumeClaim": {"claimName": "dev-data-pvc"}},
                        {"name": "logs", "emptyDir": {}}
                    ]
                }
            }
        }
    }


def get_firebase_job_manifest(run_suffix: str = ""):
    """Job manifest to push gold outputs to Firebase."""
    job_name = "firebase-push-main" if not run_suffix else f"firebase-push-main-{run_suffix}"
    firebase_pusher_image = os.getenv("FIREBASE_PUSHER_IMAGE", "vanguard/ingestion:latest")
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name, "namespace": "vanguard-health"},
        "spec": {
            "ttlSecondsAfterFinished": 86400,
            "backoffLimit": 1,
            "activeDeadlineSeconds": 1800,
            "template": {
                "metadata": {"labels": {"app": "firebase-push", "stage": "push"}},
                "spec": {
                    "serviceAccountName": "vanguard-sa",
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "firebase-pusher",
                            "image": firebase_pusher_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["python", "-m", "src.common.firebase_pusher"],
                            "envFrom": [
                                {"configMapRef": {"name": "app-config"}},
                                {"secretRef": {"name": "app-secrets"}}
                            ],
                                "env": [
                                    {"name": "LOG_LEVEL", "value": "INFO"},
                                    {"name": "GOLD_PATH", "value": "/mnt/data/gold"},
                                    {"name": "PYTHONUNBUFFERED", "value": "1"},
                                    {"name": "PYTHONPATH", "value": "/app"}
                                ],
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/mnt/data"},
                                {"name": "logs", "mountPath": "/app/logs"}
                            ],
                            "resources": {"requests": {"memory": "512Mi", "cpu": "250m"}, "limits": {"memory": "1Gi", "cpu": "500m"}}
                        }
                    ],
                    "volumes": [
                        {"name": "data", "persistentVolumeClaim": {"claimName": "dev-data-pvc"}},
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
        pipeline_run_id = os.getenv("PIPELINE_RUN_ID", str(uuid.uuid4()))
        pipeline_run_ts = os.getenv("PIPELINE_RUN_TS", str(int(datetime.utcnow().timestamp() * 1000)))
        run_suffix = pipeline_run_ts[-8:]

        # Initialize orchestrator
        orchestrator = JobOrchestrator(namespace="vanguard-health")
        metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)

        metadata_manager.upsert_pipeline_run(
            pipeline_run_id,
            {
                "status": "running",
                "run_ts": pipeline_run_ts,
                "started_at": datetime.utcnow(),
            },
        )
        # Get job manifests
        # Allow skipping ingestion when the orchestrator is triggered after a standalone fetcher job
        skip_ingestion = os.environ.get("SKIP_INGESTION", "false").lower() == "true"
        run_legacy_ingestion = os.environ.get("RUN_LEGACY_INGESTION", "false").lower() == "true"
        if skip_ingestion:
            logger.info("SKIP_INGESTION=true: orchestrator will skip fetcher/bronze ingestion stages")
            fetcher_job = None
            bronze_job = None
        else:
            fetcher_job = get_fetcher_job_manifest(run_suffix)
            if run_legacy_ingestion:
                bronze_job = get_ingestion_job_manifest(run_suffix)
                logger.info("RUN_LEGACY_INGESTION=true: running legacy ingestion-main stage in addition to fetcher")
            else:
                bronze_job = None
                logger.info("Default mode: fetcher stage enabled, legacy ingestion-main stage skipped")

        silver_job = get_bronze_to_silver_job_manifest(run_suffix)
        gold_job = get_silver_to_gold_job_manifest(run_suffix)
        firebase_job = get_firebase_job_manifest(run_suffix)

        # Pipeline context passed into each job so all stage outputs are linked.
        common_env = {
            "PIPELINE_RUN_ID": pipeline_run_id,
            "PIPELINE_RUN_TS": pipeline_run_ts,
        }

        if fetcher_job:
            inject_job_env(fetcher_job, {**common_env, "BRONZE_TS": pipeline_run_ts})
        if bronze_job:
            inject_job_env(bronze_job, {**common_env, "BRONZE_TS": pipeline_run_ts})
        if silver_job:
            inject_job_env(
                silver_job,
                {
                    **common_env,
                    "BRONZE_TS": pipeline_run_ts,
                    "SILVER_TS": pipeline_run_ts,
                },
            )
        if gold_job:
            inject_job_env(gold_job, {**common_env, "SILVER_TS": pipeline_run_ts})
        if firebase_job:
            inject_job_env(firebase_job, common_env)

        metadata_manager.set_pipeline_state(
            "pipeline_latest_run",
            {
                "pipeline_run_id": pipeline_run_id,
                "run_ts": pipeline_run_ts,
                "skip_ingestion": skip_ingestion,
                "updated_at": datetime.utcnow(),
            },
        )

        logger.info(f"Pipeline run ID: {pipeline_run_id}")
        logger.info(f"Pipeline run timestamp (epoch ms): {pipeline_run_ts}")

        # Run orchestration (fetcher first)
        success = orchestrator.orchestrate_pipeline(
            fetcher_job_manifest=fetcher_job,
            bronze_job_manifest=bronze_job,
            silver_job_manifest=silver_job,
            gold_job_manifest=gold_job,
            firebase_job_manifest=firebase_job,
            cleanup_on_complete=True
        )
        
        if success:
            metadata_manager.upsert_pipeline_run(
                pipeline_run_id,
                {
                    "status": "completed",
                    "completed_at": datetime.utcnow(),
                },
            )
            logger.info("\n✓ PIPELINE COMPLETED SUCCESSFULLY")
            exit(0)
        else:
            metadata_manager.upsert_pipeline_run(
                pipeline_run_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                },
            )
            logger.error("\n✗ PIPELINE FAILED")
            exit(1)
    
    except Exception as e:
        try:
            run_id = os.getenv("PIPELINE_RUN_ID")
            if run_id:
                metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)
                metadata_manager.upsert_pipeline_run(
                    run_id,
                    {
                        "status": "failed",
                        "completed_at": datetime.utcnow(),
                        "error": str(e),
                    },
                )
        except Exception:
            pass
        logger.error(f"Fatal orchestrator error: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
