#!/usr/bin/env python
"""
Local testing launcher for the production bronze-to-silver job.
"""
import io
import os
import subprocess
import sys
from pathlib import Path

# Handle Unicode output on Windows
if sys.platform == "win32":
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

project_root = Path(__file__).parent.parent
local_data_path = project_root / "local_testing" / "data"


def run_production_spark_job(run_ts: str = None) -> int:
    """Run the production Spark job inside the local Docker image."""
    docker_command = [
        "docker",
        "run",
        "--rm",
        "-e",
        "PYTHONUNBUFFERED=1",
        "-e",
        "PYTHONIOENCODING=utf-8",
        "-e",
        "MONGODB_URI=mongodb://root:mongopass@host.docker.internal:30017/vanguard_metadata?authSource=admin",
        "-e",
        "MONGODB_DB=vanguard_metadata",
        "-e",
        "DATA_PATH=/mnt/data",
        "-e",
        "BRONZE_PATH=/mnt/data/bronze",
        "-e",
        "SILVER_PATH=/mnt/data/silver",
        "-v",
        f"{project_root.as_posix()}:/app",
        "-v",
        f"{local_data_path.as_posix()}:/mnt/data",
        "vanguard/spark-jobs:latest",
    ]

    if run_ts:
        docker_command.extend(["-e", f"BRONZE_TS={run_ts}"])
        docker_command.extend(["-e", f"SILVER_TS={run_ts}"])

    pipeline_run_id = os.getenv("PIPELINE_RUN_ID")
    if pipeline_run_id:
        docker_command.extend(["-e", f"PIPELINE_RUN_ID={pipeline_run_id}"])
    if run_ts:
        docker_command.extend(["-e", f"PIPELINE_RUN_TS={run_ts}"])

    docker_command.extend([
        "spark-submit",
        "--master",
        "local[4]",
        "--driver-memory",
        "2g",
        "--executor-memory",
        "2g",
        "/app/src/spark_jobs/bronze_to_silver.py",
    ])

    completed = subprocess.run(
        docker_command,
        cwd=project_root,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return completed.returncode


if __name__ == "__main__":
    timestamp_arg = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(run_production_spark_job(timestamp_arg))
