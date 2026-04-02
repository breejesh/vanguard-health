#!/usr/bin/env python
"""
Local testing launcher for the production silver-to-gold job.
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


def run_production_gold_job() -> int:
    """Run the production silver-to-gold job inside the local Docker image."""
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
        "SILVER_PATH=/mnt/data/silver",
        "-e",
        "GOLD_PATH=/mnt/data/gold",
        "-v",
        f"{project_root.as_posix()}:/app",
        "-v",
        f"{local_data_path.as_posix()}:/mnt/data",
        "vanguard/spark-jobs:latest",
        "spark-submit",
        "--master",
        "local[4]",
        "--driver-memory",
        "2g",
        "--executor-memory",
        "2g",
        "/app/src/spark_jobs/silver_to_gold.py",
    ]

    silver_ts = os.getenv("SILVER_TS")
    if silver_ts:
        docker_command.extend(["-e", f"SILVER_TS={silver_ts}"])

    pipeline_run_id = os.getenv("PIPELINE_RUN_ID")
    if pipeline_run_id:
        docker_command.extend(["-e", f"PIPELINE_RUN_ID={pipeline_run_id}"])

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
    sys.exit(run_production_gold_job())

