#!/usr/bin/env python
"""
Local testing launcher for the production fetcher job.
"""
import io
import os
import runpy
import sys
import uuid
from datetime import datetime
from pathlib import Path

# Handle Unicode output on Windows
if sys.platform == "win32":
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))


if __name__ == "__main__":
    os.environ.setdefault("PIPELINE_RUN_ID", str(uuid.uuid4()))
    os.environ.setdefault("BRONZE_TS", str(int(datetime.utcnow().timestamp() * 1000)))
    print(f"PIPELINE_RUN_ID={os.environ['PIPELINE_RUN_ID']}")
    print(f"BRONZE_TS={os.environ['BRONZE_TS']}")
    runpy.run_module("src.ingestion.fetcher_job", run_name="__main__")
