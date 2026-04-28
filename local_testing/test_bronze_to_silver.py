#!/usr/bin/env python
"""
Local testing: Bronze → Silver transformation.
Uses production src code directly - calls main() with env vars.
No Docker, no MongoDB required.
"""
import io
import os
import sys
import tempfile
from pathlib import Path

# Set up Windows environment for Spark/Hadoop BEFORE any imports
if sys.platform == "win32":
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    # Point to local_testing directory (Hadoop will look for bin/ inside it)
    project_root_temp = Path(__file__).parent
    local_hadoop_home = str(project_root_temp).replace("\\", "/")
    os.environ["HADOOP_HOME"] = local_hadoop_home
    os.environ["hadoop.home.dir"] = local_hadoop_home
    os.environ["JAVA_TOOL_OPTIONS"] = f'-Dhadoop.home.dir="{local_hadoop_home}"'
    
    # Create bin directory for winutils.exe
    bin_dir = project_root_temp / "bin"
    bin_dir.mkdir(exist_ok=True)
    
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

project_root = Path(__file__).parent.parent
local_data_path = project_root / "local_testing" / "data"

# Add to path and set working directory
os.chdir(project_root)
sys.path.insert(0, str(project_root))

# Set paths for bronze/silver data
os.environ['BRONZE_PATH'] = str(local_data_path / "bronze")
os.environ['SILVER_PATH'] = str(local_data_path / "silver")

# Disable metadata manager for local testing (no MongoDB)
os.environ['USE_METADATA'] = "false"

# Bypass MongoDB env vars (won't be used with USE_METADATA=false)
os.environ['MONGODB_URI'] = 'mongodb://localhost:30017/?authSource=admin'
os.environ['MONGODB_DB'] = 'vanguard_metadata'

from src.spark_jobs.bronze_to_silver import main


def run_bronze_to_silver_local() -> int:
    """Run production bronze-to-silver main() on local data."""
    bronze_path = local_data_path / "bronze"
    silver_path = local_data_path / "silver"
    
    if not bronze_path.exists():
        print(f"ERROR: Bronze path not found: {bronze_path}")
        return 1
    
    silver_path.mkdir(parents=True, exist_ok=True)
    
    print("="*70)
    print("BRONZE → SILVER TRANSFORMATION (Local, Full Dataset)")
    print("="*70)
    print(f"Bronze:  {bronze_path}")
    print(f"Silver:  {silver_path}")
    print("Metadata: DISABLED (local testing)")
    print("="*70)
    print()
    
    # Call production main() directly
    return main()


if __name__ == "__main__":
    sys.exit(run_bronze_to_silver_local())
