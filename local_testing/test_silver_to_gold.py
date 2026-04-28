#!/usr/bin/env python
"""
Local testing: Silver → Gold transformation.
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
    # Set up Hadoop home to avoid "HADOOP_HOME and hadoop.home.dir are unset" error
    # Use forward slashes for Java compatibility
    temp_hadoop_home = tempfile.gettempdir().replace("\\", "/")
    os.environ["HADOOP_HOME"] = temp_hadoop_home
    os.environ["hadoop.home.dir"] = temp_hadoop_home
    os.environ["JAVA_TOOL_OPTIONS"] = "-Dhadoop.home.dir=" + temp_hadoop_home
    
    # Create bin directory to prevent "Hadoop bin directory does not exist" error
    bin_dir = os.path.join(tempfile.gettempdir(), "bin")
    os.makedirs(bin_dir, exist_ok=True)
    
    # Add venv site-packages to PYTHONPATH for Spark workers
    venv_site_packages = Path(__file__).parent.parent / ".venv" / "Lib" / "site-packages"
    if venv_site_packages.exists():
        os.environ['PYTHONPATH'] = str(venv_site_packages) + ";" + os.environ.get('PYTHONPATH', '')
    
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

project_root = Path(__file__).parent.parent
local_data_path = project_root / "local_testing" / "data"

# Add to path and set working directory
os.chdir(project_root)
sys.path.insert(0, str(project_root))

# Set paths for silver/gold data
os.environ['SILVER_PATH'] = str(local_data_path / "silver")
os.environ['GOLD_PATH'] = str(local_data_path / "gold")

# Disable metadata manager for local testing (no MongoDB)
os.environ['USE_METADATA'] = "false"

# Bypass MongoDB env vars (won't be used with USE_METADATA=false)
os.environ['MONGODB_URI'] = 'mongodb://localhost:30017/?authSource=admin'
os.environ['MONGODB_DB'] = 'vanguard_metadata'

from src.spark_jobs.silver_to_gold import main


def run_silver_to_gold_local() -> int:
    """Run production silver-to-gold main() on local data."""
    silver_path = local_data_path / "silver"
    gold_path = local_data_path / "gold"
    
    if not silver_path.exists():
        print(f"ERROR: Silver path not found: {silver_path}")
        return 1
    
    gold_path.mkdir(parents=True, exist_ok=True)
    
    print("="*70)
    print("SILVER → GOLD TRANSFORMATION (Local, Full Dataset)")
    print("="*70)
    print(f"Silver:  {silver_path}")
    print(f"Gold:    {gold_path}")
    print("Metadata: DISABLED (local testing)")
    print("="*70)
    print()
    
    # Call production main() directly
    return main()


if __name__ == "__main__":
    sys.exit(run_silver_to_gold_local())

