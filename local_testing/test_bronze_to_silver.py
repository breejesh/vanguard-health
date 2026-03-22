#!/usr/bin/env python
"""
Local testing: Bronze to Silver transformation (thin wrapper)
Uses production transformation from src.spark_jobs

Usage:
    python local_testing/test_bronze_to_silver.py
"""
import sys
import os
from pathlib import Path

# Handle Unicode output on Windows
if sys.platform == "win32":
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.common.config import get_config
from src.common.logger import setup_logger
from src.spark_jobs.bronze_to_silver import transform_bronze_to_silver

logger = setup_logger(__name__)


def resolve_path(config_path: str, project_root: Path) -> Path:
    """Resolve path - treat as relative to project root if not Windows absolute"""
    p = Path(config_path)
    if (len(config_path) >= 2 and config_path[1] == ':') or config_path.startswith('\\\\'):
        return p
    if config_path.startswith('/') or config_path.startswith('\\'):
        config_path = config_path.lstrip('/\\')
    return project_root / config_path


def main():
    """Run Bronze to Silver transformation using production module"""
    
    config = get_config()
    bronze_path = resolve_path(config.BRONZE_PATH, project_root)
    silver_path = resolve_path(config.SILVER_PATH, project_root)
    
    logger.info("="*60)
    logger.info("Bronze to Silver Transformation (Local Testing)")
    logger.info("="*60)
    logger.info(f"Project Root: {project_root}")
    logger.info(f"Bronze Path: {bronze_path}")
    logger.info(f"Silver Path: {silver_path}")
    
    # Verify bronze directory exists
    if not bronze_path.exists():
        logger.error(f"[ERROR] Bronze directory not found: {bronze_path}")
        logger.info("")
        logger.info("Please first fetch data:")
        logger.info(f"  python local_testing/test_fetch_api.py")
        return 1
    
    # Check for data
    bronze_files = list(bronze_path.rglob("*.json"))
    if not bronze_files:
        logger.error("[ERROR] No JSON files found in bronze directory")
        return 1
    
    logger.info(f"[OK] Found {len(bronze_files)} JSON files in bronze layer")
    logger.info("")
    
    # Call production transformation function
    try:
        exit_code, message = transform_bronze_to_silver(str(bronze_path), str(silver_path))
        
        if exit_code == 0:
            logger.info(message)
            logger.info("")
            logger.info("="*60)
            logger.info("[OK] Transformation completed successfully!")
            logger.info("="*60)
            logger.info(f"Silver layer output: {silver_path}")
        else:
            logger.warning(f"[WARN] {message}")
        
        return exit_code
    
    except Exception as e:
        logger.error(f"[ERROR] Transformation failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
