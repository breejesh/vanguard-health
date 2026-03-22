#!/usr/bin/env python
"""
Local testing: Silver to Gold transformation (thin wrapper)
Creates geographic disease analysis for pandemic hotspot visualization
Uses production transformation from src.spark_jobs

Usage:
    python local_testing/test_silver_to_gold.py
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
from src.spark_jobs.silver_to_gold import transform_silver_to_gold

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
    """Run Silver to Gold transformation using production module"""
    
    config = get_config()
    silver_path = resolve_path(config.SILVER_PATH, project_root)
    gold_path = resolve_path(getattr(config, 'GOLD_PATH', '/data/gold'), project_root)
    
    logger.info("="*60)
    logger.info("Silver to Gold Transformation (Local Testing)")
    logger.info("="*60)
    logger.info(f"Project Root: {project_root}")
    logger.info(f"Silver Path: {silver_path}")
    logger.info(f"Gold Path: {gold_path}")
    logger.info("")
    
    # Verify silver directory exists
    if not silver_path.exists():
        logger.error(f"[ERROR] Silver directory not found: {silver_path}")
        logger.info("")
        logger.info("Please first transform Bronze to Silver:")
        logger.info(f"  python local_testing/test_bronze_to_silver.py")
        return 1
    
    logger.info("Calling production transformation function...")
    
    # Call production transformation function
    try:
        exit_code, message = transform_silver_to_gold(str(silver_path), str(gold_path))
        
        if exit_code == 0:
            logger.info(message)
            logger.info("")
            logger.info("="*60)
            logger.info("[OK] Transformation completed successfully!")
            logger.info("="*60)
            logger.info(f"Gold layer output: {gold_path}")
            logger.info("")
            logger.info("Generated UI-ready JSON files:")
            logger.info("")
            logger.info("MAP VIEW:")
            logger.info("  • disease_hotspots.geojson")
            logger.info("    >>> GeoJSON with Point features (condition hotspots)")
            logger.info("    >>> Load into Leaflet/Mapbox for interactive map")
            logger.info("")
            logger.info("DASHBOARD STATS:")
            logger.info("  • disease_stats.json - Global metrics")
            logger.info("  • conditions_list.json - All unique conditions (for filters)")
            logger.info("")
            logger.info("GEOGRAPHIC FILTERS (choose your level):")
            logger.info("  • disease_by_state.json - Cases grouped by STATE")
            logger.info("  • disease_by_city.json - Cases grouped by CITY")
            logger.info("  • disease_by_zipcode.json - Cases grouped by ZIPCODE")
            logger.info("  • disease_by_condition.json - Cases grouped by CONDITION")
            logger.info("  • disease_by_region.json - Cases by region (legacy)")
            logger.info("")
            logger.info("TIME SERIES:")
            logger.info("  • timeline_data.json - Cases over time + filter query examples")
            logger.info("")
            logger.info("TIME-WINDOWED FILTERS (for UI):")
            logger.info("  When user selects time window, load corresponding file:")
            logger.info("  • disease_hotspots_1h.json + disease_stats_1h.json (last hour)")
            logger.info("  • disease_hotspots_6h.json + disease_stats_6h.json (last 6 hours)")
            logger.info("  • disease_hotspots_1d.json + disease_stats_1d.json (last 24 hours)")
            logger.info("  • disease_hotspots_3d.json + disease_stats_3d.json (last 3 days)")
            logger.info("  • disease_hotspots_1w.json + disease_stats_1w.json (last week) <---")
            logger.info("  • disease_hotspots_1mo.json + disease_stats_1mo.json (last month)")
            logger.info("  • disease_hotspots_6mo.json + disease_stats_6mo.json (last 6 months)")
            logger.info("  • disease_hotspots_1y.json + disease_stats_1y.json (last year)")
            logger.info("")
            logger.info("HOW TIME FILTERING WORKS:")
            logger.info("  1. User selects 'Last 1 Week' in UI time filter dropdown")
            logger.info("  2. UI loads: disease_hotspots_1w.json")
            logger.info("  3. Map updates to show ONLY cases from last 7 days")
            logger.info("  4. UI loads: disease_stats_1w.json")
            logger.info("  5. Dashboard stats update (case count, regions, etc.)")
            logger.info("  6. Hotspot intensity recalculated based on filtered data")
            logger.info("")
            logger.info("Firebase Ready:")
            logger.info("  [*] All data in JSON format")
            logger.info("  [*] ISO timestamps for time-based queries")
            logger.info("  [*] Normalized field names for Firestore")
        else:
            logger.warning(f"[WARN] {message}")
        
        return exit_code
    
    except Exception as e:
        logger.error(f"[ERROR] Transformation failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
