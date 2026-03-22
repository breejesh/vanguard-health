#!/usr/bin/env python
"""
Local testing: Fetch FHIR data from API (thin wrapper)
Uses production FHIRFetcher class from src.ingestion

Usage:
    python local_testing/test_fetch_api.py
"""
import sys
import os
import logging
import json
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

from src.ingestion.fhir_fetcher import FHIRFetcher
from src.common.logger import setup_logger

logger = setup_logger(__name__)
# Enable debug logging to see API calls
logging.getLogger('src.ingestion.fhir_fetcher').setLevel(logging.DEBUG)


def main():
    """Fetch FHIR data using production FHIRFetcher"""
    
    logger.info("="*60)
    logger.info("FHIR API Fetching (Local Testing)")
    logger.info("="*60)
    logger.info(f"Project Root: {project_root}")
    
    try:
        # Initialize FHIR fetcher (r4.smarthealthit.org is a public FHIR R4 server)
        fetcher = FHIRFetcher(
            api_url="https://r4.smarthealthit.org",
            api_key=""  # Public API, no auth needed
        )
        
        logger.info("[OK] FHIRFetcher initialized")
        logger.info("")
        
        # Test connection
        logger.info("Testing API connection...")
        try:
            fetcher.test_connection()
            logger.info("[OK] API connection successful")
        except Exception as e:
            logger.warning(f"[WARN] Connection test failed: {e}")
        
        logger.info("")
        logger.info("Fetching FHIR resources...")
        
        # Fetch resources (bulk fetch for local testing - no date filtering)
        resources = fetcher.fetch_bulk()
        
        if resources:
            logger.info(f"[OK] Fetched {len(resources)} total resources")
            
            # Group by resource type and save to bronze layer
            bronze_path = Path('data/bronze')
            bronze_path.mkdir(parents=True, exist_ok=True)
            
            by_type = {}
            for resource in resources:
                resource_type = resource.get('resourceType', 'Unknown')
                if resource_type not in by_type:
                    by_type[resource_type] = []
                by_type[resource_type].append(resource)
                
                # Save to bronze layer
                resource_dir = bronze_path / resource_type
                resource_dir.mkdir(exist_ok=True)
                resource_id = resource.get('id', 'unknown')
                file_path = resource_dir / f"{resource_type}_{resource_id}.json"
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(resource, f, indent=2, default=str)
            
            logger.info("")
            logger.info("Saved resources to bronze layer:")
            for resource_type, items in sorted(by_type.items()):
                logger.info(f"  {resource_type:20s}: {len(items):3d} records saved to data/bronze/{resource_type}/")
        else:
            logger.warning("[WARN] No resources fetched")
        
        logger.info("")
        logger.info("="*60)
        logger.info("[OK] FHIR API fetch completed")
        logger.info("="*60)
        
        return 0
    
    except Exception as e:
        logger.error(f"[ERROR] Fetch failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
