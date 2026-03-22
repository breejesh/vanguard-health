"""
Main entry point for FHIR data ingestion from Synthea API.
"""
import sys
import logging
from datetime import datetime, timedelta

from src.common.config import get_config
from src.common.logger import setup_logger
from src.ingestion.fhir_fetcher import FHIRFetcher
from src.ingestion.hudi_writer import HudiWriter
from src.ingestion.metadata_manager import MetadataManager

logger = setup_logger(__name__)
config = get_config()


def main():
    """Main ingestion pipeline."""
    try:
        logger.info("Starting FHIR data ingestion pipeline")
        
        # Initialize components
        fetcher = FHIRFetcher(config.SYNTHEA_API_URL, config.SYNTHEA_API_KEY)
        metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)
        
        # Get last cursor for incremental fetch
        last_cursor = metadata_manager.get_last_cursor("patient_ingestion")
        if last_cursor is None:
            last_cursor = datetime.now() - timedelta(hours=config.SYNTHEA_FETCH_INTERVAL_HOURS)
        
        logger.info(f"Fetching FHIR resources modified since {last_cursor}")
        
        # Fetch data
        resources = fetcher.fetch_incremental(last_cursor)
        logger.info(f"Fetched {len(resources)} resources from Synthea API")
        
        if not config.TEST_MODE:
            # Write to Bronze (Hudi)
            writer = HudiWriter(config.BRONZE_PATH)
            writer.write_to_bronze(resources)
            
            # Update cursor
            metadata_manager.update_cursor("patient_ingestion", datetime.now())
            
            # Record job
            metadata_manager.record_job(
                "ingestion_patient",
                "completed",
                len(resources)
            )
        else:
            logger.info("TEST_MODE enabled - skipping write to Hudi")
        
        logger.info("FHIR data ingestion completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Error in ingestion pipeline: {e}", exc_info=True)
        metadata_manager.record_job(
            "ingestion_patient",
            "failed",
            0,
            str(e)
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
