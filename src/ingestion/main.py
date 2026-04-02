"""
Main entry point for FHIR data ingestion from Synthea API.
"""
import sys
import os
import logging
from datetime import datetime, timedelta

from src.common.config import get_config
from src.common.logger import setup_logger
from src.ingestion.fhir_fetcher import FHIRFetcher
from src.ingestion.bronze_writer import BronzeWriter
from src.ingestion.metadata_manager import MetadataManager

logger = setup_logger(__name__)
config = get_config()


def main():
    """Main ingestion pipeline."""
    try:
        logger.info("Starting FHIR data ingestion pipeline")
        pipeline_run_id = os.getenv("PIPELINE_RUN_ID")
        explicit_bronze_ts = os.getenv("BRONZE_TS")
        
        # Initialize components
        fetcher = FHIRFetcher(config.SYNTHEA_API_URL, config.SYNTHEA_API_KEY)
        metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)
        
        # Get last cursor for incremental fetch; bootstrap with a wider lookback on first run.
        last_cursor = metadata_manager.get_last_cursor("patient_ingestion")
        if last_cursor is None:
            last_cursor = datetime.now() - timedelta(days=config.FETCH_LOOKBACK_DAYS)
        
        logger.info(f"Fetching FHIR resources modified since {last_cursor}")
        
        # Fetch data
        resources = fetcher.fetch_incremental(last_cursor)
        logger.info(f"Fetched {len(resources)} resources from Synthea API")
        
        if not config.TEST_MODE:
            # Write to Bronze as flat parquet files
            writer = BronzeWriter(config.BRONZE_PATH)
            writer.write_to_bronze(resources, run_ts=explicit_bronze_ts)
            
            # Update cursor
            metadata_manager.update_cursor("patient_ingestion", datetime.now())
            metadata_manager.set_pipeline_state(
                "bronze_latest_run",
                {
                    "run_ts": writer.last_run_ts,
                    "run_path": writer.last_run_path,
                    "pipeline_run_id": pipeline_run_id,
                    "records_processed": len(resources),
                    "updated_at": datetime.now(),
                },
            )
            
            # Record job
            metadata_manager.record_job(
                "ingestion_patient",
                "completed",
                len(resources),
                pipeline_run_id=pipeline_run_id,
                details={
                    "bronze_ts": writer.last_run_ts,
                    "bronze_path": writer.last_run_path,
                },
            )
        else:
            logger.info("TEST_MODE enabled - skipping write to Bronze")
        
        logger.info("FHIR data ingestion completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Error in ingestion pipeline: {e}", exc_info=True)
        metadata_manager.record_job(
            "ingestion_patient",
            "failed",
            0,
            str(e),
            pipeline_run_id=os.getenv("PIPELINE_RUN_ID"),
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
