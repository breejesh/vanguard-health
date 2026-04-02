"""
Lightweight multithreaded fetcher job for Kubernetes CronJob.
This module fetches incremental FHIR resources and writes them to the Bronze layer.
It deliberately does not spawn or trigger downstream jobs; orchestration is handled externally.
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
    try:
        logger.info("Starting multithreaded fetcher job (fetcher_job)")
        pipeline_run_id = os.getenv("PIPELINE_RUN_ID")
        explicit_bronze_ts = os.getenv("BRONZE_TS")

        fetcher = FHIRFetcher(config.SYNTHEA_API_URL, config.SYNTHEA_API_KEY)
        metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)

        # Get last cursor for incremental fetch; bootstrap with a wider lookback on first run.
        last_cursor = metadata_manager.get_last_cursor("patient_ingestion")
        if last_cursor is None:
            last_cursor = datetime.utcnow() - timedelta(days=config.FETCH_LOOKBACK_DAYS)

        logger.info(f"Fetching resources modified since {last_cursor.isoformat()}")

        # Fetch data (FHIRFetcher already handles pagination sequentially by resource type)
        resources = fetcher.fetch_incremental(last_cursor)
        logger.info(f"Fetched {len(resources)} resources from FHIR API")

        if not config.TEST_MODE:
            writer = BronzeWriter(config.BRONZE_PATH)
            success = writer.write_to_bronze(resources, run_ts=explicit_bronze_ts)

            if success:
                # Update cursor to now only after successful write
                metadata_manager.update_cursor("patient_ingestion", datetime.utcnow())
                metadata_manager.set_pipeline_state(
                    "bronze_latest_run",
                    {
                        "run_ts": writer.last_run_ts,
                        "run_path": writer.last_run_path,
                        "pipeline_run_id": pipeline_run_id,
                        "records_processed": len(resources),
                        "updated_at": datetime.utcnow(),
                    },
                )
                metadata_manager.record_job(
                    "ingestion_fetcher",
                    "completed",
                    len(resources),
                    pipeline_run_id=pipeline_run_id,
                    details={
                        "bronze_ts": writer.last_run_ts,
                        "bronze_path": writer.last_run_path,
                    },
                )
            else:
                metadata_manager.record_job(
                    "ingestion_fetcher",
                    "failed",
                    0,
                    "writer failed",
                    pipeline_run_id=pipeline_run_id,
                )
                raise RuntimeError("Bronze writer failed")
        else:
            logger.info("TEST_MODE enabled - skipping write to Bronze")

        logger.info("Fetcher job completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Fetcher job failed: {e}", exc_info=True)
        try:
            metadata_manager.record_job(
                "ingestion_fetcher",
                "failed",
                0,
                str(e),
                pipeline_run_id=os.getenv("PIPELINE_RUN_ID"),
            )
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
