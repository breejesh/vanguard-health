"""
Bronze to Silver transformation for Vanguard Health healthcare data.
"""
import logging
import sys
import os
import json
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, coalesce, concat_ws, current_timestamp, get_json_object, lit, regexp_extract, row_number, to_timestamp, trim, when, length

# Add src to path
sys.path.insert(0, '/app')

from src.common.config import get_config
from src.common.logger import setup_logger
from src.ingestion.metadata_manager import MetadataManager

logger = setup_logger(__name__)
config = get_config()

BRONZE_TO_SILVER_CURSOR_NAME = "bronze_to_silver"

SILVER_TABLE_RECORD_KEYS = {
    "patient": "patient_id",
    "encounter": "encounter_id",
    "observation": "obs_id",
    "condition": "condition_id",
    "medicationrequest": "medicationrequest_id",
}


def _canonical_reference_expr(resource_type: str, source_expr):
    """Normalize IDs/references to terminal IDs only (e.g., Patient/123 -> 123)."""
    value = trim(source_expr)
    terminal_id = regexp_extract(value, r"([^/]+)$", 1)
    return when(
        source_expr.isNull() | (length(value) == 0),
        lit(None),
    ).otherwise(terminal_id)


def _run_date_from_ts(run_ts: str) -> str:
    """Convert epoch-millisecond run timestamp to YYYYMMDD."""
    try:
        dt = datetime.utcfromtimestamp(int(run_ts) / 1000.0)
        return dt.strftime("%Y%m%d")
    except Exception:
        return datetime.utcnow().strftime("%Y%m%d")


def create_spark_session() -> SparkSession:
    """Create a plain Spark session for Parquet-backed Bronze reads/writes."""
    return SparkSession.builder \
        .appName("bronze-to-silver") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def read_bronze_table(spark: SparkSession, bronze_ts: str, resource_type: str, since_timestamp: datetime = None) -> DataFrame:
    """Read Bronze data for a specific timestamp folder, optionally filtered by watermark."""
    return read_bronze_parquet(spark, bronze_ts, resource_type, since_timestamp=since_timestamp)


def read_bronze_parquet(spark: SparkSession, bronze_ts: str, resource_type: str, since_timestamp: datetime = None) -> DataFrame:
    """Read Bronze data from Parquet files."""
    try:
        run_date = _run_date_from_ts(bronze_ts)
        parquet_path = f"{config.BRONZE_PATH}/{resource_type.lower()}/{run_date}/{bronze_ts}.parquet"
        logger.info(f"Reading Parquet from {parquet_path}")

        parquet_file = Path(parquet_path)
        if not parquet_file.exists():
            fallback_matches = list(Path(config.BRONZE_PATH).rglob(f"{bronze_ts}.parquet"))
            if not fallback_matches:
                raise FileNotFoundError(f"No bronze parquet found for timestamp {bronze_ts}")
            parquet_path = str(fallback_matches[0])
            logger.warning(f"Primary bronze path missing, using fallback file: {parquet_path}")

        parquet_df = spark.read.parquet(parquet_path)

        if since_timestamp is not None:
            parquet_df = parquet_df.withColumn("_ingestion_ts", to_timestamp(col("ingestion_timestamp")))
            parquet_df = parquet_df.filter(col("_ingestion_ts") > lit(since_timestamp))

        logger.info(f"Read {parquet_df.count()} records from Parquet for {resource_type}")
        return parquet_df
    except Exception as e:
        logger.error(f"Error reading Parquet for {resource_type}: {e}")
        return None


def silver_snapshot_file_path(silver_ts: str, table_name: str) -> str:
    """Return the file path for a silver timestamp snapshot."""
    run_date = _run_date_from_ts(silver_ts)
    return f"{config.SILVER_PATH}/{table_name}/{run_date}/{silver_ts}.parquet"


def _coerce_datetime_value(value):
    """Convert common timestamp values to naive datetime objects."""
    if value is None:
        return None

    try:
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None

    return None


def read_silver_timestamp_snapshot(spark: SparkSession, silver_ts: str, table_name: str) -> DataFrame:
    """Read a timestamp-scoped parquet snapshot for a silver table, if it exists."""
    snapshot_path = silver_snapshot_file_path(silver_ts, table_name)
    try:
        if not Path(snapshot_path).exists():
            return None
        return spark.read.parquet(snapshot_path)
    except Exception:
        return None


def merge_silver_snapshot(existing_df: DataFrame, new_df: DataFrame, record_key_field: str) -> DataFrame:
    """Merge a new parquet batch into the current silver snapshot using latest-wins dedupe."""
    if existing_df is None:
        return new_df

    if new_df is None:
        return existing_df

    combined = existing_df.unionByName(new_df, allowMissingColumns=True)

    if record_key_field in combined.columns:
        order_columns = []
        if "updated_at" in combined.columns:
            order_columns.append(col("updated_at").desc_nulls_last())
        if "created_at" in combined.columns:
            order_columns.append(col("created_at").desc_nulls_last())
        if not order_columns:
            order_columns.append(col(record_key_field).desc_nulls_last())

        window = Window.partitionBy(record_key_field).orderBy(*order_columns)
        combined = combined.withColumn("_row_number", row_number().over(window)).filter(col("_row_number") == 1).drop("_row_number")

    return combined


def transform_patients(bronze_patient_df: DataFrame) -> DataFrame:
    """Transform Patient resource from Bronze to Silver schema."""
    try:
        resource_json = col("resource_json")
        silver_patient = bronze_patient_df.select(
            _canonical_reference_expr("Patient", get_json_object(resource_json, "$.id")).alias("patient_id"),
            coalesce(
                get_json_object(resource_json, "$.name[0].text"),
                trim(
                    concat_ws(
                        " ",
                        get_json_object(resource_json, "$.name[0].given[0]"),
                        get_json_object(resource_json, "$.name[0].family"),
                    )
                ),
            ).alias("name"),
            get_json_object(resource_json, "$.birthDate").alias("dob"),
            get_json_object(resource_json, "$.gender").alias("gender"),
            get_json_object(resource_json, "$.address[0].city").alias("city"),
            get_json_object(resource_json, "$.address[0].state").alias("state"),
            get_json_object(resource_json, "$.address[0].postalCode").alias("zipcode"),
            get_json_object(resource_json, "$.address[0].country").alias("country"),
            regexp_extract(
                resource_json,
                r'"url"\s*:\s*"latitude"\s*,\s*"valueDecimal"\s*:\s*(-?\d+(?:\.\d+)?)',
                1,
            ).cast("double").alias("latitude"),
            regexp_extract(
                resource_json,
                r'"url"\s*:\s*"longitude"\s*,\s*"valueDecimal"\s*:\s*(-?\d+(?:\.\d+)?)',
                1,
            ).cast("double").alias("longitude"),
            get_json_object(resource_json, "$.telecom[0].value").alias("phone"),
            col("ingestion_timestamp").cast("timestamp").alias("created_at"),
            current_timestamp().alias("updated_at"),
        ).dropDuplicates(["patient_id"])
        
        logger.info(f"Transformed {silver_patient.count()} patient records")
        return silver_patient
        
    except Exception as e:
        logger.error(f"Error transforming patients: {e}")
        raise


def transform_encounters(bronze_encounter_df: DataFrame) -> DataFrame:
    """Transform Encounter resource from Bronze to Silver schema."""
    try:
        resource_json = col("resource_json")
        silver_encounter = bronze_encounter_df.select(
            _canonical_reference_expr("Encounter", get_json_object(resource_json, "$.id")).alias("encounter_id"),
            _canonical_reference_expr("Patient", get_json_object(resource_json, "$.subject.reference")).alias("patient_id"),
            get_json_object(resource_json, "$.period.start").cast("timestamp").alias("encounter_date"),
            _canonical_reference_expr("Practitioner", get_json_object(resource_json, "$.participant[0].individual.reference")).alias("provider_id"),
            coalesce(
                get_json_object(resource_json, "$.reasonCode[0].coding[0].display"),
                get_json_object(resource_json, "$.reasonCode[0].text"),
            ).alias("diagnosis"),
            col("ingestion_timestamp").cast("timestamp").alias("created_at"),
            current_timestamp().alias("updated_at"),
        ).dropDuplicates(["encounter_id"])
        
        logger.info(f"Transformed {silver_encounter.count()} encounter records")
        return silver_encounter
        
    except Exception as e:
        logger.error(f"Error transforming encounters: {e}")
        raise


def transform_observations(bronze_observation_df: DataFrame) -> DataFrame:
    """Transform Observation resource from Bronze to Silver schema."""
    try:
        resource_json = col("resource_json")
        silver_observation = bronze_observation_df.select(
            _canonical_reference_expr("Observation", get_json_object(resource_json, "$.id")).alias("obs_id"),
            _canonical_reference_expr("Patient", get_json_object(resource_json, "$.subject.reference")).alias("patient_id"),
            _canonical_reference_expr("Encounter", get_json_object(resource_json, "$.encounter.reference")).alias("encounter_id"),
            get_json_object(resource_json, "$.code.coding[0].code").alias("observation_code"),
            coalesce(
                get_json_object(resource_json, "$.code.coding[0].display"),
                get_json_object(resource_json, "$.code.text"),
            ).alias("observation_text"),
            get_json_object(resource_json, "$.valueQuantity.value").alias("value"),
            get_json_object(resource_json, "$.valueQuantity.unit").alias("unit"),
            get_json_object(resource_json, "$.issued").cast("timestamp").alias("observation_time"),
            col("ingestion_timestamp").cast("timestamp").alias("created_at"),
            current_timestamp().alias("updated_at"),
        ).dropDuplicates(["obs_id"])
        
        logger.info(f"Transformed {silver_observation.count()} observation records")
        return silver_observation
        
    except Exception as e:
        logger.error(f"Error transforming observations: {e}")
        raise


def transform_conditions(bronze_condition_df: DataFrame) -> DataFrame:
    """Transform Condition resource from Bronze to Silver schema."""
    try:
        resource_json = col("resource_json")
        silver_condition = bronze_condition_df.select(
            _canonical_reference_expr("Condition", get_json_object(resource_json, "$.id")).alias("condition_id"),
            _canonical_reference_expr("Patient", get_json_object(resource_json, "$.subject.reference")).alias("patient_id"),
            _canonical_reference_expr("Encounter", get_json_object(resource_json, "$.encounter.reference")).alias("encounter_id"),
            get_json_object(resource_json, "$.code.coding[0].code").alias("condition_code"),
            coalesce(
                get_json_object(resource_json, "$.code.coding[0].display"),
                get_json_object(resource_json, "$.code.text"),
            ).alias("condition_text"),
            get_json_object(resource_json, "$.clinicalStatus.coding[0].code").alias("clinical_status"),
            get_json_object(resource_json, "$.verificationStatus.coding[0].code").alias("verification_status"),
            get_json_object(resource_json, "$.recordedDate").cast("timestamp").alias("condition_date"),
            col("ingestion_timestamp").cast("timestamp").alias("created_at"),
            current_timestamp().alias("updated_at"),
        ).dropDuplicates(["condition_id"])

        logger.info(f"Transformed {silver_condition.count()} condition records")
        return silver_condition

    except Exception as e:
        logger.error(f"Error transforming conditions: {e}")
        raise


def transform_medication_requests(bronze_medreq_df: DataFrame) -> DataFrame:
    """Transform MedicationRequest resource from Bronze to Silver schema."""
    try:
        resource_json = col("resource_json")
        silver_medreq = bronze_medreq_df.select(
            _canonical_reference_expr("MedicationRequest", get_json_object(resource_json, "$.id")).alias("medicationrequest_id"),
            _canonical_reference_expr("Patient", get_json_object(resource_json, "$.subject.reference")).alias("patient_id"),
            _canonical_reference_expr("Encounter", get_json_object(resource_json, "$.encounter.reference")).alias("encounter_id"),
            get_json_object(resource_json, "$.medicationCodeableConcept.coding[0].code").alias("medication_code"),
            coalesce(
                get_json_object(resource_json, "$.medicationCodeableConcept.coding[0].display"),
                get_json_object(resource_json, "$.medicationCodeableConcept.text"),
            ).alias("medication_text"),
            get_json_object(resource_json, "$.status").alias("status"),
            get_json_object(resource_json, "$.intent").alias("intent"),
            get_json_object(resource_json, "$.authoredOn").cast("timestamp").alias("authored_on"),
            col("ingestion_timestamp").cast("timestamp").alias("created_at"),
            current_timestamp().alias("updated_at"),
        ).dropDuplicates(["medicationrequest_id"])

        logger.info(f"Transformed {silver_medreq.count()} medication request records")
        return silver_medreq

    except Exception as e:
        logger.error(f"Error transforming medication requests: {e}")
        raise


def write_silver_table(df: DataFrame, output_path: str):
    """Write DataFrame to a Silver parquet snapshot path."""
    try:
        write_silver_parquet(df, output_path)

    except Exception as e:
        logger.error(f"Error writing Parquet for {output_path}: {e}")
        raise


def write_silver_parquet(df: DataFrame, output_path: str):
    """Write DataFrame to Silver as Parquet."""
    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        temp_dir = output_file.parent / f".tmp_{output_file.stem}_{int(time.time() * 1000)}"
        df.coalesce(1).write.mode("overwrite").parquet(str(temp_dir))

        part_files = list(temp_dir.glob("part-*.parquet"))
        if not part_files:
            raise RuntimeError(f"No parquet part file generated in temp dir {temp_dir}")

        shutil.copyfile(part_files[0], output_file)
        shutil.rmtree(temp_dir, ignore_errors=True)

        logger.info(f"Wrote {df.count()} records to Parquet: {output_file}")
        
    except Exception as e:
        logger.error(f"Error writing Parquet for {output_path}: {e}")
        raise


def transform_bronze_to_silver(bronze_path: str, silver_path: str) -> Tuple[int, str]:
    """
    Transform Bronze to Silver layer using pure Python JSON (local testing friendly).
    
    Args:
        bronze_path: Path to bronze layer directory
        silver_path: Path to silver layer directory
        
    Returns:
        Tuple of (exit_code, message)
        - 0 on success
        - 1 on error
    """
    try:
        bronze_path = Path(bronze_path)
        silver_path = Path(silver_path)

        if not bronze_path.exists():
            return 1, f"Bronze directory not found: {bronze_path}"

        silver_path.mkdir(parents=True, exist_ok=True)

        resource_types = set()
        for json_file in bronze_path.rglob("*.json"):
            resource_type = json_file.parent.name
            resource_types.add(resource_type)

        if not resource_types:
            return 1, "No JSON files found in bronze directory"

        logger.info(f"Processing {len(resource_types)} resource types")

        for resource_type in sorted(resource_types):
            resource_dir = bronze_path / resource_type
            json_files = list(resource_dir.glob("*.json"))

            if not json_files:
                continue

            try:
                output_dir = silver_path / f"{resource_type}_silver"
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / "data.jsonl"

                processed_count = 0
                with open(output_file, 'w', encoding='utf-8') as out_f:
                    for json_file in json_files:
                        try:
                            with open(json_file, 'r', encoding='utf-8') as in_f:
                                record = json.load(in_f)
                                record['_processed_at'] = datetime.now().isoformat()
                                record['_resource_type'] = resource_type
                                out_f.write(json.dumps(record) + '\n')
                                processed_count += 1
                        except Exception as e:
                            logger.debug(f"Skipped {json_file.name}: {e}")

                if processed_count > 0:
                    logger.info(f"[OK] Processed {processed_count} {resource_type} records -> {output_dir}")
                else:
                    logger.warning(f"[SKIP] No valid records found for {resource_type}")

            except Exception as e:
                logger.warning(f"Error processing {resource_type}: {e}")
                continue

        return 0, f"Transformation completed. Output: {silver_path}"

    except Exception as e:
        logger.error(f"Error in transform_bronze_to_silver: {e}", exc_info=True)
        return 1, f"Transformation failed: {str(e)}"


def main():
    """Main transformation pipeline - Spark with flat parquet snapshots."""
    spark = None
    metadata_manager = None
    try:
        logger.info("🔥 Starting Bronze to Silver transformation (SPARK + PARQUET)")
        
        spark = create_spark_session()
        metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)
        pipeline_run_id = os.getenv("PIPELINE_RUN_ID")
        bronze_state = metadata_manager.get_pipeline_state("bronze_latest_run")

        explicit_bronze_ts = os.getenv("BRONZE_TS")
        explicit_silver_ts = os.getenv("SILVER_TS")
        if explicit_bronze_ts:
            bronze_ts = explicit_bronze_ts
            logger.info(f"Using explicit bronze timestamp from BRONZE_TS: {bronze_ts}")
        else:
            if bronze_state and bronze_state.get("run_ts"):
                bronze_ts = bronze_state.get("run_ts")
                logger.info(f"Using bronze timestamp from state: {bronze_ts}")
            else:
                bronze_root = Path(config.BRONZE_PATH)
                ts_candidates = []
                if bronze_root.exists():
                    for parquet_file in bronze_root.rglob("*.parquet"):
                        stem = parquet_file.stem
                        if stem.isdigit():
                            ts_candidates.append(stem)

                if ts_candidates:
                    bronze_ts = sorted(ts_candidates)[-1]
                    logger.warning(f"No bronze state found, using latest timestamp file id: {bronze_ts}")
                else:
                    bronze_ts = str(int(datetime.utcnow().timestamp() * 1000))
                    logger.warning(f"No bronze timestamp directories found, fallback timestamp: {bronze_ts}")

        silver_ts = explicit_silver_ts or bronze_ts
        logger.info(f"Silver output timestamp: {silver_ts}")
        if not pipeline_run_id and bronze_state:
            pipeline_run_id = bronze_state.get("pipeline_run_id")
        if pipeline_run_id:
            logger.info(f"Pipeline run context: {pipeline_run_id}")

        last_cursor = metadata_manager.get_last_cursor(BRONZE_TO_SILVER_CURSOR_NAME)
        if last_cursor is None:
            logger.info("No bronze-to-silver cursor found, processing the current bronze snapshot")
        else:
            logger.info(f"Processing bronze rows newer than {last_cursor.isoformat()}")
        
        # Read Bronze tables
        logger.info("Reading Bronze layer...")
        bronze_tables = [
            ("Patient", transform_patients, "patient", "patient_id"),
            ("Encounter", transform_encounters, "encounter", "encounter_id"),
            ("Observation", transform_observations, "observation", "obs_id"),
            ("Condition", transform_conditions, "condition", "condition_id"),
            ("MedicationRequest", transform_medication_requests, "medicationrequest", "medicationrequest_id"),
        ]

        latest_processed_cursor = last_cursor
        processed_records = 0
        
        for resource_type, transform_fn, table_name, record_key_field in bronze_tables:
            logger.info(f"Transforming {resource_type} data...")
            bronze_delta = read_bronze_table(spark, bronze_ts, resource_type, last_cursor)

            if bronze_delta is None or len(bronze_delta.take(1)) == 0:
                logger.info(f"No new {resource_type} rows to process")
                continue

            batch_cursor_value = bronze_delta.selectExpr("max(ingestion_timestamp) as latest_ingestion_timestamp").collect()[0][0]
            batch_cursor = _coerce_datetime_value(batch_cursor_value)
            if batch_cursor is not None and (latest_processed_cursor is None or batch_cursor > latest_processed_cursor):
                latest_processed_cursor = batch_cursor

            silver_delta = transform_fn(bronze_delta)
            processed_records += silver_delta.count()

            current_snapshot = read_silver_timestamp_snapshot(spark, silver_ts, table_name)
            silver_current = merge_silver_snapshot(current_snapshot, silver_delta, record_key_field)
            write_silver_table(silver_current, silver_snapshot_file_path(silver_ts, table_name))
        
        if latest_processed_cursor is not None and (last_cursor is None or latest_processed_cursor > last_cursor):
            metadata_manager.update_cursor(BRONZE_TO_SILVER_CURSOR_NAME, latest_processed_cursor)

        metadata_manager.set_pipeline_state(
            "silver_latest_run",
            {
                "run_ts": silver_ts,
                "bronze_ts": bronze_ts,
                "pipeline_run_id": pipeline_run_id,
                "bronze_run_path": f"{config.BRONZE_PATH}/<table>/{_run_date_from_ts(bronze_ts)}/{bronze_ts}.parquet",
                "silver_run_path": f"{config.SILVER_PATH}/<table>/{_run_date_from_ts(silver_ts)}/{silver_ts}.parquet",
                "updated_at": datetime.utcnow(),
            },
        )

        metadata_manager.record_job(
            "bronze_to_silver",
            "completed",
            processed_records,
            pipeline_run_id=pipeline_run_id,
            details={
                "bronze_ts": bronze_ts,
                "silver_ts": silver_ts,
                "bronze_path": f"{config.BRONZE_PATH}/<table>/{_run_date_from_ts(bronze_ts)}/{bronze_ts}.parquet",
                "silver_path": f"{config.SILVER_PATH}/<table>/{_run_date_from_ts(silver_ts)}/{silver_ts}.parquet",
            },
        )

        logger.info("✅ Bronze to Silver transformation COMPLETED")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Spark transformation failed: {e}", exc_info=True)
        try:
            if metadata_manager is not None:
                metadata_manager.record_job(
                    "bronze_to_silver",
                    "failed",
                    0,
                    str(e),
                    pipeline_run_id=os.getenv("PIPELINE_RUN_ID"),
                )
        except Exception:
            pass
        return 1
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
