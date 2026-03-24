"""
Bronze to Silver transformation for Vanguard Health healthcare data.
"""
import logging
import sys
import os
import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, schema_of_json, explode_outer, current_timestamp, get_json_object, when

# Add src to path
sys.path.insert(0, '/app')

from src.common.config import get_config
from src.common.logger import setup_logger

# Optional import - only needed if using Hudi/MongoDB (not for local testing)
try:
    from src.ingestion.metadata_manager import MetadataManager
    HAS_METADATA_MANAGER = True
except (ImportError, ModuleNotFoundError):
    MetadataManager = None
    HAS_METADATA_MANAGER = False

logger = setup_logger(__name__)
config = get_config()


def create_spark_session() -> SparkSession:
    """Create Spark session with Hudi support."""
    return SparkSession.builder \
        .appName("bronze-to-silver") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.hudi.datasource.write.precombine.field", "_hoodie_commit_time") \
        .config("spark.hudi.parquet.small.file.limit", "104857600") \
        .getOrCreate()


def read_bronze_table(spark: SparkSession, resource_type: str) -> DataFrame:
    """Read from Bronze Hudi table."""
    try:
        table_path = f"{config.BRONZE_PATH}/bronze_{resource_type.lower()}"
        logger.info(f"Reading from {table_path}")
        
        df = spark.read \
            .format("hudi") \
            .load(table_path)
        
        logger.info(f"Read {df.count()} records for {resource_type}")
        return df
    except Exception as e:
        logger.warning(f"Could not read Hudi table for {resource_type}: {e}, trying JSONL fallback")
        return read_bronze_jsonl(spark, resource_type)


def read_bronze_jsonl(spark: SparkSession, resource_type: str) -> DataFrame:
    """Fallback: Read from Bronze JSONL files."""
    try:
        jsonl_path = f"{config.BRONZE_PATH}/bronze_{resource_type.lower()}/*/*.jsonl"
        df = spark.read.json(jsonl_path)
        logger.info(f"Read {df.count()} records from JSONL for {resource_type}")
        return df
    except Exception as e:
        logger.error(f"Error reading JSONL for {resource_type}: {e}")
        return None


def transform_patients(bronze_patient_df: DataFrame) -> DataFrame:
    """Transform Patient resource from Bronze to Silver schema."""
    try:
        # Register as SQL view for easier nested access
        bronze_patient_df.createOrReplaceTempView("patients_bronze")
        
        # Use SQL to safely extract nested fields - handles nulls and arrays
        spark = bronze_patient_df.sparkSession
        silver_patient = spark.sql("""
            SELECT
                id AS patient_id,
                name[0].text AS name,
                birthDate AS dob,
                gender AS gender,
                address[0].city AS city,
                address[0].state AS state,
                telecom[0].value AS phone,
                CAST(_hoodie_commit_time AS TIMESTAMP) AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM patients_bronze
        """).drop_duplicates(["patient_id"])
        
        logger.info(f"Transformed {silver_patient.count()} patient records")
        return silver_patient
        
    except Exception as e:
        logger.error(f"Error transforming patients: {e}")
        raise


def transform_encounters(bronze_encounter_df: DataFrame) -> DataFrame:
    """Transform Encounter resource from Bronze to Silver schema."""
    try:
        bronze_encounter_df.createOrReplaceTempView("encounters_bronze")
        spark = bronze_encounter_df.sparkSession
        
        silver_encounter = spark.sql("""
            SELECT
                id AS encounter_id,
                subject.reference AS patient_id,
                CAST(period.start AS TIMESTAMP) AS encounter_date,
                participant[0].individual.reference AS provider_id,
                reasonCode[0].coding[0].display AS diagnosis,
                CAST(_hoodie_commit_time AS TIMESTAMP) AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM encounters_bronze
        """).drop_duplicates(["encounter_id"])
        
        logger.info(f"Transformed {silver_encounter.count()} encounter records")
        return silver_encounter
        
    except Exception as e:
        logger.error(f"Error transforming encounters: {e}")
        raise


def transform_observations(bronze_observation_df: DataFrame) -> DataFrame:
    """Transform Observation resource from Bronze to Silver schema."""
    try:
        bronze_observation_df.createOrReplaceTempView("observations_bronze")
        spark = bronze_observation_df.sparkSession
        
        silver_observation = spark.sql("""
            SELECT
                id AS obs_id,
                subject.reference AS patient_id,
                encounter.reference AS encounter_id,
                code.coding[0].code AS observation_code,
                code.coding[0].display AS observation_text,
                value.Quantity.value AS value,
                value.Quantity.unit AS unit,
                CAST(issued AS TIMESTAMP) AS observation_time,
                CAST(_hoodie_commit_time AS TIMESTAMP) AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM observations_bronze
        """).drop_duplicates(["obs_id"])
        
        logger.info(f"Transformed {silver_observation.count()} observation records")
        return silver_observation
        
    except Exception as e:
        logger.error(f"Error transforming observations: {e}")
        raise


def write_silver_table(df: DataFrame, table_name: str):
    """Write DataFrame to Silver Hudi table."""
    try:
        hudi_options = {
            "hoodie.table.name": table_name,
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.recordkey.field": table_name.replace("silver_", "") + "_id",
            "hoodie.datasource.write.precombine.field": "updated_at",
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "hoodie.datasource.write.partitionpath.field": "created_at",
            "hoodie.parquet.small.file.limit": "104857600",
        }
        
        silver_path = f"{config.SILVER_PATH}/{table_name}"
        df.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(silver_path)
        
        logger.info(f"Wrote {df.count()} records to {table_name}")
        
    except Exception as e:
        logger.warning(f"Hudi write failed for {table_name}: {e}, falling back to Parquet")
        write_silver_parquet(df, table_name)


def write_silver_parquet(df: DataFrame, table_name: str):
    """Fallback: Write DataFrame to Silver as Parquet."""
    try:
        date_str = datetime.now().strftime("%Y-%m-%d")
        parquet_path = f"{config.SILVER_PATH}/{table_name}/date={date_str}"
        
        df.coalesce(1).write \
            .mode("append") \
            .parquet(parquet_path)
        
        logger.info(f"Wrote {df.count()} records to Parquet: {parquet_path}")
        
    except Exception as e:
        logger.error(f"Error writing Parquet for {table_name}: {e}")
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
        
        # Verify bronze directory exists
        if not bronze_path.exists():
            return 1, f"Bronze directory not found: {bronze_path}"
        
        # Create silver directory
        silver_path.mkdir(parents=True, exist_ok=True)
        
        # Find all resource types in bronze
        resource_types = set()
        for json_file in bronze_path.rglob("*.json"):
            resource_type = json_file.parent.name
            resource_types.add(resource_type)
        
        if not resource_types:
            return 1, "No JSON files found in bronze directory"
        
        logger.info(f"Processing {len(resource_types)} resource types")
        
        # Process each resource type using pure Python (no Spark/Hudi for local testing)
        for resource_type in sorted(resource_types):
            resource_dir = bronze_path / resource_type
            json_files = list(resource_dir.glob("*.json"))
            
            if not json_files:
                continue
            
            try:
                # Create output directory for this resource type
                output_dir = silver_path / f"{resource_type}_silver"
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / "data.jsonl"
                
                # Read JSON files, add processing metadata, write as JSONL
                processed_count = 0
                with open(output_file, 'w', encoding='utf-8') as out_f:
                    for json_file in json_files:
                        try:
                            with open(json_file, 'r', encoding='utf-8') as in_f:
                                record = json.load(in_f)
                                # Add processing metadata
                                record['_processed_at'] = datetime.now().isoformat()
                                record['_resource_type'] = resource_type
                                # Write as JSONL (one record per line)
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
    """Main transformation pipeline - pure Spark with Hudi."""
    spark = None
    try:
        logger.info("🔥 Starting Bronze to Silver transformation (SPARK ONLY)")
        
        spark = create_spark_session()
        
        # Read Bronze tables
        logger.info("Reading Bronze layer...")
        bronze_patient = read_bronze_table(spark, "Patient")
        bronze_encounter = read_bronze_table(spark, "Encounter")
        bronze_observation = read_bronze_table(spark, "Observation")
        
        # Transform
        logger.info("Transforming Patient data...")
        if bronze_patient is not None:
            silver_patient = transform_patients(bronze_patient)
            write_silver_table(silver_patient, "silver_patient")
        
        logger.info("Transforming Encounter data...")
        if bronze_encounter is not None:
            silver_encounter = transform_encounters(bronze_encounter)
            write_silver_table(silver_encounter, "silver_encounter")
        
        logger.info("Transforming Observation data...")
        if bronze_observation is not None:
            silver_observation = transform_observations(bronze_observation)
            write_silver_table(silver_observation, "silver_observation")
        
        logger.info("✅ Bronze to Silver transformation COMPLETED")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Spark transformation failed: {e}", exc_info=True)
        return 1
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
