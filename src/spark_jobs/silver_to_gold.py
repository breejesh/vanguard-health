"""
Silver to Gold transformation using Spark.
Aggregates conditions by geography (H3), demographics, and time.
Outputs condition-level parquet files and metadata parquet artifacts.
"""
import json
import logging
import os
import shutil
import sys
from pathlib import Path
from datetime import datetime
from typing import Tuple
import h3

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import get_config
from src.ingestion.metadata_manager import MetadataManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

H3_RESOLUTION = 4
config = get_config()


def create_spark_session():
    """Create Spark session optimized for local/distributed execution."""
    import platform
    
    builder = SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Windows: disable Hadoop native + permissions to avoid winutils.exe
    if platform.system() == "Windows":
        builder = builder \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1") \
            .config("spark.hadoop.hadoop.security.groups.cache.warn.after.ms", "30000") \
            .config("spark.hadoop.hadoop.native.lib", "false") \
            .config("spark.hadoop.io.nativeio.NativeIO.USABLE", "false")
    
    return builder.getOrCreate()



def calculate_age_group(dob_timestamp) -> str:
    """Calculate age group from DOB timestamp or string date."""
    if dob_timestamp is None:
        return 'Unknown'
    try:
        # Handle string DOB in format 'YYYY-MM-DD'
        if isinstance(dob_timestamp, str):
            birth_date = datetime.strptime(dob_timestamp, '%Y-%m-%d')
        # Handle timestamp in milliseconds
        elif isinstance(dob_timestamp, (int, float)):
            birth_date = datetime.fromtimestamp(dob_timestamp / 1000.0)
        # Assume it's already a datetime object
        else:
            birth_date = dob_timestamp
        
        today = datetime.now()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        
        if age < 0 or age > 150:
            return 'Unknown'
        elif age <= 17:
            return '0-17'
        elif age <= 34:
            return '18-34'
        elif age <= 54:
            return '35-54'
        else:
            return '55+'
    except:
        return 'Unknown'


def normalize_gender(gender) -> str:
    """Normalize gender to standard values."""
    if gender is None:
        return 'unknown'
    
    gender_str = str(gender).strip().lower()
    if gender_str in {'male', 'm'}:
        return 'male'
    elif gender_str in {'female', 'f'}:
        return 'female'
    elif gender_str in {'other', 'undifferentiated'}:
        return 'other'
    else:
        return 'unknown'


def latlng_to_h3(lat: float, lng: float, resolution: int) -> str:
    """Convert coordinates to H3 index."""
    try:
        if hasattr(h3, "latlng_to_cell"):
            return h3.latlng_to_cell(lat, lng, resolution)
        elif hasattr(h3, "geo_to_h3"):
            return h3.geo_to_h3(lat, lng, resolution)
    except:
        pass
    return None


def h3_to_latlng(h3_id: str) -> Tuple[float, float]:
    """Convert H3 index to center coordinates."""
    try:
        if hasattr(h3, "cell_to_latlng"):
            return h3.cell_to_latlng(h3_id)
        elif hasattr(h3, "h3_to_geo"):
            return h3.h3_to_geo(h3_id)
    except:
        pass
    return None, None


def read_silver_table(spark, silver_path: Path, table_name: str, run_ts: str = None):
    """Read Parquet files from silver layer - store as temp Parquet, then load with Spark SQL."""
    import pyarrow.parquet as pq
    import pyarrow as pa
    import tempfile
    
    table_dir = silver_path / table_name
    if not table_dir.exists():
        logger.warning(f"Table directory not found: {table_dir}")
        return None
    
    if run_ts:
        parquet_files = list(table_dir.glob(f"**/{run_ts}.parquet"))
        if not parquet_files:
            # Fall back to the latest snapshot only to avoid duplicate history reads.
            all_parquet_files = list(table_dir.glob("**/*.parquet"))
            if all_parquet_files:
                latest_stem = max(
                    (pf.stem for pf in all_parquet_files),
                    key=lambda stem: int(stem) if str(stem).isdigit() else -1,
                )
                parquet_files = [pf for pf in all_parquet_files if pf.stem == latest_stem]
                logger.warning(
                    f"No parquet files found for {run_ts}, using latest snapshot {latest_stem}"
                )
    else:
        parquet_files = list(table_dir.glob("**/*.parquet"))
    
    if not parquet_files:
        logger.warning(f"No parquet files found in {table_dir}")
        return None
    
    try:
        # Read all parquet files using PyArrow (bypasses Hadoop)
        tables = []
        for pf in parquet_files:
            try:
                table = pq.read_table(str(pf))
                tables.append(table)
            except Exception as e:
                logger.warning(f"Could not read {pf}: {e}")
        
        if not tables:
            return None
        
        # Combine all tables using PyArrow
        combined_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        
        # Write to temporary parquet file using PyArrow (no Hadoop involved)
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name
        
        pq.write_table(combined_table, tmp_path)
        logger.info(f"Wrote temp parquet to {tmp_path}")
        
        # Now read from temp file using Spark SQL (bypasses DataFrame creation pickling issues)
        # Tell Spark to use the file:// scheme explicitly and bypass Hadoop for this read
        # On Windows, need file:/// (three slashes) for local paths
        file_uri = tmp_path.replace("\\", "/")
        if not file_uri.startswith("/"):
            file_uri = "/" + file_uri
        file_uri = f"file:///{file_uri}"
        
        spark_df = spark.read.format("parquet") \
            .option("mergeSchema", "true") \
            .load(file_uri)
        
        return spark_df
    except Exception as e:
        logger.error(f"Error reading parquet from {table_dir}: {e}")
        return None


def write_single_parquet(df, output_file: Path) -> None:
    """Write a Spark DataFrame as one parquet file at a deterministic path."""
    temp_dir = output_file.with_suffix(".tmp")

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output_file.exists():
        output_file.unlink()

    df.coalesce(1).write.mode("overwrite").parquet(str(temp_dir))

    part_files = list(temp_dir.glob("part-*.parquet"))
    if not part_files:
        raise RuntimeError(f"No parquet part file found in {temp_dir}")

    part_files[0].replace(output_file)
    shutil.rmtree(temp_dir)


def merge_condition_with_existing(spark, condition_df, parquet_output: Path):
    """Merge new condition aggregates with existing output using key-based upsert semantics."""
    required_columns = [
        "condition_code",
        "date_key",
        "h3",
        "age_group",
        "gender",
        "case_count",
        "city",
        "state",
    ]
    column_types = {
        "condition_code": StringType(),
        "date_key": StringType(),
        "h3": StringType(),
        "age_group": StringType(),
        "gender": StringType(),
        "case_count": LongType(),
        "city": StringType(),
        "state": StringType(),
    }

    new_df = condition_df.select(
        F.col("condition_code"),
        F.col("date_key"),
        F.col("h3_id").alias("h3"),
        F.col("age_group"),
        F.col("gender"),
        F.col("case_count").cast(LongType()).alias("case_count"),
        F.col("city"),
        F.col("state"),
    ).withColumn("_source_priority", F.lit(1))

    if parquet_output.exists():
        existing_df = spark.read.parquet(str(parquet_output))
        for column_name in required_columns:
            if column_name not in existing_df.columns:
                existing_df = existing_df.withColumn(column_name, F.lit(None).cast(column_types[column_name]))
            else:
                existing_df = existing_df.withColumn(column_name, F.col(column_name).cast(column_types[column_name]))

        existing_df = existing_df.select(*required_columns).withColumn("_source_priority", F.lit(0))
        combined_df = existing_df.unionByName(new_df.select(*required_columns, "_source_priority"), allowMissingColumns=True)
    else:
        combined_df = new_df.select(*required_columns, "_source_priority")

    key_window = Window.partitionBy(
        "condition_code",
        "date_key",
        "h3",
        "age_group",
        "gender",
    ).orderBy(F.desc("_source_priority"))

    return combined_df.withColumn("_rn", F.row_number().over(key_window)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn", "_source_priority")


def load_existing_gold_metadata(gold_path: Path):
    """Load existing parquet metadata so incremental runs preserve condition/h3 context."""
    condition_display_map = {}
    condition_patient_counts = {}
    h3_metadata = {}

    conditions_parquet = gold_path / "_conditions.parquet"
    if conditions_parquet.exists():
        try:
            import pyarrow.parquet as pq

            for row in pq.read_table(str(conditions_parquet)).to_pylist():
                condition_code = row.get("condition_code")
                if not condition_code:
                    continue

                condition_code = str(condition_code)
                condition_display_map[condition_code] = str(
                    row.get("condition_display") or condition_code
                )
                condition_patient_counts[condition_code] = int(row.get("patient_count") or 0)
        except Exception as exc:
            logger.warning(f"Could not read existing _conditions.parquet: {exc}")

    h3_parquet = gold_path / "_h3_reference.parquet"
    if h3_parquet.exists():
        try:
            import pyarrow.parquet as pq

            for row in pq.read_table(str(h3_parquet)).to_pylist():
                h3_id = row.get("h3")
                if not h3_id:
                    continue
                h3_metadata[str(h3_id)] = {
                    "latitude": float(row.get("latitude") or 0.0),
                    "longitude": float(row.get("longitude") or 0.0),
                }
        except Exception as exc:
            logger.warning(f"Could not read existing _h3_reference.parquet: {exc}")

    return condition_display_map, condition_patient_counts, h3_metadata

def transform_silver_to_gold(spark, silver_path: Path, gold_path: Path) -> Tuple[int, str]:
    """
    Transform silver layer to gold layer using Spark.
    Aggregates by: condition, date, H3 cell, age_group, gender
    """
    try:
        silver_path = Path(silver_path)
        gold_path = Path(gold_path)
        gold_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("="*60)
        logger.info("SILVER TO GOLD TRANSFORMATION (Spark-based)")
        logger.info("="*60)
        logger.info(f"Silver Path: {silver_path}")
        logger.info(f"Gold Path: {gold_path}")
        
        active_run_ts = os.getenv("SILVER_TS")
        
        # Read silver tables
        logger.info("Reading silver layer...")
        patients_df = read_silver_table(spark, silver_path, "patient", active_run_ts)
        conditions_df = read_silver_table(spark, silver_path, "condition", active_run_ts)
        
        if patients_df is None or conditions_df is None:
            return 1, "Missing patient or condition data"
        
        logger.info(f"[OK] Read {patients_df.count()} patients")
        logger.info(f"[OK] Read {conditions_df.count()} conditions")
        
        # Extract patient demographics and location
        patients_df = patients_df.select(
            F.col("patient_id"),
            F.col("latitude").cast(DoubleType()).alias("lat"),
            F.col("longitude").cast(DoubleType()).alias("lng"),
            F.col("city"),
            F.col("state"),
            F.col("dob"),
            F.col("gender")
        ).filter(F.col("lat").isNotNull() & F.col("lng").isNotNull())
        
        # Extract condition data
        conditions_df = conditions_df.select(
            F.col("patient_id"),
            F.col("condition_code"),
            F.col("condition_text"),
            F.to_date(F.col("condition_date")).alias("condition_date")
        ).filter(F.col("condition_code").isNotNull())
        
        # Join patient info with conditions
        logger.info("Joining patient data with conditions...")
        joined_df = conditions_df.join(patients_df, on="patient_id", how="inner")
        
        # Calculate demographics
        udf_age_group = F.udf(calculate_age_group, StringType())
        udf_gender = F.udf(normalize_gender, StringType())
        udf_h3 = F.udf(latlng_to_h3, StringType())
        
        joined_df = joined_df \
            .withColumn("age_group", udf_age_group(F.col("dob"))) \
            .withColumn("gender_norm", udf_gender(F.col("gender"))) \
            .withColumn("h3_id", udf_h3(F.col("lat"), F.col("lng"), F.lit(H3_RESOLUTION)))
        
        # Filter valid H3 cells and dates
        joined_df = joined_df \
            .filter(F.col("h3_id").isNotNull()) \
            .filter(F.col("condition_date").isNotNull())
        
        # Aggregate: condition, date, H3, age_group, gender
        logger.info("Aggregating by condition, date, H3, age_group, gender...")
        aggregated_df = joined_df \
            .groupBy("condition_code", "condition_date", "h3_id", "age_group", "gender_norm") \
            .agg(
                F.count("*").alias("case_count"),
                F.first("city").alias("city"),
                F.first("state").alias("state")
            ) \
            .select(
                F.col("condition_code"),
                F.date_format(F.col("condition_date"), "yyyy-MM-dd").alias("date_key"),
                F.col("h3_id"),
                F.col("age_group"),
                F.col("gender_norm").alias("gender"),
                F.col("case_count"),
                F.col("city"),
                F.col("state")
            )
        
        # Write output partitioned by condition
        logger.info("Writing gold layer with demographic breakdowns...")
        
        conditions_list = aggregated_df.select("condition_code").distinct().collect()
        condition_display_map, condition_patient_counts, h3_metadata = load_existing_gold_metadata(gold_path)
        
        total_partitions = 0
        total_cases = 0
        
        for row in conditions_list:
            condition_code = row.condition_code
            condition_display_map[condition_code] = condition_code
            safe_condition_code = condition_code.replace("/", "_").replace(" ", "_")
            
            # Filter for this condition
            condition_data = aggregated_df.filter(F.col("condition_code") == condition_code)

            parquet_output = gold_path / f"{safe_condition_code}.parquet"
            merged_condition_data = merge_condition_with_existing(spark, condition_data, parquet_output)
            write_single_parquet(merged_condition_data, parquet_output)
            logger.info(f"[OK] Wrote parquet {parquet_output.name}")
            total_partitions += 1
            
            # Count unique patients (approximate via case_sum)
            patient_count = merged_condition_data.agg(F.sum("case_count")).collect()[0][0] or 0
            condition_patient_counts[condition_code] = patient_count

            # Keep H3 metadata current from parquet-backed aggregates.
            for h3_row in merged_condition_data.select("h3").distinct().collect():
                h3_id = h3_row.h3
                if not h3_id or h3_id in h3_metadata:
                    continue
                lat, lng = h3_to_latlng(h3_id)
                if lat is not None and lng is not None:
                    h3_metadata[h3_id] = {
                        "latitude": lat,
                        "longitude": lng,
                    }
        
        total_cases = int(sum(int(value) for value in condition_patient_counts.values()))
        logger.info(f"[OK] Created {total_partitions} partitions ({total_cases} total cases)")
        
        # Write metadata files
        generated_at = datetime.now().isoformat()
        age_groups = ["0-17", "18-34", "35-54", "55+", "Unknown"]
        genders = ["male", "female", "other", "unknown"]

        conditions_metadata_rows = [
            (
                condition_code,
                condition_display_map.get(condition_code, condition_code),
                int(condition_patient_counts.get(condition_code, 0)),
                generated_at,
                int(len(condition_display_map)),
                json.dumps(age_groups),
                json.dumps(genders),
            )
            for condition_code in sorted(condition_display_map.keys())
        ]
        conditions_metadata_schema = StructType([
            StructField("condition_code", StringType(), nullable=True),
            StructField("condition_display", StringType(), nullable=True),
            StructField("patient_count", LongType(), nullable=False),
            StructField("generated_at", StringType(), nullable=False),
            StructField("total_unique", IntegerType(), nullable=False),
            StructField("age_groups_json", StringType(), nullable=False),
            StructField("genders_json", StringType(), nullable=False),
        ])
        conditions_metadata_df = spark.createDataFrame(
            conditions_metadata_rows,
            schema=conditions_metadata_schema,
        ) if conditions_metadata_rows else spark.createDataFrame([], schema=conditions_metadata_schema)
        write_single_parquet(conditions_metadata_df, gold_path / "_conditions.parquet")
        logger.info(f"[OK] Created _conditions.parquet ({len(condition_display_map)} conditions)")

        h3_metadata_rows = [
            (
                h3_id,
                float(coords.get('latitude', 0.0)),
                float(coords.get('longitude', 0.0)),
                generated_at,
                int(len(h3_metadata)),
            )
            for h3_id, coords in sorted(h3_metadata.items())
        ]
        h3_metadata_schema = StructType([
            StructField("h3", StringType(), nullable=True),
            StructField("latitude", DoubleType(), nullable=False),
            StructField("longitude", DoubleType(), nullable=False),
            StructField("generated_at", StringType(), nullable=False),
            StructField("total_unique", IntegerType(), nullable=False),
        ])
        h3_metadata_df = spark.createDataFrame(
            h3_metadata_rows,
            schema=h3_metadata_schema,
        ) if h3_metadata_rows else spark.createDataFrame([], schema=h3_metadata_schema)
        write_single_parquet(h3_metadata_df, gold_path / "_h3_reference.parquet")
        logger.info(f"[OK] Created _h3_reference.parquet ({len(h3_metadata)} H3 cells)")
        
        return 0, "Transformation completed successfully"
    
    except Exception as e:
        logger.error(f"❌ Transform error: {e}", exc_info=True)
        return 1, str(e)



def main():
    """Main orchestration: setup, transform, metadata updates, cleanup."""
    spark = None
    metadata_manager = None
    
    try:
        spark = create_spark_session()
        
        # Get paths from environment
        silver_ts = os.getenv("SILVER_TS")
        pipeline_run_id = os.getenv("PIPELINE_RUN_ID")
        
        use_metadata = os.getenv("USE_METADATA", "true").lower() == "true"
        if use_metadata:
            try:
                metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)
                silver_state = metadata_manager.get_pipeline_state("silver_latest_run")
                if not pipeline_run_id and silver_state:
                    pipeline_run_id = silver_state.get("pipeline_run_id")
                if not silver_ts and silver_state and silver_state.get("run_ts"):
                    silver_ts = silver_state.get("run_ts")
            except Exception as e:
                logger.warning(f"MetadataManager unavailable: {e}")
                metadata_manager = None
        
        if silver_ts:
            os.environ["SILVER_TS"] = str(silver_ts)
        
        silver_path = os.getenv("SILVER_PATH", config.SILVER_PATH)
        gold_path = os.getenv("GOLD_PATH")
        if not gold_path:
            gold_path = str(Path(silver_path).parent / "gold")
        
        logger.info("="*60)
        logger.info("SILVER TO GOLD TRANSFORMATION JOB")
        logger.info("="*60)
        logger.info(f"Silver Path: {silver_path}")
        logger.info(f"Gold Path: {gold_path}")
        if pipeline_run_id:
            logger.info(f"Pipeline Run ID: {pipeline_run_id}")
        
        # Run transformation
        exit_code, message = transform_silver_to_gold(spark, Path(silver_path), Path(gold_path))
        
        # Update metadata if successful
        if exit_code == 0 and metadata_manager:
            try:
                metadata_manager.set_pipeline_state("gold_latest_run", {
                    "pipeline_run_id": pipeline_run_id,
                    "silver_ts": silver_ts,
                    "gold_path": gold_path,
                    "updated_at": datetime.utcnow(),
                })
                metadata_manager.record_job("silver_to_gold", "completed", 0, 
                                           pipeline_run_id=pipeline_run_id)
            except Exception as e:
                logger.warning(f"Failed to update metadata: {e}")
        elif metadata_manager:
            try:
                metadata_manager.record_job("silver_to_gold", "failed", 0, message, 
                                           pipeline_run_id=pipeline_run_id)
            except Exception:
                pass
        
        logger.info("")
        logger.info("="*60)
        logger.info("✅ TRANSFORMATION SUCCESSFUL" if exit_code == 0 else "❌ TRANSFORMATION FAILED")
        logger.info(f"Message: {message}")
        logger.info("="*60)
        
        return exit_code
    
    except Exception as e:
        logger.error(f"❌ Main error: {e}", exc_info=True)
        return 1
    
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
