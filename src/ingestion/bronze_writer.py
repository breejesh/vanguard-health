"""
Flat-file writer for ingesting FHIR data into the Bronze layer.
"""
import os
import json
import logging
from datetime import datetime
from typing import Dict, List

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class BronzeWriter:
    """Writes data to Bronze tables as parquet files."""

    def __init__(self, bronze_path: str):
        self.bronze_path = bronze_path
        self.last_run_ts = None
        self.last_run_path = None
        self.last_run_date = None
        os.makedirs(bronze_path, exist_ok=True)
        logger.info(f"Initialized BronzeWriter with path: {bronze_path}")

    @staticmethod
    def _run_date_from_ts(run_ts: str) -> str:
        """Convert epoch-millisecond run timestamp to YYYYMMDD."""
        try:
            dt = datetime.utcfromtimestamp(int(run_ts) / 1000.0)
            return dt.strftime("%Y%m%d")
        except Exception:
            return datetime.utcnow().strftime("%Y%m%d")

    def write_to_bronze(self, resources: List[Dict], run_ts: str = None) -> bool:
        """Write FHIR resources to Bronze layer (grouped by resource type)."""
        try:
            self.last_run_ts = run_ts or str(int(datetime.utcnow().timestamp() * 1000))
            self.last_run_date = self._run_date_from_ts(self.last_run_ts)
            self.last_run_path = self.bronze_path
            os.makedirs(self.last_run_path, exist_ok=True)

            resources_by_type = {}
            for resource in resources:
                resource_type = resource.get('resourceType', 'Unknown')
                resources_by_type.setdefault(resource_type, []).append(resource)

            for resource_type, type_resources in resources_by_type.items():
                self._write_resource_type(resource_type, type_resources)

            logger.info(f"Successfully wrote {len(resources)} resources to Bronze")
            return True

        except Exception as e:
            logger.error(f"Error writing to Bronze: {e}")
            return False

    def _write_resource_type(self, resource_type: str, resources: List[Dict]) -> None:
        """Write resources of a specific type to a parquet table."""
        try:
            if self.last_run_path is None or self.last_run_date is None or self.last_run_ts is None:
                raise RuntimeError("Bronze run path not initialized")

            table_dir = os.path.join(self.bronze_path, resource_type.lower(), self.last_run_date)
            os.makedirs(table_dir, exist_ok=True)

            row_timestamp = datetime.now().isoformat()
            rows = []
            for resource in resources:
                rows.append({
                    "resource_id": resource.get("id"),
                    "resource_type": resource_type,
                    "ingestion_timestamp": row_timestamp,
                    "ingestion_date": self.last_run_date,
                    "resource_json": json.dumps(resource, default=str),
                })

            table = pa.Table.from_pylist(rows)
            output_file = os.path.join(table_dir, f"{self.last_run_ts}.parquet")
            pq.write_table(table, output_file, compression="snappy")

            logger.info(f"Wrote {len(resources)} {resource_type} resources to {table_dir}")
            logger.info(f"  - Parquet: {output_file}")

        except Exception as e:
            logger.error(f"Error writing {resource_type}: {e}")
            raise

    def read_bronze(self, resource_type: str, date: str = None) -> List[Dict]:
        """Read from Bronze table."""
        try:
            if date is None:
                date = self.last_run_date or datetime.utcnow().strftime("%Y%m%d")

            table_dir = os.path.join(self.bronze_path, resource_type.lower(), date)

            resources = []
            if os.path.exists(table_dir):
                dataset = ds.dataset(table_dir, format="parquet")
                for row in dataset.to_table().to_pylist():
                    resource_json = row.get("resource_json")
                    if resource_json:
                        resources.append(json.loads(resource_json))

            logger.info(f"Read {len(resources)} {resource_type} resources from Bronze")
            return resources

        except Exception as e:
            logger.error(f"Error reading from Bronze: {e}")
            return []