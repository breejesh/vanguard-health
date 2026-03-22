"""
Hudi writer for ingesting FHIR data into Bronze layer.
"""
import os
import json
import logging
from datetime import datetime
from typing import Dict, List

logger = logging.getLogger(__name__)


class HudiWriter:
    """Writes data to Hudi Bronze tables (local parquet files for now)."""
    
    def __init__(self, bronze_path: str):
        self.bronze_path = bronze_path
        os.makedirs(bronze_path, exist_ok=True)
        logger.info(f"Initialized HudiWriter with path: {bronze_path}")
    
    def write_to_bronze(self, resources: List[Dict]) -> bool:
        """Write FHIR resources to Bronze layer (grouped by resource type)."""
        try:
            # Group resources by type
            resources_by_type = {}
            for resource in resources:
                resource_type = resource.get('resourceType', 'Unknown')
                if resource_type not in resources_by_type:
                    resources_by_type[resource_type] = []
                resources_by_type[resource_type].append(resource)
            
            # Write each resource type
            for resource_type, type_resources in resources_by_type.items():
                self._write_resource_type(resource_type, type_resources)
            
            logger.info(f"Successfully wrote {len(resources)} resources to Bronze")
            return True
            
        except Exception as e:
            logger.error(f"Error writing to Bronze: {e}")
            return False
    
    def _write_resource_type(self, resource_type: str, resources: List[Dict]) -> None:
        """Write resources of a specific type to Hudi table."""
        try:
            # Create bronze table directory
            table_dir = os.path.join(self.bronze_path, f"bronze_{resource_type.lower()}")
            os.makedirs(table_dir, exist_ok=True)
            
            # Partition by date (for easy incremental processing)
            date_str = datetime.now().strftime("%Y-%m-%d")
            partition_dir = os.path.join(table_dir, f"ingestion_date={date_str}")
            os.makedirs(partition_dir, exist_ok=True)
            
            # Create individual JSON files directory for easy viewing
            json_dir = os.path.join(partition_dir, "json_files")
            os.makedirs(json_dir, exist_ok=True)
            
            # Write resources as JSON lines (for bulk processing)
            jsonl_file_path = os.path.join(partition_dir, "data.jsonl")
            with open(jsonl_file_path, 'a') as jsonl_file:
                for idx, resource in enumerate(resources):
                    # Add metadata
                    timestamp = datetime.now().isoformat()
                    resource['ingestion_timestamp'] = timestamp
                    resource['ingestion_date'] = date_str
                    
                    # Write to JSONL
                    jsonl_file.write(json.dumps(resource) + '\n')
                    
                    # Write individual JSON file for easy viewing
                    resource_id = resource.get('id', f'resource_{idx}')
                    json_file_path = os.path.join(json_dir, f"{resource_type}_{resource_id}.json")
                    with open(json_file_path, 'w') as json_file:
                        json.dump(resource, json_file, indent=2, default=str)
            
            logger.info(f"Wrote {len(resources)} {resource_type} resources to {table_dir}")
            logger.info(f"  - JSONL (bulk): {jsonl_file_path}")
            logger.info(f"  - JSON files (view): {json_dir}")
            
        except Exception as e:
            logger.error(f"Error writing {resource_type}: {e}")
            raise
    
    def read_bronze(self, resource_type: str, date: str = None) -> List[Dict]:
        """Read from Bronze table."""
        try:
            table_dir = os.path.join(self.bronze_path, f"bronze_{resource_type.lower()}")
            
            if date is None:
                date = datetime.now().strftime("%Y-%m-%d")
            
            partition_dir = os.path.join(table_dir, f"ingestion_date={date}")
            file_path = os.path.join(partition_dir, "data.jsonl")
            
            resources = []
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    for line in f:
                        if line.strip():
                            resources.append(json.loads(line))
            
            logger.info(f"Read {len(resources)} {resource_type} resources from Bronze")
            return resources
            
        except Exception as e:
            logger.error(f"Error reading from Bronze: {e}")
            return []
