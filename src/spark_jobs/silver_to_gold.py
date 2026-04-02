"""
Silver to Gold transformation for geographic disease analysis.

Aggregates conditions by geographic location, condition type, and time
and produces UI-ready JSON outputs for dashboard queries with time filtering.

Reads Parquet files from Silver layer (output of bronze_to_silver.py)
"""
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple
import h3
import pandas as pd
import pyarrow.parquet as pq

from src.common.config import get_config
from src.ingestion.metadata_manager import MetadataManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# H3 resolution for hexagonal binning (lower = larger hexagons, more aggregation)
H3_RESOLUTION = 4
config = get_config()


def _new_partitioned_data():
    return defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
        'city': None,
        'state': None,
        'latitude': None,
        'longitude': None,
        'case_count': 0,
        'dates': []
    })))


def _normalize_location_value(value):
    """Return a clean scalar value or None for empty / null-like inputs."""
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, str):
        value = value.strip()
        return value or None

    return value


def _format_location_bucket_component(value, missing_label: str) -> str:
    """Format a location field for bucket keys with a stable missing-value label."""
    normalized_value = _normalize_location_value(value)
    if normalized_value is None:
        return missing_label

    text = str(normalized_value).strip().replace(' ', '_')
    if text.lower() in {'na', 'n/a', 'unknown'}:
        return missing_label

    return text


def _coerce_datetime_value(value):
    """Convert common pandas / string timestamp values to naive datetime objects."""
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if hasattr(value, 'to_pydatetime'):
        try:
            value = value.to_pydatetime()
        except Exception:
            return None

    if isinstance(value, datetime):
        return value.replace(tzinfo=None)

    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)
        except Exception:
            return None

    return None


def load_existing_gold_state(gold_path: Path):
    """Load the current gold snapshot so the next run can merge into it."""
    partitioned_data = _new_partitioned_data()
    h3_metadata = {}
    condition_display_map = {}

    if not gold_path.exists():
        return partitioned_data, h3_metadata, condition_display_map

    conditions_file = gold_path / "_conditions.json"
    if conditions_file.exists():
        try:
            with open(conditions_file, 'r', encoding='utf-8') as f:
                conditions_meta = json.load(f)
                if isinstance(conditions_meta.get('condition_display_map'), dict):
                    for code, display in conditions_meta.get('condition_display_map', {}).items():
                        if code:
                            condition_display_map[str(code)] = str(display or code)
                else:
                    for code in conditions_meta.get('conditions', []):
                        if code:
                            condition_display_map[str(code)] = str(code)
        except Exception:
            pass

    h3_ref_file = gold_path / "_h3_reference.json"
    if h3_ref_file.exists():
        try:
            with open(h3_ref_file, 'r', encoding='utf-8') as f:
                h3_meta = json.load(f)
                for h3_id, meta in h3_meta.get('h3_cells', {}).items():
                    if isinstance(h3_id, str) and h3_id.startswith('NO_GEO::'):
                        continue
                    if isinstance(meta, dict):
                        latitude = meta.get('latitude')
                        longitude = meta.get('longitude')
                        if latitude is not None and longitude is not None:
                            h3_metadata[h3_id] = {
                                'latitude': latitude,
                                'longitude': longitude,
                            }
        except Exception:
            pass

    for condition_dir in gold_path.iterdir():
        if not condition_dir.is_dir():
            continue
        if condition_dir.name.startswith('_') or condition_dir.name in {'current', 'runs'}:
            continue

        condition_code = condition_dir.name
        if condition_display_map and condition_code not in condition_display_map:
            # Ignore legacy directories that do not match current condition-code keys.
            continue
        condition_display_map.setdefault(condition_code, condition_code)

        for date_file in condition_dir.glob('*.jsonl'):
            date_key = date_file.stem
            for record in read_jsonl_file(date_file):
                h3_id = record.get('h3')
                if not h3_id:
                    continue
                if isinstance(h3_id, str) and h3_id.startswith('NO_GEO::'):
                    continue

                agg = partitioned_data[condition_code][date_key][h3_id]
                agg['case_count'] += int(record.get('case_count', 0))

    return partitioned_data, h3_metadata, condition_display_map


def extract_condition_date(condition: dict) -> datetime:
    """Extract onset date from FHIR condition resource, default to now if not found."""
    if 'onsetDateTime' in condition:
        try:
            return datetime.fromisoformat(condition['onsetDateTime'].replace('Z', '+00:00')).replace(tzinfo=None)
        except:
            pass
    return datetime.now()


def extract_location(patient: dict) -> dict:
    """Extract city, state, zipcode, and coordinates from silver or FHIR patient data."""
    location = {
        'city': None,
        'state': None,
        'zipcode': None,
        'country': None,
        'latitude': None,
        'longitude': None
    }

    # Silver patient rows are already flattened, so prefer those fields first.
    flat_field_map = {
        'city': 'city',
        'state': 'state',
        'zipcode': 'zipcode',
        'postalCode': 'zipcode',
        'country': 'country',
        'latitude': 'latitude',
        'longitude': 'longitude',
    }

    for source_field, target_field in flat_field_map.items():
        value = _normalize_location_value(patient.get(source_field))
        if value is not None and location[target_field] is None:
            location[target_field] = value
    
    # Fall back to nested FHIR addresses when present.
    if 'address' in patient and isinstance(patient['address'], list):
        for addr in patient['address']:
            if isinstance(addr, dict):
                location['city'] = _normalize_location_value(addr.get('city')) or location['city']
                location['state'] = _normalize_location_value(addr.get('state')) or location['state']
                location['zipcode'] = _normalize_location_value(addr.get('postalCode')) or location['zipcode']
                location['country'] = _normalize_location_value(addr.get('country')) or location['country']
                
                # Extract geolocation if available
                if 'extension' in addr:
                    for ext in addr['extension']:
                        if isinstance(ext, dict) and ext.get('url', '').endswith('geolocation'):
                            if 'extension' in ext:
                                for geo_ext in ext['extension']:
                                    if 'latitude' in geo_ext.get('url', ''):
                                        location['latitude'] = geo_ext.get('valueDecimal')
                                    elif 'longitude' in geo_ext.get('url', ''):
                                        location['longitude'] = geo_ext.get('valueDecimal')

    # Normalize coordinates after all extraction paths have been applied.
    location['latitude'] = _coerce_coordinate_value(location.get('latitude'))
    location['longitude'] = _coerce_coordinate_value(location.get('longitude'))
    
    return location


def extract_condition_code(condition: dict) -> str:
    """Extract condition code/display from FHIR condition resource."""
    if 'code' in condition and isinstance(condition['code'], dict):
        if 'text' in condition['code']:
            return condition['code']['text']
        if 'coding' in condition['code'] and isinstance(condition['code']['coding'], list):
            for coding in condition['code']['coding']:
                if isinstance(coding, dict) and 'display' in coding:
                    return coding['display']
                elif isinstance(coding, dict) and 'code' in coding:
                    return coding['code']
    return 'Unknown Condition'


def extract_condition_identity(condition: dict) -> Tuple[str, str]:
    """Extract stable condition code and display label from Silver/FHIR records."""
    condition_code = _normalize_location_value(condition.get('condition_code'))
    condition_display = _normalize_location_value(condition.get('condition_text'))

    if 'code' in condition and isinstance(condition['code'], dict):
        code_obj = condition['code']
        if condition_display is None:
            condition_display = _normalize_location_value(code_obj.get('text'))
        if 'coding' in code_obj and isinstance(code_obj['coding'], list):
            for coding in code_obj['coding']:
                if not isinstance(coding, dict):
                    continue
                if condition_code is None:
                    condition_code = _normalize_location_value(coding.get('code'))
                if condition_display is None:
                    condition_display = _normalize_location_value(coding.get('display'))
                if condition_code is not None and condition_display is not None:
                    break

    if condition_display is None:
        fallback_display = extract_condition_code(condition)
        if fallback_display != 'Unknown Condition':
            condition_display = fallback_display

    if condition_code is None:
        condition_code = condition_display

    if condition_code is None:
        return "", ""

    if condition_display is None:
        condition_display = condition_code

    return str(condition_code), str(condition_display)


def condition_code_to_dir_name(condition_code: str) -> str:
    """Convert a condition code into a filesystem-safe directory name."""
    return str(condition_code).strip().replace('/', '_').replace(' ', '_')


def _coerce_coordinate_value(value):
    """Normalize coordinate-like values to float, returning None when invalid."""
    normalized = _normalize_location_value(value)
    if normalized is None:
        return None

    try:
        return float(normalized)
    except (TypeError, ValueError):
        return None


def _latlng_to_h3(lat: float, lng: float, resolution: int):
    """Convert coordinates to H3 index across h3-py API versions."""
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lng, resolution)
    if hasattr(h3, "geo_to_h3"):
        return h3.geo_to_h3(lat, lng, resolution)
    raise AttributeError("No supported H3 coordinate conversion API found")


def _h3_to_latlng(h3_id: str):
    """Convert H3 index to center coordinates across h3-py API versions."""
    if hasattr(h3, "cell_to_latlng"):
        return h3.cell_to_latlng(h3_id)
    if hasattr(h3, "h3_to_geo"):
        return h3.h3_to_geo(h3_id)
    raise AttributeError("No supported H3 reverse conversion API found")


def read_parquet_files(parquet_dir: Path, run_ts: str = None) -> pd.DataFrame:
    """Read all Parquet files from a directory and return combined DataFrame."""
    try:
        if not parquet_dir.exists():
            logger.warn(f"Directory not found: {parquet_dir}")
            return pd.DataFrame()

        # Silver is stored as plain parquet, so recurse through the table
        # directories and collect every parquet file that was written.
        if run_ts:
            parquet_files = [
                parquet_file
                for parquet_file in sorted(parquet_dir.rglob(f"{run_ts}.parquet"), key=lambda path: str(path))
                if ".hoodie" not in parquet_file.parts
            ]
            # If the requested run timestamp is missing, gracefully fall back to latest available snapshots.
            if not parquet_files:
                logger.warning(
                    f"No parquet files found for SILVER_TS={run_ts} in {parquet_dir}; "
                    "falling back to all available parquet snapshots"
                )
                parquet_files = [
                    parquet_file
                    for parquet_file in sorted(parquet_dir.rglob("*.parquet"), key=lambda path: str(path))
                    if ".hoodie" not in parquet_file.parts
                ]
        else:
            parquet_files = [
                parquet_file
                for parquet_file in sorted(parquet_dir.rglob("*.parquet"), key=lambda path: str(path))
                if ".hoodie" not in parquet_file.parts
            ]
        
        if not parquet_files:
            logger.warning(f"No parquet files found in {parquet_dir}")
            return pd.DataFrame()
        
        dfs = []
        for pf in parquet_files:
            try:
                df = pd.read_parquet(pf)
                dfs.append(df)
            except Exception as e:
                logger.debug(f"Skipped parquet file {pf}: {e}")
        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading parquet files from {parquet_dir}: {e}")
        return pd.DataFrame()


def read_jsonl_file(filepath: Path) -> List[dict]:
    """Read JSONL file and return list of dictionaries."""
    records = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.debug(f"Skipped invalid JSON line: {e}")
    except FileNotFoundError:
        logger.warning(f"File not found: {filepath}")
    except Exception as e:
        logger.error(f"Error reading JSONL file {filepath}: {e}")
    return records


def generate_time_filtered_hotspots(conditions_by_location: dict, time_window_hours: int, gold_path: Path, window_name: str) -> dict:
    """
    Generate hotspots and stats for a specific time window (e.g., last 1 week).
    
    Args:
        conditions_by_location: Full dataset with timestamp info
        time_window_hours: Hours to look back (e.g., 168 for 1 week)
        gold_path: Output directory
        window_name: Time window name (e.g., "1w")
    
    Returns:
        Dictionary with counts for this window
    """
    cutoff_time = datetime.now() - timedelta(hours=time_window_hours)

    # Filter cases by time window
    filtered_hotspots = []
    window_stats = {
        'total_cases': 0,
        'unique_conditions': set(),
        'affected_regions': set(),
        'unique_cities': set()
    }
    
    max_case_count = 1
    conditions_data = {}  # condition -> {city: count, ...}
    
    for condition, locations_dict in conditions_by_location.items():
        conditions_data[condition] = {}
        for location_name, loc_data in locations_dict.items():
            # Filter dates within time window
            filtered_dates = [d for d in loc_data['dates'] if datetime.fromisoformat(d) > cutoff_time]
            case_count = len(filtered_dates)
            
            if case_count > 0 and loc_data['latitude'] is not None and loc_data['longitude'] is not None:
                window_stats['total_cases'] += case_count
                window_stats['unique_conditions'].add(condition)
                if loc_data['state']:
                    window_stats['affected_regions'].add(loc_data['state'])
                if loc_data['city']:
                    window_stats['unique_cities'].add(loc_data['city'])
                
                conditions_data[condition][location_name] = case_count
                max_case_count = max(max_case_count, case_count)
                
                # Create hotspot feature
                intensity = case_count / max_case_count
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(loc_data['longitude']), float(loc_data['latitude'])]
                    },
                    "properties": {
                        "condition": condition,
                        "location": location_name,
                        "city": loc_data['city'],
                        "state": loc_data['state'],
                        "zipcode": loc_data['zipcode'],
                        "case_count": case_count,
                        "intensity": round(intensity, 3),
                        "time_window": window_name,
                        "generated_at": datetime.now().isoformat()
                    }
                }
                filtered_hotspots.append(feature)
    
    # Save time-window filtered hotspots
    output_file = gold_path / f"disease_hotspots_{window_name}.json"
    geojson = {
        "type": "FeatureCollection",
        "features": filtered_hotspots,
        "properties": {
            "time_window": window_name,
            "cutoff_time": cutoff_time.isoformat(),
            "generated_at": datetime.now().isoformat()
        }
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2)
    
    logger.info(f"[OK] Created disease_hotspots_{window_name}.json ({window_stats['total_cases']} cases)")
    
    # Save time-window filtered stats
    output_stats = gold_path / f"disease_stats_{window_name}.json"
    stats = {
        "generated_at": datetime.now().isoformat(),
        "time_window": window_name,
        "cutoff_time": cutoff_time.isoformat(),
        "total_cases": window_stats['total_cases'],
        "unique_conditions": len(window_stats['unique_conditions']),
        "affected_regions": len(window_stats['affected_regions']),
        "unique_cities": len(window_stats['unique_cities'])
    }
    
    with open(output_stats, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)
    
    logger.info(f"[OK] Created disease_stats_{window_name}.json")
    
    return window_stats


def transform_silver_to_gold(silver_path, gold_path) -> Tuple[int, str]:
    """
    Transform silver layer to gold layer with daily aggregations partitioned by date and condition.
    
    Output structure (scalable for big data):
    gold/
    ├── 2026-03-22/
    │   ├── Acute_bronchitis/
    │   │   └── data.json (all Acute bronchitis cases on this date)
    │   ├── Flu/
    │   │   └── data.json
    │   └── Normal_pregnancy/
    │       └── data.json
    ├── 2026-03-21/
    │   └── ... (same structure)
    
    Each partition contains daily counts by location and state.
    This enables:
    - Query by date range: load only needed date partitions
    - Query by condition: load only condition partitions
    - Incremental updates: add new dates without reprocessing
    - Distributed processing: each partition processed independently
    
    Args:
        silver_path: Path to silver layer directory (str or Path)
        gold_path: Path to gold layer directory (str or Path, will be created if not exists)
    
    Returns:
        Tuple of (success_code, message)
    """
    # Convert string paths to Path objects
    silver_path = Path(silver_path)
    gold_path = Path(gold_path)
    
    logger.info("="*60)
    logger.info("Silver to Gold Transformation (Partitioned by Date + Condition)")
    logger.info("="*60)
    logger.info(f"Silver Path: {silver_path}")
    logger.info(f"Gold Path: {gold_path}")
    logger.info("Output: Partitioned by date/condition for big data scalability")
    
    # Create gold directory
    gold_path.mkdir(parents=True, exist_ok=True)
    
    # Read silver layer data from Parquet files
    logger.info("")
    logger.info("Reading silver layer data (Parquet format)...")
    
    # Silver layer structure: <silver_root>/<table>/<YYYYMMDD>/<timestamp>.parquet
    patient_dir = silver_path / "patient"
    encounter_dir = silver_path / "encounter"
    observation_dir = silver_path / "observation"
    condition_dir = silver_path / "condition"
    
    active_run_ts = os.getenv("SILVER_TS")

    patients_df = read_parquet_files(patient_dir, active_run_ts)
    encounter_df = read_parquet_files(encounter_dir, active_run_ts)
    observation_df = read_parquet_files(observation_dir, active_run_ts)
    condition_df = read_parquet_files(condition_dir, active_run_ts)
    
    if patients_df.empty:
        msg = f"No patient data found in {patient_dir}"
        logger.error(f"[ERROR] {msg}")
        # fallback: try JSONL data files
        patient_dir_data = patient_dir / "data.jsonl"
        encounter_dir_data = encounter_dir / "data.jsonl"
        observation_dir_data = observation_dir / "data.jsonl"
        patients_list = read_jsonl_file(patient_dir_data)
        encounter_list = read_jsonl_file(encounter_dir_data)
        observation_list = read_jsonl_file(observation_dir_data)

        # Convert lists (from JSONL) into DataFrames for consistent downstream usage
        try:
            if isinstance(patients_list, list):
                patients_df = pd.DataFrame(patients_list)
            else:
                patients_df = pd.DataFrame()
        except Exception:
            patients_df = pd.DataFrame()

        try:
            if isinstance(encounter_list, list):
                encounter_df = pd.DataFrame(encounter_list)
            else:
                encounter_df = pd.DataFrame()
        except Exception:
            encounter_df = pd.DataFrame()

        try:
            if isinstance(observation_list, list):
                observation_df = pd.DataFrame(observation_list)
            else:
                observation_df = pd.DataFrame()
        except Exception:
            observation_df = pd.DataFrame()

    if condition_df.empty:
        condition_dir_data = condition_dir / "data.jsonl"
        condition_list = read_jsonl_file(condition_dir_data)

        try:
            if isinstance(condition_list, list):
                condition_df = pd.DataFrame(condition_list)
            else:
                condition_df = pd.DataFrame()
        except Exception:
            condition_df = pd.DataFrame()
        
    logger.info(f"[OK] Read {len(patients_df)} patients")
    logger.info(f"[OK] Read {len(encounter_df)} encounters")
    logger.info(f"[OK] Read {len(observation_df)} observations")
    logger.info(f"[OK] Read {len(condition_df)} conditions")

    existing_partitioned_data, h3_metadata, condition_display_map = load_existing_gold_state(gold_path)
    
    # Build patient ID -> location mapping from silver patients
    patient_locations = {}
    # this should work across parquet and jsonl formats
    for _, patient_row in patients_df.iterrows():
        # Ensure we have a dict-like object
        try:
            patient_dict = patient_row if isinstance(patient_row, dict) else patient_row.to_dict()
        except Exception:
            patient_dict = {}

        patient_id = patient_dict.get('patient_id') or patient_dict.get('id') or patient_dict.get('subject')
        if not patient_id:
            continue

        # Extract location using existing helper (works with FHIR patient dict)
        loc = extract_location(patient_dict)

        patient_locations[patient_id] = {
            'city': loc.get('city'),
            'state': loc.get('state'),
            'zipcode': loc.get('zipcode'),
            'country': loc.get('country'),
            'latitude': loc.get('latitude'),
            'longitude': loc.get('longitude')
        }
    
    logger.info(f"[OK] Extracted locations for {len(patient_locations)} patients")

    encounter_dates = {}
    for _, encounter_row in encounter_df.iterrows():
        try:
            encounter_dict = encounter_row if isinstance(encounter_row, dict) else encounter_row.to_dict()
        except Exception:
            encounter_dict = {}

        encounter_id = encounter_dict.get('encounter_id') or encounter_dict.get('id') or encounter_dict.get('encounter')

        encounter_date = _coerce_datetime_value(encounter_dict.get('encounter_date'))
        if encounter_date is None:
            period = encounter_dict.get('period')
            if isinstance(period, dict):
                encounter_date = _coerce_datetime_value(period.get('start'))

        if encounter_id and encounter_date is not None:
            encounter_dates[str(encounter_id)] = encounter_date
    
    # Aggregate conditions by geography WITH COORDINATES and TIMESTAMPS
    logger.info("")
    logger.info("Aggregating conditions by geography, time, and condition type...")
    
    # Structure: condition -> city_name -> data with count and dates
    conditions_by_location = defaultdict(lambda: defaultdict(lambda: {
        'latitude': None,
        'longitude': None,
        'city': None,
        'state': None,
        'zipcode': None,
        'case_count': 0,
        'dates': []  # Track dates for time-series
    }))
    
    # Time-based aggregations
    conditions_by_state = defaultdict(lambda: defaultdict(int))  # state -> condition -> count
    conditions_by_city = defaultdict(lambda: defaultdict(int))  # city -> condition -> count
    conditions_by_zipcode = defaultdict(lambda: defaultdict(int))  # zipcode -> condition -> count
    conditions_by_condition = defaultdict(lambda: defaultdict(int))  # condition -> state -> count
    # condition_display_map is seeded from the current gold snapshot and updated with this run
    
    patient_conditions = defaultdict(list)
    condition_patient_sets = defaultdict(set)

    # Gold derives condition events from Condition_silver records.
    # Encounter dates are only used as a fallback when condition timestamps are missing.
    condition_events = []
    skip_stats = {
        'missing_patient_id': 0,
        'missing_condition_label': 0,
        'patient_not_found_in_locations': 0,
        'missing_geo': 0,
        'h3_conversion_failed': 0,
    }
    for _, row in condition_df.iterrows():
        # Resolve patient reference
        patient_ref = None
        if 'patient_id' in row and not pd.isna(row.get('patient_id')):
            patient_ref = row.get('patient_id')
        else:
            subj = row.get('subject')
            if isinstance(subj, dict):
                patient_ref = subj.get('reference')
            elif isinstance(subj, str):
                patient_ref = subj

        patient_id = patient_ref

        if patient_id is None:
            skip_stats['missing_patient_id'] += 1
            continue

        condition_dict = row if isinstance(row, dict) else row.to_dict()

        # Resolve stable condition code/display from Silver condition fields.
        condition_code, condition_display = extract_condition_identity(condition_dict)
        if not condition_code:
            skip_stats['missing_condition_label'] += 1
            continue


        condition_date = _coerce_datetime_value(condition_dict.get('condition_date'))
        if condition_date is None:
            condition_date = _coerce_datetime_value(condition_dict.get('recordedDate'))
        if condition_date is None:
            condition_date = _coerce_datetime_value(condition_dict.get('onsetDateTime'))

        encounter_id = condition_dict.get('encounter_id')
        if condition_date is None and encounter_id:
            condition_date = encounter_dates.get(str(encounter_id))

        if condition_date is None:
            condition_date = datetime.now()

        condition_events.append({
            'patient_id': patient_id,
            'condition_code': condition_code,
            'condition_display': condition_display,
            'condition_date': condition_date
        })

    for event in condition_events:
        patient_id = event['patient_id']

        if not patient_id or patient_id not in patient_locations:
            skip_stats['patient_not_found_in_locations'] += 1
            continue

        condition_code = event['condition_code']
        condition_display = event['condition_display']
        condition_date = event['condition_date']
        location = patient_locations[patient_id]

        patient_conditions[patient_id].append(condition_code)
        condition_patient_sets[condition_code].add(patient_id)
        condition_display_map[condition_code] = condition_display

        # Create location key
        city_key = f"{location['city']}, {location['state']}" if location['city'] and location['state'] else location['city'] or 'Unknown'

        # Aggregate with coordinates for mapping
        loc_data = conditions_by_location[condition_code][city_key]
        loc_data['latitude'] = location['latitude']
        loc_data['longitude'] = location['longitude']
        loc_data['city'] = location['city']
        loc_data['state'] = location['state']
        loc_data['zipcode'] = location['zipcode']
        loc_data['case_count'] += 1
        loc_data['dates'].append(condition_date.isoformat())

        # State and condition aggregations
        if location['state']:
            conditions_by_state[condition_code][location['state']] += 1
            conditions_by_condition[condition_code][location['state']] += 1
        if location['city']:
            conditions_by_city[condition_code][location['city']] += 1
        if location['zipcode']:
            conditions_by_zipcode[condition_code][location['zipcode']] += 1
    
    logger.info(f"[OK] Processed {len(patient_conditions)} patient-condition relationships")
    
    # OUTPUT: Partition by CONDITION then DATE (optimal for big data + zipcode aggregation)
    logger.info("")
    logger.info("Generating partitioned gold layer (condition/date.jsonl with H3 binning)...")
    logger.info("")
    logger.info("H3 Configuration:")
    logger.info(f"  Resolution: {H3_RESOLUTION}")
    logger.info("  Aggregation: One record per H3 cell (hexagon center) per date per condition")
    logger.info("")
    
    # Group data by (condition, date, h3_id) for aggregation.
    # Only coordinate-backed rows are emitted to Gold.
    partitioned_data = existing_partitioned_data
    
    for condition, locations_dict in conditions_by_location.items():
        for location_name, loc_data in locations_dict.items():
            lat = loc_data.get('latitude')
            lng = loc_data.get('longitude')
            has_geo = lat is not None and lng is not None and not pd.isna(lat) and not pd.isna(lng)

            if not has_geo:
                skip_stats['missing_geo'] += loc_data.get('case_count', 0)
                continue
            else:
                # Convert lat/long to H3 cell ID
                try:
                    h3_id = _latlng_to_h3(lat, lng, H3_RESOLUTION)
                except Exception:
                    skip_stats['h3_conversion_failed'] += loc_data.get('case_count', 0)
                    continue
            
            # Process each date occurrence
            for date_str in loc_data['dates']:
                try:
                    date_obj = datetime.fromisoformat(date_str)
                except Exception:
                    date_obj = datetime.now()

                date_key = date_obj.strftime("%Y-%m-%d")

                # Aggregate at H3/fallback-cell level
                agg = partitioned_data[condition][date_key][h3_id]
                agg['city'] = loc_data['city']
                agg['state'] = loc_data['state']
                agg['latitude'] = lat if has_geo else None
                agg['longitude'] = lng if has_geo else None
                agg['case_count'] += 1
    
    # Write partitioned output: gold/condition/date.jsonl
    total_partitions = 0
    
    for condition in sorted(partitioned_data.keys()):
        # Use condition code as directory key.
        condition_dir_name = condition_code_to_dir_name(condition)
        condition_dir = gold_path / condition_dir_name
        condition_dir.mkdir(parents=True, exist_ok=True)
        
        for date_key in sorted(partitioned_data[condition].keys()):
            # Write JSONL: one record per H3 cell per date (normalized: h3 + case_count only)
            output_file = condition_dir / f"{date_key}.jsonl"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for h3_id, agg_data in partitioned_data[condition][date_key].items():
                    # Minimal record: h3 ID (hexagon) + case count
                    record = {
                        "date": date_key,
                        "h3": h3_id,
                        "case_count": agg_data['case_count']
                    }
                    f.write(json.dumps(record) + '\n')
                    
                    # Collect H3 cell metadata (hexagon center coordinates)
                    if h3_id not in h3_metadata:
                        try:
                            # Get hexagon center coordinates
                            center_lat, center_lng = _h3_to_latlng(h3_id)
                            h3_metadata[h3_id] = {
                                'latitude': center_lat,
                                'longitude': center_lng
                            }
                        except Exception:
                            pass
            
            total_partitions += 1
    
    logger.info(f"[OK] Created {total_partitions} partitions (condition/date.jsonl with H3 binning)")
    
    # Metadata: conditions list
    output_conditions = gold_path / "_conditions.json"
    eligible_condition_codes = sorted(
        [
            code
            for code, patient_ids in condition_patient_sets.items()
            if len(patient_ids) > 0
        ]
    )
    condition_code_map = {
        code: condition_display_map.get(code, code)
        for code in eligible_condition_codes
    }
    condition_patient_counts = {
        code: len(condition_patient_sets[code])
        for code in eligible_condition_codes
    }

    conditions_meta = {
        "generated_at": datetime.now().isoformat(),
        "conditions": eligible_condition_codes,
        "condition_display_map": condition_code_map,
        "condition_patient_counts": condition_patient_counts,
        "total_unique": len(eligible_condition_codes)
    }
    with open(output_conditions, 'w', encoding='utf-8') as f:
        json.dump(conditions_meta, f, indent=2)
    
    logger.info(f"[OK] Created _conditions.json ({len(eligible_condition_codes)} conditions with patient_count > 0)")
    
    # Metadata: H3 cell reference (lat/long lookup - shared across all conditions)
    output_h3_ref = gold_path / "_h3_reference.json"
    with open(output_h3_ref, 'w', encoding='utf-8') as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "h3_resolution": H3_RESOLUTION,
            "h3_cells": h3_metadata,
            "total_unique": len(h3_metadata)
        }, f, indent=2)
    
    logger.info(f"[OK] Created _h3_reference.json ({len(h3_metadata)} H3 cells)")
    logger.info(
        "Gold aggregation skip stats: "
        f"missing_patient_id={skip_stats['missing_patient_id']}, "
        f"missing_condition_label={skip_stats['missing_condition_label']}, "
        f"patient_not_found_in_locations={skip_stats['patient_not_found_in_locations']}, "
        f"missing_geo={skip_stats['missing_geo']}, "
        f"h3_conversion_failed={skip_stats['h3_conversion_failed']}"
    )
    
    return 0, "Transformation completed successfully"


if __name__ == "__main__":
    """Main entry point for Silver to Gold transformation job."""
    import sys
    import os
    
    # Get paths from environment or use defaults
    metadata_manager = MetadataManager(config.MONGODB_URI, config.MONGODB_DB)
    silver_state = metadata_manager.get_pipeline_state("silver_latest_run")
    silver_ts = os.getenv("SILVER_TS")
    pipeline_run_id = os.getenv("PIPELINE_RUN_ID")
    if not pipeline_run_id and silver_state:
        pipeline_run_id = silver_state.get("pipeline_run_id")
    if not silver_ts and silver_state and silver_state.get("run_ts"):
        silver_ts = silver_state.get("run_ts")
    if silver_ts:
        os.environ["SILVER_TS"] = str(silver_ts)
    silver_path = os.getenv("SILVER_PATH", config.SILVER_PATH)
    gold_path = os.getenv("GOLD_PATH")
    if not gold_path:
        try:
            gold_path = str(Path(silver_path).parent / "gold")
        except Exception:
            gold_path = config.GOLD_PATH

    if silver_ts and str(silver_ts).isdigit():
        silver_run_date = datetime.utcfromtimestamp(int(silver_ts) / 1000.0).strftime("%Y%m%d")
        resolved_silver_path = f"{silver_path}/<table>/{silver_run_date}/{silver_ts}.parquet"
    elif silver_ts:
        resolved_silver_path = f"{silver_path}/<table>/<YYYYMMDD>/{silver_ts}.parquet"
    else:
        resolved_silver_path = silver_path
    
    logger.info("="*60)
    logger.info("SILVER TO GOLD TRANSFORMATION JOB")
    logger.info("="*60)
    logger.info(f"Silver Path: {silver_path}")
    logger.info(f"Gold Path: {gold_path}")
    if pipeline_run_id:
        logger.info(f"Pipeline Run ID: {pipeline_run_id}")
    
    # Run transformation
    exit_code, message = transform_silver_to_gold(silver_path, gold_path)

    if exit_code == 0:
        try:
            metadata_manager.set_pipeline_state(
                "gold_latest_run",
                {
                    "pipeline_run_id": pipeline_run_id,
                    "silver_ts": silver_ts,
                    "silver_run_path": resolved_silver_path,
                    "silver_root_path": silver_path,
                    "gold_run_path": gold_path,
                    "updated_at": datetime.utcnow(),
                },
            )
            metadata_manager.record_job(
                "silver_to_gold",
                "completed",
                0,
                pipeline_run_id=pipeline_run_id,
                details={
                    "silver_ts": silver_ts,
                    "silver_path": resolved_silver_path,
                    "silver_root_path": silver_path,
                    "gold_path": gold_path,
                },
            )
        except Exception as metadata_err:
            logger.warning(f"Failed to write gold metadata: {metadata_err}")
    else:
        try:
            metadata_manager.record_job(
                "silver_to_gold",
                "failed",
                0,
                message,
                pipeline_run_id=pipeline_run_id,
                details={
                    "silver_ts": silver_ts,
                    "silver_path": resolved_silver_path,
                    "silver_root_path": silver_path,
                    "gold_path": gold_path,
                },
            )
        except Exception as metadata_err:
            logger.warning(f"Failed to write gold failure metadata: {metadata_err}")
    
    logger.info("")
    logger.info("="*60)
    if exit_code == 0:
        logger.info("✅ TRANSFORMATION SUCCESSFUL")
    else:
        logger.error("❌ TRANSFORMATION FAILED")
    logger.info(f"Message: {message}")
    logger.info("="*60)
    
    sys.exit(exit_code)
