"""
FHIR API client for fetching data from Synthea.
"""
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class FHIRFetcher:
    """Fetches FHIR resources from Synthea API."""
    
    def __init__(self, api_url: str, api_key: str = ""):
        # Normalize URL: remove trailing slash, remove /fhir if present to avoid double paths
        self.api_url = api_url.rstrip('/').rstrip('/fhir').rstrip('/')
        self.api_key = api_key
        # Only add auth if key is provided (public APIs don't need it)
        self.headers = {"Accept": "application/fhir+json"}
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"
    
    def fetch_bulk(self, resource_types: List[str] = None, max_per_type: int = None) -> List[Dict]:
        """Fetch all FHIR resources without date filtering (for local testing/bulk load).
        
        Args:
            resource_types: List of resource types to fetch (default: all standard types)
            max_per_type: Max resources per type (None = all, but use wisely on public servers!)
        """
        if resource_types is None:
            resource_types = ["Patient", "Encounter", "Observation", "Condition", "Medication", "MedicationRequest"]
        
        # For local testing, use a reasonable default limit to avoid endless fetching
        if max_per_type is None:
            max_per_type = 50  # Fetch up to 50 per type by default for testing
            logger.info(f"Default limit set: {max_per_type} resources per type (use max_per_type param to override)")
        
        resources = []
        
        for resource_type in resource_types:
            try:
                logger.info(f"Fetching {resource_type} resources (limit: {max_per_type})...")
                fetched = self._fetch_resource_type_bulk(resource_type, max_results=max_per_type)
                resources.extend(fetched)
                logger.info(f"  ✓ {len(fetched)} {resource_type} resources fetched")
            except Exception as e:
                logger.error(f"Error fetching {resource_type}: {e}")
                continue
        
        return resources
    
    def _fetch_resource_type_bulk(self, resource_type: str, max_results: int = None) -> List[Dict]:
        """Fetch specific resource type without date filtering."""
        resources = []
        url = f"{self.api_url}/{resource_type}"
        params = {
            "_count": "100",
            "_format": "json"
        }
        
        page_num = 0
        while True:
            if max_results and len(resources) >= max_results:
                break
            
            page_num += 1
            logger.debug(f"    Fetching page {page_num} (current total: {len(resources)})...")
            
            response = requests.get(url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            bundle = response.json()
            entries = bundle.get('entry', [])
            
            if not entries:
                logger.debug(f"    No entries on page {page_num}, stopping pagination.")
                break
            
            for entry in entries:
                if max_results and len(resources) >= max_results:
                    break
                resource = entry.get('resource')
                if resource:
                    resources.append(resource)
            
            logger.debug(f"    Page {page_num}: {len(entries)} entries, total so far: {len(resources)}")
            
            # Follow link for next page
            links = bundle.get('link', [])
            next_link = next((link.get('url') for link in links if link.get('relation') == 'next'), None)
            
            if not next_link:
                logger.debug(f"    No next page link found, stopping pagination.")
                break
            
            # Use the full next URL without additional params
            url = next_link
            params = {}
        
        logger.debug(f"  Total {resource_type} resources fetched: {len(resources)}")
        return resources
        
        return resources
    
    def fetch_incremental(self, last_modified: datetime = None) -> List[Dict]:
        """Fetch FHIR resources modified since last_modified."""
        if last_modified is None:
            last_modified = datetime.now() - timedelta(hours=6)
        
        resources = []
        resource_types = ["Patient", "Encounter", "Observation", "Condition", "Medication", "MedicationRequest"]
        
        for resource_type in resource_types:
            try:
                logger.info(f"Fetching {resource_type} resources modified since {last_modified}")
                resources.extend(self._fetch_resource_type(resource_type, last_modified))
            except Exception as e:
                logger.error(f"Error fetching {resource_type}: {e}")
                continue
        
        return resources
    
    def _fetch_resource_type(self, resource_type: str, last_modified: datetime) -> List[Dict]:
        """Fetch specific resource type."""
        resources = []
        
        # Format timestamp for FHIR query (_lastUpdated=ge2024-01-01T00:00:00Z)
        timestamp_str = last_modified.isoformat() + 'Z' if not last_modified.isoformat().endswith('Z') else last_modified.isoformat()
        
        url = f"{self.api_url}/{resource_type}"
        params = {
            "_lastUpdated": f"ge{timestamp_str}",
            "_count": "100",
            "_format": "json"
        }
        
        while True:
            response = requests.get(url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            bundle = response.json()
            entries = bundle.get('entry', [])
            
            if not entries:
                break
            
            for entry in entries:
                resource = entry.get('resource')
                if resource:
                    resources.append(resource)
            
            logger.info(f"Fetched page of {resource_type}: {len(entries)} entries")
            
            # Check if there's a next page
            links = bundle.get('link', [])
            next_link = next((link.get('url') for link in links if link.get('relation') == 'next'), None)
            
            if not next_link:
                break
            
            url = next_link
            params = {}  # URL already contains params
        
        logger.info(f"Total {resource_type} resources fetched: {len(resources)}")
        return resources
    
    def fetch_patient(self, patient_id: str) -> Optional[Dict]:
        """Fetch single patient record."""
        try:
            url = f"{self.api_url}/Patient/{patient_id}"
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching patient {patient_id}: {e}")
            return None
    
    def test_connection(self) -> bool:
        """Test connection to FHIR API."""
        try:
            url = f"{self.api_url}/Patient?_count=1"
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            logger.info("Successfully connected to FHIR API")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to FHIR API: {e}")
            return False
