import json
import os
import logging
from typing import Dict, Any, Optional
import firebase_admin
from firebase_admin import db, credentials
from src.common.config import get_config

logger = logging.getLogger(__name__)
config = get_config()


class FirebaseClient:
    """Firebase Realtime Database client for syncing transformed data."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        try:
            # Load Firebase credentials from the secret env var when available,
            # otherwise fall back to the configured file path.
            credentials_json = os.getenv("FIREBASE_CREDENTIALS_JSON") or os.getenv("FIREBASE_CREDENTIALS")
            if credentials_json:
                creds_dict = json.loads(credentials_json)
                credentials_source = "environment"
            else:
                with open(config.FIREBASE_CREDENTIALS_PATH, 'r') as f:
                    creds_dict = json.load(f)
                credentials_source = config.FIREBASE_CREDENTIALS_PATH
            
            cred = credentials.Certificate(creds_dict)
            firebase_admin.initialize_app(cred, {
                'databaseURL': f'https://{config.FIREBASE_PROJECT_ID}.firebaseio.com'
            })
            
            self.db = db
            self._initialized = True
            logger.info(f"Firebase client initialized from {credentials_source}")
        except Exception as e:
            logger.error(f"Failed to initialize Firebase: {e}")
            raise
    
    def write_patients(self, patients: list) -> bool:
        """Write patient data to Firebase."""
        try:
            ref = self.db.reference('patients')
            for patient in patients:
                patient_id = patient.get('patient_id')
                ref.child(patient_id).set(patient)
            logger.info(f"Wrote {len(patients)} patients to Firebase")
            return True
        except Exception as e:
            logger.error(f"Error writing patients to Firebase: {e}")
            return False
    
    def write_encounters(self, encounters: list) -> bool:
        """Write encounter data to Firebase."""
        try:
            ref = self.db.reference('encounters')
            for encounter in encounters:
                encounter_id = encounter.get('encounter_id')
                ref.child(encounter_id).set(encounter)
            logger.info(f"Wrote {len(encounters)} encounters to Firebase")
            return True
        except Exception as e:
            logger.error(f"Error writing encounters to Firebase: {e}")
            return False
    
    def write_observations(self, observations: list) -> bool:
        """Write observation data to Firebase."""
        try:
            ref = self.db.reference('observations')
            for obs in observations:
                obs_id = obs.get('obs_id')
                ref.child(obs_id).set(obs)
            logger.info(f"Wrote {len(observations)} observations to Firebase")
            return True
        except Exception as e:
            logger.error(f"Error writing observations to Firebase: {e}")
            return False
    
    def write_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Write sync metadata to Firebase."""
        try:
            ref = self.db.reference('metadata')
            ref.update(metadata)
            logger.info(f"Updated metadata: {metadata}")
            return True
        except Exception as e:
            logger.error(f"Error writing metadata to Firebase: {e}")
            return False
    
    def read_patients(self) -> Optional[Dict]:
        """Read patient data from Firebase."""
        try:
            ref = self.db.reference('patients')
            return ref.get().val()
        except Exception as e:
            logger.error(f"Error reading patients from Firebase: {e}")
            return None
