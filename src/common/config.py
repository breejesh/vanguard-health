import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
env_file = os.getenv("ENV_FILE", "config/local-dev.env")
if os.path.exists(env_file):
    load_dotenv(env_file)


class Config:
    """Application configuration."""
    
    # Data paths
    DATA_PATH = os.getenv("DATA_PATH", "/data")
    BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/bronze")
    SILVER_PATH = os.getenv("SILVER_PATH", "/data/silver")
    GOLD_PATH = os.getenv("GOLD_PATH", "/data/gold")
    
    # Synthea FHIR API
    SYNTHEA_API_URL = os.getenv("SYNTHEA_API_URL", "https://r4.smarthealthit.org")
    SYNTHEA_API_KEY = os.getenv("SYNTHEA_API_KEY", "local-key")
    SYNTHEA_FETCH_INTERVAL_HOURS = int(os.getenv("SYNTHEA_FETCH_INTERVAL_HOURS", "6"))
    FETCH_LOOKBACK_DAYS = int(os.getenv("FETCH_LOOKBACK_DAYS", "180"))
    
    # MongoDB
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://root:mongopass@localhost:27017/vanguard_metadata?authSource=admin")
    MONGODB_DB = os.getenv("MONGODB_DB", "vanguard_metadata")
    
    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
    
    # Firebase
    FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "vanguard-health")
    FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH", "config/firebase-creds.json")
    
    # Spark
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[4]")
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "logs/vanguard.log")
    
    # Feature flags
    ENABLE_REDIS_CACHING = os.getenv("ENABLE_REDIS_CACHING", "false").lower() == "true"
    ENABLE_FIREBASE_SYNC = os.getenv("ENABLE_FIREBASE_SYNC", "true").lower() == "true"
    TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"


def get_config() -> Config:
    """Get application configuration."""
    return Config()
