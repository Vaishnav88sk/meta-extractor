from dotenv import load_dotenv
import os
from src.utils.logger import get_logger
from typing import Any, Dict

load_dotenv()
logger = get_logger(__name__)

# Fetch DB credentials from env vars based on db_type
def get_db_credentials(db_type: str) -> Dict[str, Any]:
    credentials = {
        "user": os.getenv(f"{db_type}_USER"),
        "password": os.getenv(f"{db_type}_PASSWORD"),
        "host": os.getenv(f"{db_type}_HOST"),
        "port": os.getenv(f"{db_type}_PORT"),
        "database": os.getenv(f"{db_type}_DATABASE"),
        "service_name": os.getenv(f"{db_type}_SERVICE") if db_type == "ORACLE" else None  # Oracle uses service_name instead of database
    }
    logger.info(f"Loaded credentials for {db_type}: {credentials['user']}@{credentials['host']}:{credentials['port']}")
    return credentials