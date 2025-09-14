import mysql.connector
from application_sdk.clients.sql import AsyncBaseSQLClient
from src.utils.logger import get_logger
from typing import Any, Dict

class MySQLClient(AsyncBaseSQLClient):
    DB_CONFIG = {
        "template": "mysql://{user}:{password}@{host}:{port}/{database}",
        "required": ["user", "password", "host", "port", "database"],
    }

    def __init__(self):
        self.logger = get_logger(__name__)

    async def connect(self, credentials: Dict[str, Any]) -> None:       # Connect
        try:
            self.engine = mysql.connector.connect(
                user=credentials["user"],
                password=credentials["password"],
                host=credentials["host"],
                port=credentials["port"],
                database=credentials["database"],
                charset='utf8mb4',
                use_pure=True
            )
            self.logger.info("Successfully connected to MySQL database")        # Logger
        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL: {str(e)}")
            raise

    async def execute_query(self, query: str, *args) -> list:       # Execute a query
        try:
            cursor = self.engine.cursor(dictionary=True)
            cursor.execute(query, args)
            result = cursor.fetchall()
            normalized_result = [{k.lower(): v for k, v in row.items()} for row in result]
            cursor.close()
            self.logger.debug(f"MySQL query result: {normalized_result}")
            return normalized_result
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise

    async def close(self):      # Close connection
        if hasattr(self, "engine") and self.engine:
            self.engine.close()
            self.logger.info("MySQL connection closed")