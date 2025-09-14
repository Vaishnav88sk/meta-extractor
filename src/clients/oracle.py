import oracledb
from application_sdk.clients.sql import AsyncBaseSQLClient
from src.utils.logger import get_logger
from typing import Any, Dict

class OracleClient(AsyncBaseSQLClient):
    DB_CONFIG = {
        "template": "oracle://{user}:{password}@{host}:{port}/{service_name}",
        "required": ["user", "password", "host", "port", "service_name"],
    }

    def __init__(self):
        self.logger = get_logger(__name__)

    async def connect(self, credentials: Dict[str, Any]) -> None:
        # Connect synchronously but wrapped in async interface.
        try:
            dsn = f"{credentials['host']}:{credentials['port']}/{credentials['service_name']}"
            self.engine = oracledb.connect(
                user=credentials["user"],
                password=credentials["password"],
                dsn=dsn
            )
            self.logger.info("Successfully connected to Oracle database")
        except Exception as e:
            self.logger.error(f"Failed to connect to Oracle: {str(e)}")
            raise

    async def execute_query(self, query: str, *args) -> list:
        # Execute a query using synchronous cursor in thread pool to avoid blocking.
        try:
            cursor = self.engine.cursor()
            cursor.execute(query, args)
            columns = [col[0].lower() for col in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            self.logger.debug(f"Oracle query result: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise

    async def close(self):
        # Close connection synchronously but wrapped in async interface.
        if hasattr(self, "engine") and self.engine:
            self.engine.close()
            self.logger.info("Oracle connection closed")