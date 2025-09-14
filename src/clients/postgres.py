import asyncpg
from application_sdk.clients.sql import AsyncBaseSQLClient
from src.utils.logger import get_logger
from typing import Any, Dict

class PostgresClient(AsyncBaseSQLClient):
    DB_CONFIG = {
        "template": "postgresql://{user}:{password}@{host}:{port}/{database}",
        "required": ["user", "password", "host", "port", "database"],
    }

    def __init__(self):
        self.logger = get_logger(__name__)

    async def connect(self, credentials: Dict[str, Any]) -> None:
        """
        Establish a connection pool to PostgreSQL using credentials.
        """
        try:
            connection_string = self.DB_CONFIG["template"].format(**credentials)
            self.engine = await asyncpg.create_pool(connection_string)
            self.logger.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

    async def execute_query(self, query: str, *args) -> list:
        """
        Execute a query with optional arguments and return list of dict results.
        """
        async with self.engine.acquire() as connection:
            try:
                result = await connection.fetch(query, *args)
                self.logger.debug(f"PostgreSQL query result: {result}")
                return result
            except Exception as e:
                self.logger.error(f"Query execution failed: {str(e)}")
                raise

    async def close(self):
        if hasattr(self, "engine") and self.engine:
            await self.engine.close()
            self.logger.info("PostgreSQL connection closed")