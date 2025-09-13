from flask import jsonify, request
from src.clients.postgres import PostgresClient
from src.clients.mysql import MySQLClient
from src.clients.oracle import OracleClient
from src.transformers.atlas import GenericAtlasTransformer
from src.utils.config import get_db_credentials
from src.utils.logger import get_logger
import asyncio
from typing import Any, Dict, List

logger = get_logger(__name__)

QUERIES = {
    "schema": {
        "postgresql": """
            SELECT s.schema_name, obj_description(n.oid, 'pg_namespace') AS description
            FROM information_schema.schemata s
            JOIN pg_namespace n ON s.schema_name = n.nspname
            WHERE s.schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        """,
        "mysql": """
            SELECT SCHEMA_NAME AS schema_name, NULL AS description
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        """,
        "oracle": """
            SELECT username AS schema_name, NULL AS description
            FROM all_users
            WHERE username NOT IN ('SYS', 'SYSTEM', 'PUBLIC')
        """
    },
    "table": {
        "postgresql": """
            SELECT t.table_name, obj_description(to_regclass(t.table_schema || '.' || t.table_name)::oid, 'pg_class') AS description
            FROM information_schema.tables t
            WHERE t.table_schema = $1
        """,
        "mysql": """
            SELECT TABLE_NAME AS table_name, TABLE_COMMENT AS description
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
        """,
        "oracle": """
            SELECT table_name, comments AS description
            FROM all_tab_comments
            WHERE owner = %s AND table_type = 'TABLE'
        """
    },
    "column": {
        "postgresql": """
            SELECT c.column_name, c.data_type, c.is_nullable, c.ordinal_position,
                   col_description(to_regclass(c.table_schema || '.' || c.table_name)::oid, c.ordinal_position) AS description
            FROM information_schema.columns c
            WHERE c.table_schema = $1 AND c.table_name = $2
        """,
        "mysql": """
            SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, IS_NULLABLE AS is_nullable, ORDINAL_POSITION AS ordinal_position, COLUMN_COMMENT AS description
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """,
        "oracle": """
            SELECT column_name, data_type, nullable AS is_nullable, column_id AS ordinal_position, comments AS description
            FROM all_col_comments
            WHERE owner = %s AND table_name = %s
        """
    },
    "constraint": {
        "postgresql": """
            SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = $1 AND tc.table_name = $2
        """,
        "mysql": """
            SELECT tc.CONSTRAINT_NAME AS constraint_name, tc.CONSTRAINT_TYPE AS constraint_type, kcu.COLUMN_NAME AS column_name
            FROM information_schema.TABLE_CONSTRAINTS tc
            LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = %s AND tc.TABLE_NAME = %s
        """,
        "oracle": """
            SELECT c.constraint_name, c.constraint_type, cc.column_name
            FROM all_constraints c
            LEFT JOIN all_cons_columns cc
            ON c.constraint_name = cc.constraint_name
            AND c.owner = cc.owner
            WHERE c.owner = %s AND c.table_name = %s
        """
    }
}

async def fetch_db_metadata(client, credentials: Dict[str, Any], connection_qualified_name: str, db_type: str) -> List[Dict[str, Any]]:
    await client.connect(credentials)
    transformer = GenericAtlasTransformer(connector_name=db_type.lower())
    result = []

    try:
        schemas = await client.execute_query(QUERIES["schema"][db_type])
        if not schemas:
            logger.warning(f"No schemas found for {db_type}")
            return result

        for schema_row in schemas:
            schema_data = {
                "schema_name": schema_row["schema_name"],
                "database_name": credentials.get("database", credentials.get("service_name", "")),
                "connection_qualified_name": connection_qualified_name,
                "description": schema_row["description"] or "",
                "tags": []
            }
            schema_entity = transformer.transform_row("SCHEMA", schema_data)
            if schema_entity:
                schema_entity["tables"] = []
                result.append(schema_entity)

                tables = await client.execute_query(QUERIES["table"][db_type], schema_row["schema_name"])
                for table_row in tables:
                    table_data = {
                        "table_name": table_row["table_name"],
                        "schema_name": schema_row["schema_name"],
                        "database_name": credentials.get("database", credentials.get("service_name", "")),
                        "connection_qualified_name": connection_qualified_name,
                        "description": table_row["description"] or "",
                        "tags": []
                    }
                    table_entity = transformer.transform_row("TABLE", table_data)
                    if table_entity:
                        table_entity["columns"] = []
                        table_entity["constraints"] = []
                        schema_entity["tables"].append(table_entity)

                        columns = await client.execute_query(QUERIES["column"][db_type], schema_row["schema_name"], table_row["table_name"])
                        constraints = await client.execute_query(QUERIES["constraint"][db_type], schema_row["schema_name"], table_row["table_name"])

                        for col_row in columns:
                            col_constraints = [cons["constraint_type"] for cons in constraints if cons["column_name"] == col_row["column_name"]]
                            constraint_type = col_constraints[0] if col_constraints else ""
                            col_data = {
                                "column_name": col_row["column_name"],
                                "schema_name": schema_row["schema_name"],
                                "table_name": table_row["table_name"],
                                "database_name": credentials.get("database", credentials.get("service_name", "")),
                                "connection_qualified_name": connection_qualified_name,
                                "data_type": col_row["data_type"],
                                "is_nullable": col_row["is_nullable"],
                                "ordinal_position": col_row["ordinal_position"],
                                "description": col_row["description"] or "",
                                "tags": [],
                                "constraint_type": constraint_type
                            }
                            col_entity = transformer.transform_row("COLUMN", col_data)
                            if col_entity:
                                table_entity["columns"].append(col_entity)

                        for cons_row in constraints:
                            table_entity["constraints"].append({
                                "constraint_name": cons_row["constraint_name"],
                                "constraint_type": cons_row["constraint_type"]
                            })

        return result
    except Exception as e:
        logger.error(f"Query execution failed for {db_type}: {str(e)}")
        raise
    finally:
        await client.close()

def register_database_routes(app):
    @app.route('/extract_postgres', methods=['POST'])
    async def extract_postgres():
        try:
            data = request.get_json() or {}
            credentials = get_db_credentials("PG")
            connection_qualified_name = data.get("connection_qualified_name", "default/postgresql")
            metadata = await fetch_db_metadata(PostgresClient(), credentials, connection_qualified_name, "postgresql")
            return jsonify({"schemas": metadata}), 200
        except Exception as e:
            logger.error(f"Error in extract_postgres: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route('/extract_mysql', methods=['POST'])
    async def extract_mysql():
        try:
            data = request.get_json() or {}
            credentials = get_db_credentials("MYSQL")
            connection_qualified_name = data.get("connection_qualified_name", "default/mysql")
            metadata = await fetch_db_metadata(MySQLClient(), credentials, connection_qualified_name, "mysql")
            return jsonify({"schemas": metadata}), 200
        except Exception as e:
            logger.error(f"Error in extract_mysql: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route('/extract_oracle', methods=['POST'])
    async def extract_oracle():
        try:
            data = request.get_json() or {}
            credentials = get_db_credentials("ORACLE")
            connection_qualified_name = data.get("connection_qualified_name", "default/oracle")
            metadata = await fetch_db_metadata(OracleClient(), credentials, connection_qualified_name, "oracle")
            return jsonify({"schemas": metadata}), 200
        except Exception as e:
            logger.error(f"Error in extract_oracle: {str(e)}")
            return jsonify({"error": str(e)}), 500