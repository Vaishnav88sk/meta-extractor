from application_sdk.transformers.atlas import AtlasTransformer
from src.utils.logger import get_logger
from typing import Any, Dict, Optional

logger = get_logger(__name__)

class GenericSchema:
    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        attributes = {
            "name": obj.get("schema_name", ""),
            "qualifiedName": f"{obj.get('connection_qualified_name', '')}/{obj.get('schema_name', '')}",
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
            "databaseName": obj.get("database_name", ""),
        }
        return {
            "attributes": attributes,
            "custom_attributes": {
                "description": obj.get("description", ""),
                "tags": obj.get("tags", [])
            }
        }

class GenericTable:
    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        attributes = {
            "name": obj.get("table_name", ""),
            "schemaName": obj.get("schema_name", ""),
            "databaseName": obj.get("database_name", ""),
            "qualifiedName": f"{obj.get('connection_qualified_name', '')}/{obj.get('schema_name', '')}/{obj.get('table_name', '')}",
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
        }
        return {
            "attributes": attributes,
            "custom_attributes": {
                "description": obj.get("description", ""),
                "tags": obj.get("tags", [])
            }
        }

class GenericColumn:
    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        attributes = {
            "name": obj.get("column_name", ""),
            "qualifiedName": f"{obj.get('connection_qualified_name', '')}/{obj.get('schema_name', '')}/{obj.get('table_name', '')}/{obj.get('column_name', '')}",
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
            "tableName": obj.get("table_name", ""),
            "schemaName": obj.get("schema_name", ""),
            "databaseName": obj.get("database_name", ""),
            "dataType": obj.get("data_type", ""),
            "isNullable": obj.get("is_nullable", "NO") == "YES",
            "order": obj.get("ordinal_position", 1),
        }
        return {
            "attributes": attributes,
            "custom_attributes": {
                "description": obj.get("description", ""),
                "tags": obj.get("tags", []),
                "constraint_type": obj.get("constraint_type", "")
            }
        }

class GenericAtlasTransformer(AtlasTransformer):
    def __init__(self, connector_name: str, tenant_id: str = "default", **kwargs: Any):
        super().__init__(connector_name, tenant_id, **kwargs)
        self.entity_class_definitions = {
            "SCHEMA": GenericSchema,
            "TABLE": GenericTable,
            "COLUMN": GenericColumn
        }

    def transform_row(self, typename: str, data: Dict[str, Any], workflow_id: str = "workflow_1", workflow_run_id: str = "run_1") -> Optional[Dict[str, Any]]:
        typename = typename.upper()
        creator = self.entity_class_definitions.get(typename)
        if creator:
            try:
                entity_attributes = creator.get_attributes(data)
                return {
                    "typeName": typename,
                    "attributes": entity_attributes["attributes"],
                    "customAttributes": entity_attributes["custom_attributes"],
                    "status": "ACTIVE",
                }
            except Exception as e:
                logger.error(f"Error transforming {typename} entity: {str(e)}")
                return None
        logger.error(f"Unknown typename: {typename}")
        return None