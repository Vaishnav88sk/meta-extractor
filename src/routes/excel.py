from flask import jsonify, request
from src.transformers.atlas import GenericAtlasTransformer
from src.utils.logger import get_logger
import pandas as pd
import os

logger = get_logger(__name__)

def fetch_excel_metadata(file_path: str, connection_qualified_name: str) -> list:
    transformer = GenericAtlasTransformer(connector_name="excel")
    result = []

    try:
        xl = pd.ExcelFile(file_path)
        for sheet_name in xl.sheet_names:
            schema_data = {
                "schema_name": sheet_name,
                "database_name": os.path.basename(file_path),
                "connection_qualified_name": connection_qualified_name,
                "description": "",
                "tags": []
            }
            schema_entity = transformer.transform_row("SCHEMA", schema_data)
            if schema_entity:
                schema_entity["tables"] = []
                result.append(schema_entity)

                df = pd.read_excel(file_path, sheet_name=sheet_name)
                table_data = {
                    "table_name": sheet_name,
                    "schema_name": sheet_name,
                    "database_name": os.path.basename(file_path),
                    "connection_qualified_name": connection_qualified_name,
                    "description": "",
                    "tags": []
                }
                table_entity = transformer.transform_row("TABLE", table_data)
                if table_entity:
                    table_entity["columns"] = []
                    table_entity["constraints"] = []
                    schema_entity["tables"].append(table_entity)

                    for col_name, dtype in df.dtypes.items():
                        col_data = {
                            "column_name": str(col_name),
                            "schema_name": sheet_name,
                            "table_name": sheet_name,
                            "database_name": os.path.basename(file_path),
                            "connection_qualified_name": connection_qualified_name,
                            "data_type": str(dtype),
                            "is_nullable": True,
                            "ordinal_position": df.columns.get_loc(col_name) + 1,
                            "description": "",
                            "tags": [],
                            "constraint_type": ""
                        }
                        col_entity = transformer.transform_row("COLUMN", col_data)
                        if col_entity:
                            table_entity["columns"].append(col_entity)

        return result
    except Exception as e:
        logger.error(f"Excel metadata extraction failed: {str(e)}")
        raise

def register_excel_route(app):
    @app.route('/extract_excel', methods=['POST'])
    async def extract_excel():
        try:
            data = request.form.to_dict()
            connection_qualified_name = data.get("connection_qualified_name", "default/excel")
            file_path = None
            if 'file' in request.files and request.files['file'].filename:
                file = request.files['file']
                if not file.filename.endswith(('.xlsx', '.xls')):
                    return jsonify({"error": "Invalid file format, must be .xlsx or .xls"}), 400
                file_path = f"/tmp/{file.filename}"
                file.save(file_path)
                logger.info(f"Processing uploaded Excel file: {file_path}")
            else:
                file_path = os.getenv("EXCEL_FILE_PATH")
                if not file_path:
                    return jsonify({"error": "No file uploaded and EXCEL_FILE_PATH not set in .env"}), 400
                if not os.path.exists(file_path):
                    return jsonify({"error": f"File not found at {file_path}"}), 400
                if not file_path.endswith(('.xlsx', '.xls')):
                    return jsonify({"error": "Invalid file format in EXCEL_FILE_PATH, must be .xlsx or .xls"}), 400
                logger.info(f"Processing Excel file from .env: {file_path}")

            metadata = fetch_excel_metadata(file_path, connection_qualified_name)
            if 'file' in request.files and request.files['file'].filename and os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted temporary file: {file_path}")
            return jsonify({"schemas": metadata}), 200
        except Exception as e:
            logger.error(f"Error in extract_excel: {str(e)}")
            return jsonify({"error": str(e)}), 500