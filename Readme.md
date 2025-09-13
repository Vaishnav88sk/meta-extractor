# Metadata Extractor

<!-- A Flask-based application to extract metadata from PostgreSQL, MySQL, Oracle databases, and Excel files using the `atlan-application-sdk`. The application exposes REST API endpoints to retrieve metadata in a structured format (schemas → tables → columns/constraints) with descriptions and tags, using credentials from a `.env` file. -->

## Project Structure

```
metadata-extractor/
├── src/
│   ├── clients/
│   │   ├── __init__.py
│   │   ├── postgres.py
│   │   ├── mysql.py
│   │   ├── oracle.py
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── atlas.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── logger.py
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── database.py
│   │   ├── excel.py
│   ├── main.py
├── .env
├── .gitignore
├── requirements.txt
├── README.md
```

## Prerequisites

- Python 3.8+
- Virtual environment (recommended)
- PostgreSQL, MySQL, and Oracle servers (if extracting database metadata)
- Oracle client libraries (for Oracle support)
- Excel file (`.xlsx` or `.xls`) at the path specified in `.env`

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Vaishnav88sk/metadata-extractor.git
   cd metadata-extractor/
   ```

2. **Set Up Virtual Environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   If `atlan-application-sdk` fails to install, try:
   ```bash
   pip install --pre atlan-application-sdk
   ```
   For Oracle, ensure client libraries are installed: [python-oracledb installation guide](https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html).

4. **Configure Environment**:
   Create a `.env` file in the project root with the following template:
   ```plaintext
   # PostgreSQL
   PG_USER=your_postgres_username
   PG_PASSWORD=your_postgres_password
   PG_HOST=localhost
   PG_PORT=5432
   PG_DATABASE=your_postgres_database

   # MySQL
   MYSQL_USER=your_mysql_username
   MYSQL_PASSWORD=your_mysql_password
   MYSQL_HOST=localhost
   MYSQL_PORT=3306
   MYSQL_DATABASE=your_mysql_database

   # Oracle
   ORACLE_USER=your_oracle_username
   ORACLE_PASSWORD=your_oracle_password
   ORACLE_HOST=localhost
   ORACLE_PORT=1521
   ORACLE_SERVICE=your_oracle_service_name

   # Excel
   EXCEL_FILE_PATH=./files/sample.xlsx
   ```
   - Replace placeholders with your database credentials.
   - Ensure the Excel file (e.g., `/files/sample.xlsx`) exists and is readable.

## Running the Application

1. **Start the Flask Server**:
   From the project root (`metadata-extractor`):
   ```bash
   python3 -m src.main
   ```
   Alternatively, if using the path-fixed `main.py`:
   ```bash
   python3 src/main.py
   ```
   The server runs on `http://127.0.0.1:5000`.

2. **API Endpoints**:
   - **PostgreSQL Metadata**:
     - **Endpoint**: `POST /extract_postgres`
     - **Body (JSON)**: `{"connection_qualified_name": "default/postgresql"}`
   - **MySQL Metadata**:
     - **Endpoint**: `POST /extract_mysql`
     - **Body (JSON)**: `{"connection_qualified_name": "default/mysql"}`
   - **Oracle Metadata**:
     - **Endpoint**: `POST /extract_oracle`
     - **Body (JSON)**: `{"connection_qualified_name": "default/oracle"}`
   - **Excel Metadata**:
     - **Endpoint**: `POST /extract_excel`
     - **Body**:
       - **Option 1 (File Upload)**: Form-data with `file` (upload `.xlsx` or `.xls`) and optional `connection_qualified_name`.
       - **Option 2 (Use .env Path)**: JSON with `{"connection_qualified_name": "default/excel"}` to use `EXCEL_FILE_PATH`.
   - **Response Format**:
     ```json
     {
         "schemas": [
             {
                 "typeName": "SCHEMA",
                 "attributes": {
                     "name": "<schema_name>",
                     "qualifiedName": "<connection_qualified_name>/<schema_name>",
                     "connectionQualifiedName": "<connection_qualified_name>",
                     "databaseName": "<database_name>"
                 },
                 "customAttributes": {
                     "description": "",
                     "tags": []
                 },
                 "status": "ACTIVE",
                 "tables": [
                     {
                         "typeName": "TABLE",
                         "attributes": { ... },
                         "customAttributes": { ... },
                         "status": "ACTIVE",
                         "columns": [ ... ],
                         "constraints": [ ... ]
                     }
                 ]
             }
         ]
     }
     ```

3. **Testing with Postman**:
   - Use Postman to send POST requests to the endpoints above.
   - For `/extract_excel`, test both file upload (form-data) and `.env` path (JSON body).
   - Verify the response contains the expected metadata structure.

## Troubleshooting

- **ModuleNotFoundError**:
  - Ensure you’re in the project root and run `python3 -m src.main`.
  - Verify all `__init__.py` files exist in `src/clients`, `src/transformers`, `src/utils`, and `src/routes`.
- **Dependency Issues**:
  - Confirm all packages are installed (`pip list`).
  - For `atlan-application-sdk`, use `--pre` if needed.
- **Database Errors**:
  - Ensure database servers are running and credentials in `.env` are correct.
  - Check Oracle client library setup.
- **Excel Errors**:
  - Verify `/files/sample.xlsx` exists and is readable (`ls -l /files/sample.xlsx`).
  - Ensure uploaded files are valid `.xlsx` or `.xls`.
- **Logs**:
  - Check logs for errors (`logger.error` in `src/utils/logger.py`).

<!-- ## Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/<feature-name>`).
3. Commit changes (`git commit -m "Add feature"`).
4. Push to the branch (`git push origin feature/<feature-name>`).
5. Open a pull request. -->

## License

This project is licensed under the MIT License.