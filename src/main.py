# from flask import Flask
# from src.utils.logger import get_logger
# from src.routes.database import register_database_routes
# from src.routes.excel import register_excel_route

# app = Flask(__name__)
# logger = get_logger(__name__)

# # Register routes
# register_database_routes(app)
# register_excel_route(app)

# if __name__ == "__main__":
#     logger.info("Starting metadata extractor application")
#     app.run(host="0.0.0.0", port=5000)

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from flask import Flask
from src.utils.logger import get_logger
from src.routes.database import register_database_routes
from src.routes.excel import register_excel_route
app = Flask(__name__)
logger = get_logger(__name__)
# Register routes
register_database_routes(app)
register_excel_route(app)
if __name__ == "__main__":
    logger.info("Starting metadata extractor application")
    app.run(host="0.0.0.0", port=5000)