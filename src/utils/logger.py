# from application_sdk.observability.logger_adaptor import get_logger

# # Wrapper to ensure consistent logger usage
# def get_logger(name: str):
#     return get_logger(name)

from application_sdk.observability.logger_adaptor import get_logger as sdk_get_logger

def get_logger(name: str):
    return sdk_get_logger(name)