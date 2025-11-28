import logging
import logging.config
from pythonjsonlogger import json

from workers.config.app_config import APP_LOG_LEVEL

# APP_LOG_LEVEL set in app config
LOG_LEVEL = APP_LOG_LEVEL

# JSON formatter for structured logs
class JsonFormatter(json.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        log_record["pathname"] = record.pathname
        log_record["lineno"] = record.lineno

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
        },
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "json",   # or "standard"
            "level": LOG_LEVEL,
            "stream": "ext://sys.stdout",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": LOG_LEVEL,
    },
}


def setup_logging():
    """Apply the logging configuration"""
    logging.config.dictConfig(LOGGING_CONFIG)