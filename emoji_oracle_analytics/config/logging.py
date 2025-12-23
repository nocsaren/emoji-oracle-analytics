import logging
import os
from datetime import datetime

_LOGGING_INITIALIZED = False

def get_logger(name=__name__):
    global _LOGGING_INITIALIZED

    if not _LOGGING_INITIALIZED:
        # Ensure log directory exists
        log_dir = os.getenv("LOG_DIR", "./logs")
        os.makedirs(log_dir, exist_ok=True)

        # Timestamped log file
        log_file = os.path.join(log_dir, f"{datetime.now():%Y-%m-%d}.log")

        level_name = os.getenv("LOG_LEVEL", "INFO").upper().strip()
        level = getattr(logging, level_name, logging.INFO)

        # More actionable format: includes module+line
        log_format = (
            "%(asctime)-29s%(levelname)-10s%(name)-40s"
            "%(module)s:%(lineno)d %(message)s"
        )

        root_logger = logging.getLogger()
        if not root_logger.handlers:
            logging.basicConfig(
                level=level,
                format=log_format,
                handlers=[
                    logging.FileHandler(log_file, encoding="utf-8"),
                    logging.StreamHandler(),
                ],
            )
        else:
            root_logger.setLevel(level)

        _LOGGING_INITIALIZED = True

    return logging.getLogger(name)