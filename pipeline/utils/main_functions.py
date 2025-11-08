import os
from config.logging import get_logger

logger = get_logger(__name__)
def ensure_directories(directories):
    """Ensure that the specified directories exist."""
    try:
        for d in directories:
            os.makedirs(d, exist_ok=True)
        logger.info("Folder structure validated.")
    except Exception as e:
        logger.error(f"Error creating directories: {e}")
        exit(1)

