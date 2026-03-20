import logging
import os

def setup_logger(log_file):
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(log_file)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not logger.handlers:
        file_handler = logging.FileHandler(f"logs/{log_file}.log")
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger