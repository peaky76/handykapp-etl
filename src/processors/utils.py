import os

import psutil
from prefect import get_run_logger


def log_memory_usage():
    logger = get_run_logger()
    process = psutil.Process(os.getpid())
    logger.info(f"Memory usage: {process.memory_info().rss / 1024**2:.2f} MB")
