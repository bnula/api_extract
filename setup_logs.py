import logging
import os
import time

day_stamp = time.strftime("%Y-%m-%d")
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")


def setup_logger(name, path, level=logging.INFO):
    os.makedirs(path, exist_ok=True)
    
    handler = logging.FileHandler(f"{path}/{day_stamp} - {name}.txt", encoding="utf-8")
    handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    
    return logger
