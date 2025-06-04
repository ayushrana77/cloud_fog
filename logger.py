"""
Logging Configuration Module

This module provides logging functionality for the fog computing system.
It includes functions for setting up loggers and logging task/tuple details.
"""

import logging
from datetime import datetime

def setup_logger(name='tuple_logger', log_file='task_load.log', level=logging.INFO):
    """
    Configure and return a logger instance that writes to the specified log file.
    
    Args:
        name (str): Name of the logger instance
        log_file (str): Path to the log file
        level (int): Logging level (default: logging.INFO)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding multiple handlers to the logger
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def log_tuple_details(logger, tuple_data):
    """
    Log detailed information about a single tuple/task.
    
    Args:
        logger (logging.Logger): Logger instance to use
        tuple_data (dict): Dictionary containing tuple/task information
            Required keys: Name, ID, Size, MIPS, RAM, BW, DataType, DeviceType, CreationTime
    """
    logger.info(f"\nTuple Details:")
    logger.info(f"Name: {tuple_data['Name']}")
    logger.info(f"ID: {tuple_data['ID']}")
    logger.info(f"Size: {tuple_data['Size']}")
    logger.info(f"MIPS: {tuple_data['MIPS']}")
    logger.info(f"RAM: {tuple_data['RAM']}")
    logger.info(f"BW: {tuple_data['BW']}")
    logger.info(f"DataType: {tuple_data['DataType']}")
    logger.info(f"DeviceType: {tuple_data['DeviceType']}")
    logger.info(f"CreationTime: {tuple_data['CreationTime']}")
    logger.info("-" * 50) 