"""
Logger Configuration Module

This module provides logging functionality for the fog computing system.
Logs are organized in subfolders based on their type:
- algorithms/: For MCT, FCFS, FCFSC algorithm logs
- nodes/: For fog and cloud node logs (not directly implemented here, but implied by structure)
- tasks/: For task-related logs (if implemented)
"""

import logging
import os
from datetime import datetime

def setup_logger(name, log_file, sub_directory=None):
    """
    Set up a logger with the specified name and log file.
    Logs are stored in appropriate subfolders with timestamp-based filenames.
    
    Args:
        name (str): Name of the logger
        log_file (str): Name of the log file
        sub_directory (str, optional): Subdirectory within 'logs' to store the log file.
                                      e.g., 'cloud', 'fog', 'algorithms'.
                                      If None, logs directly into 'logs/'.
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create base logs directory if it doesn't exist
    base_logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    if not os.path.exists(base_logs_dir):
        os.makedirs(base_logs_dir)
    
    # Determine the target directory for the log file
    if sub_directory:
        target_logs_dir = os.path.join(base_logs_dir, sub_directory)
    else:
        target_logs_dir = base_logs_dir

    # Create the target directory if it doesn't exist
    if not os.path.exists(target_logs_dir):
        os.makedirs(target_logs_dir)
    
    # Create a timestamp for the log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"{os.path.splitext(log_file)[0]}_{timestamp}.log"
    log_path = os.path.join(target_logs_dir, log_filename)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Create file handler with UTF-8 encoding
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # Create console handler with UTF-8 encoding
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger (avoid duplicate handlers)
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
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