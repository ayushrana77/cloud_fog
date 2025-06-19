"""
Single-Task Fog Computing Node Implementation

This module implements a fog computing node that processes exactly one task at a time.
It ensures thread safety and provides direct task processing without queuing.
"""

from config import FOG_NODES_CONFIG
from utility import (
    calculate_processing_time,
    calculate_transmission_time,
    validate_location,
    calculate_power_consumption
)
import time
import threading
from logger import setup_logger

class SingleTaskFogNode:
    """
    Represents a fog computing node that processes exactly one task at a time.
    
    Attributes:
        name (str): Unique identifier for the fog node
        mips (float): Million Instructions Per Second - processing power
        bandwidth (float): Network bandwidth in Mbps
        memory (float): Available memory in MB
        storage (float): Available storage in GB
        location (dict): Geographic location with lat/lon coordinates
        current_task (dict): Currently processing task
        is_processing (bool): Flag indicating if node is processing a task
        completion_callbacks (list): Callbacks for task completion events
        logger (logging.Logger): Node-specific logger
        lock (threading.Lock): Thread lock for resource management
    """
    def __init__(self, name, mips, bandwidth, memory, storage, location):
        self.name = name
        self.mips = mips
        self.bandwidth = bandwidth
        self.memory = memory
        self.storage = storage
        self.location = validate_location(location)
        self.current_task = None
        self.is_processing = False
        self.completion_callbacks = []
        self.lock = threading.Lock()
        self.logger = setup_logger(f'fog_single_{name}', f'fog_single_{name}.log', sub_directory='fog')
        
        self.logger.info(f"Initialized Single-Task Fog Node: {name}")
        self.logger.info(f"Resources: MIPS={mips}, Memory={memory}, Bandwidth={bandwidth}, Storage={storage}")
        self.logger.info(f"Location: {self.location}")

    def add_completion_callback(self, callback):
        """
        Add a callback function to be called when a task is completed.
        
        Args:
            callback (function): Function to be called with task completion info
        """
        self.completion_callbacks.append(callback)

    def _notify_completion(self, task_info):
        """
        Notify all registered callbacks about task completion.
        
        Args:
            task_info (dict): Information about the completed task
        """
        for callback in self.completion_callbacks:
            try:
                callback(self.name, task_info)
            except Exception as e:
                self.logger.error(f"Error in completion callback: {str(e)}")
    
    def can_handle_task(self, task):
        """
        Check if the fog node has sufficient resources to handle a task.
        
        Args:
            task (dict): Task information including resource requirements
            
        Returns:
            bool: True if node can handle the task, False otherwise
        """
        return (self.mips >= task['MIPS'] and
                self.memory >= task['RAM'] and
                self.bandwidth >= task['BW'] and
                self.storage >= task.get('Storage', 0))
                
    def assign_task(self, task):
        """
        Process a task directly if node is available, otherwise reject.
        
        Args:
            task (dict): Task information including resource requirements
            
        Returns:
            tuple: (bool, float) - (success status, processing time)
        """
        with self.lock:
            # Check if node is busy
            if self.is_processing:
                self.logger.warning(f"Node {self.name} is busy, cannot accept task {task['Name']}")
                return False, 0
                
            # Check if node has sufficient resources
            if not self.can_handle_task(task):
                self.logger.warning(f"Insufficient resources on {self.name} for task {task['Name']}")
                return False, 0

            # Calculate transmission time
            transmission_time = calculate_transmission_time(
                task['GeoLocation'],
                self.location,
                self,
                task.get('Size'),
                task.get('MIPS'),
                self.logger
            )
            task['transmission_time'] = transmission_time

            # Calculate processing time
            processing_time = calculate_processing_time(task['Size'], self.mips)
            
            # Mark node as busy
            self.is_processing = True
            self.current_task = task
            
            self.logger.info(f"Task {task['Name']} assigned to {self.name}")
            self.logger.info(f"Expected processing time: {processing_time:.2f} seconds")
            self.logger.info(f"Transmission time: {transmission_time:.2f} seconds")
            
            # Start a new thread to process this task
            process_thread = threading.Thread(
                target=self._process_task,
                args=(task, processing_time, transmission_time),
                daemon=True
            )
            process_thread.start()
            
            return True, processing_time
            
    def _process_task(self, task, processing_time, transmission_time):
        """
        Process a task and notify completion when done.
        
        Args:
            task (dict): The task to process
            processing_time (float): Time required to process the task
            transmission_time (float): Time required to transmit task data
        """
        try:
            self.logger.info(f"Starting processing of task {task['Name']} on {self.name}")
            
            # Simulate task processing
            time.sleep(processing_time)
            
            # Calculate total time
            completion_time = time.time()
            
            # Calculate power consumption for this task
            load_factor = 1.0  # Single-task mode, always at full load when processing
            power_info = calculate_power_consumption(
                transmission_time, 
                processing_time, 
                'fog', 
                load_factor
            )
            
            # Prepare completion info
            completion_info = {
                'task': task,
                'processing_time': processing_time,
                'transmission_time': transmission_time,
                'queue_time': 0,  # No queue in single-task mode
                'completion_time': completion_time,
                'total_time': processing_time + transmission_time,
                'power_consumption': power_info  # Add power consumption information
            }
            
            # Notify completion
            self._notify_completion(completion_info)
            
            with self.lock:
                self.is_processing = False
                self.current_task = None
            
            self.logger.info(f"Task {task['Name']} completed on {self.name}")
            self.logger.info(f"Total processing time: {processing_time:.2f} seconds")
            self.logger.info(f"Power consumption: {power_info['total_energy_wh']:.6f} Wh (Avg: {power_info['avg_power_watts']:.2f} W)")
            
        except Exception as e:
            self.logger.error(f"Error processing task: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            with self.lock:
                self.is_processing = False
                self.current_task = None
                
    def get_status(self):
        """
        Get current status of the fog node.
        
        Returns:
            dict: Current status information about the single-task fog node
        """
        with self.lock:
            status = {
                'name': self.name,
                'is_processing': self.is_processing,
                'current_task': self.current_task['Name'] if self.current_task else None,
                'mips': self.mips,
                'memory': self.memory,
                'bandwidth': self.bandwidth,
                'storage': self.storage
            }
        
        self.logger.debug(f"Status of {self.name}:")
        for key, value in status.items():
            self.logger.debug(f"  {key}: {value}")
            
        return status

def create_single_task_fog_nodes():
    """
    Create single-task fog nodes from configuration.
    
    Returns:
        dict: Dictionary of fog nodes with names as keys
    """
    fog_nodes = {}
    for node_config in FOG_NODES_CONFIG:
        node = SingleTaskFogNode(
            name=node_config['name'],
            mips=node_config['mips'],
            bandwidth=node_config['bandwidth'],
            memory=node_config['memory'],
            storage=node_config['storage'],
            location=node_config['location']
        )
        fog_nodes[node_config['name']] = node
    return fog_nodes

# Create fog nodes when module is imported
fog_nodes = create_single_task_fog_nodes()

def get_fog_node(name):
    """
    Get a specific fog node by name.
    
    Args:
        name (str): Name of the fog node
        
    Returns:
        SingleTaskFogNode: The requested fog node or None if not found
    """
    return fog_nodes.get(name)

def get_all_fog_nodes():
    """
    Get all fog nodes.
    
    Returns:
        dict: Dictionary of all fog nodes
    """
    return fog_nodes

def get_fog_node_status():
    """
    Get status of all fog nodes.
    
    Returns:
        dict: Dictionary of fog node statuses
    """
    return {name: node.get_status() for name, node in fog_nodes.items()}

if __name__ == "__main__":
    # Test fog node creation and status
    print("Single-Task Fog Nodes Status:")
    for name, status in get_fog_node_status().items():
        print(f"\n{name}:")
        for key, value in status.items():
            print(f"  {key}: {value}") 