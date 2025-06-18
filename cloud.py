"""
Cloud Node Implementation
This module implements the cloud computing node functionality for task processing.
Cloud nodes are high-performance computing resources that can handle large and bulk tasks.
"""

from config import CLOUD_SERVICES_CONFIG
from utility import (
    calculate_processing_time,
    calculate_transmission_time,
    validate_location,
    calculate_power_consumption
)
import time
import threading
from queue import Queue
from collections import deque
from logger import setup_logger

class CloudNode:
    """
    Cloud computing node class that handles task processing and resource management.
    Cloud nodes have higher resource capacity compared to fog nodes.
    """
    
    def __init__(self, name, mips, bandwidth, memory, storage, location):
        """
        Initialize a cloud computing node with specified resources.
        
        Args:
            name (str): Unique identifier for the cloud node
            mips (float): Million Instructions Per Second capacity
            bandwidth (float): Network bandwidth in Mbps
            memory (float): Available memory in MB
            storage (float): Available storage in GB
            location (dict): Geographic location with latitude and longitude
        """
        self.name = name
        self.mips = mips
        self.bandwidth = bandwidth
        self.memory = memory
        self.storage = storage
        self.location = validate_location(location)  # Validate location on initialization
        self.current_load = 0
        self.available_mips = mips
        self.available_bandwidth = bandwidth
        self.available_memory = memory
        self.available_storage = storage
        self.assigned_tasks = []
        self.processing_times = []
        self.transmission_times = []  # Track transmission times
        self.completed_tasks = []
        self.task_queue = deque()  # Queue for waiting tasks
        self.processing = False
        self.lock = threading.Lock()  # Lock for thread-safe resource management
        self.completion_callbacks = []  # List to store completion callbacks
        self.is_cloud = True  # Flag to identify this as a cloud node
        self.power_consumption_history = []  # Track power consumption history
        # Each cloud node has its own logger
        self.logger = setup_logger(f'cloud_{name}', f'cloud_{name}.log', sub_directory='cloud')
        
        self.logger.info(f"Initialized Cloud Node: {name}")
        self.logger.info(f"Resources: MIPS={mips}, Memory={memory}, Bandwidth={bandwidth}, Storage={storage}")
        self.logger.info(f"Location: {self.location}")

    def add_completion_callback(self, callback):
        """
        Add a callback function to be called when a task is completed
        
        Args:
            callback (function): Function to be called on task completion
        """
        self.completion_callbacks.append(callback)

    def _notify_completion(self, task_info):
        """
        Notify all registered callbacks about task completion
        
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
        Check if the cloud node can handle the given task.
        Cloud nodes typically have higher resource thresholds.
        
        Args:
            task (dict): Task information including resource requirements
            
        Returns:
            bool: True if the node can handle the task, False otherwise
        """
        can_handle = (self.available_mips >= task['MIPS'] and
                     self.available_memory >= task['RAM'] and
                     self.available_bandwidth >= task['BW'] and
                     self.available_storage >= task.get('Storage', 0))
        
        self.logger.info(f"Resource check for task {task['Name']} on {self.name}:")
        self.logger.info(f"  Required MIPS: {task['MIPS']}, Available: {self.available_mips}")
        self.logger.info(f"  Required Memory: {task['RAM']}, Available: {self.available_memory}")
        self.logger.info(f"  Required Bandwidth: {task['BW']}, Available: {self.available_bandwidth}")
        self.logger.info(f"  Required Storage: {task.get('Storage', 0)}, Available: {self.available_storage}")
        self.logger.info(f"  Can handle: {can_handle}")
        
        return can_handle

    def assign_task(self, task):
        """
        Assign a task to this cloud node or queue it if resources are not available.
        Cloud nodes typically have higher bandwidth for faster transmission.
        
        Args:
            task (dict): Task information including resource requirements
            
        Returns:
            tuple: (success, processing_time) where success is a boolean indicating
                  if the task was assigned, and processing_time is the estimated
                  processing time if successful
        """
        with self.lock:
            self.logger.info(f"Attempting to assign task {task['Name']} to {self.name}")
            
            # Calculate transmission time once when task is first received
            if 'transmission_time' not in task:
                transmission_time = calculate_transmission_time(
                    task['GeoLocation'],
                    self.location,
                    self,
                    task.get('Size'),  # Keep Size for backward compatibility
                    task.get('MIPS'),  # Add MIPS parameter
                    self.logger
                )
                task['transmission_time'] = transmission_time
                self.transmission_times.append(transmission_time)
            
            if self.can_handle_task(task):
                # Calculate processing time for this task
                processing_time = calculate_processing_time(task['Size'], self.mips)
                self.processing_times.append(processing_time)
                
                # Add task with its processing time and transmission time
                task_info = {
                    'task': task,
                    'processing_time': processing_time,
                    'transmission_time': task['transmission_time'],
                    'start_time': time.time(),
                    'queue_time': 0  # No queuing time since task is processed immediately
                }
                self.assigned_tasks.append(task_info)
                
                # Allocate resources
                self.available_mips -= task['MIPS']
                self.available_memory -= task['RAM']
                self.available_bandwidth -= task['BW']
                self.available_storage -= task.get('Storage', 0)
                self.current_load = (1 - (self.available_mips / self.mips)) * 100
                
                self.logger.info(f"Task {task['Name']} assigned to {self.name}")
                self.logger.info(f"Processing time: {processing_time:.2f} seconds")
                self.logger.info(f"Transmission time: {task['transmission_time']:.2f} seconds")
                self.logger.info(f"Resources allocated: MIPS={task['MIPS']}, Memory={task['RAM']}, Bandwidth={task['BW']}, Storage={task.get('Storage', 0)}")
                self.logger.info(f"Current load: {self.current_load:.2f}%")
                
                # Start task processing in a new thread
                processing_thread = threading.Thread(
                    target=self._process_task,
                    args=(task_info,)
                )
                processing_thread.start()
                return True, processing_time
            else:
                # Queue the task if resources are not available
                self.logger.warning(f"Insufficient resources on {self.name} for task {task['Name']}")
                self.logger.info(f"Queueing task {task['Name']} on {self.name}")
                # Add queue entry time to the task
                task['queue_entry_time'] = time.time()
                self.task_queue.append(task)
                self.logger.info(f"Current queue size: {len(self.task_queue)}")
                return False, 0

    def _process_task(self, task_info):
        """
        Process a task and release resources after completion.
        Cloud nodes typically have faster processing capabilities.
        
        Args:
            task_info (dict): Information about the task to be processed
        """
        task = task_info['task']
        processing_time = task_info['processing_time']
        
        self.logger.info(f"Starting processing of task {task['Name']} on {self.name}")
        self.logger.info(f"Expected processing time: {processing_time:.2f} seconds")
        
        # Simulate task processing
        time.sleep(processing_time)
        
        with self.lock:
            # Release resources
            self.available_mips += task['MIPS']
            self.available_memory += task['RAM']
            self.available_bandwidth += task['BW']
            self.available_storage += task.get('Storage', 0)
            
            # Update current load
            self.current_load = (1 - (self.available_mips / self.mips)) * 100
            
            # Calculate power consumption for this task
            transmission_time = task_info['transmission_time']
            load_factor = 1.0 - (self.available_mips / self.mips)  # Current load factor
            power_info = calculate_power_consumption(
                transmission_time, 
                processing_time, 
                'cloud', 
                load_factor
            )
            
            # Store power consumption in history
            self.power_consumption_history.append({
                'task_name': task['Name'],
                'timestamp': time.time(),
                'power_info': power_info
            })
            
            # Move task to completed list with all its information including power consumption
            self.assigned_tasks.remove(task_info)
            completion_info = {
                'task': task,
                'processing_time': processing_time,
                'transmission_time': task_info['transmission_time'],
                'completion_time': time.time(),
                'queue_time': task_info['queue_time'],
                'cloud_node': self.name,
                'total_time': processing_time + task_info['transmission_time'] + task_info['queue_time'],
                'power_consumption': power_info  # Add power consumption information
            }
            self.completed_tasks.append(completion_info)
            
            self.logger.info(f"Task {task['Name']} completed on {self.name}")
            self.logger.info(f"Resources released: MIPS={task['MIPS']}, Memory={task['RAM']}, Bandwidth={task['BW']}, Storage={task.get('Storage', 0)}")
            self.logger.info(f"Current load: {self.current_load:.2f}%")
            self.logger.info(f"Completed tasks: {len(self.completed_tasks)}")
            self.logger.info(f"Power consumption: {power_info['total_energy_wh']:.6f} Wh (Avg: {power_info['avg_power_watts']:.2f} W)")
            
            # Notify about task completion
            self._notify_completion(completion_info)
            
            # Check if any queued tasks can be processed
            self._process_queued_tasks()

    def _process_queued_tasks(self):
        """
        Process any queued tasks that can now be handled.
        Cloud nodes typically have better queue management.
        """
        self.logger.info(f"Checking queued tasks on {self.name}")
        self.logger.info(f"Current queue size: {len(self.task_queue)}")
        
        while self.task_queue:
            next_task = self.task_queue[0]  # Peek at the next task
            if self.can_handle_task(next_task):
                self.task_queue.popleft()  # Remove the task from queue
                self.logger.info(f"Processing queued task {next_task['Name']} on {self.name}")
                
                # Recalculate transmission time for queued task
                transmission_time = calculate_transmission_time(
                    next_task['GeoLocation'],
                    self.location,
                    self,
                    next_task.get('Size'),
                    next_task.get('MIPS'),
                    self.logger
                )
                next_task['transmission_time'] = transmission_time
                self.transmission_times.append(transmission_time)
                
                # Calculate processing time for this task
                processing_time = calculate_processing_time(next_task['Size'], self.mips)
                self.processing_times.append(processing_time)
                
                # Calculate queuing time
                queue_time = time.time() - next_task['queue_entry_time']
                
                # Calculate power consumption for this queued task
                load_factor = 1.0 - (self.available_mips / self.mips)  # Current load factor
                power_info = calculate_power_consumption(
                    transmission_time, 
                    processing_time, 
                    'cloud', 
                    load_factor
                )
                
                # Add task with its processing time, transmission time, queue time and power consumption
                task_info = {
                    'task': next_task,
                    'processing_time': processing_time,
                    'transmission_time': transmission_time,
                    'start_time': time.time(),
                    'queue_time': queue_time,
                    'power_info': power_info  # Add power consumption information
                }
                self.assigned_tasks.append(task_info)
                
                # Allocate resources
                self.available_mips -= next_task['MIPS']
                self.available_memory -= next_task['RAM']
                self.available_bandwidth -= next_task['BW']
                self.available_storage -= next_task.get('Storage', 0)
                self.current_load = (1 - (self.available_mips / self.mips)) * 100
                
                self.logger.info(f"Task {next_task['Name']} assigned to {self.name}")
                self.logger.info(f"Processing time: {processing_time:.2f} seconds")
                self.logger.info(f"Transmission time: {transmission_time:.2f} seconds")
                self.logger.info(f"Queue time: {queue_time:.2f} seconds")
                self.logger.info(f"Resources allocated: MIPS={next_task['MIPS']}, Memory={next_task['RAM']}, Bandwidth={next_task['BW']}, Storage={next_task.get('Storage', 0)}")
                self.logger.info(f"Current load: {self.current_load:.2f}%")
                self.logger.info(f"Power consumption: {power_info['total_energy_wh']:.6f} Wh (Avg: {power_info['avg_power_watts']:.2f} W)")
                
                # Start task processing in a new thread
                processing_thread = threading.Thread(
                    target=self._process_task,
                    args=(task_info,)
                )
                processing_thread.start()
            else:
                self.logger.info(f"Next queued task {next_task['Name']} still cannot be processed")
                break  # Stop if next task can't be processed

    def get_status(self):
        """
        Get current status of the cloud node.
        Includes resource utilization and performance metrics.
        
        Returns:
            dict: Current status information including resource usage and performance metrics
        """
        avg_processing_time = sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0
        avg_queue_time = sum(task['queue_time'] for task in self.completed_tasks) if self.completed_tasks else 0
        avg_transmission_time = sum(self.transmission_times) / len(self.transmission_times) if self.transmission_times else 0
        
        # Calculate used resources
        mips_used = self.mips - self.available_mips
        memory_used = self.memory - self.available_memory
        bandwidth_used = self.bandwidth - self.available_bandwidth
        storage_used = self.storage - self.available_storage
        
        # Calculate power consumption metrics
        total_energy_wh = 0
        avg_power_watts = 0
        if self.completed_tasks:
            total_energy_wh = sum(task.get('power_consumption', {}).get('total_energy_wh', 0) for task in self.completed_tasks)
            total_time = sum(task.get('power_consumption', {}).get('total_time', 0) for task in self.completed_tasks)
            avg_power_watts = (total_energy_wh * 3600) / total_time if total_time > 0 else 0
        
        # Calculate ready time based on assigned tasks
        ready_time = 0
        current_time = time.time()
        if self.assigned_tasks:
            latest_completion = max(
                task_info['start_time'] + task_info['processing_time']
                for task_info in self.assigned_tasks
            )
            ready_time = max(0, latest_completion - current_time)
            self.logger.info(f"Node {self.name} is busy with {len(self.assigned_tasks)} tasks")
            self.logger.info(f"Latest task completion time: {latest_completion}")
            self.logger.info(f"Current time: {current_time}")
            self.logger.info(f"Calculated ready time: {ready_time*1000:.2f}ms")
        else:
            self.logger.info(f"Node {self.name} is idle, ready time: 0ms")
        
        status = {
            'name': self.name,
            'current_load': f"{self.current_load:.2f}%",
            'available_mips': self.available_mips,
            'available_memory': self.available_memory,
            'available_bandwidth': self.available_bandwidth,
            'available_storage': self.available_storage,
            'mips_used': mips_used,
            'memory_used': memory_used,
            'bandwidth_used': bandwidth_used,
            'storage_used': storage_used,
            'assigned_tasks': len(self.assigned_tasks),
            'queued_tasks': len(self.task_queue),
            'completed_tasks': len(self.completed_tasks),
            'average_processing_time': f"{avg_processing_time:.2f} seconds",
            'average_queue_time': f"{avg_queue_time:.2f} seconds",
            'average_transmission_time': f"{avg_transmission_time:.2f} seconds",
            'ready_time': ready_time,  # Add ready time to status
            'total_energy_consumed_wh': f"{total_energy_wh:.6f}",
            'average_power_consumption_watts': f"{avg_power_watts:.2f}",
            'energy_per_task_wh': f"{total_energy_wh / len(self.completed_tasks):.6f}" if self.completed_tasks else "0.000000"
        }
        
        self.logger.debug(f"Status of {self.name}:")
        for key, value in status.items():
            self.logger.debug(f"  {key}: {value}")
            
        return status

def create_cloud_nodes():
    """
    Create cloud nodes from configuration
    
    Returns:
        dict: Dictionary mapping node names to CloudNode instances
    """
    cloud_nodes = {}
    for node_config in CLOUD_SERVICES_CONFIG:
        node = CloudNode(
            name=node_config['name'],
            mips=node_config['mips'],
            bandwidth=node_config['bandwidth'],
            memory=node_config['memory'],
            storage=node_config['storage'],
            location=node_config['location']
        )
        cloud_nodes[node_config['name']] = node
    return cloud_nodes

# Create cloud nodes when module is imported
cloud_nodes = create_cloud_nodes()

def get_cloud_node(name):
    """
    Get a specific cloud node by name
    
    Args:
        name (str): Name of the cloud node to retrieve
        
    Returns:
        CloudNode: The requested cloud node instance or None if not found
    """
    return cloud_nodes.get(name)

def get_all_cloud_nodes():
    """
    Get all cloud nodes
    
    Returns:
        dict: Dictionary of all cloud nodes
    """
    return cloud_nodes

def get_cloud_node_status():
    """
    Get status of all cloud nodes
    
    Returns:
        dict: Dictionary mapping node names to their status information
    """
    return {name: node.get_status() for name, node in cloud_nodes.items()}

if __name__ == "__main__":
    # Test cloud node creation and status
    print("Cloud Nodes Status:")
    for name, status in get_cloud_node_status().items():
        print(f"\n{name}:")
        for key, value in status.items():
            print(f"  {key}: {value}")
