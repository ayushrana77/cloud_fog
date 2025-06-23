"""
Fog Computing Node Implementation with Queue

This module implements a fog computing node that can process tasks, manage resources,
and handle task queuing. It includes functionality for task assignment, processing,
and resource management with thread-safe operations.
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
from queue import Queue
from collections import deque
from logger import setup_logger

class FogNode:
    """
    Represents a fog computing node with processing capabilities and resource management.
    
    Attributes:
        name (str): Unique identifier for the fog node
        mips (float): Million Instructions Per Second - processing power
        bandwidth (float): Network bandwidth in Mbps
        memory (float): Available memory in MB
        storage (float): Available storage in GB
        location (dict): Geographic location with lat/lon coordinates
        current_load (float): Current load percentage
        available_mips (float): Available processing power
        available_bandwidth (float): Available network bandwidth
        available_memory (float): Available memory
        available_storage (float): Available storage
        assigned_tasks (list): Currently assigned tasks
        processing_times (list): Historical processing times
        transmission_times (list): Historical transmission times
        completed_tasks (list): Successfully completed tasks
        task_queue (deque): Queue for waiting tasks
        processing (bool): Processing status flag
        lock (threading.Lock): Thread lock for resource management
        completion_callbacks (list): Callbacks for task completion events
        logger (logging.Logger): Node-specific logger
    """
    
    def __init__(self, name, mips, bandwidth, memory, storage, location):
        """
        Initialize a fog computing node with specified resources.
        
        Args:
            name (str): Unique identifier for the fog node
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
        self.location = validate_location(location)
        self.current_load = 0
        self.available_mips = mips
        self.available_bandwidth = bandwidth
        self.available_memory = memory
        self.available_storage = storage
        self.assigned_tasks = []
        self.processing_times = []
        self.transmission_times = []
        self.completed_tasks = []
        self.task_queue = deque()
        self.processing = False
        self.lock = threading.Lock()
        self.completion_callbacks = []
        self.logger = setup_logger(f'fog_{name}', f'fog_{name}.log', sub_directory='fog')
        
        self.logger.info(f"Initialized Fog Node: {name}")
        self.logger.info(f"Resources: MIPS={mips}, Memory={memory}, Bandwidth={bandwidth}, Storage={storage}")
        self.logger.info(f"Location: {self.location}")

    def add_completion_callback(self, callback):
        """Add a callback function to be called when a task is completed"""
        self.completion_callbacks.append(callback)

    def _notify_completion(self, task_info):
        """Notify all registered callbacks about task completion"""
        for callback in self.completion_callbacks:
            try:
                callback(self.name, task_info)
            except Exception as e:
                self.logger.error(f"Error in completion callback: {str(e)}")

    def can_handle_task(self, task):
        """
        Check if the fog node can handle the given task.
        Fog nodes have lower resource thresholds compared to cloud nodes.
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
        Assign a task to this fog node or queue it if resources are not available.
        Fog nodes have lower bandwidth compared to cloud nodes.
        """
        with self.lock:
            self.logger.info(f"Attempting to assign task {task['Name']} to {self.name}")
            
            if 'transmission_time' not in task:
                transmission_time = calculate_transmission_time(
                    task['GeoLocation'],
                    self.location,
                    self,
                    task.get('Size'),
                    task.get('MIPS'),
                    self.logger
                )
                task['transmission_time'] = transmission_time
                self.transmission_times.append(transmission_time)
            
            if self.can_handle_task(task):
                processing_time = calculate_processing_time(task['Size'], self.mips)
                self.processing_times.append(processing_time)
                
                task_info = {
                    'task': task,
                    'processing_time': processing_time,
                    'transmission_time': task['transmission_time'],
                    'start_time': time.time(),
                    'queue_time': 0
                }
                self.assigned_tasks.append(task_info)
                
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
                
                processing_thread = threading.Thread(
                    target=self._process_task,
                    args=(task_info,)
                )
                processing_thread.start()
                return True, processing_time
            else:
                self.logger.warning(f"Insufficient resources on {self.name} for task {task['Name']}")
                self.logger.info(f"Queueing task {task['Name']} on {self.name}")
                task['queue_entry_time'] = time.time()
                self.task_queue.append(task)
                self.logger.info(f"Current queue size: {len(self.task_queue)}")
                return False, 0

    def _process_task(self, task_info):
        """Process a task and release resources after completion"""
        task = task_info['task']
        processing_time = task_info['processing_time']
        
        self.logger.info(f"Starting processing of task {task['Name']} on {self.name}")
        self.logger.info(f"Expected processing time: {processing_time:.2f} seconds")
        
        time.sleep(processing_time)
        
        with self.lock:
            self.available_mips += task['MIPS']
            self.available_memory += task['RAM']
            self.available_bandwidth += task['BW']
            self.available_storage += task.get('Storage', 0)
            
            self.current_load = (1 - (self.available_mips / self.mips)) * 100
            
            # Calculate power consumption for this task
            transmission_time = task_info['transmission_time']
            queue_time = task_info.get('queue_time', 0)
            load_factor = 1.0 - (self.available_mips / self.mips)  # Current load factor
            power_info = calculate_power_consumption(
                transmission_time, 
                processing_time, 
                queue_time,
                'fog', 
                load_factor
            )
            
            self.assigned_tasks.remove(task_info)
            completion_info = {
                'task': task,
                'processing_time': processing_time,
                'transmission_time': task_info['transmission_time'],
                'completion_time': time.time(),
                'queue_time': task_info['queue_time'],
                'fog_node': self.name,
                'total_time': processing_time + task_info['transmission_time'] + task_info['queue_time'],
                'power_consumption': power_info  # Add power consumption information
            }
            self.completed_tasks.append(completion_info)
            
            self.logger.info(f"Task {task['Name']} completed on {self.name}")
            self.logger.info(f"Resources released: MIPS={task['MIPS']}, Memory={task['RAM']}, Bandwidth={task['BW']}, Storage={task.get('Storage', 0)}")
            self.logger.info(f"Current load: {self.current_load:.2f}%")
            self.logger.info(f"Completed tasks: {len(self.completed_tasks)}")
            self.logger.info(f"Power consumption: {power_info['total_energy_wh']:.6f} Wh (Avg: {power_info['avg_power_watts']:.2f} W)")
            
            self._notify_completion(completion_info)
            self._process_queued_tasks()

    def _process_queued_tasks(self):
        """Process any queued tasks that can now be handled"""
        self.logger.info(f"Checking queued tasks on {self.name}")
        self.logger.info(f"Current queue size: {len(self.task_queue)}")
        
        while self.task_queue:
            next_task = self.task_queue[0]
            if self.can_handle_task(next_task):
                self.task_queue.popleft()
                self.logger.info(f"Processing queued task {next_task['Name']} on {self.name}")
                
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
                
                processing_time = calculate_processing_time(next_task['Size'], self.mips)
                self.processing_times.append(processing_time)
                
                queue_time = time.time() - next_task['queue_entry_time']
                
                # Calculate power consumption for this queued task
                load_factor = 1.0 - (self.available_mips / self.mips)  # Current load factor
                power_info = calculate_power_consumption(
                    transmission_time, 
                    processing_time, 
                    queue_time,
                    'fog', 
                    load_factor
                )
                
                task_info = {
                    'task': next_task,
                    'processing_time': processing_time,
                    'transmission_time': transmission_time,
                    'start_time': time.time(),
                    'queue_time': queue_time,
                    'power_info': power_info  # Add power consumption information
                }
                self.assigned_tasks.append(task_info)
                
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
                
                processing_thread = threading.Thread(
                    target=self._process_task,
                    args=(task_info,)
                )
                processing_thread.start()
            else:
                self.logger.info(f"Next queued task {next_task['Name']} still cannot be processed")
                break

    def get_status(self):
        """Get current status of the fog node"""
        avg_processing_time = sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0
        avg_queue_time = sum(task['queue_time'] for task in self.completed_tasks) / len(self.completed_tasks) if self.completed_tasks else 0
        avg_transmission_time = sum(self.transmission_times) / len(self.transmission_times) if self.transmission_times else 0
        
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
            'total_energy_consumed_wh': f"{total_energy_wh:.6f}",
            'average_power_consumption_watts': f"{avg_power_watts:.2f}",
            'energy_per_task_wh': f"{total_energy_wh / len(self.completed_tasks):.6f}" if self.completed_tasks else "0.000000"
        }
        
        self.logger.debug(f"Status of {self.name}:")
        for key, value in status.items():
            self.logger.debug(f"  {key}: {value}")
            
        return status

def create_fog_nodes():
    """Create fog nodes from configuration"""
    fog_nodes = {}
    for node_config in FOG_NODES_CONFIG:
        node = FogNode(
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
fog_nodes = create_fog_nodes()

def get_fog_node(name):
    """Get a specific fog node by name"""
    return fog_nodes.get(name)

def get_all_fog_nodes():
    """Get all fog nodes"""
    return fog_nodes

def get_fog_node_status():
    """Get status of all fog nodes"""
    return {name: node.get_status() for name, node in fog_nodes.items()}

if __name__ == "__main__":
    # Test fog node creation and status
    print("Fog Nodes Status:")
    for name, status in get_fog_node_status().items():
        print(f"\n{name}:")
        for key, value in status.items():
            print(f"  {key}: {value}") 