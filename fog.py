"""
Fog Node implementation for processing time calculation with queuing.
"""
import random
from collections import deque
from datetime import datetime
from config import (
    PROCESSING_VARIATION_MIN,
    PROCESSING_VARIATION_MAX,
    FOG_NODES_CONFIG
)
import time

class FogNode:
    """Represents a fog node for processing time calculation with queuing."""
    
    def __init__(self, node_id):
        """Initialize a fog node with configurations from config.py."""
        self.node_id = node_id
        
        # Get node configuration from config.py
        config = FOG_NODES_CONFIG[node_id]
        self.name = config["name"]
        self.mips = config["mips"]
        self.bandwidth = config["bandwidth"]
        self.memory = config["memory"]
        
        # Resource tracking
        self.available_mips = self.mips
        self.available_memory = self.memory
        self.available_bandwidth = self.bandwidth
        
        # Task queue
        self.task_queue = deque()
        self.current_tasks = []
        
        # Initialize log file
        self.log_file = open(f'fog_{self.name.lower()}.log', 'w', encoding='utf-8')
        self._write_log_header()
        
    def _write_log_header(self):
        """Write header to log file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"""
{'='*80}
Fog Node Log - {self.name}
Started at: {timestamp}
{'='*80}
"""
        self.log_file.write(header)
        self.log_file.flush()
        
    def _log_activity(self, message):
        """Log activity to file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"\n[{timestamp}] {message}"
        self.log_file.write(log_entry)
        self.log_file.flush()

    def calculate_processing_time(self, task):
        """Calculate processing time based on task requirements."""
        # Get task requirements with defaults
        task_size = getattr(task, 'size', 1.0)
        ram_required = getattr(task, 'ram_required', 512)
        bandwidth_required = getattr(task, 'bandwidth_required', 100)
        
        # Calculate processing components
        cpu_time = (task_size * 1000) / self.mips
        memory_time = cpu_time * (ram_required / self.memory)
        network_time = (task_size * 8) / (self.bandwidth * 0.8)
        
        # Total time with variation
        total_time = (cpu_time + memory_time + network_time) * \
                    random.uniform(PROCESSING_VARIATION_MIN, PROCESSING_VARIATION_MAX)
        
        self._log_activity(f"Calculated processing time: {total_time:.2f}ms for task size {task_size}MB")
        return total_time, ram_required, bandwidth_required

    def can_process_task(self, ram_required, mips_required, bandwidth_required):
        """Check if node has enough resources to process the task."""
        can_process = (self.available_memory >= ram_required and 
                      self.available_mips >= mips_required and
                      self.available_bandwidth >= bandwidth_required)
        self._log_activity(f"Resource check - Memory: {ram_required}MB/{self.available_memory}MB, "
                          f"MIPS: {mips_required}/{self.available_mips}, "
                          f"BW: {bandwidth_required}Mbps/{self.available_bandwidth}Mbps - "
                          f"Can process: {can_process}")
        return can_process

    def allocate_resources(self, ram_required, mips_required, bandwidth_required):
        """Allocate resources for task processing."""
        self.available_memory -= ram_required
        self.available_mips -= mips_required
        self.available_bandwidth -= bandwidth_required
        self._log_activity(f"Resources allocated - Memory: {ram_required}MB, "
                          f"MIPS: {mips_required}, BW: {bandwidth_required}Mbps")

    def release_resources(self, ram_required, mips_required, bandwidth_required):
        """Release resources after task completion."""
        self.available_memory += ram_required
        self.available_mips += mips_required
        self.available_bandwidth += bandwidth_required
        self._log_activity(f"Resources released - Memory: {ram_required}MB, "
                          f"MIPS: {mips_required}, BW: {bandwidth_required}Mbps")
    
    def process_task(self, task):
        """Process a task and return the processing time."""
        # Calculate processing time and resource requirements
        processing_time, ram_required, bandwidth_required = self.calculate_processing_time(task)
        
        # Calculate MIPS required based on processing time
        mips_required = (processing_time * self.mips) / 1000  # Convert to MIPS
        
        # Check if resources are available
        if self.can_process_task(ram_required, mips_required, bandwidth_required):
            # Allocate resources
            self.allocate_resources(ram_required, mips_required, bandwidth_required)
            
            # Add to current tasks
            self.current_tasks.append({
                'task': task,
                'processing_time': processing_time,
                'ram_required': ram_required,
                'mips_required': mips_required,
                'bandwidth_required': bandwidth_required,
                'task_size': getattr(task, 'size', 1.0) if not isinstance(task, dict) else task.get('Size', 1.0),
                'start_time': time.time()
            })
            
            self._log_activity(f"Task started processing. Resources allocated: "
                             f"Memory={ram_required}MB, MIPS={mips_required}, BW={bandwidth_required}Mbps")
            return {
                'processing_time': processing_time,
                'memory_used': ram_required,
                'bandwidth_used': bandwidth_required,
                'mips_used': mips_required,
                'ram_required': ram_required,
                'task_size': getattr(task, 'size', 1.0) if not isinstance(task, dict) else task.get('Size', 1.0)
            }
        else:
            # Add to queue if resources are not available
            self.task_queue.append({
                'task': task,
                'ram_required': ram_required,
                'mips_required': mips_required,
                'bandwidth_required': bandwidth_required,
                'task_size': getattr(task, 'size', 1.0) if not isinstance(task, dict) else task.get('Size', 1.0)
            })
            self._log_activity(f"Task queued. Queue size: {len(self.task_queue)}")
            return None

    def update(self, elapsed_time):
        """Update node state and process queued tasks."""
        # Process current tasks
        completed_tasks = []
        current_time = time.time()
        
        for task_info in self.current_tasks:
            task_elapsed_time = (current_time - task_info['start_time']) * 1000  # Convert to milliseconds
            if task_elapsed_time >= task_info['processing_time']:
                completed_tasks.append(task_info)
                self.release_resources(
                    task_info['ram_required'],
                    task_info['mips_required'],
                    task_info['bandwidth_required']
                )
                self._log_activity(f"Task completed. Processing time: {task_elapsed_time:.2f}ms")
        
        # Remove completed tasks
        for task_info in completed_tasks:
            self.current_tasks.remove(task_info)
        
        # Try to process queued tasks
        while self.task_queue:
            queued_task = self.task_queue[0]
            if self.can_process_task(
                queued_task['ram_required'],
                queued_task['mips_required'],
                queued_task['bandwidth_required']
            ):
                # Process the task
                self.task_queue.popleft()
                processing_time = self.process_task(queued_task['task'])
                if processing_time is not None:
                    self._log_activity(f"Queued task started processing. Expected time: {processing_time['processing_time']:.2f}ms")
                    return processing_time
            else:
                break
        
        return None
        
    def __del__(self):
        """Cleanup when object is destroyed."""
        if hasattr(self, 'log_file'):
            self.log_file.close()

    def get_stats(self):
        """Return statistics about this fog node."""
        # Calculate current resource utilization based on active tasks
        total_memory_used = sum(task['ram_required'] for task in self.current_tasks)
        total_mips_used = sum(task['mips_required'] for task in self.current_tasks)
        total_bandwidth_used = sum(task['bandwidth_required'] for task in self.current_tasks)
        
        memory_utilization = total_memory_used / self.memory if self.memory > 0 else 0
        mips_utilization = total_mips_used / self.mips if self.mips > 0 else 0
        bandwidth_utilization = total_bandwidth_used / self.bandwidth if self.bandwidth > 0 else 0
        
        return {
            "node_id": self.node_id,
            "name": self.name,
            "total_processed": len(self.current_tasks),
            "avg_processing_time": 0,  # Will be calculated by scheduler
            "avg_transmission_time": 0,  # Will be calculated by scheduler
            "memory_utilization": memory_utilization,
            "mips_utilization": mips_utilization,
            "bandwidth_utilization": bandwidth_utilization,
            "current_tasks": len(self.current_tasks),
            "queue_size": len(self.task_queue),
            "system_load": max(memory_utilization, mips_utilization, bandwidth_utilization),
            "used_memory": total_memory_used,
            "used_mips": total_mips_used,
            "used_bandwidth": total_bandwidth_used,
            "available_memory": self.memory - total_memory_used,
            "available_mips": self.mips - total_mips_used,
            "available_bandwidth": self.bandwidth - total_bandwidth_used
        }
