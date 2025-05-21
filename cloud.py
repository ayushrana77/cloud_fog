"""
Cloud Node implementation for processing tasks.
"""
import random
import math
import asyncio
import time
from datetime import datetime
from config import (
    CLOUD_SERVICES_CONFIG, 
    EARTH_RADIUS_KM,
    TASK_MEMORY_MIN,
    TASK_MEMORY_MAX
)

class CloudNode:
    """Represents a cloud node for processing tasks."""
    
    def __init__(self, node_id):
        """Initialize a cloud node with configurations from config.py."""
        self.node_id = node_id
        config = CLOUD_SERVICES_CONFIG[node_id]
        self.name = config["name"]
        self.mips = config["mips"]
        self.bandwidth = config["bandwidth"]
        self.memory = config["memory"]
        self.location = config["location"]
        
        # Statistics tracking
        self.total_processed = 0
        self.total_processing_time = 0
        self.total_transmission_time = 0
        self.total_queue_time = 0  # Track total queue time
        self.used_memory = 0  # Track used memory
        self.used_mips = 0    # Track used MIPS
        self.used_bandwidth = 0  # Track used bandwidth
        self.current_tasks = []  # Track current tasks and their resource usage
        self.task_memory = {}  # Track memory usage per task
        self.task_queue_times = {}  # Track queue times per task
        
        # Initialize log file
        self.log_file = open(f'cloud_{self.name.lower()}.log', 'w', encoding='utf-8')
        self._write_log_header()
        
    def _write_log_header(self):
        """Write header to log file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"""
{'='*100}
Cloud Node Log - {self.name}
Started at: {timestamp}
Node Configuration:
    MIPS: {self.mips}
    Memory: {self.memory} MB
    Bandwidth: {self.bandwidth} Mbps
    Location: {self.location['lat']}, {self.location['lon']}
{'='*100}
"""
        self.log_file.write(header)
        self.log_file.flush()
        
    def _log_activity(self, message, task_id=None, task_info=None):
        """Log activity to file with detailed information."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Base log entry
        log_entry = f"\n[{timestamp}] {message}"
        
        # Add task details if provided
        if task_id and task_info:
            log_entry += f"\nTask Details:"
            log_entry += f"\n    Task ID: {task_id}"
            log_entry += f"\n    Processing Time: {task_info.get('processing_time', 0):.2f}ms"
            log_entry += f"\n    Queue Time: {task_info.get('queue_time', 0):.2f}ms"
            log_entry += f"\n    Memory Required: {task_info.get('memory', 0)}MB"
            log_entry += f"\n    MIPS Required: {task_info.get('mips', 0)}"
            log_entry += f"\n    Bandwidth Required: {task_info.get('bandwidth', 0)}Mbps"
        
        # Add current resource utilization
        active_tasks = len(self.current_tasks)
        total_memory_used = sum(task['memory'] for task in self.current_tasks)
        total_mips_used = sum(task['mips'] for task in self.current_tasks)
        total_bandwidth_used = sum(task['bandwidth'] for task in self.current_tasks)
        
        log_entry += f"\nResource Utilization:"
        log_entry += f"\n    Active Tasks: {active_tasks}"
        log_entry += f"\n    Memory Used: {total_memory_used}MB/{self.memory}MB ({total_memory_used/self.memory*100:.1f}%)"
        log_entry += f"\n    MIPS Used: {total_mips_used}/{self.mips} ({total_mips_used/self.mips*100:.1f}%)"
        log_entry += f"\n    Bandwidth Used: {total_bandwidth_used}Mbps/{self.bandwidth}Mbps ({total_bandwidth_used/self.bandwidth*100:.1f}%)"
        
        # Add performance metrics
        if self.total_processed > 0:
            avg_processing = self.total_processing_time / self.total_processed
            avg_transmission = self.total_transmission_time / self.total_processed
            avg_queue = self.total_queue_time / self.total_processed
            log_entry += f"\nPerformance Metrics:"
            log_entry += f"\n    Total Tasks Processed: {self.total_processed}"
            log_entry += f"\n    Average Processing Time: {avg_processing:.2f}ms"
            log_entry += f"\n    Average Transmission Time: {avg_transmission:.2f}ms"
            log_entry += f"\n    Average Queue Time: {avg_queue:.2f}ms"
        
        log_entry += "\n" + "-"*100
        self.log_file.write(log_entry)
        self.log_file.flush()
        
    def calculate_processing_time(self, task):
        """Calculate processing time based on task requirements."""
        # Get task requirements with defaults
        if isinstance(task, dict):
            task_size = task.get('Size', 1.0)
            ram_required = task.get('Memory', random.randint(TASK_MEMORY_MIN, TASK_MEMORY_MAX))
            bandwidth_required = task.get('Bandwidth', 100)
        else:
            task_size = getattr(task, 'size', 1.0)
            ram_required = getattr(task, 'ram_required', random.randint(TASK_MEMORY_MIN, TASK_MEMORY_MAX))
            bandwidth_required = getattr(task, 'bandwidth_required', 100)
        
        # Base processing time in milliseconds
        base_time = 50  # 50ms base processing time
        
        # Calculate processing components
        cpu_time = (task_size * 1000) / self.mips  # Convert to milliseconds
        memory_time = (ram_required * 0.1) / (self.memory / 1000)  # Memory access time
        network_time = (task_size * 8) / (self.bandwidth * 0.8)  # Network processing time
        
        # Add node-specific variations
        if self.name == "Cloud-NA":
            # USA node has slightly better processing for North American tasks
            cpu_factor = 0.9
        else:
            # Asia node has slightly better processing for Asian tasks
            cpu_factor = 1.1
        
        # Calculate total processing time with variations
        total_time = (base_time + cpu_time + memory_time + network_time) * cpu_factor
        
        # Add random variation
        total_time *= random.uniform(0.9, 1.1)
        
        self._log_activity(f"Calculated processing time: {total_time:.2f}ms for task size {task_size}MB")
        return total_time, ram_required, bandwidth_required
    
    def calculate_distance(self, source_location):
        """Calculate distance between source and cloud in kilometers using Haversine formula."""
        if not source_location:
            return 1000  # Default distance if location not provided
        
        if 'lat' not in source_location or 'lon' not in source_location:
            return 1000  # Default distance if location not provided
        
        lat1 = math.radians(source_location['lat'])
        lon1 = math.radians(source_location['lon'])
        lat2 = math.radians(self.location['lat'])
        lon2 = math.radians(self.location['lon'])
        
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        distance = EARTH_RADIUS_KM * c
        return distance
    
    def calculate_transmission_delay(self, distance_km, task_size):
        """Calculate transmission delay based on distance and task size."""
        # Base network overhead in milliseconds
        base_overhead = 20
        
        # Minimum theoretical delay based on speed of light
        min_delay = distance_km / 300  # Light travels ~300,000 km/s
        
        # Distance-based network delay
        distance_delay = distance_km / 50  # 1ms per 50km
        
        # Size-based delay
        size_delay = task_size / 10  # 1ms per 10 units of size
        
        # Node-specific network conditions
        if self.name == "Cloud-NA":
            # USA node has better connectivity in North America
            if distance_km < 5000:  # Within North America
                network_factor = 0.8
            else:
                network_factor = 1.2
        else:
            # Asia node has better connectivity in Asia
            if distance_km < 5000:  # Within Asia
                network_factor = 0.8
            else:
                network_factor = 1.2
        
        # Calculate final transmission delay
        transmission_delay = (base_overhead + min_delay + distance_delay + size_delay) * network_factor
        
        # Add random variation
        transmission_delay *= random.uniform(0.9, 1.1)
        
        return transmission_delay

    async def process_task(self, task):
        """Process a task and return the processing time."""
        # Get task location
        if isinstance(task, dict):
            # Handle dictionary input (JSON format)
            if 'GeoLocation' in task:
                source_location = {
                    'lat': task['GeoLocation']['latitude'],
                    'lon': task['GeoLocation']['longitude']
                }
            else:
                source_location = None
        else:
            source_location = getattr(task, 'source_location', None) or getattr(task, 'location', None)
        
        # Calculate processing time and resource requirements
        processing_time, ram_required, bandwidth_required = self.calculate_processing_time(task)
        
        # Calculate MIPS required based on processing time
        mips_required = (processing_time * self.mips) / 1000  # Convert to MIPS
        
        # Generate task ID
        task_id = id(task)
        
        # Record queue start time
        queue_start_time = time.time()
        
        # Log task start with detailed information
        task_info = {
            'processing_time': processing_time,
            'memory': ram_required,
            'mips': mips_required,
            'bandwidth': bandwidth_required
        }
        self._log_activity(f"Starting task processing", task_id, task_info)
        
        # Check if we have enough resources
        if (self.used_memory + ram_required > self.memory or 
            self.used_mips + mips_required > self.mips or 
            self.used_bandwidth + bandwidth_required > self.bandwidth):
            self._log_activity(f"Waiting for resources", task_id, task_info)
            # If not enough resources, wait for some tasks to complete
            await self._wait_for_resources(ram_required, mips_required, bandwidth_required)
        
        # Calculate queue time
        queue_time = (time.time() - queue_start_time) * 1000  # Convert to milliseconds
        self.total_queue_time += queue_time
        self.task_queue_times[task_id] = queue_time
        
        # Allocate resources
        self.used_memory += ram_required
        self.used_mips += mips_required
        self.used_bandwidth += bandwidth_required
        self.task_memory[task_id] = ram_required
        self.current_tasks.append({
            'task_id': task_id,
            'task': task,
            'memory': ram_required,
            'mips': mips_required,
            'bandwidth': bandwidth_required,
            'processing_time': processing_time,
            'queue_time': queue_time,
            'start_time': time.time()
        })
        
        # Calculate distance and transmission delay
        distance_km = self.calculate_distance(source_location)
        task_size = getattr(task, 'size', 1.0) if not isinstance(task, dict) else task.get('Size', 1.0)
        transmission_delay = self.calculate_transmission_delay(distance_km, task_size)
        
        # Total time includes transmission, queue, and processing
        total_time = transmission_delay + queue_time + processing_time
        
        # Update statistics
        self.total_processed += 1
        self.total_processing_time += processing_time
        self.total_transmission_time += transmission_delay
        
        # Create a task for processing
        processing_task = asyncio.create_task(self._process_task_async(task_id, processing_time))
        
        # Wait for processing to complete
        await processing_task
        
        # Log task completion with detailed information
        self._log_activity(f"Task completed", task_id, {
            'processing_time': processing_time,
            'transmission_time': transmission_delay,
            'queue_time': queue_time,
            'total_time': total_time,
            'distance_km': distance_km,
            'memory': ram_required,
            'mips': mips_required,
            'bandwidth': bandwidth_required
        })
        
        return {
            'total_time': total_time,
            'processing_time': processing_time,
            'transmission_time': transmission_delay,
            'queue_time': queue_time,
            'distance_km': distance_km,
            'memory_used': ram_required,
            'bandwidth_used': bandwidth_required,
            'mips_used': mips_required,
            'ram_required': ram_required,
            'task_size': task_size
        }
    
    async def _process_task_async(self, task_id, processing_time):
        """Process a task asynchronously."""
        try:
            # Log task start
            self._log_activity(f"Task {task_id} started parallel processing. "
                             f"Expected time: {processing_time:.2f}ms")
            
            # Simulate task processing time
            await asyncio.sleep(processing_time / 1000)  # Convert to seconds
            
            # Release resources after processing
            if task_id in self.task_memory:
                self.used_memory -= self.task_memory[task_id]
                self.used_mips -= self.current_tasks[0]['mips']  # Get MIPS from current task
                self.used_bandwidth -= self.current_tasks[0]['bandwidth']  # Get bandwidth from current task
                del self.task_memory[task_id]
                self.current_tasks = [t for t in self.current_tasks if t['task_id'] != task_id]
            
            # Log task completion with parallel processing info
            active_tasks = len(self.current_tasks)
            total_memory_used = sum(task['memory'] for task in self.current_tasks)
            total_mips_used = sum(task['mips'] for task in self.current_tasks)
            total_bandwidth_used = sum(task['bandwidth'] for task in self.current_tasks)
            
            self._log_activity(f"Task {task_id} completed parallel processing. "
                             f"Processing time: {processing_time:.2f}ms. "
                             f"Remaining active tasks: {active_tasks}, "
                             f"Total Memory Used: {total_memory_used}MB/{self.memory}MB, "
                             f"Total MIPS Used: {total_mips_used}/{self.mips}, "
                             f"Total Bandwidth Used: {total_bandwidth_used}Mbps/{self.bandwidth}Mbps")
        except Exception as e:
            self._log_activity(f"Error in parallel processing of task {task_id}: {str(e)}")
            # Release resources in case of error
            if task_id in self.task_memory:
                self.used_memory -= self.task_memory[task_id]
                self.used_mips -= self.current_tasks[0]['mips']
                self.used_bandwidth -= self.current_tasks[0]['bandwidth']
                del self.task_memory[task_id]
                self.current_tasks = [t for t in self.current_tasks if t['task_id'] != task_id]
    
    async def _wait_for_resources(self, required_memory, required_mips, required_bandwidth):
        """Wait for enough resources to become available."""
        while (self.used_memory + required_memory > self.memory or 
               self.used_mips + required_mips > self.mips or 
               self.used_bandwidth + required_bandwidth > self.bandwidth):
            # Process some tasks to free up resources
            completed_tasks = []
            current_time = time.time()
            
            for task_info in self.current_tasks:
                elapsed_time = (current_time - task_info['start_time']) * 1000  # Convert to milliseconds
                if elapsed_time >= task_info['processing_time']:
                    completed_tasks.append(task_info)
                    task_id = task_info['task_id']
                    if task_id in self.task_memory:
                        self.used_memory -= self.task_memory[task_id]
                        self.used_mips -= task_info['mips']
                        self.used_bandwidth -= task_info['bandwidth']
                        del self.task_memory[task_id]
            
            # Remove completed tasks
            for task_info in completed_tasks:
                self.current_tasks.remove(task_info)
            
            # Log waiting status
            active_tasks = len(self.current_tasks)
            total_memory_used = sum(task['memory'] for task in self.current_tasks)
            total_mips_used = sum(task['mips'] for task in self.current_tasks)
            total_bandwidth_used = sum(task['bandwidth'] for task in self.current_tasks)
            
            self._log_activity(f"Waiting for resources. Active tasks: {active_tasks}, "
                             f"Total Memory Used: {total_memory_used}MB/{self.memory}MB, "
                             f"Total MIPS Used: {total_mips_used}/{self.mips}, "
                             f"Total Bandwidth Used: {total_bandwidth_used}Mbps/{self.bandwidth}Mbps")
            
            # Add a small delay to prevent busy waiting
            await asyncio.sleep(0.001)
    
    def get_stats(self):
        """Return statistics about this cloud node."""
        avg_processing_time = 0
        avg_transmission_time = 0
        avg_queue_time = 0
        
        if self.total_processed > 0:
            avg_processing_time = self.total_processing_time / self.total_processed
            avg_transmission_time = self.total_transmission_time / self.total_processed
            avg_queue_time = self.total_queue_time / self.total_processed
            
        # Calculate current resource utilization based on active tasks
        total_memory_used = sum(task['memory'] for task in self.current_tasks)
        total_mips_used = sum(task['mips'] for task in self.current_tasks)
        total_bandwidth_used = sum(task['bandwidth'] for task in self.current_tasks)
        
        memory_utilization = total_memory_used / self.memory if self.memory > 0 else 0
        mips_utilization = total_mips_used / self.mips if self.mips > 0 else 0
        bandwidth_utilization = total_bandwidth_used / self.bandwidth if self.bandwidth > 0 else 0
        
        return {
            "node_id": self.node_id,
            "name": self.name,
            "total_processed": self.total_processed,
            "avg_processing_time": avg_processing_time,
            "avg_transmission_time": avg_transmission_time,
            "avg_queue_time": avg_queue_time,
            "memory_utilization": memory_utilization,
            "mips_utilization": mips_utilization,
            "bandwidth_utilization": bandwidth_utilization,
            "current_tasks": len(self.current_tasks),
            "queue_size": len(self.current_tasks),
            "system_load": max(memory_utilization, mips_utilization, bandwidth_utilization),
            "used_memory": total_memory_used,
            "used_mips": total_mips_used,
            "used_bandwidth": total_bandwidth_used,
            "available_memory": self.memory - total_memory_used,
            "available_mips": self.mips - total_mips_used,
            "available_bandwidth": self.bandwidth - total_bandwidth_used
        }
        
    def __del__(self):
        """Cleanup when object is destroyed."""
        if hasattr(self, 'log_file'):
            self.log_file.close() 