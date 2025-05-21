"""
Cloud and Fog Task Scheduler implementation for selecting optimal server.
"""
import math
import time
from datetime import datetime
from collections import deque
from cloud import CloudNode
from fog import FogNode
from config import CLOUD_SERVICES_CONFIG, FOG_NODES_CONFIG, EARTH_RADIUS_KM

class Scheduler:
    """Scheduler for selecting optimal server (cloud or fog) based on location and load."""
    
    def __init__(self, mode='cloud'):
        """Initialize the scheduler with cloud or fog nodes."""
        self.mode = mode
        if mode == 'cloud':
            self.nodes = [CloudNode(i) for i in range(len(CLOUD_SERVICES_CONFIG))]
        else:  # fog mode
            self.nodes = [FogNode(i) for i in range(len(FOG_NODES_CONFIG))]
            
        self.current_time = 0.0
        self.task_queues = {node.name: deque() for node in self.nodes}
        self.task_times = {}  # Track timing information for each task
        self.next_task_id = 1
        self.processed_tasks = {node.name: 0 for node in self.nodes}  # Track processed tasks per node
        
        # Track historical metrics for each node
        self.node_metrics = {
            node.name: {
                'processing_times': [],
                'transmission_times': [],
                'queue_times': [],
                'total_times': []
            } for node in self.nodes
        }
        
        # Initialize log file
        self.log_file = open(f'scheduler_{mode}.log', 'w', encoding='utf-8')
        self._write_log_header()
        
    def _write_log_header(self):
        """Write header to log file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"""
{'='*80}
Scheduler Log - {self.mode.upper()} Mode
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
        
    def calculate_distance(self, loc1, loc2):
        """Calculate distance between two locations using Haversine formula."""
        if not loc1 or not loc2 or 'lat' not in loc1 or 'lon' not in loc1 or 'lat' not in loc2 or 'lon' not in loc2:
            return float('inf')
            
        lat1 = math.radians(loc1['lat'])
        lon1 = math.radians(loc1['lon'])
        lat2 = math.radians(loc2['lat'])
        lon2 = math.radians(loc2['lon'])
        
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        distance = EARTH_RADIUS_KM * c
        self._log_activity(f"Calculated distance: {distance:.2f}km between locations")
        return distance

    def calculate_geographic_latency(self, distance_km, node):
        """Calculate geographic latency based on distance and node's bandwidth."""
        # Speed of light in fiber optic cable (approximately 200,000 km/s)
        speed_of_light = 200000  # km/s
        base_latency = (distance_km / speed_of_light) * 1000  # Convert to milliseconds
        
        # Adjust latency based on node's bandwidth
        bandwidth_factor = 10000 / node.bandwidth  # Normalize against 10Gbps
        return base_latency * bandwidth_factor

    def calculate_queue_delay(self, node):
        """Calculate queue delay based on actual queue size and historical processing times."""
        queue_size = len(self.task_queues[node.name])
        if not self.node_metrics[node.name]['processing_times']:
            return 0
            
        # Use average of last 100 processing times or all available if less than 100
        recent_times = self.node_metrics[node.name]['processing_times'][-100:]
        avg_processing_time = sum(recent_times) / len(recent_times)
        
        return queue_size * avg_processing_time

    def calculate_system_load(self, node):
        """Calculate system load based on actual metrics."""
        if len(node.current_tasks) == 0:
            return 0
            
        metrics = self.node_metrics[node.name]
        if not metrics['total_times']:
            return 0
            
        # Calculate load based on recent processing times and queue size
        recent_times = metrics['total_times'][-100:] if len(metrics['total_times']) > 100 else metrics['total_times']
        avg_processing_time = sum(recent_times) / len(recent_times)
        queue_size = len(self.task_queues[node.name])
        
        # Get memory utilization from node stats
        node_stats = node.get_stats()
        memory_utilization = node_stats.get('memory_utilization', 0)
        
        # Calculate CPU load
        cpu_load = (queue_size * avg_processing_time) / (node.mips * 1000)  # Convert MIPS to instructions per millisecond
        
        # Return the maximum of CPU and memory utilization
        return max(cpu_load, memory_utilization)

    def calculate_bandwidth_utilization(self, node):
        """Calculate bandwidth utilization based on transmission times."""
        if not self.node_metrics[node.name]['transmission_times'] or len(node.current_tasks) == 0:
            return 0
        
        # Use average of last 100 transmission times or all available if less than 100
        recent_times = self.node_metrics[node.name]['transmission_times'][-100:]
        avg_transmission = sum(recent_times) / len(recent_times)
        
        # Calculate utilization as ratio of transmission time to bandwidth capacity
        return avg_transmission / (node.bandwidth / 1000)  # Convert bandwidth to Mbps

    def calculate_mips_utilization(self, node):
        """Calculate MIPS utilization based on processing times."""
        if not self.node_metrics[node.name]['processing_times'] or len(node.current_tasks) == 0:
            return 0
        
        # Use average of last 100 processing times or all available if less than 100
        recent_times = self.node_metrics[node.name]['processing_times'][-100:]
        avg_processing = sum(recent_times) / len(recent_times)
        
        # Calculate utilization as ratio of processing time to MIPS capacity
        return avg_processing / (node.mips / 10000)  # Convert MIPS to instructions per millisecond

    def select_node(self, task):
        """Select the optimal node for a task based on geographic proximity and load."""
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
        
        if not source_location:
            # If no location, use the least loaded node
            selected_node = min(self.nodes, key=lambda x: len(self.task_queues[x.name]))
            self._log_activity(f"No location provided. Selected least loaded node: {selected_node.name}")
            return selected_node
        
        # Calculate distances to all nodes
        node_distances = []
        for node in self.nodes:
            distance = self.calculate_distance(source_location, node.location)
            node_distances.append((node, distance))
        
        # Sort nodes by distance
        node_distances.sort(key=lambda x: x[1])
        
        # Get the two closest nodes
        closest_nodes = node_distances[:2]
        
        # Calculate scores for the closest nodes
        node_scores = []
        for node, distance in closest_nodes:
            # Calculate geographic latency
            geo_latency = self.calculate_geographic_latency(distance, node)
            
            # Calculate queue delay
            queue_delay = self.calculate_queue_delay(node)
            
            # Calculate system load
            system_load = self.calculate_system_load(node)
            
            # Calculate bandwidth utilization
            bandwidth_utilization = self.calculate_bandwidth_utilization(node)
            
            # Calculate MIPS utilization
            mips_utilization = self.calculate_mips_utilization(node)
            
            # Calculate total score (lower is better)
            total_score = (
                geo_latency * 0.4 +      # Geographic latency weight
                queue_delay * 0.2 +      # Queue delay weight
                system_load * 0.2 +      # System load weight
                bandwidth_utilization * 0.1 +  # Bandwidth weight
                mips_utilization * 0.1   # MIPS weight
            )
            
            node_scores.append((node, total_score))
            self._log_activity(f"Node {node.name} score: {total_score:.2f} (Distance: {distance:.2f}km)")
        
        # Sort nodes by score
        node_scores.sort(key=lambda x: x[1])
        selected_node = node_scores[0][0]
        self._log_activity(f"Selected node: {selected_node.name} with score: {node_scores[0][1]:.2f}")
        return selected_node

    async def schedule_task(self, task):
        """Schedule a task to the optimal node."""
        # Generate task ID
        task_id = self.next_task_id
        self.next_task_id += 1
        
        self._log_activity(f"Scheduling task {task_id}")
        
        # Select the best node
        selected_node = self.select_node(task)
        
        # Record task start time
        start_time = time.time()
        
        # Add task to queue
        self.task_queues[selected_node.name].append((task_id, task, start_time))
        self._log_activity(f"Task {task_id} added to queue of {selected_node.name}")
        
        # Process the task
        result = await selected_node.process_task(task)
        
        # Remove task from queue and increment processed count
        self.task_queues[selected_node.name].remove((task_id, task, start_time))
        self.processed_tasks[selected_node.name] += 1
        
        # Calculate queue time
        queue_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        # Update node metrics
        self.node_metrics[selected_node.name]['processing_times'].append(result['processing_time'])
        self.node_metrics[selected_node.name]['transmission_times'].append(result.get('transmission_time', 0))
        self.node_metrics[selected_node.name]['queue_times'].append(queue_time)
        self.node_metrics[selected_node.name]['total_times'].append(result['total_time'])
        
        self._log_activity(f"Task {task_id} completed on {selected_node.name}. "
                          f"Total time: {result['total_time']:.2f}ms, "
                          f"Memory used: {result['memory_used']}MB, "
                          f"Bandwidth used: {result['bandwidth_used']}Mbps")
        
        return {
            'task_id': task_id,
            'node_name': selected_node.name,
            'distance_km': result.get('distance_km', 0),
            'transmission_time': result.get('transmission_time', 0),
            'processing_time': result['processing_time'],
            'queue_time': queue_time,
            'total_time': result.get('total_time', result['processing_time'] + queue_time),
            'queue_size': len(self.task_queues[selected_node.name]),
            'system_load': self.calculate_system_load(selected_node),
            'bandwidth_utilization': self.calculate_bandwidth_utilization(selected_node),
            'mips_utilization': self.calculate_mips_utilization(selected_node),
            'memory_used': result['memory_used'],
            'bandwidth_used': result['bandwidth_used'],
            'ram_required': result['ram_required'],
            'task_size': result['task_size']
        }

    def get_stats(self):
        """Get comprehensive statistics for all nodes."""
        stats = {}
        for node in self.nodes:
            metrics = self.node_metrics[node.name]
            node_stats = node.get_stats()
            
            # Calculate averages from actual metrics
            avg_processing = sum(metrics['processing_times']) / len(metrics['processing_times']) if metrics['processing_times'] else 0
            avg_transmission = sum(metrics['transmission_times']) / len(metrics['transmission_times']) if metrics['transmission_times'] else 0
            avg_queue = sum(metrics['queue_times']) / len(metrics['queue_times']) if metrics['queue_times'] else 0
            avg_total = sum(metrics['total_times']) / len(metrics['total_times']) if metrics['total_times'] else 0
            
            # Calculate memory utilization based on current tasks
            total_memory_used = sum(task['memory'] for task in node.current_tasks)
            memory_utilization = total_memory_used / node.memory if node.memory > 0 else 0
            
            # If no active tasks, all utilizations should be 0
            if len(node.current_tasks) == 0:
                mips_utilization = 0
                bandwidth_utilization = 0
                system_load = 0
            else:
                # Calculate utilizations only when there are active tasks
                mips_utilization = avg_processing / (node.mips / 10000) if avg_processing > 0 else 0
                bandwidth_utilization = avg_transmission / (node.bandwidth / 1000) if avg_transmission > 0 else 0
                system_load = self.calculate_system_load(node)
            
            # Update stats with calculated metrics
            node_stats.update({
                'queue_size': len(self.task_queues[node.name]),
                'total_processed': self.processed_tasks[node.name],
                'system_load': system_load,
                'avg_processing_time': avg_processing,
                'avg_transmission_time': avg_transmission,
                'avg_queue_time': avg_queue,
                'avg_total_time': avg_total,
                'bandwidth_utilization': bandwidth_utilization,
                'mips_utilization': mips_utilization,
                'memory_utilization': memory_utilization,
                'current_tasks': len(node.current_tasks)
            })
            
            stats[node.node_id] = node_stats
            self._log_activity(f"Stats for {node.name}: {node_stats}")
        
        return stats
        
    def __del__(self):
        """Cleanup when object is destroyed."""
        if hasattr(self, 'log_file'):
            self.log_file.close() 