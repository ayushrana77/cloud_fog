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
    
    def __init__(self, mode='hybrid'):
        """Initialize the scheduler with cloud and/or fog nodes."""
        self.mode = mode
        self.nodes = []
        
        # Initialize nodes based on mode
        if mode in ['cloud', 'hybrid']:
            self.nodes.extend([CloudNode(i) for i in range(len(CLOUD_SERVICES_CONFIG))])
        if mode in ['fog', 'hybrid']:
            self.nodes.extend([FogNode(i) for i in range(len(FOG_NODES_CONFIG))])
            
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
                'total_times': [],
                'response_times': [],  # Added response times tracking
                'power_consumption': [],  # Track power consumption history
                'energy_consumption': 0.0  # Track total energy consumption
            } for node in self.nodes
        }
        
        # Initialize log files
        self.log_file = open(f'scheduler_{mode}.log', 'w', encoding='utf-8')
        self.system_log_file = open(f'system_{mode}.log', 'w', encoding='utf-8')
        self._write_log_header()
        
    def _write_log_header(self):
        """Write header to log files."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"""
{'='*100}
Scheduler Log - {self.mode.upper()} Mode
Started at: {timestamp}
Configuration:
    Number of Nodes: {len(self.nodes)}
    Node Types: {', '.join(node.name for node in self.nodes)}
    Cloud Nodes: {sum(1 for node in self.nodes if isinstance(node, CloudNode))}
    Fog Nodes: {sum(1 for node in self.nodes if isinstance(node, FogNode))}
{'='*100}
"""
        self.log_file.write(header)
        self.system_log_file.write(header)
        self.log_file.flush()
        self.system_log_file.flush()

    def _log_system_status(self):
        """Log system-wide status to the system log file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Calculate system-wide metrics
        total_tasks = sum(len(queue) for queue in self.task_queues.values())
        total_processed = sum(self.processed_tasks.values())
        
        # Calculate total power consumption
        total_power = 0
        total_energy = 0
        node_power_info = {}
        
        for node in self.nodes:
            node_stats = node.get_stats()
            power_info = node_stats.get('power_consumption', {})
            node_power = power_info.get('current_power', 0)
            node_energy = power_info.get('total_energy', 0)
            
            total_power += node_power
            total_energy += node_energy
            node_power_info[node.name] = {
                'current_power': node_power,
                'total_energy': node_energy,
                'cpu_power': power_info.get('cpu_power', 0),
                'memory_power': power_info.get('memory_power', 0),
                'network_power': power_info.get('network_power', 0)
            }
        
        # Create system status log entry
        log_entry = f"\n[{timestamp}] System Status Update\n"
        log_entry += f"System-wide Statistics:\n"
        log_entry += f"    Total Tasks in Queue: {total_tasks}\n"
        log_entry += f"    Total Tasks Processed: {total_processed}\n"
        log_entry += f"    Active Nodes: {sum(1 for node in self.nodes if len(self.task_queues[node.name]) > 0)}\n"
        
        # Add power consumption information
        log_entry += f"\nPower Consumption:\n"
        log_entry += f"    Total System Power: {total_power:.2f}W\n"
        log_entry += f"    Total Energy Consumed: {total_energy:.2f}kWh\n"
        
        # Add node-specific metrics
        log_entry += f"\nNode Status:\n"
        for node in self.nodes:
            queue_size = len(self.task_queues[node.name])
            processed = self.processed_tasks[node.name]
            metrics = self.node_metrics[node.name]
            power_info = node_power_info[node.name]
            
            avg_processing = sum(metrics['processing_times']) / len(metrics['processing_times']) if metrics['processing_times'] else 0
            avg_queue = sum(metrics['queue_times']) / len(metrics['queue_times']) if metrics['queue_times'] else 0
            avg_response = sum(metrics['response_times']) / len(metrics['response_times']) if metrics['response_times'] else 0
            avg_total = sum(metrics['total_times']) / len(metrics['total_times']) if metrics['total_times'] else 0
            
            log_entry += f"    {node.name}:\n"
            log_entry += f"        Queue Size: {queue_size}\n"
            log_entry += f"        Tasks Processed: {processed}\n"
            log_entry += f"        Average Processing Time: {avg_processing:.2f}ms\n"
            log_entry += f"        Average Queue Time: {avg_queue:.2f}ms\n"
            log_entry += f"        Average Response Time: {avg_response:.2f}ms\n"
            log_entry += f"        Average Total Time: {avg_total:.2f}ms\n"
            log_entry += f"        Current Power: {power_info['current_power']:.2f}W\n"
            log_entry += f"        Total Energy: {power_info['total_energy']:.2f}kWh\n"
            log_entry += f"        CPU Power: {power_info['cpu_power']:.2f}W\n"
            log_entry += f"        Memory Power: {power_info['memory_power']:.2f}W\n"
            log_entry += f"        Network Power: {power_info['network_power']:.2f}W\n"
        
        log_entry += "\n" + "-"*100
        self.system_log_file.write(log_entry)
        self.system_log_file.flush()

    def _log_activity(self, message, task_id=None, task_info=None):
        """Log activity to file with detailed information."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Base log entry
        log_entry = f"\n[{timestamp}] {message}"
        
        # Add task details if provided
        if task_id and task_info:
            log_entry += f"\nTask Details:"
            log_entry += f"\n    Task ID: {task_id}"
            log_entry += f"\n    Node: {task_info.get('node_name', 'N/A')}"
            
            # Timing Metrics Section
            log_entry += f"\n    Timing Metrics:"
            log_entry += f"\n        Total Time: {task_info.get('total_time', 0):.2f}ms"
            log_entry += f"\n        Response Time: {task_info.get('response_time', 0):.2f}ms"
            log_entry += f"\n        Processing Time: {task_info.get('processing_time', 0):.2f}ms"
            log_entry += f"\n        Queue Time: {task_info.get('queue_time', 0):.2f}ms"
            log_entry += f"\n        Transmission Time: {task_info.get('transmission_time', 0):.2f}ms"
            
            # Resource Usage Section
            log_entry += f"\n    Resource Usage:"
            log_entry += f"\n        Memory Used: {task_info.get('memory_used', 0)}MB"
            log_entry += f"\n        Bandwidth Used: {task_info.get('bandwidth_used', 0)}Mbps"
            log_entry += f"\n        RAM Required: {task_info.get('ram_required', 0)}MB"
            log_entry += f"\n        Task Size: {task_info.get('task_size', 0)}MB"
            
            # Network Metrics Section
            log_entry += f"\n    Network Metrics:"
            log_entry += f"\n        Distance: {task_info.get('distance_km', 0):.2f}km"
            log_entry += f"\n        Queue Size: {task_info.get('queue_size', 0)}"
            
            # System Load Section
            log_entry += f"\n    System Load:"
            log_entry += f"\n        System Load: {task_info.get('system_load', 0)*100:.1f}%"
            log_entry += f"\n        Bandwidth Utilization: {task_info.get('bandwidth_utilization', 0)*100:.1f}%"
            log_entry += f"\n        MIPS Utilization: {task_info.get('mips_utilization', 0)*100:.1f}%"
        
        log_entry += "\n" + "-"*100
        self.log_file.write(log_entry)
        self.log_file.flush()
        
        # Log system status separately
        self._log_system_status()

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
        # Get task ID from task data if available, otherwise generate new one
        if isinstance(task, dict):
            task_id = task.get('TaskID', self.next_task_id)
            task_size = task.get('Size', 1.0)
            task_memory = task.get('Memory', 512)
            task_bandwidth = task.get('Bandwidth', 100)
            task_location = task.get('GeoLocation', None)
        else:
            task_id = getattr(task, 'task_id', self.next_task_id)
            task_size = getattr(task, 'size', 1.0)
            task_memory = getattr(task, 'ram_required', 512)
            task_bandwidth = getattr(task, 'bandwidth_required', 100)
            task_location = getattr(task, 'source_location', None) or getattr(task, 'location', None)
        
        # Only increment next_task_id if we generated a new ID
        if task_id == self.next_task_id:
            self.next_task_id += 1
        
        self._log_activity(f"Scheduling task {task_id}", task_id, {
            'task_size': task_size,
            'task_memory': task_memory,
            'task_bandwidth': task_bandwidth,
            'task_location': task_location
        })
        
        # Select the best node
        selected_node = self.select_node(task)
        
        # Record task start time
        start_time = time.time()
        
        # Add task to queue
        self.task_queues[selected_node.name].append((task_id, task, start_time))
        self._log_activity(f"Task {task_id} added to queue of {selected_node.name}", task_id, {
            'node_name': selected_node.name,
            'queue_position': len(self.task_queues[selected_node.name]),
            'task_size': task_size,
            'task_memory': task_memory,
            'task_bandwidth': task_bandwidth
        })
        
        # Process the task
        result = await selected_node.process_task(task)
        
        # Remove task from queue and increment processed count
        self.task_queues[selected_node.name].remove((task_id, task, start_time))
        self.processed_tasks[selected_node.name] += 1
        
        # Calculate times
        end_time = time.time()
        queue_time = (end_time - start_time) * 1000  # Convert to milliseconds
        response_time = queue_time + result['processing_time']  # Response time includes queue and processing
        
        # Update node metrics
        self.node_metrics[selected_node.name]['processing_times'].append(result['processing_time'])
        self.node_metrics[selected_node.name]['transmission_times'].append(result.get('transmission_time', 0))
        self.node_metrics[selected_node.name]['queue_times'].append(queue_time)
        self.node_metrics[selected_node.name]['total_times'].append(result['total_time'])
        self.node_metrics[selected_node.name]['response_times'].append(response_time)
        
        # Log task completion with detailed information
        task_completion_info = {
            'task_id': task_id,
            'node_name': selected_node.name,
            'queue_time': queue_time,
            'processing_time': result['processing_time'],
            'transmission_time': result.get('transmission_time', 0),
            'total_time': result['total_time'],
            'response_time': response_time,
            'memory_used': result['memory_used'],
            'bandwidth_used': result['bandwidth_used'],
            'task_size': task_size,
            'task_memory': task_memory,
            'task_bandwidth': task_bandwidth,
            'distance_km': result.get('distance_km', 0),
            'queue_size': len(self.task_queues[selected_node.name]),
            'system_load': self.calculate_system_load(selected_node),
            'bandwidth_utilization': self.calculate_bandwidth_utilization(selected_node),
            'mips_utilization': self.calculate_mips_utilization(selected_node)
        }
        
        self._log_activity(
            f"Task {task_id} completed on {selected_node.name}\n"
            f"Task Details:\n"
            f"    ID: {task_id}\n"
            f"    Size: {task_size}MB\n"
            f"    Memory Required: {task_memory}MB\n"
            f"    Bandwidth Required: {task_bandwidth}Mbps\n"
            f"Timing Metrics:\n"
            f"    Queue Time: {queue_time:.2f}ms\n"
            f"    Processing Time: {result['processing_time']:.2f}ms\n"
            f"    Transmission Time: {result.get('transmission_time', 0):.2f}ms\n"
            f"    Total Time: {result['total_time']:.2f}ms\n"
            f"    Response Time: {response_time:.2f}ms\n"
            f"Resource Usage:\n"
            f"    Memory Used: {result['memory_used']}MB\n"
            f"    Bandwidth Used: {result['bandwidth_used']}Mbps\n"
            f"    Distance: {result.get('distance_km', 0):.2f}km",
            task_id,
            task_completion_info
        )
        
        return task_completion_info

    def get_stats(self):
        """Get comprehensive statistics for all nodes."""
        stats = {}
        total_processing_time = 0
        total_transmission_time = 0
        total_queue_time = 0
        total_response_time = 0
        total_tasks = 0
        total_power = 0
        total_energy = 0
        
        for node in self.nodes:
            metrics = self.node_metrics[node.name]
            node_stats = node.get_stats()
            power_info = node_stats.get('power_consumption', {})
            
            # Calculate averages from actual metrics
            avg_processing = sum(metrics['processing_times']) / len(metrics['processing_times']) if metrics['processing_times'] else 0
            avg_transmission = sum(metrics['transmission_times']) / len(metrics['transmission_times']) if metrics['transmission_times'] else 0
            avg_queue = sum(metrics['queue_times']) / len(metrics['queue_times']) if metrics['queue_times'] else 0
            avg_total = sum(metrics['total_times']) / len(metrics['total_times']) if metrics['total_times'] else 0
            avg_response = sum(metrics['response_times']) / len(metrics['response_times']) if metrics['response_times'] else 0
            avg_power = sum(metrics['power_consumption']) / len(metrics['power_consumption']) if metrics['power_consumption'] else 0
            
            # Update totals
            total_processing_time += sum(metrics['processing_times'])
            total_transmission_time += sum(metrics['transmission_times'])
            total_queue_time += sum(metrics['queue_times'])
            total_response_time += sum(metrics['response_times'])
            total_tasks += self.processed_tasks[node.name]
            total_power += power_info.get('current_power', 0)
            total_energy += power_info.get('total_energy', 0)
            
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
                'avg_response_time': avg_response,
                'avg_total_time': avg_total,
                'bandwidth_utilization': bandwidth_utilization,
                'mips_utilization': mips_utilization,
                'memory_utilization': memory_utilization,
                'current_tasks': len(node.current_tasks),
                'min_processing_time': min(metrics['processing_times']) if metrics['processing_times'] else 0,
                'max_processing_time': max(metrics['processing_times']) if metrics['processing_times'] else 0,
                'min_queue_time': min(metrics['queue_times']) if metrics['queue_times'] else 0,
                'max_queue_time': max(metrics['queue_times']) if metrics['queue_times'] else 0,
                'min_response_time': min(metrics['response_times']) if metrics['response_times'] else 0,
                'max_response_time': max(metrics['response_times']) if metrics['response_times'] else 0,
                'avg_power': avg_power,
                'total_energy': power_info.get('total_energy', 0)
            })
            
            stats[node.node_id] = node_stats
            self._log_activity(f"Stats for {node.name}: {node_stats}")
        
        # Add system-wide statistics
        if total_tasks > 0:
            stats['system'] = {
                'total_tasks': total_tasks,
                'avg_processing_time': total_processing_time / total_tasks,
                'avg_transmission_time': total_transmission_time / total_tasks,
                'avg_queue_time': total_queue_time / total_tasks,
                'avg_response_time': total_response_time / total_tasks,
                'total_processing_time': total_processing_time,
                'total_transmission_time': total_transmission_time,
                'total_queue_time': total_queue_time,
                'total_response_time': total_response_time,
                'active_nodes': sum(1 for node in self.nodes if len(self.task_queues[node.name]) > 0),
                'total_queue_size': sum(len(queue) for queue in self.task_queues.values()),
                'total_power': total_power,
                'total_energy': total_energy,
                'avg_power_per_node': total_power / len(self.nodes) if self.nodes else 0,
                'avg_energy_per_node': total_energy / len(self.nodes) if self.nodes else 0
            }
        
        return stats
        
    def __del__(self):
        """Cleanup when object is destroyed."""
        if hasattr(self, 'log_file'):
            # Write summary before closing
            stats = self.get_stats()
            if 'system' in stats:
                summary = f"""
{'='*100}
SCHEDULER SUMMARY REPORT - {self.mode.upper()} Mode
{'='*100}

System-wide Statistics:
    Total Tasks Processed: {stats['system']['total_tasks']}
    Total Processing Time: {stats['system']['total_processing_time']:.2f}ms
    Total Queue Time: {stats['system']['total_queue_time']:.2f}ms
    Total Transmission Time: {stats['system']['total_transmission_time']:.2f}ms
    Total Response Time: {stats['system']['total_response_time']:.2f}ms

Power Consumption Summary:
    Total System Power: {stats['system']['total_power']:.2f}W
    Total Energy Consumed: {stats['system']['total_energy']:.2f}kWh
    Average Power per Node: {stats['system']['avg_power_per_node']:.2f}W
    Average Energy per Node: {stats['system']['avg_energy_per_node']:.2f}kWh

Average Metrics:
    Processing Time: {stats['system']['avg_processing_time']:.2f}ms
    Queue Time: {stats['system']['avg_queue_time']:.2f}ms
    Transmission Time: {stats['system']['avg_transmission_time']:.2f}ms
    Response Time: {stats['system']['avg_response_time']:.2f}ms

Node Performance:
"""
                for node_id, node_stats in stats.items():
                    if node_id != 'system':
                        node = next(n for n in self.nodes if n.node_id == node_id)
                        summary += f"""
    {node.name}:
        Tasks Processed: {node_stats['total_processed']}
        Average Processing Time: {node_stats['avg_processing_time']:.2f}ms
        Average Queue Time: {node_stats['avg_queue_time']:.2f}ms
        Average Response Time: {node_stats['avg_response_time']:.2f}ms
        Current Queue Size: {node_stats['queue_size']}
        System Load: {node_stats['system_load']*100:.1f}%
        Memory Utilization: {node_stats['memory_utilization']*100:.1f}%
        MIPS Utilization: {node_stats['mips_utilization']*100:.1f}%
        Bandwidth Utilization: {node_stats['bandwidth_utilization']*100:.1f}%
        Current Power: {node_stats['power_consumption']['current_power']:.2f}W
        Total Energy: {node_stats['power_consumption']['total_energy']:.2f}kWh
"""
                summary += f"\n{'='*100}"
                self.log_file.write(summary)
                self.system_log_file.write(summary)
                self.log_file.flush()
                self.system_log_file.flush()
            self.log_file.close()
            self.system_log_file.close() 