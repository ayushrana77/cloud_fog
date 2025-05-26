import json
import asyncio
import time
import math
from datetime import datetime
from typing import List, Dict, Any
from collections import deque
from config import TASK_RATE_PER_SECOND, CLOUD_SERVICES_CONFIG, FOG_NODES_CONFIG, EARTH_RADIUS_KM
from cloud import CloudNode
from fog import FogNode

class GlobalGatewayFCFS:
    def __init__(self):
        self.start_time = time.time()
        self.next_task_id = 1
        
        # Initialize nodes
        self.nodes = []
        self.nodes.extend([CloudNode(i) for i in range(len(CLOUD_SERVICES_CONFIG))])
        self.nodes.extend([FogNode(i) for i in range(len(FOG_NODES_CONFIG))])
        
        # Initialize task tracking
        self.task_queues = {node.name: deque() for node in self.nodes}
        self.processed_tasks = {node.name: 0 for node in self.nodes}
        
        # Initialize metrics tracking
        self.node_metrics = {
            node.name: {
                'processing_times': [],
                'transmission_times': [],
                'queue_times': [],
                'total_times': [],
                'response_times': [],
                'power_consumption': [],
                'energy_consumption': 0.0
            } for node in self.nodes
        }
        
        # Initialize log files
        self.log_file = open('global_gateway_fcfs.log', 'w', encoding='utf-8')
        self.cloud_log_file = open('cloud_summary.log', 'w', encoding='utf-8')
        self.fog_log_file = open('fog_summary.log', 'w', encoding='utf-8')
        self._write_log_header()

    def _write_log_header(self):
        """Write header to log files."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"""
{'='*100}
{'*'*40} GLOBAL GATEWAY FCFS (GGFNC) LOG {'*'*40}
{'='*100}
Started at: {timestamp}
Configuration:
    Processing Rate: {TASK_RATE_PER_SECOND} tuples/second
    Cloud Nodes: {sum(1 for node in self.nodes if isinstance(node, CloudNode))}
    Fog Nodes: {sum(1 for node in self.nodes if isinstance(node, FogNode))}
{'='*100}
"""
        self.log_file.write(header)
        self.cloud_log_file.write(header)
        self.fog_log_file.write(header)
        self.log_file.flush()
        self.cloud_log_file.flush()
        self.fog_log_file.flush()

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
        
        return EARTH_RADIUS_KM * c

    def calculate_geographic_latency(self, distance_km, node):
        """Calculate geographic latency based on distance and node's bandwidth."""
        speed_of_light = 200000  # km/s
        base_latency = (distance_km / speed_of_light) * 1000  # Convert to milliseconds
        bandwidth_factor = 10000 / node.bandwidth  # Normalize against 10Gbps
        return base_latency * bandwidth_factor

    def calculate_queue_delay(self, node):
        """Calculate queue delay based on actual queue size and historical processing times."""
        queue_size = len(self.task_queues[node.name])
        if not self.node_metrics[node.name]['processing_times']:
            return 0
            
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
            
        recent_times = metrics['total_times'][-100:] if len(metrics['total_times']) > 100 else metrics['total_times']
        avg_processing_time = sum(recent_times) / len(recent_times)
        queue_size = len(self.task_queues[node.name])
        
        node_stats = node.get_stats()
        memory_utilization = node_stats.get('memory_utilization', 0)
        
        cpu_load = (queue_size * avg_processing_time) / (node.mips * 1000)
        return max(cpu_load, memory_utilization)

    def calculate_bandwidth_utilization(self, node):
        """Calculate bandwidth utilization based on transmission times."""
        if not self.node_metrics[node.name]['transmission_times'] or len(node.current_tasks) == 0:
            return 0
        
        recent_times = self.node_metrics[node.name]['transmission_times'][-100:]
        avg_transmission = sum(recent_times) / len(recent_times)
        return avg_transmission / (node.bandwidth / 1000)

    def calculate_mips_utilization(self, node):
        """Calculate MIPS utilization based on processing times."""
        if not self.node_metrics[node.name]['processing_times'] or len(node.current_tasks) == 0:
            return 0
        
        recent_times = self.node_metrics[node.name]['processing_times'][-100:]
        avg_processing = sum(recent_times) / len(recent_times)
        return avg_processing / (node.mips / 10000)

    def select_node(self, task, mode='hybrid'):
        """Select the optimal node for a task based on geographic proximity."""
        # Get task location
        if isinstance(task, dict):
            if 'GeoLocation' in task:
                source_location = {
                    'lat': task['GeoLocation']['latitude'],
                    'lon': task['GeoLocation']['longitude']
                }
            else:
                source_location = None
        else:
            source_location = getattr(task, 'source_location', None) or getattr(task, 'location', None)
        
        # Filter nodes based on mode
        available_nodes = []
        if mode == 'cloud':
            available_nodes = [node for node in self.nodes if isinstance(node, CloudNode)]
        elif mode == 'fog':
            available_nodes = [node for node in self.nodes if isinstance(node, FogNode)]
        else:  # hybrid mode
            available_nodes = self.nodes
            
        if not available_nodes:
            raise Exception(f"No available nodes for mode: {mode}")
            
        if not source_location:
            # If no location, use the first available node
            selected_node = available_nodes[0]
            self._log_activity(f"No location provided. Selected first available node: {selected_node.name}")
            return selected_node
        
        # Calculate distances to all available nodes
        node_distances = []
        for node in available_nodes:
            distance = self.calculate_distance(source_location, node.location)
            node_distances.append((node, distance))
            self._log_activity(f"Distance to {node.name}: {distance:.2f}km")
        
        # Sort nodes by distance and select the closest one
        node_distances.sort(key=lambda x: x[1])
        selected_node = node_distances[0][0]
        self._log_activity(f"Selected nearest node: {selected_node.name} (Distance: {node_distances[0][1]:.2f}km)")
        return selected_node

    async def _schedule_task(self, task, mode='hybrid'):
        """Schedule a task to the optimal node."""
        try:
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
            
            if task_id == self.next_task_id:
                self.next_task_id += 1
            
            self._log_activity(f"Scheduling task {task_id}", task_id, {
                'task_size': task_size,
                'task_memory': task_memory,
                'task_bandwidth': task_bandwidth,
                'task_location': task_location
            })
            
            # Select the best node
            selected_node = self.select_node(task, mode)
            
            # Record task start time
            start_time = time.time()
            
            # Add task to queue with timeout
            try:
                async with asyncio.timeout(5.0):  # 5 second timeout for queue operations
                    self.task_queues[selected_node.name].append((task_id, task, start_time))
                    self._log_activity(f"Task {task_id} added to queue of {selected_node.name}", task_id, {
                        'node_name': selected_node.name,
                        'queue_position': len(self.task_queues[selected_node.name]),
                        'task_size': task_size,
                        'task_memory': task_memory,
                        'task_bandwidth': task_bandwidth
                    })
            except asyncio.TimeoutError:
                self._log_activity(f"Timeout while adding task {task_id} to queue", task_id)
                raise Exception("Queue operation timeout")
            
            # Process the task with timeout
            try:
                async with asyncio.timeout(10.0):  # 10 second timeout for task processing
                    result = await selected_node.process_task(task)
            except asyncio.TimeoutError:
                self._log_activity(f"Timeout while processing task {task_id}", task_id)
                raise Exception("Task processing timeout")
            
            # Remove task from queue and increment processed count
            try:
                async with asyncio.timeout(5.0):  # 5 second timeout for queue operations
                    self.task_queues[selected_node.name].remove((task_id, task, start_time))
                    self.processed_tasks[selected_node.name] += 1
            except asyncio.TimeoutError:
                self._log_activity(f"Timeout while removing task {task_id} from queue", task_id)
                raise Exception("Queue operation timeout")
            
            # Calculate times
            end_time = time.time()
            queue_time = (end_time - start_time) * 1000
            response_time = queue_time + result['processing_time']
            
            # Calculate power consumption
            cpu_power = (result['processing_time'] / 1000) * selected_node.mips * 0.1
            memory_power = (task_memory / selected_node.memory) * 100
            network_power = (result.get('transmission_time', 0) / 1000) * (task_bandwidth / selected_node.bandwidth) * 50
            
            current_power = cpu_power + memory_power + network_power
            energy_consumption = (current_power * (result['total_time'] / 1000)) / 3600
            
            # Update node metrics
            self.node_metrics[selected_node.name]['processing_times'].append(result['processing_time'])
            self.node_metrics[selected_node.name]['transmission_times'].append(result.get('transmission_time', 0))
            self.node_metrics[selected_node.name]['queue_times'].append(queue_time)
            self.node_metrics[selected_node.name]['total_times'].append(result['total_time'])
            self.node_metrics[selected_node.name]['response_times'].append(response_time)
            self.node_metrics[selected_node.name]['power_consumption'].append(current_power)
            self.node_metrics[selected_node.name]['energy_consumption'] += energy_consumption
            
            # Log task completion
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
                'mips_utilization': self.calculate_mips_utilization(selected_node),
                'current_power': current_power,
                'total_energy': energy_consumption,
                'power_consumption': {
                    'cpu_power': cpu_power,
                    'memory_power': memory_power,
                    'network_power': network_power,
                    'current_power': current_power,
                    'total_energy': energy_consumption
                }
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
                f"    Distance: {result.get('distance_km', 0):.2f}km\n"
                f"Power Consumption:\n"
                f"    CPU Power: {cpu_power:.2f}W\n"
                f"    Memory Power: {memory_power:.2f}W\n"
                f"    Network Power: {network_power:.2f}W\n"
                f"    Total Power: {current_power:.2f}W\n"
                f"    Energy Consumed: {energy_consumption:.4f}kWh",
                task_id,
                task_completion_info
            )
            
            return task_completion_info
            
        except Exception as e:
            self._log_activity(f"Error in _schedule_task: {str(e)}", task_id if 'task_id' in locals() else None)
            raise

    async def process_tuple(self, tuple_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single tuple using FCFS policy (GGFNC algorithm)."""
        try:
            # Step 1: Check data type
            data_type = tuple_data.get('DataType', '').lower()
            task_id = tuple_data.get('ID', 'Unknown')
            self._log_activity(f"Processing tuple {task_id} with type: {data_type}", task_id, tuple_data)
            
            # Step 2: Route bulk/large tasks directly to Cloud
            if data_type in ['bulk', 'large', 'Bulk', 'Large']:
                self._log_activity(f"GGFNC: Routing {data_type} data to Cloud", task_id, tuple_data)
                result = await self._schedule_task(tuple_data, mode='cloud')
                result['is_cloud_task'] = True
                result['task_id'] = task_id
                return result
            
            # Step 3: For non-bulk/large tasks, check fog resources first
            task_memory = tuple_data.get('Memory', 512)
            task_bandwidth = tuple_data.get('Bandwidth', 100)
            
            # Get available fog nodes
            fog_nodes = [node for node in self.nodes if isinstance(node, FogNode)]
            
            # Check if any fog node has sufficient resources
            fog_node_available = False
            for node in fog_nodes:
                try:
                    node_stats = node.get_stats()
                    available_memory = node.memory - sum(task['memory'] for task in node.current_tasks)
                    available_bandwidth = node.bandwidth - sum(task['bandwidth'] for task in node.current_tasks)
                    
                    if available_memory >= task_memory and available_bandwidth >= task_bandwidth:
                        fog_node_available = True
                        break
                except Exception as e:
                    self._log_activity(f"Error checking fog node {node.name} resources: {str(e)}", task_id)
                    continue
            
            # If no fog node has sufficient resources, route to nearest cloud
            if not fog_node_available:
                self._log_activity(f"GGFNC: No fog nodes with sufficient resources, routing to nearest Cloud", task_id, tuple_data)
                try:
                    # Get cloud nodes
                    cloud_nodes = [node for node in self.nodes if isinstance(node, CloudNode)]
                    if not cloud_nodes:
                        raise Exception("No cloud nodes available")
                    
                    # Find nearest cloud node
                    if 'GeoLocation' in tuple_data:
                        source_location = {
                            'lat': tuple_data['GeoLocation']['latitude'],
                            'lon': tuple_data['GeoLocation']['longitude']
                        }
                        # Calculate distances to cloud nodes
                        cloud_distances = [(node, self.calculate_distance(source_location, node.location)) 
                                        for node in cloud_nodes]
                        cloud_distances.sort(key=lambda x: x[1])
                        nearest_cloud = cloud_distances[0][0]
                    else:
                        nearest_cloud = cloud_nodes[0]
                    
                    self._log_activity(f"Selected nearest cloud node: {nearest_cloud.name}", task_id)
                    cloud_result = await self._schedule_task(tuple_data, mode='cloud')
                    cloud_result['is_cloud_task'] = True
                    cloud_result['task_id'] = task_id
                    return cloud_result
                except Exception as e:
                    self._log_activity(f"Error routing to cloud: {str(e)}", task_id)
                    raise
            
            # Step 4: If fog resources are available, try fog first
            try:
                self._log_activity(f"GGFNC: Attempting Fog for {data_type} data", task_id, tuple_data)
                fog_result = await self._schedule_task(tuple_data, mode='fog')
                fog_result['is_cloud_task'] = False
                fog_result['task_id'] = task_id
                return fog_result
            except Exception as e:
                # Step 5: If fog processing fails, fall back to nearest cloud
                self._log_activity(f"GGFNC: Fog processing failed, falling back to nearest Cloud", task_id, tuple_data)
                try:
                    # Get cloud nodes
                    cloud_nodes = [node for node in self.nodes if isinstance(node, CloudNode)]
                    if not cloud_nodes:
                        raise Exception("No cloud nodes available")
                    
                    # Find nearest cloud node
                    if 'GeoLocation' in tuple_data:
                        source_location = {
                            'lat': tuple_data['GeoLocation']['latitude'],
                            'lon': tuple_data['GeoLocation']['longitude']
                        }
                        # Calculate distances to cloud nodes
                        cloud_distances = [(node, self.calculate_distance(source_location, node.location)) 
                                        for node in cloud_nodes]
                        cloud_distances.sort(key=lambda x: x[1])
                        nearest_cloud = cloud_distances[0][0]
                    else:
                        nearest_cloud = cloud_nodes[0]
                    
                    self._log_activity(f"Selected nearest cloud node: {nearest_cloud.name}", task_id)
                    cloud_result = await self._schedule_task(tuple_data, mode='cloud')
                    cloud_result['is_cloud_task'] = True
                    cloud_result['task_id'] = task_id
                    return cloud_result
                except Exception as e:
                    self._log_activity(f"Error routing to cloud: {str(e)}", task_id)
                    raise
                    
        except Exception as e:
            self._log_activity(f"Error processing tuple {task_id}: {str(e)}", task_id)
            raise

    async def process_all_tuples(self, tuples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process all tuples using FCFS policy (GGFNC algorithm).
        Resources are allocated sequentially, but tasks run in parallel once allocated."""
        try:
            # List to store running tasks
            running_tasks = []
            successful_results = []
            
            # Process tuples one by one for resource allocation
            for i, tuple_data in enumerate(tuples, 1):
                try:
                    task_id = tuple_data.get('ID', f'Task_{i}')
                    self._log_activity(f"Allocating resources for tuple {i} of {len(tuples)}", task_id)
                    
                    # Allocate resources and start task processing
                    async with asyncio.timeout(5.0):  # 5 second timeout for resource allocation
                        # Create task but don't await it yet
                        task = asyncio.create_task(self.process_tuple(tuple_data))
                        running_tasks.append((task, task_id))
                        self._log_activity(f"Resources allocated for tuple {i}, task started in background", task_id)
                        
                except asyncio.TimeoutError:
                    self._log_activity(f"Timeout while allocating resources for tuple {i}", task_id)
                    continue
                except Exception as e:
                    self._log_activity(f"Error allocating resources for tuple {i}: {str(e)}", task_id)
                    continue
            
            # Wait for all running tasks to complete
            if running_tasks:
                self._log_activity(f"Waiting for {len(running_tasks)} tasks to complete")
                for task, task_id in running_tasks:
                    try:
                        async with asyncio.timeout(30.0):  # 30 second timeout for task completion
                            result = await task
                            successful_results.append(result)
                            self._log_activity(f"Task {task_id} completed successfully")
                    except asyncio.TimeoutError:
                        self._log_activity(f"Timeout while waiting for task {task_id} to complete")
                    except Exception as e:
                        self._log_activity(f"Error in task {task_id}: {str(e)}")
            
            # Calculate summary statistics
            total_tasks = len(successful_results)
            cloud_tasks = sum(1 for r in successful_results if r.get('is_cloud_task', False))
            fog_tasks = total_tasks - cloud_tasks
            
            # Log summary
            self._log_activity("Processing complete!", None, {
                'total_tasks': total_tasks,
                'cloud_tasks': cloud_tasks,
                'fog_tasks': fog_tasks,
                'failed_tasks': len(tuples) - total_tasks
            })
            
            # Log detailed statistics
            self._log_node_statistics(successful_results)
            
            # Log routing details
            self._log_routing_details(tuples, successful_results)
            
            # Log system status
            self._log_system_status(successful_results)
            
            return successful_results
            
        except Exception as e:
            self._log_activity(f"Error in process_all_tuples: {str(e)}")
            raise
        finally:
            # Close log files
            self.log_file.close()
            self.cloud_log_file.close()
            self.fog_log_file.close()

    def _log_activity(self, message: str, task_id: str = None, task_info: Dict[str, Any] = None):
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

    def _log_node_statistics(self, results: List[Dict[str, Any]]):
        """Write detailed node statistics to log file."""
        self.log_file.write("\nNode Statistics:")
        self.log_file.write("\n" + "="*80)
        
        # Cloud Nodes
        self.log_file.write("\n\nCloud Nodes:")
        self.log_file.write("\n" + "-"*40)
        
        cloud_nodes = [node for node in self.nodes if isinstance(node, CloudNode)]
        for node in cloud_nodes:
            node_results = [r for r in results if r.get('node_name') == node.name]
            tasks = len(node_results)
            
            self.log_file.write(f"\n\n{node.name}:")
            self.log_file.write(f"\n  Tasks Processed: {tasks}")
            self.log_file.write(f"\n  Average Processing Time: {self._calculate_avg_time(results, node.name, 'processing_time'):.2f}ms")
            self.log_file.write(f"\n  Average Queue Time: {self._calculate_avg_time(results, node.name, 'queue_time'):.2f}ms")
            self.log_file.write(f"\n  Average Response Time: {self._calculate_avg_time(results, node.name, 'total_time'):.2f}ms")
            self.log_file.write(f"\n  Current Queue Size: {self._get_node_stat(results, node.name, 'queue_size', 0)}")
            self.log_file.write(f"\n  System Load: {self._get_node_stat(results, node.name, 'system_load', 0.0)*100:.1f}%")
            self.log_file.write(f"\n  Memory Utilization: {self._get_node_stat(results, node.name, 'memory_utilization', 0.0)*100:.1f}%")
            self.log_file.write(f"\n  MIPS Utilization: {self._get_node_stat(results, node.name, 'mips_utilization', 0.0)*100:.1f}%")
            self.log_file.write(f"\n  Bandwidth Utilization: {self._get_node_stat(results, node.name, 'bandwidth_utilization', 0.0)*100:.1f}%")
            self.log_file.write(f"\n  Current Power: {self._get_node_stat(results, node.name, 'current_power', 0.0):.2f}W")
            self.log_file.write(f"\n  Total Energy: {self._get_node_stat(results, node.name, 'total_energy', 0.0):.2f}kWh")
        
        # Fog Nodes
        self.log_file.write("\n\nFog Nodes:")
        self.log_file.write("\n" + "-"*40)
        
        fog_nodes = [node for node in self.nodes if isinstance(node, FogNode)]
        for node in fog_nodes:
            node_results = [r for r in results if r.get('node_name') == node.name]
            tasks = len(node_results)
            
            self.log_file.write(f"\n\n{node.name}:")
            self.log_file.write(f"\n  Tasks Processed: {tasks}")
            self.log_file.write(f"\n  Average Processing Time: {self._calculate_avg_time(results, node.name, 'processing_time'):.2f}ms")
            self.log_file.write(f"\n  Average Queue Time: {self._calculate_avg_time(results, node.name, 'queue_time'):.2f}ms")
            self.log_file.write(f"\n  Average Response Time: {self._calculate_avg_time(results, node.name, 'total_time'):.2f}ms")
            self.log_file.write(f"\n  Current Queue Size: {self._get_node_stat(results, node.name, 'queue_size', 0)}")
            self.log_file.write(f"\n  System Load: {self._get_node_stat(results, node.name, 'system_load', 0.0)*100:.1f}%")
            self.log_file.write(f"\n  Memory Utilization: {self._get_node_stat(results, node.name, 'memory_utilization', 0.0)*100:.1f}%")
            self.log_file.write(f"\n  MIPS Utilization: {self._get_node_stat(results, node.name, 'mips_utilization', 0.0)*100:.1f}%")
            self.log_file.write(f"\n  Bandwidth Utilization: {self._get_node_stat(results, node.name, 'bandwidth_utilization', 0.0)*100:.1f}%")
            self.log_file.write(f"\n  Current Power: {self._get_node_stat(results, node.name, 'current_power', 0.0):.2f}W")
            self.log_file.write(f"\n  Total Energy: {self._get_node_stat(results, node.name, 'total_energy', 0.0):.2f}kWh")
        
        self.log_file.flush()

    def _log_routing_details(self, data: List[Dict[str, Any]], results: List[Dict[str, Any]]):
        """Write routing details to log file."""
        self.log_file.write("\n\nRouting Details:")
        for i, (tuple_data, result) in enumerate(zip(data, results), 1):
            data_type = tuple_data.get('DataType', 'Unknown')
            node_name = result.get('node_name', 'Unknown')
            task_id = tuple_data.get('ID', 'Unknown')
            self.log_file.write(f"\nTuple {i} (ID: {task_id}): Type={data_type}, Routed to={node_name}")
            self.log_file.write(f"\nTuple {i} Data: {json.dumps(tuple_data, indent=2)}")
            self.log_file.write("\n" + "-"*50)
        self.log_file.flush()

    def _log_system_status(self, results: List[Dict[str, Any]]):
        """Log system-wide status to the log file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Calculate system-wide metrics
        total_tasks = len(results)
        cloud_tasks = sum(1 for r in results if r.get('is_cloud_task', False))
        fog_tasks = total_tasks - cloud_tasks
        
        # Calculate total power consumption
        total_power = sum(r.get('current_power', 0) for r in results)
        total_energy = sum(r.get('total_energy', 0) for r in results)
        
        # Create system status log entry
        log_entry = f"\n[{timestamp}] System Status Update\n"
        log_entry += f"System-wide Statistics:\n"
        log_entry += f"    Total Tasks Processed: {total_tasks}\n"
        log_entry += f"    Tasks Processed by Cloud: {cloud_tasks}\n"
        log_entry += f"    Tasks Processed by Fog: {fog_tasks}\n"
        
        # Add power consumption information
        log_entry += f"\nPower Consumption:\n"
        log_entry += f"    Total System Power: {total_power:.2f}W\n"
        log_entry += f"    Total Energy Consumed: {total_energy:.2f}kWh\n"
        
        # Add node-specific metrics
        log_entry += f"\nNode Status:\n"
        
        # Cloud Nodes
        cloud_nodes = [node for node in self.nodes if isinstance(node, CloudNode)]
        for node in cloud_nodes:
            node_results = [r for r in results if r.get('node_name') == node.name]
            if node_results:
                tasks = len(node_results)
                avg_processing = sum(r.get('processing_time', 0) for r in node_results) / tasks
                avg_queue = sum(r.get('queue_time', 0) for r in node_results) / tasks
                avg_response = sum(r.get('response_time', 0) for r in node_results) / tasks
                current_power = sum(r.get('current_power', 0) for r in node_results)
                total_energy = sum(r.get('total_energy', 0) for r in node_results)
                
                log_entry += f"    {node.name}:\n"
                log_entry += f"        Tasks Processed: {tasks}\n"
                log_entry += f"        Average Processing Time: {avg_processing:.2f}ms\n"
                log_entry += f"        Average Queue Time: {avg_queue:.2f}ms\n"
                log_entry += f"        Average Response Time: {avg_response:.2f}ms\n"
                log_entry += f"        Current Power: {current_power:.2f}W\n"
                log_entry += f"        Total Energy: {total_energy:.2f}kWh\n"
        
        # Fog Nodes
        fog_nodes = [node for node in self.nodes if isinstance(node, FogNode)]
        for node in fog_nodes:
            node_results = [r for r in results if r.get('node_name') == node.name]
            if node_results:
                tasks = len(node_results)
                avg_processing = sum(r.get('processing_time', 0) for r in node_results) / tasks
                avg_queue = sum(r.get('queue_time', 0) for r in node_results) / tasks
                avg_response = sum(r.get('response_time', 0) for r in node_results) / tasks
                current_power = sum(r.get('current_power', 0) for r in node_results)
                total_energy = sum(r.get('total_energy', 0) for r in node_results)
                
                log_entry += f"    {node.name}:\n"
                log_entry += f"        Tasks Processed: {tasks}\n"
                log_entry += f"        Average Processing Time: {avg_processing:.2f}ms\n"
                log_entry += f"        Average Queue Time: {avg_queue:.2f}ms\n"
                log_entry += f"        Average Response Time: {avg_response:.2f}ms\n"
                log_entry += f"        Current Power: {current_power:.2f}W\n"
                log_entry += f"        Total Energy: {total_energy:.2f}kWh\n"
        
        # Add Final Statistics Summary
        log_entry += f"\nFinal Statistics Summary:\n"
        log_entry += f"    Total Tasks Completed: {total_tasks}\n"
        log_entry += f"    Cloud Tasks: {cloud_tasks} ({cloud_tasks/total_tasks*100:.1f}%)\n"
        log_entry += f"    Fog Tasks: {fog_tasks} ({fog_tasks/total_tasks*100:.1f}%)\n"
        
        # Calculate average metrics across all tasks
        avg_processing = sum(r.get('processing_time', 0) for r in results) / total_tasks
        avg_queue = sum(r.get('queue_time', 0) for r in results) / total_tasks
        avg_response = sum(r.get('response_time', 0) for r in results) / total_tasks
        avg_power = sum(r.get('current_power', 0) for r in results) / total_tasks
        
        log_entry += f"\n    Average Metrics Across All Tasks:\n"
        log_entry += f"        Average Processing Time: {avg_processing:.2f}ms\n"
        log_entry += f"        Average Queue Time: {avg_queue:.2f}ms\n"
        log_entry += f"        Average Response Time: {avg_response:.2f}ms\n"
        log_entry += f"        Average Power Consumption: {avg_power:.2f}W\n"
        
        # Calculate performance metrics
        min_processing = min(r.get('processing_time', 0) for r in results)
        max_processing = max(r.get('processing_time', 0) for r in results)
        min_response = min(r.get('response_time', 0) for r in results)
        max_response = max(r.get('response_time', 0) for r in results)
        
        log_entry += f"\n    Performance Metrics:\n"
        log_entry += f"        Fastest Processing Time: {min_processing:.2f}ms\n"
        log_entry += f"        Slowest Processing Time: {max_processing:.2f}ms\n"
        log_entry += f"        Fastest Response Time: {min_response:.2f}ms\n"
        log_entry += f"        Slowest Response Time: {max_response:.2f}ms\n"
        
        # Calculate energy efficiency
        total_processing_time = sum(r.get('processing_time', 0) for r in results)
        energy_per_task = total_energy / total_tasks
        energy_per_ms = total_energy / (total_processing_time / 1000)  # kWh per second
        
        log_entry += f"\n    Energy Efficiency:\n"
        log_entry += f"        Energy per Task: {energy_per_task:.4f}kWh\n"
        log_entry += f"        Energy per Second: {energy_per_ms:.4f}kWh\n"
        
        log_entry += "\n" + "-"*100
        self.log_file.write(log_entry)
        self.log_file.flush()
        
        # Also print to console
        print("\n" + "="*100)
        print("SYSTEM STATUS UPDATE")
        print("="*100)
        print(log_entry)

    def _calculate_avg_time(self, results: List[Dict[str, Any]], node_name: str, time_field: str) -> float:
        """Calculate average time for a specific node and time field."""
        node_results = [r for r in results if r.get('node_name') == node_name]
        if not node_results:
            return 0.0
        return sum(r.get(time_field, 0) for r in node_results) / len(node_results)
        
    def _get_node_stat(self, results: List[Dict[str, Any]], node_name: str, stat_field: str, default: Any) -> Any:
        """Get a specific statistic for a node."""
        node_results = [r for r in results if r.get('node_name') == node_name]
        if not node_results:
            return default
        return node_results[0].get(stat_field, default)

async def main():
    try:
        # Read the JSON file
        with open('Tuple10.json', 'r', encoding='utf-8-sig') as file:
            data = json.load(file)
            
        print(f"\nProcessing {len(data)} tuples")
        print(f"Processing rate: {TASK_RATE_PER_SECOND} tuples/second")
        print("-" * 50)
        
        # Initialize and run the Global Gateway FCFS processor
        processor = GlobalGatewayFCFS()
        results = await processor.process_all_tuples(data)
        
        # Print summary
        print("\nProcessing complete!")
        print(f"Total tuples processed: {len(results)}")
        
        # Calculate and print statistics
        cloud_tasks = sum(1 for r in results if r.get('is_cloud_task', False))
        fog_tasks = len(results) - cloud_tasks
        
        print(f"Tasks processed by Cloud: {cloud_tasks}")
        print(f"Tasks processed by Fog: {fog_tasks}")
        
        # Print detailed routing information
        print("\nRouting Details:")
        for i, (tuple_data, result) in enumerate(zip(data, results), 1):
            data_type = tuple_data.get('DataType', 'Unknown')
            node_name = result.get('node_name', 'Unknown')
            print(f"Tuple {i}: Type={data_type}, Routed to={node_name}")
            
            # Print full tuple data for debugging
            print(f"Tuple {i} Data: {json.dumps(tuple_data, indent=2)}")
            print("-" * 50)
        
    except FileNotFoundError:
        print("Error: Tuple10.json file not found in the current directory.")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 