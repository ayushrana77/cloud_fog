"""
MCT Fog-Only (Minimum Completion Time) Task Scheduling Implementation

This module implements a fog-only variant of the MCT algorithm that minimizes 
completion time by considering only fog resources with optimal resource allocation.

Key Features:
- Calculates optimal task assignments between fog nodes
- Considers multiple factors including:
  * Distance between task and fog nodes
  * Resource availability (MIPS, Memory, Bandwidth, Storage)
  * Processing and transmission times
  * Queue times
- Provides detailed scoring and analysis of fog node performance
- Implements comprehensive logging of task execution and performance metrics
- Tasks that cannot be immediately processed are queued at fog nodes

The algorithm follows these main steps:
1. Calculate scores for all available fog nodes
2. Select the fog node with the highest score
3. If fog node cannot immediately handle the task, queue it at the best fog node
4. Track and log all performance metrics

Dependencies:
- task_load: For reading and logging task tuples
- config: For fog node configurations
- utility: For distance calculations
- fog_with_queue: For fog node operations with queue support
- logger: For logging functionality
"""
import math
import time
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG
from utility import calculate_distance, calculate_transmission_time, calculate_power_consumption
from fog_with_queue import get_fog_node, get_fog_node_status, get_all_fog_nodes
from logger import setup_logger

# Initialize logger for MCT Fog-Only events
mct_fog_logger = setup_logger('mct_fog_only', 'mct_fog_only.log', sub_directory='algorithms')

def calculate_task_distances(task):
    """
    Calculate distances from task to all fog nodes
    
    Args:
        task (dict): Task information containing location data
        
    Returns:
        dict: Dictionary mapping node names to their distances from the task
    """
    distances = {}
    task_location = {
        'lat': task['GeoLocation']['latitude'],
        'lon': task['GeoLocation']['longitude']
    }
    
    # Calculate distances to fog nodes only
    for node in FOG_NODES_CONFIG:
        node_location = node['location']
        distance = calculate_distance(task_location, node_location)
        distances[node['name']] = distance
    
    return distances

def calculate_fog_node_score(task, fog_node, distance):
    """
    Calculate a comprehensive score for a fog node based on multiple factors
    
    Args:
        task (dict): Task information
        fog_node: Fog node object
        distance (float): Distance to the fog node
        
    Returns:
        float: Calculated score (higher is better)
    """
    # Get node status
    status = fog_node.get_status()
    
    # Calculate transmission time using comprehensive implementation
    transmission_time = calculate_transmission_time(
        task['GeoLocation'],
        fog_node.location,
        fog_node,
        task.get('Size'),
        task.get('MIPS'),
        mct_fog_logger
    )
    
    # Calculate processing time
    processing_time = task['Size'] / fog_node.mips
    
    # Calculate resource availability scores for each resource type (0 to 1, where 1 is fully available)
    mips_availability = status.get('available_mips', fog_node.mips) / fog_node.mips
    memory_availability = status.get('available_memory', fog_node.memory) / fog_node.memory
    bandwidth_availability = status.get('available_bandwidth', fog_node.bandwidth) / fog_node.bandwidth
    storage_availability = status.get('available_storage', fog_node.storage) / fog_node.storage
    
    # Calculate weighted resource availability score
    # Equal weights for all resources to ensure balanced consideration
    resource_score = (0.25 * mips_availability + 
                     0.25 * memory_availability + 
                     0.25 * bandwidth_availability +
                     0.25 * storage_availability)
    
    # Normalize transmission and processing times to 0-1 range
    # Using inverse square root for better scaling
    max_transmission_time = 5.0  # Maximum acceptable time in seconds
    transmission_score = 1.0 / (1.0 + math.sqrt(transmission_time / max_transmission_time))
    
    max_processing_time = 5.0  # Maximum acceptable time in seconds
    processing_score = 1.0 / (1.0 + math.sqrt(processing_time / max_processing_time))
    
    # Calculate weighted components
    # Adjusted weights based on importance:
    # - Resource availability is most important (40%)
    # - Processing time is second (35%)
    # - Transmission time is third (25%)
    resource_component = 0.40 * resource_score
    processing_component = 0.35 * processing_score
    transmission_component = 0.25 * transmission_score
    
    # Final score calculation (higher is better)
    score = (resource_component + 
             processing_component + 
             transmission_component)
    
    # Create detailed resource information
    resource_info = {
        'mips': {
            'available': status.get('available_mips', fog_node.mips),
            'total': fog_node.mips,
            'availability': mips_availability
        },
        'memory': {
            'available': status.get('available_memory', fog_node.memory),
            'total': fog_node.memory,
            'availability': memory_availability
        },
        'bandwidth': {
            'available': status.get('available_bandwidth', fog_node.bandwidth),
            'total': fog_node.bandwidth,
            'availability': bandwidth_availability
        },
        'storage': {
            'available': status.get('available_storage', fog_node.storage),
            'total': fog_node.storage,
            'availability': storage_availability
        }
    }
    
    # Create score breakdown
    score_breakdown = {
        'resource_availability': {
            'value': resource_score,
            'weighted': resource_component
        },
        'processing': {
            'value': processing_time,
            'weighted': processing_component
        },
        'transmission': {
            'value': transmission_time,
            'weighted': transmission_component,
            'score': transmission_score
        }
    }
    
    return score, resource_info, score_breakdown

def calculate_fog_completion_time(task, fog_node, distance, task_queue_times):
    """
    Calculate completion time for a fog node using MCT approach
    
    Args:
        task (dict): Task information
        fog_node: Fog node object
        distance (float): Distance to the fog node
        task_queue_times (dict): Dictionary tracking task queue times
        
    Returns:
        float: Completion time
    """
    # Get node status
    status = fog_node.get_status()
    
    # Calculate transmission time
    transmission_time = calculate_transmission_time(
        task['GeoLocation'],
        fog_node.location,
        fog_node,
        task.get('Size'),
        task.get('MIPS'),
        mct_fog_logger
    )
    
    # Calculate execution time
    execution_time = task['Size'] / fog_node.mips
    
    # Calculate ready time (time until node becomes available)
    ready_time = 0
    current_time = time.time()
    
    # Check if node is busy with current tasks
    if fog_node.assigned_tasks:
        # Find the latest completion time among current tasks
        try:
            latest_completion = max(
                task_info['start_time'] + task_info['processing_time']
                for task_info in fog_node.assigned_tasks
            )
            ready_time = max(0, latest_completion - current_time)
            mct_fog_logger.info(f"Node {fog_node.name} is busy with {len(fog_node.assigned_tasks)} tasks")
            mct_fog_logger.info(f"Latest task completion time: {latest_completion}")
            mct_fog_logger.info(f"Current time: {current_time}")
            mct_fog_logger.info(f"Calculated ready time: {ready_time*1000:.2f}ms")
        except ValueError:
            # Handle case where assigned_tasks list becomes empty during iteration
            mct_fog_logger.info(f"Node {fog_node.name} is idle (assigned_tasks became empty), ready time: 0ms")
            ready_time = 0
    else:
        mct_fog_logger.info(f"Node {fog_node.name} is idle, ready time: 0ms")
    
    # Calculate queue time
    queue_time = 0
    if task['Name'] in task_queue_times:
        queue_time = time.time() - task_queue_times[task['Name']]
    
    # Total completion time
    completion_time = transmission_time + execution_time + ready_time + queue_time
    
    # Log detailed timing information
    mct_fog_logger.info(f"\nDetailed timing for {fog_node.name}:")
    mct_fog_logger.info(f"  - Distance: {distance:.2f} km")
    mct_fog_logger.info(f"  - Transmission Time: {transmission_time*1000:.2f}ms")
    mct_fog_logger.info(f"  - Execution Time: {execution_time*1000:.2f}ms")
    mct_fog_logger.info(f"  - Ready Time: {ready_time*1000:.2f}ms")
    mct_fog_logger.info(f"  - Queue Time: {queue_time*1000:.2f}ms")
    mct_fog_logger.info(f"  - Total Time: {completion_time*1000:.2f}ms")
    
    return completion_time

def process_mct(tasks):
    """
    Process tasks using Minimum Completion Time (MCT) algorithm with fog nodes only
    
    This function implements the MCT fog-only scheduling logic:
    1. For each task, calculate scores for all fog nodes
    2. Select the fog node with maximum score
    3. If fog node can't handle the task immediately, queue it at the best fog node
    4. Track and log all performance metrics including queue times
    
    Args:
        tasks (list): List of tasks to be processed
    """
    if not tasks:
        print("No tasks to process")
        mct_fog_logger.warning("No tasks to process.")
        return
        
    print("\n=== MCT Fog-Only Processing ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    mct_fog_logger.info(f"Total Tasks to Process: {len(tasks)}")
    
    # Initialize and log fog nodes
    fog_nodes = get_all_fog_nodes()
    for name, node in fog_nodes.items():
        mct_fog_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Storage={node.storage}, Location={node.location})")
    
    # Initialize tracking variables
    task_completion_info = {}
    queued_tasks_info = {}
    completed_tasks_count = 0
    total_tasks = len(tasks)
    task_queue_times = {}  # Initialize task queue times dictionary
    
    # Create a set to track unique task IDs to prevent double counting
    completed_task_ids = set()
    
    def task_completion_callback(node_name, completion_info):
        """Callback function to handle task completion events"""
        nonlocal completed_tasks_count
        task = completion_info['task']
        task_name = task['Name']
        
        # Skip if we've already counted this task
        if task_name in completed_task_ids:
            return
            
        # Add to completed tasks set
        completed_task_ids.add(task_name)
        
        # Calculate queue time if task was queued
        queue_time = 0
        if task_name in task_queue_times:
            queue_time = time.time() - task_queue_times[task_name]
            del task_queue_times[task_name]
        
        # Get power consumption information
        power_info = completion_info.get('power_consumption', {})
        
        # Store completion information
        task_completion_info[task_name] = {
            'node': node_name,
            'transmission_time': completion_info.get('transmission_time', 0),
            'processing_time': completion_info.get('processing_time', 0),
            'queue_time': queue_time,
            'total_time': completion_info.get('total_time', 0) + queue_time,
            'completion_time': completion_info.get('completion_time', 0),
            'storage_used': task.get('Storage', 0),
            'power_consumption': power_info,  # Add power consumption information
            'task': task  # Store the complete task information
        }
        
        # Update completion counter and logging
        completed_tasks_count += 1
        mct_fog_logger.info(f"\nTask Completed: {task_name}")
        mct_fog_logger.info(f"  Completed at: {node_name}")
        mct_fog_logger.info(f"  Transmission Time: {completion_info.get('transmission_time', 0):.2f}s")
        mct_fog_logger.info(f"  Processing Time: {completion_info.get('processing_time', 0):.2f}s")
        mct_fog_logger.info(f"  Queue Time: {queue_time:.2f}s")
        mct_fog_logger.info(f"  Total Time: {task_completion_info[task_name]['total_time']:.2f}s")
        if power_info:
            mct_fog_logger.info(f"  Power Consumption: {power_info.get('total_energy_wh', 0):.6f} Wh")
            mct_fog_logger.info(f"  Average Power: {power_info.get('avg_power_watts', 0):.2f} W")
        mct_fog_logger.info("  " + "-" * 30)
        
        if task_name in queued_tasks_info:
            del queued_tasks_info[task_name]
    
    # Register completion callback for all fog nodes
    for node in fog_nodes.values():
        node.add_completion_callback(task_completion_callback)
    
    # Add a unique ID to each task for tracking
    for i, task in enumerate(tasks):
        task['TaskId'] = f"{task['Name']}_{i}"
    
    # Process each task
    for i, task in enumerate(tasks, 1):
        print(f"\nProcessing Task {i}:")
        print(f"Task Name: {task['Name']}")
        print(f"Task ID: {task['TaskId']}")
        print(f"Size: {task['Size']} MI")
        print(f"MIPS: {task['MIPS']}")
        print(f"RAM: {task['RAM']}")
        print(f"BW: {task['BW']}")
        print(f"Storage: {task.get('Storage', 0)} GB")
        print(f"DataType: {task['DataType']}")
        
        # Calculate distances to all fog nodes
        fog_distances = calculate_task_distances(task)
        
        task_assigned = False

        # Calculate scores for all fog nodes
        fog_scores = {}
        print("\nFog Node Scores:")
        print("-" * 80)
        for fog_name, distance in fog_distances.items():
            fog_node = get_fog_node(fog_name)
            if fog_node:
                score, resource_info, score_breakdown = calculate_fog_node_score(task, fog_node, distance)
                fog_scores[fog_name] = {
                    'score': score,
                    'node': fog_node
                }
                print(f"{fog_name}:")
                print(f"  Final Score: {score:.2f}")
                print(f"  Distance: {distance:.2f} km")
                
                print("\n  Score Breakdown:")
                print(f"    Resource Availability: {score_breakdown['resource_availability']['value']:.2f} (weighted: {score_breakdown['resource_availability']['weighted']:.2f})")
                print(f"    Processing Time: {score_breakdown['processing']['value']:.2f}s (weighted: {score_breakdown['processing']['weighted']:.2f})")
                print(f"    Transmission Time: {score_breakdown['transmission']['value']:.2f}s (weighted: {score_breakdown['transmission']['weighted']:.2f})")
                
                print("\n  Current Resource Availability:")
                print(f"    MIPS: {resource_info['mips']['available']:.2f}/{resource_info['mips']['total']:.2f} ({resource_info['mips']['availability']*100:.1f}%)")
                print(f"    Memory: {resource_info['memory']['available']:.2f}/{resource_info['memory']['total']:.2f} ({resource_info['memory']['availability']*100:.1f}%)")
                print(f"    Bandwidth: {resource_info['bandwidth']['available']:.2f}/{resource_info['bandwidth']['total']:.2f} ({resource_info['bandwidth']['availability']*100:.1f}%)")
                print(f"    Storage: {resource_info['storage']['available']:.2f}/{resource_info['storage']['total']:.2f} ({resource_info['storage']['availability']*100:.1f}%)")
                print("-" * 80)
        
        # Find best fog node (highest score)
        best_fog = None
        if fog_scores:
            best_fog_name = max(fog_scores.items(), key=lambda x: x[1]['score'])[0]
            best_fog = fog_scores[best_fog_name]
        
        # Try to assign to best fog node first
        if best_fog:
            fog_node = best_fog['node']
            mct_fog_logger.info(f"\nAttempting to assign task {task['Name']} to fog node {fog_node.name}")
            
            # Calculate completion time before any assignment
            fog_completion_time = calculate_fog_completion_time(task, fog_node, fog_distances[fog_node.name], task_queue_times)
            
            # Calculate completion times for all fog nodes
            fog_completion_times = {}
            for fog_name, distance in fog_distances.items():
                fog_node_for_completion = get_fog_node(fog_name)
                if fog_node_for_completion:
                    completion_time = calculate_fog_completion_time(task, fog_node_for_completion, distance, task_queue_times)
                    fog_completion_times[fog_name] = {
                        'completion_time': completion_time,
                        'node': fog_node_for_completion
                    }
            
            # Try to assign to fog node with best score first
            success, processing_time = fog_node.assign_task(task)
            
            if success:
                mct_fog_logger.info(f"Task successfully assigned to fog node {fog_node.name}")
                print(f"\nTask assigned to fog node {fog_node.name}")
                print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                print("Fog Node Status:")
                status = fog_node.get_status()
                for key, value in status.items():
                    print(f"  {key}: {value}")
                task_assigned = True
            else:
                # If the best fog node can't handle the task, try to find the node with the minimum completion time
                best_completion_fog_name = min(fog_completion_times.items(), 
                                              key=lambda x: x[1]['completion_time'])[0]
                best_completion_fog = fog_completion_times[best_completion_fog_name]['node']
                
                # Try to assign to fog node with minimum completion time
                success, processing_time = best_completion_fog.assign_task(task)
                
                if success:
                    mct_fog_logger.info(f"Task assigned to minimum completion time fog node {best_completion_fog.name}")
                    print(f"\nTask assigned to fog node {best_completion_fog.name} (minimum completion time)")
                    print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                    print("Fog Node Status:")
                    status = best_completion_fog.get_status()
                    for key, value in status.items():
                        print(f"  {key}: {value}")
                    task_assigned = True
                else:
                    # Queue at the best fog node if task can't be immediately processed
                    mct_fog_logger.info(f"No fog node can immediately process task {task['Name']} - queuing at best fog node {fog_node.name}")
                    print(f"\nTask queued at fog node {fog_node.name}")
                    
                    # Queue task at best fog node
                    task['queue_entry_time'] = time.time()
                    fog_node.task_queue.append(task)
                    task_queue_times[task['Name']] = time.time()
                    
                    queue_position = len(fog_node.task_queue)
                    queued_tasks_info[task['Name']] = {
                        'node': fog_node.name,
                        'distance': fog_distances[fog_node.name],
                        'completion_time': fog_completion_time,
                        'queue_position': queue_position
                    }
                    
                    mct_fog_logger.info(f"Task {task['Name']} queued at fog node {fog_node.name} at position {queue_position}")
                    print(f"Queue position: {queue_position}")
                    task_assigned = True
        
        if not task_assigned:
            mct_fog_logger.error(f"Failed to assign or queue task {task['Name']}")
            print(f"\nError: Could not assign or queue task {task['Name']}")
        
        print("-" * 40)
    
    # Wait for all tasks to complete with a timeout to prevent infinite loops
    max_wait_time = 60  # Maximum wait time in seconds
    wait_start = time.time()
    
    while completed_tasks_count < total_tasks:
        elapsed = time.time() - wait_start
        if elapsed > max_wait_time:
            mct_fog_logger.warning(f"Timeout after {max_wait_time}s. Completed {completed_tasks_count}/{total_tasks} tasks.")
            print(f"\nTimeout after {max_wait_time}s. Completed {completed_tasks_count}/{total_tasks} tasks.")
            break
        time.sleep(0.1)  # Small delay to prevent CPU overuse
    
    # Log final statistics
    if task_completion_info:
        # Calculate total workload (MIPS * processing time for each task)
        total_workload = sum(info['task']['MIPS'] * info.get('processing_time', 0) for info in task_completion_info.values())
        
        # Calculate total size and bandwidth
        total_size = sum(info['task']['Size'] for info in task_completion_info.values())
        total_bandwidth = sum(info['task']['BW'] for info in task_completion_info.values())
        
        # Calculate total power consumption
        total_energy_wh = sum(info.get('power_consumption', {}).get('total_energy_wh', 0) for info in task_completion_info.values())
        total_power_watts = sum(info.get('power_consumption', {}).get('avg_power_watts', 0) for info in task_completion_info.values())
        
        # Calculate Overall Statistics
        total_transmission = sum(info.get('transmission_time', 0) for info in task_completion_info.values())
        total_processing = sum(info.get('processing_time', 0) for info in task_completion_info.values())
        total_queue = sum(info.get('queue_time', 0) for info in task_completion_info.values())
        total_time = sum(info.get('total_time', 0) for info in task_completion_info.values())
        total_storage = sum(info.get('storage_used', 0) for info in task_completion_info.values())
        
        # Calculate workload per fog node
        fog_node_workloads = {}
        fog_node_storage = {}
        fog_node_size = {}
        fog_node_bandwidth = {}
        fog_node_queue_times = {}
        fog_node_energy = {}
        fog_node_power = {}
        
        for info in task_completion_info.values():
            node_name = info['node']
            if node_name not in fog_node_workloads:
                fog_node_workloads[node_name] = 0
                fog_node_storage[node_name] = 0
                fog_node_size[node_name] = 0
                fog_node_bandwidth[node_name] = 0
                fog_node_queue_times[node_name] = []
                fog_node_energy[node_name] = 0
                fog_node_power[node_name] = 0
            
            fog_node_workloads[node_name] += info['task']['MIPS'] * info.get('processing_time', 0)
            fog_node_storage[node_name] += info.get('storage_used', 0)
            fog_node_size[node_name] += info['task']['Size']
            fog_node_bandwidth[node_name] += info['task']['BW']
            fog_node_queue_times[node_name].append(info.get('queue_time', 0))
            fog_node_energy[node_name] += info.get('power_consumption', {}).get('total_energy_wh', 0)
            fog_node_power[node_name] += info.get('power_consumption', {}).get('avg_power_watts', 0)
        
        # Calculate averages for overall statistics
        avg_transmission = total_transmission / len(task_completion_info) if task_completion_info else 0
        avg_processing = total_processing / len(task_completion_info) if task_completion_info else 0
        avg_queue = total_queue / len(task_completion_info) if task_completion_info else 0
        avg_total = total_time / len(task_completion_info) if task_completion_info else 0
        avg_storage = total_storage / len(task_completion_info) if task_completion_info else 0
        avg_workload = total_workload / len(task_completion_info) if task_completion_info else 0
        avg_size = total_size / len(task_completion_info) if task_completion_info else 0
        avg_bandwidth = total_bandwidth / len(task_completion_info) if task_completion_info else 0
        avg_energy = total_energy_wh / len(task_completion_info) if task_completion_info else 0
        avg_power = total_power_watts / len(task_completion_info) if task_completion_info else 0
        
        # Log Overall Statistics
        mct_fog_logger.info("\n=== Overall Statistics ===")
        mct_fog_logger.info(f"Total Tasks: {len(task_completion_info)}")
        mct_fog_logger.info(f"  Completed Tasks: {completed_tasks_count}")
        mct_fog_logger.info(f"  Queued Tasks Remaining: {len(queued_tasks_info)}")
        mct_fog_logger.info("\nWorkload:")
        mct_fog_logger.info(f"  Total System Workload: {total_workload:.2f} MIPS-seconds")
        mct_fog_logger.info(f"  Average Workload per Task: {avg_workload:.2f} MIPS-seconds")
        mct_fog_logger.info("\nResource Usage:")
        mct_fog_logger.info(f"  Total Storage Used: {total_storage:.2f} GB")
        mct_fog_logger.info(f"  Total Data Size: {total_size:.2f} MI")
        mct_fog_logger.info(f"  Total Bandwidth Used: {total_bandwidth:.2f} Mbps")
        mct_fog_logger.info(f"  Average Storage per Task: {avg_storage:.2f} GB")
        mct_fog_logger.info(f"  Average Size per Task: {avg_size:.2f} MI")
        mct_fog_logger.info(f"  Average Bandwidth per Task: {avg_bandwidth:.2f} Mbps")
        mct_fog_logger.info("\nPower Consumption:")
        mct_fog_logger.info(f"  Total Energy Consumed: {total_energy_wh:.6f} Wh")
        mct_fog_logger.info(f"  Total Power Used: {total_power_watts:.2f} W")
        mct_fog_logger.info(f"  Average Energy per Task: {avg_energy:.6f} Wh")
        mct_fog_logger.info(f"  Average Power per Task: {avg_power:.2f} W")
        mct_fog_logger.info("\nTransmission Time:")
        mct_fog_logger.info(f"  Total: {total_transmission*1000:.6f}ms")
        mct_fog_logger.info(f"  Average: {avg_transmission*1000:.6f}ms")
        mct_fog_logger.info("\nProcessing Time:")
        mct_fog_logger.info(f"  Total: {total_processing*1000:.6f}ms")
        mct_fog_logger.info(f"  Average: {avg_processing*1000:.6f}ms")
        mct_fog_logger.info("\nQueue Time:")
        mct_fog_logger.info(f"  Total: {total_queue*1000:.6f}ms")
        mct_fog_logger.info(f"  Average: {avg_queue*1000:.6f}ms")
        mct_fog_logger.info("\nTotal Time:")
        mct_fog_logger.info(f"  Total: {total_time*1000:.6f}ms")
        mct_fog_logger.info(f"  Average: {avg_total*1000:.6f}ms")
        
        # Log Per Node Statistics
        mct_fog_logger.info("\n=== Per Fog Node Statistics ===")
        for node_name in fog_node_workloads:
            mct_fog_logger.info(f"\n  {node_name}:")
            mct_fog_logger.info(f"    Workload: {fog_node_workloads[node_name]:.2f} MIPS-seconds")
            mct_fog_logger.info(f"    Storage Used: {fog_node_storage[node_name]:.2f} GB")
            mct_fog_logger.info(f"    Data Size: {fog_node_size[node_name]:.2f} MI")
            mct_fog_logger.info(f"    Bandwidth Used: {fog_node_bandwidth[node_name]:.2f} Mbps")
            mct_fog_logger.info(f"    Energy Consumed: {fog_node_energy[node_name]:.6f} Wh")
            mct_fog_logger.info(f"    Power Used: {fog_node_power[node_name]:.2f} W")
            if fog_node_queue_times[node_name]:
                avg_queue_time = sum(fog_node_queue_times[node_name]) / len(fog_node_queue_times[node_name])
                mct_fog_logger.info(f"    Average Queue Time: {avg_queue_time*1000:.6f}ms")
    else:
        # If no tasks were completed, still log basic statistics
        mct_fog_logger.info("\n=== Overall Statistics ===")
        mct_fog_logger.info("No tasks were completed during the execution.")

if __name__ == "__main__":
    # Load tasks from input
    tasks = read_and_log_tuples()
    
    # Print debug information
    print(f"\nDebug: Number of tasks received: {len(tasks)}")
    mct_fog_logger.info(f"Number of tasks received: {len(tasks)}")
    
    # Process tasks using MCT Fog-Only algorithm
    if tasks:
        process_mct(tasks)
    else:
        print("Error: No tasks were loaded")
        mct_fog_logger.error("No tasks were loaded.")
