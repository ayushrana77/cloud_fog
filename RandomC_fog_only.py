"""
RandomC Fog-Only Task Scheduling Implementation

This module implements a fog-only variant of the RandomC algorithm that prioritizes
cooperative resource sharing between fog nodes. All tasks are either assigned
to fog nodes immediately or queued at fog nodes for later processing, with no
cloud fallback.
"""

import json
import time
import random
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG
from utility import calculate_distance, calculate_storage_requirements, calculate_transmission_time, calculate_power_consumption
from fog_with_queue import get_fog_node, get_fog_node_status, get_all_fog_nodes
from logger import setup_logger

# Initialize logger for RandomC Fog-Only events
randomc_fog_logger = setup_logger('randomc_fog_only', 'randomc_fog_only.log', sub_directory='algorithms')

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

def calculate_processing_time(task_size, node_mips):
    """
    Calculate processing time using RandomC's formula with overheads
    """
    # Base processing time
    base_time = task_size / node_mips
    
    # Add CPU overhead with random variation (20-40% of base time)
    cpu_overhead = base_time * random.uniform(0.2, 0.4)
    
    # Add memory access time with random variation (15-30% of base time)
    memory_access = base_time * random.uniform(0.15, 0.3)
    
    # Add system load factor with random variation (10-25% of base time)
    system_load = base_time * random.uniform(0.1, 0.25)
    
    # Add cache miss penalty with random variation (5-15% of base time)
    cache_miss = base_time * random.uniform(0.05, 0.15)
    
    # Add I/O wait time with random variation (5-20% of base time)
    io_wait = base_time * random.uniform(0.05, 0.2)
    
    # Total processing time with all factors
    total_time = base_time + cpu_overhead + memory_access + system_load + cache_miss + io_wait
    
    # Add some random variation (±15%) to make it more realistic
    variation = random.uniform(0.85, 1.15)
    total_time *= variation
    
    return total_time

def process_fcfs(tasks):
    """
    Process tasks using RandomC algorithm with fog-only approach
    
    This function implements the main RandomC Fog-Only scheduling logic:
    1. Tasks are sorted by creation time
    2. Each task is assigned to fog nodes based on proximity
    3. Cooperative resource sharing is implemented between fog nodes
    4. Tasks that cannot be immediately processed are queued at fog nodes
    5. Performance metrics are tracked and logged
    
    Args:
        tasks (list): List of tasks to be processed
    """
    if not tasks:
        print("No tasks to process")
        randomc_fog_logger.warning("No tasks to process.")
        return
        
    print("\n=== RandomC Fog-Only Processing ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    randomc_fog_logger.info(f"Total Tasks to Process: {len(tasks)}")
    
    # Initialize and log fog nodes
    fog_nodes = get_all_fog_nodes()
    for name, node in fog_nodes.items():
        randomc_fog_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Location={node.location})")
    
    # Initialize tracking variables
    task_completion_info = {}  # Store completion details for each task
    queued_tasks_info = {}     # Track tasks waiting in queue
    completed_tasks_count = 0  # Counter for completed tasks
    total_tasks = len(tasks)   # Total number of tasks to process
    task_queue_times = {}      # Track when tasks enter queue
    processed_tasks = set()    # Track processed tasks to avoid duplicates
    
    def task_completion_callback(node_name, completion_info):
        """
        Callback function to handle task completion events
        
        Args:
            node_name (str): Name of the node that completed the task
            completion_info (dict): Information about the completed task
        """
        nonlocal completed_tasks_count
        task = completion_info['task']
        task_name = task['Name']
        task_id = task.get('TaskId', task_name)  # Use TaskId if available, otherwise use Name
        
        # Skip if this task has already been processed (prevent duplicates)
        if task_id in processed_tasks:
            randomc_fog_logger.debug(f"Task {task_name} with ID {task_id} already processed, skipping duplicate")
            return
            
        processed_tasks.add(task_id)
        
        # Calculate queue time if task was queued
        queue_time = 0
        if task_name in task_queue_times:
            queue_time = time.time() - task_queue_times[task_name]
            del task_queue_times[task_name]
        
        # Get power consumption information
        power_info = completion_info.get('power_consumption', {})
        
        # Store completion information with unique key
        task_completion_info[task_id] = {
            'node': node_name,
            'transmission_time': completion_info.get('transmission_time', 0),
            'queue_time': completion_info.get('queue_time', 0) or queue_time,
            'processing_time': completion_info.get('processing_time', 0),
            'total_time': completion_info.get('total_time', 0) + queue_time,
            'completion_time': completion_info.get('completion_time', 0),
            'storage_used': task.get('Storage', 0),
            'power_consumption': power_info,  # Add power consumption information
            'task': task  # Store the complete task information
        }
        
        # Update completion counter and logging
        completed_tasks_count += 1
        randomc_fog_logger.info(f"\nTask Completed: {task_name}")
        randomc_fog_logger.info(f"  Completed at: {node_name}")
        randomc_fog_logger.info(f"  Transmission Time: {completion_info.get('transmission_time', 0):.2f}s")
        randomc_fog_logger.info(f"  Queue Time: {task_completion_info[task_id]['queue_time']:.2f}s")
        randomc_fog_logger.info(f"  Processing Time: {completion_info.get('processing_time', 0):.2f}s")
        randomc_fog_logger.info(f"  Storage Used: {task.get('Storage', 0):.2f}GB")
        randomc_fog_logger.info(f"  Total Time: {task_completion_info[task_id]['total_time']:.2f}s")
        if power_info:
            randomc_fog_logger.info(f"  Power Consumption: {power_info.get('total_energy_wh', 0):.6f} Wh")
            randomc_fog_logger.info(f"  Average Power: {power_info.get('avg_power_watts', 0):.2f} W")
        randomc_fog_logger.info("  " + "-" * 30)
        
        if task_name in queued_tasks_info:
            del queued_tasks_info[task_name]
    
    # Register completion callback for all fog nodes
    for node in fog_nodes.values():
        node.add_completion_callback(task_completion_callback)
    
    # Sort tasks by creation time (FCFS order)
    sorted_tasks = sorted(tasks, key=lambda x: x['CreationTime'])
    
    # Assign a unique ID to each task to prevent duplicate processing
    for i, task in enumerate(sorted_tasks):
        task['TaskId'] = f"{task['Name']}_{i}"
    
    # Process each task in order
    for i, task in enumerate(sorted_tasks, 1):
        # Calculate storage requirement for the task
        task['Storage'] = calculate_storage_requirements(task['Size'])
        
        # Log task details
        print(f"\nProcessing Task {i}:")
        print(f"Task Name: {task['Name']}")
        print(f"Task ID: {task['TaskId']}")
        print(f"Creation Time: {task['CreationTime']}")
        print(f"Size: {task['Size']} MI")
        print(f"MIPS: {task['MIPS']}")
        print(f"RAM: {task['RAM']}")
        print(f"BW: {task['BW']}")
        print(f"Storage: {task['Storage']}GB")
        print(f"DataType: {task['DataType']}")
        print(f"DeviceType: {task['DeviceType']}")
        
        # Calculate distances to fog nodes
        fog_distances = calculate_task_distances(task)
        
        # Sort fog nodes by distance for optimal assignment
        sorted_fog_nodes = sorted(fog_distances.items(), key=lambda x: x[1])
        
        # Log distance calculations
        randomc_fog_logger.info(f"\nTask {task['Name']} - Distance Calculations:")
        randomc_fog_logger.info("Fog Node Distances:")
        for fog_name, distance in sorted_fog_nodes:
            randomc_fog_logger.info(f"  {fog_name}: {distance:.2f} km")
        
        # Task assignment logic - try first fog node
        task_assigned = False
        if len(sorted_fog_nodes) > 0:
            first_fog_name, first_fog_distance = sorted_fog_nodes[0]
            fog_node = get_fog_node(first_fog_name)
            if fog_node:
                randomc_fog_logger.info(f"\nAttempting to assign task {task['Name']} to fog node {first_fog_name} ({first_fog_distance:.2f} km)")
                # Check all resources before assignment
                if fog_node.can_handle_task(task):
                    success, processing_time = fog_node.assign_task(task)
                    if success:
                        # Task successfully assigned to fog node
                        randomc_fog_logger.info(f"Task successfully assigned to fog node {first_fog_name}")
                        print(f"\nTask assigned to fog node {first_fog_name} ({first_fog_distance:.2f} km)")
                        print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                        print("Fog Node Status:")
                        status = fog_node.get_status()
                        for key, value in status.items():
                            print(f"  {key}: {value}")
                        task_assigned = True
                    else:
                        # Queue at first fog node if can't be assigned immediately
                        randomc_fog_logger.info(f"Fog node {first_fog_name} cannot process immediately - queuing")
                        print(f"\nFog node {first_fog_name} is busy - task queued")
                        # Set queue entry time for tracking
                        task['queue_entry_time'] = time.time()
                        # Direct access to the queue 
                        fog_node.task_queue.append(task)
                        task_queue_times[task['Name']] = time.time()
                        
                        queue_position = len(fog_node.task_queue)
                        queued_tasks_info[task['Name']] = {
                            'node': first_fog_name,
                            'distance': first_fog_distance,
                            'queue_position': queue_position,
                            'task_size': task['Size'],
                            'required_mips': task['MIPS'],
                            'required_ram': task['RAM'],
                            'required_bw': task['BW'],
                            'required_storage': task['Storage']
                        }
                        
                        randomc_fog_logger.info(f"Task {task['Name']} queued at fog node {first_fog_name} at position {queue_position}")
                        print(f"Queue position: {queue_position}")
                        task_assigned = True
                else:
                    randomc_fog_logger.info(f"Fog node {first_fog_name} does not have sufficient resources - trying next node")
                    print(f"\nFog node {first_fog_name} does not have sufficient resources")
            
            # Try second fog node if available and first one failed
            if not task_assigned and len(sorted_fog_nodes) > 1:
                second_fog_name, second_fog_distance = sorted_fog_nodes[1]
                fog_node = get_fog_node(second_fog_name)
                if fog_node:
                    randomc_fog_logger.info(f"Attempting to assign task to fog node {second_fog_name} ({second_fog_distance:.2f} km)")
                    # Check all resources before assignment
                    if fog_node.can_handle_task(task):
                        success, processing_time = fog_node.assign_task(task)
                        if success:
                            # Task successfully assigned to second fog node
                            randomc_fog_logger.info(f"Task successfully assigned to fog node {second_fog_name}")
                            print(f"Task assigned to fog node {second_fog_name} ({second_fog_distance:.2f} km)")
                            print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                            print("Fog Node Status:")
                            status = fog_node.get_status()
                            for key, value in status.items():
                                print(f"  {key}: {value}")
                            task_assigned = True
                        else:
                            # Queue at second fog node if can't be assigned immediately
                            randomc_fog_logger.info(f"Second fog node {second_fog_name} cannot process immediately - queuing")
                            print(f"\nSecond fog node {second_fog_name} is busy - task queued")
                            # Set queue entry time for tracking
                            task['queue_entry_time'] = time.time()
                            # Direct access to the queue
                            fog_node.task_queue.append(task)
                            task_queue_times[task['Name']] = time.time()
                            
                            queue_position = len(fog_node.task_queue)
                            queued_tasks_info[task['Name']] = {
                                'node': second_fog_name,
                                'distance': second_fog_distance,
                                'queue_position': queue_position,
                                'task_size': task['Size'],
                                'required_mips': task['MIPS'],
                                'required_ram': task['RAM'],
                                'required_bw': task['BW'],
                                'required_storage': task['Storage']
                            }
                            
                            randomc_fog_logger.info(f"Task {task['Name']} queued at fog node {second_fog_name} at position {queue_position}")
                            print(f"Queue position: {queue_position}")
                            task_assigned = True
                    else:
                        randomc_fog_logger.info(f"Second fog node {second_fog_name} does not have sufficient resources")
                        print(f"\nSecond fog node {second_fog_name} does not have sufficient resources")
            
            # If both preferred fog nodes failed, try any remaining fog node that can handle the task
            if not task_assigned and len(sorted_fog_nodes) > 2:
                for fog_name, fog_distance in sorted_fog_nodes[2:]:
                    fog_node = get_fog_node(fog_name)
                    if fog_node:
                        randomc_fog_logger.info(f"Attempting to assign task to alternative fog node {fog_name} ({fog_distance:.2f} km)")
                        if fog_node.can_handle_task(task):
                            success, processing_time = fog_node.assign_task(task)
                            if success:
                                # Task successfully assigned to alternative fog node
                                randomc_fog_logger.info(f"Task successfully assigned to alternative fog node {fog_name}")
                                print(f"\nTask assigned to alternative fog node {fog_name} ({fog_distance:.2f} km)")
                                print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                                print("Fog Node Status:")
                                status = fog_node.get_status()
                                for key, value in status.items():
                                    print(f"  {key}: {value}")
                                task_assigned = True
                                break
                            else:
                                # Queue at alternative fog node
                                randomc_fog_logger.info(f"Alternative fog node {fog_name} cannot process immediately - queuing")
                                print(f"\nAlternative fog node {fog_name} is busy - task queued")
                                # Set queue entry time for tracking
                                task['queue_entry_time'] = time.time()
                                # Direct access to the queue
                                fog_node.task_queue.append(task)
                                task_queue_times[task['Name']] = time.time()
                                
                                queue_position = len(fog_node.task_queue)
                                queued_tasks_info[task['Name']] = {
                                    'node': fog_name,
                                    'distance': fog_distance,
                                    'queue_position': queue_position,
                                    'task_size': task['Size'],
                                    'required_mips': task['MIPS'],
                                    'required_ram': task['RAM'],
                                    'required_bw': task['BW'],
                                    'required_storage': task['Storage']
                                }
                                
                                randomc_fog_logger.info(f"Task {task['Name']} queued at fog node {fog_name} at position {queue_position}")
                                print(f"Queue position: {queue_position}")
                                task_assigned = True
                                break
                        else:
                            randomc_fog_logger.info(f"Alternative fog node {fog_name} does not have sufficient resources")
            
            # If all direct assignments failed, queue the task at the nearest fog node
            if not task_assigned:
                nearest_fog_name, nearest_fog_distance = sorted_fog_nodes[0]
                fog_node = get_fog_node(nearest_fog_name)
                if fog_node:
                    randomc_fog_logger.info(f"No fog node can handle the task directly - queuing at nearest fog node {nearest_fog_name}")
                    print(f"\nNo fog node can handle the task directly - queuing at nearest fog node {nearest_fog_name}")
                    # Set queue entry time for tracking
                    task['queue_entry_time'] = time.time()
                    # Direct access to the queue
                    fog_node.task_queue.append(task)
                    task_queue_times[task['Name']] = time.time()
                    
                    queue_position = len(fog_node.task_queue)
                    queued_tasks_info[task['Name']] = {
                        'node': nearest_fog_name,
                        'distance': nearest_fog_distance,
                        'queue_position': queue_position,
                        'task_size': task['Size'],
                        'required_mips': task['MIPS'],
                        'required_ram': task['RAM'],
                        'required_bw': task['BW'],
                        'required_storage': task['Storage']
                    }
                    
                    randomc_fog_logger.info(f"Task {task['Name']} queued at nearest fog node {nearest_fog_name} at position {queue_position}")
                    print(f"Queue position: {queue_position}")
                    task_assigned = True
        
        # Log assignment failure if task couldn't be assigned or queued
        if not task_assigned:
            randomc_fog_logger.error(f"Failed to assign or queue task {task['Name']}")
            print(f"\nError: Could not assign or queue task {task['Name']}")
        
        print("-" * 40)
    
    # Wait for all tasks to complete with timeout
    wait_start = time.time()
    max_wait_time = 60  # Maximum wait time in seconds
    
    while completed_tasks_count < total_tasks:
        current_time = time.time()
        elapsed = current_time - wait_start
        
        if elapsed > max_wait_time:
            randomc_fog_logger.warning(f"Timed out after {max_wait_time} seconds. Completed {completed_tasks_count}/{total_tasks} tasks.")
            print(f"\nTimed out after {max_wait_time} seconds. Completed {completed_tasks_count}/{total_tasks} tasks.")
            break
            
        # Show progress every 5 seconds
        if int(elapsed) % 5 == 0 and int(elapsed) > 0:
            print(f"\rWaiting for tasks to complete: {completed_tasks_count}/{total_tasks} completed ({elapsed:.1f}s elapsed)", end="")
        
        time.sleep(0.1)  # Small delay to prevent CPU overuse
    
    # Log queued tasks information
    if queued_tasks_info:
        randomc_fog_logger.info("\n=== Queued Tasks Summary ===")
        for task_name, info in queued_tasks_info.items():
            randomc_fog_logger.info(f"\nQueued Task: {task_name}")
            randomc_fog_logger.info(f"  Queued at: {info['node']} (distance={info['distance']:.2f} km)")
            randomc_fog_logger.info(f"  Queue Position: {info['queue_position']}")
            randomc_fog_logger.info(f"  Task Size: {info['task_size']} MI")
            randomc_fog_logger.info(f"  Required Resources:")
            randomc_fog_logger.info(f"    MIPS: {info['required_mips']}")
            randomc_fog_logger.info(f"    RAM: {info['required_ram']}")
            randomc_fog_logger.info(f"    Bandwidth: {info['required_bw']}")
            randomc_fog_logger.info(f"    Storage: {info['required_storage']}GB")
    
    # Log final statistics
    if task_completion_info:
        # Calculate fog statistics (all tasks are fog tasks in this implementation)
        fog_tasks = list(task_completion_info.values())
        
        # Calculate total workload (MIPS * processing time for each task)
        total_workload = sum(info['task']['MIPS'] * info.get('processing_time', 0) for info in fog_tasks)
        
        # Calculate total size and bandwidth
        total_size = sum(info['task']['Size'] for info in fog_tasks)
        total_bandwidth = sum(info['task']['BW'] for info in fog_tasks)
        
        # Calculate total power consumption
        total_energy_wh = sum(info.get('power_consumption', {}).get('total_energy_wh', 0) for info in fog_tasks)
        total_power_watts = sum(info.get('power_consumption', {}).get('avg_power_watts', 0) for info in fog_tasks)
        
        # Calculate Overall Statistics
        total_transmission = sum(info.get('transmission_time', 0) for info in fog_tasks)
        total_processing = sum(info.get('processing_time', 0) for info in fog_tasks)
        total_queue = sum(info.get('queue_time', 0) for info in fog_tasks)
        total_time = sum(info.get('total_time', 0) for info in fog_tasks)
        total_storage = sum(info.get('storage_used', 0) for info in fog_tasks)
        
        # Calculate workload per fog node
        fog_node_workloads = {}
        fog_node_storage = {}
        fog_node_size = {}
        fog_node_bandwidth = {}
        fog_node_energy = {}
        fog_node_power = {}
        
        for info in fog_tasks:
            node_name = info['node']
            if node_name not in fog_node_workloads:
                fog_node_workloads[node_name] = 0
                fog_node_storage[node_name] = 0
                fog_node_size[node_name] = 0
                fog_node_bandwidth[node_name] = 0
                fog_node_energy[node_name] = 0
                fog_node_power[node_name] = 0
            
            fog_node_workloads[node_name] += info['task']['MIPS'] * info.get('processing_time', 0)
            fog_node_storage[node_name] += info.get('storage_used', 0)
            fog_node_size[node_name] += info['task']['Size']
            fog_node_bandwidth[node_name] += info['task']['BW']
            fog_node_energy[node_name] += info.get('power_consumption', {}).get('total_energy_wh', 0)
            fog_node_power[node_name] += info.get('power_consumption', {}).get('avg_power_watts', 0)
        
        # Calculate averages
        avg_transmission = total_transmission / len(fog_tasks) if fog_tasks else 0
        avg_processing = total_processing / len(fog_tasks) if fog_tasks else 0
        avg_queue = total_queue / len(fog_tasks) if fog_tasks else 0
        avg_total = total_time / len(fog_tasks) if fog_tasks else 0
        avg_storage = total_storage / len(fog_tasks) if fog_tasks else 0
        avg_workload = total_workload / len(fog_tasks) if fog_tasks else 0
        avg_size = total_size / len(fog_tasks) if fog_tasks else 0
        avg_bandwidth = total_bandwidth / len(fog_tasks) if fog_tasks else 0
        avg_energy = total_energy_wh / len(fog_tasks) if fog_tasks else 0
        avg_power = total_power_watts / len(fog_tasks) if fog_tasks else 0
        
        # Log Overall Statistics
        randomc_fog_logger.info("\n=== Overall Statistics ===")
        randomc_fog_logger.info(f"Total Tasks: {len(task_completion_info)}")
        randomc_fog_logger.info(f"  Fog Tasks: {len(fog_tasks)}")
        randomc_fog_logger.info("\nWorkload:")
        randomc_fog_logger.info(f"  Total System Workload: {total_workload:.2f} MIPS-seconds")
        randomc_fog_logger.info(f"  Average Workload per Task: {avg_workload:.2f} MIPS-seconds")
        randomc_fog_logger.info("\nResource Usage:")
        randomc_fog_logger.info(f"  Total Storage Used: {total_storage:.2f} GB")
        randomc_fog_logger.info(f"  Total Data Size: {total_size:.2f} MI")
        randomc_fog_logger.info(f"  Total Bandwidth Used: {total_bandwidth:.2f} Mbps")
        randomc_fog_logger.info(f"  Average Storage per Task: {avg_storage:.2f} GB")
        randomc_fog_logger.info(f"  Average Size per Task: {avg_size:.2f} MI")
        randomc_fog_logger.info(f"  Average Bandwidth per Task: {avg_bandwidth:.2f} Mbps")
        randomc_fog_logger.info("\nPower Consumption:")
        randomc_fog_logger.info(f"  Total Energy Consumed: {total_energy_wh:.6f} Wh")
        randomc_fog_logger.info(f"  Total Power Used: {total_power_watts:.2f} W")
        randomc_fog_logger.info(f"  Average Energy per Task: {avg_energy:.6f} Wh")
        randomc_fog_logger.info(f"  Average Power per Task: {avg_power:.2f} W")
        randomc_fog_logger.info("\nTransmission Time:")
        randomc_fog_logger.info(f"  Total: {total_transmission*1000:.6f}ms")
        randomc_fog_logger.info(f"  Average: {avg_transmission*1000:.6f}ms")
        randomc_fog_logger.info("\nProcessing Time:")
        randomc_fog_logger.info(f"  Total: {total_processing*1000:.6f}ms")
        randomc_fog_logger.info(f"  Average: {avg_processing*1000:.6f}ms")
        randomc_fog_logger.info("\nQueue Time:")
        randomc_fog_logger.info(f"  Total: {total_queue*1000:.6f}ms")
        randomc_fog_logger.info(f"  Average: {avg_queue*1000:.6f}ms")
        randomc_fog_logger.info("\nTotal Time:")
        randomc_fog_logger.info(f"  Total: {total_time*1000:.6f}ms")
        randomc_fog_logger.info(f"  Average: {avg_total*1000:.6f}ms")
        
        # Log Fog Node Statistics
        randomc_fog_logger.info("\n=== Per Fog Node Statistics ===")
        for node_name in fog_node_workloads:
            randomc_fog_logger.info(f"\n  {node_name}:")
            randomc_fog_logger.info(f"    Workload: {fog_node_workloads[node_name]:.2f} MIPS-seconds")
            randomc_fog_logger.info(f"    Storage Used: {fog_node_storage[node_name]:.2f} GB")
            randomc_fog_logger.info(f"    Data Size: {fog_node_size[node_name]:.2f} MI")
            randomc_fog_logger.info(f"    Bandwidth Used: {fog_node_bandwidth[node_name]:.2f} Mbps")
            randomc_fog_logger.info(f"    Energy Consumed: {fog_node_energy[node_name]:.6f} Wh")
            randomc_fog_logger.info(f"    Power Used: {fog_node_power[node_name]:.2f} W")
    else:
        # If no tasks were completed, still log basic statistics
        randomc_fog_logger.info("\n=== Overall Statistics ===")
        randomc_fog_logger.info("No tasks were completed during the execution.")

if __name__ == "__main__":
    # Load tasks from input
    tasks = read_and_log_tuples()
    
    # Print debug information
    print(f"\nDebug: Number of tasks received: {len(tasks)}")
    randomc_fog_logger.info(f"Number of tasks received: {len(tasks)}")
    
    # Process tasks using RandomC Fog-Only algorithm
    if tasks:
        process_fcfs(tasks)
    else:
        print("Error: No tasks were loaded")
        randomc_fog_logger.error("No tasks were loaded.")
