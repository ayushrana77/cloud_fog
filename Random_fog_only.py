"""
Random Task Scheduling Implementation (Fog Only)
This module implements a fog-only task scheduling algorithm using the Random approach.
Tasks are processed with random variations in processing times, with consideration for fog resources only.
"""

import json
import time
import random
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG
from utility import calculate_distance, calculate_storage_requirements, calculate_transmission_time, calculate_power_consumption
from fog_with_queue import get_fog_node, get_fog_node_status, get_all_fog_nodes
from logger import setup_logger

# Initialize logger for Random Fog Only events
random_fog_logger = setup_logger('random_fog_only', 'random_fog_only.log', sub_directory='algorithms')

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
    
    # Calculate distances to fog nodes
    for node in FOG_NODES_CONFIG:
        node_location = node['location']
        distance = calculate_distance(task_location, node_location)
        distances[node['name']] = distance
    
    return distances

def calculate_processing_time(task_size, node_mips):
    """
    Calculate processing time using Random's formula with overheads
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
    Process tasks using Random algorithm with fog-only approach
    
    This function implements the main Random scheduling logic:
    1. Tasks are sorted by creation time
    2. Each task is assigned to nearest available fog node
    3. Resources are allocated and tasks are processed with random variations
    4. Performance metrics are tracked and logged
    
    Args:
        tasks (list): List of tasks to be processed
    """
    if not tasks:
        print("No tasks to process")
        random_fog_logger.warning("No tasks to process.")
        return
        
    print("\n=== Random Processing (Fog Only) ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    random_fog_logger.info(f"Total Tasks to Process: {len(tasks)}")
    
    # Initialize and log fog nodes
    fog_nodes = get_all_fog_nodes()
    for name, node in fog_nodes.items():
        random_fog_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Storage={node.storage}GB)")
    
    # Initialize tracking variables
    task_completion_info = {}  # Store completion details for each task
    queued_tasks_info = {}     # Track tasks waiting in queue
    completed_tasks_count = 0  # Counter for completed tasks
    total_tasks = len(tasks)   # Total number of tasks to process
    task_queue_times = {}      # Track task queue times
    
    # Track unique task IDs to prevent duplicate counting
    processed_task_ids = set()
    
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
        task_id = task.get('ID', task_name)  # Use ID if available, otherwise name
        
        # Skip if this task has already been processed (prevent duplicates)
        if task_id in processed_task_ids:
            random_fog_logger.debug(f"Task {task_name} with ID {task_id} already processed, skipping duplicate")
            return
            
        processed_task_ids.add(task_id)
        
        # Calculate queue time if task was queued
        queue_time = 0
        if task_name in task_queue_times:
            queue_time = time.time() - task_queue_times[task_name]
            del task_queue_times[task_name]
        
        # Get power consumption information
        power_info = completion_info.get('power_consumption', {})
        
        # Store completion information with unique key (using ID if available)
        task_completion_info[task_id] = {
            'node': node_name,
            'transmission_time': completion_info.get('transmission_time', 0),
            'queue_time': queue_time,
            'processing_time': completion_info.get('processing_time', 0),
            'total_time': completion_info.get('total_time', 0) + queue_time,
            'completion_time': completion_info.get('completion_time', 0),
            'storage_used': task.get('Storage', 0),
            'power_consumption': power_info,  # Add power consumption information
            'task': task,  # Store the complete task information
            'task_name': task_name  # Store task name separately for reporting
        }
        
        # Update completion counter and logging
        completed_tasks_count += 1
        random_fog_logger.info(f"\nTask Completed: {task_name}")
        random_fog_logger.info(f"  Completed at: {node_name}")
        random_fog_logger.info(f"  Transmission Time: {completion_info.get('transmission_time', 0):.2f}s")
        random_fog_logger.info(f"  Queue Time: {queue_time:.2f}s")
        random_fog_logger.info(f"  Processing Time: {completion_info.get('processing_time', 0):.2f}s")
        random_fog_logger.info(f"  Storage Used: {task.get('Storage', 0):.2f}GB")
        random_fog_logger.info(f"  Total Time: {task_completion_info[task_id]['total_time']:.2f}s")
        if power_info:
            random_fog_logger.info(f"  Power Consumption: {power_info.get('total_energy_wh', 0):.6f} Wh")
            random_fog_logger.info(f"  Average Power: {power_info.get('avg_power_watts', 0):.2f} W")
        random_fog_logger.info("  " + "-" * 30)
        
        # Remove from queued tasks if it was queued
        if task_name in queued_tasks_info:
            del queued_tasks_info[task_name]
    
    # Register completion callback for all fog nodes
    for node in fog_nodes.values():
        node.add_completion_callback(task_completion_callback)
    
    # Sort tasks by creation time (FCFS order)
    sorted_tasks = sorted(tasks, key=lambda x: x['CreationTime'])
    
    # Process each task in order
    for i, task in enumerate(sorted_tasks, 1):
        # Calculate storage requirement for the task
        task['Storage'] = calculate_storage_requirements(task['Size'])
        
        # Log task details
        print(f"\nProcessing Task {i}:")
        print(f"Task Name: {task['Name']}")
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
        random_fog_logger.info(f"\nTask {task['Name']} - Distance Calculations:")
        random_fog_logger.info("Fog Node Distances:")
        for fog_name, distance in sorted_fog_nodes:
            random_fog_logger.info(f"  {fog_name}: {distance:.2f} km")
        
        # Task assignment logic
        task_assigned = False
        
        # Try to assign the task to fog nodes in order of distance
        for fog_name, distance in sorted_fog_nodes:
            fog_node = get_fog_node(fog_name)
            if fog_node:
                # Calculate current usage percentages based on task requirements
                mips_usage = (task['MIPS'] / fog_node.mips) * 100 if fog_node.mips > 0 else 0
                mem_usage = (task['RAM'] / fog_node.memory) * 100 if fog_node.memory > 0 else 0
                bw_usage = (task['BW'] / fog_node.bandwidth) * 100 if fog_node.bandwidth > 0 else 0
                storage_usage = (task['Storage'] / fog_node.storage) * 100 if fog_node.storage > 0 else 0
                
                # Calculate overall load as the maximum resource usage
                current_load = max(mips_usage, mem_usage, bw_usage, storage_usage)
                
                random_fog_logger.info(f"\nAttempting to assign task {task['Name']} to fog node {fog_name} ({distance:.2f} km)")
                random_fog_logger.info(f"Resources required: MIPS={task['MIPS']}/{fog_node.mips}, Memory={task['RAM']}/{fog_node.memory}, Bandwidth={task['BW']}/{fog_node.bandwidth}, Storage={task['Storage']}/{fog_node.storage}")
                random_fog_logger.info(f"Resource usage: MIPS={mips_usage:.2f}%, Memory={mem_usage:.2f}%, Bandwidth={bw_usage:.2f}%, Storage={storage_usage:.2f}%")
                random_fog_logger.info(f"Current load: {current_load:.2f}%")
                
                success, processing_time = fog_node.assign_task(task)
                if success:
                    # Task successfully assigned to fog node
                    random_fog_logger.info(f"Task successfully assigned to fog node {fog_name}")
                    print(f"\nTask assigned to fog node {fog_name} ({distance:.2f} km)")
                    print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                    print(f"Current load: {current_load:.2f}%")
                    print("Fog Node Status:")
                    status = fog_node.get_status()
                    for key, value in status.items():
                        print(f"  {key}: {value}")
                    task_assigned = True
                    break
                else:
                    # Try to queue the task at this fog node using fog_with_queue functionality
                    random_fog_logger.info(f"Fog node {fog_name} cannot process task immediately - queueing")
                    print(f"\nFog node {fog_name} is busy. Task {task['Name']} queued.")
                    # Set queue entry time for tracking
                    task['queue_entry_time'] = time.time()
                    # Direct access to the queue since queue_task method doesn't exist
                    fog_node.task_queue.append(task)
                    task_queue_times[task['Name']] = time.time()
                    
                    queue_position = len(fog_node.task_queue)
                    queued_tasks_info[task['Name']] = {
                        'node': fog_name,
                        'distance': distance,
                        'queue_position': queue_position,
                        'task_size': task['Size'],
                        'required_mips': task['MIPS'],
                        'required_ram': task['RAM'],
                        'required_bw': task['BW'],
                        'required_storage': task['Storage']
                    }
                    
                    # Get updated node info for display
                    node_status = fog_node.get_status()
                    current_load = fog_node.current_load
                    
                    random_fog_logger.info(f"Task {task['Name']} queued at fog node {fog_name} at position {queue_position}")
                    random_fog_logger.info(f"Current load: {current_load:.2f}%")
                    random_fog_logger.info(f"Current queue size: {queue_position}")
                    
                    print(f"Queue position: {queue_position}")
                    print(f"Current load: {current_load:.2f}%")
                    
                    task_assigned = True
                    break
        
        # Log assignment failure if task couldn't be assigned
        if not task_assigned:
            random_fog_logger.error(f"Failed to assign or queue task {task['Name']}")
            print(f"\nError: Could not assign or queue task {task['Name']}")
        
        print("-" * 40)
    
    # Wait for all tasks to complete (with timeout)
    start_wait_time = time.time()
    max_wait_time = 30  # Maximum wait time in seconds
    
    while completed_tasks_count < total_tasks and time.time() - start_wait_time < max_wait_time:
        time.sleep(0.1)  # Small delay to prevent CPU overuse
        
    # Add warning if we didn't complete all tasks
    if completed_tasks_count < total_tasks:
        random_fog_logger.warning(f"Waited {max_wait_time} seconds but only completed {completed_tasks_count} of {total_tasks} tasks.")
    
    # Log queued tasks information
    if queued_tasks_info:
        random_fog_logger.info("\n=== Queued Tasks Summary ===")
        for task_name, info in queued_tasks_info.items():
            random_fog_logger.info(f"\nQueued Task: {task_name}")
            random_fog_logger.info(f"  Queued at: {info['node']} (distance={info['distance']:.2f} km)")
            random_fog_logger.info(f"  Queue Position: {info['queue_position']}")
            random_fog_logger.info(f"  Task Size: {info['task_size']} MI")
            random_fog_logger.info(f"  Required Resources:")
            random_fog_logger.info(f"    MIPS: {info['required_mips']}")
            random_fog_logger.info(f"    RAM: {info['required_ram']}")
            random_fog_logger.info(f"    Bandwidth: {info['required_bw']}")
            random_fog_logger.info(f"    Storage: {info['required_storage']}GB")
    
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
        fog_node_energy = {}
        fog_node_power = {}
        for info in task_completion_info.values():
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
        random_fog_logger.info("\n=== Overall Statistics ===")
        random_fog_logger.info(f"Total Tasks: {len(task_completion_info)}")
        random_fog_logger.info(f"  Fog Tasks: {len(task_completion_info)}")  # All tasks are fog tasks
        random_fog_logger.info("\nWorkload:")
        random_fog_logger.info(f"  Total System Workload: {total_workload:.2f} MIPS-seconds")
        random_fog_logger.info(f"  Average Workload per Task: {avg_workload:.2f} MIPS-seconds")
        random_fog_logger.info("\nResource Usage:")
        random_fog_logger.info(f"  Total Storage Used: {total_storage:.2f} GB")
        random_fog_logger.info(f"  Total Data Size: {total_size:.2f} MI")
        random_fog_logger.info(f"  Total Bandwidth Used: {total_bandwidth:.2f} Mbps")
        random_fog_logger.info(f"  Average Storage per Task: {avg_storage:.2f} GB")
        random_fog_logger.info(f"  Average Size per Task: {avg_size:.2f} MI")
        random_fog_logger.info(f"  Average Bandwidth per Task: {avg_bandwidth:.2f} Mbps")
        random_fog_logger.info("\nPower Consumption:")
        random_fog_logger.info(f"  Total Energy Consumed: {total_energy_wh:.6f} Wh")
        random_fog_logger.info(f"  Total Power Used: {total_power_watts:.2f} W")
        random_fog_logger.info(f"  Average Energy per Task: {avg_energy:.6f} Wh")
        random_fog_logger.info(f"  Average Power per Task: {avg_power:.2f} W")
        random_fog_logger.info("\nTransmission Time:")
        random_fog_logger.info(f"  Total: {total_transmission*1000:.6f}ms")
        random_fog_logger.info(f"  Average: {avg_transmission*1000:.6f}ms")
        random_fog_logger.info("\nProcessing Time:")
        random_fog_logger.info(f"  Total: {total_processing*1000:.6f}ms")
        random_fog_logger.info(f"  Average: {avg_processing*1000:.6f}ms")
        random_fog_logger.info("\nQueue Time:")
        random_fog_logger.info(f"  Total: {total_queue*1000:.6f}ms")
        random_fog_logger.info(f"  Average: {avg_queue*1000:.6f}ms")
        random_fog_logger.info("\nTotal Time:")
        random_fog_logger.info(f"  Total: {total_time*1000:.6f}ms")
        random_fog_logger.info(f"  Average: {avg_total*1000:.6f}ms")
        
        # Log Fog Node Statistics
        random_fog_logger.info("\n=== Per Fog Node Statistics ===")
        for node_name in fog_node_workloads:
            random_fog_logger.info(f"\n  {node_name}:")
            random_fog_logger.info(f"    Workload: {fog_node_workloads[node_name]:.2f} MIPS-seconds")
            random_fog_logger.info(f"    Storage Used: {fog_node_storage[node_name]:.2f} GB")
            random_fog_logger.info(f"    Data Size: {fog_node_size[node_name]:.2f} MI")
            random_fog_logger.info(f"    Bandwidth Used: {fog_node_bandwidth[node_name]:.2f} Mbps")
            random_fog_logger.info(f"    Energy Consumed: {fog_node_energy[node_name]:.6f} Wh")
            random_fog_logger.info(f"    Power Used: {fog_node_power[node_name]:.2f} W")

if __name__ == "__main__":
    # Load tasks from input
    tasks = read_and_log_tuples()
    
    # Print debug information
    print(f"\nDebug: Number of tasks received: {len(tasks)}")
    random_fog_logger.info(f"Number of tasks received: {len(tasks)}")
    
    # Process tasks using Random Fog Only algorithm
    if tasks:
        process_fcfs(tasks)
    else:
        print("Error: No tasks were loaded")
        random_fog_logger.error("No tasks were loaded.")
