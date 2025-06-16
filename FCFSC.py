"""
FCFSC (First Come First Serve with Cooperative) Task Scheduling Implementation
This module implements a modified FCFS algorithm that prioritizes cooperative resource sharing between fog and cloud nodes.
It extends the basic FCFS approach with cooperative scheduling and resource optimization mechanisms.
"""

import json
import time
import random
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG, CLOUD_SERVICES_CONFIG
from utility import calculate_distance, calculate_storage_requirements, calculate_transmission_time
from fog import get_fog_node, get_fog_node_status, get_all_fog_nodes
from cloud import get_cloud_node, get_cloud_node_status, get_all_cloud_nodes
from logger import setup_logger

# Initialize logger for FCFSC events
fcfsc_logger = setup_logger('fcfsc', 'fcfsc.log', sub_directory='algorithms')

def calculate_task_distances(task, is_cloud=False):
    """
    Calculate distances from task to all nodes (fog or cloud)
    
    Args:
        task (dict): Task information containing location data
        is_cloud (bool): Flag to determine if calculating distances to cloud nodes
        
    Returns:
        dict: Dictionary mapping node names to their distances from the task
    """
    distances = {}
    task_location = {
        'lat': task['GeoLocation']['latitude'],
        'lon': task['GeoLocation']['longitude']
    }
    
    # Select appropriate configuration based on node type
    config = CLOUD_SERVICES_CONFIG if is_cloud else FOG_NODES_CONFIG
    for node in config:
        node_location = node['location']
        distance = calculate_distance(task_location, node_location)
        distances[node['name']] = distance
    
    return distances

def calculate_transmission_time(task, node, distance):
    """
    Calculate transmission time using the comprehensive implementation from utility.py
    """
    return calculate_transmission_time(
        task['GeoLocation'],
        node.location,
        node,
        task.get('Size'),
        task.get('MIPS'),
        fcfsc_logger
    )

def calculate_processing_time(task_size, node_mips):
    """
    Calculate processing time using MCT's formula with overheads
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
    Process tasks using FCFSC algorithm with cooperative fog-cloud approach
    
    This function implements the main FCFSC scheduling logic:
    1. Tasks are sorted by creation time
    2. Each task is assigned to either fog or cloud based on data type
    3. Cooperative resource sharing is implemented between nodes
    4. Only tries 2 nearest fog nodes before falling back to cloud
    5. Performance metrics are tracked and logged
    
    Args:
        tasks (list): List of tasks to be processed
    """
    if not tasks:
        print("No tasks to process")
        fcfsc_logger.warning("No tasks to process.")
        return
        
    print("\n=== FCFS Hybrid Processing (Fog-Cloud) ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    fcfsc_logger.info(f"Total Tasks to Process: {len(tasks)}")
    
    # Initialize and log fog nodes
    fog_nodes = get_all_fog_nodes()
    for name, node in fog_nodes.items():
        fcfsc_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Location={node.location})")
    
    # Initialize and log cloud nodes
    cloud_nodes = get_all_cloud_nodes()
    for name, node in cloud_nodes.items():
        fcfsc_logger.info(f"Cloud node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Location={node.location})")
    
    # Initialize tracking variables
    task_completion_info = {}  # Store completion details for each task
    queued_tasks_info = {}     # Track tasks waiting in queue
    completed_tasks_count = 0  # Counter for completed tasks
    total_tasks = len(tasks)   # Total number of tasks to process
    task_queue_times = {}      # Track when tasks enter queue
    
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
        
        # Calculate queue time if task was queued
        queue_time = 0
        if task_name in task_queue_times:
            queue_time = time.time() - task_queue_times[task_name]
            del task_queue_times[task_name]
        
        # Store completion information
        task_completion_info[task_name] = {
            'node': node_name,
            'transmission_time': completion_info.get('transmission_time', 0),
            'queue_time': queue_time,
            'processing_time': completion_info.get('processing_time', 0),
            'total_time': completion_info.get('total_time', 0) + queue_time,
            'completion_time': completion_info.get('completion_time', 0),
            'storage_used': task.get('Storage', 0),
            'task': task  # Store the complete task information
        }
        
        # Update completion counter and logging
        completed_tasks_count += 1
        fcfsc_logger.info(f"\nTask Completed: {task_name}")
        fcfsc_logger.info(f"  Completed at: {node_name}")
        fcfsc_logger.info(f"  Transmission Time: {completion_info.get('transmission_time', 0):.2f}s")
        fcfsc_logger.info(f"  Queue Time: {queue_time:.2f}s")
        fcfsc_logger.info(f"  Processing Time: {completion_info.get('processing_time', 0):.2f}s")
        fcfsc_logger.info(f"  Storage Used: {task.get('Storage', 0):.2f}GB")
        fcfsc_logger.info(f"  Total Time: {task_completion_info[task_name]['total_time']:.2f}s")
        fcfsc_logger.info("  " + "-" * 30)
        
        if task_name in queued_tasks_info:
            del queued_tasks_info[task_name]
    
    # Register completion callback for all nodes
    for node in fog_nodes.values():
        node.add_completion_callback(task_completion_callback)
    for node in cloud_nodes.values():
        node.add_completion_callback(task_completion_callback)
    
    # Sort tasks by creation time (FCFS order)
    sorted_tasks = sorted(tasks, key=lambda x: x['CreationTime'])
    
    # Process each task in order
    for i, task in enumerate(sorted_tasks, 1):
        # Log task details
        print(f"\nProcessing Task {i}:")
        print(f"Task Name: {task['Name']}")
        print(f"Creation Time: {task['CreationTime']}")
        print(f"Size: {task['Size']} GB")
        print(f"MIPS: {task['MIPS']}")
        print(f"RAM: {task['RAM']}")
        print(f"BW: {task['BW']}")
        print(f"Storage: {task['Storage']}GB")
        print(f"DataType: {task['DataType']}")
        print(f"DeviceType: {task['DeviceType']}")
        
        # Determine task type (cloud or fog)
        is_cloud_task = task['DataType'] in ['Bulk', 'Large']
        print(f"\nTask Type: {'Cloud' if is_cloud_task else 'Fog'}")
        
        # Calculate distances to nodes
        fog_distances = calculate_task_distances(task, is_cloud=False)
        cloud_distances = calculate_task_distances(task, is_cloud=True)
        
        # Sort nodes by distance for optimal assignment
        sorted_fog_nodes = sorted(fog_distances.items(), key=lambda x: x[1])
        sorted_cloud_nodes = sorted(cloud_distances.items(), key=lambda x: x[1])
        
        # Log distance calculations
        fcfsc_logger.info(f"\nTask {task['Name']} - Distance Calculations:")
        fcfsc_logger.info("Fog Node Distances:")
        for fog_name, distance in sorted_fog_nodes:
            fcfsc_logger.info(f"  {fog_name}: {distance:.2f} km")
        fcfsc_logger.info("Cloud Node Distances:")
        for cloud_name, distance in sorted_cloud_nodes:
            fcfsc_logger.info(f"  {cloud_name}: {distance:.2f} km")
        
        # Task assignment logic
        task_assigned = False
        if not is_cloud_task:
            # Try first fog node
            first_fog_name, first_fog_distance = sorted_fog_nodes[0]
            fog_node = get_fog_node(first_fog_name)
            if fog_node:
                fcfsc_logger.info(f"\nAttempting to assign task {task['Name']} to fog node {first_fog_name} ({first_fog_distance:.2f} km)")
                # Check all resources before assignment
                if fog_node.can_handle_task(task):
                    success, processing_time = fog_node.assign_task(task)
                    if success:
                        # Task successfully assigned to fog node
                        fcfsc_logger.info(f"Task successfully assigned to fog node {first_fog_name}")
                        print(f"\nTask assigned to fog node {first_fog_name} ({first_fog_distance:.2f} km)")
                        print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                        print("Fog Node Status:")
                        status = fog_node.get_status()
                        for key, value in status.items():
                            print(f"  {key}: {value}")
                        task_assigned = True
                    else:
                        fcfsc_logger.info(f"Fog node {first_fog_name} cannot handle task - trying second fog node")
                        print(f"\nFog node {first_fog_name} cannot handle task due to resource constraints")
                else:
                    fcfsc_logger.info(f"Fog node {first_fog_name} does not have sufficient resources")
                    print(f"\nFog node {first_fog_name} does not have sufficient resources")
                
                # Try second fog node if available
                if not task_assigned and len(sorted_fog_nodes) > 1:
                    second_fog_name, second_fog_distance = sorted_fog_nodes[1]
                    fog_node = get_fog_node(second_fog_name)
                    if fog_node:
                        fcfsc_logger.info(f"Attempting to assign task to fog node {second_fog_name} ({second_fog_distance:.2f} km)")
                        # Check all resources before assignment
                        if fog_node.can_handle_task(task):
                            success, processing_time = fog_node.assign_task(task)
                            if success:
                                # Task successfully assigned to second fog node
                                fcfsc_logger.info(f"Task successfully assigned to fog node {second_fog_name}")
                                print(f"Task assigned to fog node {second_fog_name} ({second_fog_distance:.2f} km)")
                                print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                                print("Fog Node Status:")
                                status = fog_node.get_status()
                                for key, value in status.items():
                                    print(f"  {key}: {value}")
                                task_assigned = True
                            else:
                                fcfsc_logger.info(f"Second fog node {second_fog_name} also cannot handle task - attempting cloud fallback")
                                print(f"\nSecond fog node {second_fog_name} cannot handle task - attempting cloud fallback")
                        else:
                            fcfsc_logger.info(f"Second fog node {second_fog_name} does not have sufficient resources")
                            print(f"\nSecond fog node {second_fog_name} does not have sufficient resources")
                
                # Try cloud nodes if both fog nodes failed
                if not task_assigned:
                    fcfsc_logger.info("Attempting cloud fallback")
                    print("\nAttempting cloud fallback")
                    
                    # Attempt cloud assignment
                    for cloud_name, cloud_distance in sorted_cloud_nodes:
                        cloud_node = get_cloud_node(cloud_name)
                        if cloud_node:
                            fcfsc_logger.info(f"Attempting to assign task to cloud node {cloud_name} ({cloud_distance:.2f} km)")
                            # Check all resources before assignment
                            if cloud_node.can_handle_task(task):
                                success, processing_time = cloud_node.assign_task(task)
                                if success:
                                    # Task successfully assigned to cloud node
                                    fcfsc_logger.info(f"Task successfully assigned to cloud node {cloud_name}")
                                    print(f"Task assigned to cloud node {cloud_name} ({cloud_distance:.2f} km)")
                                    print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                                    print("Cloud Node Status:")
                                    status = cloud_node.get_status()
                                    for key, value in status.items():
                                        print(f"  {key}: {value}")
                                    task_assigned = True
                                    break
                                else:
                                    # Queue task at cloud node
                                    fcfsc_logger.info(f"Cloud node {cloud_name} cannot handle task immediately - queuing")
                                    print(f"Task queued at cloud node {cloud_name} ({cloud_distance:.2f} km)")
                                    # Record queue entry time
                                    task_queue_times[task['Name']] = time.time()
                                    queued_tasks_info[task['Name']] = {
                                        'node': cloud_name,
                                        'node_type': 'Cloud',
                                        'distance': cloud_distance,
                                        'queue_position': len(cloud_node.task_queue),
                                        'queue_entry_time': time.time(),
                                        'task_size': task['Size'],
                                        'required_mips': task['MIPS'],
                                        'required_ram': task['RAM'],
                                        'required_bw': task['BW'],
                                        'required_storage': task['Storage']
                                    }
                                    task_assigned = True
                                    break
                            else:
                                fcfsc_logger.info(f"Cloud node {cloud_name} does not have sufficient resources")
                                print(f"\nCloud node {cloud_name} does not have sufficient resources")
        else:
            # Direct cloud assignment for cloud tasks
            fcfsc_logger.info(f"\nTask {task['Name']} is a cloud task - attempting cloud nodes directly")
            for cloud_name, distance in sorted_cloud_nodes:
                cloud_node = get_cloud_node(cloud_name)
                if cloud_node:
                    fcfsc_logger.info(f"Attempting to assign task to cloud node {cloud_name} ({distance:.2f} km)")
                    # Check all resources before assignment
                    if cloud_node.can_handle_task(task):
                        success, processing_time = cloud_node.assign_task(task)
                        if success:
                            # Task successfully assigned to cloud node
                            fcfsc_logger.info(f"Task successfully assigned to cloud node {cloud_name}")
                            print(f"\nTask assigned to cloud node {cloud_name} ({distance:.2f} km)")
                            print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                            print("Cloud Node Status:")
                            status = cloud_node.get_status()
                            for key, value in status.items():
                                print(f"  {key}: {value}")
                            task_assigned = True
                            break
                        else:
                            # Queue task at cloud node
                            fcfsc_logger.info(f"Cloud node {cloud_name} cannot handle task immediately - queuing")
                            print(f"\nTask queued at cloud node {cloud_name} ({distance:.2f} km)")
                            # Record queue entry time
                            task_queue_times[task['Name']] = time.time()
                            queued_tasks_info[task['Name']] = {
                                'node': cloud_name,
                                'node_type': 'Cloud',
                                'distance': distance,
                                'queue_position': len(cloud_node.task_queue),
                                'queue_entry_time': time.time(),
                                'task_size': task['Size'],
                                'required_mips': task['MIPS'],
                                'required_ram': task['RAM'],
                                'required_bw': task['BW'],
                                'required_storage': task['Storage']
                            }
                            task_assigned = True
                            break
                    else:
                        fcfsc_logger.info(f"Cloud node {cloud_name} does not have sufficient resources")
                        print(f"\nCloud node {cloud_name} does not have sufficient resources")
        
        # Log assignment failure if task couldn't be assigned
        if not task_assigned:
            fcfsc_logger.error(f"Failed to assign or queue task {task['Name']}")
            print(f"\nError: Could not assign or queue task {task['Name']}")
        
        print("-" * 40)
    
    # Wait for all tasks to complete
    while completed_tasks_count < total_tasks:
        time.sleep(0.1)  # Small delay to prevent CPU overuse
    
    # Log queued tasks information
    if queued_tasks_info:
        fcfsc_logger.info("\n=== Queued Tasks Summary ===")
        for task_name, info in queued_tasks_info.items():
            fcfsc_logger.info(f"\nQueued Task: {task_name}")
            fcfsc_logger.info(f"  Queued at: {info['node']} ({info['node_type']}, distance={info['distance']:.2f} km)")
            fcfsc_logger.info(f"  Queue Position: {info['queue_position']}")
            fcfsc_logger.info(f"  Task Size: {info['task_size']} GB")
            fcfsc_logger.info(f"  Required Resources:")
            fcfsc_logger.info(f"    MIPS: {info['required_mips']}")
            fcfsc_logger.info(f"    RAM: {info['required_ram']}")
            fcfsc_logger.info(f"    Bandwidth: {info['required_bw']}")
            fcfsc_logger.info(f"    Storage: {info['required_storage']}GB")
    
    # Log final statistics
    if task_completion_info:
        # Calculate statistics by node type
        cloud_tasks = [info for info in task_completion_info.values() if info['node'] in cloud_nodes]
        fog_tasks = [info for info in task_completion_info.values() if info['node'] in fog_nodes]
        
        # Calculate total workload (MIPS * processing time for each task)
        total_workload = sum(info['task']['MIPS'] * info.get('processing_time', 0) for info in task_completion_info.values())
        
        # Calculate total size and bandwidth
        total_size = sum(info['task']['Size'] for info in task_completion_info.values())
        total_bandwidth = sum(info['task']['BW'] for info in task_completion_info.values())
        
        # Calculate Overall Statistics
        total_transmission = sum(info.get('transmission_time', 0) for info in task_completion_info.values())
        total_processing = sum(info.get('processing_time', 0) for info in task_completion_info.values())
        total_queue = sum(info.get('queue_time', 0) for info in task_completion_info.values())
        total_time = sum(info.get('total_time', 0) for info in task_completion_info.values())
        total_storage = sum(info.get('storage_used', 0) for info in task_completion_info.values())
        
        # Calculate Cloud Statistics
        if cloud_tasks:
            cloud_transmission = sum(info.get('transmission_time', 0) for info in cloud_tasks)
            cloud_processing = sum(info.get('processing_time', 0) for info in cloud_tasks)
            cloud_queue = sum(info.get('queue_time', 0) for info in cloud_tasks)
            cloud_total = sum(info.get('total_time', 0) for info in cloud_tasks)
            cloud_storage = sum(info.get('storage_used', 0) for info in cloud_tasks)
            cloud_workload = sum(info['task']['MIPS'] * info.get('processing_time', 0) for info in cloud_tasks)
            cloud_size = sum(info['task']['Size'] for info in cloud_tasks)
            cloud_bandwidth = sum(info['task']['BW'] for info in cloud_tasks)
            
            # Calculate workload per cloud node
            cloud_node_workloads = {}
            cloud_node_storage = {}
            cloud_node_size = {}
            cloud_node_bandwidth = {}
            for info in cloud_tasks:
                node_name = info['node']
                if node_name not in cloud_node_workloads:
                    cloud_node_workloads[node_name] = 0
                    cloud_node_storage[node_name] = 0
                    cloud_node_size[node_name] = 0
                    cloud_node_bandwidth[node_name] = 0
                cloud_node_workloads[node_name] += info['task']['MIPS'] * info.get('processing_time', 0)
                cloud_node_storage[node_name] += info.get('storage_used', 0)
                cloud_node_size[node_name] += info['task']['Size']
                cloud_node_bandwidth[node_name] += info['task']['BW']
            
            avg_cloud_transmission = cloud_transmission / len(cloud_tasks)
            avg_cloud_processing = cloud_processing / len(cloud_tasks)
            avg_cloud_queue = cloud_queue / len(cloud_tasks)
            avg_cloud_total = cloud_total / len(cloud_tasks)
            avg_cloud_storage = cloud_storage / len(cloud_tasks)
            avg_cloud_workload = cloud_workload / len(cloud_tasks)
            avg_cloud_size = cloud_size / len(cloud_tasks)
            avg_cloud_bandwidth = cloud_bandwidth / len(cloud_tasks)
        
        # Calculate Fog Statistics
        if fog_tasks:
            fog_transmission = sum(info.get('transmission_time', 0) for info in fog_tasks)
            fog_processing = sum(info.get('processing_time', 0) for info in fog_tasks)
            fog_queue = sum(info.get('queue_time', 0) for info in fog_tasks)
            fog_total = sum(info.get('total_time', 0) for info in fog_tasks)
            fog_storage = sum(info.get('storage_used', 0) for info in fog_tasks)
            fog_workload = sum(info['task']['MIPS'] * info.get('processing_time', 0) for info in fog_tasks)
            fog_size = sum(info['task']['Size'] for info in fog_tasks)
            fog_bandwidth = sum(info['task']['BW'] for info in fog_tasks)
            
            # Calculate workload per fog node
            fog_node_workloads = {}
            fog_node_storage = {}
            fog_node_size = {}
            fog_node_bandwidth = {}
            for info in fog_tasks:
                node_name = info['node']
                if node_name not in fog_node_workloads:
                    fog_node_workloads[node_name] = 0
                    fog_node_storage[node_name] = 0
                    fog_node_size[node_name] = 0
                    fog_node_bandwidth[node_name] = 0
                fog_node_workloads[node_name] += info['task']['MIPS'] * info.get('processing_time', 0)
                fog_node_storage[node_name] += info.get('storage_used', 0)
                fog_node_size[node_name] += info['task']['Size']
                fog_node_bandwidth[node_name] += info['task']['BW']
            
            avg_fog_transmission = fog_transmission / len(fog_tasks)
            avg_fog_processing = fog_processing / len(fog_tasks)
            avg_fog_queue = fog_queue / len(fog_tasks)
            avg_fog_total = fog_total / len(fog_tasks)
            avg_fog_storage = fog_storage / len(fog_tasks)
            avg_fog_workload = fog_workload / len(fog_tasks)
            avg_fog_size = fog_size / len(fog_tasks)
            avg_fog_bandwidth = fog_bandwidth / len(fog_tasks)
        
        # Calculate averages for overall statistics
        avg_transmission = total_transmission / len(task_completion_info) if task_completion_info else 0
        avg_processing = total_processing / len(task_completion_info) if task_completion_info else 0
        avg_queue = total_queue / len(task_completion_info) if task_completion_info else 0
        avg_total = total_time / len(task_completion_info) if task_completion_info else 0
        avg_storage = total_storage / len(task_completion_info) if task_completion_info else 0
        avg_workload = total_workload / len(task_completion_info) if task_completion_info else 0
        avg_size = total_size / len(task_completion_info) if task_completion_info else 0
        avg_bandwidth = total_bandwidth / len(task_completion_info) if task_completion_info else 0
        
        # Log Overall Statistics
        fcfsc_logger.info("\n=== Overall Statistics ===")
        fcfsc_logger.info(f"Total Tasks: {len(task_completion_info)}")
        fcfsc_logger.info(f"  Cloud Tasks: {len(cloud_tasks)}")
        fcfsc_logger.info(f"  Fog Tasks: {len(fog_tasks)}")
        fcfsc_logger.info("\nWorkload:")
        fcfsc_logger.info(f"  Total System Workload: {total_workload:.2f} MIPS-seconds")
        fcfsc_logger.info(f"  Average Workload per Task: {avg_workload:.2f} MIPS-seconds")
        fcfsc_logger.info("\nResource Usage:")
        fcfsc_logger.info(f"  Total Storage Used: {total_storage:.2f} GB")
        fcfsc_logger.info(f"  Total Data Size: {total_size:.2f} MI")
        fcfsc_logger.info(f"  Total Bandwidth Used: {total_bandwidth:.2f} Mbps")
        fcfsc_logger.info(f"  Average Storage per Task: {avg_storage:.2f} GB")
        fcfsc_logger.info(f"  Average Size per Task: {avg_size:.2f} MI")
        fcfsc_logger.info(f"  Average Bandwidth per Task: {avg_bandwidth:.2f} Mbps")
        fcfsc_logger.info("\nTransmission Time:")
        fcfsc_logger.info(f"  Total: {total_transmission*1000:.6f}ms")
        fcfsc_logger.info(f"  Average: {avg_transmission*1000:.6f}ms")
        fcfsc_logger.info("\nProcessing Time:")
        fcfsc_logger.info(f"  Total: {total_processing*1000:.6f}ms")
        fcfsc_logger.info(f"  Average: {avg_processing*1000:.6f}ms")
        fcfsc_logger.info("\nQueue Time:")
        fcfsc_logger.info(f"  Total: {total_queue*1000:.6f}ms")
        fcfsc_logger.info(f"  Average: {avg_queue*1000:.6f}ms")
        fcfsc_logger.info("\nTotal Time:")
        fcfsc_logger.info(f"  Total: {total_time*1000:.6f}ms")
        fcfsc_logger.info(f"  Average: {avg_total*1000:.6f}ms")
        
        # Log Fog Statistics
        if fog_tasks:
            fcfsc_logger.info("\n=== Overall Fog Statistics ===")
            fcfsc_logger.info(f"Total Fog Tasks: {len(fog_tasks)}")
            fcfsc_logger.info("\nWorkload:")
            fcfsc_logger.info(f"  Total Fog Workload: {fog_workload:.2f} MIPS-seconds")
            fcfsc_logger.info(f"  Average Workload per Fog Task: {avg_fog_workload:.2f} MIPS-seconds")
            fcfsc_logger.info("\nResource Usage:")
            fcfsc_logger.info(f"  Total Fog Storage: {fog_storage:.2f} GB")
            fcfsc_logger.info(f"  Total Fog Data Size: {fog_size:.2f} MI")
            fcfsc_logger.info(f"  Total Fog Bandwidth: {fog_bandwidth:.2f} Mbps")
            fcfsc_logger.info(f"  Average Storage per Fog Task: {avg_fog_storage:.2f} GB")
            fcfsc_logger.info(f"  Average Size per Fog Task: {avg_fog_size:.2f} MI")
            fcfsc_logger.info(f"  Average Bandwidth per Fog Task: {avg_fog_bandwidth:.2f} Mbps")
            fcfsc_logger.info("\nPer Node Statistics:")
            for node_name in fog_node_workloads:
                fcfsc_logger.info(f"\n  {node_name}:")
                fcfsc_logger.info(f"    Workload: {fog_node_workloads[node_name]:.2f} MIPS-seconds")
                fcfsc_logger.info(f"    Storage Used: {fog_node_storage[node_name]:.2f} GB")
                fcfsc_logger.info(f"    Data Size: {fog_node_size[node_name]:.2f} MI")
                fcfsc_logger.info(f"    Bandwidth Used: {fog_node_bandwidth[node_name]:.2f} Mbps")
            fcfsc_logger.info("\nTransmission Time:")
            fcfsc_logger.info(f"  Total: {fog_transmission*1000:.6f}ms")
            fcfsc_logger.info(f"  Average: {avg_fog_transmission*1000:.6f}ms")
            fcfsc_logger.info("\nProcessing Time:")
            fcfsc_logger.info(f"  Total: {fog_processing*1000:.6f}ms")
            fcfsc_logger.info(f"  Average: {avg_fog_processing*1000:.6f}ms")
            fcfsc_logger.info("\nQueue Time:")
            fcfsc_logger.info(f"  Total: {fog_queue*1000:.6f}ms")
            fcfsc_logger.info(f"  Average: {avg_fog_queue*1000:.6f}ms")
            fcfsc_logger.info("\nTotal Time:")
            fcfsc_logger.info(f"  Total: {fog_total*1000:.6f}ms")
            fcfsc_logger.info(f"  Average: {avg_fog_total*1000:.6f}ms")
        
        # Log Cloud Statistics
        if cloud_tasks:
            fcfsc_logger.info("\n=== Overall Cloud Statistics ===")
            fcfsc_logger.info(f"Total Cloud Tasks: {len(cloud_tasks)}")
            fcfsc_logger.info("\nWorkload:")
            fcfsc_logger.info(f"  Total Cloud Workload: {cloud_workload:.2f} MIPS-seconds")
            fcfsc_logger.info(f"  Average Workload per Cloud Task: {avg_cloud_workload:.2f} MIPS-seconds")
            fcfsc_logger.info("\nResource Usage:")
            fcfsc_logger.info(f"  Total Cloud Storage: {cloud_storage:.2f} GB")
            fcfsc_logger.info(f"  Total Cloud Data Size: {cloud_size:.2f} MI")
            fcfsc_logger.info(f"  Total Cloud Bandwidth: {cloud_bandwidth:.2f} Mbps")
            fcfsc_logger.info(f"  Average Storage per Cloud Task: {avg_cloud_storage:.2f} GB")
            fcfsc_logger.info(f"  Average Size per Cloud Task: {avg_cloud_size:.2f} MI")
            fcfsc_logger.info(f"  Average Bandwidth per Cloud Task: {avg_cloud_bandwidth:.2f} Mbps")
            fcfsc_logger.info("\nPer Node Statistics:")
            for node_name in cloud_node_workloads:
                fcfsc_logger.info(f"\n  {node_name}:")
                fcfsc_logger.info(f"    Workload: {cloud_node_workloads[node_name]:.2f} MIPS-seconds")
                fcfsc_logger.info(f"    Storage Used: {cloud_node_storage[node_name]:.2f} GB")
                fcfsc_logger.info(f"    Data Size: {cloud_node_size[node_name]:.2f} MI")
                fcfsc_logger.info(f"    Bandwidth Used: {cloud_node_bandwidth[node_name]:.2f} Mbps")
            fcfsc_logger.info("\nTransmission Time:")
            fcfsc_logger.info(f"  Total: {cloud_transmission*1000:.6f}ms")
            fcfsc_logger.info(f"  Average: {avg_cloud_transmission*1000:.6f}ms")
            fcfsc_logger.info("\nProcessing Time:")
            fcfsc_logger.info(f"  Total: {cloud_processing*1000:.6f}ms")
            fcfsc_logger.info(f"  Average: {avg_cloud_processing*1000:.6f}ms")
            fcfsc_logger.info("\nQueue Time:")
            fcfsc_logger.info(f"  Total: {cloud_queue*1000:.6f}ms")
            fcfsc_logger.info(f"  Average: {avg_cloud_queue*1000:.6f}ms")
            fcfsc_logger.info("\nTotal Time:")
            fcfsc_logger.info(f"  Total: {cloud_total*1000:.6f}ms")
            fcfsc_logger.info(f"  Average: {avg_cloud_total*1000:.6f}ms")

if __name__ == "__main__":
    # Load tasks from input
    tasks = read_and_log_tuples()
    
    # Print debug information
    print(f"\nDebug: Number of tasks received: {len(tasks)}")
    fcfsc_logger.info(f"Number of tasks received: {len(tasks)}")
    
    # Process tasks using FCFSC algorithm
    if tasks:
        process_fcfs(tasks)
    else:
        print("Error: No tasks were loaded")
        fcfsc_logger.error("No tasks were loaded.")