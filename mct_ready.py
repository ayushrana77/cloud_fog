"""
MCT (Minimum Completion Time) Task Scheduling Implementation

This module implements a task scheduling algorithm that minimizes completion time
by considering both fog and cloud resources with optimal resource allocation.

Key Features:
- Calculates optimal task assignments between fog and cloud nodes
- Considers multiple factors including:
  * Distance between task and nodes
  * Resource availability (MIPS, Memory, Bandwidth, Storage)
  * Processing and transmission times
  * Queue times
- Provides detailed scoring and analysis of node performance
- Implements comprehensive logging of task execution and performance metrics

The algorithm follows these main steps:
1. Calculate scores for all available fog nodes
2. Select the fog node with the highest score
3. If fog node cannot handle the task, use cloud nodes with MCT approach
4. Track and log all performance metrics

Dependencies:
- task_load: For reading and logging task tuples
- config: For fog and cloud node configurations
- utility: For distance calculations
- fog_with_queue: For fog node operations with queue support
- cloud: For cloud node operations
- logger: For logging functionality
"""

import json
import time
import math
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG, CLOUD_SERVICES_CONFIG
from utility import calculate_distance, calculate_transmission_time
from fog_with_queue import get_fog_node, get_fog_node_status, get_all_fog_nodes
from cloud import get_cloud_node, get_cloud_node_status, get_all_cloud_nodes
from logger import setup_logger

# Initialize logger for MCT events
mct_logger = setup_logger('mct', 'mct.log', sub_directory='algorithms')

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
        mct_logger
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

def calculate_cloud_completion_time(task, cloud_node, distance, task_queue_times):
    """
    Calculate completion time for a cloud node using MCT approach
    
    Args:
        task (dict): Task information
        cloud_node: Cloud node object
        distance (float): Distance to the cloud node
        task_queue_times (dict): Dictionary tracking task queue times
        
    Returns:
        float: Completion time
    """
    # Get node status
    status = cloud_node.get_status()
    
    # Calculate transmission time
    transmission_time = calculate_transmission_time(
        task['GeoLocation'],
        cloud_node.location,
        cloud_node,
        task.get('Size'),
        task.get('MIPS'),
        mct_logger
    )
    
    # Calculate execution time
    execution_time = task['Size'] / cloud_node.mips
    
    # Calculate ready time (time until node becomes available)
    ready_time = 0
    current_time = time.time()
    
    # Check if node is busy with current tasks
    if cloud_node.assigned_tasks:
        # Find the latest completion time among current tasks
        latest_completion = max(
            task_info['start_time'] + task_info['processing_time']
            for task_info in cloud_node.assigned_tasks
        )
        ready_time = max(0, latest_completion - current_time)
        mct_logger.info(f"Node {cloud_node.name} is busy with {len(cloud_node.assigned_tasks)} tasks")
        mct_logger.info(f"Latest task completion time: {latest_completion}")
        mct_logger.info(f"Current time: {current_time}")
        mct_logger.info(f"Calculated ready time: {ready_time*1000:.2f}ms")
    else:
        mct_logger.info(f"Node {cloud_node.name} is idle, ready time: 0ms")
    
    # Calculate queue time
    queue_time = 0
    if task['Name'] in task_queue_times:
        queue_time = time.time() - task_queue_times[task['Name']]
    
    # Total completion time
    completion_time = transmission_time + execution_time + ready_time + queue_time
    
    # Log detailed timing information
    mct_logger.info(f"\nDetailed timing for {cloud_node.name}:")
    mct_logger.info(f"  - Distance: {distance:.2f} km")
    mct_logger.info(f"  - Transmission Time: {transmission_time*1000:.2f}ms")
    mct_logger.info(f"  - Execution Time: {execution_time*1000:.2f}ms")
    mct_logger.info(f"  - Ready Time: {ready_time*1000:.2f}ms")
    mct_logger.info(f"  - Queue Time: {queue_time*1000:.2f}ms")
    mct_logger.info(f"  - Total Time: {completion_time*1000:.2f}ms")
    
    return completion_time

def process_mct(tasks):
    """
    Process tasks using Minimum Completion Time (MCT) algorithm
    
    This function implements the MCT scheduling logic:
    1. For each task, calculate scores for all fog nodes
    2. Select the fog node with maximum score
    3. If fog node can't handle the task, use cloud with MCT
    4. Track and log all performance metrics including queue times
    """
    if not tasks:
        print("No tasks to process")
        mct_logger.warning("No tasks to process.")
        return
        
    print("\n=== MCT Processing (Fog-Cloud) ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    mct_logger.info(f"Total Tasks to Process: {len(tasks)}")
    
    # Initialize and log fog nodes
    fog_nodes = get_all_fog_nodes()
    for name, node in fog_nodes.items():
        mct_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Storage={node.storage}, Location={node.location})")
    
    # Initialize and log cloud nodes
    cloud_nodes = get_all_cloud_nodes()
    for name, node in cloud_nodes.items():
        mct_logger.info(f"Cloud node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Storage={node.storage}, Location={node.location})")
    
    # Initialize tracking variables
    task_completion_info = {}
    queued_tasks_info = {}
    completed_tasks_count = 0
    total_tasks = len(tasks)
    task_queue_times = {}  # Initialize task queue times dictionary
    
    def task_completion_callback(node_name, completion_info):
        """Callback function to handle task completion events"""
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
            'processing_time': completion_info.get('processing_time', 0),
            'queue_time': queue_time,
            'total_time': completion_info.get('total_time', 0) + queue_time,
            'completion_time': completion_info.get('completion_time', 0),
            'storage_used': task.get('Storage', 0),
            'task': task  # Store the complete task information
        }
        
        # Update completion counter and logging
        completed_tasks_count += 1
        mct_logger.info(f"\nTask Completed: {task_name}")
        mct_logger.info(f"  Completed at: {node_name}")
        mct_logger.info(f"  Transmission Time: {completion_info.get('transmission_time', 0):.2f}s")
        mct_logger.info(f"  Processing Time: {completion_info.get('processing_time', 0):.2f}s")
        mct_logger.info(f"  Queue Time: {queue_time:.2f}s")
        mct_logger.info(f"  Total Time: {task_completion_info[task_name]['total_time']:.2f}s")
        mct_logger.info("  " + "-" * 30)
        
        if task_name in queued_tasks_info:
            del queued_tasks_info[task_name]
    
    # Register completion callback for all nodes
    for node in fog_nodes.values():
        node.add_completion_callback(task_completion_callback)
    for node in cloud_nodes.values():
        node.add_completion_callback(task_completion_callback)
    
    # Process each task
    for i, task in enumerate(tasks, 1):
        print(f"\nProcessing Task {i}:")
        print(f"Task Name: {task['Name']}")
        print(f"Size: {task['Size']} MI")
        print(f"MIPS: {task['MIPS']}")
        print(f"RAM: {task['RAM']}")
        print(f"BW: {task['BW']}")
        print(f"Storage: {task.get('Storage', 0)} GB")
        print(f"DataType: {task['DataType']}")
        
        # Determine if it's a cloud-specific task
        is_cloud_task = task['DataType'] in ['Bulk', 'Large']
        print(f"Task Type: {'Cloud' if is_cloud_task else 'Fog/General'}")

        # Calculate distances to all nodes
        fog_distances = calculate_task_distances(task, is_cloud=False)
        cloud_distances = calculate_task_distances(task, is_cloud=True)
        
        task_assigned = False

        if not is_cloud_task:
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
                mct_logger.info(f"\nAttempting to assign task {task['Name']} to fog node {fog_node.name}")
                
                # Calculate completion times before any assignment
                fog_completion_time = calculate_cloud_completion_time(task, fog_node, fog_distances[fog_node.name], task_queue_times)
                
                # Calculate cloud completion times
                cloud_completion_times = {}
                for cloud_name, distance in cloud_distances.items():
                    cloud_node = get_cloud_node(cloud_name)
                    if cloud_node:
                        completion_time = calculate_cloud_completion_time(task, cloud_node, distance, task_queue_times)
                        cloud_completion_times[cloud_name] = {
                            'completion_time': completion_time,
                            'node': cloud_node
                        }
                
                # Find best cloud node (minimum completion time)
                best_cloud = None
                if cloud_completion_times:
                    best_cloud_name = min(cloud_completion_times.items(), 
                                        key=lambda x: x[1]['completion_time'])[0]
                    best_cloud = cloud_completion_times[best_cloud_name]
                
                # Compare fog and cloud completion times
                if best_cloud and best_cloud['completion_time'] < fog_completion_time:
                    # Cloud is better, try cloud first
                    cloud_node = best_cloud['node']
                    success, processing_time = cloud_node.assign_task(task)
                    if success:
                        mct_logger.info(f"Task assigned to cloud node {cloud_node.name}")
                        print(f"\nTask assigned to cloud node {cloud_node.name}")
                        task_assigned = True
                    else:
                        # Queue at cloud
                        mct_logger.info(f"Cloud node {cloud_node.name} cannot handle task immediately - queuing")
                        print(f"\nTask queued at cloud node {cloud_node.name}")
                        task_queue_times[task['Name']] = time.time()
                        queued_tasks_info[task['Name']] = {
                            'node': cloud_node.name,
                            'node_type': 'Cloud',
                            'completion_time': best_cloud['completion_time'],
                            'queue_position': len(cloud_node.task_queue)
                        }
                        task_assigned = True
                else:
                    # Fog is better, try fog
                    success, processing_time = fog_node.assign_task(task)
                    if success:
                        mct_logger.info(f"Task successfully assigned to fog node {fog_node.name}")
                        print(f"\nTask assigned to fog node {fog_node.name}")
                        task_assigned = True
                    else:
                        # Queue at fog
                        mct_logger.info(f"Task queued at fog node {fog_node.name}")
                        print(f"\nTask queued at fog node {fog_node.name}")
                        task_queue_times[task['Name']] = time.time()
                        queued_tasks_info[task['Name']] = {
                            'node': fog_node.name,
                            'node_type': 'Fog',
                            'completion_time': fog_completion_time,
                            'queue_position': len(fog_node.task_queue)
                        }
                        task_assigned = True
        
        # If not assigned to fog or if it's a cloud task, try cloud nodes
        if not task_assigned:
            cloud_completion_times = {}
            for cloud_name, distance in cloud_distances.items():
                cloud_node = get_cloud_node(cloud_name)
                if cloud_node:
                    completion_time = calculate_cloud_completion_time(task, cloud_node, distance, task_queue_times)
                    cloud_completion_times[cloud_name] = {
                        'completion_time': completion_time,
                        'node': cloud_node
                    }
            
            if cloud_completion_times:
                # Find cloud node with minimum completion time
                best_cloud_name = min(cloud_completion_times.items(), 
                                    key=lambda x: x[1]['completion_time'])[0]
                best_cloud = cloud_completion_times[best_cloud_name]
                cloud_node = best_cloud['node']
                
                # Print and log detailed metrics for the selected cloud node
                print("\nCloud Node Selection Metrics:")
                print("-" * 80)
                mct_logger.info("\n=== Cloud Node Selection Metrics ===")
                
                print(f"Selected Cloud Node: {cloud_node.name}")
                mct_logger.info(f"Selected Cloud Node: {cloud_node.name}")
                
                # Get node status for current metrics
                status = cloud_node.get_status()
                
                # Calculate and print/log transmission time
                transmission_time = calculate_transmission_time(
                    task['GeoLocation'],
                    cloud_node.location,
                    cloud_node,
                    task.get('Size'),
                    task.get('MIPS'),
                    mct_logger
                )
                print(f"\nTransmission Time: {transmission_time*1000:.2f}ms")
                print(f"  - Distance: {distance:.2f} km")
                print(f"  - Bandwidth: {cloud_node.bandwidth} Mbps")
                print(f"  - Data Size: {task.get('Size')} MI")
                
                mct_logger.info(f"\nTransmission Time: {transmission_time*1000:.2f}ms")
                mct_logger.info(f"  - Distance: {distance:.2f} km")
                mct_logger.info(f"  - Bandwidth: {cloud_node.bandwidth} Mbps")
                mct_logger.info(f"  - Data Size: {task.get('Size')} MI")
                
                # Calculate and print/log processing time
                processing_time = task['Size'] / cloud_node.mips
                print(f"\nProcessing Time: {processing_time*1000:.2f}ms")
                print(f"  - Task Size: {task['Size']} MI")
                print(f"  - Node MIPS: {cloud_node.mips}")
                
                mct_logger.info(f"\nProcessing Time: {processing_time*1000:.2f}ms")
                mct_logger.info(f"  - Task Size: {task['Size']} MI")
                mct_logger.info(f"  - Node MIPS: {cloud_node.mips}")
                
                # Calculate and print/log queue time
                queue_time = 0
                if task['Name'] in task_queue_times:
                    queue_time = time.time() - task_queue_times[task['Name']]
                print(f"\nQueue Time: {queue_time*1000:.2f}ms")
                print(f"  - Current Queue Size: {len(cloud_node.task_queue)}")
                print(f"  - Queue Position: {len(cloud_node.task_queue) + 1}")
                
                mct_logger.info(f"\nQueue Time: {queue_time*1000:.2f}ms")
                mct_logger.info(f"  - Current Queue Size: {len(cloud_node.task_queue)}")
                mct_logger.info(f"  - Queue Position: {len(cloud_node.task_queue) + 1}")
                
                # Calculate and print/log ready time
                ready_time = 0
                current_time = time.time()
                if cloud_node.assigned_tasks:
                    latest_completion = max(
                        task_info['start_time'] + task_info['processing_time']
                        for task_info in cloud_node.assigned_tasks
                    )
                    ready_time = max(0, latest_completion - current_time)
                print(f"\nReady Time: {ready_time*1000:.2f}ms")
                print(f"  - Current Load: {status.get('current_load', '0%')}")
                print(f"  - Available MIPS: {status.get('available_mips', cloud_node.mips)}")
                
                mct_logger.info(f"\nReady Time: {ready_time*1000:.2f}ms")
                mct_logger.info(f"  - Current Load: {status.get('current_load', '0%')}")
                mct_logger.info(f"  - Available MIPS: {status.get('available_mips', cloud_node.mips)}")
                
                # Print/log total completion time
                total_completion_time = transmission_time + processing_time + queue_time + ready_time
                print(f"\nTotal Completion Time: {total_completion_time*1000:.2f}ms")
                print(f"  - Transmission: {transmission_time*1000:.2f}ms")
                print(f"  - Processing: {processing_time*1000:.2f}ms")
                print(f"  - Queue: {queue_time*1000:.2f}ms")
                print(f"  - Ready: {ready_time*1000:.2f}ms")
                
                mct_logger.info(f"\nTotal Completion Time: {total_completion_time*1000:.2f}ms")
                mct_logger.info(f"  - Transmission: {transmission_time*1000:.2f}ms")
                mct_logger.info(f"  - Processing: {processing_time*1000:.2f}ms")
                mct_logger.info(f"  - Queue: {queue_time*1000:.2f}ms")
                mct_logger.info(f"  - Ready: {ready_time*1000:.2f}ms")
                
                # Print/log resource availability
                print("\nResource Availability:")
                print(f"  - Available MIPS: {status.get('available_mips', cloud_node.mips)}/{cloud_node.mips}")
                print(f"  - Available Memory: {status.get('available_memory', cloud_node.memory)}/{cloud_node.memory}")
                print(f"  - Available Bandwidth: {status.get('available_bandwidth', cloud_node.bandwidth)}/{cloud_node.bandwidth}")
                print(f"  - Available Storage: {status.get('available_storage', cloud_node.storage)}/{cloud_node.storage}")
                print("-" * 80)
                
                mct_logger.info("\nResource Availability:")
                mct_logger.info(f"  - Available MIPS: {status.get('available_mips', cloud_node.mips)}/{cloud_node.mips}")
                mct_logger.info(f"  - Available Memory: {status.get('available_memory', cloud_node.memory)}/{cloud_node.memory}")
                mct_logger.info(f"  - Available Bandwidth: {status.get('available_bandwidth', cloud_node.bandwidth)}/{cloud_node.bandwidth}")
                mct_logger.info(f"  - Available Storage: {status.get('available_storage', cloud_node.storage)}/{cloud_node.storage}")
                mct_logger.info("-" * 80)
                
                mct_logger.info(f"\nAttempting to assign task {task['Name']} to cloud node {cloud_node.name}")
                
                success, processing_time = cloud_node.assign_task(task)
                if success:
                    mct_logger.info(f"Task successfully assigned to cloud node {cloud_node.name}")
                    print(f"\nTask assigned to cloud node {cloud_node.name}")
                    task_assigned = True
                else:
                    # Queue task at cloud node
                    mct_logger.info(f"Cloud node {cloud_node.name} cannot handle task immediately - queuing")
                    print(f"\nTask queued at cloud node {cloud_node.name}")
                    task_queue_times[task['Name']] = time.time()
                    queued_tasks_info[task['Name']] = {
                        'node': cloud_node.name,
                        'node_type': 'Cloud',
                        'completion_time': best_cloud['completion_time'],
                        'queue_position': len(cloud_node.task_queue)
                    }
                    task_assigned = True
        
        if not task_assigned:
            mct_logger.error(f"Failed to assign or queue task {task['Name']}")
            print(f"\nError: Could not assign or queue task {task['Name']}")
        
        print("-" * 40)
    
    # Wait for all tasks to complete
    while completed_tasks_count < total_tasks:
        time.sleep(0.1)
    
    # Log final statistics
    if task_completion_info:
        # Calculate statistics by node type
        cloud_tasks = [info for info in task_completion_info.values() if info['node'] in cloud_nodes]
        fog_tasks = [info for info in task_completion_info.values() if info['node'] in fog_nodes]
        
        # Calculate Overall Statistics
        total_transmission = sum(info.get('transmission_time', 0) for info in task_completion_info.values())
        total_processing = sum(info.get('processing_time', 0) for info in task_completion_info.values())
        total_queue = sum(info.get('queue_time', 0) for info in task_completion_info.values())
        total_time = sum(info.get('total_time', 0) for info in task_completion_info.values())
        total_storage = sum(info.get('storage_used', 0) for info in task_completion_info.values())
        
        # Calculate total workload (MIPS * processing time for each task)
        total_workload = sum(info['task']['MIPS'] * info.get('processing_time', 0) for info in task_completion_info.values())
        
        # Calculate total size and bandwidth
        total_size = sum(info['task']['Size'] for info in task_completion_info.values())
        total_bandwidth = sum(info['task']['BW'] for info in task_completion_info.values())
        
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
            cloud_node_queue_times = {}
            for info in cloud_tasks:
                node_name = info['node']
                if node_name not in cloud_node_workloads:
                    cloud_node_workloads[node_name] = 0
                    cloud_node_storage[node_name] = 0
                    cloud_node_size[node_name] = 0
                    cloud_node_bandwidth[node_name] = 0
                    cloud_node_queue_times[node_name] = []
                cloud_node_workloads[node_name] += info['task']['MIPS'] * info.get('processing_time', 0)
                cloud_node_storage[node_name] += info.get('storage_used', 0)
                cloud_node_size[node_name] += info['task']['Size']
                cloud_node_bandwidth[node_name] += info['task']['BW']
                cloud_node_queue_times[node_name].append(info.get('queue_time', 0))
            
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
            fog_node_queue_times = {}
            for info in fog_tasks:
                node_name = info['node']
                if node_name not in fog_node_workloads:
                    fog_node_workloads[node_name] = 0
                    fog_node_storage[node_name] = 0
                    fog_node_size[node_name] = 0
                    fog_node_bandwidth[node_name] = 0
                    fog_node_queue_times[node_name] = []
                fog_node_workloads[node_name] += info['task']['MIPS'] * info.get('processing_time', 0)
                fog_node_storage[node_name] += info.get('storage_used', 0)
                fog_node_size[node_name] += info['task']['Size']
                fog_node_bandwidth[node_name] += info['task']['BW']
                fog_node_queue_times[node_name].append(info.get('queue_time', 0))
            
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
        mct_logger.info("\n=== Overall Statistics ===")
        mct_logger.info(f"Total Tasks: {len(task_completion_info)}")
        mct_logger.info(f"  Cloud Tasks: {len(cloud_tasks)}")
        mct_logger.info(f"  Fog Tasks: {len(fog_tasks)}")
        mct_logger.info("\nWorkload:")
        mct_logger.info(f"  Total System Workload: {total_workload:.2f} MIPS-seconds")
        mct_logger.info(f"  Average Workload per Task: {avg_workload:.2f} MIPS-seconds")
        mct_logger.info("\nResource Usage:")
        mct_logger.info(f"  Total Storage Used: {total_storage:.2f} GB")
        mct_logger.info(f"  Total Data Size: {total_size:.2f} MI")
        mct_logger.info(f"  Total Bandwidth Used: {total_bandwidth:.2f} Mbps")
        mct_logger.info(f"  Average Storage per Task: {avg_storage:.2f} GB")
        mct_logger.info(f"  Average Size per Task: {avg_size:.2f} MI")
        mct_logger.info(f"  Average Bandwidth per Task: {avg_bandwidth:.2f} Mbps")
        mct_logger.info("\nTransmission Time:")
        mct_logger.info(f"  Total: {total_transmission*1000:.6f}ms")
        mct_logger.info(f"  Average: {avg_transmission*1000:.6f}ms")
        mct_logger.info("\nProcessing Time:")
        mct_logger.info(f"  Total: {total_processing*1000:.6f}ms")
        mct_logger.info(f"  Average: {avg_processing*1000:.6f}ms")
        mct_logger.info("\nQueue Time:")
        mct_logger.info(f"  Total: {total_queue*1000:.6f}ms")
        mct_logger.info(f"  Average: {avg_queue*1000:.6f}ms")
        mct_logger.info("\nTotal Time:")
        mct_logger.info(f"  Total: {total_time*1000:.6f}ms")
        mct_logger.info(f"  Average: {avg_total*1000:.6f}ms")
        
        # Log Fog Statistics
        if fog_tasks:
            mct_logger.info("\n=== Overall Fog Statistics ===")
            mct_logger.info(f"Total Fog Tasks: {len(fog_tasks)}")
            mct_logger.info("\nWorkload:")
            mct_logger.info(f"  Total Fog Workload: {fog_workload:.2f} MIPS-seconds")
            mct_logger.info(f"  Average Workload per Fog Task: {avg_fog_workload:.2f} MIPS-seconds")
            mct_logger.info("\nResource Usage:")
            mct_logger.info(f"  Total Fog Storage: {fog_storage:.2f} GB")
            mct_logger.info(f"  Total Fog Data Size: {fog_size:.2f} MI")
            mct_logger.info(f"  Total Fog Bandwidth: {fog_bandwidth:.2f} Mbps")
            mct_logger.info(f"  Average Storage per Fog Task: {avg_fog_storage:.2f} GB")
            mct_logger.info(f"  Average Size per Fog Task: {avg_fog_size:.2f} MI")
            mct_logger.info(f"  Average Bandwidth per Fog Task: {avg_fog_bandwidth:.2f} Mbps")
            mct_logger.info("\nPer Node Statistics:")
            for node_name in fog_node_workloads:
                mct_logger.info(f"\n  {node_name}:")
                mct_logger.info(f"    Workload: {fog_node_workloads[node_name]:.2f} MIPS-seconds")
                mct_logger.info(f"    Storage Used: {fog_node_storage[node_name]:.2f} GB")
                mct_logger.info(f"    Data Size: {fog_node_size[node_name]:.2f} MI")
                mct_logger.info(f"    Bandwidth Used: {fog_node_bandwidth[node_name]:.2f} Mbps")
                if fog_node_queue_times[node_name]:
                    avg_queue_time = sum(fog_node_queue_times[node_name]) / len(fog_node_queue_times[node_name])
                    mct_logger.info(f"    Average Queue Time: {avg_queue_time*1000:.6f}ms")
            mct_logger.info("\nTransmission Time:")
            mct_logger.info(f"  Total: {fog_transmission*1000:.6f}ms")
            mct_logger.info(f"  Average: {avg_fog_transmission*1000:.6f}ms")
            mct_logger.info("\nProcessing Time:")
            mct_logger.info(f"  Total: {fog_processing*1000:.6f}ms")
            mct_logger.info(f"  Average: {avg_fog_processing*1000:.6f}ms")
            mct_logger.info("\nQueue Time:")
            mct_logger.info(f"  Total: {fog_queue*1000:.6f}ms")
            mct_logger.info(f"  Average: {avg_fog_queue*1000:.6f}ms")
            mct_logger.info("\nTotal Time:")
            mct_logger.info(f"  Total: {fog_total*1000:.6f}ms")
            mct_logger.info(f"  Average: {avg_fog_total*1000:.6f}ms")
        
        # Log Cloud Statistics
        if cloud_tasks:
            mct_logger.info("\n=== Overall Cloud Statistics ===")
            mct_logger.info(f"Total Cloud Tasks: {len(cloud_tasks)}")
            mct_logger.info("\nWorkload:")
            mct_logger.info(f"  Total Cloud Workload: {cloud_workload:.2f} MIPS-seconds")
            mct_logger.info(f"  Average Workload per Cloud Task: {avg_cloud_workload:.2f} MIPS-seconds")
            mct_logger.info("\nResource Usage:")
            mct_logger.info(f"  Total Cloud Storage: {cloud_storage:.2f} GB")
            mct_logger.info(f"  Total Cloud Data Size: {cloud_size:.2f} MI")
            mct_logger.info(f"  Total Cloud Bandwidth: {cloud_bandwidth:.2f} Mbps")
            mct_logger.info(f"  Average Storage per Cloud Task: {avg_cloud_storage:.2f} GB")
            mct_logger.info(f"  Average Size per Cloud Task: {avg_cloud_size:.2f} MI")
            mct_logger.info(f"  Average Bandwidth per Cloud Task: {avg_cloud_bandwidth:.2f} Mbps")
            mct_logger.info("\nPer Node Statistics:")
            for node_name in cloud_node_workloads:
                mct_logger.info(f"\n  {node_name}:")
                mct_logger.info(f"    Workload: {cloud_node_workloads[node_name]:.2f} MIPS-seconds")
                mct_logger.info(f"    Storage Used: {cloud_node_storage[node_name]:.2f} GB")
                mct_logger.info(f"    Data Size: {cloud_node_size[node_name]:.2f} MI")
                mct_logger.info(f"    Bandwidth Used: {cloud_node_bandwidth[node_name]:.2f} Mbps")
                if cloud_node_queue_times[node_name]:
                    avg_queue_time = sum(cloud_node_queue_times[node_name]) / len(cloud_node_queue_times[node_name])
                    mct_logger.info(f"    Average Queue Time: {avg_queue_time*1000:.6f}ms")
            mct_logger.info("\nTransmission Time:")
            mct_logger.info(f"  Total: {cloud_transmission*1000:.6f}ms")
            mct_logger.info(f"  Average: {avg_cloud_transmission*1000:.6f}ms")
            mct_logger.info("\nProcessing Time:")
            mct_logger.info(f"  Total: {cloud_processing*1000:.6f}ms")
            mct_logger.info(f"  Average: {avg_cloud_processing*1000:.6f}ms")
            mct_logger.info("\nQueue Time:")
            mct_logger.info(f"  Total: {cloud_queue*1000:.6f}ms")
            mct_logger.info(f"  Average: {avg_cloud_queue*1000:.6f}ms")
            mct_logger.info("\nTotal Time:")
            mct_logger.info(f"  Total: {cloud_total*1000:.6f}ms")
            mct_logger.info(f"  Average: {avg_cloud_total*1000:.6f}ms")

if __name__ == "__main__":
    # Load tasks from input
    tasks = read_and_log_tuples()
    
    # Print debug information
    print(f"\nDebug: Number of tasks received: {len(tasks)}")
    mct_logger.info(f"Number of tasks received: {len(tasks)}")
    
    # Process tasks using MCT algorithm
    if tasks:
        process_mct(tasks)
    else:
        print("Error: No tasks were loaded")
        mct_logger.error("No tasks were loaded.")
