"""
FCFS (First Come First Serve) Task Scheduling Implementation
-------------------------------------------------------------------------------
This module implements a hybrid fog-cloud task scheduling algorithm using the FCFS approach.
Tasks are processed in the order they arrive, with consideration for fog and cloud resources.

Key Features:
- Implements traditional FCFS (First Come First Serve) scheduling
- Enhanced with geographical awareness for selecting nearest nodes
- Hybrid approach that leverages both fog and cloud computing resources
- Automatic fallback from fog to cloud when resources are constrained
- Comprehensive performance metrics tracking and logging
- Task type-based assignment (small tasks to fog, bulk/large to cloud)

Performance Metrics Tracked:
- Transmission time: Time to transmit task data to the processing node
- Queue time: Time spent waiting in a node's queue
- Processing time: Actual computation time on the node
- Total time: Complete end-to-end task completion time

Author: [Your Name]
Date: June 2025
Version: 1.0
"""

import json
import time
from task_load import read_and_log_tuples  # Module for loading task data from input files
from config import FOG_NODES_CONFIG, CLOUD_SERVICES_CONFIG  # Configuration constants for nodes
from utility import calculate_distance  # Utility function for geographical distance calculation
from fog import get_fog_node, get_fog_node_status, get_all_fog_nodes  # Fog node management
from cloud import get_cloud_node, get_cloud_node_status, get_all_cloud_nodes  # Cloud node management
from logger import setup_logger  # Custom logging configuration

# Initialize logger for FCFS events
fcfs_logger = setup_logger('fcfs', 'FCFS.log')

def calculate_task_distances(task, is_cloud=False):
    """
    Calculate geographical distances from task to all available nodes (fog or cloud)
    
    This function computes the distance between a task's geographical location and all
    available nodes of the specified type (fog or cloud). These distances are used to
    select the nearest node for task assignment, which reduces latency and improves
    overall system performance.
    
    Args:
        task (dict): Task information containing location data in GeoLocation field
                    (expected format: {'GeoLocation': {'latitude': float, 'longitude': float}})
        is_cloud (bool): Flag to determine target node type - True for cloud nodes, False for fog nodes
        
    Returns:
        dict: Dictionary mapping node names to their distances (in km) from the task
              Format: {'node_name': distance_in_km, ...}
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

def process_fcfs(tasks):
    """
    Process tasks using FCFS algorithm with hybrid fog-cloud approach
    
    This function implements the main FCFS scheduling logic:
    1. Tasks are sorted by creation time (First Come First Serve principle)
    2. For each task, its geographical distance to available nodes is calculated
    3. Each task is assigned based on a hybrid strategy:
       - Tasks with 'Small' data type are first attempted on nearest fog nodes
       - Tasks with 'Bulk' or 'Large' data types are directly sent to cloud
       - If fog nodes lack resources, automatic fallback to cloud occurs
    4. Resource allocation is tracked and task processing is simulated
    5. Comprehensive performance metrics are tracked and logged
    6. Final statistics are calculated to evaluate system performance
    
    The function handles the complete task scheduling lifecycle:
    - Node initialization
    - Task sorting and processing
    - Resource allocation and constraint checking
    - Queueing when immediate processing is not possible
    - Waiting for task completion
    - Performance metric collection and statistical analysis
    
    Args:
        tasks (list): List of tasks to be processed, each containing metadata
                     like creation time, resource requirements, and location
    """
    if not tasks:
        print("No tasks to process")
        fcfs_logger.warning("No tasks to process.")
        return
        
    print("\n=== FCFS Hybrid Processing (Fog-Cloud) ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    fcfs_logger.info(f"Total Tasks to Process: {len(tasks)}")
    
    # Initialize and log fog nodes
    fog_nodes = get_all_fog_nodes()
    for name, node in fog_nodes.items():
        fcfs_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Location={node.location})")
    
    # Initialize and log cloud nodes
    cloud_nodes = get_all_cloud_nodes()
    for name, node in cloud_nodes.items():
        fcfs_logger.info(f"Cloud node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Location={node.location})")
    
    # Initialize tracking variables
    task_completion_info = {}  # Store completion details for each task
    queued_tasks_info = {}     # Track tasks waiting in queue
    completed_tasks_count = 0  # Counter for completed tasks
    total_tasks = len(tasks)   # Total number of tasks to process
    
    def task_completion_callback(node_name, completion_info):
        """
        Callback function to handle task completion events
        
        This callback is executed when a task finishes processing on any node.
        It updates statistics, logs completion information, and manages the 
        tracking of completed tasks. This function is essential for monitoring 
        the progress of task execution across the system and collecting 
        performance metrics.
        
        Args:
            node_name (str): Name of the node that completed the task
            completion_info (dict): Information about the completed task including:
                                    - task: The original task dictionary
                                    - transmission_time: Time to transfer data to node
                                    - queue_time: Time spent waiting in queue
                                    - processing_time: Actual execution time
                                    - total_time: Overall time from submission to completion
                                    - completion_time: Timestamp when task completed
        """
        nonlocal completed_tasks_count
        task = completion_info['task']
        task_name = task['Name']
        
        # Store completion information
        task_completion_info[task_name] = {
            'node': node_name,
            'transmission_time': completion_info['transmission_time'],
            'queue_time': completion_info['queue_time'],
            'processing_time': completion_info['processing_time'],
            'total_time': completion_info['total_time'],
            'completion_time': completion_info['completion_time']
        }
        
        # Update completion counter and logging
        completed_tasks_count += 1
        fcfs_logger.info(f"\nTask Completed: {task_name}")
        fcfs_logger.info(f"  Completed at: {node_name}")
        fcfs_logger.info(f"  Transmission Time: {completion_info['transmission_time']:.2f}s")
        fcfs_logger.info(f"  Queue Time: {completion_info['queue_time']:.2f}s")
        fcfs_logger.info(f"  Processing Time: {completion_info['processing_time']:.2f}s")
        fcfs_logger.info(f"  Total Time: {completion_info['total_time']:.2f}s")
        fcfs_logger.info("  " + "-" * 30)
        
        # Remove from queued tasks if it was queued
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
        print(f"Size: {task['Size']} MI")
        print(f"MIPS: {task['MIPS']}")
        print(f"RAM: {task['RAM']}")
        print(f"BW: {task['BW']}")
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
        fcfs_logger.info(f"\nTask {task['Name']} - Distance Calculations:")
        fcfs_logger.info("Fog Node Distances:")
        for fog_name, distance in sorted_fog_nodes:
            fcfs_logger.info(f"  {fog_name}: {distance:.2f} km")
        fcfs_logger.info("Cloud Node Distances:")
        for cloud_name, distance in sorted_cloud_nodes:
            fcfs_logger.info(f"  {cloud_name}: {distance:.2f} km")
        
        # Task assignment logic
        task_assigned = False
        if not is_cloud_task:
            # Try fog nodes first for non-cloud tasks
            # This follows our strategy of using nearby fog nodes for small data tasks
            # We iterate through fog nodes in order of increasing distance from the task
            for fog_name, distance in sorted_fog_nodes:
                fog_node = get_fog_node(fog_name)
                if fog_node:
                    fcfs_logger.info(f"\nAttempting to assign task {task['Name']} to nearest fog node {fog_name} ({distance:.2f} km)")
                    # Attempt to assign the task to this fog node, which checks resource availability
                    success, processing_time = fog_node.assign_task(task)
                    if success:
                        # Task successfully assigned to fog node
                        fcfs_logger.info(f"Task successfully assigned to fog node {fog_name}")
                        print(f"\nTask assigned to fog node {fog_name} ({distance:.2f} km)")
                        print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                        print("Fog Node Status:")
                        status = fog_node.get_status()
                        for key, value in status.items():
                            print(f"  {key}: {value}")
                        task_assigned = True
                        break
                    else:
                        # Try cloud nodes if fog node cannot handle task
                        fcfs_logger.info(f"Fog node {fog_name} cannot handle task - attempting cloud fallback")
                        print(f"\nFog node {fog_name} cannot handle task due to resource constraints")
                        print("Attempting to assign to cloud...")
                        
                        # Attempt cloud assignment
                        for cloud_name, cloud_distance in sorted_cloud_nodes:
                            cloud_node = get_cloud_node(cloud_name)
                            if cloud_node:
                                fcfs_logger.info(f"Attempting to assign task to cloud node {cloud_name} ({cloud_distance:.2f} km)")
                                success, processing_time = cloud_node.assign_task(task)
                                if success:
                                    # Task successfully assigned to cloud node
                                    fcfs_logger.info(f"Task successfully assigned to cloud node {cloud_name}")
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
                                    fcfs_logger.info(f"Cloud node {cloud_name} cannot handle task immediately - queuing")
                                    print(f"Task queued at cloud node {cloud_name} ({cloud_distance:.2f} km)")
                                    queued_tasks_info[task['Name']] = {
                                        'node': cloud_name,
                                        'node_type': 'Cloud',
                                        'distance': cloud_distance,
                                        'queue_position': len(cloud_node.task_queue),
                                        'task_size': task['Size'],
                                        'required_mips': task['MIPS'],
                                        'required_ram': task['RAM'],
                                        'required_bw': task['BW']
                                    }
                                    task_assigned = True
                                    break
                        if task_assigned:
                            break
        else:
            # Direct cloud assignment for cloud tasks
            fcfs_logger.info(f"\nTask {task['Name']} is a cloud task - attempting cloud nodes directly")
            for cloud_name, distance in sorted_cloud_nodes:
                cloud_node = get_cloud_node(cloud_name)
                if cloud_node:
                    fcfs_logger.info(f"Attempting to assign task to cloud node {cloud_name} ({distance:.2f} km)")
                    success, processing_time = cloud_node.assign_task(task)
                    if success:
                        # Task successfully assigned to cloud node
                        fcfs_logger.info(f"Task successfully assigned to cloud node {cloud_name}")
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
                        fcfs_logger.info(f"Cloud node {cloud_name} cannot handle task immediately - queuing")
                        print(f"\nTask queued at cloud node {cloud_name} ({distance:.2f} km)")
                        queued_tasks_info[task['Name']] = {
                            'node': cloud_name,
                            'node_type': 'Cloud',
                            'distance': distance,
                            'queue_position': len(cloud_node.task_queue),
                            'task_size': task['Size'],
                            'required_mips': task['MIPS'],
                            'required_ram': task['RAM'],
                            'required_bw': task['BW']
                        }
                        task_assigned = True
                        break
        
        # Log assignment failure if task couldn't be assigned
        if not task_assigned:
            fcfs_logger.error(f"Failed to assign or queue task {task['Name']}")
            print(f"\nError: Could not assign or queue task {task['Name']}")
        
        print("-" * 40)
    
    # Wait for all tasks to complete
    # This loop continues until all tasks have been processed and callbacks executed
    # The sleep prevents excessive CPU usage while waiting for background task processing
    while completed_tasks_count < total_tasks:
        time.sleep(0.1)  # Small delay to prevent CPU overuse
    
    # Log queued tasks information - this may still contain tasks that were queued but are now processing
    if queued_tasks_info:
        fcfs_logger.info("\n=== Queued Tasks Summary ===")
        for task_name, info in queued_tasks_info.items():
            fcfs_logger.info(f"\nQueued Task: {task_name}")
            fcfs_logger.info(f"  Queued at: {info['node']} ({info['node_type']}, distance={info['distance']:.2f} km)")
            fcfs_logger.info(f"  Queue Position: {info['queue_position']}")
            fcfs_logger.info(f"  Task Size: {info['task_size']} MI")
            fcfs_logger.info(f"  Required Resources:")
            fcfs_logger.info(f"    MIPS: {info['required_mips']}")
            fcfs_logger.info(f"    RAM: {info['required_ram']}")
            fcfs_logger.info(f"    Bandwidth: {info['required_bw']}")
    
    # Log final statistics
    if task_completion_info:
        # Sort completions by time for consistent logging
        sorted_completions = sorted(task_completion_info.items(), 
                                  key=lambda x: x[1]['completion_time'])
        
        # Log completion details
        fcfs_logger.info("\n=== Task Completion Summary ===")
        for task_name, info in sorted_completions:
            fcfs_logger.info(f"\nTask Completed: {task_name}")
            fcfs_logger.info(f"  Completed at: {info['node']}")
            fcfs_logger.info(f"  Transmission Time: {info['transmission_time']:.2f}s")
            fcfs_logger.info(f"  Queue Time: {info['queue_time']:.2f}s")
            fcfs_logger.info(f"  Processing Time: {info['processing_time']:.2f}s")
            fcfs_logger.info(f"  Total Time: {info['total_time']:.2f}s")
            fcfs_logger.info("  " + "-" * 30)
        
        # Calculate and log performance metrics
        # Compute average time values for key performance indicators across all completed tasks
        # These metrics help evaluate the efficiency of the scheduling algorithm
        avg_transmission = sum(info['transmission_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_queue = sum(info['queue_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_processing = sum(info['processing_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_total = sum(info['total_time'] for info in task_completion_info.values()) / len(task_completion_info)
        
        # Calculate statistics by node type
        # Segregate tasks by the type of node they were executed on (cloud vs. fog)
        # This helps analyze the load distribution and performance differences between node types
        cloud_tasks = [info for info in task_completion_info.values() if info['node'] in cloud_nodes]
        fog_tasks = [info for info in task_completion_info.values() if info['node'] in fog_nodes]
        
        # Log final statistics
        fcfs_logger.info("\n=== Final Overall Statistics ===")
        fcfs_logger.info(f"Total Tasks Completed: {len(task_completion_info)}")
        fcfs_logger.info(f"  Cloud Tasks: {len(cloud_tasks)}")
        fcfs_logger.info(f"  Fog Tasks: {len(fog_tasks)}")
        fcfs_logger.info(f"Average Transmission Time: {avg_transmission:.2f}s")
        fcfs_logger.info(f"Average Queue Time: {avg_queue:.2f}s")
        fcfs_logger.info(f"Average Processing Time: {avg_processing:.2f}s")
        fcfs_logger.info(f"Average Total Time: {avg_total:.2f}s")

if __name__ == "__main__":
    # Load tasks from input files using the task_load module
    # The read_and_log_tuples function handles parsing the JSON task data
    # and creates the necessary data structures for processing
    tasks = read_and_log_tuples()
    
    # Print and log debug information about the number of tasks received
    # This helps identify potential issues with task loading early on
    print(f"\nDebug: Number of tasks received: {len(tasks)}")
    fcfs_logger.info(f"Number of tasks received: {len(tasks)}")
    
    # Process tasks using the FCFS algorithm if tasks were successfully loaded
    # This is the main entry point for the scheduling algorithm
    if tasks:
        process_fcfs(tasks)
    else:
        # Handle the error case where no tasks were loaded
        # This could happen due to file access issues or empty input files
        print("Error: No tasks were loaded")
        fcfs_logger.error("No tasks were loaded.")