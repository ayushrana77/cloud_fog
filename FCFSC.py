"""
FCFSC (First Come First Serve with Cloud) Task Scheduling Implementation
This module implements a modified FCFS algorithm that prioritizes cloud resources for certain tasks.
It extends the basic FCFS approach with cloud-specific optimizations and fallback mechanisms.
"""

import json
import time
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG, CLOUD_SERVICES_CONFIG
from utility import calculate_distance
from fog import get_fog_node, get_fog_node_status, get_all_fog_nodes
from cloud import get_cloud_node, get_cloud_node_status, get_all_cloud_nodes
from logger import setup_logger

# Initialize logger for FCFSC events
fcfsc_logger = setup_logger('fcfsc', 'FCFSC.log')

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

def process_fcfs(tasks):
    """
    Process tasks using FCFSC algorithm with hybrid fog-cloud approach
    
    This function implements the main FCFSC scheduling logic:
    1. Tasks are sorted by creation time
    2. Each task is assigned to either fog or cloud based on data type
    3. Cloud resources are prioritized for certain task types
    4. Resources are allocated and tasks are processed
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
        fcfsc_logger.info(f"\nTask Completed: {task_name}")
        fcfsc_logger.info(f"  Completed at: {node_name}")
        fcfsc_logger.info(f"  Transmission Time: {completion_info['transmission_time']:.2f}s")
        fcfsc_logger.info(f"  Queue Time: {completion_info['queue_time']:.2f}s")
        fcfsc_logger.info(f"  Processing Time: {completion_info['processing_time']:.2f}s")
        fcfsc_logger.info(f"  Total Time: {completion_info['total_time']:.2f}s")
        fcfsc_logger.info("  " + "-" * 30)
        
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
                    # Try one other fog node if available
                    fcfsc_logger.info(f"Fog node {first_fog_name} cannot handle task - trying one other fog node")
                    print(f"\nFog node {first_fog_name} cannot handle task due to resource constraints")
                    
                    if len(sorted_fog_nodes) > 1:
                        second_fog_name, second_fog_distance = sorted_fog_nodes[1]
                        fog_node = get_fog_node(second_fog_name)
                        if fog_node:
                            fcfsc_logger.info(f"Attempting to assign task to fog node {second_fog_name} ({second_fog_distance:.2f} km)")
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
                                # Try cloud nodes if both fog nodes failed
                                fcfsc_logger.info(f"Second fog node {second_fog_name} also cannot handle task - attempting cloud fallback")
                                print(f"\nSecond fog node {second_fog_name} cannot handle task - attempting cloud fallback")
                                
                                # Attempt cloud assignment
                                for cloud_name, cloud_distance in sorted_cloud_nodes:
                                    cloud_node = get_cloud_node(cloud_name)
                                    if cloud_node:
                                        fcfsc_logger.info(f"Attempting to assign task to cloud node {cloud_name} ({cloud_distance:.2f} km)")
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
                    else:
                        # If only one fog node exists, go straight to cloud
                        fcfsc_logger.info("No other fog nodes available - attempting cloud fallback")
                        print("\nNo other fog nodes available - attempting cloud fallback")
                        
                        # Attempt cloud assignment
                        for cloud_name, cloud_distance in sorted_cloud_nodes:
                            cloud_node = get_cloud_node(cloud_name)
                            if cloud_node:
                                fcfsc_logger.info(f"Attempting to assign task to cloud node {cloud_name} ({cloud_distance:.2f} km)")
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
        else:
            # Direct cloud assignment for cloud tasks
            fcfsc_logger.info(f"\nTask {task['Name']} is a cloud task - attempting cloud nodes directly")
            for cloud_name, distance in sorted_cloud_nodes:
                cloud_node = get_cloud_node(cloud_name)
                if cloud_node:
                    fcfsc_logger.info(f"Attempting to assign task to cloud node {cloud_name} ({distance:.2f} km)")
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
            fcfsc_logger.info(f"  Task Size: {info['task_size']} MI")
            fcfsc_logger.info(f"  Required Resources:")
            fcfsc_logger.info(f"    MIPS: {info['required_mips']}")
            fcfsc_logger.info(f"    RAM: {info['required_ram']}")
            fcfsc_logger.info(f"    Bandwidth: {info['required_bw']}")
    
    # Log final statistics
    if task_completion_info:
        # Sort completions by time for consistent logging
        sorted_completions = sorted(task_completion_info.items(), 
                                  key=lambda x: x[1]['completion_time'])
        
        # Log completion details
        fcfsc_logger.info("\n=== Task Completion Summary ===")
        for task_name, info in sorted_completions:
            fcfsc_logger.info(f"\nTask Completed: {task_name}")
            fcfsc_logger.info(f"  Completed at: {info['node']}")
            fcfsc_logger.info(f"  Transmission Time: {info['transmission_time']:.2f}s")
            fcfsc_logger.info(f"  Queue Time: {info['queue_time']:.2f}s")
            fcfsc_logger.info(f"  Processing Time: {info['processing_time']:.2f}s")
            fcfsc_logger.info(f"  Total Time: {info['total_time']:.2f}s")
            fcfsc_logger.info("  " + "-" * 30)
        
        # Calculate and log performance metrics
        avg_transmission = sum(info['transmission_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_queue = sum(info['queue_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_processing = sum(info['processing_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_total = sum(info['total_time'] for info in task_completion_info.values()) / len(task_completion_info)
        
        # Calculate statistics by node type
        cloud_tasks = [info for info in task_completion_info.values() if info['node'] in cloud_nodes]
        fog_tasks = [info for info in task_completion_info.values() if info['node'] in fog_nodes]
        
        # Log final statistics
        fcfsc_logger.info("\n=== Final Overall Statistics ===")
        fcfsc_logger.info(f"Total Tasks Completed: {len(task_completion_info)}")
        fcfsc_logger.info(f"  Cloud Tasks: {len(cloud_tasks)}")
        fcfsc_logger.info(f"  Fog Tasks: {len(fog_tasks)}")
        fcfsc_logger.info(f"Average Transmission Time: {avg_transmission:.2f}s")
        fcfsc_logger.info(f"Average Queue Time: {avg_queue:.2f}s")
        fcfsc_logger.info(f"Average Processing Time: {avg_processing:.2f}s")
        fcfsc_logger.info(f"Average Total Time: {avg_total:.2f}s")

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