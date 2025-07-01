"""
FCFS (First Come First Serve) Cloud Task Scheduling Implementation
This module implements a cloud-only task scheduling algorithm using the FCFS approach.
Tasks are processed in the order they arrive, with consideration for cloud resources.
"""

import json
import time
from task_load import read_and_log_tuples
from config import CLOUD_SERVICES_CONFIG
from utility import calculate_distance, calculate_storage_requirements
from cloud import get_cloud_node, get_cloud_node_status, get_all_cloud_nodes
from logger import setup_logger

# Initialize logger for FCFS events
fcfs_logger = setup_logger('fcfs_cloud', 'fcfs_cloud.log', sub_directory='algorithms')

def calculate_task_distances(task):
    """
    Calculate distances from task to all cloud nodes
    
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
    
    for node in CLOUD_SERVICES_CONFIG:
        node_location = node['location']
        distance = calculate_distance(task_location, node_location)
        distances[node['name']] = distance
    
    return distances

def process_fcfs_cloud(tasks):
    """
    Process tasks using FCFS algorithm with cloud-only approach
    
    This function implements the main FCFS scheduling logic:
    1. Tasks are sorted by creation time
    2. Each task is assigned to cloud nodes based on storage requirements
    3. Resources are allocated and tasks are processed
    4. Performance metrics are tracked and logged
    
    Args:
        tasks (list): List of tasks to be processed
    """
    if not tasks:
        print("No tasks to process")
        fcfs_logger.warning("No tasks to process.")
        return
        
    print("\n=== FCFS Cloud Processing ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    fcfs_logger.info(f"Total Tasks to Process: {len(tasks)}")
    
    # Initialize and log cloud nodes
    cloud_nodes = get_all_cloud_nodes()
    for name, node in cloud_nodes.items():
        fcfs_logger.info(f"Cloud node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Storage={node.storage}GB)")
    
    # Initialize tracking variables
    task_completion_info = {}  # Store completion details for each task
    queued_tasks_info = {}     # Track tasks waiting in queue
    completed_tasks_count = 0  # Counter for completed tasks
    total_tasks = len(tasks)   # Total number of tasks to process
    
    def task_completion_callback(cloud_name, completion_info):
        """
        Callback function to handle task completion events
        
        Args:
            cloud_name (str): Name of the cloud node that completed the task
            completion_info (dict): Information about the completed task
        """
        nonlocal completed_tasks_count
        task = completion_info['task']
        task_name = task['Name']
        
        # Store completion information
        task_completion_info[task_name] = {
            'node': cloud_name,
            'transmission_time': completion_info['transmission_time'],
            'queue_time': completion_info['queue_time'],
            'processing_time': completion_info['processing_time'],
            'total_time': completion_info['total_time'],
            'completion_time': completion_info['completion_time'],
            'storage_used': task.get('Storage', 0)
        }
        
        # Update completion counter and logging
        completed_tasks_count += 1
        fcfs_logger.info(f"\nTask Completed: {task_name}")
        fcfs_logger.info(f"  Completed at: {cloud_name}")
        fcfs_logger.info(f"  Transmission Time: {completion_info['transmission_time']:.2f}s")
        fcfs_logger.info(f"  Queue Time: {completion_info['queue_time']:.2f}s")
        fcfs_logger.info(f"  Processing Time: {completion_info['processing_time']:.2f}s")
        fcfs_logger.info(f"  Storage Used: {task.get('Storage', 0):.2f}GB")
        fcfs_logger.info(f"  Total Time: {completion_info['total_time']:.2f}s")
        fcfs_logger.info("  " + "-" * 30)
        
        # Remove from queued tasks if it was queued
        if task_name in queued_tasks_info:
            del queued_tasks_info[task_name]
    
    # Register completion callback for all cloud nodes
    for node in cloud_nodes.values():
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
        
        # Calculate distances to cloud nodes
        cloud_distances = calculate_task_distances(task)
        
        # Sort cloud nodes by distance for optimal assignment
        sorted_cloud_nodes = sorted(cloud_distances.items(), key=lambda x: x[1])
        
        # Log distance calculations
        fcfs_logger.info(f"\nTask {task['Name']} - Distance Calculations:")
        fcfs_logger.info("Cloud Node Distances:")
        for cloud_name, distance in sorted_cloud_nodes:
            fcfs_logger.info(f"  {cloud_name}: {distance:.2f} km")
        
        # Task assignment logic
        task_assigned = False
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
                        'required_bw': task['BW'],
                        'required_storage': task['Storage']
                    }
                    task_assigned = True
                    break
        
        # Log assignment failure if task couldn't be assigned
        if not task_assigned:
            fcfs_logger.error(f"Failed to assign or queue task {task['Name']}")
            print(f"\nError: Could not assign or queue task {task['Name']}")
        
        print("-" * 40)
    
    # Wait for all tasks to complete
    while completed_tasks_count < total_tasks:
        time.sleep(0.1)  # Small delay to prevent CPU overuse
    
    # Log queued tasks information
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
            fcfs_logger.info(f"    Storage: {info['required_storage']}GB")
    
    # Log final statistics
    if task_completion_info:
        # Calculate and log performance metrics
        total_transmission = sum(info['transmission_time'] for info in task_completion_info.values())
        total_processing = sum(info['processing_time'] for info in task_completion_info.values())
        total_queue = sum(info['queue_time'] for info in task_completion_info.values())
        total_time = sum(info['total_time'] for info in task_completion_info.values())
        total_storage = sum(info['storage_used'] for info in task_completion_info.values())
        
        avg_transmission = total_transmission / len(task_completion_info)
        avg_processing = total_processing / len(task_completion_info)
        avg_queue = total_queue / len(task_completion_info)
        avg_total = total_time / len(task_completion_info)
        avg_storage = total_storage / len(task_completion_info)
        
        # Log final statistics
        fcfs_logger.info("\n=== Final Overall Statistics ===")
        fcfs_logger.info(f"Total Tasks Completed: {len(task_completion_info)}")
        fcfs_logger.info("\nTransmission Time:")
        fcfs_logger.info(f"  Total: {total_transmission*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_transmission*1000:.6f}ms")
        fcfs_logger.info("\nProcessing Time:")
        fcfs_logger.info(f"  Total: {total_processing*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_processing*1000:.6f}ms")
        fcfs_logger.info("\nQueue Time:")
        fcfs_logger.info(f"  Total: {total_queue*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_queue*1000:.6f}ms")
        fcfs_logger.info("\nStorage Usage:")
        fcfs_logger.info(f"  Total: {total_storage:.2f}GB")
        fcfs_logger.info(f"  Average: {avg_storage:.2f}GB")
        fcfs_logger.info("\nTotal Time:")
        fcfs_logger.info(f"  Total: {total_time*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_total*1000:.6f}ms")

if __name__ == "__main__":
    # Load tasks from input
    tasks = read_and_log_tuples()
    
    # Print debug information
    print(f"\nDebug: Number of tasks received: {len(tasks)}")
    fcfs_logger.info(f"Number of tasks received: {len(tasks)}")
    
    # Process tasks using FCFS algorithm
    if tasks:
        process_fcfs_cloud(tasks)
    else:
        print("Error: No tasks were loaded")
        fcfs_logger.error("No tasks were loaded.") 