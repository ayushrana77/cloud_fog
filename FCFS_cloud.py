import json
import time
from task_load import read_and_log_tuples
from config import CLOUD_SERVICES_CONFIG
from utility import calculate_distance
from cloud import get_cloud_node, get_cloud_node_status, get_all_cloud_nodes
from logger import setup_logger

# Logger for FCFS Cloud events
fcfs_cloud_logger = setup_logger('fcfs_cloud', 'FCFS_cloud.log')

def calculate_task_distances(task):
    """
    Calculate distances from task to all cloud nodes
    """
    distances = {}
    task_location = {
        'lat': task['GeoLocation']['latitude'],
        'lon': task['GeoLocation']['longitude']
    }
    
    for cloud_node in CLOUD_SERVICES_CONFIG:
        cloud_location = cloud_node['location']
        distance = calculate_distance(task_location, cloud_location)
        distances[cloud_node['name']] = distance
    
    return distances

def process_fcfs_cloud(tasks):
    """
    Process tasks using FCFS algorithm with cloud nodes
    """
    if not tasks:
        print("No tasks to process")
        fcfs_cloud_logger.warning("No tasks to process.")
        return
        
    print("\n=== FCFS Cloud Processing ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    fcfs_cloud_logger.info(f"Total Tasks to Process: {len(tasks)}")
    
    # Log cloud node creation
    cloud_nodes = get_all_cloud_nodes()
    for name, node in cloud_nodes.items():
        fcfs_cloud_logger.info(f"Cloud node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Location={node.location})")
    
    # Dictionary to store task information
    task_completion_info = {}
    queued_tasks_info = {}
    completed_tasks_count = 0
    total_tasks = len(tasks)
    
    def task_completion_callback(cloud_name, completion_info):
        """Callback function to handle task completion"""
        nonlocal completed_tasks_count
        task = completion_info['task']
        task_name = task['Name']
        
        # Store completion information
        task_completion_info[task_name] = {
            'cloud_node': cloud_name,
            'transmission_time': completion_info['transmission_time'],
            'queue_time': completion_info['queue_time'],
            'processing_time': completion_info['processing_time'],
            'total_time': completion_info['total_time'],
            'completion_time': completion_info['completion_time']
        }
        
        # Increment completed tasks counter
        completed_tasks_count += 1
        
        # Log completion immediately
        fcfs_cloud_logger.info(f"\nTask Completed: {task_name}")
        fcfs_cloud_logger.info(f"  Completed at: {cloud_name}")
        fcfs_cloud_logger.info(f"  Transmission Time: {completion_info['transmission_time']:.2f}s")
        fcfs_cloud_logger.info(f"  Queue Time: {completion_info['queue_time']:.2f}s")
        fcfs_cloud_logger.info(f"  Processing Time: {completion_info['processing_time']:.2f}s")
        fcfs_cloud_logger.info(f"  Total Time: {completion_info['total_time']:.2f}s")
        fcfs_cloud_logger.info("  " + "-" * 30)
        
        # Remove from queued tasks if it was queued
        if task_name in queued_tasks_info:
            del queued_tasks_info[task_name]
    
    # Register completion callback for all cloud nodes
    for node in cloud_nodes.values():
        node.add_completion_callback(task_completion_callback)
    
    # Process tasks in FCFS order (based on CreationTime)
    sorted_tasks = sorted(tasks, key=lambda x: x['CreationTime'])
    
    for i, task in enumerate(sorted_tasks, 1):
        print(f"\nProcessing Task {i}:")
        print(f"Task Name: {task['Name']}")
        print(f"Creation Time: {task['CreationTime']}")
        print(f"Size: {task['Size']} MI")
        print(f"MIPS: {task['MIPS']}")
        print(f"RAM: {task['RAM']}")
        print(f"BW: {task['BW']}")
        print(f"DataType: {task['DataType']}")
        print(f"DeviceType: {task['DeviceType']}")
        
        # Calculate and display distances to cloud nodes
        distances = calculate_task_distances(task)
        print("\nDistances to Cloud Nodes:")
        for cloud_name, distance in distances.items():
            print(f"{cloud_name}: {distance:.2f} km")
        
        # Sort cloud nodes by distance
        sorted_cloud_nodes = sorted(distances.items(), key=lambda x: x[1])
        
        # Try to assign task to cloud nodes in order of distance
        task_assigned = False
        for cloud_name, distance in sorted_cloud_nodes:
            cloud_node = get_cloud_node(cloud_name)
            if cloud_node:
                # Try to assign the task
                success, processing_time = cloud_node.assign_task(task)
                if success:
                    print(f"\nTask assigned to {cloud_name} ({distance:.2f} km)")
                    print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                    print("Cloud Node Status:")
                    status = cloud_node.get_status()
                    for key, value in status.items():
                        print(f"  {key}: {value}")
                    task_assigned = True
                    break
                else:
                    # Task was queued at this cloud node
                    print(f"\nTask queued at {cloud_name} ({distance:.2f} km)")
                    # Store queued task information
                    queued_tasks_info[task['Name']] = {
                        'cloud_node': cloud_name,
                        'distance': distance,
                        'queue_position': len(cloud_node.task_queue),
                        'task_size': task['Size'],
                        'required_mips': task['MIPS'],
                        'required_ram': task['RAM'],
                        'required_bw': task['BW']
                    }
                    task_assigned = True
                    break
        
        if not task_assigned:
            print(f"\nError: Could not assign or queue task {task['Name']}")
        
        print("-" * 40)
    
    # Wait for all tasks to complete
    while completed_tasks_count < total_tasks:
        time.sleep(0.1)  # Small delay to prevent CPU overuse
    
    # Log queued tasks information
    if queued_tasks_info:
        fcfs_cloud_logger.info("\n=== Queued Tasks Summary ===")
        for task_name, info in queued_tasks_info.items():
            fcfs_cloud_logger.info(f"\nQueued Task: {task_name}")
            fcfs_cloud_logger.info(f"  Queued at: {info['cloud_node']} (distance={info['distance']:.2f} km)")
            fcfs_cloud_logger.info(f"  Queue Position: {info['queue_position']}")
            fcfs_cloud_logger.info(f"  Task Size: {info['task_size']} MI")
            fcfs_cloud_logger.info(f"  Required Resources:")
            fcfs_cloud_logger.info(f"    MIPS: {info['required_mips']}")
            fcfs_cloud_logger.info(f"    RAM: {info['required_ram']}")
            fcfs_cloud_logger.info(f"    Bandwidth: {info['required_bw']}")
    
    # Log overall statistics after all tasks are completed
    if task_completion_info:
        # Sort tasks by completion time for consistent logging
        sorted_completions = sorted(task_completion_info.items(), 
                                  key=lambda x: x[1]['completion_time'])
        
        # Log all task completions in order
        fcfs_cloud_logger.info("\n=== Task Completion Summary ===")
        for task_name, info in sorted_completions:
            fcfs_cloud_logger.info(f"\nTask Completed: {task_name}")
            fcfs_cloud_logger.info(f"  Completed at: {info['cloud_node']}")
            fcfs_cloud_logger.info(f"  Transmission Time: {info['transmission_time']:.2f}s")
            fcfs_cloud_logger.info(f"  Queue Time: {info['queue_time']:.2f}s")
            fcfs_cloud_logger.info(f"  Processing Time: {info['processing_time']:.2f}s")
            fcfs_cloud_logger.info(f"  Total Time: {info['total_time']:.2f}s")
            fcfs_cloud_logger.info("  " + "-" * 30)
        
        # Calculate statistics
        avg_transmission = sum(info['transmission_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_queue = sum(info['queue_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_processing = sum(info['processing_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_total = sum(info['total_time'] for info in task_completion_info.values()) / len(task_completion_info)
        
        fcfs_cloud_logger.info("\n=== Final Overall Statistics ===")
        fcfs_cloud_logger.info(f"Total Tasks Completed: {len(task_completion_info)}")
        fcfs_cloud_logger.info(f"Average Transmission Time: {avg_transmission:.2f}s")
        fcfs_cloud_logger.info(f"Average Queue Time: {avg_queue:.2f}s")
        fcfs_cloud_logger.info(f"Average Processing Time: {avg_processing:.2f}s")
        fcfs_cloud_logger.info(f"Average Total Time: {avg_total:.2f}s")

if __name__ == "__main__":
    # First load the tasks and get the task list
    tasks = read_and_log_tuples()
    
    # Print debug information
    print(f"\nDebug: Number of tasks received: {len(tasks)}")
    fcfs_cloud_logger.info(f"Number of tasks received: {len(tasks)}")
    
    # Process tasks using FCFS Cloud
    if tasks:
        process_fcfs_cloud(tasks)
    else:
        print("Error: No tasks were loaded")
        fcfs_cloud_logger.error("No tasks were loaded.") 