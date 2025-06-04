import json
import time
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG
from utility import calculate_distance
from fog import get_fog_node, get_fog_node_status, get_all_fog_nodes
from logger import setup_logger

# Logger for FCFS Fog events
fcfs_fog_logger = setup_logger('fcfs_fog', 'FCFS_fog.log')

def calculate_task_distances(task):
    """
    Calculate distances from task to all fog nodes
    """
    distances = {}
    task_location = {
        'lat': task['GeoLocation']['latitude'],
        'lon': task['GeoLocation']['longitude']
    }
    
    for fog_node in FOG_NODES_CONFIG:
        fog_location = fog_node['location']
        distance = calculate_distance(task_location, fog_location)
        distances[fog_node['name']] = distance
    
    return distances

def process_fcfs_fog(tasks):
    """
    Process tasks using FCFS algorithm with fog nodes
    """
    if not tasks:
        print("No tasks to process")
        fcfs_fog_logger.warning("No tasks to process.")
        return
        
    print("\n=== FCFS Fog Processing ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    fcfs_fog_logger.info(f"Total Tasks to Process: {len(tasks)}")
    
    # Log fog node creation
    fog_nodes = get_all_fog_nodes()
    for name, node in fog_nodes.items():
        fcfs_fog_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Location={node.location})")
    
    # Dictionary to store task information
    task_completion_info = {}
    queued_tasks_info = {}
    
    def task_completion_callback(fog_name, completion_info):
        """Callback function to handle task completion"""
        task = completion_info['task']
        task_name = task['Name']
        
        # Store completion information
        task_completion_info[task_name] = {
            'fog_node': fog_name,
            'transmission_time': completion_info['transmission_time'],
            'queue_time': completion_info['queue_time'],
            'processing_time': completion_info['processing_time'],
            'total_time': completion_info['total_time'],
            'completion_time': completion_info['completion_time']
        }
        
        # Log completion immediately
        fcfs_fog_logger.info(f"\nTask Completed: {task_name}")
        fcfs_fog_logger.info(f"  Completed at: {fog_name}")
        fcfs_fog_logger.info(f"  Transmission Time: {completion_info['transmission_time']:.2f}s")
        fcfs_fog_logger.info(f"  Queue Time: {completion_info['queue_time']:.2f}s")
        fcfs_fog_logger.info(f"  Processing Time: {completion_info['processing_time']:.2f}s")
        fcfs_fog_logger.info(f"  Total Time: {completion_info['total_time']:.2f}s")
        
        # Remove from queued tasks if it was queued
        if task_name in queued_tasks_info:
            del queued_tasks_info[task_name]
    
    # Register completion callback for all fog nodes
    for node in fog_nodes.values():
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
        
        # Calculate and display distances to fog nodes
        distances = calculate_task_distances(task)
        print("\nDistances to Fog Nodes:")
        for fog_name, distance in distances.items():
            print(f"{fog_name}: {distance:.2f} km")
        
        # Sort fog nodes by distance
        sorted_fog_nodes = sorted(distances.items(), key=lambda x: x[1])
        
        # Try to assign task to fog nodes in order of distance
        task_assigned = False
        for fog_name, distance in sorted_fog_nodes:
            fog_node = get_fog_node(fog_name)
            if fog_node:
                # Try to assign the task
                success, processing_time = fog_node.assign_task(task)
                if success:
                    print(f"\nTask assigned to {fog_name} ({distance:.2f} km)")
                    print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                    print("Fog Node Status:")
                    status = fog_node.get_status()
                    for key, value in status.items():
                        print(f"  {key}: {value}")
                    task_assigned = True
                    break
                else:
                    # Task was queued at this fog node
                    print(f"\nTask queued at {fog_name} ({distance:.2f} km)")
                    # Store queued task information
                    queued_tasks_info[task['Name']] = {
                        'fog_node': fog_name,
                        'distance': distance,
                        'queue_position': len(fog_node.task_queue),
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
        time.sleep(0.01)  # Small delay to allow for status updates
    
    # Wait for all tasks to complete
    while True:
        # Check if all tasks are either completed or queued
        all_tasks_handled = all(task['Name'] in task_completion_info or task['Name'] in queued_tasks_info 
                              for task in sorted_tasks)
        if all_tasks_handled:
            break
        time.sleep(0.1)  # Wait a bit before checking again
    
    # Log queued tasks information
    if queued_tasks_info:
        fcfs_fog_logger.info("\n=== Queued Tasks Summary ===")
        for task_name, info in queued_tasks_info.items():
            fcfs_fog_logger.info(f"\nQueued Task: {task_name}")
            fcfs_fog_logger.info(f"  Queued at: {info['fog_node']} (distance={info['distance']:.2f} km)")
            fcfs_fog_logger.info(f"  Queue Position: {info['queue_position']}")
            fcfs_fog_logger.info(f"  Task Size: {info['task_size']} MI")
            fcfs_fog_logger.info(f"  Required Resources:")
            fcfs_fog_logger.info(f"    MIPS: {info['required_mips']}")
            fcfs_fog_logger.info(f"    RAM: {info['required_ram']}")
            fcfs_fog_logger.info(f"    Bandwidth: {info['required_bw']}")
    
    # Wait for all queued tasks to complete
    while queued_tasks_info:
        time.sleep(0.1)  # Wait a bit before checking again
    
    # Log overall statistics after all tasks are completed
    if task_completion_info:
        avg_transmission = sum(info['transmission_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_queue = sum(info['queue_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_processing = sum(info['processing_time'] for info in task_completion_info.values()) / len(task_completion_info)
        avg_total = sum(info['total_time'] for info in task_completion_info.values()) / len(task_completion_info)
        
        fcfs_fog_logger.info("\n=== Final Overall Statistics ===")
        fcfs_fog_logger.info(f"Total Tasks Completed: {len(task_completion_info)}")
        fcfs_fog_logger.info(f"Average Transmission Time: {avg_transmission:.2f}s")
        fcfs_fog_logger.info(f"Average Queue Time: {avg_queue:.2f}s")
        fcfs_fog_logger.info(f"Average Processing Time: {avg_processing:.2f}s")
        fcfs_fog_logger.info(f"Average Total Time: {avg_total:.2f}s")

if __name__ == "__main__":
    # First load the tasks and get the task list
    tasks = read_and_log_tuples()
    
    # Print debug information
    print(f"\nDebug: Number of tasks received: {len(tasks)}")
    fcfs_fog_logger.info(f"Number of tasks received: {len(tasks)}")
    
    # Process tasks using FCFS Fog
    if tasks:
        process_fcfs_fog(tasks)
    else:
        print("Error: No tasks were loaded")
        fcfs_fog_logger.error("No tasks were loaded.") 