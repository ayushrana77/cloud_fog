"""
FCFSC (First Come First Serve with Cooperative) Task Scheduling with Single-Task Fog Nodes (Fog Only)
This module implements a modified FCFS algorithm for fog only that prioritizes cooperative resource sharing between fog nodes.
It extends the basic FCFS approach with cooperative scheduling and resource optimization mechanisms.
This version uses single-task fog nodes with queueing that only process one task at a time.
"""

import time
from collections import defaultdict
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG
from utility import calculate_distance, calculate_storage_requirements, calculate_system_load
from fog_single_queue import get_fog_node, get_all_fog_nodes
from logger import setup_logger

# Initialize logger for FCFSC with Single-Task Fog Nodes events
fcfsc_logger = setup_logger('fcfsc_single_fog_only', 'fcfsc_single_fog_only.log', sub_directory='algorithms')

def calculate_task_distances(task):
    """Calculate distances from task to all fog nodes."""
    distances = {}
    task_location = {
        'lat': task['GeoLocation']['latitude'],
        'lon': task['GeoLocation']['longitude']
    }
    for node in FOG_NODES_CONFIG:
        node_location = node['location']
        distance = calculate_distance(task_location, node_location)
        distances[node['name']] = distance
    return distances

def process_fcfs(tasks):
    """Process tasks using FCFSC with fog nodes only and single-task queueing."""
    if not tasks:
        print("No tasks to process")
        fcfsc_logger.warning("No tasks to process.")
        return
    print("\n=== FCFS Cooperative Processing (Fog Only, Single-Task, With Queue) ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    fcfsc_logger.info(f"Total Tasks to Process: {len(tasks)}")
      # Initialize and log fog nodes
    fog_nodes = get_all_fog_nodes()
    for name, node in fog_nodes.items():
        fcfsc_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Storage={node.storage}GB)")    
    
    # Initialize tracking variables
    task_completion_info = {}
    completed_tasks_count = 0
    total_tasks = len(tasks)
    task_queue_times = {}
      # Initialize node metrics for system load calculation
    node_metrics = defaultdict(lambda: {'processing_times': [], 'total_times': []})
    task_queues = defaultdict(list)
    
    def task_completion_callback(node_name, completion_info):
        nonlocal completed_tasks_count, node_metrics, task_queues
        task = completion_info['task']
        task_name = task['Name']
        # Use the queue time from the fog node implementation
        queue_time = completion_info.get('queue_time', 0)
        # Clean up our tracking dictionary if needed
        if task_name in task_queue_times:
            del task_queue_times[task_name]
        
        # Update node metrics for system load calculation
        processing_time = completion_info.get('processing_time', 0)
        total_time = completion_info.get('total_time', 0)
        node_metrics[node_name]['processing_times'].append(processing_time)
        node_metrics[node_name]['total_times'].append(total_time)
        
        # Remove task from queue if it was there
        if task_name in task_queues[node_name]:
            task_queues[node_name].remove(task_name)
        
        power_info = completion_info.get('power_consumption', {})
        task_completion_info[task_name] = {
            'node': node_name,
            'transmission_time': completion_info.get('transmission_time', 0),
            'queue_time': queue_time,
            'processing_time': processing_time,
            'total_time': total_time,  # Queue time already included
            'completion_time': completion_info.get('completion_time', 0),
            'storage_used': task.get('Storage', 0),
            'power_consumption': power_info,
            'task': task
        }
        completed_tasks_count += 1
        fcfsc_logger.info(f"\nTask Completed: {task_name}")
        fcfsc_logger.info(f"  Completed at: {node_name}")
        fcfsc_logger.info(f"  Transmission Time: {completion_info.get('transmission_time', 0):.2f}s")
        fcfsc_logger.info(f"  Queue Time: {queue_time:.2f}s")
        fcfsc_logger.info(f"  Processing Time: {processing_time:.2f}s")
        fcfsc_logger.info(f"  Storage Used: {task.get('Storage', 0):.2f}GB")
        fcfsc_logger.info(f"  Total Time: {total_time:.2f}s")
        if power_info:
            fcfsc_logger.info(f"  Power Consumption: {power_info.get('total_energy_wh', 0):.6f} Wh")
            fcfsc_logger.info(f"  Average Power: {power_info.get('avg_power_watts', 0):.2f} W")
        fcfsc_logger.info("  " + "-" * 30)
    
    # Register completion callback for all nodes
    for node in fog_nodes.values():
        node.add_completion_callback(task_completion_callback)
    
    # Sort tasks by creation time (FCFS order)
    sorted_tasks = sorted(tasks, key=lambda x: x['CreationTime'])
    
    # Process each task in order
    for i, task in enumerate(sorted_tasks, 1):
        task['Storage'] = calculate_storage_requirements(task['Size'])
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
        sorted_fog_nodes = sorted(fog_distances.items(), key=lambda x: x[1])
        
        # Log distance calculations
        fcfsc_logger.info(f"\nTask {task['Name']} - Distance Calculations:")
        fcfsc_logger.info("Fog Node Distances:")
        for fog_name, distance in sorted_fog_nodes:
            fcfsc_logger.info(f"  {fog_name}: {distance:.2f} km")        # Cooperative resource sharing - try first two closest fog nodes, then queue if both are busy
        task_assigned = False
        
        # If we have at least one fog node
        if sorted_fog_nodes:
            # Try first closest node
            first_node_name, first_distance = sorted_fog_nodes[0]
            first_node = get_fog_node(first_node_name)
            
            if first_node and not first_node.is_busy():                # First node is available, assign task to it
                # Add task to track for system load calculation
                task_queues[first_node_name].append(task['Name'])
                
                # Use the advanced system load calculation
                system_load = calculate_system_load(first_node, node_metrics, task_queues)
                current_load = system_load * 100  # Convert from 0-1 scale to percentage
                
                # Also calculate traditional resource usage for logging
                mips_usage = (task['MIPS'] / first_node.mips) * 100 if first_node.mips > 0 else 0
                mem_usage = (task['RAM'] / first_node.memory) * 100 if first_node.memory > 0 else 0
                bw_usage = (task['BW'] / first_node.bandwidth) * 100 if first_node.bandwidth > 0 else 0
                storage_usage = (task['Storage'] / first_node.storage) * 100 if first_node.storage > 0 else 0
                
                fcfsc_logger.info(f"Assigning task {task['Name']} to closest fog node {first_node_name} ({first_distance:.2f} km)")
                fcfsc_logger.info(f"Resources allocated: MIPS={task['MIPS']}/{first_node.mips}, Memory={task['RAM']}/{first_node.memory}, Bandwidth={task['BW']}/{first_node.bandwidth}, Storage={task['Storage']}/{first_node.storage}")
                fcfsc_logger.info(f"Resource usage: MIPS={mips_usage:.2f}%, Memory={mem_usage:.2f}%, Bandwidth={bw_usage:.2f}%, Storage={storage_usage:.2f}%")
                fcfsc_logger.info(f"Current system load: {current_load:.2f}%")
                
                success, processing_time = first_node.assign_task(task)
                print(f"\nTask assigned to closest fog node {first_node_name} ({first_distance:.2f} km)")
                print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                print(f"Current load: {current_load:.2f}%")
                print("Fog Node Status:")
                status = first_node.get_status()
                for key, value in status.items():
                    print(f"  {key}: {value}")
                task_assigned = True
            
            # If we have at least two fog nodes and first node is busy
            elif len(sorted_fog_nodes) > 1 and first_node and first_node.is_busy():
                # Try second closest node
                second_node_name, second_distance = sorted_fog_nodes[1]
                second_node = get_fog_node(second_node_name)
                
                if second_node and not second_node.is_busy():                    # Second node is available, assign task to it
                    # Add task to track for system load calculation
                    task_queues[second_node_name].append(task['Name'])
                    
                    # Use the advanced system load calculation
                    system_load = calculate_system_load(second_node, node_metrics, task_queues)
                    current_load = system_load * 100  # Convert from 0-1 scale to percentage
                    
                    # Also calculate traditional resource usage for logging
                    mips_usage = (task['MIPS'] / second_node.mips) * 100 if second_node.mips > 0 else 0
                    mem_usage = (task['RAM'] / second_node.memory) * 100 if second_node.memory > 0 else 0
                    bw_usage = (task['BW'] / second_node.bandwidth) * 100 if second_node.bandwidth > 0 else 0
                    storage_usage = (task['Storage'] / second_node.storage) * 100 if second_node.storage > 0 else 0
                    
                    fcfsc_logger.info(f"First node {first_node_name} is busy. Assigning task {task['Name']} to second closest fog node {second_node_name} ({second_distance:.2f} km)")
                    fcfsc_logger.info(f"Resources allocated: MIPS={task['MIPS']}/{second_node.mips}, Memory={task['RAM']}/{second_node.memory}, Bandwidth={task['BW']}/{second_node.bandwidth}, Storage={task['Storage']}/{second_node.storage}")
                    fcfsc_logger.info(f"Resource usage: MIPS={mips_usage:.2f}%, Memory={mem_usage:.2f}%, Bandwidth={bw_usage:.2f}%, Storage={storage_usage:.2f}%")
                    fcfsc_logger.info(f"Current system load: {current_load:.2f}%")
                    
                    success, processing_time = second_node.assign_task(task)
                    print(f"\nFirst node {first_node_name} is busy. Task assigned to second closest fog node {second_node_name} ({second_distance:.2f} km)")
                    print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                    print(f"Current load: {current_load:.2f}%")                    
                    print("Fog Node Status:")
                    status = second_node.get_status()
                    for key, value in status.items():
                        print(f"  {key}: {value}")
                    task_assigned = True
                elif second_node:  # Both nodes are busy, queue at the second closest
                    # Add task name to task_queues for load calculation
                    task_queues[second_node_name].append(task['Name'])
                    queue_size = len(task_queues[second_node_name])
                    
                    # Calculate system load with the new task in queue
                    system_load = calculate_system_load(second_node, node_metrics, task_queues)
                    current_load = max(system_load * 100, 95)  # When busy, ensure load is at least 95%
                    
                    fcfsc_logger.info(f"Both closest fog nodes are busy. Queueing task {task['Name']} at second closest node {second_node_name}.")
                    fcfsc_logger.info(f"Current queue size on {second_node_name}: {queue_size}")
                    fcfsc_logger.info(f"Current system load: {current_load:.2f}% (Node busy)")
                    
                    second_node.queue_task(task)
                    task_queue_times[task['Name']] = time.time()
                    print(f"\nBoth closest fog nodes are busy. Task {task['Name']} queued at second closest node {second_node_name} at position {len(second_node.task_queue)}.")
                    task_assigned = True
        
        if not task_assigned:
            fcfsc_logger.error(f"Failed to assign or queue task {task['Name']}")
            print(f"\nError: Could not assign or queue task {task['Name']}")
        
        print("-" * 40)
    
    # Wait for all tasks to complete
    while completed_tasks_count < total_tasks:
        time.sleep(0.1)
    
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
        fcfsc_logger.info("\n=== Overall Statistics ===")
        fcfsc_logger.info(f"Total Tasks: {len(task_completion_info)}")
        fcfsc_logger.info(f"  Fog Tasks: {len(task_completion_info)}")  # All tasks are fog tasks
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
        fcfsc_logger.info("\nPower Consumption:")
        fcfsc_logger.info(f"  Total Energy Consumed: {total_energy_wh:.6f} Wh")
        fcfsc_logger.info(f"  Total Power Used: {total_power_watts:.2f} W")
        fcfsc_logger.info(f"  Average Energy per Task: {avg_energy:.6f} Wh")
        fcfsc_logger.info(f"  Average Power per Task: {avg_power:.2f} W")
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
        
        # Calculate Fog Statistics (in this case, all tasks are fog tasks)
        fog_transmission = total_transmission
        fog_processing = total_processing
        fog_queue = total_queue
        fog_total = total_time
        fog_storage = total_storage
        fog_workload = total_workload
        fog_size = total_size
        fog_bandwidth = total_bandwidth
        fog_energy = total_energy_wh
        fog_power = total_power_watts
        
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
        
        avg_fog_transmission = fog_transmission / len(task_completion_info) if task_completion_info else 0
        avg_fog_processing = fog_processing / len(task_completion_info) if task_completion_info else 0
        avg_fog_queue = fog_queue / len(task_completion_info) if task_completion_info else 0
        avg_fog_total = fog_total / len(task_completion_info) if task_completion_info else 0
        avg_fog_storage = fog_storage / len(task_completion_info) if task_completion_info else 0
        avg_fog_workload = fog_workload / len(task_completion_info) if task_completion_info else 0
        avg_fog_size = fog_size / len(task_completion_info) if task_completion_info else 0
        avg_fog_bandwidth = fog_bandwidth / len(task_completion_info) if task_completion_info else 0
        avg_fog_energy = fog_energy / len(task_completion_info) if task_completion_info else 0
        avg_fog_power = fog_power / len(task_completion_info) if task_completion_info else 0
        
        # Log Fog Statistics
        fcfsc_logger.info("\n=== Overall Fog Statistics ===")
        fcfsc_logger.info(f"Total Fog Tasks: {len(task_completion_info)}")
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
        fcfsc_logger.info("\nPower Consumption:")
        fcfsc_logger.info(f"  Total Fog Energy Consumed: {fog_energy:.6f} Wh")
        fcfsc_logger.info(f"  Total Fog Power Used: {fog_power:.2f} W")
        fcfsc_logger.info(f"  Average Energy per Fog Task: {avg_fog_energy:.6f} Wh")
        fcfsc_logger.info(f"  Average Power per Fog Task: {avg_fog_power:.2f} W")
        fcfsc_logger.info("\nPer Node Statistics:")
        for node_name in fog_node_workloads:
            fcfsc_logger.info(f"\n  {node_name}:")
            fcfsc_logger.info(f"    Workload: {fog_node_workloads[node_name]:.2f} MIPS-seconds")
            fcfsc_logger.info(f"    Storage Used: {fog_node_storage[node_name]:.2f} GB")
            fcfsc_logger.info(f"    Data Size: {fog_node_size[node_name]:.2f} MI")
            fcfsc_logger.info(f"    Bandwidth Used: {fog_node_bandwidth[node_name]:.2f} Mbps")
            fcfsc_logger.info(f"    Energy Consumed: {fog_node_energy[node_name]:.6f} Wh")
            fcfsc_logger.info(f"    Power Used: {fog_node_power[node_name]:.2f} W")
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
        
if __name__ == "__main__":
    tasks = read_and_log_tuples()
    print(f"Loaded {len(tasks)} tasks")
    if tasks:
        process_fcfs(tasks)
    else:
        print("Error: No tasks were loaded")
        fcfsc_logger.error("No tasks were loaded.")
