"""
FCFS (First Come First Serve, Fog Only, Single-Task) Task Scheduling with Queueing
Each fog node can process only one task at a time. If busy, tasks are queued.
"""

import time
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG
from utility import calculate_distance, calculate_storage_requirements
from fog_single_queue import get_fog_node, get_all_fog_nodes
from logger import setup_logger

# Logger for FCFS events
fcfs_logger = setup_logger('fcfs', 'fcfs.log', sub_directory='algorithms')

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
    """Process tasks using FCFS with fog nodes only and single-task queueing."""
    if not tasks:
        print("No tasks to process")
        fcfs_logger.warning("No tasks to process.")
        return
    print("\n=== FCFS Processing (Fog Only, Single-Task, With Queue) ===")
    print(f"Total Tasks to Process: {len(tasks)}")
    fcfs_logger.info(f"Total Tasks to Process: {len(tasks)}")
    fog_nodes = get_all_fog_nodes()
    for name, node in fog_nodes.items():
        fcfs_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Storage={node.storage}GB)")    
        task_completion_info = {}
    completed_tasks_count = 0
    total_tasks = len(tasks)
    task_queue_times = {}
    
    def task_completion_callback(node_name, completion_info):
        nonlocal completed_tasks_count
        task = completion_info['task']
        task_name = task['Name']
        # Use the queue time from the fog node implementation
        queue_time = completion_info.get('queue_time', 0)
        # Clean up our tracking dictionary if needed
        if task_name in task_queue_times:
            del task_queue_times[task_name]
        power_info = completion_info.get('power_consumption', {})
        task_completion_info[task_name] = {
            'node': node_name,
            'transmission_time': completion_info.get('transmission_time', 0),            'queue_time': queue_time,
            'processing_time': completion_info.get('processing_time', 0),
            'total_time': completion_info.get('total_time', 0),  # Queue time already included
            'completion_time': completion_info.get('completion_time', 0),
            'storage_used': task.get('Storage', 0),
            'power_consumption': power_info,
            'task': task
        }
        completed_tasks_count += 1
        fcfs_logger.info(f"\nTask Completed: {task_name}")
        fcfs_logger.info(f"  Completed at: {node_name}")
        fcfs_logger.info(f"  Transmission Time: {completion_info.get('transmission_time', 0):.2f}s")
        fcfs_logger.info(f"  Queue Time: {queue_time:.2f}s")
        fcfs_logger.info(f"  Processing Time: {completion_info.get('processing_time', 0):.2f}s")
        fcfs_logger.info(f"  Storage Used: {task.get('Storage', 0):.2f}GB")
        fcfs_logger.info(f"  Total Time: {task_completion_info[task_name]['total_time']:.2f}s")
        if power_info:
            fcfs_logger.info(f"  Power Consumption: {power_info.get('total_energy_wh', 0):.6f} Wh")
            fcfs_logger.info(f"  Average Power: {power_info.get('avg_power_watts', 0):.2f} W")
        fcfs_logger.info("  " + "-" * 30)
    for node in fog_nodes.values():
        node.add_completion_callback(task_completion_callback)
    sorted_tasks = sorted(tasks, key=lambda x: x['CreationTime'])
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
        fog_distances = calculate_task_distances(task)
        sorted_fog_nodes = sorted(fog_distances.items(), key=lambda x: x[1])
        fcfs_logger.info(f"\nTask {task['Name']} - Distance Calculations:")
        fcfs_logger.info("Fog Node Distances:")
        for fog_name, distance in sorted_fog_nodes:
            fcfs_logger.info(f"  {fog_name}: {distance:.2f} km")
        task_assigned = False
        for fog_name, distance in sorted_fog_nodes:
            fog_node = get_fog_node(fog_name)
            if fog_node:
                if fog_node.is_busy():
                    fcfs_logger.info(f"Fog node {fog_name} is busy. Queueing task {task['Name']}.")
                    fog_node.queue_task(task)
                    task_queue_times[task['Name']] = time.time()
                    print(f"\nFog node {fog_name} is busy. Task {task['Name']} queued at position {len(fog_node.task_queue)}.")
                else:
                    fcfs_logger.info(f"Assigning task {task['Name']} to fog node {fog_name} ({distance:.2f} km)")
                    success, processing_time = fog_node.assign_task(task)
                    print(f"\nTask assigned to fog node {fog_name} ({distance:.2f} km)")
                    print(f"Estimated Processing Time: {processing_time:.2f} seconds")
                    print("Fog Node Status:")
                    status = fog_node.get_status()
                    for key, value in status.items():
                        print(f"  {key}: {value}")
                task_assigned = True
                break
        if not task_assigned:
            fcfs_logger.error(f"Failed to assign or queue task {task['Name']}")
            print(f"\nError: Could not assign or queue task {task['Name']}")
        print("-" * 40)
    while completed_tasks_count < total_tasks:
        time.sleep(0.1)
    # (Logging/statistics code remains unchanged) 

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
        fcfs_logger.info("\n=== Overall Statistics ===")
        fcfs_logger.info(f"Total Tasks: {len(task_completion_info)}")
        fcfs_logger.info(f"  Fog Tasks: {len(task_completion_info)}")  # All tasks are fog tasks
        fcfs_logger.info("\nWorkload:")
        fcfs_logger.info(f"  Total System Workload: {total_workload:.2f} MIPS-seconds")
        fcfs_logger.info(f"  Average Workload per Task: {avg_workload:.2f} MIPS-seconds")
        fcfs_logger.info("\nResource Usage:")
        fcfs_logger.info(f"  Total Storage Used: {total_storage:.2f} GB")
        fcfs_logger.info(f"  Total Data Size: {total_size:.2f} MI")
        fcfs_logger.info(f"  Total Bandwidth Used: {total_bandwidth:.2f} Mbps")
        fcfs_logger.info(f"  Average Storage per Task: {avg_storage:.2f} GB")
        fcfs_logger.info(f"  Average Size per Task: {avg_size:.2f} MI")
        fcfs_logger.info(f"  Average Bandwidth per Task: {avg_bandwidth:.2f} Mbps")
        fcfs_logger.info("\nPower Consumption:")
        fcfs_logger.info(f"  Total Energy Consumed: {total_energy_wh:.6f} Wh")
        fcfs_logger.info(f"  Total Power Used: {total_power_watts:.2f} W")
        fcfs_logger.info(f"  Average Energy per Task: {avg_energy:.6f} Wh")
        fcfs_logger.info(f"  Average Power per Task: {avg_power:.2f} W")
        fcfs_logger.info("\nTransmission Time:")
        fcfs_logger.info(f"  Total: {total_transmission*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_transmission*1000:.6f}ms")
        fcfs_logger.info("\nProcessing Time:")
        fcfs_logger.info(f"  Total: {total_processing*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_processing*1000:.6f}ms")
        fcfs_logger.info("\nQueue Time:")
        fcfs_logger.info(f"  Total: {total_queue*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_queue*1000:.6f}ms")
        fcfs_logger.info("\nTotal Time:")
        fcfs_logger.info(f"  Total: {total_time*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_total*1000:.6f}ms")
        
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
        fcfs_logger.info("\n=== Overall Fog Statistics ===")
        fcfs_logger.info(f"Total Fog Tasks: {len(task_completion_info)}")
        fcfs_logger.info("\nWorkload:")
        fcfs_logger.info(f"  Total Fog Workload: {fog_workload:.2f} MIPS-seconds")
        fcfs_logger.info(f"  Average Workload per Fog Task: {avg_fog_workload:.2f} MIPS-seconds")
        fcfs_logger.info("\nResource Usage:")
        fcfs_logger.info(f"  Total Fog Storage: {fog_storage:.2f} GB")
        fcfs_logger.info(f"  Total Fog Data Size: {fog_size:.2f} MI")
        fcfs_logger.info(f"  Total Fog Bandwidth: {fog_bandwidth:.2f} Mbps")
        fcfs_logger.info(f"  Average Storage per Fog Task: {avg_fog_storage:.2f} GB")
        fcfs_logger.info(f"  Average Size per Fog Task: {avg_fog_size:.2f} MI")
        fcfs_logger.info(f"  Average Bandwidth per Fog Task: {avg_fog_bandwidth:.2f} Mbps")
        fcfs_logger.info("\nPower Consumption:")
        fcfs_logger.info(f"  Total Fog Energy Consumed: {fog_energy:.6f} Wh")
        fcfs_logger.info(f"  Total Fog Power Used: {fog_power:.2f} W")
        fcfs_logger.info(f"  Average Energy per Fog Task: {avg_fog_energy:.6f} Wh")
        fcfs_logger.info(f"  Average Power per Fog Task: {avg_fog_power:.2f} W")
        fcfs_logger.info("\nPer Node Statistics:")
        for node_name in fog_node_workloads:
            fcfs_logger.info(f"\n  {node_name}:")
            fcfs_logger.info(f"    Workload: {fog_node_workloads[node_name]:.2f} MIPS-seconds")
            fcfs_logger.info(f"    Storage Used: {fog_node_storage[node_name]:.2f} GB")
            fcfs_logger.info(f"    Data Size: {fog_node_size[node_name]:.2f} MI")
            fcfs_logger.info(f"    Bandwidth Used: {fog_node_bandwidth[node_name]:.2f} Mbps")
            fcfs_logger.info(f"    Energy Consumed: {fog_node_energy[node_name]:.6f} Wh")
            fcfs_logger.info(f"    Power Used: {fog_node_power[node_name]:.2f} W")
        fcfs_logger.info("\nTransmission Time:")
        fcfs_logger.info(f"  Total: {fog_transmission*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_fog_transmission*1000:.6f}ms")
        fcfs_logger.info("\nProcessing Time:")
        fcfs_logger.info(f"  Total: {fog_processing*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_fog_processing*1000:.6f}ms")
        fcfs_logger.info("\nQueue Time:")
        fcfs_logger.info(f"  Total: {fog_queue*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_fog_queue*1000:.6f}ms")
        fcfs_logger.info("\nTotal Time:")
        fcfs_logger.info(f"  Total: {fog_total*1000:.6f}ms")
        fcfs_logger.info(f"  Average: {avg_fog_total*1000:.6f}ms")
        
if __name__ == "__main__":
    tasks = read_and_log_tuples()
    print(f"Loaded {len(tasks)} tasks")
    if tasks:
        process_fcfs(tasks)
    else:
        print("Error: No tasks were loaded")
        fcfs_logger.error("No tasks were loaded.")