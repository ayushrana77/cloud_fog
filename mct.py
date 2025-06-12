"""
MCT (Minimum Completion Time) Task Scheduling Implementation
This module implements a task scheduling algorithm that minimizes completion time
by considering both fog and cloud resources with optimal resource allocation.
"""

import json
import time
from task_load import read_and_log_tuples
from config import FOG_NODES_CONFIG, CLOUD_SERVICES_CONFIG
from utility import calculate_distance
from fog import get_fog_node, get_fog_node_status, get_all_fog_nodes
from cloud import get_cloud_node, get_cloud_node_status, get_all_cloud_nodes
from logger import setup_logger

# Initialize logger for MCT events
mct_logger = setup_logger('mct', 'MCT.log')

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
    
    # Calculate transmission time (based on distance and bandwidth)
    transmission_time = (distance * task['Size']) / (fog_node.bandwidth * 1000)  # Convert to seconds
    
    # Calculate processing time
    processing_time = task['Size'] / fog_node.mips
    
    # Calculate resource availability scores for each resource type (0 to 1, where 1 is fully available)
    mips_availability = status.get('available_mips', fog_node.mips) / fog_node.mips
    memory_availability = status.get('available_memory', fog_node.memory) / fog_node.memory
    bandwidth_availability = status.get('available_bandwidth', fog_node.bandwidth) / fog_node.bandwidth
    
    # Calculate overall resource availability score (0 to 1, where 1 is fully available)
    resource_score = (mips_availability + memory_availability + bandwidth_availability) / 3
    
    # Calculate weighted components (inverted for transmission and processing - higher is better)
    transmission_component = 0.4 * (1 / (1 + transmission_time))  # Inverted transmission time
    processing_component = 0.3 * (1 / (1 + processing_time))      # Inverted processing time
    resource_component = 0.3 * resource_score                     # Resource availability (already 0-1)
    
    # Final score calculation (higher is better)
    score = transmission_component + processing_component + resource_component
    
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
        }
    }
    
    # Create score breakdown
    score_breakdown = {
        'transmission': {
            'value': transmission_time,
            'weighted': transmission_component
        },
        'processing': {
            'value': processing_time,
            'weighted': processing_component
        },
        'resource_availability': {
            'value': resource_score,
            'weighted': resource_component
        }
    }
    
    return score, resource_info, score_breakdown

def calculate_cloud_completion_time(task, cloud_node, distance):
    """
    Calculate completion time for a cloud node using MCT approach
    
    Args:
        task (dict): Task information
        cloud_node: Cloud node object
        distance (float): Distance to the cloud node
        
    Returns:
        float: Completion time
    """
    # Get node status
    status = cloud_node.get_status()
    
    # Calculate transmission time
    transmission_time = (distance * task['Size']) / (cloud_node.bandwidth * 1000)
    
    # Calculate execution time
    execution_time = task['Size'] / cloud_node.mips
    
    # Calculate ready time (time until node becomes available)
    ready_time = status.get('current_task_time', 0) if status.get('current_task_time', 0) > 0 else 0
    
    # Calculate queue time
    queue_time = len(cloud_node.task_queue) * (execution_time / (1 - (status.get('mips_used', 0) / cloud_node.mips)))
    
    # Total completion time
    completion_time = transmission_time + execution_time + ready_time + queue_time
    
    return completion_time

def display_all_fog_scores():
    """
    Display scores and resource information for all fog nodes
    """
    print("\n=== Comprehensive Fog Node Analysis ===")
    print("=" * 100)
    
    # Get all fog nodes
    fog_nodes = [node for node in get_all_fog_nodes().values() if isinstance(node, FogNode)]
    
    # Calculate scores for each node
    node_scores = []
    for fog_node in fog_nodes:
        # Use a sample task for comparison
        sample_task = {
            'Size': 1000,  # 1000 MI
            'Memory': 512,  # 512 MB
            'Bandwidth': 10  # 10 Mbps
        }
        
        # Calculate distance (using a default distance for comparison)
        distance = 5.0  # 5 km default distance
        
        # Calculate score
        score, resource_info, score_breakdown = calculate_fog_node_score(
            sample_task, fog_node, distance
        )
        
        node_scores.append({
            'name': fog_node.name,
            'score': score,
            'resource_info': resource_info,
            'score_breakdown': score_breakdown
        })
    
    # Sort nodes by score (higher is better)
    node_scores.sort(key=lambda x: x['score'], reverse=True)
    
    # Display header
    print(f"{'Node Name':<15} {'Score':<10}")
    print("-" * 100)
    
    # Display each node's information
    for node in node_scores:
        print(f"{node['name']:<15} {node['score']:.2f}")
    
    print("\nDetailed Breakdown for Each Node:")
    print("=" * 100)
    
    for node in node_scores:
        print(f"\n{node['name']}:")
        print("-" * 50)
        print("Score Components:")
        print(f"  Transmission Time: {node['score_breakdown']['transmission']['value']:.2f}s (weighted: {node['score_breakdown']['transmission']['weighted']:.2f})")
        print(f"  Processing Time: {node['score_breakdown']['processing']['value']:.2f}s (weighted: {node['score_breakdown']['processing']['weighted']:.2f})")
        print(f"  Resource Availability: {node['score_breakdown']['resource_availability']['value']:.2f} (weighted: {node['score_breakdown']['resource_availability']['weighted']:.2f})")
        
        print("\nResource Details:")
        print(f"  MIPS: {node['resource_info']['mips']['available']:.2f}/{node['resource_info']['mips']['total']:.2f} ({node['resource_info']['mips']['availability']*100:.1f}%)")
        print(f"  Memory: {node['resource_info']['memory']['available']:.2f}/{node['resource_info']['memory']['total']:.2f} ({node['resource_info']['memory']['availability']*100:.1f}%)")
        print(f"  Bandwidth: {node['resource_info']['bandwidth']['available']:.2f}/{node['resource_info']['bandwidth']['total']:.2f} ({node['resource_info']['bandwidth']['availability']*100:.1f}%)")
    
    print("\nNote: Higher scores indicate better node performance")
    print("=" * 100)

def process_mct(tasks):
    """
    Process tasks using Minimum Completion Time (MCT) algorithm
    
    This function implements the MCT scheduling logic:
    1. For each task, calculate scores for all fog nodes
    2. Select the fog node with maximum score
    3. If fog node can't handle the task, use cloud with MCT
    4. Track and log all performance metrics
    
    Args:
        tasks (list): List of tasks to be processed
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
        mct_logger.info(f"Fog node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Location={node.location})")
    
    # Initialize and log cloud nodes
    cloud_nodes = get_all_cloud_nodes()
    for name, node in cloud_nodes.items():
        mct_logger.info(f"Cloud node created: {name} (MIPS={node.mips}, RAM={node.memory}, BW={node.bandwidth}, Location={node.location})")
    
    # Initialize tracking variables
    task_completion_info = {}
    queued_tasks_info = {}
    completed_tasks_count = 0
    total_tasks = len(tasks)
    
    def task_completion_callback(node_name, completion_info):
        """Callback function to handle task completion events"""
        nonlocal completed_tasks_count
        task = completion_info['task']
        task_name = task['Name']
        
        # Store completion information
        task_completion_info[task_name] = {
            'node': node_name,
            'transmission_time': completion_info['transmission_time'],
            'processing_time': completion_info['processing_time'],
            'queue_time': completion_info['queue_time'],
            'total_time': completion_info['total_time']
        }
        
        # Update completion counter and logging
        completed_tasks_count += 1
        mct_logger.info(f"\nTask Completed: {task_name}")
        mct_logger.info(f"  Completed at: {node_name}")
        mct_logger.info(f"  Transmission Time: {completion_info['transmission_time']:.2f}s")
        mct_logger.info(f"  Processing Time: {completion_info['processing_time']:.2f}s")
        mct_logger.info(f"  Queue Time: {completion_info['queue_time']:.2f}s")
        mct_logger.info(f"  Total Time: {completion_info['total_time']:.2f}s")
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
        
        # Calculate distances to all nodes
        fog_distances = calculate_task_distances(task, is_cloud=False)
        cloud_distances = calculate_task_distances(task, is_cloud=True)
        
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
                print(f"    Transmission Time: {score_breakdown['transmission']['value']:.2f}s (weighted: {score_breakdown['transmission']['weighted']:.2f})")
                print(f"    Processing Time: {score_breakdown['processing']['value']:.2f}s (weighted: {score_breakdown['processing']['weighted']:.2f})")
                print(f"    Resource Availability: {score_breakdown['resource_availability']['value']:.2f} (weighted: {score_breakdown['resource_availability']['weighted']:.2f})")
                
                print("\n  Current Resource Availability:")
                print(f"    MIPS: {resource_info['mips']['available']:.2f}/{resource_info['mips']['total']:.2f} ({resource_info['mips']['availability']*100:.1f}%)")
                print(f"    Memory: {resource_info['memory']['available']:.2f}/{resource_info['memory']['total']:.2f} ({resource_info['memory']['availability']*100:.1f}%)")
                print(f"    Bandwidth: {resource_info['bandwidth']['available']:.2f}/{resource_info['bandwidth']['total']:.2f} ({resource_info['bandwidth']['availability']*100:.1f}%)")
                print("-" * 80)
        
        # Find best fog node (highest score)
        best_fog = None
        if fog_scores:
            best_fog_name = max(fog_scores.items(), key=lambda x: x[1]['score'])[0]
            best_fog = fog_scores[best_fog_name]
        
        # Try to assign to best fog node first
        task_assigned = False
        if best_fog:
            fog_node = best_fog['node']
            mct_logger.info(f"\nAttempting to assign task {task['Name']} to fog node {fog_node.name}")
            
            success, processing_time = fog_node.assign_task(task)
            if success:
                mct_logger.info(f"Task successfully assigned to fog node {fog_node.name}")
                print(f"\nTask assigned to fog node {fog_node.name}")
                task_assigned = True
            else:
                mct_logger.info(f"Fog node {fog_node.name} cannot handle task - attempting cloud")
                print(f"\nFog node {fog_node.name} cannot handle task - attempting cloud")
        
        # If fog assignment failed, try cloud nodes
        if not task_assigned:
            cloud_completion_times = {}
            for cloud_name, distance in cloud_distances.items():
                cloud_node = get_cloud_node(cloud_name)
                if cloud_node:
                    completion_time = calculate_cloud_completion_time(task, cloud_node, distance)
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
        # Calculate and log performance metrics
        total_transmission = sum(info['transmission_time'] for info in task_completion_info.values())
        total_processing = sum(info['processing_time'] for info in task_completion_info.values())
        total_queue = sum(info['queue_time'] for info in task_completion_info.values())
        total_time = sum(info['total_time'] for info in task_completion_info.values())
        
        avg_transmission = total_transmission / len(task_completion_info)
        avg_processing = total_processing / len(task_completion_info)
        avg_queue = total_queue / len(task_completion_info)
        avg_total = total_time / len(task_completion_info)
        
        # Calculate statistics by node type
        cloud_tasks = [info for info in task_completion_info.values() if info['node'] in cloud_nodes]
        fog_tasks = [info for info in task_completion_info.values() if info['node'] in fog_nodes]
        
        # Log final statistics
        mct_logger.info("\n=== Final Overall Statistics ===")
        mct_logger.info(f"Total Tasks Completed: {len(task_completion_info)}")
        mct_logger.info(f"  Cloud Tasks: {len(cloud_tasks)}")
        mct_logger.info(f"  Fog Tasks: {len(fog_tasks)}")
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
