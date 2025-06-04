"""
Utility Functions Module

This module provides utility functions for the fog computing system, including:
- Geographic distance calculations
- Location validation
- Transmission time calculations
- Processing time calculations
- System load and queue delay calculations
"""

import math
from config import EARTH_RADIUS_KM
from collections import deque
import random

def calculate_distance(loc1, loc2):
    """
    Calculate distance between two locations using Haversine formula.
    
    Args:
        loc1 (dict): First location with 'lat' and 'lon' keys
        loc2 (dict): Second location with 'lat' and 'lon' keys
        
    Returns:
        float: Distance in kilometers between the two locations
    """
    if not loc1 or not loc2 or 'lat' not in loc1 or 'lon' not in loc1 or 'lat' not in loc2 or 'lon' not in loc2:
        return float('inf')
        
    lat1 = math.radians(loc1['lat'])
    lon1 = math.radians(loc1['lon'])
    lat2 = math.radians(loc2['lat'])
    lon2 = math.radians(loc2['lon'])
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return EARTH_RADIUS_KM * c

def validate_location(location, default_location=None):
    """
    Validate location data and return default if invalid.
    
    Args:
        location (dict): Location data to validate
        default_location (dict, optional): Default location to use if validation fails
        
    Returns:
        dict: Valid location data with lat/lon coordinates
    """
    if default_location is None:
        default_location = {'lat': 0, 'lon': 0}
        
    if not location or 'lat' not in location or 'lon' not in location:
        return default_location
    return location

def calculate_transmission_time(task_location, fog_location, fog_node, task_size_mi=None, logger=None):
    """
    Calculate transmission time between task and fog node.
    
    Args:
        task_location (dict): Task location data
        fog_location (dict): Fog node location data
        fog_node: Fog node object with bandwidth attribute
        task_size_mi (float): Task size in Million Instructions (MI)
        logger: Optional logger for debug information
        
    Returns:
        float: Transmission time in seconds
    """
    # Validate locations
    task_location = validate_location(task_location)
    fog_location = validate_location(fog_location)
    
    # Calculate distance
    distance = calculate_distance(task_location, fog_location)
    if distance == float('inf'):
        if logger:
            logger.warning("Invalid distance calculated, using default distance")
        distance = 1.0  # Default to 1km
    
    # Calculate base latency
    speed_of_light = 200000  # km/s
    base_latency = (distance / speed_of_light)  # in seconds
    
    # Calculate bandwidth delay based on actual task size
    if task_size_mi is not None:
        # Convert MI to bytes (assuming 1 instruction = 4 bytes)
        task_size_bytes = task_size_mi * 1000000 * 4
        bandwidth = fog_node.bandwidth * 1024 * 1024  # Convert Mbps to bytes per second
        bandwidth_delay = task_size_bytes / bandwidth
    else:
        if logger:
            logger.warning("Task size not provided, using minimum bandwidth delay")
        bandwidth_delay = 0.001  # 1ms minimum delay
    
    # Total transmission time is sum of propagation delay and bandwidth delay
    transmission_time = base_latency + bandwidth_delay
    
    # Add some random variation (±10%) to make it more realistic
    variation = random.uniform(0.9, 1.1)
    transmission_time *= variation
    
    # Ensure minimum transmission time
    transmission_time = max(transmission_time, 0.01)  # Minimum 10ms
    
    # Log debug information if logger is provided
    if logger:
        logger.debug("Transmission calculation details:")
        logger.debug(f"  Task location: {task_location}")
        logger.debug(f"  Fog node location: {fog_location}")
        logger.debug(f"  Distance: {distance:.2f} km")
        logger.debug(f"  Task size: {task_size_mi if task_size_mi is not None else 'unknown'} MI")
        logger.debug(f"  Base latency: {base_latency*1000:.2f} ms")
        logger.debug(f"  Bandwidth delay: {bandwidth_delay*1000:.2f} ms")
        logger.debug(f"  Total transmission time: {transmission_time*1000:.2f} ms")
    
    return transmission_time

def calculate_geographic_latency(distance_km, node):
    """
    Calculate geographic latency based on distance and node's bandwidth.
    
    Args:
        distance_km (float): Distance in kilometers
        node: Node object with bandwidth attribute
        
    Returns:
        float: Latency in milliseconds
    """
    speed_of_light = 200000  # km/s
    base_latency = (distance_km / speed_of_light) * 1000  # Convert to milliseconds
    bandwidth_factor = 10000 / node.bandwidth  # Normalize against 10Gbps
    return base_latency * bandwidth_factor

def calculate_queue_delay(node_metrics, task_queues, node_name):
    """
    Calculate queue delay based on actual queue size and historical processing times.
    
    Args:
        node_metrics (dict): Dictionary containing node metrics including processing times
        task_queues (dict): Dictionary of task queues for each node
        node_name (str): Name of the node to calculate delay for
        
    Returns:
        float: Estimated queue delay in milliseconds
    """
    queue_size = len(task_queues[node_name])
    if not node_metrics[node_name]['processing_times']:
        return 0
        
    recent_times = node_metrics[node_name]['processing_times'][-100:]
    avg_processing_time = sum(recent_times) / len(recent_times)
    return queue_size * avg_processing_time

def calculate_system_load(node, node_metrics, task_queues):
    """
    Calculate system load based on actual metrics.
    
    Args:
        node: Node object with mips attribute and current_tasks list
        node_metrics (dict): Dictionary containing node metrics including total times
        task_queues (dict): Dictionary of task queues for each node
        
    Returns:
        float: System load as a value between 0 and 1
    """
    if len(node.current_tasks) == 0:
        return 0
        
    metrics = node_metrics[node.name]
    if not metrics['total_times']:
        return 0
        
    recent_times = metrics['total_times'][-100:] if len(metrics['total_times']) > 100 else metrics['total_times']
    avg_processing_time = sum(recent_times) / len(recent_times)
    queue_size = len(task_queues[node.name])
    
    node_stats = node.get_stats()
    memory_utilization = node_stats.get('memory_utilization', 0)
    
    cpu_load = (queue_size * avg_processing_time) / (node.mips * 1000)
    return max(cpu_load, memory_utilization)

def calculate_processing_time(task_size_mi, processing_power_mips):
    """
    Calculate processing time (latency) for a task on a fog node.
    
    Args:
        task_size_mi (float): Task size in Million Instructions (MI)
        processing_power_mips (float): Processing power in Million Instructions per Second (MIPS)
        
    Returns:
        float: Processing time in seconds
    """
    if processing_power_mips <= 0:
        return float('inf')
    return task_size_mi / processing_power_mips
