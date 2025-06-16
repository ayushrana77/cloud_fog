"""
Utility Functions Module

This module provides utility functions for the fog computing system, including:
- Geographic distance calculations
- Location validation
- Transmission time calculations
- Processing time calculations
- Storage calculations
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
        
    if not location:
        return default_location
        
    # Handle both lat/lon and latitude/longitude keys
    if 'lat' in location and 'lon' in location:
        return location
    elif 'latitude' in location and 'longitude' in location:
        return {
            'lat': location['latitude'],
            'lon': location['longitude']
        }
    return default_location

def calculate_transmission_time(task_location, fog_location, fog_node, task_size_mi=None, task_mips=None, logger=None):
    """
    Calculate transmission time between task and fog node using:
    1. Transmission Time (Tₜₓ) = (Task MIPS * Distance) / (Bandwidth * 1000)
    2. Propagation Delay (Tₚᵣₒₚ) = Distance / Speed of Light
    
    Distance has a strong impact on transmission time through:
    - Bandwidth reduction (more reduction for longer distances)
    - Network congestion (more congestion for longer distances)
    - Signal degradation (more degradation for longer distances)
    - Packet loss (higher for longer distances)
    - Distance penalty multiplier
    
    Args:
        task_location (dict): Task location data
        fog_location (dict): Fog node location data
        fog_node: Fog node object with bandwidth attribute
        task_size_mi (float): Task size in Million Instructions (MI) - deprecated
        task_mips (float): Task MIPS requirement
        logger: Optional logger for debug information
        
    Returns:
        float: Total transmission time in seconds
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
    
    # Calculate base Propagation Delay (Tₚᵣₒₚ)
    speed_of_light = 200000  # km/s (speed of light in fiber optic cable)
    base_propagation_delay = distance / speed_of_light
    
    # Check if it's a cloud node
    is_cloud = hasattr(fog_node, 'is_cloud') and fog_node.is_cloud
    
    # Add signal degradation factor based on distance - reduced variation
    if is_cloud:
        # Cloud nodes have higher base degradation
        if distance <= 500:  # Within 500km
            signal_factor = random.uniform(1.1, 1.2)  # Higher base degradation
        elif distance <= 1000:  # Within 1000km
            signal_factor = random.uniform(1.2, 1.3)
        elif distance <= 2000:  # Within 2000km
            signal_factor = random.uniform(1.3, 1.4)
        else:  # Long distance
            signal_factor = random.uniform(1.4, 1.5)
    else:
        # Fog nodes have lower base degradation
        if distance <= 100:  # Within 100km
            signal_factor = random.uniform(0.97, 1.03)  # Minimal degradation
        elif distance <= 500:  # Within 500km
            signal_factor = random.uniform(0.95, 1.05)  # Low degradation
        elif distance <= 1000:  # Within 1000km
            signal_factor = random.uniform(0.93, 1.07)  # Moderate degradation
        elif distance <= 2000:  # Within 2000km
            signal_factor = random.uniform(0.90, 1.10)  # High degradation
        else:  # Long distance
            signal_factor = random.uniform(0.87, 1.13)  # Very high degradation
    
    propagation_delay = base_propagation_delay * signal_factor
    
    # Calculate base transmission time
    transmission_time = (distance * task_size_mi) / (fog_node.bandwidth * 1000)  # Convert to seconds
    
    # Add distance-based penalties - reduced penalties
    if is_cloud:
        # Cloud nodes have higher distance penalties
        if distance <= 500:  # Within 500km
            distance_penalty = 1.3
        elif distance <= 1000:  # Within 1000km
            distance_penalty = 1.5
        elif distance <= 2000:  # Within 2000km
            distance_penalty = 1.7
        else:  # Long distance
            distance_penalty = 1.9
    else:
        # Fog nodes have lower distance penalties
        if distance <= 100:  # Within 100km
            distance_penalty = 1.0
        elif distance <= 500:  # Within 500km
            distance_penalty = 1.2
        elif distance <= 1000:  # Within 1000km
            distance_penalty = 1.4
        elif distance <= 2000:  # Within 2000km
            distance_penalty = 1.6
        else:  # Long distance
            distance_penalty = 1.8
    
    # Apply distance penalty
    transmission_time *= distance_penalty
    
    # Add network congestion factor based on distance - reduced variation
    if is_cloud:
        # Cloud nodes have higher congestion
        if distance <= 500:  # Within 500km
            congestion_factor = random.uniform(1.2, 1.3)  # Higher base congestion
        elif distance <= 1000:  # Within 1000km
            congestion_factor = random.uniform(1.3, 1.4)
        elif distance <= 2000:  # Within 2000km
            congestion_factor = random.uniform(1.4, 1.5)
        else:  # Long distance
            congestion_factor = random.uniform(1.5, 1.6)
    else:
        # Fog nodes have lower congestion
        if distance <= 100:  # Within 100km
            congestion_factor = random.uniform(1.0, 1.1)  # Low congestion
        elif distance <= 500:  # Within 500km
            congestion_factor = random.uniform(1.1, 1.2)  # Moderate congestion
        elif distance <= 1000:  # Within 1000km
            congestion_factor = random.uniform(1.2, 1.3)  # High congestion
        elif distance <= 2000:  # Within 2000km
            congestion_factor = random.uniform(1.3, 1.4)  # Very high congestion
        else:  # Long distance
            congestion_factor = random.uniform(1.4, 1.5)  # Extreme congestion
    
    transmission_time *= congestion_factor
    
    # Add packet loss factor based on distance - reduced packet loss
    if is_cloud:
        # Cloud nodes have higher packet loss
        if distance <= 500:  # Within 500km
            packet_loss = random.uniform(0.01, 0.02)  # 1-2% packet loss
        elif distance <= 1000:  # Within 1000km
            packet_loss = random.uniform(0.02, 0.03)  # 2-3% packet loss
        elif distance <= 2000:  # Within 2000km
            packet_loss = random.uniform(0.03, 0.04)  # 3-4% packet loss
        else:  # Long distance
            packet_loss = random.uniform(0.04, 0.05)  # 4-5% packet loss
    else:
        # Fog nodes have lower packet loss
        if distance <= 100:  # Within 100km
            packet_loss = random.uniform(0.0, 0.005)  # 0-0.5% packet loss
        elif distance <= 500:  # Within 500km
            packet_loss = random.uniform(0.005, 0.015)  # 0.5-1.5% packet loss
        elif distance <= 1000:  # Within 1000km
            packet_loss = random.uniform(0.015, 0.025)  # 1.5-2.5% packet loss
        elif distance <= 2000:  # Within 2000km
            packet_loss = random.uniform(0.025, 0.035)  # 2.5-3.5% packet loss
        else:  # Long distance
            packet_loss = random.uniform(0.035, 0.050)  # 3.5-5% packet loss
    
    # Add retransmission overhead based on packet loss
    retransmission_overhead = transmission_time * (packet_loss / (1 - packet_loss))
    
    # Add network jitter (random delay variation) - reduced jitter
    if is_cloud:
        # Cloud nodes have higher jitter
        if distance <= 500:  # Within 500km
            jitter = random.uniform(0.1, 0.15) * base_propagation_delay
        elif distance <= 1000:  # Within 1000km
            jitter = random.uniform(0.15, 0.2) * base_propagation_delay
        elif distance <= 2000:  # Within 2000km
            jitter = random.uniform(0.2, 0.25) * base_propagation_delay
        else:  # Long distance
            jitter = random.uniform(0.25, 0.3) * base_propagation_delay
    else:
        # Fog nodes have lower jitter
        if distance <= 100:  # Within 100km
            jitter = random.uniform(0.0, 0.05) * base_propagation_delay
        elif distance <= 500:  # Within 500km
            jitter = random.uniform(0.0, 0.10) * base_propagation_delay
        elif distance <= 1000:  # Within 1000km
            jitter = random.uniform(0.0, 0.15) * base_propagation_delay
        elif distance <= 2000:  # Within 2000km
            jitter = random.uniform(0.0, 0.20) * base_propagation_delay
        else:  # Long distance
            jitter = random.uniform(0.0, 0.25) * base_propagation_delay
    
    transmission_time += retransmission_overhead + jitter
    
    # Total transmission time is sum of transmission time and propagation delay
    total_time = transmission_time + propagation_delay
    
    # Log debug information if logger is provided
    if logger:
        logger.debug("Transmission calculation details:")
        logger.debug(f"  Task location: {task_location}")
        logger.debug(f"  Fog node location: {fog_location}")
        logger.debug(f"  Distance: {distance:.2f} km")
        logger.debug(f"  Task MIPS: {task_mips if task_mips is not None else 'unknown'}")
        logger.debug(f"  Base Propagation Delay: {base_propagation_delay*1000:.2f} ms")
        logger.debug(f"  Signal Factor: {signal_factor:.2f}")
        logger.debug(f"  Final Propagation Delay (Tₚᵣₒₚ): {propagation_delay*1000:.2f} ms")
        logger.debug(f"  Base Transmission Time: {transmission_time*1000:.2f} ms")
        logger.debug(f"  Congestion Factor: {congestion_factor:.2f}")
        logger.debug(f"  Packet Loss: {packet_loss*100:.2f}%")
        logger.debug(f"  Retransmission Overhead: {retransmission_overhead*1000:.2f} ms")
        logger.debug(f"  Network Jitter: {jitter*1000:.2f} ms")
        logger.debug(f"  Distance Penalty: {distance_penalty:.2f}x")
        logger.debug(f"  Final Transmission Time (Tₜₓ): {transmission_time*1000:.2f} ms")
        logger.debug(f"  Total transmission time: {total_time*1000:.2f} ms")
    
    if logger:
        logger.info(f"Transmission time: {total_time*1000:.2f}ms")
    return total_time

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
    latency = base_latency * bandwidth_factor
    
    if logger:
        logger.info(f"Geographic latency: {latency*1000:.2f}ms")
    return latency

def calculate_storage_requirements(task_size):
    """
    Calculate storage requirements for a task based on its size.
    
    Args:
        task_size (float): Size of the task in MI
        
    Returns:
        float: Required storage in GB
    """
    # Storage is 10% of task size
    storage_required = task_size * 0.1
    return round(storage_required, 2)

def calculate_storage_utilization(node):
    """
    Calculate storage utilization for a node.
    
    Args:
        node: Node object with storage attributes
        
    Returns:
        float: Storage utilization as a percentage
    """
    if not hasattr(node, 'storage') or not hasattr(node, 'available_storage'):
        return 0.0
        
    total_storage = node.storage
    available_storage = node.available_storage
    used_storage = total_storage - available_storage
    
    return (used_storage / total_storage) * 100 if total_storage > 0 else 0.0

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
    queue_delay = queue_size * avg_processing_time
    
    if logger:
        logger.info(f"Queue delay: {queue_delay*1000:.2f}ms")
    return queue_delay

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
    """Calculate processing time based on task size and node processing power"""
    if processing_power_mips <= 0:
        return float('inf')
    
    # Calculate base processing time
    base_time = task_size_mi / processing_power_mips
    
    # Add CPU overhead with reduced random variation (10-20% of base time)
    cpu_overhead = base_time * random.uniform(0.1, 0.2)
    
    # Add memory access time with reduced random variation (7-15% of base time)
    memory_access = base_time * random.uniform(0.07, 0.15)
    
    # Add system load factor with reduced random variation (5-12% of base time)
    system_load = base_time * random.uniform(0.05, 0.12)
    
    # Add cache miss penalty with reduced random variation (2-8% of base time)
    cache_miss = base_time * random.uniform(0.02, 0.08)
    
    # Add I/O wait time with reduced random variation (3-10% of base time)
    io_wait = base_time * random.uniform(0.03, 0.1)
    
    # Total processing time with all factors
    total_time = base_time + cpu_overhead + memory_access + system_load + cache_miss + io_wait
    
    # Add smaller random variation (±7%) to make it more realistic
    variation = random.uniform(0.93, 1.07)
    total_time *= variation
    
    return total_time
