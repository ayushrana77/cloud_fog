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
    Calculate transmission time between task and fog node using:
    1. Transmission Time (Tₜₓ) = File Size / Bandwidth
    2. Propagation Delay (Tₚᵣₒₚ) = Distance / Speed of Light
    
    Random factors are added to simulate real-world network conditions:
    - Network congestion (affects bandwidth)
    - Signal degradation (affects propagation)
    - Packet loss and retransmission
    - Network jitter
    
    Args:
        task_location (dict): Task location data
        fog_location (dict): Fog node location data
        fog_node: Fog node object with bandwidth attribute
        task_size_mi (float): Task size in Million Instructions (MI)
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
    base_propagation_delay = distance / speed_of_light  # in seconds
    
    # Add signal degradation factor based on distance
    if distance <= 500:  # Within 500km
        signal_factor = random.uniform(0.98, 1.02)  # Less degradation
    elif distance <= 2000:  # Within 2000km
        signal_factor = random.uniform(0.90, 1.20)  # Moderate degradation
    else:  # Long distance
        signal_factor = random.uniform(0.80, 1.40)  # More degradation
    
    propagation_delay = base_propagation_delay * signal_factor
    
    # Calculate base Transmission Time (Tₜₓ)
    if task_size_mi is not None:
        # Convert MI to bytes (assuming 1 instruction = 4 bytes)
        task_size_bytes = task_size_mi * 1000000 * 4
        base_bandwidth = fog_node.bandwidth * 1024 * 1024  # Convert Mbps to bytes per second
        
        # Apply distance-based bandwidth reduction
        if distance <= 500:  # Within 500km
            bandwidth_factor = random.uniform(0.95, 1.0)  # 5% max reduction
        elif distance <= 2000:  # Within 2000km
            bandwidth_factor = random.uniform(0.70, 0.85)  # 30% max reduction
        else:  # Long distance
            bandwidth_factor = random.uniform(0.40, 0.60)  # 60% max reduction
            
        bandwidth = base_bandwidth * bandwidth_factor
        
        # Add network congestion factor based on distance
        if distance <= 500:  # Within 500km
            congestion_factor = random.uniform(0.95, 1.15)  # Less congestion
        elif distance <= 2000:  # Within 2000km
            congestion_factor = random.uniform(0.85, 1.35)  # Moderate congestion
        else:  # Long distance
            congestion_factor = random.uniform(0.75, 1.50)  # More congestion
        
        # Calculate packet loss probability based on distance
        if distance <= 500:  # Within 500km
            packet_loss_rate = random.uniform(0.0, 0.03)  # 0-3% packet loss
        elif distance <= 2000:  # Within 2000km
            packet_loss_rate = random.uniform(0.0, 0.08)  # 0-8% packet loss
        else:  # Long distance
            packet_loss_rate = random.uniform(0.0, 0.15)  # 0-15% packet loss
        
        # Calculate base transmission time with congestion
        base_transmission_time = (task_size_bytes / bandwidth) * congestion_factor
        
        # Add retransmission overhead due to packet loss
        retransmission_overhead = base_transmission_time * packet_loss_rate
        
        # Add network jitter (random delay variation)
        if distance <= 500:  # Within 500km
            jitter = random.uniform(-0.1, 0.1) * base_transmission_time
        elif distance <= 2000:  # Within 2000km
            jitter = random.uniform(-0.2, 0.2) * base_transmission_time
        else:  # Long distance
            jitter = random.uniform(-0.3, 0.3) * base_transmission_time
        
        transmission_time = base_transmission_time + retransmission_overhead + jitter
    else:
        if logger:
            logger.warning("Task size not provided, using minimum transmission time")
        transmission_time = 0.001  # 1ms minimum transmission time
    
    # Total transmission time is sum of transmission time and propagation delay
    total_time = transmission_time + propagation_delay
    
    # Log debug information if logger is provided
    if logger:
        logger.debug("Transmission calculation details:")
        logger.debug(f"  Task location: {task_location}")
        logger.debug(f"  Fog node location: {fog_location}")
        logger.debug(f"  Distance: {distance:.2f} km")
        logger.debug(f"  Task size: {task_size_mi if task_size_mi is not None else 'unknown'} MI")
        logger.debug(f"  Base Propagation Delay: {base_propagation_delay*1000:.2f} ms")
        logger.debug(f"  Signal Factor: {signal_factor:.2f}")
        logger.debug(f"  Final Propagation Delay (Tₚᵣₒₚ): {propagation_delay*1000:.2f} ms")
        if task_size_mi is not None:
            logger.debug(f"  Base Transmission Time: {base_transmission_time*1000:.2f} ms")
            logger.debug(f"  Congestion Factor: {congestion_factor:.2f}")
            logger.debug(f"  Packet Loss Rate: {packet_loss_rate*100:.2f}%")
            logger.debug(f"  Retransmission Overhead: {retransmission_overhead*1000:.2f} ms")
            logger.debug(f"  Network Jitter: {jitter*1000:.2f} ms")
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
    
    # Add CPU overhead with random variation (20-40% of base time)
    cpu_overhead = base_time * random.uniform(0.2, 0.4)
    
    # Add memory access time with random variation (15-30% of base time)
    memory_access = base_time * random.uniform(0.15, 0.3)
    
    # Add system load factor with random variation (10-25% of base time)
    system_load = base_time * random.uniform(0.1, 0.25)
    
    # Add cache miss penalty with random variation (5-15% of base time)
    cache_miss = base_time * random.uniform(0.05, 0.15)
    
    # Add I/O wait time with random variation (5-20% of base time)
    io_wait = base_time * random.uniform(0.05, 0.2)
    
    # Total processing time with all factors
    total_time = base_time + cpu_overhead + memory_access + system_load + cache_miss + io_wait
    
    # Add some random variation (±15%) to make it more realistic
    variation = random.uniform(0.85, 1.15)
    total_time *= variation
    
    
    return total_time
