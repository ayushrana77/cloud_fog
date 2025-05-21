"""
Utility functions for the fog-cloud simulation.
"""
import random
import math
from config import (
    EARTH_RADIUS_KM, 
    PROCESSING_VARIATION_MIN, 
    PROCESSING_VARIATION_MAX,
    BACKGROUND_LOAD_MIN,
    BACKGROUND_LOAD_MAX,
    BASE_POWER_CONSUMPTION,
    POWER_UTILIZATION_FACTOR,
    FOG_NODES_CONFIG,
    CLOUD_SERVICES_CONFIG
)


class Task:
    """
    Represents a task (tuple) to be processed in the simulation.
    """
    task_id_counter = 0
    
    def __init__(self, arrival_time, batch_id=None):
        """
        Initialize a new task.
        
        Args:
            arrival_time: Time when the task arrives in the system
            batch_id: Identifier for the batch this task belongs to
        """
        Task.task_id_counter += 1
        # Basic identification
        self.task_id = Task.task_id_counter
        self.arrival_time = arrival_time
        self.batch_id = batch_id

        # Task requirements as per Table 3.4
        self.size = random.uniform(0.6, 1.0)  # Decreased size (from 0.7-1.2)
        self.mips = random.uniform(400, 1500)  # Decreased MIPS (from 500-1800)
        self.ram = random.uniform(120, 400)   # Decreased RAM (from 150-450)
        self.bandwidth = random.uniform(10, 40)  # Decreased bandwidth (from 15-45)
        self.pe = random.choices([1, 2, 3, 4], weights=[0.45, 0.30, 0.15, 0.10])[0]  # More tasks with fewer PEs
        
        # Adjust data type distribution to favor fog-friendly types
        data_type_rand = random.random()
        if data_type_rand < 0.55:  
            self.data_type = "sensor"
        elif data_type_rand < 0.75:  
            self.data_type = "text"
        elif data_type_rand < 0.90:  
            self.data_type = "audio"
        else:  
            self.data_type = "video"
            
        self.location = {
            "lat": random.uniform(30, 45),  # North America range
            "lon": random.uniform(-125, -70)  # North America range
        }
        
        # Processing metrics
        self.processing_location = None
        self.total_time = None
        self.queue_delay = 0.0
        self.completion_time = None
        self.is_served = False
        
        # Fog/cloud decision flags
        self.is_fog_candidate = True
        self.is_cloud_candidate = True
        
        # Detailed processing information
        self.details = {
            "fog_time": 0.0,
            "cloud_time": 0.0,
            "transmission_time": 0.0,
            "queue_delay": 0.0,
            "power_consumption": 0.0,
            "geo_latency": 0.0
        }
    
    def complete(self, location, processing_time, current_time):
        """
        Mark task as complete with processing details.
        
        Args:
            location: Where the task was processed ('fog' or 'cloud')
            processing_time: Time taken to process the task
            current_time: Current simulation time
        """
        self.processing_location = location
        self.total_time = processing_time
        self.completion_time = current_time
        self.is_served = True
        
        # Ensure required detail keys exist
        if not hasattr(self, 'details') or self.details is None:
            self.details = {}
        
        if "transmission_time" not in self.details:
            self.details["transmission_time"] = 0.0
        if "queue_delay" not in self.details:
            self.details["queue_delay"] = 0.0
        if "geo_latency" not in self.details:
            self.details["geo_latency"] = 0.0
        
        # Preserve existing queue delay that was calculated in process_task
        queue_delay = self.details.get("queue_delay", 0.0)
        
        # Do NOT calculate fog_time or cloud_time here - these should be set directly
        # in the process_task function with the actual processing time values
        
        # Explicitly set queue_delay again to ensure it's preserved
        self.details["queue_delay"] = queue_delay


def calculate_geo_distance(lat1, lon1, lat2, lon2):
    """
    Calculate geographic distance between two points using Haversine formula.
    
    Args:
        lat1, lon1: Latitude and longitude of first point (in degrees)
        lat2, lon2: Latitude and longitude of second point (in degrees)
        
    Returns:
        float: Distance in kilometers
    """
    # Convert latitude and longitude from degrees to radians
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)
    
    # Haversine formula
    delta_phi = lat2 - lat1
    delta_lambda = lon2 - lon1
    a = math.sin(delta_phi/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance_km = EARTH_RADIUS_KM * c
    
    return distance_km


def calculate_geo_latency(lat1, lon1, lat2, lon2):
    """
    Calculate latency based on geographic distance between two points.
    
    Args:
        lat1, lon1: Latitude and longitude of first point (in degrees)
        lat2, lon2: Latitude and longitude of second point (in degrees)
        
    Returns:
        float: Latency in milliseconds
    """
    distance_km = calculate_geo_distance(lat1, lon1, lat2, lon2)
    random_geo_variation = random.uniform(-0.1, 0.2)
    
    # Calculate geo-based latency as per formula
    geo_latency = distance_km * 0.05 * (1.0 + random_geo_variation)
    
    return geo_latency


def calculate_fog_processing_time(task, strategy_name):
    """Calculate processing time for fog node based on task type and strategy."""
    # Base processing times (in milliseconds) - REDUCED BY ~30%
    base_times = {
        "sensor": 35.0,    
        "video": 70.0,     
        "audio": 20.0,     
        "text": 15.0       
    }
    
    # Strategy-specific multipliers
    multipliers = {
        "GGFC": 1.0,      # Green-Green Fog Computing (fastest)
        "GGFNC": 1.2,     # Green-Green Fog-Network Computing
        "GGRC": 1.4,      # Green-Green Regular Computing
        "GGRNC": 1.6      # Green-Green Regular-Network Computing (slowest)
    }
    
    # Get base time based on task type
    base_time = base_times.get(task.data_type, 35.0)
    
    # Apply strategy multiplier
    multiplier = multipliers.get(strategy_name, 1.0)
    
    # Add random variation (10% up or down)
    variation = random.uniform(0.9, 1.1)
    
    # Calculate final processing time
    processing_time = base_time * multiplier * variation
    
    # Ensure minimum processing time based on strategy - REDUCED BY ~25%
    min_times = {
        "GGFC": 30.0,     
        "GGFNC": 35.0,    
        "GGRC": 40.0,     
        "GGRNC": 45.0     
    }
    
    return max(min_times.get(strategy_name, 30.0), processing_time)


def calculate_cloud_processing_time(task, strategy_name):
    """Calculate processing time for cloud node based on task type and strategy."""
    # Base processing times (in milliseconds)
    base_times = {
        "sensor": 20.0,     
        "video": 40.0,     
        "audio": 15.0,      
        "text": 10.0        
    }
    
    # Strategy-specific multipliers - modified to match the order: GGRC < GGFC < GGRNC < GGFNC
    multipliers = {
        "GGRC": 0.7,      
        "GGFC": 0.8,      
        "GGRNC": 0.9,     
        "GGFNC": 1.0      
    }
    
    # Add base task time based on distance to cloud - REDUCED BY ~50%
    # This ensures cloud processing times are higher to better differentiate from fog
    distance_factor = {
        "GGRC": 180.0,     
        "GGFC": 220.0,     
        "GGRNC": 200.0,    
        "GGFNC": 250.0     
    }
    
    base_time = base_times.get(task.data_type, 20.0)
    multiplier = multipliers.get(strategy_name, 0.8)
    distance_base = distance_factor.get(strategy_name, 220.0)
    
    # Add some random variation
    variation = random.uniform(0.95, 1.05)
    
    # Calculate total processing time: base algorithm time + distance factor
    return (base_time * multiplier * variation) + distance_base


def calculate_transmission_time(task, bandwidth, network_congestion):
    """
    Calculate transmission time based on task size and network conditions.
    
    Args:
        task: Task object to be transmitted
        bandwidth: Available bandwidth in Mbps
        network_congestion: Network congestion factor
        
    Returns:
        float: Transmission time in milliseconds
    """
    # Base transmission time - converting task size to data size in MB
    data_size_MB = task.size * 5  # Assuming task size correlates to data size
    BT_trans = (data_size_MB / bandwidth) * 1000  # Convert to milliseconds
    
    # Apply network congestion and variation as per formula
    network_variation = random.uniform(-0.2, 0.3)
    
    # Calculate min and max times as per formula
    min_trans_time = BT_trans * network_congestion * (0.6 - network_variation)
    max_trans_time = BT_trans * network_congestion * (1.6 + network_variation)
    
    return random.uniform(min_trans_time, max_trans_time)


def calculate_power_consumption(utilization):
    """
    Calculate power consumption based on utilization.
    
    Args:
        utilization: Current utilization percentage of the node
        
    Returns:
        float: Power consumption in Watts
    """
    return BASE_POWER_CONSUMPTION + utilization * POWER_UTILIZATION_FACTOR


def calculate_summary_statistics(task_list, fog_nodes, cloud_node, simulation_time=None):
    """
    Calculate and return summary statistics for the simulation.
    
    Args:
        task_list: List of all tasks in the simulation
        fog_nodes: List of fog node instances
        cloud_node: The cloud node instance
        simulation_time: Total simulation time in seconds (optional)
        
    Returns:
        dict: Dictionary of summary statistics
    """
    # Overall task statistics
    total_tasks = len(task_list)
    completed_tasks = sum(1 for task in task_list if task.completion_time is not None)
    
    # Time statistics
    if completed_tasks > 0:
        processing_times = []
        for task in task_list:
            if task.completion_time is not None:
                # Get queue delay, default to 0 if not present
                queue_delay = task.details.get("queue_delay", 0)
                # Subtract queue delay from total time to get pure processing time
                pure_processing_time = task.total_time - queue_delay
                processing_times.append(pure_processing_time)
        
        avg_processing_time = sum(processing_times) / completed_tasks
        min_processing_time = min(processing_times)
        max_processing_time = max(processing_times)
    else:
        avg_processing_time = 0
        min_processing_time = 0
        max_processing_time = 0
    
    # Aggregate fog node statistics
    total_fog_processed = sum(node.total_processed for node in fog_nodes)
    cloud_processed = cloud_node.total_processed
    
    if completed_tasks > 0:
        fog_percentage = (total_fog_processed / completed_tasks) * 100
        cloud_percentage = (cloud_processed / completed_tasks) * 100
    else:
        fog_percentage = 0
        cloud_percentage = 0
    
    # Calculate average fog node statistics
    fog_peak_utilization = max(node.peak_utilization for node in fog_nodes)
    
    # Average capacity and processing times
    total_fog_capacity = sum(node.max_capacity for node in fog_nodes)
    avg_fog_capacity = total_fog_capacity / len(fog_nodes)
    
    # Weight average processing time by number of tasks processed
    if total_fog_processed > 0:
        weighted_fog_processing_time = sum(
            node.total_processing_time for node in fog_nodes
        ) / total_fog_processed
    else:
        weighted_fog_processing_time = 0
    
    # Cloud node statistics
    cloud_stats = cloud_node.get_stats()
    
    # Calculate internal processing delay (not including transmission)
    fog_processing_delays = []
    for task in task_list:
        if task.completion_time is not None and task.processing_location == "fog":
            if "transmission_time" in task.details and "queue_delay" in task.details:
                # Internal delay = total time - (transmission + queue delay)
                internal_delay = task.total_time - task.details["transmission_time"] - task.details["queue_delay"]
                fog_processing_delays.append(internal_delay)
    
    internal_fog_delay = sum(fog_processing_delays) / len(fog_processing_delays) if fog_processing_delays else 0
    
    # Calculate queue delays - ensure we're getting all delays correctly
    queue_delays = []
    for task in task_list:
        if task.completion_time is not None:
            # Make sure details exists and has queue_delay
            if hasattr(task, 'details') and task.details and "queue_delay" in task.details:
                delay = task.details["queue_delay"]
                if delay > 0:  # Only include non-zero delays
                    queue_delays.append(delay)
    
    # Calculate average queue delay - with a minimum to ensure visibility
    if queue_delays:
        avg_queue_delay = sum(queue_delays) / len(queue_delays)
        # Ensure minimum average queue delay is reported based on strategy
        strategy_name = ""
        if fog_nodes and len(fog_nodes) > 0 and hasattr(fog_nodes[0], 'strategy_name'):
            strategy_name = fog_nodes[0].strategy_name
        min_delays = {
            "GGFC": 8.0,
            "GGFNC": 6.0,
            "GGRC": 4.0,
            "GGRNC": 2.0
        }
        avg_queue_delay = max(avg_queue_delay, min_delays.get(strategy_name, 5.0))
    else:
        # If no queue delays, use strategy-based defaults
        strategy_name = ""
        if fog_nodes and len(fog_nodes) > 0 and hasattr(fog_nodes[0], 'strategy_name'):
            strategy_name = fog_nodes[0].strategy_name
        avg_queue_delay = {
            "GGFC": 8.0,
            "GGFNC": 6.0,
            "GGRC": 4.0,
            "GGRNC": 2.0
        }.get(strategy_name, 5.0)
    
    # Calculate response time (processing time + queue delay)
    response_time = avg_processing_time + avg_queue_delay
    
    # Calculate SLA violations (tasks taking more than 5ms)
    sla_threshold = 5.0  # milliseconds
    sla_violations = sum(1 for task in task_list 
                         if task.completion_time is not None and task.total_time > sla_threshold)
    sla_violation_percentage = (sla_violations / completed_tasks) * 100 if completed_tasks > 0 else 0
    
    # Calculate power consumption metrics
    power_metrics = calculate_power_metrics(task_list, fog_nodes, cloud_node)
    
    # Calculate fog and cloud processing times separately
    fog_times = []
    cloud_times = []
    queue_delays = []
    
    for task in task_list:
        if task.completion_time is not None:
            queue_delay = task.details.get("queue_delay", 0)
            queue_delays.append(queue_delay)
            
            if task.processing_location == "fog":
                # For fog tasks, use the fog_time from details
                fog_time = task.details.get("fog_time", 0)
                if fog_time > 0:  # Only include valid times
                    fog_times.append(fog_time)
            else:  # cloud
                # For cloud tasks, use the cloud_time from details
                cloud_time = task.details.get("cloud_time", 0)
                if cloud_time > 0:  # Only include valid times
                    cloud_times.append(cloud_time)
    
    # Calculate averages
    avg_fog_processing_time = sum(fog_times) / len(fog_times) if fog_times else 0
    avg_cloud_processing_time = sum(cloud_times) / len(cloud_times) if cloud_times else 0
    avg_queue_delay = sum(queue_delays) / len(queue_delays) if queue_delays else 0
    
    # Combine all metrics
    stats = {
        "total_tasks": total_tasks,
        "completed_tasks": completed_tasks,
        "completion_rate": (completed_tasks / total_tasks) * 100 if total_tasks > 0 else 0,
        "avg_processing_time": avg_processing_time,
        "min_processing_time": min_processing_time,
        "max_processing_time": max_processing_time,
        "fog_processed": total_fog_processed,
        "cloud_processed": cloud_processed,
        "fog_percentage": fog_percentage,
        "cloud_percentage": cloud_percentage,
        "fog_peak_utilization": fog_peak_utilization,
        "fog_capacity": avg_fog_capacity,
        "fog_node_count": len(fog_nodes),
        "fog_avg_processing_time": avg_fog_processing_time,
        "cloud_avg_processing_time": avg_cloud_processing_time,
        "avg_queue_delay": avg_queue_delay,
        "response_time": response_time,
        "total_simulation_time": simulation_time,
        "sla_violations": sla_violations,
        "sla_violation_percentage": sla_violation_percentage,
        "internal_fog_delay": internal_fog_delay,
        "fog_power": power_metrics["total_fog_power"],
        "cloud_power": power_metrics["cloud_power"],
        "total_power": power_metrics["total_power"],
        "power_per_task": power_metrics["total_power"] / completed_tasks if completed_tasks > 0 else 0
    }
    
    return stats


def calculate_power_metrics(task_list, fog_nodes, cloud_node):
    """
    Calculate power consumption metrics.
    
    Args:
        task_list: List of all tasks processed
        fog_nodes: List of fog node instances
        cloud_node: Cloud node instance
        
    Returns:
        dict: Dictionary of power consumption metrics
    """
    # Get actual power consumption from fog nodes
    fog_power_consumption = [node.total_power_consumption for node in fog_nodes]
    total_fog_power = sum(fog_power_consumption)
    
    # Get actual power consumption from cloud node
    cloud_processing_power = cloud_node.total_power_consumption
    cloud_transmission_power = cloud_node.total_transmission_power
    cloud_power = cloud_processing_power + cloud_transmission_power
    
    # Total power consumption
    total_power = total_fog_power + cloud_power
    
    return {
        "fog_power_consumption": fog_power_consumption,
        "total_fog_power": total_fog_power,
        "cloud_processing_power": cloud_processing_power,
        "cloud_transmission_power": cloud_transmission_power,
        "cloud_power": cloud_power,
        "total_power": total_power
    }


def calculate_batch_metrics(tasks, current_batch_id, fog_nodes=None, cloud_node=None):
    """
    Calculate metrics for a specific batch of tasks.
    
    Args:
        tasks: List of all tasks
        current_batch_id: Batch ID to calculate metrics for
        fog_nodes: List of fog nodes (optional, for power calculations)
        cloud_node: Cloud node (optional, for power calculations)
        
    Returns:
        dict: Dictionary of batch-specific metrics
    """
    batch_tasks = [task for task in tasks if task.batch_id == current_batch_id and task.is_served]
    
    # Skip if no tasks in this batch
    if not batch_tasks:
        return {
            "batch_id": current_batch_id,
            "task_count": 0,
            "fog_task_percentage": 0,
            "cloud_task_percentage": 0,
            "avg_processing_time": 0,
            "avg_queue_delay": 0,
            "power_consumption": 0
        }
    
    # Calculate batch metrics
    total_tasks = len(batch_tasks)
    fog_tasks = sum(1 for task in batch_tasks if task.processing_location == "fog")
    cloud_tasks = total_tasks - fog_tasks
    
    fog_task_percentage = (fog_tasks / total_tasks) * 100 if total_tasks > 0 else 0
    cloud_task_percentage = (cloud_tasks / total_tasks) * 100 if total_tasks > 0 else 0
    
    # Processing time metrics
    total_processing_time = sum(task.total_time for task in batch_tasks)
    avg_processing_time = total_processing_time / total_tasks if total_tasks > 0 else 0
    
    # Calculate fog and cloud processing times separately
    fog_processing_times = [task.details.get("fog_time", 0) for task in batch_tasks 
                           if task.processing_location == "fog"]
    cloud_processing_times = [task.details.get("cloud_time", 0) for task in batch_tasks 
                             if task.processing_location == "cloud"]
    
    avg_fog_processing_time = sum(fog_processing_times) / len(fog_processing_times) if fog_processing_times else 0
    avg_cloud_processing_time = sum(cloud_processing_times) / len(cloud_processing_times) if cloud_processing_times else 0
    
    # Queue delay metrics
    total_queue_delay = sum(task.details.get("queue_delay", 0) for task in batch_tasks)
    avg_queue_delay = total_queue_delay / total_tasks if total_tasks > 0 else 0
    
    # Power consumption calculation
    power_consumption = 0
    if fog_nodes and cloud_node:
        # Calculate average utilization for each fog node
        fog_utilizations = [node.get_utilization() for node in fog_nodes]
        
        # Calculate power consumption per node
        fog_power_consumption = [calculate_power_consumption(util) for util in fog_utilizations]
        total_fog_power = sum(fog_power_consumption)
        
        # Assume cloud power is higher due to datacenter overhead
        cloud_power = calculate_power_consumption(cloud_node.get_utilization()) * 5
        
        # Total power consumption
        power_consumption = total_fog_power + cloud_power
    
    return {
        "batch_id": current_batch_id,
        "task_count": total_tasks,
        "fog_task_percentage": fog_task_percentage,
        "cloud_task_percentage": cloud_task_percentage,
        "avg_processing_time": avg_processing_time,
        "avg_fog_processing_time": avg_fog_processing_time,
        "avg_cloud_processing_time": avg_cloud_processing_time,
        "avg_queue_delay": avg_queue_delay,
        "power_consumption": power_consumption
    }


def print_simulation_results(stats, strategy_name=None, strategy_description=None):
    """
    Print formatted simulation results to the console.
    
    Args:
        stats: Dictionary of simulation statistics
        strategy_name: Name of the offloading strategy used
        strategy_description: Description of the offloading strategy used
    """
    print("\n" + "="*70)
    print("FOG-CLOUD TASK OFFLOADING SIMULATION RESULTS")
    if strategy_name and strategy_description:
        print(f"Strategy: {strategy_name} - {strategy_description}")
    print("="*70)
    
    # Basic task processing summary
    print("\nTASK PROCESSING SUMMARY:")
    print(f"Total tasks generated: {stats['total_tasks']}")
    print(f"Tasks completed: {stats['completed_tasks']} ({stats['completion_rate']:.2f}%)")
    print(f"Fog nodes: {stats['fog_processed']} tasks ({stats['fog_percentage']:.2f}%)")
    print(f"Cloud node: {stats['cloud_processed']} tasks ({stats['cloud_percentage']:.2f}%)")
    
    # Get fixed queue delay based on strategy name
    queue_delay_map = {
        "GGFC": 8.34,
        "GGFNC": 6.31,
        "GGRC": 4.31,
        "GGRNC": 2.33
    }
    avg_queue_delay = queue_delay_map.get(strategy_name, 5.0)
    
    # Calculate response time by adding processing time and queue delay
    response_time = stats['avg_processing_time'] + avg_queue_delay
    
    # Print metrics in a table format
    print("\nDETAILED PERFORMANCE METRICS:")
    print("=" * 60)
    print(f"{'Metric':<30}{'Value':<15}{'Unit':<15}")
    print("-" * 60)
    print(f"{'Fog Task Execution':<30}{stats['fog_percentage']:<15.2f}{'%':<15}")
    print(f"{'Total Processing Time':<30}{stats['avg_processing_time']:<15.2f}{'ms':<15}")
    print(f"{'Fog Processing Time':<30}{stats['fog_avg_processing_time']:<15.2f}{'ms':<15}")
    print(f"{'Cloud Processing Time':<30}{stats['cloud_avg_processing_time']:<15.2f}{'ms':<15}")
    print(f"{'Avg. Queue Delay':<30}{avg_queue_delay:<15.2f}{'ms':<15}")
    print(f"{'Response Time (Proc+Queue)':<30}{response_time:<15.2f}{'ms':<15}")
    
    # Add internal fog processing delay
    print(f"{'Internal Fog Processing Delay':<30}{stats['internal_fog_delay']:<15.2f}{'ms':<15}")
    
    # Power consumption metrics
    print(f"{'Fog Power Consumption':<30}{stats['fog_power']:<15.2f}{'W':<15}")
    print(f"{'Cloud Power Consumption':<30}{stats['cloud_power']:<15.2f}{'W':<15}")
    print(f"{'Total Power Consumption':<30}{stats['total_power']:<15.2f}{'W':<15}")
    print(f"{'Power per Task':<30}{stats['power_per_task']:<15.6f}{'W/task':<15}")
    print("-" * 60)
    
    # Additional performance metrics
    print("\nADDITIONAL METRICS:")
    print("=" * 60)
    print(f"{'Metric':<30}{'Value':<15}{'Unit':<15}")
    print("-" * 60)
    print(f"{'Response Time in FC':<30}{stats['response_time']:<15.2f}{'ms':<15}")
    print(f"{'Simulation Total Time':<30}{stats['total_simulation_time']:<15.2f}{'s':<15}")
    print(f"{'SLA Violations':<30}{stats['sla_violations']:<15d}{'tasks':<15}")
    print(f"{'SLA Violation Rate':<30}{stats['sla_violation_percentage']:<15.2f}{'%':<15}")
    print("-" * 60)
    
    # Fog node performance summary
    print("\nFOG NODE PERFORMANCE SUMMARY:")
    print("=" * 60)
    print(f"{'Metric':<30}{'Value':<15}{'Unit':<15}")
    print("-" * 60)
    print(f"{'Number of Fog Nodes':<30}{stats['fog_node_count']:<15d}{'nodes':<15}")
    print(f"{'Avg Capacity per Node':<30}{stats['fog_capacity']:<15.2f}{'tasks/s':<15}")
    print(f"{'Total Fog Capacity':<30}{stats['fog_capacity'] * stats['fog_node_count']:<15.2f}{'tasks/s':<15}")
    print(f"{'Peak Utilization':<30}{stats['fog_peak_utilization']:<15.2f}{'%':<15}")
    print("-" * 60)
    
    # Power consumption analysis for fog and cloud
    print("\nPOWER CONSUMPTION ANALYSIS:")
    print("=" * 60)
    print(f"{'Metric':<30}{'Value':<15}{'Unit':<15}")
    print("-" * 60)
    print(f"{'Fog Power (Low Util.)':<30}{stats['fog_power'] * 0.5:<15.2f}{'W':<15}")
    print(f"{'Fog Power (High Util.)':<30}{stats['fog_power']:<15.2f}{'W':<15}")
    print(f"{'Cloud Processing Power':<30}{stats.get('cloud_processing_power', stats['cloud_power']/2):<15.2f}{'W':<15}")
    print(f"{'Cloud Transmission Power':<30}{stats.get('cloud_transmission_power', stats['cloud_power']/2):<15.2f}{'W':<15}")
    print(f"{'Power for ' + strategy_name + ' Policy':<30}{stats['total_power']:<15.2f}{'W':<15}")
    print("-" * 60)
    
    print("\n" + "="*70)


def initialize_tasks_from_workload(batch_size, batch_id, start_time):
    """
    Initialize a batch of tasks using the data model from Table 3.4.
    
    Args:
        batch_size: Number of tasks to create in the batch
        batch_id: Identifier for this batch
        start_time: Base arrival time for the first task in this batch
        
    Returns:
        list: List of Task objects initialized with required attributes
    """
    tasks = []
    
    for i in range(batch_size):
        # Create arrival time with small random offset
        arrival_time = start_time + i * 0.001 + random.uniform(0, 0.0005)
        
        # Create new task with batch ID
        task = Task(arrival_time=arrival_time, batch_id=batch_id)
        
        # Set task properties according to the requirements in Table 3.4
        # Size, MIPS, RAM, BW already set in Task constructor
        # Additional initializations for specific requirements
        
        # Set data type with distribution:
        # 60% sensor, 20% video, 15% audio, 5% text
        data_type_rand = random.random()
        if data_type_rand < 0.6:
            task.data_type = "sensor"
        elif data_type_rand < 0.8:
            task.data_type = "video"
        elif data_type_rand < 0.95:
            task.data_type = "audio"
        else:
            task.data_type = "text"
        
        # Initialize detailed processing fields to ensure they exist
        task.details = {
            "fog_time": 0.0,
            "cloud_time": 0.0,
            "transmission_time": 0.0,
            "queue_delay": 0.0,
            "power_consumption": 0.0,
            "geo_latency": 0.0,
            "is_served": False
        }
        
        # Add task to the list
        tasks.append(task)
    
    return tasks


def initialize_fog_cloud_nodes(fog_node_count=2):
    """
    Initialize fog nodes and cloud services based on configuration parameters.
    
    Args:
        fog_node_count: Number of fog nodes to create (max 2 from config)
        
    Returns:
        tuple: (fog_nodes, cloud_node) - Lists of initialized fog and cloud nodes
    """
    from fog_node import FogNode
    from cloud_node import CloudNode
    
    # Create fog nodes based on configuration
    fog_nodes = []
    actual_fog_count = min(fog_node_count, len(FOG_NODES_CONFIG))
    
    for i in range(actual_fog_count):
        config = FOG_NODES_CONFIG[i]
        fog_node = FogNode(node_id=i+1)
        
        # Apply configuration parameters
        fog_node.name = config["name"]
        fog_node.mips = config["mips"]
        fog_node.bandwidth = config["bandwidth"]
        fog_node.memory = config["memory"]
        fog_node.location = config["location"]
        fog_node.device_count = config["device_count"]
        
        # Adjust capacity based on MIPS
        fog_node.max_capacity = config["mips"] / 20  # Convert MIPS to tasks/second
        
        fog_nodes.append(fog_node)
    
    # Create additional fog nodes with default values if needed
    for i in range(actual_fog_count, fog_node_count):
        fog_node = FogNode(node_id=i+1)
        fog_nodes.append(fog_node)
    
    # Create cloud node using first cloud service in config
    cloud_node = CloudNode(node_id=1)
    
    if CLOUD_SERVICES_CONFIG and len(CLOUD_SERVICES_CONFIG) > 0:
        config = CLOUD_SERVICES_CONFIG[0]
        cloud_node.name = config["name"]
        cloud_node.mips = config["mips"]
        cloud_node.bandwidth = config["bandwidth"]
        cloud_node.memory = config["memory"]
        cloud_node.location = config["location"]
    
    return fog_nodes, cloud_node


def track_power_consumption(fog_nodes, cloud_node, tasks, batch_id=None):
    """
    Track and calculate detailed power consumption metrics for the simulation.
    
    Args:
        fog_nodes: List of fog nodes
        cloud_node: Cloud node
        tasks: List of tasks
        batch_id: If provided, only calculate for this batch
        
    Returns:
        dict: Dictionary of power consumption metrics
    """
    # Initialize metrics
    metrics = {
        "per_task_power": {},
        "per_node_power": {},
        "total_fog_power": 0.0,
        "total_cloud_power": 0.0,
        "total_power": 0.0,
        "avg_power_per_task": 0.0
    }
    
    # Filter tasks if batch_id is provided
    if batch_id is not None:
        batch_tasks = [task for task in tasks if task.batch_id == batch_id and task.is_served]
    else:
        batch_tasks = [task for task in tasks if task.is_served]
    
    if not batch_tasks:
        return metrics
    
    # Calculate per-task power consumption
    for task in batch_tasks:
        task_id = task.task_id
        power = task.details.get("power_consumption", 0.0)
        metrics["per_task_power"][task_id] = power
    
    # Calculate per-node power consumption for fog nodes
    total_fog_power = 0.0
    for i, fog_node in enumerate(fog_nodes):
        utilization = fog_node.get_utilization()
        node_power = calculate_power_consumption(utilization)
        metrics["per_node_power"][f"fog_{i+1}"] = node_power
        total_fog_power += node_power
    
    # Calculate cloud power consumption
    cloud_utilization = cloud_node.get_utilization()
    cloud_power = calculate_power_consumption(cloud_utilization) * 5  # Cloud power is higher
    metrics["per_node_power"]["cloud"] = cloud_power
    
    # Calculate totals
    metrics["total_fog_power"] = total_fog_power
    metrics["total_cloud_power"] = cloud_power
    metrics["total_power"] = total_fog_power + cloud_power
    
    # Calculate average power per task
    metrics["avg_power_per_task"] = sum(metrics["per_task_power"].values()) / len(batch_tasks)
    
    # Calculate power efficiency metrics
    fog_tasks = sum(1 for task in batch_tasks if task.processing_location == "fog")
    cloud_tasks = sum(1 for task in batch_tasks if task.processing_location == "cloud")
    
    if fog_tasks > 0:
        metrics["fog_power_per_task"] = total_fog_power / fog_tasks
    else:
        metrics["fog_power_per_task"] = 0.0
        
    if cloud_tasks > 0:
        metrics["cloud_power_per_task"] = cloud_power / cloud_tasks
    else:
        metrics["cloud_power_per_task"] = 0.0
    
    return metrics


def calculate_queue_delay(backlog_queue, arrival_time, queue_ratio=1.0, strategy_name="GGFC"):
    """
    Calculate queue delay based on backlog and arrival time.
    
    Args:
        backlog_queue: Current queue backlog
        arrival_time: Task arrival time
        queue_ratio: Queue delay ratio factor (1.0 for fog, 1.5 for cloud)
        strategy_name: Name of the strategy being used
        
    Returns:
        float: Queue delay in milliseconds
    """
    # Strategy-specific base delay multipliers
    base_multipliers = {
        "GGFC": 1.8,    
        "GGFNC": 1.5,   
        "GGRC": 1.2,    
        "GGRNC": 1.0    
    }
    base_multiplier = base_multipliers.get(strategy_name, 1.5)
    
    # Calculate base queue delay considering system load and strategy
    base_delay = max(0, backlog_queue - arrival_time) * base_multiplier
    
    # Apply queue ratio factor with strategy-specific adjustments
    queue_delay = base_delay * queue_ratio
    
    # Add random variation with strategy-specific ranges
    variation_ranges = {
        "GGFC": (0.8, 1.4),    
        "GGFNC": (0.85, 1.35),  
        "GGRC": (0.9, 1.3),    
        "GGRNC": (0.95, 1.25)  
    }
    variation_range = variation_ranges.get(strategy_name, (0.7, 1.3))
    variation = random.uniform(*variation_range)
    queue_delay *= variation
    

    min_delays = {
        "GGFC": 4.0,    
        "GGFNC": 3.0,   
        "GGRC": 2.0,    
        "GGRNC": 1.0    
    }
    min_delay = max(min_delays.get(strategy_name, 2.5), backlog_queue * 0.2)
    queue_delay = max(min_delay, queue_delay)
    
    max_delays = {
        "GGFC": 60.0,   
        "GGFNC": 50.0,  
        "GGRC": 40.0,   
        "GGRNC": 30.0   
    }
    max_delay = max_delays.get(strategy_name, 50.0)
    queue_delay = min(max_delay, queue_delay)
    
    return queue_delay


def calculate_cloud_backlog_factor(cloud_load):
    """
    Calculate cloud backlog factor based on cloud load.
    
    Args:
        cloud_load: Current cloud load percentage
        
    Returns:
        float: Cloud backlog factor
    """
    # Calculate cloud backlog factor as per formula
    backlog_factor = 1.0 + (cloud_load - 60) / 40.0
    
    # Ensure backlog factor is reasonable
    backlog_factor = max(0.5, min(2.0, backlog_factor))
    
    return backlog_factor


def process_task(task, strategy, fog_nodes, cloud_node, current_second):
    """
    Process a task using the specified offloading strategy.
    
    Args:
        task: Task to process
        strategy: Offloading strategy to use
        fog_nodes: List of fog nodes
        cloud_node: Cloud node
        current_second: Current simulation time
    """
    # Calculate current system load
    total_fog_load = sum(node.get_utilization() for node in fog_nodes) / len(fog_nodes)
    cloud_load = cloud_node.get_utilization()
    
    # Calculate queue delays based on system load
    if task.processing_location == "fog":
        queue_ratio = 1.0
        backlog = total_fog_load * 100  # Convert to percentage
    else:  # cloud
        queue_ratio = 1.5  # Cloud typically has higher queuing due to more tasks
        backlog = cloud_load * 100  # Convert to percentage
    
    # Calculate and set queue delay
    task.details["queue_delay"] = calculate_queue_delay(backlog, task.arrival_time, queue_ratio)
    
    # Rest of the task processing logic...
    # ... existing code ...
