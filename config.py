"""
Configuration file for fog-cloud simulation constants.
"""

# Simulation parameters
TASK_RATE_PER_SECOND = 1000  # 1000 requests per second
MAX_SIMULATION_TIME = 100  # seconds (100k tuples / 1000 per second = 100 seconds)

# Network parameters
EARTH_RADIUS_KM = 6371.0  # Earth's radius in kilometers
SPEED_OF_LIGHT = 200000  # Speed of light in fiber optic cable (km/s)

# Variability parameters
NETWORK_CONGESTION_MIN = 0.6
NETWORK_CONGESTION_MAX = 1.5
PROCESSING_VARIATION_MIN = 0.7
PROCESSING_VARIATION_MAX = 1.8
BACKGROUND_LOAD_MIN = 0.0
BACKGROUND_LOAD_MAX = 0.25
BASE_VARIATION_FACTOR = 0.4

# Threshold parameters
HIGH_UTILIZATION_THRESHOLD = 100  # Increased from 90 to allow higher utilization
NETWORK_CONGESTION_THRESHOLD = 1.25  # Increased from 1.2 to reduce network congestion rejection

# Cloud node configurations
CLOUD_SERVICES_CONFIG = [
    # North America
    {
        "name": "Cloud-NA",
        "mips": 9000,  # Doubled MIPS for single server
        "bandwidth": 2000,  # 20 Gbps
        "memory": 9024,  # Doubled memory
        "location": {"lat": 39.0997, "lon": -94.5786},  # Kansas City (central USA)
    },
    # Asia
    {
        "name": "Cloud-AS",
        "mips": 9000,  # Doubled MIPS for single server
        "bandwidth": 9000,  # 20 Gbps
        "memory": 9024,  # Doubled memory
        "location": {"lat": 1.3521, "lon": 103.8198},  # Singapore
    }
]

# Fog node configurations
FOG_NODES_CONFIG = [
    # Fog Node 1 - North America
    {
        "name": "Fog-NA-1",
        "mips": 50000,  # Half of cloud MIPS
        "bandwidth": 10000,  # 10 Gbps
        "memory": 51200,  # Half of cloud memory
        "location": {"lat": 40.7128, "lon": -74.0060},  # New York
    },
    # Fog Node 2 - North America
    {
        "name": "Fog-NA-2",
        "mips": 50000,
        "bandwidth": 10000,
        "memory": 51200,
        "location": {"lat": 34.0522, "lon": -118.2437},  # Los Angeles
    },
    # Fog Node 3 - Asia
    {
        "name": "Fog-AS-1",
        "mips": 100000,
        "bandwidth": 25000,
        "memory": 101200,
        "location": {"lat": 35.6762, "lon": 139.6503},  # Tokyo
    }
]

# Task parameters
TASK_SIZE_MIN = 100  # KB
TASK_SIZE_MAX = 1000  # KB
TASK_MIPS_MIN = 1000
TASK_MIPS_MAX = 5000
TASK_MEMORY_MIN = 1000  
TASK_MEMORY_MAX = 5000  
