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
        "mips": 4000,  # Doubled MIPS for single server
        "bandwidth": 4000,  # 20 Gbps
        "memory": 4096,  # Doubled memory
        "storage": 10240,  # 10 TB storage
        "location": {"lat": 39.0997, "lon": -94.5786},  # Kansas City (central USA)
    },
    # Asia
    {
        "name": "Cloud-AS",
        "mips": 4000,  # Doubled MIPS for single server
        "bandwidth": 4000,  # 20 Gbps
        "memory": 4024,  # Doubled memory
        "storage": 10240,  # 10 TB storage
        "location": {"lat": 1.3521, "lon": 103.8198},  # Singapore
    }
]

# Fog Nodes Configuration
FOG_NODES_CONFIG = [
    # Delhi Region
    {
        "name": "Fog-DL1",
        "mips": 1000,
        "bandwidth": 2000,
        "memory": 1024,
        "storage": 1024,
        "location": {
            "lat": 28.6139,
            "lon": 77.2090
        }
    },
    # Gurgaon
    {
        "name": "Fog-GN1",
        "mips": 1000,
        "bandwidth": 2000,
        "memory": 1024,
        "storage": 1024,
        "location": {
            "lat": 28.4595,
            "lon": 77.0266
        }
    },
    # Noida
    {
        "name": "Fog-ND1",
        "mips": 1000,
        "bandwidth": 2000,
        "memory": 1024,
        "storage": 1024,
        "location": {
            "lat": 28.5355,
            "lon": 77.3910
        }
    },
    # Bangalore Region
    {
        "name": "Fog-BL1",
        "mips": 1000,
        "bandwidth": 2000,
        "memory": 1024,
        "storage": 1024,
        "location": {
            "lat": 12.9716,
            "lon": 77.5946
        }
    },
    # Electronic City
    {
        "name": "Fog-EC1",
        "mips": 1000,
        "bandwidth": 2000,
        "memory": 1024,
        "storage": 1024,
        "location": {
            "lat": 12.8458,
            "lon": 77.6658
        }
    },
    # Whitefield
    {
        "name": "Fog-WF1",
        "mips": 1000,
        "bandwidth": 2000,
        "memory": 1024,
        "storage": 1024,
        "location": {
            "lat": 12.9698,
            "lon": 77.7499
        }
    }
]

