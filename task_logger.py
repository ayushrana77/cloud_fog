import json
import asyncio
import time
from datetime import datetime
from config import TASK_RATE_PER_SECOND
from scheduler import Scheduler

class TaskLogger:
    def __init__(self):
        # Open all three log files
        self.task_log = open('task_log.txt', 'w', encoding='utf-8')
        self.cloud_log = open('cloud_log.txt', 'w', encoding='utf-8')
        self.fog_log = open('fog_log.txt', 'w', encoding='utf-8')
        self.start_time = time.time()
        
        # Write headers to each log file
        self._write_headers()
        
    def _write_headers(self):
        """Write headers to all log files."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Task log header
        task_header = f"""
Task Processing Log
Started at: {timestamp}
{'='*80}
"""
        self.task_log.write(task_header)
        
        # Cloud log header
        cloud_header = f"""
Cloud Node Status Log
Started at: {timestamp}
{'='*80}
"""
        self.cloud_log.write(cloud_header)
        
        # Fog log header
        fog_header = f"""
Fog Node Status Log
Started at: {timestamp}
{'='*80}
"""
        self.fog_log.write(fog_header)
        
        # Flush all files
        self._flush_all()
    
    def log_task(self, task_data, result, node):
        """Log task processing details."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Get task details
        task_size = task_data.get('Size', 0)
        task_mips = task_data.get('MIPS', 0)
        task_ram = task_data.get('RAM', 0)
        task_bw = task_data.get('BW', 0)
        task_location = task_data.get('GeoLocation', {})
        
        # Get node stats
        node_stats = node.get_stats()
        
        # Create task log entry (for task_log.txt)
        task_log_entry = f"""
{'='*80}
Task Details - {timestamp}
Task ID: {result.get('task_id', 'N/A')}
Size: {task_size} MB
MIPS Required: {task_mips} MIPS
RAM Required: {task_ram} MB
Bandwidth Required: {task_bw} Mbps
Location: {task_location.get('latitude', 'N/A')}, {task_location.get('longitude', 'N/A')}
Raw Task Data: {json.dumps(task_data, indent=4)}
{'='*80}
"""
        self.task_log.write(task_log_entry)
        
        # Create node-specific log entry
        node_log_entry = f"""
{'='*80}
Processing Details - {timestamp}
Node: {node.name}
Task ID: {result.get('task_id', 'N/A')}

Processing Metrics:
    Distance to Node: {result['distance_km']:.2f} km
    Transmission Time: {result['transmission_time']:.2f} ms
    Processing Time: {result['processing_time']:.2f} ms
    Queue Time: {result['queue_time']:.2f} ms
    Total Time: {result['total_time']:.2f} ms
    Memory Used: {result.get('memory_used', 0)} MB
    Concurrent Tasks: {len(node.current_tasks)} tasks

Node Status:
    Queue Size: {node_stats['queue_size']} tasks
    Tasks Processed: {node_stats['total_processed']}
    Active Tasks: {node_stats['current_tasks']}
    MIPS Utilization: {node_stats['mips_utilization']*100:.1f}%
    Bandwidth Utilization: {node_stats['bandwidth_utilization']*100:.1f}%
    Memory Utilization: {node_stats['memory_utilization']*100:.1f}%
    System Load: {node_stats['system_load']*100:.1f}%

Resource Availability:
    Free MIPS: {node.mips * (1 - node_stats['mips_utilization']):.0f} / {node.mips}
    Free Bandwidth: {node.bandwidth * (1 - node_stats['bandwidth_utilization']):.0f} Mbps / {node.bandwidth} Mbps
    Free Memory: {node.memory * (1 - node_stats['memory_utilization']):.0f} MB / {node.memory} MB

Performance Metrics:
    Average Processing Time: {self._calculate_avg_processing_time(node):.1f} ms
    Average Queue Time: {self._calculate_avg_queue_time(node):.1f} ms
    Processing Rate: {node_stats['total_processed'] / (time.time() - self.start_time):.1f} tasks/sec
{'='*80}
"""
        # Write to appropriate node log file
        if node.name.startswith('Cloud-'):
            self.cloud_log.write(node_log_entry)
        else:
            self.fog_log.write(node_log_entry)
        
        self._flush_all()
    
    def _calculate_avg_processing_time(self, node):
        """Calculate average processing time for the node."""
        if not hasattr(node, 'processing_times') or not node.processing_times:
            return 0
        return sum(node.processing_times) / len(node.processing_times)
    
    def _calculate_avg_queue_time(self, node):
        """Calculate average queue time for the node."""
        if not hasattr(node, 'queue_times') or not node.queue_times:
            return 0
        return sum(node.queue_times) / len(node.queue_times)
    
    def _flush_all(self):
        """Flush all log files."""
        self.task_log.flush()
        self.cloud_log.flush()
        self.fog_log.flush()
    
    def close(self):
        """Close all log files with summary."""
        duration = time.time() - self.start_time
        summary = f"""
Processing Complete
Duration: {duration:.1f} seconds
{'='*80}
"""
        self.task_log.write(summary)
        self.cloud_log.write(summary)
        self.fog_log.write(summary)
        
        self._flush_all()
        self.task_log.close()
        self.cloud_log.close()
        self.fog_log.close()

async def main():
    try:
        # Initialize task logger
        logger = TaskLogger()
        
        # Read the JSON file
        with open('Tuple10.json', 'r', encoding='utf-8-sig') as file:
            data = json.load(file)
            
            print(f"\nProcessing {len(data)} tuples")
            print(f"Processing rate: {TASK_RATE_PER_SECOND} tuples/second")
            print("-" * 50)
            
            # Ask user for processing mode
            while True:
                mode = input("\nSelect processing mode:\n1. Cloud Only\n2. Fog Only\nEnter choice (1 or 2): ")
                if mode in ['1', '2']:
                    break
                print("Invalid choice. Please enter 1 or 2.")
            
            # Initialize scheduler
            scheduler = Scheduler(mode='cloud' if mode == '1' else 'fog')
            
            try:
                # Process all tasks concurrently
                tasks = [scheduler.schedule_task(task_data) for task_data in data]
                results = await asyncio.gather(*tasks)
                
                # Log results for each task
                for task_data, result in zip(data, results):
                    node = next((n for n in scheduler.nodes if n.name == result['node_name']), None)
                    if node:
                        logger.log_task(task_data, result, node)
                
            finally:
                logger.close()
            
            print("\nProcessing complete! Check the following log files:")
            print("- task_log.txt: Detailed task processing information")
            print("- cloud_log.txt: Cloud node status updates")
            print("- fog_log.txt: Fog node status updates")
            
    except FileNotFoundError:
        print("Error: Tuple10.json file not found in the current directory.")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 