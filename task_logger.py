import json
import asyncio
import time
from datetime import datetime
from config import TASK_RATE_PER_SECOND
from scheduler import Scheduler

class TaskLogger:
    def __init__(self):
        self.start_time = time.time()
    
    def log_task(self, task_data, result, node):
        """Log task processing details."""
        pass
    
    def close(self):
        """Close logger."""
        pass

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
            
            print("\nProcessing complete!")
            
    except FileNotFoundError:
        print("Error: Tuple10.json file not found in the current directory.")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 