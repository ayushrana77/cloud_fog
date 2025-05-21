import json
import traceback
import asyncio
import time
from config import TASK_RATE_PER_SECOND
from scheduler import Scheduler

async def process_task(scheduler, task_data):
    """Process a single task asynchronously."""
    try:
        return await scheduler.schedule_task(task_data)
    except Exception as e:
        print(f"Error processing task: {e}")
        return None

def print_resource_status(scheduler):
    """Print available resources for each server."""
    print(f"\n{scheduler.mode.upper()} Resource Status:")
    print("-" * 50)
    
    for node in scheduler.nodes:
        # Get node stats
        stats = scheduler.get_stats()[node.node_id]
        
        # Calculate utilization percentages (ensure they're between 0 and 1)
        mips_util = min(max(stats['mips_utilization'], 0), 1)
        bw_util = min(max(stats['bandwidth_utilization'], 0), 1)
        mem_util = min(max(stats['memory_utilization'], 0), 1)
        
        # Calculate free resources based on active tasks
        active_tasks = stats['current_tasks']
        if active_tasks == 0:
            # If no active tasks, all resources are free
            free_mips = node.mips
            free_bandwidth = node.bandwidth
            free_memory = node.memory
        else:
            # Calculate free resources based on utilization
            free_mips = node.mips * (1 - mips_util)
            free_bandwidth = node.bandwidth * (1 - bw_util)
            free_memory = node.memory * (1 - mem_util)
        
        print(f"\n{node.name}:")
        print(f"  Free MIPS: {free_mips:.0f} / {node.mips} ({free_mips/node.mips*100:.1f}% free)")
        print(f"  Free Bandwidth: {free_bandwidth:.0f} Mbps / {node.bandwidth} Mbps ({free_bandwidth/node.bandwidth*100:.1f}% free)")
        print(f"  Free Memory: {free_memory:.0f} MB / {node.memory} MB ({free_memory/node.memory*100:.1f}% free)")
        print(f"  Queue Size: {stats['queue_size']} tasks")
        print(f"  Tasks Processed: {stats['total_processed']}")
        print(f"  Current Utilization:")
        print(f"    MIPS: {mips_util*100:.1f}%")
        print(f"    Bandwidth: {bw_util*100:.1f}%")
        print(f"    Memory: {mem_util*100:.1f}%")
        print(f"    Active Tasks: {stats['current_tasks']}")

async def main():
try:
    # Read the JSON file with utf-8-sig encoding to handle BOM
    with open('Tuple100k.json', 'r', encoding='utf-8-sig') as file:
        try:
            # Load the JSON data
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
                
                # Initialize scheduler with selected mode
                scheduler = Scheduler(mode='cloud' if mode == '1' else 'fog')
            
                # Process tasks in smaller chunks with rate limiting
                CHUNK_SIZE = 10  # Reduced chunk size
                total_chunks = (len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE
            
                start_time = time.time()
                tasks_processed = 0
            
                for chunk_idx in range(total_chunks):
                    start_idx = chunk_idx * CHUNK_SIZE
                    end_idx = min((chunk_idx + 1) * CHUNK_SIZE, len(data))
                    chunk = data[start_idx:end_idx]
                
                    # Create tasks for this chunk
                    tasks = [process_task(scheduler, task_data) for task_data in chunk]
                    
                    # Process chunk and wait for completion
                    await asyncio.gather(*tasks)
                    tasks_processed += len(chunk)
                    
                    # Calculate current processing rate
                    elapsed = time.time() - start_time
                    current_rate = tasks_processed / elapsed if elapsed > 0 else 0
                    
                    # If we're processing too fast, add a small delay
                    if current_rate > TASK_RATE_PER_SECOND:
                        await asyncio.sleep(0.1)  # Small delay to prevent queue buildup
                    
                    # Print progress every 1000 tasks
                    if tasks_processed % 1000 == 0:
                        print(f"\nProcessed {tasks_processed}/{len(data)} tasks (Rate: {current_rate:.1f} tasks/sec)")
                        # Print resource status
                        print_resource_status(scheduler)
                
            # Print final statistics
                print(f"\n{scheduler.mode.upper()} Processing Complete!")
            print("=" * 50)
                print(f"Final {scheduler.mode.upper()} Statistics:")
            print("=" * 50)
                print_resource_status(scheduler)
                
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            print("The file might not be in valid JSON format.")
            traceback.print_exc()
            
except FileNotFoundError:
    print("Error: Tuple100k.json file not found in the current directory.")
except Exception as e:
    print(f"An error occurred: {e}")
    traceback.print_exc() 

if __name__ == "__main__":
    asyncio.run(main()) 