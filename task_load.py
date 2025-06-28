import json
from logger import setup_logger
import os

# Global variable to store tasks
task_list = []
input_file = 'Tuple50K_modified.json'  # Default input file

def set_input_file(filename):
    global input_file
    input_file = filename
    print(f"Input file set to: {input_file}")

def read_and_log_tuples():
    global task_list, input_file
    # Setup logger
    logger = setup_logger('task_load', 'task_load.log', sub_directory='tasks')
    
    try:
        # Read the JSON file and store in data structure
        import os
        file_size_mb = os.path.getsize(input_file) / (1024 * 1024)
        print(f"Loading tasks from {input_file} (File size: {file_size_mb:.2f} MB)...")
        
        with open(input_file, 'r', encoding='utf-8-sig') as file:
            task_list = json.load(file)
            
        task_count = len(task_list)
        print(f"Successfully loaded {task_count} tasks")
        
        # Estimate memory usage for large datasets
        estimated_memory_mb = task_count * 2  # Rough estimate: 2KB per task
        if estimated_memory_mb > 100:  # If estimated memory usage is over 100MB
            print(f"Warning: Large dataset detected ({estimated_memory_mb:.2f} MB estimated memory usage)")
            print("Performance might be affected. Processing may take longer.")
            
        # Log the number of tuples
        logger.info(f"Total number of tasks in data structure: {task_count} (File size: {file_size_mb:.2f} MB)")
        
        # Only log detailed information for a reasonable number of tasks to avoid massive logs
        max_detailed_logs = 1000
        detailed_log_count = min(task_count, max_detailed_logs)
        
        # Process each task from the data structure
        for i, task in enumerate(task_list):
            # Use original values directly
            task['Storage'] = task.get('Size', 0)
            task['MIPS'] = task.get('MIPS', 0)
            task['RAM'] = task.get('RAM', 0)
            task['BW'] = task.get('BW', 0)
            
            # Add a unique ID to each task for better tracking
            if 'ID' not in task:
                import uuid
                task['ID'] = str(uuid.uuid4())
            
            # Log detailed information only for the first max_detailed_logs tasks
            if i < detailed_log_count:
                logger.debug(
                    f"Task Loaded: "
                    f"Name={task.get('Name')}, "
                    f"ID={task.get('ID')}, "
                    f"Size={task.get('Size')}, "
                    f"Storage={task.get('Storage')}, "
                    f"MIPS={task.get('MIPS')}, "
                    f"RAM={task.get('RAM')}, "
                    f"BW={task.get('BW')}, "
                    f"CreationTime={task.get('CreationTime')}"
                )
        
        if task_count > max_detailed_logs:
            logger.info(f"Detailed logging limited to first {max_detailed_logs} tasks to maintain performance")
            
        return task_list
            
    except FileNotFoundError:
        print(f"Error: {input_file} file not found")
        logger.error(f"{input_file} file not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {input_file}")
        logger.error(f"Error decoding JSON file: {input_file}")
        return []
    except Exception as e:
        print(f"Error: {str(e)}")
        logger.error(f"An error occurred: {str(e)}")
        return []

if __name__ == "__main__":
    read_and_log_tuples()
