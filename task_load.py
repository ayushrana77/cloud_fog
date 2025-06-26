import json
from logger import setup_logger
import os

# Global variable to store tasks
task_list = []
input_file = 'Tuple10.json'  # Default input file

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
        print(f"Loading tasks from {input_file}...")
        with open(input_file, 'r', encoding='utf-8-sig') as file:
            task_list = json.load(file)
            print(f"Successfully loaded {len(task_list)} tasks")
            
        # Log the number of tuples
        logger.info(f"Total number of tasks in data structure: {len(task_list)}")
        
        # Process each task from the data structure
        for task in task_list:
            # Use original values directly
            task['Storage'] = task.get('Size', 0)
            task['MIPS'] = task.get('MIPS', 0)
            task['RAM'] = task.get('RAM', 0)
            task['BW'] = task.get('BW', 0)
            
            # Log all task details
            logger.debug(
                f"Task Loaded: "
                f"Name={task.get('Name')}, "
                f"Size={task.get('Size')}GB, "
                f"Storage={task.get('Storage')}GB, "
                f"MIPS={task.get('MIPS')}, "
                f"RAM={task.get('RAM')}GB, "
                f"BW={task.get('BW')}Mbps, "
                f"CreationTime={task.get('CreationTime')}"
            )
            
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
