import json
from logger import setup_logger, log_tuple_details

# Global variable to store tasks
task_list = []

def read_and_log_tuples():
    global task_list
    # Setup logger
    logger = setup_logger()
    
    try:
        # Read the JSON file and store in data structure
        with open('Tuple50K.json', 'r', encoding='utf-8-sig') as file:
            task_list = json.load(file)
            print(f"Successfully loaded {len(task_list)} tasks")
            
        # Log the number of tuples
        logger.info(f"Total number of tasks in data structure: {len(task_list)}")
        
        # Process each task from the data structure
        for task in task_list:
            log_tuple_details(logger, task)
            
        return task_list
            
    except FileNotFoundError:
        print("Error: Tuple10.json file not found")
        logger.error("Tuple10.json file not found")
        return []
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in Tuple10.json")
        logger.error("Error decoding JSON file")
        return []
    except Exception as e:
        print(f"Error: {str(e)}")
        logger.error(f"An error occurred: {str(e)}")
        return []

if __name__ == "__main__":
    read_and_log_tuples()
