import json
import random

# Define coordinates for Indian cities and nearby locations
indian_locations = {
    "Delhi": {"latitude": 28.6139, "longitude": 77.2090},
    "Bangalore": {"latitude": 12.9716, "longitude": 77.5946},
}

# Function to add some randomness to coordinates (to avoid all entries having exactly the same coordinates)
def add_location_variance(base_latitude, base_longitude, variance=0.05):
    latitude_variance = random.uniform(-variance, variance)
    longitude_variance = random.uniform(-variance, variance)
    return base_latitude + latitude_variance, base_longitude + longitude_variance

def change_geolocations(input_file_path, output_file_path=None):
    """
    Change GeoLocation values in a JSON file to Indian locations
    
    Args:
        input_file_path: Path to the input JSON file
        output_file_path: Path to save the modified JSON. If None, overwrite the input file
    """
    if output_file_path is None:
        output_file_path = input_file_path
    
    try:
        # Load the JSON data
        with open(input_file_path, 'r') as file:
            data = json.load(file)
        
        # Check if it's an array or object with nested arrays
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and any(isinstance(data.get(key), list) for key in data):
            # Find the key that contains the array
            for key in data:
                if isinstance(data[key], list):
                    items = data[key]
                    break
        else:
            print("Unable to identify the data structure. Please check the JSON format.")
            return
        
        # Count of modified records
        modified_count = 0
        
        # Process each item
        for item in items:
            if isinstance(item, dict) and "GeoLocation" in item:
                # Choose a random Indian location
                location_name = random.choice(list(indian_locations.keys()))
                location = indian_locations[location_name]
                
                # Add some variance to make coordinates slightly different
                latitude, longitude = add_location_variance(location["latitude"], location["longitude"])
                
                # Update the GeoLocation
                item["GeoLocation"] = {"latitude": latitude, "longitude": longitude}
                
                # If there's a City or Location field, update it too
                if "City" in item:
                    item["City"] = location_name
                if "Location" in item:
                    item["Location"] = location_name
                    
                modified_count += 1
        
        # Save the modified data
        with open(output_file_path, 'w') as file:
            json.dump(data, file, indent=2)
            
        print(f"Successfully modified {modified_count} GeoLocation entries.")
        print(f"Modified data saved to {output_file_path}")
            
    except json.JSONDecodeError:
        print("Error: The file is not a valid JSON.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    import sys
    
    # Get file path from command line argument or use default
    input_file = "f:\\sem3\\offload_project\\Tuple50K.json"
    output_file = "f:\\sem3\\offload_project\\Tuple50K_modified.json"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    print(f"Changing GeoLocation values in {input_file}")
    change_geolocations(input_file, output_file)
