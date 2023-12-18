import os
import json
import csv

def find_files_with_extension(directory, target_extension, results=None):
    if results is None:
        results = []

    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith(target_extension):
                results.append(os.path.join(root, filename))

    return results

def flatten_json(obj, prefix=""):
    flat_dict = {}
    for key, value in obj.items():
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, f"{prefix}{key}_"))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                flat_dict.update(flatten_json(item, f"{prefix}{key}_{i}_"))
        else:
            flat_dict[f"{prefix}{key}"] = value
    return flat_dict

def convert_and_save_to_csv(json_file, result_directory):
    try:
        with open(json_file, 'r') as json_file:
            json_data = json.load(json_file)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error loading JSON file {json_file}: {e}")
        return

    modified_json = flatten_json(json_data)

    base_filename = os.path.splitext(os.path.basename(json_file))[0]
    csv_filename = os.path.join(result_directory, f"{base_filename}.csv")

    counter = 1
    while os.path.exists(csv_filename):
        csv_filename = os.path.join(result_directory, f"{base_filename}({counter}).csv")
        counter += 1

    with open(csv_filename, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(modified_json.keys())
        csv_writer.writerow(modified_json.values())

def main():
    directory_to_search = "data"
    target_extension = ".json"
    result_directory = "result"

    if not os.path.exists(result_directory):
        os.makedirs(result_directory)

    found_files = find_files_with_extension(directory_to_search, target_extension)

    for file in found_files:
        convert_and_save_to_csv(file, result_directory)

if __name__ == "__main__":
    main()
