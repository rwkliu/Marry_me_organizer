import json

file_name = "dataset_1.json"
file_path = "./data/" + file_name

with open(file_path) as file:
    contents = json.load(file)

print(contents[0]["id"])
