import json


def read_file(file_path):
    with open(file_path) as file:
        contents = json.load(file)
    return contents


def sort_contents(unsorted):
    sorted_contents = sorted(unsorted, key=lambda d: d["timestamp"])
    return sorted_contents
