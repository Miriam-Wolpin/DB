import json


def write_to_json(path, data):
    with open(path, "w") as the_file:
        json.dump(data, the_file)

def add_to_json(path,data):
    the_file = open(path)
    json_data = json.load(the_file)
    with open(path, "w") as the_file:
        json_data.update(data)
        json.dump(json_data, the_file)

def read_from_json(path):
    the_file = open(path)
    data = json.load(the_file)
    the_file.close()
    return data