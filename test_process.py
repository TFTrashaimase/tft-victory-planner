from test import *

def analyze_json_structure(json_obj, indent=0):
    """
    Analyze and print the nested structure of a JSON-like dictionary.

    :param json_obj: The JSON-like object (dictionary or list).
    :param indent: Current indentation level (used for recursion).
    """
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            print(" " * indent + f"Key: {key}")
            analyze_json_structure(value, indent + 2)
    elif isinstance(json_obj, list):
        print(" " * indent + "List:")
        if len(json_obj) > 0:
            # Analyze the structure of the first element to infer list structure
            analyze_json_structure(json_obj[0], indent + 2)
        else:
            print(" " * (indent + 2) + "(Empty List)")
    else:
        print(" " * indent + f"Value: {type(json_obj).__name__}")

# print(analyze_json_structure(json_dict))

print(len(json_dict))
print(len(json_dict['data']))
