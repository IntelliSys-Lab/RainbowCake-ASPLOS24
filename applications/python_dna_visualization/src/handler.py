import os
import sys
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from squiggle import transform
import requests

filename1 = "gene1.txt"
filename2 = "gene2.txt"
local_path = "./"

def visualize(data1, data2):
    return transform(data1) + transform(data2)

def handler(event, context=None):
    # Visualize sequences
    data1 = open(local_path + filename1, "r").read()
    data2 = open(local_path + filename2, "r").read()
    result = visualize(data1, data2)

    # Write result
    with open(local_path + "result.txt", "wb") as out:
        out.write(json.dumps(result).encode("utf-8"))

    return {
        "result": "Visualize {}+{} finished!".format(filename1, filename2)
    }

if __name__ == "__main__":
    print(handler(None))
    