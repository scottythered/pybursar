import json
import os


def auth_puller(auth_file, object):
    path = os.path.dirname(os.path.realpath(__file__))
    auth_path = os.path.join(path, auth_file)
    with open(auth_path, "r") as j:
        json_payload = json.load(j)

    return json_payload[object]
