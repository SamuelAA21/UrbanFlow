import json
from urllib.error import URLError
from urllib.request import urlopen

from .settings import YARN


def read(path):
    with urlopen(YARN + "/ws/v1/cluster/" + path, timeout=2) as response:
        return json.load(response)


def status():
    try:
        nodes = (read("nodes").get("nodes") or {}).get("node", [])
        apps = (read("apps?limit=12").get("apps") or {}).get("app", [])
    except (OSError, URLError, ValueError):
        return {"available": False, "nodes": [], "applications": []}
    return {
        "available": True,
        "nodes": [{key: node.get(key) for key in
                   ("nodeHostName", "state", "numContainers", "usedMemoryMB", "availMemoryMB",
                    "usedVirtualCores", "availableVirtualCores")} for node in nodes],
        "applications": [{key: app.get(key) for key in
                          ("id", "name", "state", "finalStatus", "progress", "elapsedTime")}
                         for app in apps],
    }
