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
        apps = (read("apps").get("apps") or {}).get("app", [])
        # RM does not guarantee that its limited response contains the latest apps.
        # Keep active work visible even after many completed applications accumulate.
        active_states={'NEW','NEW_SAVING','SUBMITTED','ACCEPTED','RUNNING'}
        apps=sorted(apps,key=lambda app:(app.get('state') in active_states,
                    app.get('startedTime',0),app.get('id','')),reverse=True)[:12]
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
