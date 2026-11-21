import os
from pathlib import Path

SOURCE = Path(__file__).resolve().parent
APP = Path(os.environ.get("URBANFLOW_HOME", SOURCE.parent))
RUNTIME = Path(os.environ.get("URBANFLOW_RUNTIME", APP / "runtime"))
DB = RUNTIME / "urbanflow.sqlite3"
YARN = os.environ.get("URBANFLOW_YARN", "http://127.0.0.1:8088")
MAX_JOB_SECONDS = 2700
