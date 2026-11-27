import csv
import json
import sqlite3
from contextlib import contextmanager

from .settings import DB

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
 id INTEGER PRIMARY KEY, kind TEXT NOT NULL, months TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('queued','running','completed','failed')),
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, started_at TEXT, ended_at TEXT,
 stage TEXT, stage_started_at TEXT, yarn_id TEXT, error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_job ON jobs ((1))
 WHERE status IN ('queued','running');
CREATE TABLE IF NOT EXISTS stages (
 job_id INTEGER NOT NULL REFERENCES jobs(id), name TEXT NOT NULL,
 started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, ended_at TEXT,
 status TEXT NOT NULL DEFAULT 'running', yarn_id TEXT, PRIMARY KEY(job_id,name)
);
CREATE TABLE IF NOT EXISTS releases (
 name TEXT PRIMARY KEY, job_id INTEGER NOT NULL REFERENCES jobs(id),
 published_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS profile (
 zone INTEGER NOT NULL, weekday INTEGER NOT NULL, hour INTEGER NOT NULL,
 trips INTEGER NOT NULL, days INTEGER NOT NULL, PRIMARY KEY(weekday,hour,zone)
);
CREATE TABLE IF NOT EXISTS predictions (
 day TEXT NOT NULL, zone INTEGER NOT NULL, hour INTEGER NOT NULL,
 observed REAL, estimate REAL NOT NULL, baseline REAL,
 PRIMARY KEY(day,zone,hour)
);
CREATE TABLE IF NOT EXISTS corridors (
 origin TEXT NOT NULL, destination TEXT NOT NULL, trips INTEGER NOT NULL,
 minutes REAL, PRIMARY KEY(origin,destination)
);
CREATE TABLE IF NOT EXISTS lab_models (
 id INTEGER PRIMARY KEY, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 artifact TEXT NOT NULL, metrics TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lab_requests (
 id INTEGER PRIMARY KEY, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 request_key TEXT NOT NULL UNIQUE, model_id INTEGER NOT NULL REFERENCES lab_models(id),
 source TEXT NOT NULL CHECK(source='synthetic'), seed INTEGER NOT NULL,
 day TEXT NOT NULL, hour INTEGER NOT NULL CHECK(hour BETWEEN 0 AND 23),
 zone INTEGER NOT NULL, weekday INTEGER NOT NULL, weekend INTEGER NOT NULL,
 period TEXT NOT NULL, static_estimate REAL NOT NULL, ml_estimate REAL NOT NULL,
 simulated_count INTEGER NOT NULL CHECK(simulated_count>=0),
 observed REAL CHECK(observed IS NULL)
);
"""


@contextmanager
def connect(readonly=False):
    target = DB.as_uri() + "?mode=ro" if readonly else str(DB)
    conn = sqlite3.connect(target, uri=readonly, timeout=8)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        with conn:
            yield conn
    finally:
        conn.close()


def initialize():
    DB.parent.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.executescript(SCHEMA)


def rows(conn, sql, params=()):
    return [dict(row) for row in conn.execute(sql, params)]


def replace_csv(conn, table, sql, path, convert):
    with path.open(encoding="utf-8", newline="") as source:
        records = csv.DictReader(source)
        first = next(records, None)
        if first is None:
            raise ValueError(f"Spark no produjo filas en {path.name}")
        conn.execute(f"DELETE FROM {table}")
        conn.execute(sql, convert(first))
        conn.executemany(sql, (convert(row) for row in records))


def publish(conn, job, folder):
    if job["kind"] == "prepare":
        replace_csv(conn, "profile", "INSERT INTO profile VALUES (?,?,?,?,?)", folder / "perfil.csv",
                    lambda r: (r["zona_origen"], r["dia_semana"], r["hora"], r["viajes"], r["dias"]))
        replace_csv(conn, "predictions", "INSERT INTO predictions VALUES (?,?,?,?,?,?)",
                    folder / "predicciones_prueba.csv",
                    lambda r: (r["fecha"], r["zona_origen"], r["hora"], r["viajes"],
                               r["pred_modelo"], r["pred_perfil"]))
        with (folder / "prediccion_manana.csv").open(encoding="utf-8", newline="") as source:
            conn.executemany("INSERT INTO predictions VALUES (?,?,?,NULL,?,NULL)",
                             ((r["fecha"], r["zona_origen"], r["hora"], r["viajes_esperados"])
                              for r in csv.DictReader(source)))
        metadata = {name: json.loads((folder / (name + ".json")).read_text(encoding="utf-8"))
                    for name in ("etl_resumen", "metricas_modelo")}
        size_file = folder / "bytes_crudos.txt"
        if size_file.exists():
            input_bytes = int(size_file.read_text(encoding="utf-8").strip())
            metadata["etl_resumen"]["bytes_crudos"] = input_bytes
            metadata["etl_resumen"]["bytes_hdfs_replicados"] = input_bytes * 3
        release(conn, "mobility", job["id"], metadata)
    replace_csv(conn, "corridors", "INSERT INTO corridors VALUES (?,?,?,?)",
                folder / "sql_corredores.csv",
                lambda r: (r["origen"], r["destino"], r["viajes"], r["duracion_media_min"]))
    release(conn, "corridors", job["id"], {"months": job["months"].split(",")})


def release(conn, name, job_id, details):
    conn.execute("""INSERT INTO releases(name,job_id,details) VALUES(?,?,?)
                 ON CONFLICT(name) DO UPDATE SET job_id=excluded.job_id,
                 details=excluded.details,published_at=CURRENT_TIMESTAMP""",
                 (name, job_id, json.dumps(details)))
