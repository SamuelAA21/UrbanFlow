import os
import json
import re
import signal
import subprocess
import time
from pathlib import Path

from .db import connect, initialize, publish
from .settings import APP, MAX_JOB_SECONDS, RUNTIME
from .replay import enqueue_due

APP_ID = re.compile(r"application_\d+_\d+")
PREPARE = ("cargar", "etl", "perfil", "modelo", "sql")


def claim():
    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM jobs WHERE status='queued' ORDER BY id LIMIT 1").fetchone()
        if row:
            conn.execute("UPDATE jobs SET status='running',started_at=CURRENT_TIMESTAMP WHERE id=?", (row["id"],))
        return row


def run_stage(job, name, folder, deadline):
    identity = (job["id"], name)
    with connect() as conn:
        conn.execute("INSERT INTO stages(job_id,name) VALUES(?,?)", identity)
        conn.execute("""UPDATE jobs SET stage=?,stage_started_at=CURRENT_TIMESTAMP,
                     yarn_id=NULL WHERE id=?""", (name, job["id"]))
    logs = folder / "logs"
    logs.mkdir(exist_ok=True)
    env = dict(os.environ, URBANFLOW_RESULTS=str(folder), URBANFLOW_LOGS=str(logs))
    if job['kind'] == 'replay':
        env['URBANFLOW_CUTOFF'] = job['cutoff_day']
        env['URBANFLOW_MODEL_PATH'] = f"/urbanflow/modelos/replay/{job['id']}"
    if job['kind']=='city':
        with connect(readonly=True) as conn:
            snapshot=conn.execute('SELECT * FROM city_runs WHERE job_id=?',(job['id'],)).fetchone()
            previous=conn.execute('SELECT * FROM city_runs WHERE job_id=?',(snapshot['previous_job'],)).fetchone()
            paths=[r[0] for r in conn.execute("SELECT path FROM city_batches WHERE state='archived' AND last_id>? AND first_id<=? ORDER BY first_id",
                                             (previous['high_water'] if previous else 0,snapshot['high_water']))]
        env['URBANFLOW_CITY_JOB']=str(job['id'])
        env['URBANFLOW_CITY_HIGH']=str(snapshot['high_water'])
        env['URBANFLOW_CITY_LOW']=str(previous['high_water'] if previous else 0)
        env['URBANFLOW_CITY_PATHS']=json.dumps(paths)
        if previous:
            env['URBANFLOW_CITY_PREVIOUS']=f"/urbanflow/city/runs/{previous['job_id']}"
    if job["kind"] == "corridors":
        env["URBANFLOW_SQL_MONTH"] = job["months"]
    yarn_id = None
    with (logs / (name + ".out")).open("wb") as output:
        process = subprocess.Popen(
            ["bash", str(APP / "pipeline.sh"), name, job["months"]],
            cwd=APP, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            while True:
                path = logs / (name + ".log")
                if not yarn_id and path.exists():
                    match = APP_ID.search(path.read_text(errors="replace")[:200000])
                    if match:
                        yarn_id = match.group()
                        with connect() as conn:
                            conn.execute("UPDATE stages SET yarn_id=? WHERE job_id=? AND name=?",
                                         (yarn_id, *identity))
                            conn.execute("UPDATE jobs SET yarn_id=? WHERE id=?", (yarn_id, job["id"]))
                code = process.poll()
                if code is not None:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Se agotó el tiempo en {name}")
                time.sleep(1)
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    if code:
        raise RuntimeError(f"{name} terminó con código {code}")
    with connect() as conn:
        conn.execute("UPDATE stages SET status='completed',ended_at=CURRENT_TIMESTAMP WHERE job_id=? AND name=?",
                     identity)


def fail(job_id, error):
    with connect() as conn:
        conn.execute("""UPDATE stages SET status='failed',ended_at=CURRENT_TIMESTAMP
                     WHERE job_id=? AND status='running'""", (job_id,))
        conn.execute("""UPDATE jobs SET status='failed',ended_at=CURRENT_TIMESTAMP,error=?
                     WHERE id=?""", (error, job_id))
        if conn.execute("SELECT 1 FROM city_runs WHERE job_id=?",(job_id,)).fetchone():
            conn.execute("UPDATE city_runs SET status='failed' WHERE job_id=?",(job_id,))
            conn.execute("UPDATE city_sessions SET error=? WHERE id=(SELECT max(id) FROM city_sessions)",(error,))
            conn.execute('UPDATE city_engine SET error=? WHERE id=1',(error,))


def main():
    initialize()
    with connect() as conn:
        interrupted = conn.execute("SELECT id FROM jobs WHERE status='running'").fetchall()
    for job in interrupted:
        fail(job["id"], "El ejecutor se reinició durante el trabajo.")
    while True:
        enqueue_due()
        job = claim()
        if job is None:
            time.sleep(2)
            continue
        try:
            folder = RUNTIME / "runs" / str(job["id"])
            folder.mkdir(parents=True, exist_ok=False)
            deadline = time.monotonic() + MAX_JOB_SECONDS
            stages = PREPARE if job['kind'] in ('prepare','replay','history') else {
                'city_seed':('city_seed',),'city':('city',),'benchmark':('cargar','benchmark_1','benchmark_3')}.get(job['kind'],('sql',))
            for name in stages:
                run_stage(job, name, folder, deadline)
            with connect() as conn:
                publish(conn, job, folder)
                conn.execute("UPDATE jobs SET status='completed',ended_at=CURRENT_TIMESTAMP WHERE id=?", (job["id"],))
        except Exception as error:
            print(f"Trabajo {job['id']}: {error}", flush=True)
            fail(job["id"], str(error) if isinstance(error, (RuntimeError, TimeoutError))
                 else "Falló la ejecución o la validación de resultados. Consulta el registro del ejecutor.")


if __name__ == "__main__":
    main()
