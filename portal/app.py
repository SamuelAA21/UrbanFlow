import json
import os
import re
import sqlite3
from datetime import date

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from . import cluster_status
from .db import connect, rows
from .settings import APP, SOURCE

WEB = SOURCE / "web"
ADMIN = os.environ.get("URBANFLOW_ADMIN") == "1"
MONTH = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])")


def number(request, key, low, high, default):
    try:
        value = int(request.query_params.get(key, default))
        if low <= value <= high:
            return value
    except ValueError:
        pass
    raise HTTPException(422, f"Valor inválido para {key}")


def error_response(request, error):
    return JSONResponse({"error": error.detail}, status_code=error.status_code)


def page(request):
    return FileResponse(WEB / ("admin.html" if ADMIN else "index.html"))


def health(request):
    with connect(readonly=True) as conn:
        conn.execute("SELECT 1 FROM releases LIMIT 1")
    return JSONResponse({"status": "ok"})


def metadata(request):
    with connect(readonly=True) as conn:
        data = rows(conn, "SELECT name,job_id,published_at,details FROM releases")
        dates = conn.execute("SELECT min(day), max(day) FROM predictions").fetchone()
    for release in data:
        release["details"] = json.loads(release["details"])
    return JSONResponse({"releases": {item["name"]: item for item in data},
                         "first_prediction": dates[0], "last_prediction": dates[1]})


def geography(request):
    return FileResponse(APP / "dashboard" / "taxi_zones.geojson", media_type="application/geo+json")


def profile(request):
    weekday = number(request, "weekday", 1, 7, 6)
    hour = number(request, "hour", 0, 23, 18)
    zone = number(request, "zone", 1, 265, 132)
    with connect(readonly=True) as conn:
        ranking = rows(conn, """SELECT zone,round(1.0*trips/days,1) AS average,days
                               FROM profile WHERE weekday=? AND hour=?
                               ORDER BY trips DESC,zone""", (weekday, hour))
        hourly = rows(conn, """SELECT hour,round(1.0*trips/days,1) AS average,days
                              FROM profile WHERE weekday=? AND zone=? ORDER BY hour""", (weekday, zone))
    return JSONResponse({"ranking": ranking, "hourly": hourly})


def predictions(request):
    zone = number(request, "zone", 1, 265, 132)
    try:
        day = date.fromisoformat(request.query_params.get("day", "")).isoformat()
    except ValueError:
        raise HTTPException(422, "Selecciona una fecha válida.")
    with connect(readonly=True) as conn:
        data = rows(conn, """SELECT hour,observed,estimate,baseline FROM predictions
                            WHERE day=? AND zone=? ORDER BY hour""", (day, zone))
    return JSONResponse(data)


def corridors(request):
    with connect(readonly=True) as conn:
        data = rows(conn, "SELECT * FROM corridors ORDER BY trips DESC,origin,destination")
    return JSONResponse(data)


def jobs(request):
    with connect(readonly=True) as conn:
        conn.execute("BEGIN")
        items = rows(conn, "SELECT * FROM jobs ORDER BY id DESC LIMIT 12")
        for job in items:
            job["stages"] = rows(conn, "SELECT * FROM stages WHERE job_id=? ORDER BY rowid", (job["id"],))
    return JSONResponse(items)


def enqueue(body):
    if not isinstance(body, dict):
        raise HTTPException(422, "Solicitud inválida.")
    month = body.get("month")
    if not isinstance(month, str) or not MONTH.fullmatch(month):
        raise HTTPException(422, "Selecciona un mes válido.")
    try:
        with connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            release = conn.execute("SELECT details FROM releases WHERE name='mobility'").fetchone()
            if not release or month not in json.loads(release[0])["etl_resumen"]["meses"]:
                raise HTTPException(422, "El mes debe estar en los datos publicados.")
            job_id = conn.execute("INSERT INTO jobs(kind,months,status) VALUES('corridors',?,'queued')",
                                  (month,)).lastrowid
    except sqlite3.IntegrityError:
        raise HTTPException(409, "Ya hay un trabajo pendiente o en ejecución.")
    return JSONResponse({"id": job_id, "status": "queued"}, status_code=202)


async def create_job(request):
    if request.headers.get("content-type", "").split(";")[0] != "application/json":
        raise HTTPException(415, "Envía JSON.")
    data = bytearray()
    async for chunk in request.stream():
        data.extend(chunk)
        if len(data) > 1024:
            raise HTTPException(413, "Solicitud demasiado grande.")
    try:
        body = json.loads(data)
    except (ValueError, UnicodeError):
        raise HTTPException(422, "JSON inválido.")
    return await run_in_threadpool(enqueue, body)


def cluster(request):
    return JSONResponse(cluster_status.status())


routes = [Route("/", page), Route("/health", health), Route("/api/metadata", metadata),
          Mount("/assets", StaticFiles(directory=WEB / "assets"))]
if ADMIN:
    routes += [Route("/api/jobs", jobs), Route("/api/jobs", create_job, methods=["POST"]),
               Route("/api/cluster", cluster)]
else:
    routes += [Route("/api/profile", profile), Route("/api/predictions", predictions),
               Route("/api/corridors", corridors), Route("/api/geography", geography)]

app = Starlette(routes=routes, exception_handlers={HTTPException: error_response})
if ADMIN:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1"])
