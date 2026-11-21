import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.routing import Match

from portal import app as application, db, worker


def request(path="/", query="", method="GET"):
    return Request({"type": "http", "method": method, "path": path,
                    "query_string": query.encode(), "headers": []})


def body(response):
    return json.loads(response.body)


class PortalTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        patcher = patch.object(db, "DB", Path(self.folder.name) / "test.sqlite3")
        patcher.start()
        self.addCleanup(patcher.stop)
        db.initialize()
        with db.connect() as conn:
            conn.execute("INSERT INTO jobs(id,kind,months,status) VALUES(1,'prepare','2025-06','completed')")
            db.release(conn, "mobility", 1, {"etl_resumen": {"meses": ["2025-06"]}})
            conn.execute("INSERT INTO profile VALUES(132,6,18,25,2)")
            conn.execute("INSERT INTO predictions VALUES('2025-06-17',132,18,10,9.5,8)")
            conn.execute("INSERT INTO corridors VALUES('JFK','Queens',23,12)")

    def routes(self, admin=False):
        with patch.dict(os.environ, {"URBANFLOW_ADMIN": "1" if admin else "0"}):
            importlib.reload(application)
        scope = request("/api/jobs", method="POST").scope
        return [route for route in application.app.routes if route.matches(scope)[0] is Match.FULL]

    def test_public_queries_database_and_cannot_launch_jobs(self):
        self.assertEqual(self.routes(), [])
        self.assertEqual(application.health(request()).status_code, 200)
        self.assertEqual(body(application.profile(request()))["ranking"][0]["average"], 12.5)
        self.assertEqual(body(application.profile(request(query="hour=3")))["ranking"], [])
        with self.assertRaises(HTTPException) as error:
            application.profile(request(query="hour=24"))
        self.assertEqual(error.exception.status_code, 422)
        self.assertEqual(body(application.predictions(request(query="day=2025-06-17")))[0]["estimate"], 9.5)

    def test_admin_validates_job_and_prevents_concurrent_commands(self):
        self.assertEqual(len(self.routes(admin=True)), 1)
        with self.assertRaises(HTTPException) as error:
            application.enqueue({"month": "2025-05"})
        self.assertEqual(error.exception.status_code, 422)
        self.assertEqual(application.enqueue({"month": "2025-06"}).status_code, 202)
        with self.assertRaises(HTTPException) as error:
            application.enqueue({"month": "2025-06"})
        self.assertEqual(error.exception.status_code, 409)
        self.assertEqual(worker.claim()["months"], "2025-06")
        self.assertIsNone(worker.claim())
        self.assertEqual(body(application.jobs(request()))[0]["status"], "running")

    def test_corrupt_output_preserves_published_snapshot(self):
        folder = Path(self.folder.name)
        with db.connect() as conn:
            db.release(conn, "corridors", 1, {"months": ["2025-06"]})
        (folder / "sql_corredores.csv").write_text(
            "origen,destino,viajes,duracion_media_min\nA,B,23,5\nA,B,2,5\n", encoding="utf-8")
        with self.assertRaises(db.sqlite3.IntegrityError):
            with db.connect() as conn:
                db.publish(conn, {"id": 1, "kind": "corridors", "months": "2025-06"}, folder)
        self.assertEqual(body(application.corridors(request()))[0]["origin"], "JFK")

    def test_successful_publication_changes_visible_results(self):
        folder = Path(self.folder.name)
        (folder / "sql_corredores.csv").write_text(
            "origen,destino,viajes,duracion_media_min\nNueva zona,Queens,42,7\n", encoding="utf-8")
        with db.connect() as conn:
            db.publish(conn, {"id": 1, "kind": "corridors", "months": "2025-06"}, folder)
        self.assertEqual(body(application.metadata(request()))["releases"]["corridors"]["job_id"], 1)
        self.assertEqual(body(application.corridors(request()))[0]["trips"], 42)

    def test_prepare_publication_includes_hdfs_input_size(self):
        folder = Path(self.folder.name)
        (folder / "perfil.csv").write_text(
            "zona_origen,dia_semana,hora,viajes,dias\n132,6,18,25,2\n", encoding="utf-8")
        (folder / "predicciones_prueba.csv").write_text(
            "fecha,zona_origen,hora,viajes,pred_modelo,pred_perfil\n2025-06-17,132,18,10,9.5,8\n", encoding="utf-8")
        (folder / "prediccion_manana.csv").write_text(
            "fecha,zona_origen,hora,viajes_esperados\n2025-07-01,132,18,11\n", encoding="utf-8")
        (folder / "sql_corredores.csv").write_text(
            "origen,destino,viajes,duracion_media_min\nJFK,Queens,23,12\n", encoding="utf-8")
        (folder / "etl_resumen.json").write_text('{"meses":["2025-06"]}', encoding="utf-8")
        (folder / "metricas_modelo.json").write_text('{"mejor_modelo":"Random Forest"}', encoding="utf-8")
        (folder / "bytes_crudos.txt").write_text("73542954\n", encoding="utf-8")
        with db.connect() as conn:
            db.publish(conn, {"id": 1, "kind": "prepare", "months": "2025-06"}, folder)
            details = json.loads(conn.execute("SELECT details FROM releases WHERE name='mobility'").fetchone()[0])
        self.assertEqual(details["etl_resumen"]["bytes_crudos"], 73542954)
        self.assertEqual(details["etl_resumen"]["bytes_hdfs_replicados"], 220628862)


if __name__ == "__main__":
    unittest.main()
