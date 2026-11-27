import json
import importlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from portal import db, laboratory
from portal import app as application
from starlette.requests import Request
from starlette.routing import Match


class LaboratoryTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        patcher = patch.object(db, 'DB', Path(self.folder.name) / 'lab.sqlite3')
        patcher.start()
        self.addCleanup(patcher.stop)
        db.initialize()
        with db.connect() as conn:
            for day in range(1, 11):
                for hour in (8, 18):
                    conn.execute('INSERT INTO predictions VALUES(?,?,?, ?,999999,999999)',
                                 (f'2025-06-{day:02}', 132, hour, 10 + day + hour))

    def test_chronological_split_and_synthetic_isolation(self):
        before = laboratory.train()
        self.assertEqual(before['training_until'], '2025-06-07')
        self.assertEqual(before['test_from'], '2025-06-08')
        for seed in range(5):
            item = laboratory.generate({'seed': seed})
            self.assertEqual(item['source'], 'synthetic')
            self.assertIsNone(item['observed'])
            self.assertGreater(item['day'], '2025-06-10')
        after = laboratory.train()
        for key in ('source_sha256', 'static', 'ml', 'training_rows', 'test_rows'):
            self.assertEqual(before[key], after[key])
        self.assertEqual(laboratory.status()['persisted_requests'], 5)
        # Altering genuine test labels changes metrics, never learned parameters.
        with db.connect() as conn:
            original = json.loads(conn.execute('SELECT artifact FROM lab_models LIMIT 1').fetchone()[0])
            conn.execute("UPDATE predictions SET observed=10000 WHERE day>'2025-06-07'")
        changed = laboratory.train()
        with db.connect() as conn:
            current = json.loads(conn.execute('SELECT artifact FROM lab_models ORDER BY id DESC LIMIT 1').fetchone()[0])
        self.assertEqual(original['means'], current['means'])
        self.assertEqual(original['weights'], current['weights'])
        self.assertNotEqual(before['ml']['mae'], changed['ml']['mae'])

    def test_idempotence_validation_and_no_false_truth(self):
        with self.assertRaises(laboratory.LabError):
            laboratory.generate({})
        laboratory.train()
        first = laboratory.generate({'request_key': 'same', 'seed': 7})
        again = laboratory.generate({'request_key': 'same', 'seed': 7})
        self.assertEqual(first, again)
        self.assertEqual(laboratory.status()['persisted_requests'], 1)
        for bad in ({'observed': 42}, {'source': 'real'}, {'seed': True}, {'seed': -1}, []):
            with self.assertRaises(laboratory.LabError):
                laboratory.generate(bad)
        with db.connect() as conn:
            with self.assertRaises(db.sqlite3.IntegrityError):
                conn.execute('UPDATE lab_requests SET observed=42')

    def test_lab_routes_are_private(self):
        for admin in (False, True):
            with patch.dict(os.environ, {'URBANFLOW_ADMIN': '1' if admin else '0'}):
                importlib.reload(application)
            for path, method in (('/api/lab', 'GET'), ('/api/lab/train', 'POST'),
                                 ('/api/lab/requests', 'POST'), ('/laboratory', 'GET')):
                scope = Request({'type': 'http', 'method': method, 'path': path,
                                 'query_string': b'', 'headers': []}).scope
                matches = [r for r in application.app.routes if r.matches(scope)[0] is Match.FULL]
                self.assertEqual(len(matches), int(admin))


if __name__ == '__main__':
    unittest.main()
