import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from portal import db, replay, worker


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        patched = patch.object(db, 'DB', self.folder / 'test.sqlite3')
        patched.start()
        self.addCleanup(patched.stop)
        env = patch.dict(os.environ, {'URBANFLOW_REPLAY_ENGINE': '1'})
        env.start()
        self.addCleanup(env.stop)
        db.initialize()
        self.configuration = dict(action='start', months='2025-06', start_day='2025-06-28',
                                  end_day='2025-06-30', interval_minutes=2)

    def test_scheduler_runs_once_and_advances_only_after_completion(self):
        replay.configure(self.configuration)
        replay.enqueue_due()
        replay.enqueue_due()
        job = worker.claim()
        self.assertEqual(job['kind'], 'replay')
        self.assertEqual(job['cutoff_day'], '2025-06-28')
        replay.enqueue_due()
        self.assertIsNone(worker.claim())
        with db.connect() as conn:
            conn.execute("UPDATE jobs SET status='completed' WHERE id=?", (job['id'],))
        replay.enqueue_due()
        schedule = replay.status()['schedule']
        self.assertEqual(schedule['next_day'], '2025-06-29')
        self.assertIsNone(schedule['last_job_id'])
        replay.enqueue_due()
        self.assertIsNone(worker.claim())
        replay.configure({'action': 'run_now'})
        replay.enqueue_due()
        self.assertEqual(worker.claim()['cutoff_day'], '2025-06-29')

    def test_failures_disable_scheduler_and_stop_does_not_kill_active_job(self):
        replay.configure(self.configuration)
        replay.enqueue_due()
        job = worker.claim()
        worker.fail(job['id'], 'Spark failed')
        replay.enqueue_due()
        self.assertEqual(replay.status()['schedule']['enabled'], 0)
        replay.configure(self.configuration)
        replay.enqueue_due()
        active = worker.claim()
        replay.configure({'action': 'stop'})
        with db.connect() as conn:
            self.assertEqual(conn.execute('SELECT status FROM jobs WHERE id=?', (active['id'],)).fetchone()[0], 'running')
        self.assertEqual(replay.status()['schedule']['enabled'], 0)

    def test_replay_finishes_at_end_and_rejects_invalid_configuration(self):
        replay.configure({**self.configuration, 'end_day': '2025-06-28'})
        replay.enqueue_due()
        job = worker.claim()
        with db.connect() as conn:
            conn.execute("UPDATE jobs SET status='completed' WHERE id=?", (job['id'],))
        replay.enqueue_due()
        self.assertEqual(replay.status()['schedule']['enabled'], 0)
        for change in ({'start_day': '2025-06-01'}, {'months': '2025-06,2025-08'}, {'interval_minutes': 0},
                       {'months': None}, {'start_day': None}):
            with self.assertRaises(ValueError):
                replay.configure({**self.configuration, **change})
        with patch.dict(os.environ, {'URBANFLOW_REPLAY_ENGINE': '0'}):
            with self.assertRaises(ValueError):
                replay.configure(self.configuration)

    def write_outputs(self, cutoff, future):
        files = {
            'perfil.csv': 'zona_origen,dia_semana,hora,viajes,dias\n132,6,18,25,2\n',
            'predicciones_prueba.csv': 'fecha,zona_origen,hora,viajes,pred_modelo,pred_perfil\n2025-06-17,132,18,10,9,8\n',
            'prediccion_manana.csv': f'fecha,zona_origen,hora,viajes_esperados,pred_perfil\n{future},132,18,11,9\n',
            'observaciones_lote.csv': f'fecha,zona_origen,hora,viajes\n{cutoff},132,18,13\n',
            'sql_corredores.csv': 'origen,destino,viajes,duracion_media_min\nJFK,Queens,23,12\n',
            'etl_resumen.json': json.dumps({'meses': ['2025-06'], 'cutoff_day': cutoff}),
            'metricas_modelo.json': json.dumps({'prueba_hasta': cutoff, 'deployment_training_until': cutoff}),
        }
        for name, value in files.items():
            (self.folder / name).write_text(value, encoding='utf-8')

    def publish(self, cutoff, future):
        with db.connect() as conn:
            job_id = conn.execute("INSERT INTO jobs(kind,months,status,cutoff_day) VALUES('replay','2025-06','completed',?)", (cutoff,)).lastrowid
        self.write_outputs(cutoff, future)
        with db.connect() as conn:
            job = conn.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
            db.publish(conn, job, self.folder)
        return job_id

    def test_forecasts_are_evaluated_only_when_real_next_batch_arrives(self):
        first = self.publish('2025-06-28', '2025-06-29')
        self.assertEqual(replay.status()['evaluation'], [])
        self.publish('2025-06-29', '2025-06-30')
        metric = replay.status()['evaluation'][0]
        self.assertEqual(metric['job_id'], first)
        self.assertEqual(metric['mae'], 2)
        self.assertEqual(metric['baseline_mae'], 4)
        self.assertEqual(len(replay.status()['forecasts']), 2)
        with db.connect() as conn:
            self.assertEqual(conn.execute('SELECT observed FROM predictions WHERE day=?', ('2025-06-30',)).fetchone()[0], None)

    def test_invalid_future_output_rolls_back_publication(self):
        self.publish('2025-06-28', '2025-06-29')
        with self.assertRaises(ValueError):
            self.publish('2025-06-29', '2025-07-01')
        self.assertEqual(replay.status()['forecasts'][0]['day'], '2025-06-29')
        with db.connect() as conn:
            details = json.loads(conn.execute("SELECT details FROM releases WHERE name='mobility'").fetchone()[0])
            self.assertEqual(details['replay']['cutoff_day'], '2025-06-28')


if __name__ == '__main__':
    unittest.main()
