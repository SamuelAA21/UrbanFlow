import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from portal import city,city_service,db,worker


class CityTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        for obj,name,value in ((db,'DB',self.root/'db.sqlite3'),(city,'RUNTIME',self.root),(city_service,'RUNTIME',self.root)):
            p=patch.object(obj,name,value);p.start();self.addCleanup(p.stop)
        p=patch.dict(os.environ,{'URBANFLOW_REPLAY_ENGINE':'1'});p.start();self.addCleanup(p.stop)
        db.initialize()
        seed={'source':'real historical fixture','suggested_start':'2025-07-01T00:00:00',
              'trips':[{'origin':132,'destination':161,'hour':0,'duration_min':1,'distance_mi':2.,'fare':10.}]}
        (self.root/'city_seed.json').write_text(json.dumps(seed),encoding='utf-8')

    def test_server_tick_lifecycle_is_persisted_and_never_labels_requests(self):
        with patch('portal.city.time.time',return_value=1000):
            city.configure({'action':'start','rate':100,'max_requests':100,'clock_speed':3600})
        with patch('portal.city.time.time',return_value=1001):
            result=city.tick()
        self.assertGreater(result['requests'],0)
        self.assertLessEqual(result['requests'],100)
        for now in (1003,1005,1007):
            with patch('portal.city.time.time',return_value=now):
                city.tick()
        data=city.status()
        self.assertEqual(data['session']['state'],'completed')
        counts={c['kind']:c['n'] for c in data['counts']}
        self.assertEqual(counts['requested'],100)
        self.assertEqual(counts.get('completed',0)+counts.get('cancelled',0),100)
        self.assertTrue(all(r['source']=='simulated' for r in data['recent']))
        with db.connect() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM predictions').fetchone()[0],0)
            self.assertEqual(conn.execute('SELECT count(*) FROM city_pending').fetchone()[0],0)

    def test_stop_freezes_requests_while_pending_trips_finish(self):
        with patch('portal.city.time.time',return_value=1000):
            city.configure({'action':'start','rate':10,'max_requests':1000,'clock_speed':1})
        with patch('portal.city.time.time',return_value=1001):
            city.tick()
        before=city.status()['session']['generated']
        self.assertGreater(before,0)
        city.configure({'action':'stop'})
        self.assertEqual(city.status()['session']['state'],'draining')
        with db.connect() as conn:
            conn.execute('UPDATE city_sessions SET clock_speed=3600')
        with patch('portal.city.time.time',return_value=1006):
            city.tick()
        data=city.status()
        self.assertEqual(data['session']['generated'],before)
        self.assertEqual(data['session']['state'],'completed')
        counts={c['kind']:c['n'] for c in data['counts']}
        self.assertEqual(counts.get('completed',0)+counts.get('cancelled',0),before)

    def test_driver_contract_idempotency_and_batch_rollback(self):
        event=dict(event_id='report-1',trip_id='driver-1',kind='completed',source='driver_report',
                   event_time='2025-07-01T10:10:00',pickup_time='2025-07-01T10:00:00',
                   dropoff_time='2025-07-01T10:10:00',origin=132,destination=161,distance_mi=2.,fare=10.)
        self.assertEqual(city.ingest({'events':[event]})['accepted'],1)
        self.assertEqual(city.ingest({'events':[event]})['duplicates'],1)
        self.assertEqual(city.ingest({'events':[{**event,'event_id':'cancelled-late','kind':'cancelled'}]})['duplicates'],1)
        with self.assertRaises(ValueError):
            city.ingest({'events':[{**event,'event_id':'report-2','trip_id':'driver-2'},
                                   {**event,'event_id':'bad','trip_id':'bad','distance_mi':-1}]})
        self.assertEqual(sum(c['n'] for c in city.status()['counts']),1)
        with self.assertRaises(ValueError):
            city.ingest({'events':[{**event,'source':'simulated'}]})

    def test_outbox_export_is_retryable_and_queues_only_archived_snapshots(self):
        city.configure({'action':'start','rate':100,'max_requests':100})
        with db.connect() as conn:
            for i in range(10):
                city.store(conn,dict(event_id=f'e-{i}',trip_id=f't-{i}',kind='requested',source='driver_report',
                                     event_time='2025-07-01T10:00:00',origin=132,destination=161))
            conn.execute('UPDATE city_engine SET next_run=0')
        city.queue_processing()
        self.assertEqual(city.status()['runs'],[])
        calls=[]
        def fake_hdfs(*args,**kwargs):
            calls.append(args)
            if args[0]=='-test':
                class R: returncode=1
                return R()
        with patch('portal.city_service.hdfs',side_effect=fake_hdfs):
            city_service.export_batch()
        self.assertTrue(any(c[0]=='-mv' for c in calls))
        self.assertEqual(city.status()['unarchived_events'],0)
        city.queue_processing()
        job=worker.claim()
        self.assertEqual(job['kind'],'city')
        self.assertEqual(city.status()['runs'][0]['high_water'],10)
        city.queue_processing()
        self.assertIsNone(worker.claim())

    def test_manual_processing_reports_no_data_archive_wait_queue_and_active_job(self):
        result=city.enqueue({'action':'run_now'})
        self.assertEqual(result['status'],'no_new_data')
        with db.connect() as conn:
            city.store(conn,dict(event_id='e',trip_id='t',kind='requested',source='simulated',
                                 event_time='2025-07-01T10:00:00',origin=132,destination=161))
        self.assertEqual(city.enqueue({'action':'run_now'})['status'],'waiting_archive')
        with db.connect() as conn:
            conn.execute("INSERT INTO city_batches(first_id,last_id,path,rows,state) VALUES(1,1,'/test.jsonl',1,'archived')")
            conn.execute('UPDATE city_events SET archived=1')
        queued=city.enqueue({'action':'run_now'})
        self.assertEqual(queued['status'],'queued')
        self.assertEqual(queued['new_event_rows'],1)
        active=city.enqueue({'action':'run_now'})
        self.assertEqual(active['status'],'waiting_job')
        self.assertEqual(active['active_job_id'],queued['id'])
        with db.connect() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM jobs').fetchone()[0],1)
            conn.execute("UPDATE jobs SET status='completed'")
            conn.execute("UPDATE city_runs SET status='completed'")
        self.assertEqual(city.enqueue({'action':'run_now'})['status'],'no_new_data')

    def test_failed_export_keeps_events_for_retry(self):
        with db.connect() as conn:
            city.store(conn,dict(event_id='e',trip_id='t',kind='requested',source='driver_report',
                                 event_time='2025-07-01T10:00:00',origin=132,destination=161))
        with patch('portal.city_service.hdfs',side_effect=RuntimeError('HDFS unavailable')):
            with self.assertRaises(RuntimeError):
                city_service.export_batch()
        self.assertEqual(city.status()['unarchived_events'],1)
        self.assertEqual(city.status()['hdfs_batches'],0)

    def test_evidence_counts_only_completed_simulations_in_successfully_trained_snapshots(self):
        with db.connect() as conn:
            conn.executemany('INSERT INTO city_counters VALUES(?,?,?)',[
                ('simulated','completed',9),('simulated','requested',20),('simulated','cancelled',3),
                ('driver_report','completed',100)])
        empty=city.status()['simulation_evidence']
        self.assertIsNone(empty['model_job_id'])
        self.assertEqual(empty['pending_completed'],9)
        def snapshot(completed,trained=True,status='completed'):
            with db.connect() as conn:
                job=conn.execute("INSERT INTO jobs(kind,months,status,ended_at) VALUES('city','city',?,'2026-11-28 06:00:00')",(status,)).lastrowid
                metrics={'counts':{'simulated:completed':completed},'new_event_rows':10,
                         'training':{'simulated':{'model_path':f'/models/{job}','forecast_day':'2025-07-03'}} if trained else {}}
                conn.execute('INSERT INTO city_runs(job_id,high_water,metrics,status) VALUES(?,?,?,?)',
                             (job,job*10,json.dumps(metrics),status))
            return job
        first=snapshot(5)
        snapshot(9,status='failed')
        snapshot(8,trained=False)
        e=city.status()['simulation_evidence']
        self.assertEqual((e['model_job_id'],e['saved_completed'],e['included_completed'],e['pending_completed']),(first,9,5,4))
        latest=snapshot(8)
        with db.connect() as conn:
            conn.execute("INSERT INTO city_forecasts VALUES(?,'2025-07-03',1,132,42,'simulated')",(latest,))
        e=city.status()['simulation_evidence']
        self.assertEqual((e['model_job_id'],e['previous_job_id'],e['previous_included_completed']),(latest,first,5))
        self.assertEqual((e['included_completed'],e['pending_completed'],e['forecast_cells']),(8,1,1))

    def test_city_publication_keeps_real_model_and_forecasts_separate(self):
        with db.connect() as conn:
            job=conn.execute("INSERT INTO jobs(kind,months,status) VALUES('city','city','completed')").lastrowid
            conn.execute('INSERT INTO city_runs(job_id,high_water) VALUES(?,10)',(job,))
            conn.execute("INSERT INTO predictions VALUES('2025-07-01',132,8,NULL,42,40)")
        (self.root/'city_metrics.json').write_text(json.dumps({'counts':{'simulated:completed':8},'real_world_accuracy':None}),encoding='utf-8')
        (self.root/'city_forecasts.csv').write_text('day,hour,zone,estimate,source\n2025-07-03,8,132,99,simulated\n',encoding='utf-8')
        with db.connect() as conn:
            db.publish(conn,{'id':job,'kind':'city'},self.root)
        with db.connect() as conn:
            self.assertEqual(conn.execute('SELECT estimate FROM predictions').fetchone()[0],42)
            self.assertEqual(conn.execute('SELECT source FROM city_forecasts').fetchone()[0],'simulated')
            self.assertEqual(conn.execute('SELECT count(*) FROM replay_observations').fetchone()[0],0)


if __name__=='__main__':unittest.main()
