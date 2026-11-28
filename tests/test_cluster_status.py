import unittest
from unittest.mock import patch
from portal import cluster_status


class ClusterStatusTests(unittest.TestCase):
    def test_active_and_recent_apps_stay_visible_after_many_completed_runs(self):
        apps=[dict(id=f'application_1_{i:04d}',state='FINISHED',startedTime=i) for i in range(1,40)]
        apps.append(dict(id='application_1_0000',state='RUNNING',startedTime=0))
        def read(path):
            return {'nodes':{'node':[]}} if path=='nodes' else {'apps':{'app':apps}}
        with patch.object(cluster_status,'read',side_effect=read) as fetch:
            result=cluster_status.status()
        self.assertTrue(result['available'])
        ids=[a['id'] for a in result['applications']]
        self.assertEqual(len(ids),12)
        self.assertEqual(ids[:3],['application_1_0000','application_1_0039','application_1_0038'])
        self.assertNotIn('application_1_0001',ids)
        self.assertIn(unittest.mock.call('apps'),fetch.call_args_list)
