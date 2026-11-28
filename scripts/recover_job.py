"""Mark only an explicitly stopped, matching application as failed; keep all data."""
import argparse
import sqlite3

p=argparse.ArgumentParser()
p.add_argument('database')
p.add_argument('job',type=int)
p.add_argument('application')
args=p.parse_args()
with sqlite3.connect(args.database) as conn:
    job=conn.execute('SELECT status,yarn_id FROM jobs WHERE id=?',(args.job,)).fetchone()
    if not job or job!=('running',args.application):
        raise SystemExit('Job no longer matches the stopped application; no update performed.')
    conn.execute("UPDATE stages SET status='failed',ended_at=CURRENT_TIMESTAMP WHERE job_id=? AND status='running'",(args.job,))
    conn.execute("UPDATE jobs SET status='failed',ended_at=CURRENT_TIMESTAMP,error=? WHERE id=?",
                 ('Interrupted after executor heartbeat timeouts; retry with memory-safe persistence. Original data and outputs retained.',args.job))
print('Matching stopped job marked failed; no files or observations deleted.')
