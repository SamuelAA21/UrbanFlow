"""AWS service: HTTP city ticks, durable HDFS batches and Spark scheduling."""
import json
import os
import subprocess
import time
import threading
import urllib.request
import uuid

from .db import connect, initialize, rows
from .city import queue_processing
from .settings import RUNTIME


def hdfs(*args, check=True):
    return subprocess.run(['/opt/hadoop/bin/hdfs','dfs',*args],check=check,
                          stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)


def export_batch():
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        batch = conn.execute("SELECT * FROM city_batches WHERE state='pending' ORDER BY id LIMIT 1").fetchone()
        if not batch:
            events = rows(conn,'SELECT id FROM city_events WHERE archived=0 ORDER BY id LIMIT 10000')
            if not events:
                return
            path = '/urbanflow/city/events/'+uuid.uuid4().hex+'.jsonl'
            key = conn.execute('INSERT INTO city_batches(first_id,last_id,path,rows) VALUES(?,?,?,?)',
                               (events[0]['id'],events[-1]['id'],path,len(events))).lastrowid
            batch = conn.execute('SELECT * FROM city_batches WHERE id=?',(key,)).fetchone()
        records = rows(conn,'SELECT id,payload FROM city_events WHERE id BETWEEN ? AND ? ORDER BY id',
                       (batch['first_id'],batch['last_id']))
    spool = RUNTIME / 'city-spool'
    spool.mkdir(exist_ok=True)
    local = spool / f"batch-{batch['id']}.jsonl"
    with local.open('w',encoding='utf-8') as target:
        for record in records:
            event=json.loads(record['payload'])
            event['row_id']=record['id']
            target.write(json.dumps(event,separators=(',',':'))+'\n')
    size=local.stat().st_size
    hdfs('-mkdir','-p','/urbanflow/city/events')
    if hdfs('-test','-e',batch['path'],check=False).returncode:
        hdfs('-put','-f',str(local),batch['path']+'.part')
        hdfs('-mv',batch['path']+'.part',batch['path'])
    with connect() as conn:
        conn.execute('UPDATE city_events SET archived=1 WHERE id BETWEEN ? AND ?',
                     (batch['first_id'],batch['last_id']))
        conn.execute("UPDATE city_batches SET state='archived',bytes=? WHERE id=?",(size,batch['id']))
    local.unlink()


def main():
    initialize()
    # hadoop's shell environment is not assumed by systemd.
    os.environ.setdefault('HADOOP_CONF_DIR','/opt/hadoop/etc/hadoop')
    os.environ.setdefault('JAVA_HOME','/usr/lib/jvm/java-17-amazon-corretto.x86_64')
    def exports():
        while True:
            try:
                export_batch()
                queue_processing()
            except Exception as error:
                print(f'City export: {error}',flush=True)
            time.sleep(5)
    threading.Thread(target=exports,daemon=True).start()
    while True:
        started=time.monotonic()
        try:
            req=urllib.request.Request('http://127.0.0.1:8090/api/city/tick',data=b'{}',
                                       headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(req,timeout=30) as response:
                response.read()
        except Exception as error:
            print(f'City service: {error}',flush=True)
        time.sleep(max(.05,1-(time.monotonic()-started)))


if __name__=='__main__':
    main()
