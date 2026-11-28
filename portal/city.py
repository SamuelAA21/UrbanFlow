"""Individual trip lifecycle, durable event outbox and isolated city experiments."""
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timedelta

from .db import connect, rows
from .settings import RUNTIME


def require_engine():
    if os.environ.get('URBANFLOW_REPLAY_ENGINE') != '1':
        raise ValueError('La ciudad se ejecuta en el motor AWS, no en el navegador.')


def integer(body, key, default, low, high):
    value = body.get(key, default)
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{key}: selecciona un entero entre {low} y {high}.')
    return value


def parsed_time(value):
    if not isinstance(value,str):
        raise ValueError('La fecha debe ser texto ISO 8601.')
    return datetime.fromisoformat(value.replace('Z','+00:00'))


def configure(body):
    require_engine()
    if not isinstance(body, dict):
        raise ValueError('Se requiere JSON.')
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        if body.get('action') == 'stop':
            conn.execute("UPDATE city_sessions SET state='draining' WHERE state='running'")
            return {'status': 'draining', 'message': 'No se crean solicitudes nuevas; se terminan los viajes pendientes.'}
        if body.get('action') != 'start':
            raise ValueError('Accion invalida.')
        if conn.execute("SELECT 1 FROM city_sessions WHERE state IN ('running','draining')").fetchone():
            raise ValueError('Ya hay una ciudad activa o terminando viajes.')
        sample_path = RUNTIME / 'city_seed.json'
        if not sample_path.exists():
            raise ValueError('Ejecuta primero la preparacion de la semilla de ciudad en Spark.')
        sample = json.loads(sample_path.read_text(encoding='utf-8'))
        if not sample.get('trips'):
            raise ValueError('La semilla no contiene viajes historicos reales.')
        start = datetime.fromisoformat(body.get('clock_start', sample['suggested_start']))
        if start.tzinfo is not None:
            raise ValueError('Usa hora local simulada sin zona horaria.')
        rate = integer(body, 'rate', 100, 1, 2000)
        maximum = integer(body, 'max_requests', 30000, 100, 1000000)
        speed = integer(body, 'clock_speed', 60, 1, 3600)
        interval = integer(body, 'interval_minutes', 3, 1, 60)
        seed = integer(body, 'seed', 42, 0, 2**31-1)
        now = time.time()
        session = conn.execute('''INSERT INTO city_sessions
            (state,rate,max_requests,clock_start,clock_speed,last_tick,interval_minutes,next_run,seed,sample)
            VALUES('running',?,?,?,?,?,?,?,?,?)''',
            (rate,maximum,start.isoformat(),speed,now,interval,now+interval*60,seed,json.dumps(sample))).lastrowid
        conn.execute('''INSERT INTO city_engine VALUES(1,1,?,?,NULL) ON CONFLICT(id) DO UPDATE
            SET enabled=1,interval_minutes=excluded.interval_minutes,next_run=excluded.next_run,error=NULL''',
            (interval,now+interval*60))
    return {'id': session, 'status': 'running', 'source': 'simulated'}


def store(conn, event):
    required = ('event_id','trip_id','kind','source','event_time','origin','destination')
    if not isinstance(event, dict) or any(key not in event for key in required):
        raise ValueError('Faltan campos del evento.')
    if event['kind'] not in ('requested','completed','cancelled') or event['source'] not in ('simulated','driver_report'):
        raise ValueError('Tipo u origen invalido.')
    for key in ('event_id','trip_id'):
        if not isinstance(event[key], str) or not 1 <= len(event[key]) <= 120:
            raise ValueError('Identificador invalido.')
    parsed_time(event['event_time'])
    for key in ('origin','destination'):
        integer(event,key,0,1,265)
    if event['kind'] == 'completed':
        pickup = parsed_time(event.get('pickup_time',''))
        dropoff = parsed_time(event.get('dropoff_time',''))
        if not 60 <= (dropoff-pickup).total_seconds() <= 10800:
            raise ValueError('Duracion invalida para un viaje realizado.')
        for field in ('distance_mi','fare'):
            value = event.get(field)
            if type(value) not in (int,float) or not math.isfinite(value) or value <= 0:
                raise ValueError('Distancia o tarifa invalida.')
    # Both unique keys provide retry protection without treating a request as a label.
    result = conn.execute('''INSERT OR IGNORE INTO city_events
        (event_id,trip_id,session_id,kind,source,event_time,origin,destination,payload)
        VALUES(?,?,?,?,?,?,?,?,?)''',
        (event['event_id'],event['trip_id'],event.get('session_id'),event['kind'],event['source'],
         event['event_time'],event['origin'],event['destination'],json.dumps(event,separators=(',',':'))))
    if result.rowcount:
        conn.execute('INSERT INTO city_counters VALUES(?,?,1) ON CONFLICT(source,kind) DO UPDATE SET n=n+1',
                     (event['source'],event['kind']))
    return result.rowcount


def ingest(body):
    require_engine()
    events = body.get('events') if isinstance(body,dict) else None
    if not isinstance(events,list) or not 1 <= len(events) <= 2000:
        raise ValueError('Envia de 1 a 2000 eventos por lote.')
    with connect() as conn:
        for event in events:
            if not isinstance(event,dict) or event.get('source') != 'driver_report' or event.get('session_id') is not None:
                raise ValueError('Esta entrada privada admite reportes de conductor; la simulacion tiene su propio generador.')
        accepted = sum(store(conn,event) for event in events)
    return {'accepted': accepted, 'duplicates': len(events)-accepted,
            'trust': 'operator-supplied driver report; production driver authentication is not implemented'}


def tick():
    """Called over HTTP by the AWS service. A browser never generates this traffic."""
    require_engine()
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        session = conn.execute("SELECT * FROM city_sessions WHERE state IN ('running','draining') ORDER BY id DESC LIMIT 1").fetchone()
        if not session:
            return {'created':0}
        now = time.time()
        elapsed = min(5,max(0,now-session['last_tick']))
        if elapsed < .5:
            return {'created':0}
        clock = session['clock_seconds'] + elapsed*session['clock_speed']
        logical = datetime.fromisoformat(session['clock_start']) + timedelta(seconds=clock)
        rng = random.Random(session['seed']+session['generated'])
        sample = json.loads(session['sample'])
        spark_weekday=logical.isoweekday()%7+1
        matching = [t for t in sample['trips'] if t['hour']==logical.hour and t.get('weekday',spark_weekday)==spark_weekday]
        weekday_seed=any('weekday' in t for t in sample['trips'])
        candidates = matching or sample['trips']
        total=conn.execute('SELECT coalesce(sum(n),0) FROM city_counters').fetchone()[0]
        archived=conn.execute("SELECT coalesce(sum(rows),0) FROM city_batches WHERE state='archived'").fetchone()[0]
        pressure=total-archived>=100000
        hour_factor=len(matching)*(168 if weekday_seed else 24)/len(sample['trips']) if matching else 1
        expected=elapsed*session['rate']*hour_factor
        arrivals=max(0,round(rng.gauss(expected,math.sqrt(expected))))
        n = min(arrivals,2000,session['max_requests']-session['generated']) if session['state']=='running' and not pressure else 0
        created = 0
        for i in range(n):
            original = rng.choice(candidates)
            trip_id = f"city-{session['id']}-{session['generated']+i}"
            pickup = logical-timedelta(seconds=rng.random()*elapsed*session['clock_speed'])
            minutes = min(180,max(1,original['duration_min']*rng.lognormvariate(-.02,.2)))
            dropoff = pickup+timedelta(minutes=minutes)
            variation=rng.lognormvariate(-.005,.1)
            event = {'event_id':trip_id+'-request','trip_id':trip_id,'session_id':session['id'],
                     'kind':'requested','source':'simulated','event_time':pickup.isoformat(),
                     'origin':original['origin'],'destination':original['destination'],
                     'pickup_time':pickup.isoformat(),'dropoff_time':dropoff.isoformat(),
                     'distance_mi':round(max(.1,original['distance_mi']*variation),3),
                     'fare':round(max(.01,original['fare']*variation),2),
                     'seed_source':sample['source'],'seed':session['seed'],
                     'weekday':pickup.isoweekday(),'hour':pickup.hour}
            created += store(conn,event)
            event['kind'] = 'cancelled' if rng.random()<.08 else 'completed'
            event['event_id'] = trip_id+'-'+event['kind']
            event['event_time'] = dropoff.isoformat()
            conn.execute('INSERT INTO city_pending VALUES(?,?,?,?)',
                         (trip_id,session['id'],(dropoff-datetime.fromisoformat(session['clock_start'])).total_seconds(),json.dumps(event)))
        pending = rows(conn,'SELECT * FROM city_pending WHERE session_id=? AND due<=? ORDER BY due LIMIT 4000',(session['id'],clock))
        for p in pending:
            created += store(conn,json.loads(p['payload']))
            conn.execute('DELETE FROM city_pending WHERE trip_id=?',(p['trip_id'],))
        generated = session['generated']+n
        state = 'draining' if generated>=session['max_requests'] else session['state']
        if state=='draining' and not conn.execute('SELECT 1 FROM city_pending WHERE session_id=?',(session['id'],)).fetchone():
            state='completed'
        conn.execute('UPDATE city_sessions SET state=?,generated=?,clock_seconds=?,last_tick=? WHERE id=?',
                     (state,generated,clock,now,session['id']))
    return {'created':created,'requests':n,'state':state,'simulated_time':logical.isoformat(),'backpressure':pressure}


def queue_processing():
    """Only process an immutable HDFS high-water snapshot, never unarchived rows."""
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        active = conn.execute("SELECT id,kind,status FROM jobs WHERE status IN ('queued','running') ORDER BY id LIMIT 1").fetchone()
        if active:
            return {'status':'waiting_job','active_job_id':active['id'],'job_kind':active['kind'],
                    'message':f"El trabajo #{active['id']} ya está en cola o ejecutándose. No se inicia otro a la vez; el lote pendiente se procesará después."}
        engine = conn.execute('SELECT * FROM city_engine WHERE id=1 AND enabled=1').fetchone()
        if not engine or time.time()<engine['next_run']:
            return {'status':'scheduled','message':'El motor está esperando su próxima ejecución programada.'}
        if engine['error']:
            return {'status':'blocked','message':engine['error']}
        previous = conn.execute("SELECT * FROM city_runs WHERE status='completed' ORDER BY job_id DESC LIMIT 1").fetchone()
        low = previous['high_water'] if previous else 0
        high = conn.execute('SELECT coalesce(max(last_id),0) FROM city_batches WHERE state=\'archived\'').fetchone()[0]
        # A failed batch must prevent advancing past its gap.
        gap = conn.execute("SELECT min(first_id) FROM city_batches WHERE state!='archived'").fetchone()[0]
        if gap is not None:
            high=min(high,gap-1)
        if high<=low:
            pending = conn.execute('SELECT coalesce(sum(n),0) FROM city_counters').fetchone()[0]
            pending -= conn.execute("SELECT coalesce(sum(rows),0) FROM city_batches WHERE state='archived'").fetchone()[0]
            if pending>0:
                return {'status':'waiting_archive','pending_events':pending,
                        'message':f'Esperando que {pending} eventos lleguen a HDFS. El procesamiento comenzará automáticamente al archivarse.'}
            return {'status':'no_new_data','message':'No hay eventos nuevos archivados por procesar. El último lote ya fue procesado; activa la ciudad para recibir más viajes.'}
        job = conn.execute("INSERT INTO jobs(kind,months,status) VALUES('city','city','queued')").lastrowid
        conn.execute('INSERT INTO city_runs(job_id,high_water,previous_job) VALUES(?,?,?)',
                     (job,high,previous['job_id'] if previous else None))
        conn.execute('UPDATE city_engine SET next_run=? WHERE id=1',
                     (time.time()+engine['interval_minutes']*60,))
        return {'status':'queued','id':job,'new_event_rows':high-low,
                'message':f'Trabajo #{job} en cola: {high-low} eventos nuevos. El pronóstico se actualizará cuando termine el cálculo, si hay suficientes viajes completados.'}


def status():
    with connect(readonly=True) as conn:
        session = conn.execute('SELECT id,state,created_at,rate,max_requests,generated,clock_start,clock_speed,clock_seconds,next_run,error FROM city_sessions ORDER BY id DESC LIMIT 1').fetchone()
        counts = rows(conn,'SELECT * FROM city_counters')
        totals = conn.execute('SELECT count(*),coalesce(sum(bytes),0),coalesce(sum(rows),0) FROM city_batches WHERE state=\'archived\'').fetchone()
        columns='id,trip_id,kind,source,event_time,received_at,origin,destination,archived'
        recent=rows(conn,f"SELECT {columns} FROM city_events WHERE kind='requested' ORDER BY id DESC LIMIT 10")
        recent+=rows(conn,f"SELECT {columns} FROM city_events WHERE kind!='requested' ORDER BY id DESC LIMIT 10")
        recent.sort(key=lambda item:item['id'],reverse=True)
        runs = rows(conn,'SELECT c.*,j.status AS job_status,j.stage,j.yarn_id,j.error FROM city_runs c JOIN jobs j ON j.id=c.job_id ORDER BY job_id DESC LIMIT 8')
        for run in runs:
            run['metrics'] = json.loads(run['metrics']) if run['metrics'] else None
        forecast = rows(conn,'SELECT * FROM city_forecasts WHERE job_id=(SELECT max(job_id) FROM city_forecasts) ORDER BY day,hour,zone LIMIT 30')
        forecast_totals=rows(conn,'SELECT day,source,count(*) AS cells,sum(estimate) AS total FROM city_forecasts WHERE job_id=(SELECT max(job_id) FROM city_forecasts) GROUP BY day,source')
        engine=conn.execute('SELECT * FROM city_engine WHERE id=1').fetchone()
        active_job=conn.execute("SELECT id,kind,status,stage FROM jobs WHERE status IN ('queued','running') ORDER BY id LIMIT 1").fetchone()
    return {'engine_enabled':os.environ.get('URBANFLOW_REPLAY_ENGINE')=='1',
            'session':dict(session) if session else None,'counts':counts,'hdfs_batches':totals[0],
            'hdfs_logical_bytes':totals[1],'recent':recent,'runs':runs,'forecasts':forecast,
            'unarchived_events':sum(c['n'] for c in counts)-totals[2],
            'engine':dict(engine) if engine else None,
            'active_job':dict(active_job) if active_job else None,
            'forecast_totals':forecast_totals,
            'training_policy':'Only completed reports; simulated model separate from real mobility model. No guarantee of improved real-world accuracy.'}


def enqueue(body):
    require_engine()
    if not isinstance(body,dict):
        raise ValueError('Se requiere JSON.')
    action=body.get('action')
    if action not in ('city_seed','benchmark','history','run_now'):
        raise ValueError('Accion invalida.')
    if action=='run_now':
        with connect() as conn:
            conn.execute('INSERT INTO city_engine VALUES(1,1,3,0,NULL) ON CONFLICT(id) DO UPDATE SET enabled=1,next_run=0,error=NULL')
            conn.execute('UPDATE city_sessions SET error=NULL')
        return queue_processing()
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        if conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running')").fetchone():
            raise ValueError('Hay un trabajo en curso. Espera a que termine.')
        if conn.execute('SELECT 1 FROM replay_schedule WHERE enabled=1').fetchone():
            raise ValueError('Deten primero la reproduccion historica.')
        months=body.get('months','')
        if action in ('history','benchmark'):
            import re
            parts=months.split(',') if isinstance(months,str) else []
            if not 1<=len(parts)<=24 or any(not re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])',m) for m in parts):
                raise ValueError('Selecciona de 1 a 24 meses consecutivos.')
            serial=[int(m[:4])*12+int(m[5:]) for m in parts]
            if serial!=list(range(serial[0],serial[0]+len(serial))):
                raise ValueError('Los meses deben ser consecutivos.')
        else:
            months='city'
        job=conn.execute('INSERT INTO jobs(kind,months,status) VALUES(?,?,\'queued\')',(action,months)).lastrowid
    return {'id':job,'status':'queued'}
