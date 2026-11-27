"""Server-side historical replay scheduler. Only the worker advances this clock."""
import calendar
import os
import re
from datetime import date, timedelta

from .db import connect, rows


def configure(body):
    if os.environ.get('URBANFLOW_REPLAY_ENGINE') != '1':
        raise ValueError('Este servidor no tiene motor Spark habilitado. Despliega el portal en AWS para programar corridas.')
    if not isinstance(body, dict):
        raise ValueError('Se requiere un objeto JSON.')
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        if body.get('action') == 'stop':
            conn.execute("UPDATE replay_schedule SET enabled=0,message='Detenido por el operador' WHERE id=1")
            return {'status': 'stopped'}
        if body.get('action') == 'run_now':
            schedule = conn.execute('SELECT * FROM replay_schedule WHERE id=1 AND enabled=1').fetchone()
            if not schedule or schedule['last_job_id']:
                raise ValueError('No hay un proximo lote disponible o aun hay una corrida en curso.')
            conn.execute("UPDATE replay_schedule SET next_run=CURRENT_TIMESTAMP WHERE id=1")
            return {'status': 'due'}
        if conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running')").fetchone():
            raise ValueError('Espera a que termine el trabajo activo antes de cambiar la programacion.')
        if body.get('action') != 'start':
            raise ValueError('Accion invalida.')
        if any(not isinstance(body.get(key), str) for key in ('months', 'start_day', 'end_day')):
            raise ValueError('Indica meses y fechas como texto.')
        months = body['months'].split(',')
        if not 1 <= len(months) <= 6 or not all(re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])', m) for m in months):
            raise ValueError('Selecciona de uno a seis meses consecutivos YYYY-MM.')
        numbers = [int(m[:4]) * 12 + int(m[5:]) for m in months]
        if numbers != list(range(numbers[0], numbers[0] + len(months))):
            raise ValueError('Los meses deben ser consecutivos y ordenados.')
        start, end = date.fromisoformat(body.get('start_day', '')), date.fromisoformat(body.get('end_day', ''))
        first = date.fromisoformat(months[0] + '-01')
        year, month = map(int, months[-1].split('-'))
        last = date(year, month, calendar.monthrange(year, month)[1])
        if not first + timedelta(days=27) <= start <= end <= last:
            raise ValueError('El corte inicial requiere al menos 28 dias de historia y el fin debe estar dentro de los meses elegidos.')
        minutes = body.get('interval_minutes', 10)
        if type(minutes) is not int or not 1 <= minutes <= 1440:
            raise ValueError('El intervalo debe ser un entero de 1 a 1440 minutos.')
        conn.execute('''INSERT INTO replay_schedule(id,enabled,months,next_day,end_day,interval_minutes)
            VALUES(1,1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET enabled=1,months=excluded.months,
            next_day=excluded.next_day,end_day=excluded.end_day,interval_minutes=excluded.interval_minutes,
            next_run=CURRENT_TIMESTAMP,last_job_id=NULL,message='Programado por el operador' ''',
            (','.join(months), start.isoformat(), end.isoformat(), minutes))
    return {'status': 'scheduled'}


def enqueue_due():
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        schedule = conn.execute('SELECT * FROM replay_schedule WHERE id=1 AND enabled=1').fetchone()
        if not schedule:
            return
        if schedule['last_job_id']:
            job = conn.execute('SELECT * FROM jobs WHERE id=?', (schedule['last_job_id'],)).fetchone()
            if job['status'] == 'failed':
                conn.execute("UPDATE replay_schedule SET enabled=0,message='Trabajo fallido; revisa los logs antes de reanudar' WHERE id=1")
                return
            if job['status'] != 'completed':
                return
            next_day = (date.fromisoformat(job['cutoff_day']) + timedelta(days=1)).isoformat()
            conn.execute('''UPDATE replay_schedule SET next_day=?,last_job_id=NULL,
                next_run=datetime('now','+' || interval_minutes || ' minutes'),
                message='Lote terminado; esperando el siguiente intervalo' WHERE id=1''', (next_day,))
            if next_day > schedule['end_day']:
                conn.execute("UPDATE replay_schedule SET enabled=0,message='Reproduccion terminada' WHERE id=1")
            return
        if not conn.execute("SELECT 1 FROM replay_schedule WHERE id=1 AND next_run<=CURRENT_TIMESTAMP").fetchone():
            return
        if conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running')").fetchone():
            return
        job_id = conn.execute("INSERT INTO jobs(kind,months,status,cutoff_day) VALUES('replay',?,'queued',?)",
                              (schedule['months'], schedule['next_day'])).lastrowid
        conn.execute("UPDATE replay_schedule SET last_job_id=?,message='Lote en cola de Spark' WHERE id=1", (job_id,))


def status():
    with connect(readonly=True) as conn:
        schedule = conn.execute('SELECT * FROM replay_schedule WHERE id=1').fetchone()
        jobs = rows(conn, "SELECT * FROM jobs WHERE kind='replay' ORDER BY id DESC LIMIT 10")
        forecast = rows(conn, '''SELECT job_id,cutoff_day,day,count(*) AS cells,sum(estimate) AS total
            FROM forecast_history GROUP BY job_id,cutoff_day,day ORDER BY job_id DESC LIMIT 10''')
        evaluation = rows(conn, '''SELECT f.job_id,f.day,count(*) AS observed_cells,
            avg(abs(f.estimate-o.trips)) AS mae,avg(abs(f.baseline-o.trips)) AS baseline_mae
            FROM forecast_history f JOIN replay_observations o
            ON o.day=f.day AND o.zone=f.zone AND o.hour=f.hour
            GROUP BY f.job_id,f.day ORDER BY f.job_id DESC LIMIT 10''')
    return {'engine_enabled': os.environ.get('URBANFLOW_REPLAY_ENGINE') == '1',
            'schedule': dict(schedule) if schedule else None, 'jobs': jobs,
            'forecasts': forecast, 'evaluation': evaluation,
            'mode': 'historical_replay', 'horizon_days': 1}
