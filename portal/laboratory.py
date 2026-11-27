"""Auditable statistical baseline, ridge regression and isolated synthetic traffic.

Only predictions.observed (aggregated historical TLC counts) supplies labels.
No published estimate, profile or synthetic request is ever a training label.
The final 30% of dates is an untouched chronological test, not a tuning set.
"""
import hashlib
import json
import math
import random
import uuid
from datetime import date, timedelta

from .db import connect, rows


class LabError(ValueError):
    pass


def features(day, hour, baseline):
    weekday = date.fromisoformat(day).weekday()
    return [1.0, baseline / 100, math.sin(hour * math.tau / 24),
            math.cos(hour * math.tau / 24)] + [float(weekday == i) for i in range(6)]


def solve(matrix, vector):
    """Gaussian elimination with partial pivoting; fixed ridge penalty = 1."""
    size = len(vector)
    a = [row[:] + [value] for row, value in zip(matrix, vector)]
    for k in range(size):
        pivot = max(range(k, size), key=lambda i: abs(a[i][k]))
        a[k], a[pivot] = a[pivot], a[k]
        scale = a[k][k]
        a[k] = [v / scale for v in a[k]]
        for i in range(size):
            if i != k:
                scale = a[i][k]
                a[i] = [v - scale * w for v, w in zip(a[i], a[k])]
    return [row[-1] for row in a]


def baseline(artifact, zone, hour):
    return artifact['means'].get(f'{zone}:{hour}', artifact['global_mean'])


def predict(artifact, day, zone, hour):
    fixed = baseline(artifact, zone, hour)
    learned = max(0.0, 100 * sum(w * x for w, x in zip(
        artifact['weights'], features(day, hour, fixed))))
    return fixed, learned


def score(pairs):
    actual = [a for a, _ in pairs]
    mean = sum(actual) / len(actual)
    squared = sum((a - p) ** 2 for a, p in pairs)
    variance = sum((a - mean) ** 2 for a in actual)
    return {'mae': sum(abs(a - p) for a, p in pairs) / len(pairs),
            'rmse': math.sqrt(squared / len(pairs)),
            'r2': 1 - squared / variance if variance else None}


def train():
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        data = rows(conn, 'SELECT day,zone,hour,observed FROM predictions '
                    'WHERE observed IS NOT NULL ORDER BY day,hour,zone')
        days = sorted({r['day'] for r in data})
        if len(days) < 7:
            raise LabError('Se necesitan al menos siete fechas con observaciones reales publicadas.')
        cutoff = days[max(1, int(len(days) * .7)) - 1]
        training = [r for r in data if r['day'] <= cutoff]
        testing = [r for r in data if r['day'] > cutoff]
        sums = {}
        for r in training:
            key = f"{r['zone']}:{r['hour']}"
            total, count = sums.get(key, (0, 0))
            sums[key] = (total + r['observed'], count + 1)
        artifact = {'means': {k: total / count for k, (total, count) in sums.items()},
                    'global_mean': sum(r['observed'] for r in training) / len(training),
                    'cutoff': cutoff, 'last_day': days[-1],
                    'cells': [list(map(int, k.split(':'))) for k in sorted(sums)],
                    'source': 'predictions.observed / NYC TLC historical aggregates',
                    'sha256': hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()}
        dim = len(features(days[0], 0, 0))
        matrix = [[float(i == j) for j in range(dim)] for i in range(dim)]
        vector = [0.0] * dim
        for r in training:
            x = features(r['day'], r['hour'], baseline(artifact, r['zone'], r['hour']))
            for i in range(dim):
                vector[i] += x[i] * r['observed'] / 100
                for j in range(dim):
                    matrix[i][j] += x[i] * x[j]
        artifact['weights'] = solve(matrix, vector)
        fixed, ml = [], []
        for r in testing:
            a, b = predict(artifact, r['day'], r['zone'], r['hour'])
            fixed.append((r['observed'], a))
            ml.append((r['observed'], b))
        metrics = {'training_rows': len(training), 'test_rows': len(testing),
                   'training_from': days[0], 'training_until': cutoff,
                   'test_from': testing[0]['day'], 'test_until': days[-1],
                   'static': score(fixed), 'ml': score(ml),
                   'algorithm': 'Ridge regression, lambda=1 (fixed before test)',
                   'policy': 'Chronological 70/30 split by whole dates. Frozen test. Real labels only.',
                   'synthetic_training_rows': 0, 'source_sha256': artifact['sha256']}
        model_id = conn.execute('INSERT INTO lab_models(artifact,metrics) VALUES(?,?)',
                                (json.dumps(artifact), json.dumps(metrics))).lastrowid
    return {'id': model_id, **metrics}


def integer(body, name, default, low, high):
    value = body.get(name, default)
    if type(value) is not int or not low <= value <= high:
        raise LabError(f'{name} debe ser un entero entre {low} y {high}.')
    return value


def generate(body):
    """One HTTP request commits one synthetic scenario. Idempotent retries."""
    if not isinstance(body, dict):
        raise LabError('Se requiere un objeto JSON.')
    if any(k in body for k in ('observed', 'source', 'label')):
        raise LabError('El simulador no acepta etiquetas observadas ni cambios de origen.')
    seed = integer(body, 'seed', 42, 0, 2**31 - 1)
    key = body.get('request_key', str(uuid.uuid4()))
    if not isinstance(key, str) or not 1 <= len(key) <= 100:
        raise LabError('request_key invalida.')
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        existing = conn.execute('SELECT * FROM lab_requests WHERE request_key=?', (key,)).fetchone()
        if existing:
            return dict(existing)
        row = conn.execute('SELECT * FROM lab_models ORDER BY id DESC LIMIT 1').fetchone()
        if not row:
            raise LabError('Entrena primero los modelos con los datos reales.')
        artifact = json.loads(row['artifact'])
        rng = random.Random(seed)
        # Sample cells from the frozen historical statistical model, never from ML output.
        zone, hour = rng.choices(artifact['cells'], weights=[
            baseline(artifact, z, h) + 1 for z, h in artifact['cells']])[0]
        day = (date.fromisoformat(artifact['last_day']) + timedelta(days=rng.randint(1, 7))).isoformat()
        fixed, ml = predict(artifact, day, zone, hour)
        # Explicit simulation: log-normal variation with mean fixed, not real demand.
        simulated = round(fixed * rng.lognormvariate(-.5 * .3**2, .3))
        weekday = date.fromisoformat(day).isoweekday()
        period = 'madrugada' if hour < 6 else 'manana' if hour < 12 else 'tarde' if hour < 18 else 'noche'
        request_id = conn.execute('''INSERT INTO lab_requests
            (request_key,model_id,source,seed,day,hour,zone,weekday,weekend,period,
             static_estimate,ml_estimate,simulated_count)
            VALUES(?,?,'synthetic',?,?,?,?,?,?,?,?,?,?)''',
            (key, row['id'], seed, day, hour, zone, weekday, int(weekday >= 6), period,
             fixed, ml, simulated)).lastrowid
        return dict(conn.execute('SELECT * FROM lab_requests WHERE id=?', (request_id,)).fetchone())


def status():
    with connect(readonly=True) as conn:
        model = conn.execute('SELECT id,created_at,metrics FROM lab_models ORDER BY id DESC LIMIT 1').fetchone()
        count = conn.execute('SELECT count(*) FROM lab_requests').fetchone()[0]
        recent = rows(conn, 'SELECT * FROM lab_requests ORDER BY id DESC LIMIT 30')
    return {'model': {**dict(model), 'metrics': json.loads(model['metrics'])} if model else None,
            'persisted_requests': count, 'recent': recent,
            'synthetic_used_for_training': 0, 'synthetic_accuracy': None}
