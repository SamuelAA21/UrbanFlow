import {api, byId, table} from './common.js';
let running = false, sent = 0, limit = 0, generation = 0;
const decimal = value => value == null ? '—' : Number(value).toLocaleString('es-CO', {maximumFractionDigits: 3});
async function refresh() {
  const data = await api('lab');
  byId('count').textContent = `${data.persisted_requests} peticiones guardadas en SQLite`;
  byId('start').disabled = running || !data.model;
  if (data.model) {
    const m = data.model.metrics;
    byId('provenance').textContent = `Versión #${data.model.id} · ${m.algorithm} · SHA-256 de observaciones: ${m.source_sha256}`;
    byId('split').textContent = `Entrenamiento: ${m.training_rows} filas, ${m.training_from} a ${m.training_until}. Prueba: ${m.test_rows} filas, ${m.test_from} a ${m.test_until}. Filas sintéticas para entrenar: ${m.synthetic_training_rows}.`;
    table('metrics', [['Estadístico fijo', m.static], ['Regresión ML', m.ml]].map(([name, s]) => [name, decimal(s.mae), decimal(s.rmse), decimal(s.r2)]), 4);
  }
  table('requests', data.recent.map(r => [`#${r.id} / v${r.model_id}`, `${r.day} ${r.hour}:00`, r.zone,
    `Día ${r.weekday} · ${r.weekend ? 'fin de semana' : 'entre semana'} · ${r.period}`,
    decimal(r.static_estimate), decimal(r.ml_estimate), `${r.simulated_count} · simulado`, 'Sin observar']), 8);
}
async function tick(token) {
  if (!running || token !== generation) return;
  try {
    const result = await api('lab/requests', {method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({request_key: crypto.randomUUID(), seed: Math.floor(Math.random() * 2147483647)})});
    if (token !== generation) return;
    sent++;
    byId('progress').textContent = `${sent}/${limit} · HTTP 201 · petición #${result.id} persistida · origen: ${result.source}`;
    await refresh();
  } catch (error) {
    byId('progress').textContent = error.message;
    running = false;
  }
  if (token !== generation) return;
  if (sent >= limit) running = false;
  byId('stop').disabled = !running;
  byId('start').disabled = running;
  byId('train').disabled = running;
  if (running) setTimeout(() => tick(token), 1000);
}
byId('train').onclick = async () => {
  byId('train').disabled = true;
  byId('message').textContent = 'Entrenando con observaciones históricas. Esto puede tardar unos segundos…';
  try {
    await api('lab/train', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}'});
    await refresh();
    byId('message').textContent = 'Modelo y métricas guardados. Puedes iniciar el generador HTTP.';
  } catch (error) { byId('message').textContent = error.message; }
  finally { byId('train').disabled = false; }
};
byId('start').onclick = () => {
  limit = Number(byId('limit').value);
  if (!Number.isInteger(limit) || limit < 1 || limit > 500) {
    byId('progress').textContent = 'Selecciona entre 1 y 500 peticiones.'; return;
  }
  running = true; sent = 0;
  byId('start').disabled = true; byId('train').disabled = true; byId('stop').disabled = false;
  tick(++generation);
};
byId('stop').onclick = () => {
  running = false; generation++;
  byId('stop').disabled = true; byId('start').disabled = false; byId('train').disabled = false;
  byId('progress').textContent = 'Generador detenido. Una petición ya enviada puede terminar de guardarse.';
  refresh().catch(error => { byId('message').textContent = error.message; });
};
refresh().then(() => { byId('message').textContent = 'Backend conectado. Datos leídos de SQLite.'; })
  .catch(error => { byId('message').textContent = error.message; });
