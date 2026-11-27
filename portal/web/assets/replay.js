import {api, byId, table, stamp, format} from './common.js';
let pending = false;
const stages = {cargar:'Ingesta HDFS', etl:'Limpieza y agregación Spark', perfil:'Perfil estadístico', modelo:'Entrenamiento MLlib', sql:'Corredores Spark SQL'};
const states = {queued:'En cola', running:'Procesando', completed:'Publicado', failed:'Fallido'};
const decimal = v => v == null ? '—' : Number(v).toLocaleString('es-CO', {maximumFractionDigits:2});
async function refresh() {
  const data = await api('replay');
  const s = data.schedule, job = data.jobs[0];
  byId('engine-status').textContent = data.engine_enabled
    ? 'Motor Spark habilitado en el servidor. Actualización del panel cada 3 segundos.'
    : 'El modo local solo permite consultar. Para ejecutar estos lotes se necesita el portal desplegado con Spark/YARN en AWS.';
  byId('replay-enable').disabled = !data.engine_enabled || pending;
  byId('replay-stop').disabled = !data.engine_enabled || !s?.enabled || pending;
  byId('replay-now').disabled = !data.engine_enabled || !s?.enabled || !!s.last_job_id || pending;
  byId('engine-cutoff').textContent = job?.cutoff_day || s?.next_day || '—';
  byId('engine-stage').textContent = job ? `${states[job.status]} · ${stages[job.stage] || 'Sin etapa'}` : 'En espera';
  byId('engine-yarn').textContent = job?.yarn_id || 'Esperando aplicación';
  byId('engine-next').textContent = !s?.enabled ? 'Programación detenida' : s.last_job_id
    ? 'Al terminar + intervalo configurado' : stamp(s.next_run);
  byId('engine-detail').textContent = `${s?.message || 'Sin reproducción programada.'}${job?.error ? ` Error: ${job.error}` : ''}`;
  table('replay-forecasts', data.forecasts.map(f => [`#${f.job_id}`,f.cutoff_day,f.day,format(f.cells),format(Math.round(f.total))]),5);
  table('replay-evaluation', data.evaluation.map(e => [`#${e.job_id}`,e.day,format(e.observed_cells),decimal(e.mae),decimal(e.baseline_mae)]),5,
    'Aún no ha llegado un lote posterior con observaciones reales para comparar.');
}
async function action(body) {
  pending = true;
  try {
    await api('replay', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
    byId('replay-message').textContent = body.action === 'stop' ? 'Programación detenida; el trabajo activo puede terminar.' : 'Solicitud guardada. El servicio del maestro ejecutará el lote.';
  } catch(error) { byId('replay-message').textContent = error.message; }
  finally {
    pending = false;
    try { await refresh(); } catch(error) { byId('engine-status').textContent = error.message; }
  }
}
byId('replay-form').onsubmit = event => {
  event.preventDefault();
  action({action:'start', months:byId('replay-months').value.trim(), start_day:byId('replay-start').value,
    end_day:byId('replay-end').value, interval_minutes:Number(byId('replay-interval').value)});
};
byId('replay-stop').onclick = () => action({action:'stop'});
byId('replay-now').onclick = () => action({action:'run_now'});
api('metadata').then(meta => {
  const months = meta.releases.mobility?.details.etl_resumen.meses || [];
  byId('replay-months').value = months.join(',');
  if (months.length) {
    const last = months.at(-1), [year, month] = last.split('-').map(Number);
    byId('replay-start').value = `${last}-28`;
    byId('replay-end').value = `${last}-${new Date(Date.UTC(year,month,0)).getUTCDate()}`;
  }
}).catch(error => { byId('replay-message').textContent = error.message; });
async function poll() {
  try { await refresh(); } catch(error) { byId('engine-status').textContent = error.message; }
  setTimeout(poll,3000);
}
poll();
