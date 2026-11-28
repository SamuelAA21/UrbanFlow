import {api,byId,table,format,stamp} from './common.js';
let lastCount=null,lastTime=null,pending=false,connected=false;
let lastSeen=null;
const zones=new Map();
api('geography').then(g=>{for(const f of g.features)zones.set(f.properties.zona_id,f.properties.zona);}).catch(()=>{});
const zone=id=>zones.has(id)?`${zones.get(id)} (${id})`:id;
const labels={requested:'Solicitado',completed:'Completado',cancelled:'Cancelado'};
const decimal=n=>Number(n).toLocaleString('es-CO',{maximumFractionDigits:2});
async function action(path,body){
  if(pending)return;
  pending=true;
  try{
    const result=await api('city/'+path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    byId('city-message').textContent=result.id?`Solicitud guardada. ID #${result.id}.`:(result.message||'Acción registrada por el servidor.');
  }catch(e){byId('city-message').textContent=e.message;}
  finally{pending=false;}
}
byId('city-form').onsubmit=e=>{e.preventDefault();action('control',{action:'start',rate:Number(byId('rate').value),max_requests:Number(byId('maximum').value),clock_speed:Number(byId('speed').value),interval_minutes:Number(byId('interval').value)});};
byId('seed').onclick=()=>action('jobs',{action:'city_seed'});
byId('stop-city').onclick=()=>action('control',{action:'stop'});
byId('run-city').onclick=()=>action('jobs',{action:'run_now'});
byId('expand-history').onclick=()=>action('jobs',{action:'history',months:byId('history-months').value.trim()});
byId('benchmark').onclick=()=>action('jobs',{action:'benchmark',months:byId('history-months').value.trim()});
async function refresh(){
  try{
    const data=await api('city'),now=performance.now();
    if(!connected){byId('city-message').textContent=data.engine_enabled?'Backend AWS conectado. El generador y el archivado se ejecutan en el servidor.':'Modo local de consulta. La generación requiere el motor AWS.';connected=true;}
    const total=data.counts.reduce((s,c)=>s+c.n,0),count=kind=>data.counts.filter(c=>c.kind===kind).reduce((s,c)=>s+c.n,0);
    if(lastCount!==null)byId('city-rate').textContent=decimal((total-lastCount)*1000/(now-lastTime));
    lastCount=total;lastTime=now;
    byId('city-total').textContent=format(total);byId('city-requests').textContent=format(count('requested'));byId('city-completed').textContent=format(count('completed'));
    const states={running:'Creando solicitudes',draining:'Terminando viajes pendientes',completed:'Sesión terminada'};
    byId('city-state').textContent=data.session?`${states[data.session.state]||data.session.state}: ${format(data.session.generated)} / ${format(data.session.max_requests)} solicitudes.${data.session.error?' Error del motor: '+data.session.error:''}`:'Sin sesión. Prepara la semilla y activa el generador en AWS.';
    byId('city-storage').textContent=`${format(data.hdfs_batches)} lotes HDFS · ${decimal(data.hdfs_logical_bytes/1e9)} GB lógicos · ${format(data.unarchived_events)} eventos pendientes de archivar.`;
    if(data.session){const d=new Date(data.session.clock_start+'Z');d.setTime(d.getTime()+data.session.clock_seconds*1000);byId('city-clock').textContent=`Reloj simulado: ${d.toISOString().slice(0,19).replace('T',' ')}. La columna de recepción muestra cuándo llegó realmente al backend.`;}
    table('city-feed',data.recent.map(e=>[e.id,e.trip_id,labels[e.kind],`${zone(e.origin)} → ${zone(e.destination)}`,e.event_time.replace('T',' '),stamp(e.received_at),e.source==='simulated'?'Simulado':'Reporte de conductor']),7);
    if(lastSeen!==null)Array.from(byId('city-feed').rows).forEach((row,i)=>{if(data.recent[i]?.id>lastSeen)row.classList.add('new-arrival');});
    if(data.recent.length)lastSeen=Math.max(...data.recent.map(e=>e.id));
    table('city-runs',data.runs.map(r=>[`${r.job_id} / ${r.yarn_id||'pendiente'}`,r.job_status,r.metrics?format(r.metrics.new_event_rows):'—',r.metrics?Object.entries(r.metrics.counts).filter(([k])=>k.endsWith(':completed')).map(([k,v])=>`${k}: ${format(v)}`).join('; '):'—',r.metrics?Object.keys(r.metrics.training).join(', '):'—']),5);
    table('city-forecast',data.forecasts.map(f=>[f.job_id,f.day,f.hour,f.zone,decimal(f.estimate),f.source]),6);
    byId('city-forecast-total').textContent=(data.forecast_totals||[]).map(f=>`${f.day}: ${format(Math.round(f.total))} viajes reportados estimados en ${format(f.cells)} celdas (${f.source}).`).join(' ');
    const active=data.runs.find(r=>['queued','running'].includes(r.job_status));byId('city-job').textContent=active?`Trabajo #${active.job_id} · ${active.stage||'En cola'} · ${active.yarn_id||'Esperando YARN'}`:data.engine?.error?`Motor pendiente de revisión: ${data.engine.error}`:data.engine?.enabled?`Motor programado cada ${data.engine.interval_minutes} min; procesa cuando hay eventos nuevos archivados.`:'Sin corrida de ciudad activa.';
    for(const id of ['start-city','stop-city','seed','expand-history','benchmark','run-city'])byId(id).disabled=!data.engine_enabled||pending;
    byId('start-city').disabled ||= ['running','draining'].includes(data.session?.state);
  }catch(e){byId('city-message').textContent=e.message;}
  setTimeout(refresh,1500);
}
async function benchmark(){
  try{const meta=await api('metadata'),b=meta.releases.benchmark?.details;
    if(b)table('benchmark-results',[b.benchmark_1,b.benchmark_3].map(x=>[x.executor_instances,format(x.records),decimal(x.input_bytes/1e9),decimal(x.seconds),format(Math.round(x.rows_per_second))]),5);
    byId('benchmark-status').textContent=b?'Resultados medidos en Spark. Revisa límites de comparación indicados abajo.':'Sin medición publicada.';
  }catch(e){byId('benchmark-status').textContent=e.message;}
  setTimeout(benchmark,10000);
}
refresh();benchmark();
