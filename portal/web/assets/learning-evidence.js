import {byId,format,stamp} from './common.js';

export function renderLearningEvidence(data){
  const e=data.simulation_evidence;
  if(!e){byId('proof-status').textContent='No se pudo consultar la evidencia del modelo.';return;}
  byId('proof-saved').textContent=format(e.saved_completed);
  byId('proof-included').textContent=format(e.included_completed);
  byId('proof-pending').textContent=format(e.pending_completed);
  byId('proof-version').textContent=e.model_job_id?`#${e.model_job_id}`:'Sin entrenar';
  byId('proof-status').textContent=!e.model_job_id?'Todavía no hay un modelo entrenado con viajes simulados. Los viajes guardados están pendientes de incorporarse.':e.pending_completed>0?`El modelo #${e.model_job_id} ya utilizó ${format(e.included_completed)} viajes simulados completados. Quedan ${format(e.pending_completed)} viajes guardados por incorporar.`:`Actualizado: el modelo #${e.model_job_id} ya utilizó todos los ${format(e.included_completed)} viajes simulados completados guardados.`;
  byId('proof-comparison').textContent=e.previous_job_id?`Antes, modelo #${e.previous_job_id}: ${format(e.previous_included_completed)} viajes incluidos → Después, modelo #${e.model_job_id}: ${format(e.included_completed)}. Se incorporaron ${format(e.included_completed-e.previous_included_completed)} viajes completados más.`:e.model_job_id?'Esta es la primera versión entrenada con simulaciones. La próxima corrida permitirá comparar el antes y el después.':'Activa la ciudad y procesa los nuevos lotes para guardar la primera versión.';
  byId('proof-publication').textContent=e.model_job_id?`Entrenamiento terminado: ${stamp(e.trained_at)}. ${e.forecast_cells?`Pronóstico guardado para ${e.forecast_day}: ${format(e.forecast_cells)} combinaciones de zona y hora.`:'Esta corrida no tiene un pronóstico guardado.'}`:'';
}
