import {api, byId, option, table, format, stamp} from "./common.js";

const months = byId("month"), submit = byId("submit");
const labels = {queued:"En cola",running:"En ejecución",completed:"Completado",failed:"Fallido"};
const steps = {cargar:"Ingesta",etl:"Limpieza",perfil:"Perfil horario",modelo:"MLlib",sql:"Spark SQL"};
let busy = true, pending = false;

const decimal = (value, digits = 2) => value == null ? "—" : Number(value).toLocaleString("es-CO", {maximumFractionDigits:digits});
const bytes = value => value == null ? "—" : `${(Number(value) / 1_000_000_000).toLocaleString("es-CO", {minimumFractionDigits:2,maximumFractionDigits:2})} GB`;
const seconds = value => value == null ? "—" : `${decimal(value, 1)} s`;

function duration(start, finish) {
  const millis = value => Date.parse(value.replace(" ", "T") + "Z");
  return start ? `${Math.max(0, Math.floor(((finish ? millis(finish) : Date.now()) - millis(start)) / 1000))} s` : "";
}

function renderData(meta) {
  const mobility = meta.releases.mobility;
  if (!mobility) return;
  const etl = mobility.details.etl_resumen;
  byId("data-period").textContent = `${etl.meses.join(", ")} · Publicado ${stamp(mobility.published_at)}`;
  byId("input-size").textContent = bytes(etl.bytes_crudos);
  byId("replicated-size").textContent = etl.bytes_hdfs_replicados
    ? `${bytes(etl.bytes_hdfs_replicados)} físicos con réplica HDFS ×3`
    : "Parquet comprimido almacenado en HDFS";
  byId("raw-records").textContent = format(etl.registros_crudos);
  byId("valid-records").textContent = format(etl.registros_validos);
  const discarded = etl.registros_crudos - etl.registros_validos;
  byId("discarded-records").textContent = `${format(discarded)} · ${decimal(etl.porcentaje_descartado)} %`;
  byId("zone-hour-rows").textContent = format(etl.filas_zona_hora);
  const quality = etl.registros_crudos ? 100 * etl.registros_validos / etl.registros_crudos : 0;
  byId("quality-fill").style.width = `${quality}%`;
  byId("quality-note").textContent = `${decimal(quality,1)} % de los registros superó las reglas de fecha, distancia, tarifa, duración y zona.`;
}

function renderModel(meta) {
  const model = meta.releases.mobility?.details.metricas_modelo;
  if (!model) return;
  const best = model.metricas[model.mejor_modelo];
  byId("model-name").textContent = model.mejor_modelo;
  byId("model-r2").textContent = decimal(best.r2, 4);
  byId("model-mae").textContent = `${decimal(best.mae, 2)} recogidas/zona-hora`;
  byId("model-rmse").textContent = `${decimal(best.rmse, 2)} recogidas/zona-hora`;
  byId("training-rows").textContent = format(model.filas_entrenamiento);
  byId("training-time").textContent = seconds(model.tiempos_entrenamiento[model.mejor_modelo] ?? model.segundos_entrenamiento);
  byId("model-period").textContent = `Entrena hasta ${model.entrenamiento_hasta}; evalúa ${format(model.filas_prueba)} filas del ${model.prueba_desde} al ${model.prueba_hasta}.`;
  byId("model-reading").textContent = `El modelo explica ${decimal(best.r2 * 100,1)} % de la variación observada. Su error típico absoluto es de ${decimal(best.mae,1)} recogidas por zona y hora; no representa demanda insatisfecha.`;
  table("models", Object.entries(model.metricas).map(([name, metrics]) => [
    name === model.mejor_modelo ? `${name} · publicado` : name,
    decimal(metrics.mae, 3), decimal(metrics.rmse, 3), decimal(metrics.r2, 4)
  ]), 4);
  const variables = byId("variables"); variables.replaceChildren();
  for (const name of model.variables) {
    const item = document.createElement("span"); item.textContent = name.replaceAll("_", " "); variables.append(item);
  }
}

function renderJobs(items) {
  const root = byId("jobs"); root.replaceChildren();
  if (!items.length) root.textContent = "Aún no hay trabajos.";
  for (const job of items) {
    const section = document.createElement("div"); section.className = "job";
    const title = document.createElement("strong"); title.className = job.status;
    title.textContent = `#${job.id} · ${job.kind === "prepare" ? "Preparación completa" : "Cálculo de corredores"} · ${labels[job.status]} ${duration(job.started_at, job.ended_at)}`;
    const detail = document.createElement("p"); detail.className = "muted";
    detail.textContent = `${job.months} · Solicitado ${stamp(job.created_at)}${job.error ? ` · ${job.error}` : ""}`;
    const list = document.createElement("ol");
    for (const stage of job.stages) {
      const item = document.createElement("li");
      item.textContent = `${steps[stage.name]} · ${labels[stage.status]} ${duration(stage.started_at, stage.ended_at)}${stage.yarn_id ? ` · ${stage.yarn_id}` : ""}`;
      list.append(item);
    }
    section.append(title, detail); if (list.children.length) section.append(list); root.append(section);
  }
}

function renderCluster(cluster) {
  const nodes = cluster.nodes;
  const totals = nodes.reduce((sum, node) => ({
    usedMemory: sum.usedMemory + Number(node.usedMemoryMB || 0),
    memory: sum.memory + Number(node.usedMemoryMB || 0) + Number(node.availMemoryMB || 0),
    usedCores: sum.usedCores + Number(node.usedVirtualCores || 0),
    cores: sum.cores + Number(node.usedVirtualCores || 0) + Number(node.availableVirtualCores || 0)
  }), {usedMemory:0,memory:0,usedCores:0,cores:0});
  byId("cluster-summary").textContent = cluster.available
    ? `${nodes.filter(node => node.state === "RUNNING").length} workers activos · ${decimal(totals.memory / 1024,1)} GB YARN · ${format(totals.cores)} vCores`
    : "YARN no está disponible.";
  table("nodes", nodes.map(node => [node.nodeHostName,node.state,node.numContainers,
    `${format(node.usedMemoryMB)} / ${format(Number(node.usedMemoryMB || 0) + Number(node.availMemoryMB || 0))} MB`,
    `${format(node.usedVirtualCores)} / ${format(Number(node.usedVirtualCores || 0) + Number(node.availableVirtualCores || 0))}`]),5);
  const applications = [...cluster.applications].sort((a,b) => b.id.localeCompare(a.id, undefined, {numeric:true}));
  table("apps", applications.map(job => [`${job.name} · ${job.id}`,
    job.state === "FINISHED" ? job.finalStatus : job.state, `${decimal(job.progress,1)} %`, `${format(Math.floor(job.elapsedTime / 1000))} s`]),4);
}

async function refresh() {
  try {
    const [cluster, jobs, meta] = await Promise.all([api("cluster"), api("jobs"), api("metadata")]);
    busy = jobs.some(job => ["queued","running"].includes(job.status));
    const available = meta.releases.mobility?.details.etl_resumen.meses ?? [];
    if (Array.from(months.options, item => item.value).join() !== available.join()) {
      const current = months.value; months.replaceChildren(...available.map(value => option(value,value)));
      months.value = available.includes(current) ? current : available.at(-1) || "";
    }
    const running = cluster.nodes.filter(node => node.state === "RUNNING").length;
    byId("notice").textContent = cluster.available
      ? `${running} workers activos · ${busy ? "Hay un trabajo en ejecución." : "Procesamiento estable y sin trabajos pendientes."}`
      : "Los resultados están disponibles, pero no se pudo consultar YARN.";
    renderData(meta); renderModel(meta); renderCluster(cluster); renderJobs(jobs);
  } catch (error) {
    busy = true; byId("notice").textContent = error.message;
  }
  submit.disabled = busy || pending || !months.value;
  setTimeout(refresh, 4000);
}

byId("run").addEventListener("submit", async event => {
  event.preventDefault();
  if (busy || pending || !months.value) return;
  pending = true; submit.disabled = true;
  try {
    const result = await api("jobs", {method:"POST", headers:{"Content-Type":"application/json"},body:JSON.stringify({month:months.value})});
    busy = true; byId("message").textContent = `Trabajo #${result.id} en cola. El historial se actualizará automáticamente.`;
  } catch (error) {
    byId("message").textContent = error.message;
  } finally {
    pending = false; submit.disabled = busy || !months.value;
  }
});
refresh();
