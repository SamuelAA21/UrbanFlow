import {api, byId, option, table, format, stamp} from "./common.js";
const months = byId("month"), submit = byId("submit");
const labels = {queued:"En cola",running:"En ejecución",completed:"Completado",failed:"Fallido"};
const steps = {cargar:"Ingesta",etl:"Limpieza",perfil:"Perfil horario",modelo:"MLlib",sql:"Spark SQL"};
let busy = true, pending = false;

function duration(start, finish) {
  const millis = value => Date.parse(value.replace(" ", "T") + "Z");
  return start ? Math.max(0, Math.floor(((finish ? millis(finish) : Date.now()) - millis(start)) / 1000)) + " s" : "";
}

function renderJobs(items) {
  const root = byId("jobs");
  root.replaceChildren();
  if (!items.length) root.textContent = "Aún no hay trabajos.";
  for (const job of items) {
    const section = document.createElement("div"); section.className = "job";
    const title = document.createElement("strong");
    title.className = job.status;
    title.textContent = "#" + job.id + " · " + (job.kind === "prepare" ? "Preparación" : "Corredores") +
      " · " + labels[job.status] + " " + duration(job.started_at, job.ended_at);
    const detail = document.createElement("p"); detail.className = "muted";
    detail.textContent = job.months + " · Solicitado " + stamp(job.created_at) + (job.error ? " · " + job.error : "");
    const list = document.createElement("ol");
    for (const stage of job.stages) {
      const item = document.createElement("li");
      item.textContent = steps[stage.name] + " · " + labels[stage.status] + " " + duration(stage.started_at, stage.ended_at) +
        (stage.yarn_id ? " · " + stage.yarn_id : "");
      list.append(item);
    }
    section.append(title, detail); if (list.children.length) section.append(list); root.append(section);
  }
}

async function refresh() {
  try {
    const [cluster, jobs, meta] = await Promise.all([api("cluster"), api("jobs"), api("metadata")]);
    busy = jobs.some(job => ["queued","running"].includes(job.status));
    const available = meta.releases.mobility?.details.etl_resumen.meses ?? [];
    if (Array.from(months.options, item => item.value).join() !== available.join()) {
      const current = months.value;
      months.replaceChildren(...available.map(value => option(value,value)));
      months.value = available.includes(current) ? current : available.at(-1) || "";
    }
    const nodes = cluster.nodes;
    byId("notice").textContent = cluster.available ? nodes.filter(node => node.state === "RUNNING").length +
      " workers activos · " + (busy ? "Hay un trabajo pendiente o en ejecución." : "Sin trabajos pendientes.") : "No se pudo consultar YARN.";
    table("nodes", nodes.map(node => [node.nodeHostName,node.state,node.numContainers,
      format(node.usedMemoryMB) + " MB",format(node.usedVirtualCores)]),5);
    table("apps", cluster.applications.map(job => [job.name + " · " + job.id,
      job.state === "FINISHED" ? job.finalStatus : job.state, format(job.progress) + " %", format(Math.floor(job.elapsedTime / 1000)) + " s"]),4);
    renderJobs(jobs);
  } catch (error) {
    busy = true;
    byId("notice").textContent = error.message;
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
    busy = true;
    byId("message").textContent = "Trabajo #" + result.id + " en cola.";
  } catch (error) {
    byId("message").textContent = error.message;
  } finally {
    pending = false; submit.disabled = busy || !months.value;
  }
});
refresh();
