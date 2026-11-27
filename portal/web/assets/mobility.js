import {api, byId, option, table, hourLabel, format, stamp} from "./common.js";

const zone = byId("zone"), weekday = byId("weekday"), hour = byId("hour"), day = byId("day");
const canvas = byId("map"), tooltip = byId("map-tooltip");
let map = [], names = new Map(), boroughs = new Map(), ranking = [], hitAreas = [];
let hoveredZone = null, lastRequest = 0, lastPrediction = 0, publishedMonths = [], mapExtent = null;

const valueFor = id => ranking.find(row => row.zone === Number(id));
const zoneName = id => names.get(Number(id)) || `Zona ${id}`;
const zoneBorough = id => boroughs.get(Number(id)) || "Borough no disponible";

function pointInRing(x, y, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i], [xj, yj] = ring[j];
    if (((yi > y) !== (yj > y)) && x < (xj - xi) * (y - yi) / ((yj - yi) || Number.EPSILON) + xi) inside = !inside;
  }
  return inside;
}

function contains(area, x, y) {
  return area.polygons.some(rings => pointInRing(x, y, rings[0]) && !rings.slice(1).some(ring => pointInRing(x, y, ring)));
}

function bounds() {
  if (mapExtent) return mapExtent;
  const points = [];
  const collect = value => typeof value[0] === "number" ? points.push(value) : value.forEach(collect);
  map.forEach(feature => collect(feature.geometry.coordinates));
  mapExtent = {
    minX: Math.min(...points.map(point => point[0])), maxX: Math.max(...points.map(point => point[0])),
    minY: Math.min(...points.map(point => point[1])), maxY: Math.max(...points.map(point => point[1]))
  };
  return mapExtent;
}

function draw() {
  const box = canvas.getBoundingClientRect();
  if (!box.width || !map.length) return;
  const ratio = window.devicePixelRatio || 1;
  canvas.width = Math.round(box.width * ratio); canvas.height = Math.round(box.height * ratio);
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  const width = box.width, height = box.height, padding = 22, extent = bounds();
  const xScale = (width - padding * 2) / (extent.maxX - extent.minX);
  const yScale = (height - padding * 2) / (extent.maxY - extent.minY);
  const scale = Math.min(xScale, yScale);
  const offsetX = (width - (extent.maxX - extent.minX) * scale) / 2;
  const offsetY = (height - (extent.maxY - extent.minY) * scale) / 2;
  const project = ([lon, lat]) => [offsetX + (lon - extent.minX) * scale, height - offsetY - (lat - extent.minY) * scale];
  const counts = new Map(ranking.map(row => [row.zone, row.average]));
  const max = Math.max(1, ...counts.values());
  hitAreas = [];

  for (const feature of map) {
    const {type, coordinates} = feature.geometry;
    const id = feature.properties.zona_id;
    const selected = id === Number(zone.value), hovered = id === hoveredZone;
    const intensity = Math.sqrt((counts.get(id) || 0) / max);
    const polygons = (type === "MultiPolygon" ? coordinates : [coordinates])
      .map(polygon => polygon.map(ring => ring.map(project)));
    hitAreas.push({id, polygons});
    ctx.beginPath();
    for (const polygon of polygons) {
      for (const ring of polygon) {
        ring.forEach((point, index) => index ? ctx.lineTo(...point) : ctx.moveTo(...point));
        ctx.closePath();
      }
    }
    ctx.fillStyle = `hsl(${194 - 18 * intensity} ${22 + 46 * intensity}% ${95 - 54 * intensity}%)`;
    ctx.fill("evenodd");
    ctx.strokeStyle = selected ? "#F2B134" : hovered ? "#19A7A0" : "rgba(255,255,255,.9)";
    ctx.lineWidth = selected ? 3 : hovered ? 2 : .7;
    ctx.stroke();
  }
}

function areaAt(event) {
  const rect = canvas.getBoundingClientRect(), x = event.clientX - rect.left, y = event.clientY - rect.top;
  return {area: [...hitAreas].reverse().find(item => contains(item, x, y)), x, y};
}

function showTooltip(event) {
  const {area, x, y} = areaAt(event);
  const next = area?.id ?? null;
  if (next !== hoveredZone) { hoveredZone = next; draw(); }
  if (!area) { tooltip.hidden = true; canvas.style.cursor = "default"; return; }
  const value = valueFor(area.id);
  tooltip.replaceChildren();
  const strong = document.createElement("strong"); strong.textContent = zoneName(area.id);
  const detail = document.createElement("span");
  detail.textContent = `${zoneBorough(area.id)} · ${value ? `${format(value.average)} recogidas/hora` : "Sin actividad publicada"}`;
  tooltip.append(strong, detail); tooltip.hidden = false;
  const mapBox = canvas.parentElement.getBoundingClientRect();
  tooltip.style.left = `${Math.min(x + 14, mapBox.width - tooltip.offsetWidth - 10)}px`;
  tooltip.style.top = `${Math.max(10, y - tooltip.offsetHeight - 10)}px`;
  canvas.style.cursor = "pointer";
}

function chooseZone(id) {
  if (!id) return;
  zone.value = String(id);
  hoveredZone = null; tooltip.hidden = true;
  profiles(); predictions();
}

function renderRanking() {
  const body = byId("ranking"); body.replaceChildren();
  ranking.slice(0, 10).forEach((row, index) => {
    const tr = body.insertRow();
    if (row.zone === Number(zone.value)) tr.className = "current";
    tr.insertCell().textContent = String(index + 1);
    const nameCell = tr.insertCell(), button = document.createElement("button");
    button.type = "button"; button.className = "zone-link"; button.textContent = zoneName(row.zone);
    button.setAttribute("aria-label", `Analizar ${zoneName(row.zone)}, puesto ${index + 1}`);
    button.addEventListener("click", () => chooseZone(row.zone)); nameCell.append(button);
    tr.insertCell().textContent = zoneBorough(row.zone);
    tr.insertCell().textContent = format(row.average);
  });
}

function updateBrief(hourly) {
  const id = Number(zone.value), selected = valueFor(id), rank = ranking.findIndex(row => row.zone === id) + 1;
  const peak = hourly.reduce((best, row) => !best || row.average > best.average ? row : best, null);
  const leader = ranking[0];
  byId("zone-name").textContent = zoneName(id);
  byId("zone-borough").textContent = zoneBorough(id);
  byId("zone-average").textContent = selected ? format(selected.average) : "0";
  byId("zone-rank").textContent = rank ? `${rank} de ${ranking.length}` : "Sin posición";
  byId("zone-peak").textContent = peak ? `${hourLabel(peak.hour)} · ${format(peak.average)}/h` : "—";
  byId("city-leader").textContent = leader ? `${zoneName(leader.zone)} · ${format(leader.average)}/h` : "—";
  const ratio = selected && leader?.average ? selected.average / leader.average : 0;
  const level = ratio >= .75 ? "actividad muy alta" : ratio >= .35 ? "actividad media" : ratio > 0 ? "actividad baja" : "sin actividad registrada";
  byId("zone-reading").textContent = `${zoneName(id)} presenta ${level} para ${weekday.selectedOptions[0].text.toLowerCase()} a las ${hourLabel(hour.value)}. ` +
    `${peak ? `Su pico diario aparece a las ${hourLabel(peak.hour)}. ` : ""}Esta lectura describe recogidas observadas; no mide cuántos taxis compiten por ellas.`;
}

async function profiles() {
  const request = ++lastRequest;
  try {
    const data = await api("profile?" + new URLSearchParams({zone:zone.value, weekday:weekday.value, hour:hour.value}));
    if (request !== lastRequest) return;
    ranking = data.ranking;
    renderRanking(); updateBrief(data.hourly);
    const hist = byId("histogram"); hist.replaceChildren();
    byId("zone-title").textContent = `Actividad durante el día · ${zoneName(zone.value)}`;
    const days = ranking[0]?.days || data.hourly[0]?.days;
    byId("notice").textContent = days
      ? `Promedio de ${format(days)} días comparables${publishedMonths.length ? ` en ${publishedMonths.join(", ")}` : ""}; no es información en tiempo real.`
      : "Aún no hay un perfil publicado para este filtro.";
    const max = Math.max(1, ...data.hourly.map(row => row.average));
    for (const row of data.hourly) {
      const bar = document.createElement("div");
      bar.className = "bar" + (row.hour === Number(hour.value) ? " selected" : "");
      bar.title = `${hourLabel(row.hour)} · ${format(row.average)} recogidas por hora`;
      bar.setAttribute("aria-label", bar.title);
      const value = document.createElement("em"); value.textContent = format(row.average);
      const fill = document.createElement("i"); fill.style.setProperty("--height", `${105 * row.average / max}px`);
      const label = document.createElement("span"); label.textContent = String(row.hour).padStart(2, "0");
      bar.append(value, fill, label); hist.append(bar);
    }
    draw();
  } catch (error) {
    if (request === lastRequest) byId("notice").textContent = error.message;
  }
}

function predictionSummary(rows) {
  if (!rows.length) return "No hay estimaciones para esta fecha y zona.";
  const estimate = rows.reduce((sum, row) => sum + Number(row.estimate || 0), 0);
  const observedRows = rows.filter(row => row.observed != null);
  const peak = rows.reduce((best, row) => !best || row.estimate > best.estimate ? row : best, null);
  if (!observedRows.length) return `Pronóstico diario: ${format(Math.round(estimate))} recogidas, con el mayor nivel a las ${hourLabel(peak.hour)}. Aún no existe un valor observado para comparar.`;
  const observed = observedRows.reduce((sum, row) => sum + Number(row.observed), 0);
  const difference = estimate - observed, percent = observed ? Math.abs(difference) * 100 / observed : 0;
  return `El modelo estimó ${format(Math.round(estimate))} recogidas y se observaron ${format(Math.round(observed))}: ` +
    `${difference >= 0 ? "sobreestimó" : "subestimó"} por ${format(Math.round(Math.abs(difference)))} (${percent.toLocaleString("es-CO", {maximumFractionDigits:1})} %). El pico estimado fue a las ${hourLabel(peak.hour)}.`;
}

async function predictions() {
  const request = ++lastPrediction;
  if (!day.value) {
    table("prediction", [], 5, "Selecciona una fecha con predicciones publicadas.");
    byId("prediction-summary").textContent = "Selecciona una fecha para interpretar el resultado.";
    return;
  }
  try {
    const rows = await api("predictions?" + new URLSearchParams({zone:zone.value, day:day.value}));
    if (request !== lastPrediction) return;
    table("prediction", rows.map(row => {
      const difference = row.observed == null ? null : Number(row.estimate) - Number(row.observed);
      const differenceLabel = difference == null ? "—" : `${difference > 0 ? "+" : ""}${format(difference)}`;
      return [hourLabel(row.hour),format(row.observed),format(row.estimate),differenceLabel,format(row.baseline)];
    }), 5, "Sin estimación para esta fecha y zona.");
    byId("prediction-summary").textContent = predictionSummary(rows);
  } catch (error) {
    if (request === lastPrediction) {
      table("prediction", [], 5, error.message); byId("prediction-summary").textContent = error.message;
    }
  }
}

async function releases(initial = false) {
  const meta = await api("metadata"), mobility = meta.releases.mobility;
  if (mobility) {
    const etl = mobility.details.etl_resumen;
    publishedMonths = etl.meses;
    byId("release").textContent = `Datos publicados: ${etl.meses.join(" / ")} · ${format(etl.registros_validos)} viajes válidos`;
    byId("dataset-summary").textContent = `${format(etl.registros_crudos)} registros revisados; ${etl.porcentaje_descartado.toLocaleString("es-CO")} % se descartó por reglas de calidad.`;
  } else {
    byId("release").textContent = "La preparación aún no ha publicado datos.";
  }
  const publication = mobility ? `${mobility.job_id}:${mobility.published_at}` : '';
  if (mobility && day.dataset.publication !== publication) {
    const followLatest = !day.value || day.value === day.max;
    day.min = meta.first_prediction; day.max = meta.last_prediction;
    if (followLatest || day.value < day.min || day.value > day.max) day.value = meta.last_prediction;
    day.dataset.publication = publication;
    const model = mobility.details.metricas_modelo;
    byId("model-note").textContent = `${model.mejor_modelo}. Evaluación del ${model.prueba_desde} al ${model.prueba_hasta}; ${model.fecha_pronostico} es un pronóstico fuera de muestra.`;
    if (mobility.details.replay) byId('model-note').textContent += ` Reproducción histórica: corte ${mobility.details.replay.cutoff_day}.`;
    if (!initial) { profiles(); predictions(); }
  }
  const release = meta.releases.corridors;
  byId("corridor-release").textContent = release ? `${release.details.months.join(", ")} · Publicado ${stamp(release.published_at)}` : "Sin publicación.";
  const corridors = await api("corridors");
  table("corridors", corridors.map(row => [row.origin,row.destination,format(row.trips),`${format(row.minutes)} min`]), 4);
  const top = corridors[0];
  byId("corridor-reading").textContent = top
    ? `El corredor con más viajes fue ${top.origin} → ${top.destination}: ${format(top.trips)} viajes y ${format(top.minutes)} minutos en promedio.`
    : "No hay corredores publicados para este periodo.";
}

async function start() {
  for (let h = 0; h < 24; h++) hour.add(option(h, hourLabel(h)));
  hour.value = "18";
  map = (await api("geography")).features;
  const zones = map.map(feature => feature.properties).sort((a,b) => a.zona.localeCompare(b.zona));
  for (const item of zones) {
    names.set(item.zona_id, item.zona); boroughs.set(item.zona_id, item.borough);
    zone.add(option(item.zona_id, `${item.zona} · ${item.borough}`));
  }
  zone.value = "132";
  new ResizeObserver(draw).observe(canvas);
  canvas.addEventListener("mousemove", showTooltip);
  canvas.addEventListener("mouseleave", () => { hoveredZone = null; tooltip.hidden = true; canvas.style.cursor = "default"; draw(); });
  canvas.addEventListener("click", event => chooseZone(areaAt(event).area?.id));
  byId("filters").addEventListener("submit", event => event.preventDefault());
  byId("filters").addEventListener("change", event => { profiles(); if (event.target === zone) predictions(); });
  day.addEventListener("change", predictions);
  await releases(true);
  await Promise.all([profiles(), predictions()]);
  const poll = async () => {
    try { await releases(); }
    catch (error) { byId("release").textContent = error.message; }
    setTimeout(poll, 15000);
  };
  setTimeout(poll, 15000);
}
start().catch(error => { byId("notice").textContent = error.message; });
