import {api, byId, option, table, hourLabel, format, stamp} from "./common.js";

const zone = byId("zone"), weekday = byId("weekday"), hour = byId("hour"), day = byId("day");
const canvas = byId("map");
let map = [], names = new Map(), ranking = [], lastRequest = 0, lastPrediction = 0;

function draw() {
  const box = canvas.getBoundingClientRect();
  if (!box.width || !map.length) return;
  const ratio = window.devicePixelRatio || 1;
  canvas.width = box.width * ratio; canvas.height = box.height * ratio;
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  const width = box.width, height = box.height;
  const scale = Math.min((width - 28) / .61, (height - 28) / (.48 * 1.3));
  const project = ([lon, lat]) => [width / 2 + (lon + 73.93) * scale, height / 2 - (lat - 40.70) * scale * 1.3];
  const counts = new Map(ranking.map(row => [row.zone, row.average]));
  const max = Math.max(1, ...counts.values());
  for (const feature of map) {
    const {type, coordinates} = feature.geometry;
    const id = feature.properties.zona_id;
    const selected = id === Number(zone.value);
    const intensity = Math.sqrt((counts.get(id) || 0) / max);
    ctx.beginPath();
    for (const polygon of (type === "MultiPolygon" ? coordinates : [coordinates])) {
      for (const ring of polygon) {
        ring.forEach((point, i) => i ? ctx.lineTo(...project(point)) : ctx.moveTo(...project(point)));
        ctx.closePath();
      }
    }
    ctx.fillStyle = "hsl(" + (52 - 27 * intensity) + " " + (25 + 55 * intensity) + "% " + (94 - 63 * intensity) + "%)";
    ctx.fill("evenodd");
    ctx.strokeStyle = selected ? "#063840" : "#fff"; ctx.lineWidth = selected ? 2 : .55; ctx.stroke();
  }
}

async function profiles() {
  const request = ++lastRequest;
  try {
    const data = await api("profile?" + new URLSearchParams({zone:zone.value, weekday:weekday.value, hour:hour.value}));
    if (request !== lastRequest) return;
    ranking = data.ranking;
    table("ranking", ranking.slice(0, 10).map(row => [names.get(row.zone) || row.zone, format(row.average)]), 2);
    const hist = byId("histogram");
    hist.replaceChildren();
    byId("zone-title").textContent = "Perfil por hora · " + (names.get(Number(zone.value)) || zone.value);
    byId("notice").textContent = ranking.length ? "Promedio calculado sobre " + ranking[0].days + " días comparables." : "Aún no hay un perfil publicado.";
    const max = Math.max(1, ...data.hourly.map(row => row.average));
    for (const row of data.hourly) {
      const bar = document.createElement("div");
      bar.className = "bar" + (row.hour === Number(hour.value) ? " selected" : "");
      bar.title = hourLabel(row.hour) + " · " + format(row.average) + " recogidas";
      const fill = document.createElement("i");
      fill.style.setProperty("--height", 90 * row.average / max + "px");
      const label = document.createElement("span"); label.textContent = String(row.hour).padStart(2, "0");
      bar.append(fill, label); hist.append(bar);
    }
    draw();
  } catch (error) {
    if (request === lastRequest) byId("notice").textContent = error.message;
  }
}

async function predictions() {
  const request = ++lastPrediction;
  if (!day.value) {
    table("prediction", [], 4, "Selecciona una fecha con predicciones publicadas.");
    return;
  }
  try {
    const rows = await api("predictions?" + new URLSearchParams({zone:zone.value, day:day.value}));
    if (request === lastPrediction) table("prediction", rows.map(row => [hourLabel(row.hour),
      format(row.observed), format(row.estimate), format(row.baseline)]), 4, "Sin estimación para esta fecha y zona.");
  } catch (error) {
    if (request === lastPrediction) table("prediction", [], 4, error.message);
  }
}

async function releases(initial = false) {
  const meta = await api("metadata");
  const mobility = meta.releases.mobility;
  byId("release").textContent = mobility
    ? mobility.details.etl_resumen.meses.join(" / ") + " · " + format(mobility.details.etl_resumen.registros_validos) + " viajes válidos"
    : "La preparación aún no ha publicado datos.";
  if (mobility && !day.value) {
    day.min = meta.first_prediction; day.max = meta.last_prediction; day.value = meta.first_prediction;
    const model = mobility.details.metricas_modelo;
    byId("model-note").textContent = model.mejor_modelo + " · Prueba del " + model.prueba_desde + " al " +
      model.prueba_hasta + ". El " + model.fecha_pronostico + " corresponde al día siguiente al corte.";
    if (!initial) { profiles(); predictions(); }
  }
  const release = meta.releases.corridors;
  byId("corridor-release").textContent = release ? release.details.months.join(", ") + " · Publicado " + stamp(release.published_at) + " · Trabajo " + release.job_id : "Sin publicación.";
  table("corridors", (await api("corridors")).map(row => [row.origin,row.destination,format(row.trips),format(row.minutes)]), 4);
}

async function start() {
  for (let h = 0; h < 24; h++) hour.add(option(h, hourLabel(h)));
  hour.value = "18";
  map = (await api("geography")).features;
  const zones = map.map(feature => feature.properties).sort((a,b) => a.zona.localeCompare(b.zona));
  for (const item of zones) {
    names.set(item.zona_id, item.zona);
    zone.add(option(item.zona_id, item.zona + " · " + item.borough));
  }
  zone.value = "132";
  new ResizeObserver(draw).observe(canvas);
  byId("filters").addEventListener("submit", event => event.preventDefault());
  byId("filters").addEventListener("change", event => { profiles(); if (event.target === zone) predictions(); });
  day.addEventListener("change", predictions);
  await releases(true);
  await Promise.all([profiles(), predictions()]);
  const poll = async () => {
    try { await releases(); }
    catch (error) { byId("release").textContent = error.message; }
    setTimeout(poll, 8000);
  };
  setTimeout(poll, 8000);
}
start().catch(error => { byId("notice").textContent = error.message; });
