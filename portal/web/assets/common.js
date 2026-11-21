export const byId = id => document.getElementById(id);
export const format = value => value == null ? "—" : new Intl.NumberFormat("es-CO").format(value);
export const stamp = value => value ? new Date(value.replace(" ", "T") + "Z").toLocaleString("es-CO", {timeZone:"America/Bogota"}) : "—";
export const hourLabel = h => String(h).padStart(2, "0") + ":00";
export async function api(path, options) {
  const response = await fetch("/api/" + path, {cache: "no-store", ...options});
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw Error(body.error || "El servidor no pudo completar la consulta.");
  }
  return response.json();
}
export function option(value, label) {
  const node = document.createElement("option"); node.value = value; node.textContent = label; return node;
}
export function table(id, rows, width, empty = "Sin resultados para esta consulta.") {
  const body = byId(id);
  body.replaceChildren();
  if (!rows.length) {
    const cell = body.insertRow().insertCell(); cell.colSpan = width; cell.textContent = empty; return;
  }
  for (const values of rows) {
    const row = body.insertRow();
    for (const value of values) row.insertCell().textContent = value ?? "—";
  }
}
