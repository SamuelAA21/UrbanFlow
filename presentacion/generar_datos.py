"""Genera presentacion/datos.js a partir de resultados/ (solo biblioteca estándar).

La presentación lee window.UF desde datos.js; así funciona abriendo index.html
directamente (file://) y siempre muestra las cifras de la última ejecución real.
"""
import csv
import json
from collections import defaultdict
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
RES = RAIZ / "resultados"


def leer_json(nombre):
    ruta = RES / nombre
    return json.loads(ruta.read_text(encoding="utf-8-sig")) if ruta.exists() else None


def leer_csv(nombre, campos=None):
    ruta = RES / nombre
    if not ruta.exists():
        return []
    with open(ruta, encoding="utf-8-sig", newline="") as f:
        if campos:
            return list(csv.DictReader(f, fieldnames=campos))
        return list(csv.DictReader(f))


def main():
    uf = {"kpis": leer_json("kpis.json"), "etl": leer_json("etl_resumen.json"),
          "modelo": leer_json("metricas_modelo.json"), "rdd": leer_json("comparacion_rdd_df.json"),
          "streaming": leer_json("streaming_progreso.json"), "ejecucion": leer_json("ejecucion.json")}

    # Matriz día x hora (1 = domingo ... 7 = sábado) con viajes promedio.
    heat = [[0] * 24 for _ in range(7)]
    for r in leer_csv("heatmap_dia_hora.csv"):
        heat[int(r["dia_semana"]) - 1][int(r["hora"])] = float(r["viajes_promedio"])
    uf["heatmap"] = heat

    uf["viernes"] = [{"zona": r["zona"], "borough": r["borough"], "valor": float(r["promedio_por_viernes"])}
                     for r in leer_csv("viernes_noche_top.csv")[:8]]
    uf["zonas_top"] = [{"zona": r["zona"], "borough": r["borough"], "valor": float(r["viajes_promedio_dia"])}
                       for r in leer_csv("zonas_total.csv")[:8]]
    uf["calidad"] = leer_csv("calidad_datos.csv")
    uf["tiempos"] = [{"paso": r["paso"], "segundos": float(r["segundos"])}
                     for r in leer_csv("tiempos_pipeline.csv", ["paso", "segundos"])]
    uf["importancia"] = [{"variable": r["variable"], "valor": float(r["importancia"])}
                         for r in leer_csv("importancia_variables.csv")]

    # Serie de toda la ciudad en el periodo de prueba: real vs predicho por hora.
    serie = defaultdict(lambda: [0.0, 0.0, 0.0])
    for r in leer_csv("predicciones_prueba.csv"):
        k = "{} {:02d}".format(r["fecha"], int(r["hora"]))
        serie[k][0] += float(r["viajes"])
        serie[k][1] += float(r.get("pred_modelo") or r["pred_rf"])
        serie[k][2] += float(r["pred_perfil"])
    claves = sorted(serie)
    uf["serie_ciudad"] = {"t": claves, "real": [round(serie[k][0]) for k in claves],
                          "rf": [round(serie[k][1]) for k in claves], "perfil": [round(serie[k][2]) for k in claves]}

    manana = leer_csv("prediccion_manana.csv")
    uf["manana"] = {}
    for r in manana:
        uf["manana"].setdefault(r["zona"], [0.0] * 24)[int(r["hora"])] = float(r["viajes_esperados"])
    uf["fecha_manana"] = manana[0]["fecha"] if manana else None

    sql = leer_json("spark_sql.json")
    if sql:
        uf["sql"] = {"vs": sql.get("sql_vs_dataframe"),
                     "ventana": sql["consultas"].get("top_zonas_por_hora", {}).get("sql")}
    uf["sql_lider"] = [{"hora": int(r["hora"]), "zona": r["zona"], "borough": r["borough"], "viajes": int(r["viajes"])}
                       for r in leer_csv("sql_top_zonas_por_hora.csv") if r["puesto"] == "1"]
    uf["sql_franjas"] = [{"franja": r["franja"][2:], "tipo_dia": r["tipo_dia"], "mph": float(r["velocidad_mph"])}
                         for r in leer_csv("sql_franjas.csv")]
    uf["grafo"] = leer_json("grafo_resumen.json")
    uf["grafo_top"] = [{"zona": r["zona"], "borough": r["borough"], "valor": float(r["pagerank"])}
                       for r in leer_csv("grafo_zonas.csv")[:8] if r["pagerank"]]
    uf["grafo_ida_vuelta"] = leer_csv("grafo_ida_vuelta.csv")[:3]

    plan = RES / "plan_catalyst.txt"
    if plan.exists():
        texto = plan.read_text(encoding="utf-8")
        partes = {}
        for titulo in ("Analyzed Logical Plan", "Optimized Logical Plan"):
            marca = "== {} ==".format(titulo)
            if marca in texto:
                partes[titulo] = texto.split(marca)[1].split("\n== ")[0].strip()
        uf["plan"] = partes

    salida = Path(__file__).resolve().parent / "datos.js"
    salida.write_text("window.UF = " + json.dumps(uf, ensure_ascii=False, default=str) + ";\n", encoding="utf-8")
    print("datos.js generado ({} KB)".format(salida.stat().st_size // 1024))


if __name__ == "__main__":
    main()
