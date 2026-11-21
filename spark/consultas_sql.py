"""Paso 2b - Spark SQL: preguntas de negocio escritas en SQL estándar.

1. Catálogo: los Parquet de HDFS se registran como vistas temporales (viajes, zonas).
2. Funciones de ventana: las 3 zonas con más viajes en cada hora (RANK() OVER).
3. JOIN doble: corredores origen -> destino más frecuentes, con nombres de zona.
4. CASE + agregación: viajes, tarifa y velocidad por franja horaria y tipo de día.
5. SQL vs DataFrame: la misma consulta en los dos lenguajes llega al mismo plan
   optimizado de Catalyst; son dos formas de describir el mismo trabajo.
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import HDFS, Cronometro, guardar_csv, guardar_json, guardar_texto, log, plan_catalyst, sesion  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--resultados", required=True)
args = p.parse_args()

spark = sesion("2b Spark SQL")
viajes = spark.read.parquet(HDFS + "/limpio/viajes")
month = os.environ.get("URBANFLOW_SQL_MONTH")
if month:
    viajes = viajes.filter(F.col("mes") == month)
zonas = spark.read.parquet(HDFS + "/referencia/zonas")
viajes.createOrReplaceTempView("viajes")
zonas.createOrReplaceTempView("zonas")
log("Vistas en el catálogo de Spark SQL: " + ", ".join(t.name for t in spark.catalog.listTables()))

CONSULTAS = {
    "top_zonas_por_hora": {
        "titulo": "Las 3 zonas con más viajes en cada hora (función de ventana)",
        "sql": """
WITH por_zona AS (
    SELECT hora, zona_origen, COUNT(*) AS viajes
    FROM viajes
    GROUP BY hora, zona_origen
), ranking AS (
    SELECT hora, zona_origen, viajes,
           RANK() OVER (PARTITION BY hora ORDER BY viajes DESC) AS puesto
    FROM por_zona
)
SELECT r.hora, r.puesto, z.zona, z.borough, r.viajes
FROM ranking r
JOIN zonas z ON z.zona_id = r.zona_origen
WHERE r.puesto <= 3
ORDER BY r.hora, r.puesto""",
    },
    "corredores": {
        "titulo": "Corredores origen -> destino más frecuentes (JOIN doble)",
        "sql": """
SELECT o.zona AS origen, o.borough AS borough_origen,
       d.zona AS destino, d.borough AS borough_destino,
       COUNT(*) AS viajes,
       ROUND(AVG(v.distancia_mi), 2) AS distancia_media_mi,
       ROUND(AVG(v.duracion_min), 1) AS duracion_media_min,
       ROUND(AVG(v.tarifa), 2) AS tarifa_media_usd,
       ROUND(SUM(v.distancia_mi) / (SUM(v.duracion_min) / 60), 1) AS velocidad_mph
FROM viajes v
JOIN zonas o ON o.zona_id = v.zona_origen
JOIN zonas d ON d.zona_id = v.zona_destino
WHERE v.zona_origen <> v.zona_destino
GROUP BY o.zona, o.borough, d.zona, d.borough
ORDER BY viajes DESC
LIMIT 15""",
    },
    "franjas": {
        "titulo": "Viajes, tarifa y velocidad por franja horaria (CASE)",
        "sql": """
SELECT CASE WHEN hora BETWEEN 0 AND 5 THEN '1 Madrugada (0-5 h)'
            WHEN hora BETWEEN 6 AND 9 THEN '2 Mañana (6-9 h)'
            WHEN hora BETWEEN 10 AND 15 THEN '3 Mediodía (10-15 h)'
            WHEN hora BETWEEN 16 AND 19 THEN '4 Tarde (16-19 h)'
            ELSE '5 Noche (20-23 h)' END AS franja,
       CASE WHEN fin_de_semana = 1 THEN 'Fin de semana' ELSE 'Entre semana' END AS tipo_dia,
       COUNT(*) AS viajes,
       ROUND(AVG(tarifa), 2) AS tarifa_media_usd,
       ROUND(AVG(distancia_mi), 2) AS distancia_media_mi,
       ROUND(SUM(distancia_mi) / (SUM(duracion_min) / 60), 1) AS velocidad_mph
FROM viajes
GROUP BY 1, 2
ORDER BY 1, 2""",
    },
}

resumen = {"consultas": {}}
for nombre, c in CONSULTAS.items():
    log("SQL: " + c["titulo"] + "...")
    with Cronometro() as t:
        filas = guardar_csv(args.resultados, "sql_{}.csv".format(nombre), spark.sql(c["sql"]))
    resumen["consultas"][nombre] = {"titulo": c["titulo"], "sql": c["sql"].strip(),
                                    "filas": len(filas), "segundos": t.segundos}
    log("  {} filas en {} s".format(len(filas), t.segundos))

# --- SQL vs DataFrame: el mismo plan -------------------------------------------------
log("SQL vs DataFrame: la misma consulta en los dos lenguajes...")
SQL_EQUIVALENTE = """
SELECT z.borough, v.hora, COUNT(*) AS viajes, AVG(v.tarifa) AS tarifa_media
FROM viajes v
JOIN zonas z ON v.zona_origen = z.zona_id
WHERE v.distancia_mi > 1
GROUP BY z.borough, v.hora"""


def consulta_sql():
    return spark.sql(SQL_EQUIVALENTE)


def consulta_df():
    return (viajes.join(zonas, viajes.zona_origen == zonas.zona_id)
            .where(viajes.distancia_mi > 1)
            .groupBy(zonas.borough, viajes.hora)
            .agg(F.count(F.lit(1)).alias("viajes"), F.avg(viajes.tarifa).alias("tarifa_media")))


con_sql = consulta_sql()
qe_sql, qe_df = con_sql._jdf.queryExecution(), consulta_df()._jdf.queryExecution()
# sameResult compara los planes canónicos (sin los identificadores internos de columnas).
mismo_optimizado = qe_sql.optimizedPlan().sameResult(qe_df.optimizedPlan())


def sin_ids(plan):
    # Quita lo que cambia en cada consulta aunque el plan sea el mismo: ids de columnas y de nodos.
    return re.sub(r"#\d+L?|\[plan_id=\d+\]", "", plan)


mismo_fisico = sin_ids(qe_sql.executedPlan().toString()) == sin_ids(qe_df.executedPlan().toString())
# Se crea una consulta nueva en cada repetición: volver a ejecutar el mismo DataFrame
# reutilizaría los archivos de shuffle ya escritos y mediría casi cero.
tiempos_sql, tiempos_df = [], []
for _ in range(2):
    with Cronometro() as t:
        r_sql = sorted(tuple(f) for f in consulta_sql().collect())
    tiempos_sql.append(t.segundos)
    with Cronometro() as t:
        r_df = sorted(tuple(f) for f in consulta_df().collect())
    tiempos_df.append(t.segundos)
mismo_resultado = len(r_sql) == len(r_df) and all(
    a[:3] == b[:3] and abs(a[3] - b[3]) < 1e-9 for a, b in zip(r_sql, r_df))
log("Plan optimizado igual: {} | plan físico igual: {} | mismo resultado: {} | SQL {} s, DataFrame {} s".format(
    mismo_optimizado, mismo_fisico, mismo_resultado, min(tiempos_sql), min(tiempos_df)))
guardar_texto(args.resultados, "plan_spark_sql.txt", plan_catalyst(spark, con_sql))

resumen["sql_vs_dataframe"] = {
    "sql": SQL_EQUIVALENTE.strip(),
    "dataframe": ('viajes.join(zonas, viajes.zona_origen == zonas.zona_id)\n'
                  '      .where(viajes.distancia_mi > 1)\n'
                  '      .groupBy(zonas.borough, viajes.hora)\n'
                  '      .agg(F.count(F.lit(1)).alias("viajes"), F.avg(viajes.tarifa).alias("tarifa_media"))'),
    "mismo_plan_optimizado": mismo_optimizado,
    "mismo_plan_fisico": mismo_fisico,
    "mismo_resultado": mismo_resultado,
    "filas": len(r_sql),
    "tiempos_sql_s": tiempos_sql,
    "tiempos_dataframe_s": tiempos_df,
}
guardar_json(args.resultados, "spark_sql.json", resumen)
spark.stop()
