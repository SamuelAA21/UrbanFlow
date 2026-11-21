"""Paso 2 - Patrones de movilidad y dos experimentos sobre el motor de Spark.

1. Indicadores y patrones (día x hora, zonas, viernes por la noche, boroughs).
2. RDD vs DataFrame: el mismo promedio por clave escrito con lambdas (opaco para
   Spark) y con la API estructurada (optimizable por Catalyst y Tungsten).
3. Catalyst: plan lógico analizado vs optimizado de un join + filtro.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import HDFS, Cronometro, guardar_csv, guardar_json, guardar_texto, log, plan_catalyst, sesion  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--resultados", required=True)
args = p.parse_args()

spark = sesion("2 Analisis")
limpio = spark.read.parquet(HDFS + "/limpio/viajes")
zonas = spark.read.parquet(HDFS + "/referencia/zonas").withColumnRenamed("zona_id", "zona_origen")
zh = spark.read.parquet(HDFS + "/agregado/zona_hora").withColumn("dia_semana", F.dayofweek("fecha"))

# --- 1. Indicadores generales -------------------------------------------------------
log("Calculando indicadores generales sobre los viajes limpios...")
kpi = limpio.agg(
    F.count(F.lit(1)).alias("viajes"),
    F.avg("distancia_mi").alias("distancia_media_mi"),
    F.avg("tarifa").alias("tarifa_media_usd"),
    F.avg("duracion_min").alias("duracion_media_min"),
    F.sum("total").alias("recaudo_total_usd"),
    F.min("fecha").alias("fecha_inicial"),
    F.max("fecha").alias("fecha_final"),
    F.countDistinct("zona_origen").alias("zonas_con_viajes")).collect()[0].asDict()
kpi["dias"] = (kpi["fecha_final"] - kpi["fecha_inicial"]).days + 1
kpi["viajes_promedio_dia"] = round(kpi["viajes"] / kpi["dias"])
guardar_json(args.resultados, "kpis.json", kpi)

log("Patrones de demanda...")
dias_por_semana = zh.select("fecha", "dia_semana").distinct().groupBy("dia_semana").agg(F.count(F.lit(1)).alias("n_dias"))
guardar_csv(args.resultados, "heatmap_dia_hora.csv",
            zh.groupBy("dia_semana", "hora").agg(F.sum("viajes").alias("viajes"))
            .join(dias_por_semana, "dia_semana")
            .withColumn("viajes_promedio", F.round(F.col("viajes") / F.col("n_dias"), 1))
            .orderBy("dia_semana", "hora"))
guardar_csv(args.resultados, "viajes_por_dia.csv",
            zh.groupBy("fecha").agg(F.sum("viajes").alias("viajes")).orderBy("fecha"))
n_dias = kpi["dias"]
guardar_csv(args.resultados, "zonas_total.csv",
            zh.groupBy("zona_origen").agg(F.sum("viajes").alias("viajes"),
                                          F.avg("tarifa_media").alias("tarifa_media"))
            .withColumn("viajes_promedio_dia", F.round(F.col("viajes") / n_dias, 1))
            .join(zonas, "zona_origen")
            .orderBy(F.desc("viajes")))
# Viernes por la noche: viernes (dayofweek = 6) entre las 20:00 y las 23:59.
viernes = zh.filter((F.col("dia_semana") == 6) & (F.col("hora") >= 20))
n_viernes = viernes.select("fecha").distinct().count()
guardar_csv(args.resultados, "viernes_noche_top.csv",
            viernes.groupBy("zona_origen").agg(F.sum("viajes").alias("viajes"))
            .withColumn("promedio_por_viernes", F.round(F.col("viajes") / n_viernes, 1))
            .join(zonas, "zona_origen")
            .orderBy(F.desc("viajes")).limit(15))
guardar_csv(args.resultados, "borough_hora.csv",
            zh.join(zonas, "zona_origen")
            .groupBy("borough", "hora").agg(F.sum("viajes").alias("viajes"))
            .orderBy("borough", "hora"))

# --- 2. RDD vs DataFrame: tarifa promedio por hora ---------------------------------------
log("Experimento RDD vs DataFrame (tarifa promedio por hora, {:,} viajes)...".format(kpi["viajes"]))


def con_rdd():
    # Spark solo ve lambdas: no sabe que es un promedio, no puede optimizar
    # y cada fila viaja serializada como objeto Python.
    return dict(limpio.select("hora", "tarifa").rdd
                .map(lambda fila: (fila[0], (fila[1], 1)))
                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                .mapValues(lambda par: par[0] / par[1])
                .collect())


def con_dataframe():
    # Spark entiende la intención (agrupar y promediar): Catalyst optimiza el plan
    # y Tungsten ejecuta código generado sobre datos binarios en la JVM.
    return {r["hora"]: r["promedio"] for r in
            limpio.groupBy("hora").agg(F.avg("tarifa").alias("promedio")).collect()}


tiempos_rdd, tiempos_df = [], []
for _ in range(2):
    with Cronometro() as c:
        r_rdd = con_rdd()
    tiempos_rdd.append(c.segundos)
    with Cronometro() as c:
        r_df = con_dataframe()
    tiempos_df.append(c.segundos)
diferencia = max(abs(r_rdd[h] - r_df[h]) for h in r_df)
mejor_rdd, mejor_df = min(tiempos_rdd), min(tiempos_df)
log("RDD: {} s | DataFrame: {} s | DataFrame {:.1f}x más rápido | mismo resultado: {}".format(
    mejor_rdd, mejor_df, mejor_rdd / mejor_df, diferencia < 1e-6))
guardar_json(args.resultados, "comparacion_rdd_df.json", {
    "consulta": "Tarifa promedio por hora del día",
    "registros": kpi["viajes"],
    "tiempos_rdd_s": tiempos_rdd,
    "tiempos_dataframe_s": tiempos_df,
    "mejor_rdd_s": mejor_rdd,
    "mejor_dataframe_s": mejor_df,
    "aceleracion": round(mejor_rdd / mejor_df, 2),
    "diferencia_maxima": diferencia,
    "mismo_resultado": diferencia < 1e-6,
    "tarifa_por_hora": {str(h): round(r_df[h], 3) for h in sorted(r_df)},
})

# --- 3. Catalyst: join + filtro --------------------------------------------------------
# Se escribe el filtro DESPUÉS del join; el optimizador lo empuja antes del join
# (predicate pushdown) y hasta la lectura del Parquet (PushedFilters).
consulta = (limpio.join(zonas, "zona_origen")
            .where((F.col("borough") == "Manhattan") & (F.col("fecha") < F.date_add(F.lit(kpi["fecha_inicial"]), 7)))
            .select("inicio", "zona", "tarifa"))
guardar_texto(args.resultados, "plan_catalyst.txt", plan_catalyst(spark, consulta))
spark.stop()
