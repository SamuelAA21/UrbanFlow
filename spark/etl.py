"""Paso 1 - Ingesta, limpieza y agregación de los viajes de taxi amarillo de NYC TLC.

Lee los Parquet crudos desde HDFS, aplica reglas de calidad, guarda los viajes
limpios particionados por mes y genera la tabla agregada zona x fecha x hora,
que es la base del análisis y del modelo.
"""
import argparse
import os
import sys
import datetime
from functools import reduce

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import HDFS, Cronometro, guardar_csv, guardar_json, guardar_texto, log, plan_catalyst, sesion  # noqa: E402

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--meses", required=True, help="Ej: 2025-01,2025-02")
p.add_argument("--resultados", required=True)
args = p.parse_args()
meses = [m.strip() for m in args.meses.split(",") if m.strip()]
cutoff = os.environ.get('URBANFLOW_CUTOFF')
if cutoff:
    cutoff = datetime.date.fromisoformat(cutoff).isoformat()

spark = sesion("1 ETL")
tiempos = {}
log("ETL de {} meses: {}".format(len(meses), ", ".join(meses)))

# --- Zonas de referencia (265 zonas de taxi de NYC) -------------------------------
zonas = (spark.read.option("header", True).csv(HDFS + "/referencia/taxi_zone_lookup.csv")
         .select(F.col("LocationID").cast("int").alias("zona_id"),
                 F.col("Borough").alias("borough"),
                 F.col("Zone").alias("zona"),
                 F.col("service_zone").alias("tipo_servicio")))
zonas.write.mode("overwrite").parquet(HDFS + "/referencia/zonas")

# --- Lectura: cada mes por separado porque el esquema cambia entre archivos ------------
# (p. ej. passenger_count es entero en unos meses y decimal en otros).
partes = []
for mes in meses:
    df = spark.read.parquet("{}/crudo/yellow/yellow_tripdata_{}.parquet".format(HDFS, mes))
    partes.append(df.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("inicio"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("fin"),
        F.col("PULocationID").cast("int").alias("zona_origen"),
        F.col("DOLocationID").cast("int").alias("zona_destino"),
        F.col("passenger_count").cast("int").alias("pasajeros"),
        F.col("trip_distance").cast("double").alias("distancia_mi"),
        F.col("fare_amount").cast("double").alias("tarifa"),
        F.col("total_amount").cast("double").alias("total"),
        F.lit(mes).alias("mes")))
crudo = reduce(DataFrame.unionByName, partes)
if cutoff:
    # Files may contain later trips; no later row may reach aggregates or training.
    crudo = crudo.filter(F.to_date('inicio') <= F.lit(cutoff).cast('date'))
crudo = crudo.withColumn("duracion_min", (F.unix_timestamp("fin") - F.unix_timestamp("inicio")) / 60.0)

# --- Reglas de calidad (un valor nulo cuenta como inválido) ----------------------------
def invalido(condicion_valida):
    return F.coalesce(~condicion_valida, F.lit(True))


reglas = {
    "fuera_del_mes": invalido(F.date_format("inicio", "yyyy-MM") == F.col("mes")),
    "distancia_invalida": invalido(F.col("distancia_mi").between(0.1, 100)),
    "tarifa_invalida": invalido(F.col("tarifa").between(0.01, 1000) & (F.col("total") > 0)),
    "duracion_invalida": invalido(F.col("duracion_min").between(1, 180)),
    "zona_desconocida": invalido(F.col("zona_origen").between(1, 263) & F.col("zona_destino").between(1, 265)),
}
es_valido = ~reduce(lambda a, b: a | b, reglas.values())

with Cronometro() as c:
    calidad = (crudo.groupBy("mes")
               .agg(F.count(F.lit(1)).alias("registros_crudos"),
                    *[F.sum(F.when(cond, 1).otherwise(0)).alias(nombre) for nombre, cond in reglas.items()],
                    F.sum(F.when(es_valido, 1).otherwise(0)).alias("registros_validos"))
               .orderBy("mes"))
    filas_calidad = guardar_csv(args.resultados, "calidad_datos.csv", calidad)
tiempos["perfil_calidad_s"] = c.segundos
total_crudo = sum(r["registros_crudos"] for r in filas_calidad)
total_valido = sum(r["registros_validos"] for r in filas_calidad)
log("Registros crudos: {:,} | válidos: {:,} ({:.1f} %)".format(total_crudo, total_valido, 100.0 * total_valido / total_crudo))

limpio = (crudo.filter(es_valido)
          .withColumn("fecha", F.to_date("inicio"))
          .withColumn("hora", F.hour("inicio"))
          .withColumn("dia_semana", F.dayofweek("inicio"))  # 1 = domingo ... 7 = sábado
          .withColumn("fin_de_semana", F.col("dia_semana").isin(1, 7).cast("int")))

# Persistencia: el DataFrame limpio se reutiliza en tres acciones (escritura, conteo y agregación).
# Sin persist(), por la evaluación perezosa, Spark releería y limpiaría los Parquet tres veces.
limpio.persist(StorageLevel.MEMORY_AND_DISK)
guardar_texto(args.resultados, "plan_catalyst_etl.txt", plan_catalyst(spark, limpio))

with Cronometro() as c:
    limpio.write.mode("overwrite").partitionBy("mes").parquet(HDFS + "/limpio/viajes")
tiempos["escritura_limpio_s"] = c.segundos
log("Viajes limpios guardados en HDFS {}/limpio/viajes ({} s)".format(HDFS, c.segundos))

with Cronometro() as c:
    zona_hora = (limpio.groupBy("fecha", "hora", "zona_origen")
                 .agg(F.count(F.lit(1)).alias("viajes"),
                      F.avg("distancia_mi").alias("distancia_media"),
                      F.avg("tarifa").alias("tarifa_media"),
                      F.avg("duracion_min").alias("duracion_media")))
    zona_hora.coalesce(6).write.mode("overwrite").parquet(HDFS + "/agregado/zona_hora")
    filas_agregadas = spark.read.parquet(HDFS + "/agregado/zona_hora").count()
tiempos["agregacion_zona_hora_s"] = c.segundos
log("Tabla zona x fecha x hora: {:,} filas ({} s)".format(filas_agregadas, c.segundos))
if cutoff:
    observed = spark.read.parquet(HDFS + '/agregado/zona_hora').filter(F.col('fecha') == F.lit(cutoff).cast('date'))
    # Full grid includes observed zero-count hours, not absent forecast labels.
    zones = spark.read.parquet(HDFS + '/referencia/zonas').filter(F.col('zona_id').between(1, 263)).select(F.col('zona_id').alias('zona_origen'))
    hours = spark.range(24).select(F.col('id').cast('int').alias('hora'))
    observed = (zones.crossJoin(hours).withColumn('fecha', F.lit(cutoff).cast('date'))
                .join(observed.select('fecha','zona_origen','hora','viajes'), ['fecha','zona_origen','hora'], 'left').fillna(0, ['viajes']))
    guardar_csv(args.resultados, 'observaciones_lote.csv', observed)
limpio.unpersist()

guardar_json(args.resultados, "etl_resumen.json", {
    "meses": meses,
    'cutoff_day': cutoff,
    "registros_crudos": total_crudo,
    "registros_validos": total_valido,
    "porcentaje_descartado": round(100.0 * (total_crudo - total_valido) / total_crudo, 2),
    "filas_zona_hora": filas_agregadas,
    "particiones_lectura": crudo.rdd.getNumPartitions(),
    "tiempos": tiempos,
})
spark.stop()
