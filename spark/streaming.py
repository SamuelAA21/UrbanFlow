"""Paso 4 - Demostración de Structured Streaming con ventanas de tiempo de evento y watermark.

Simula la llegada en vivo de los viajes de un viernes por la tarde-noche:
- Cada hora de viajes se escribe como un archivo Parquet en una carpeta de preparación.
- Un alimentador lo MUEVE (rename atómico en HDFS) a la carpeta observada, uno cada
  pocos segundos: la fuente de archivos exige que el archivo aparezca completo.
- La consulta cuenta viajes por borough en ventanas de 1 hora que se deslizan cada
  30 minutos, con un watermark de 30 minutos y salida en modo update.
- Al final llegan dos lotes "tardíos": uno dentro del watermark (actualiza ventanas)
  y otro muy atrasado (Spark lo descarta y lo reporta como filas descartadas).
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import HDFS, guardar_csv, guardar_json, log, sesion  # noqa: E402

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--resultados", required=True)
p.add_argument("--intervalo", type=float, default=6.0, help="Segundos entre archivos que llegan")
args = p.parse_args()

spark = sesion("4 Streaming")
jvm = spark.sparkContext._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
Ruta = jvm.org.apache.hadoop.fs.Path
BASE = HDFS + "/streaming"
PREP, ENTRADA, CHECKPOINT = BASE + "/preparado", BASE + "/entrada", BASE + "/checkpoint"
fs.delete(Ruta(BASE), True)
fs.mkdirs(Ruta(ENTRADA))

limpio = spark.read.parquet(HDFS + "/limpio/viajes")
zonas = (spark.read.parquet(HDFS + "/referencia/zonas")
         .select(F.col("zona_id").alias("zona_origen"), "borough"))
viernes = limpio.filter(F.dayofweek("fecha") == 6).agg(F.max("fecha")).collect()[0][0]
# Se leen una sola vez, en paralelo, los viajes de esa tarde y se guardan en memoria: cada
# lote se escribe con coalesce(1), que sin esto leería todos los viajes en UNA sola tarea,
# una vez por lote (en una ejecución, 7 minutos solo en preparar los archivos).
viajes = (limpio.filter((F.col("fecha") == F.lit(viernes)) & (F.col("hora") >= 16))
          .select("inicio", "zona_origen", "tarifa").persist(StorageLevel.MEMORY_AND_DISK))
log("Simulando la llegada de los {:,} viajes del viernes {} entre las 16:00 y las 23:59".format(viajes.count(), viernes))

# Lotes: una hora por archivo + dos lotes tardíos al final.
lotes = [("{}h".format(h), viajes.filter(F.hour("inicio") == h)) for h in range(16, 24)]
lotes.append(("tardio_dentro_watermark", viajes.filter((F.hour("inicio") == 23) & (F.minute("inicio") >= 40)).limit(300)))
lotes.append(("tardio_fuera_watermark", viajes.filter(F.hour("inicio") == 17).limit(300)))
for i, (nombre, df) in enumerate(lotes):
    df.coalesce(1).write.mode("overwrite").parquet("{}/{:02d}_{}".format(PREP, i, nombre))
viajes.unpersist()

esquema = spark.read.parquet("{}/00_16h".format(PREP)).schema
flujo = (spark.readStream.schema(esquema).option("maxFilesPerTrigger", 1).parquet(ENTRADA)
         .join(F.broadcast(zonas), "zona_origen"))  # join flujo-estático
conteo = (flujo.withWatermark("inicio", "30 minutes")
          .groupBy(F.window("inicio", "1 hour", "30 minutes"), "borough")
          .agg(F.count(F.lit(1)).alias("viajes"), F.round(F.avg("tarifa"), 2).alias("tarifa_media")))

salidas = []


def procesar_lote(df, lote_id):
    filas = df.collect()
    for r in filas:
        salidas.append((lote_id, r["window"]["start"], r["window"]["end"], r["borough"], r["viajes"], r["tarifa_media"]))
    resumen = ", ".join("{} {:%H:%M}-{:%H:%M}: {}".format(r["borough"], r["window"]["start"], r["window"]["end"], r["viajes"])
                        for r in sorted(filas, key=lambda x: -x["viajes"])[:3])
    log("  micro-lote {:2d}: {:2d} ventanas actualizadas  {}".format(lote_id, len(filas), resumen))


consulta = (conteo.writeStream.outputMode("update").foreachBatch(procesar_lote)
            .option("checkpointLocation", CHECKPOINT).trigger(processingTime="2 seconds").start())
log("Consulta en streaming iniciada (start() no bloquea). Alimentando archivos cada {} s...".format(args.intervalo))

for i, (nombre, _) in enumerate(lotes):
    carpeta = Ruta("{}/{:02d}_{}".format(PREP, i, nombre))
    parte = [s.getPath() for s in fs.listStatus(carpeta) if s.getPath().getName().endswith(".parquet")][0]
    fs.rename(parte, Ruta("{}/{:02d}_{}.parquet".format(ENTRADA, i, nombre)))  # aparición atómica
    log("Llega el archivo {:02d}_{}".format(i, nombre))
    time.sleep(args.intervalo)
consulta.processAllAvailable()

progreso = []
for pr in consulta.recentProgress:
    if pr.get("numInputRows", 0) == 0:
        continue
    estado = (pr.get("stateOperators") or [{}])[0]
    progreso.append({
        "lote": pr["batchId"],
        "filas_entrada": pr["numInputRows"],
        "filas_por_segundo": round(pr.get("processedRowsPerSecond") or 0, 1),
        "watermark": pr.get("eventTime", {}).get("watermark"),
        "ventanas_en_estado": estado.get("numRowsTotal"),
        "filas_descartadas_por_watermark": estado.get("numRowsDroppedByWatermark", 0),
        "duracion_ms": pr.get("durationMs", {}).get("triggerExecution"),
    })
consulta.stop()
for pr in progreso:
    log("  lote {lote}: {filas_entrada} filas | watermark {watermark} | descartadas {filas_descartadas_por_watermark}".format(**pr))

guardar_json(args.resultados, "streaming_progreso.json", {
    "dia_simulado": viernes, "watermark": "30 minutes", "ventana": "1 hour, desliza 30 minutes",
    "archivos": [n for n, _ in lotes], "progreso": progreso})
guardar_csv(args.resultados, "streaming_ventanas.csv",
            spark.createDataFrame(salidas, "lote int, ventana_inicio timestamp, ventana_fin timestamp, borough string, viajes long, tarifa_media double"))
spark.stop()
