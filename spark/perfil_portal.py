"""Agrega recogidas por zona, día de la semana y hora para el portal."""
import argparse

from pyspark.sql import functions as F

from comun import HDFS, guardar_csv, sesion

parser = argparse.ArgumentParser()
parser.add_argument("--resultados", required=True)
args = parser.parse_args()

spark = sesion("Perfil del portal")
hourly = spark.read.parquet(HDFS + "/agregado/zona_hora").withColumn(
    "dia_semana", F.dayofweek("fecha"))
days = hourly.groupBy("dia_semana").agg(F.countDistinct("fecha").alias("dias"))
counts = hourly.groupBy("zona_origen", "dia_semana", "hora").agg(F.sum("viajes").alias("viajes"))
zones = hourly.select("zona_origen").distinct()
hours = spark.range(24).select(F.col("id").cast("int").alias("hora"))
profile = (zones.crossJoin(days).crossJoin(hours)
           .join(counts, ["zona_origen", "dia_semana", "hora"], "left").fillna(0, ["viajes"]))
guardar_csv(args.resultados, "perfil.csv", profile)
spark.stop()
