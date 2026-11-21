"""Utilidades compartidas por los trabajos PySpark de UrbanFlow.

Se ejecutan en el driver (modo cliente, en el maestro). Compatible con Python 3.9.
"""
import csv
import datetime
import json
import os
import time

from pyspark.sql import SparkSession

HDFS = "/urbanflow"


def sesion(nombre):
    spark = (SparkSession.builder
             .appName("UrbanFlow - " + nombre)
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def log(mensaje):
    print("[UrbanFlow] " + mensaje, flush=True)


class Cronometro:
    """Mide el tiempo de un bloque: with Cronometro() as c: ...; c.segundos"""

    def __enter__(self):
        self.inicio = time.time()
        return self

    def __exit__(self, *args):
        self.segundos = round(time.time() - self.inicio, 2)


def _valor(v):
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    if isinstance(v, float):
        return round(v, 4)
    return v


def guardar_csv(carpeta, nombre, df):
    """Recoge un resultado pequeño en el driver y lo guarda como CSV local."""
    filas = df.collect()
    ruta = os.path.join(carpeta, nombre)
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        for fila in filas:
            w.writerow([_valor(v) for v in fila])
    log("  -> {} ({} filas)".format(nombre, len(filas)))
    return filas


def guardar_json(carpeta, nombre, objeto):
    with open(os.path.join(carpeta, nombre), "w", encoding="utf-8") as f:
        json.dump(objeto, f, ensure_ascii=False, indent=2, default=_valor)
    log("  -> " + nombre)


def guardar_texto(carpeta, nombre, texto):
    with open(os.path.join(carpeta, nombre), "w", encoding="utf-8") as f:
        f.write(texto)
    log("  -> " + nombre)


def plan_catalyst(spark, df):
    """Plan lógico analizado, optimizado y físico (lo mismo que df.explain('extended'))."""
    return spark.sparkContext._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "extended")
