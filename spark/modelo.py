"""Paso 3 - Predicción de demanda por zona y hora con Spark MLlib (Random Forest).

Objetivo: ¿cuántos viajes saldrán de la zona Z en la hora H?
- Malla completa zona x hora (las horas sin viajes cuentan como 0).
- Variables de calendario + rezagos (misma hora ayer y hace una semana)
  + perfil histórico de la zona calculado SOLO con el periodo de entrenamiento.
- Partición temporal: los últimos 14 días son la prueba (nunca se mezclan).
- Se compara con dos líneas base sencillas para demostrar que el modelo aporta.
"""
import argparse
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import HDFS, Cronometro, guardar_csv, guardar_json, log, sesion  # noqa: E402

from pyspark import StorageLevel  # noqa: E402
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator  # noqa: E402
from pyspark.ml.feature import VectorAssembler  # noqa: E402
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--resultados", required=True)
p.add_argument("--dias-prueba", type=int, default=14)
p.add_argument("--min-viajes-zona", type=int, default=1000, help="Zonas con menos viajes en todo el periodo se excluyen")
p.add_argument("--arboles", type=int, default=40)
p.add_argument("--profundidad", type=int, default=12)
p.add_argument("--iteraciones-gbt", type=int, default=50)
args = p.parse_args()

# Festivos federales de EE. UU. (afectan la movilidad en NYC).
FESTIVOS = [
    "2024-01-01", "2024-01-15", "2024-02-19", "2024-05-27", "2024-06-19", "2024-07-04", "2024-09-02",
    "2024-10-14", "2024-11-11", "2024-11-28", "2024-12-25",
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01",
    "2025-10-13", "2025-11-11", "2025-11-27", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-10-12", "2026-11-11", "2026-11-26", "2026-12-25",
]
VARIABLES = ["zona_origen", "hora", "dia_semana", "fin_de_semana", "mes_num", "festivo",
             "viajes_ayer", "viajes_semana_pasada", "media_7_dias", "perfil_historico"]

spark = sesion("3 Modelo")
zonas = spark.read.parquet(HDFS + "/referencia/zonas").withColumnRenamed("zona_id", "zona_origen")
zh = spark.read.parquet(HDFS + "/agregado/zona_hora").select("fecha", "hora", "zona_origen", "viajes")
ultima_fecha = zh.agg(F.max('fecha')).collect()[0][0]
corte = ultima_fecha - datetime.timedelta(days=args.dias_prueba)


def calendario(df):
    return (df.withColumn("dia_semana", F.dayofweek("fecha"))
            .withColumn("fin_de_semana", F.col("dia_semana").isin(1, 7).cast("int"))
            .withColumn("mes_num", F.month("fecha"))
            .withColumn("festivo", F.col("fecha").cast("string").isin(FESTIVOS).cast("int")))


# --- Malla completa zona x fecha x hora ------------------------------------------------
activas = (zh.filter(F.col('fecha') <= F.lit(corte)).groupBy("zona_origen").agg(F.sum("viajes").alias("total"))
           .filter(F.col("total") >= args.min_viajes_zona).select("zona_origen"))
n_zonas = activas.count()
fechas = zh.select("fecha").distinct()
horas = spark.range(24).select(F.col("id").cast("int").alias("hora"))
malla = (activas.crossJoin(fechas).crossJoin(horas)
         .join(zh, ["zona_origen", "fecha", "hora"], "left").fillna(0, ["viajes"])
         .withColumn("ts", (F.unix_timestamp(F.col("fecha").cast("timestamp")) + F.col("hora") * 3600).cast("timestamp")))

# Rezagos con funciones de ventana: cada fila es una hora consecutiva de la misma zona.
w = Window.partitionBy("zona_origen").orderBy("ts")
datos = (calendario(malla)
         .withColumn("viajes_ayer", F.lag("viajes", 24).over(w))
         .withColumn("viajes_semana_pasada", F.lag("viajes", 168).over(w))
         .withColumn("media_7_dias", sum(F.lag("viajes", 24 * k).over(w) for k in range(1, 8)) / 7.0)
         .dropna(subset=["viajes_semana_pasada"]))
datos.persist(StorageLevel.MEMORY_AND_DISK)

ultima_fecha = datos.agg(F.max("fecha")).collect()[0][0]
corte = ultima_fecha - datetime.timedelta(days=args.dias_prueba)
entreno = datos.filter(F.col("fecha") <= F.lit(corte))
# Perfil histórico (media por zona, día de la semana y hora) usando SOLO entrenamiento: evita fuga de información.
perfil = entreno.groupBy("zona_origen", "dia_semana", "hora").agg(F.avg("viajes").alias("perfil_historico"))
entreno = entreno.join(perfil, ["zona_origen", "dia_semana", "hora"]).persist(StorageLevel.MEMORY_AND_DISK)
prueba = datos.filter(F.col("fecha") > F.lit(corte)).join(perfil, ["zona_origen", "dia_semana", "hora"], "left").fillna(0.0, ["perfil_historico"])
n_entreno, n_prueba = entreno.count(), prueba.count()
log("{} zonas activas | entrenamiento hasta {}: {:,} filas | prueba ({} días): {:,} filas".format(
    n_zonas, corte, n_entreno, args.dias_prueba, n_prueba))

# --- Entrenamiento: dos algoritmos de árboles de MLlib ---------------------------------
# maxBins=256: MLlib discretiza cada variable continua en "bins"; con el valor por defecto (32)
# el perfil histórico (0 a cientos de viajes) pierde resolución y el modelo queda por debajo de la línea base.
ALGORITMOS = {
    "Random Forest (MLlib)": RandomForestRegressor(
        labelCol="viajes", featuresCol="variables", numTrees=args.arboles, maxDepth=args.profundidad,
        maxBins=256, subsamplingRate=0.8, minInstancesPerNode=5, seed=42),
    "Gradient-Boosted Trees (MLlib)": GBTRegressor(
        labelCol="viajes", featuresCol="variables", maxIter=args.iteraciones_gbt, maxDepth=7,
        maxBins=256, stepSize=0.1, subsamplingRate=0.8, seed=42),
}
COLUMNA = {"Random Forest (MLlib)": "pred_rf", "Gradient-Boosted Trees (MLlib)": "pred_gbt"}
modelos, tiempos_entreno = {}, {}
for nombre, estimador in ALGORITMOS.items():
    log("Entrenando {} en el clúster...".format(nombre))
    with Cronometro() as c:
        modelos[nombre] = Pipeline(stages=[VectorAssembler(inputCols=VARIABLES, outputCol="variables"), estimador]).fit(entreno)
    tiempos_entreno[nombre] = c.segundos
    log("  {} entrenado en {} s".format(nombre, c.segundos))

# --- Evaluación contra líneas base -----------------------------------------------------
pred = prueba
for nombre, modelo_n in modelos.items():
    pred = modelo_n.transform(pred).withColumnRenamed("prediction", COLUMNA[nombre]).drop("variables")
pred = (pred.withColumn("pred_perfil", F.col("perfil_historico"))
        .withColumn("pred_semana_pasada", F.col("viajes_semana_pasada").cast("double")))
pred.persist(StorageLevel.MEMORY_AND_DISK)
metricas = {}
for nombre, columna in [("Random Forest (MLlib)", "pred_rf"), ("Gradient-Boosted Trees (MLlib)", "pred_gbt"),
                        ("Línea base: promedio histórico", "pred_perfil"),
                        ("Línea base: misma hora semana pasada", "pred_semana_pasada")]:
    ev = RegressionEvaluator(labelCol="viajes", predictionCol=columna)
    metricas[nombre] = {m: round(ev.setMetricName(m).evaluate(pred), 4) for m in ("rmse", "mae", "r2")}
    log("{:40s} RMSE {rmse:7.3f} | MAE {mae:7.3f} | R² {r2:6.3f}".format(nombre, **metricas[nombre]))

# Fixed choice before evaluation: do not select/tune on test labels.
mejor = 'Random Forest (MLlib)'
modelo = modelos[mejor]
segundos_entreno = tiempos_entreno[mejor]
log("Modelo fijado antes de evaluar: {}".format(mejor))
# Evaluation above stays untouched. Fit the fixed RF for deployment using ALL
# genuinely observed rows available at the replay cutoff, never the next day.
perfil = datos.groupBy('zona_origen', 'dia_semana', 'hora').agg(F.avg('viajes').alias('perfil_historico'))
todos = datos.join(perfil, ['zona_origen', 'dia_semana', 'hora'])
with Cronometro() as deployment_timer:
    modelo = Pipeline(stages=[VectorAssembler(inputCols=VARIABLES, outputCol='variables'), ALGORITMOS[mejor]]).fit(todos)
model_path = os.environ.get('URBANFLOW_MODEL_PATH', HDFS + '/modelos/demanda')
modelo.write().overwrite().save(model_path)
pred = pred.withColumn("pred_modelo", F.col(COLUMNA[mejor]))
importancias = modelo.stages[-1].featureImportances.toArray().tolist()
guardar_csv(args.resultados, "importancia_variables.csv",
            spark.createDataFrame([(v, float(i)) for v, i in zip(VARIABLES, importancias)], ["variable", "importancia"])
            .orderBy(F.desc("importancia")))
guardar_csv(args.resultados, "predicciones_prueba.csv",
            pred.select("fecha", "hora", "zona_origen", "viajes",
                        F.round("pred_modelo", 2).alias("pred_modelo"), F.round("pred_rf", 2).alias("pred_rf"),
                        F.round("pred_gbt", 2).alias("pred_gbt"), F.round("pred_perfil", 2).alias("pred_perfil"),
                        "pred_semana_pasada")
            .orderBy("fecha", "hora", "zona_origen"))

# --- Pronóstico del día siguiente al último dato ---------------------------------------
manana = ultima_fecha + datetime.timedelta(days=1)
semana = datos.filter(F.col("fecha") > F.lit(ultima_fecha - datetime.timedelta(days=7)))
futuro = (semana.groupBy("zona_origen", "hora").agg(
              F.avg("viajes").alias("media_7_dias"),
              F.sum(F.when(F.col("fecha") == F.lit(ultima_fecha), F.col("viajes"))).alias("viajes_ayer"),
              F.sum(F.when(F.col("fecha") == F.lit(ultima_fecha - datetime.timedelta(days=6)), F.col("viajes")))
              .alias("viajes_semana_pasada"))
          .withColumn("fecha", F.lit(manana)))
futuro = calendario(futuro).join(perfil, ["zona_origen", "dia_semana", "hora"], "left").fillna(0.0)
pronostico = (modelo.transform(futuro).join(zonas, "zona_origen")
              .select("fecha", "hora", "zona_origen", "borough", "zona",
                      F.round('perfil_historico', 1).alias('pred_perfil'),
                      F.round(F.greatest(F.col("prediction"), F.lit(0.0)), 1).alias("viajes_esperados"))
              .orderBy("zona_origen", "hora"))
guardar_csv(args.resultados, "prediccion_manana.csv", pronostico)

guardar_json(args.resultados, "metricas_modelo.json", {
    "algoritmo": mejor, "mejor_modelo": mejor,
    'selection_policy': 'Random Forest fixed a priori; GBT is a reported comparator, not selected on test',
    'deployment_training_until': ultima_fecha,
    'deployment_training_seconds': deployment_timer.segundos,
    'model_path': model_path,
    'deployment_policy': 'Refit fixed RF on all real observations through cutoff after frozen temporal evaluation',
    'evaluation_mode': 'rolling one-day observations; no synthetic labels',
    "arboles": args.arboles, "profundidad": args.profundidad, "iteraciones_gbt": args.iteraciones_gbt, "max_bins": 256,
    "tiempos_entrenamiento": tiempos_entreno,
    "variables": VARIABLES,
    "zonas_activas": n_zonas,
    "filas_entrenamiento": n_entreno, "filas_prueba": n_prueba,
    "entrenamiento_hasta": corte, "prueba_desde": corte + datetime.timedelta(days=1), "prueba_hasta": ultima_fecha,
    "fecha_pronostico": manana,
    "segundos_entrenamiento": segundos_entreno,
    "metricas": metricas,
})
spark.stop()
