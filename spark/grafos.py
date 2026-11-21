"""Paso 2c - La ciudad como grafo, con GraphFrames (cuyos algoritmos ejecutan GraphX).

GraphX solo tiene API en Scala/Java; GraphFrames lo expone a Python sobre DataFrames.
Vértices: las 263 zonas de taxi. Aristas: cada par origen -> destino con viajes
(dirigidas, con peso = número de viajes; se excluyen los viajes dentro de la misma zona).

1. aggregateMessages (paso de mensajes): viajes que entran y salen de cada zona.
2. PageRank (graphx.lib.PageRank): zonas que son destino principal de zonas importantes.
3. Búsqueda de patrones (motifs): corredores de ida y vuelta (a)->(b) y (b)->(a).
4. Label Propagation (graphx.lib.LabelPropagation): comunidades de zonas conectadas.

Corredor «fuerte»: al menos un viaje diario en promedio durante el periodo. Aun así cada
zona tiene decenas de corredores fuertes, y PageRank y Label Propagation de GraphX no usan
pesos (con todos los corredores, Label Propagation junta todo en una sola comunidad); por
eso se ejecutan sobre el grafo troncal: para cada zona, sus K corredores fuertes con más viajes.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import HDFS, Cronometro, guardar_csv, guardar_json, log, sesion  # noqa: E402

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402
from graphframes import GraphFrame  # noqa: E402  (llega con --py-files desde pipeline.sh)
from graphframes.lib import AggregateMessages as AM  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--resultados", required=True)
args = p.parse_args()

spark = sesion("2c Grafos")
limpio = spark.read.parquet(HDFS + "/limpio/viajes")
vertices = (spark.read.parquet(HDFS + "/referencia/zonas")
            .where(F.col("zona_id").between(1, 263))
            .select(F.col("zona_id").alias("id"), "zona", "borough"))
tiempos = {}

# --- Construcción del grafo ----------------------------------------------------------
log("Construyendo el grafo: zonas como vértices, viajes origen -> destino como aristas...")
with Cronometro() as c:
    entre_zonas = limpio.where(F.col("zona_destino").between(1, 263))
    dias = limpio.select("fecha").distinct().count()
    misma_zona = entre_zonas.where(F.col("zona_origen") == F.col("zona_destino")).count()
    aristas = (entre_zonas.where(F.col("zona_origen") != F.col("zona_destino"))
               .groupBy(F.col("zona_origen").alias("src"), F.col("zona_destino").alias("dst"))
               .agg(F.count(F.lit(1)).alias("viajes"),
                    F.round(F.avg("tarifa"), 2).alias("tarifa_media")))
    aristas.persist(StorageLevel.MEMORY_AND_DISK)
    resumen_aristas = aristas.agg(F.count(F.lit(1)).alias("n"), F.sum("viajes").alias("viajes")).collect()[0]
tiempos["construccion_s"] = c.segundos

grafo = GraphFrame(vertices, aristas)
umbral = dias  # al menos un viaje diario en promedio
fuerte = GraphFrame(vertices, aristas.where(F.col("viajes") >= umbral)).dropIsolatedVertices()
fuerte.persist(StorageLevel.MEMORY_AND_DISK)
n_vertices = vertices.count()
n_fuerte_v, n_fuerte_e = fuerte.vertices.count(), fuerte.edges.count()
viajes_fuertes = fuerte.edges.agg(F.sum("viajes")).collect()[0][0]
log("Grafo completo: {} zonas, {:,} aristas, {:,} viajes entre zonas distintas".format(
    n_vertices, resumen_aristas["n"], resumen_aristas["viajes"]))
log("Corredores fuertes (>= {} viajes, uno al día): {:,} aristas entre {} zonas, {:.1f} % de los viajes".format(
    umbral, n_fuerte_e, n_fuerte_v, 100.0 * viajes_fuertes / resumen_aristas["viajes"]))

K_TRONCAL = 3
principales = Window.partitionBy("src").orderBy(F.desc("viajes"))
troncal = GraphFrame(vertices, fuerte.edges.withColumn("orden", F.row_number().over(principales))
                     .where(F.col("orden") <= K_TRONCAL).drop("orden")).dropIsolatedVertices()
troncal.persist(StorageLevel.MEMORY_AND_DISK)
n_troncal_e = troncal.edges.count()
log("Grafo troncal (los {} destinos principales de cada zona): {:,} aristas".format(K_TRONCAL, n_troncal_e))

# --- 1. aggregateMessages: flujo que entra y sale de cada zona -----------------------
log("aggregateMessages: cada arista envía su número de viajes a su origen y a su destino...")
with Cronometro() as c:
    entrantes = grafo.aggregateMessages(F.sum(AM.msg).alias("viajes_entrantes"), sendToDst=AM.edge["viajes"])
    salientes = grafo.aggregateMessages(F.sum(AM.msg).alias("viajes_salientes"), sendToSrc=AM.edge["viajes"])
    flujo = (vertices.join(entrantes, "id", "left").join(salientes, "id", "left")
             .fillna(0, ["viajes_entrantes", "viajes_salientes"])
             .withColumn("balance", F.col("viajes_entrantes") - F.col("viajes_salientes")))
    flujo.persist(StorageLevel.MEMORY_AND_DISK).count()
tiempos["aggregate_messages_s"] = c.segundos

# --- 2. PageRank de GraphX ------------------------------------------------------------
log("PageRank (GraphX) sobre el grafo troncal, 10 iteraciones...")
with Cronometro() as c:
    pr = troncal.pageRank(resetProbability=0.15, maxIter=10).vertices.select("id", "pagerank")
    pr.persist(StorageLevel.MEMORY_AND_DISK).count()
tiempos["pagerank_s"] = c.segundos

# --- 3. Patrones: corredores de ida y vuelta ------------------------------------------
log("Búsqueda de patrones: (a)-[ida]->(b); (b)-[vuelta]->(a)...")
with Cronometro() as c:
    ida_vuelta = (fuerte.find("(a)-[ida]->(b); (b)-[vuelta]->(a)")
                  .where("a.id < b.id")
                  .select(F.col("a.zona").alias("zona_a"), F.col("a.borough").alias("borough_a"),
                          F.col("b.zona").alias("zona_b"), F.col("b.borough").alias("borough_b"),
                          F.col("ida.viajes").alias("viajes_a_b"), F.col("vuelta.viajes").alias("viajes_b_a"))
                  .withColumn("viajes_total", F.col("viajes_a_b") + F.col("viajes_b_a"))
                  .withColumn("asimetria_pct", F.round(
                      100.0 * F.abs(F.col("viajes_a_b") - F.col("viajes_b_a")) / F.col("viajes_total"), 1)))
    ida_vuelta.persist(StorageLevel.MEMORY_AND_DISK)
    n_ida_vuelta = ida_vuelta.count()
    guardar_csv(args.resultados, "grafo_ida_vuelta.csv", ida_vuelta.orderBy(F.desc("viajes_total")).limit(15))
tiempos["motifs_s"] = c.segundos
log("Pares de zonas con corredor fuerte en ambos sentidos: {:,} de {:,} corredores".format(n_ida_vuelta, n_fuerte_e))

# --- 4. Comunidades con Label Propagation de GraphX ----------------------------------
log("Label Propagation (GraphX) sobre el grafo troncal, 10 iteraciones...")
with Cronometro() as c:
    comunidades = troncal.labelPropagation(maxIter=10).select("id", F.col("label").alias("comunidad"))
    comunidades.persist(StorageLevel.MEMORY_AND_DISK).count()
tiempos["label_propagation_s"] = c.segundos

# --- Resultados por zona y resúmenes -------------------------------------------------
grados = (fuerte.inDegrees.withColumnRenamed("inDegree", "corredores_entrada")
          .join(fuerte.outDegrees.withColumnRenamed("outDegree", "corredores_salida"), "id", "outer"))
zonas = (flujo.join(grados, "id", "left").join(pr, "id", "left").join(comunidades, "id", "left")
         .fillna(0, ["corredores_entrada", "corredores_salida"])
         .withColumn("pagerank", F.round("pagerank", 4))
         .orderBy(F.desc_nulls_last("pagerank")))
filas_zonas = guardar_csv(args.resultados, "grafo_zonas.csv", zonas)

por_comunidad = (zonas.where(F.col("comunidad").isNotNull())
                 .groupBy("comunidad", "borough").agg(F.count(F.lit(1)).alias("zonas")))
filas_com = por_comunidad.collect()
resumen_com = {}
for f in filas_com:
    r = resumen_com.setdefault(f["comunidad"], {"zonas": 0, "por_borough": {}})
    r["zonas"] += f["zonas"]
    r["por_borough"][f["borough"]] = f["zonas"]
lista_com = sorted(({"comunidad": k, "zonas": v["zonas"],
                     "borough_principal": max(v["por_borough"], key=v["por_borough"].get),
                     "por_borough": v["por_borough"]} for k, v in resumen_com.items()),
                   key=lambda x: -x["zonas"])
log("Comunidades encontradas: {} (tamaños: {})".format(
    len(lista_com), ", ".join(str(x["zonas"]) for x in lista_com[:8])))

guardar_csv(args.resultados, "grafo_corredores.csv",
            fuerte.edges.orderBy(F.desc("viajes")).limit(40)
            .join(vertices.select(F.col("id").alias("src"), F.col("zona").alias("zona_origen"),
                                  F.col("borough").alias("borough_origen")), "src")
            .join(vertices.select(F.col("id").alias("dst"), F.col("zona").alias("zona_destino"),
                                  F.col("borough").alias("borough_destino")), "dst")
            .orderBy(F.desc("viajes")))

guardar_json(args.resultados, "grafo_resumen.json", {
    "libreria": "GraphFrames 0.8.4 (Spark 3.5); PageRank y Label Propagation ejecutan graphx.lib",
    "dias": dias,
    "vertices": n_vertices,
    "aristas": resumen_aristas["n"],
    "densidad": round(resumen_aristas["n"] / float(n_vertices * (n_vertices - 1)), 4),
    "viajes_entre_zonas": resumen_aristas["viajes"],
    "viajes_misma_zona": misma_zona,
    "umbral_corredor_fuerte": umbral,
    "corredores_fuertes": n_fuerte_e,
    "zonas_con_corredor_fuerte": n_fuerte_v,
    "porcentaje_viajes_en_corredores_fuertes": round(100.0 * viajes_fuertes / resumen_aristas["viajes"], 1),
    "pares_ida_y_vuelta": n_ida_vuelta,
    "troncal_k": K_TRONCAL,
    "troncal_aristas": n_troncal_e,
    "comunidades": lista_com,
    "top_pagerank": [{"zona": f["zona"], "borough": f["borough"], "pagerank": f["pagerank"]}
                     for f in filas_zonas[:10]],
    "tiempos": tiempos,
})
spark.stop()
