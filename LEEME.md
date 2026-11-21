# UrbanFlow

**Análisis de movilidad urbana y predicción de demanda de transporte mediante Apache Spark y Machine Learning.**

Procesa millones de viajes reales de taxi amarillo de Nueva York (NYC TLC, archivos Parquet oficiales), encuentra patrones de movilidad y predice cuántos viajes saldrán de cada zona en cada hora.

## Arquitectura

```text
 NYC TLC (CloudFront)                    AWS us-east-1 · VPC propia del proyecto
 ─────────────────────                   ──────────────────────────────────────────────
 yellow_tripdata_2025-*.parquet ──curl──▶ Maestro m7i-flex.large
 taxi_zone_lookup.csv                     · HDFS NameNode · YARN ResourceManager
                                          · Spark driver (modo cliente) · Spark History
                                                │  HDFS (replicación x3) + YARN
                                ┌───────────────┼───────────────┐
                           Worker 1         Worker 2        Worker 3   (c7i-flex.large)
                           DataNode +       DataNode +      DataNode +
                           ejecutor Spark   ejecutor Spark  ejecutor Spark

 Pipeline PySpark:  cargar ▶ etl.py ▶ analisis.py ▶ consultas_sql.py ▶ grafos.py ▶ modelo.py ▶ streaming.py ▶ resultados/
 En el computador:  resultados/ (CSV/JSON pequeños) ▶ dashboard Streamlit
```

| Paso | Script | Qué demuestra |
|---|---|---|
| 1. Ingesta | `cluster/pipeline.sh cargar` | Descarga los Parquet directamente a HDFS (replicados en 3 nodos) |
| 2. ETL | `spark/etl.py` | DataFrames, 5 reglas de calidad, `persist()`, evaluación perezosa, particionado por mes |
| 3. Análisis | `spark/analisis.py` | Patrones día × hora, zonas, viernes de noche; **RDD vs DataFrame**; plan de **Catalyst** |
| 4. Spark SQL | `spark/consultas_sql.py` | Vistas temporales, CTE, `RANK() OVER`, JOIN doble, `CASE`; la misma consulta en **SQL y DataFrame llega al mismo plan** de Catalyst |
| 5. Grafos | `spark/grafos.py` | **GraphX vía GraphFrames** (GraphX no tiene API en Python): `aggregateMessages`, **PageRank**, búsqueda de patrones y **Label Propagation** sobre la red de zonas |
| 6. Modelo | `spark/modelo.py` | **Spark MLlib** Random Forest y GBT, funciones de ventana (rezagos), validación temporal, líneas base |
| 7. Streaming | `spark/streaming.py` | **Structured Streaming**: ventanas de tiempo de evento, watermark, datos tardíos |

GraphFrames 0.8.4 (para Spark 3.5) se descarga en el maestro la primera vez que corre el paso de grafos, desde spark-packages, y se verifica con su SHA-256 (`cluster/pipeline.sh`). Es un solo jar que trae su API de Python; su PageRank y su Label Propagation llaman a `org.apache.spark.graphx.lib`.

## Uso

Requisitos: AWS CLI v2 con sesión (`aws login`), OpenSSH de Windows y Python 3 (para el dashboard).

Doble clic en **`Ejecutar.cmd`** o por comandos:

```powershell
cd C:\UrbanFlow
aws login                         # si la sesión caducó
.\urbanflow.ps1 todo              # despliega el clúster + procesa + descarga resultados (~25-40 min)
.\urbanflow.ps1 dashboard         # abre el panel en http://localhost:8501
.\urbanflow.ps1 eliminar          # BORRA TODO en AWS (instancias, discos, red)
```

Otras acciones: `desplegar`, `procesar`, `demo` (SQL, grafos y streaming en vivo), `streaming` (solo la demo de streaming), `resultados`, `estado` (enlaces a YARN, HDFS y Spark History), `conectar` (SSH al maestro).

Un solo paso, con el clúster activo y los datos ya procesados: `.\urbanflow.ps1 paso -Paso sql` (o `grafos`, `analisis`, `modelo`, `streaming`).

Para una ejecución más corta: `.\urbanflow.ps1 procesar -Meses 2025-01,2025-02,2025-03`.

## Costos y limpieza

- 4 instancias (`m7i-flex.large` + 3 × `c7i-flex.large`), elegibles para el plan gratuito de AWS; discos gp3 de 20 GB.
- Todo lleva la etiqueta `Proyecto=urbanflow`. `eliminar` termina las instancias, borra discos, grupo de seguridad, subred, tabla de rutas, Internet Gateway y VPC, y **verifica** que no quede nada. La red se busca también por etiquetas, así que funciona aunque se haya perdido `estado/urbanflow.json`.
- Protección adicional: cada instancia se apaga y se elimina sola a las 4 horas (`-HorasMaximas`). La red no se borra sola: ejecute `eliminar` igual, o el siguiente `desplegar`/`todo` la limpia antes de crear el clúster nuevo.
- Los resultados quedan en `resultados/`, así el dashboard funciona sin AWS.

## Para la presentación

1. **Antes de exponer** (con margen, unos 60 min): `.\urbanflow.ps1 todo` (o la opción 1 del menú). Despliega el clúster y ejecuta los 7 pasos, incluidos los largos (ETL y modelo).
2. **Durante la exposición**: dashboard + enlaces de `estado` (YARN muestra los 3 workers, Spark History los trabajos).
3. **Demo en vivo** (unos pocos minutos): `.\urbanflow.ps1 demo` (opción 9). Ejecuta Spark SQL, grafos y streaming sobre los datos completos ya procesados y actualiza el dashboard. Solo el streaming: `.\urbanflow.ps1 streaming`.
4. **Al terminar**: `.\urbanflow.ps1 eliminar` y confirmar el mensaje **LIMPIEZA COMPLETA**.

El clúster se autodestruye a las 4 horas de creado: lance `todo` con tiempo, pero no tan temprano que se apague antes de exponer.

## Datos

- NYC Taxi & Limousine Commission, *TLC Trip Record Data* (yellow taxi), meses 2025-01 a 2025-06 por defecto.
- Las predicciones estiman **viajes registrados esperados**, no la demanda insatisfecha.
