#!/bin/bash
# Ejecuta un paso del pipeline UrbanFlow en el maestro, como usuario hadoop.
# Uso: pipeline.sh cargar 2025-01,2025-02 | etl 2025-01,2025-02 | analisis | sql | grafos | modelo | streaming | empaquetar
set -euo pipefail
source /etc/profile.d/hadoop.sh
source /etc/profile.d/spark.sh

APP=/home/hadoop/urbanflow
RES="${URBANFLOW_RESULTS:-$APP/resultados}"
LOGS="${URBANFLOW_LOGS:-$APP/logs}"
mkdir -p "$RES" "$LOGS"
PASO="${1:?Indica el paso}"
MESES="${2:-}"
BASE_TLC="https://d37ci6vzurychx.cloudfront.net"

registrar_tiempo() { echo "$1,$2" >> "$RES/tiempos_pipeline.csv"; }

# GraphFrames: API de grafos para Python sobre GraphX. Un solo jar (trae el código Python)
# que solo depende de librerías incluidas en Spark; se fija la versión y su SHA-256.
GF_VERSION="0.8.4-spark3.5-s_2.12"
GF_SHA256="1a4826bcd839d206d0c342d7b5195c2ff06a28866a7c81f5a49d839fa1e931b7"
GF_JAR="$APP/lib/graphframes-$GF_VERSION.jar"
preparar_graphframes() {
  [ -f "$GF_JAR" ] && return 0
  mkdir -p "$APP/lib"
  echo "[UrbanFlow] Descargando GraphFrames $GF_VERSION desde spark-packages..."
  curl -fsSL --retry 4 --retry-delay 5 -o "$GF_JAR.tmp" \
    "https://repos.spark-packages.org/graphframes/graphframes/$GF_VERSION/graphframes-$GF_VERSION.jar" \
    || { echo "[UrbanFlow] ERROR: no se pudo descargar GraphFrames"; exit 1; }
  echo "$GF_SHA256  $GF_JAR.tmp" | sha256sum -c --quiet - \
    || { echo "[UrbanFlow] ERROR: GraphFrames no coincide con el SHA-256 esperado"; rm -f "$GF_JAR.tmp"; exit 1; }
  mv "$GF_JAR.tmp" "$GF_JAR"
}

ejecutar_spark() {
  local nombre=$1; shift
  local inicio fin rc
  inicio=$(date +%s)
  echo "[UrbanFlow] spark-submit $nombre en YARN (3 ejecutores)..."
  /opt/spark/bin/spark-submit --name "UrbanFlow - $nombre" "$@" > "$LOGS/$nombre.log" 2>&1 &
  local pid=$!
  # Muestra en vivo solo los mensajes del proyecto, no el registro interno de Spark.
  tail -n +1 -f "$LOGS/$nombre.log" --pid=$pid | grep --line-buffered '\[UrbanFlow\]' || true
  rc=0; wait $pid || rc=$?
  fin=$(date +%s)
  if [ $rc -ne 0 ]; then
    echo "[UrbanFlow] ERROR en $nombre (código $rc). Últimas líneas del registro:"
    grep -E 'Error|Exception|Traceback|File "' "$LOGS/$nombre.log" | tail -25
    exit $rc
  fi
  registrar_tiempo "$nombre" $((fin - inicio))
  echo "[UrbanFlow] $nombre terminado en $((fin - inicio)) s"
}

case "$PASO" in
  cargar)
    [ -n "$MESES" ] || { echo 'Faltan los meses'; exit 2; }
    rm -f "$RES/tiempos_pipeline.csv"
    inicio=$(date +%s)
    mkdir -p "$APP/descargas"
    hdfs dfs -mkdir -p /urbanflow/crudo/yellow /urbanflow/referencia
    for MES in ${MESES//,/ }; do
      ARCH="yellow_tripdata_${MES}.parquet"
      if hdfs dfs -test -e "/urbanflow/crudo/yellow/$ARCH"; then
        echo "[UrbanFlow] $ARCH ya está en HDFS"; continue
      fi
      echo "[UrbanFlow] Descargando $ARCH desde NYC TLC..."
      curl -fsSL --retry 4 --retry-delay 5 -o "$APP/descargas/$ARCH" "$BASE_TLC/trip-data/$ARCH" || { echo "[UrbanFlow] ERROR: no se pudo descargar $ARCH"; exit 1; }
      hdfs dfs -put -f "$APP/descargas/$ARCH" "/urbanflow/crudo/yellow/$ARCH" || exit 1
      rm -f "$APP/descargas/$ARCH"
    done
    curl -fsSL --retry 4 -o "$APP/descargas/taxi_zone_lookup.csv" "$BASE_TLC/misc/taxi_zone_lookup.csv" || exit 1
    hdfs dfs -put -f "$APP/descargas/taxi_zone_lookup.csv" /urbanflow/referencia/taxi_zone_lookup.csv
    echo "[UrbanFlow] Datos crudos en HDFS (replicación x3 en los 3 DataNodes):"
    hdfs dfs -du -h /urbanflow/crudo/yellow | sed 's/^/[UrbanFlow]   /'
    hdfs dfs -du -s /urbanflow/crudo/yellow | awk '{print $1}' > "$RES/bytes_crudos.txt"
    registrar_tiempo cargar $(( $(date +%s) - inicio ))
    ;;
  etl)
    [ -n "$MESES" ] || { echo 'Faltan los meses'; exit 2; }
    ejecutar_spark etl "$APP/spark/etl.py" --meses "$MESES" --resultados "$RES"
    ;;
  perfil)
    ejecutar_spark perfil "$APP/spark/perfil_portal.py" --resultados "$RES"
    ;;
  analisis)
    ejecutar_spark analisis "$APP/spark/analisis.py" --resultados "$RES"
    ;;
  sql)
    ejecutar_spark sql "$APP/spark/consultas_sql.py" --resultados "$RES"
    ;;
  grafos)
    preparar_graphframes
    ejecutar_spark grafos --jars "$GF_JAR" --py-files "$GF_JAR" "$APP/spark/grafos.py" --resultados "$RES"
    ;;
  modelo)
    ejecutar_spark modelo "$APP/spark/modelo.py" --resultados "$RES"
    ;;
  streaming)
    ejecutar_spark streaming "$APP/spark/streaming.py" --resultados "$RES"
    ;;
  empaquetar)
    cd "$APP" && tar czf /tmp/urbanflow-resultados.tgz resultados logs
    chmod 644 /tmp/urbanflow-resultados.tgz
    echo "[UrbanFlow] Resultados empaquetados: $(du -h /tmp/urbanflow-resultados.tgz | cut -f1)"
    ;;
  *)
    echo "Paso desconocido: $PASO"; exit 2 ;;
esac
