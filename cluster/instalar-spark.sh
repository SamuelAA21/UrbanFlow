#!/bin/bash
# Instala Apache Spark sobre el clúster Hadoop/YARN ya desplegado (se ejecuta en el maestro).
set -euo pipefail
SPARK_VERSION="3.5.6"
ARCHIVE="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${ARCHIVE}"

if [ ! -x /opt/spark/bin/spark-submit ]; then
  echo "Descargando Apache Spark ${SPARK_VERSION}..."
  curl -fL --retry 4 --retry-delay 5 "$URL" -o "/tmp/$ARCHIVE"
  sudo tar -xzf "/tmp/$ARCHIVE" -C /opt
  sudo ln -sfn "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark
  sudo chown -R hadoop:hadoop "/opt/spark-${SPARK_VERSION}-bin-hadoop3"
  rm -f "/tmp/$ARCHIVE"
fi
sudo tee /etc/profile.d/spark.sh >/dev/null <<'EOF'
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
EOF
sudo -iu hadoop bash -lc 'source /etc/profile.d/hadoop.sh; hdfs dfs -mkdir -p /spark-events; hdfs dfs -chmod 1777 /spark-events'

# Recursos pensados para 3 workers c7i-flex.large (2 vCPU, 4 GiB, YARN 3072 MB):
# un ejecutor de 1600 MB + 384 MB de overhead por worker y el ApplicationMaster en 1 GB.
sudo tee /opt/spark/conf/spark-defaults.conf >/dev/null <<'EOF'
spark.master                           yarn
spark.submit.deployMode                client
spark.executor.instances               3
spark.executor.cores                   2
spark.executor.memory                  1600m
spark.driver.memory                    2g
spark.yarn.am.memory                   512m
spark.sql.shuffle.partitions           24
spark.sql.session.timeZone             UTC
spark.serializer                       org.apache.spark.serializer.KryoSerializer
spark.eventLog.enabled                 true
spark.eventLog.dir                     hdfs:///spark-events
spark.history.fs.logDirectory          hdfs:///spark-events
spark.history.ui.port                  18080
spark.yarn.historyServer.address       ${hadoopconf-yarn.resourcemanager.hostname}:18080
spark.yarn.archive                     hdfs:///spark/spark-jars.zip
EOF
sudo chown hadoop:hadoop /opt/spark/conf/spark-defaults.conf
if ! sudo -iu hadoop bash -lc 'source /etc/profile.d/hadoop.sh; hdfs dfs -test -e /spark/spark-jars.zip'; then
  echo 'Creando un archivo compartido de librerías Spark en HDFS...'
  sudo -iu hadoop bash -lc 'jar cf /tmp/spark-jars.zip -C /opt/spark/jars .; hdfs dfs -mkdir -p /spark; hdfs dfs -put -f /tmp/spark-jars.zip /spark/spark-jars.zip; rm -f /tmp/spark-jars.zip'
fi
sudo -iu hadoop bash -lc 'source /etc/profile.d/hadoop.sh; source /etc/profile.d/spark.sh; /opt/spark/sbin/stop-history-server.sh >/dev/null 2>&1 || true; /opt/spark/sbin/start-history-server.sh' >/dev/null
python3 -c 'import numpy; print("numpy", numpy.__version__)'
echo "Spark instalado: $(/opt/spark/bin/spark-submit --version 2>&1 | grep -m1 version || true)"
