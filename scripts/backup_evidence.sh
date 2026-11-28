#!/usr/bin/env bash
# Run as hadoop after completed jobs. Keeps models, city events and run evidence locally.
set -euo pipefail
source /etc/profile.d/hadoop.sh
folder=$(mktemp -d /tmp/urbanflow-evidencia.XXXXXXXX)
hdfs dfs -get /urbanflow/modelos "$folder/"
if hdfs dfs -test -d /urbanflow/city; then
  hdfs dfs -get /urbanflow/city "$folder/"
fi
cp -a /var/lib/urbanflow/runs "$folder/"
tar -czf "$folder.tgz" -C "$folder" .
chmod 0644 "$folder.tgz"
echo "EVIDENCE_ARCHIVE=$folder.tgz"
