#!/usr/bin/env bash
# Run as hadoop after completed jobs. Keeps replay models and run evidence locally.
set -euo pipefail
source /etc/profile.d/hadoop.sh
folder=$(mktemp -d /tmp/urbanflow-evidencia.XXXXXXXX)
mkdir -p "$folder/modelos"
hdfs dfs -get /urbanflow/modelos/replay "$folder/modelos/"
cp -a /var/lib/urbanflow/runs "$folder/"
tar -czf "$folder.tgz" -C "$folder" .
chmod 0644 "$folder.tgz"
echo "EVIDENCE_ARCHIVE=$folder.tgz"
