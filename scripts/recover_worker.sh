#!/usr/bin/env bash
# Restore Hadoop daemons and the ORIGINAL expiry after a reboot; never extend it.
set -euo pipefail
deadline=${1:?Original UTC shutdown deadline required}
remaining=$(( $(date -d "$deadline" +%s) - $(date +%s) ))
[ "$remaining" -gt 60 ] || { echo 'Original expiry reached; recovery refused.'; exit 2; }
minutes=$((remaining / 60))
shutdown -h "+$minutes"
source /etc/profile.d/hadoop.sh
sudo -iu hadoop env HADOOP_HEAPSIZE_MAX=256 HDFS_DATANODE_OPTS=-Xmx256m \
  /opt/hadoop/bin/hdfs --daemon start datanode >/dev/null 2>&1 || true
sudo -iu hadoop env HADOOP_HEAPSIZE_MAX=256 YARN_NODEMANAGER_OPTS=-Xmx256m \
  /opt/hadoop/bin/yarn --daemon start nodemanager
sleep 2
sudo -iu hadoop jps | grep -q NodeManager
sudo -iu hadoop jps | grep -q DataNode
free -m
echo WORKER_RECOVERED
