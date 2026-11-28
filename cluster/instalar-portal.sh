#!/usr/bin/env bash
set -euo pipefail
source_dir=${1:?Falta el directorio del paquete}
months=${2:?Faltan los meses de TLC}
app=/home/hadoop/urbanflow
web=/opt/urbanflow
runtime=/var/lib/urbanflow
if [ -f "$runtime/urbanflow.sqlite3" ]; then
  active=$(python3 -c 'import sqlite3,sys; c=sqlite3.connect("file:"+sys.argv[1]+"?mode=ro",uri=True); tables={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type=\x27table\x27")}; jobs=c.execute("SELECT count(*) FROM jobs WHERE status IN (\x27running\x27,\x27queued\x27)").fetchone()[0]; enabled=c.execute("SELECT count(*) FROM replay_schedule WHERE enabled=1").fetchone()[0] if "replay_schedule" in tables else 0; print(jobs+enabled)' "$runtime/urbanflow.sqlite3")
  [ "$active" = 0 ] || { echo 'Deten la programacion y espera a que terminen los trabajos antes de actualizar el motor.'; exit 2; }
  systemctl stop urbanflow-worker.service || true
  systemctl stop urbanflow-city.service 2>/dev/null || true
fi
dnf install -y python3-pip
getent group urbanflow-read >/dev/null || groupadd --system urbanflow-read
id urbanflow-web >/dev/null 2>&1 || useradd --system --gid urbanflow-read --no-create-home urbanflow-web
install -d -m 2750 -o hadoop -g urbanflow-read "$runtime"
install -d -m 755 "$web"
cp -r "$source_dir/portal" "$web/"
install -d -m 755 "$web/dashboard"
install -m 644 "$source_dir/dashboard/taxi_zones.geojson" "$web/dashboard/"
python3 -m venv "$web/venv"
"$web/venv/bin/python" -m pip install --disable-pip-version-check -r "$web/portal/requirements.txt"

install -d -o hadoop -g hadoop "$app" "$app/spark"
install -o hadoop -g hadoop -m 755 "$source_dir/cluster/pipeline.sh" "$app/pipeline.sh"
install -o hadoop -g hadoop -m 644 "$source_dir"/spark/*.py "$app/spark/"
environment="URBANFLOW_RUNTIME=$runtime"
yarn_environment="URBANFLOW_YARN=http://10.78.1.10:8088"
cd "$web"
sudo -u hadoop env "$environment" "$yarn_environment" bash -c 'umask 027; exec "$@"' bash \
  "$web/venv/bin/python" -m portal.bootstrap --prepare "$months"

make_unit() {
  local name=$1 user=$2 command=$3 home=$4 sandbox=$5
  cat > "/etc/systemd/system/urbanflow-$name.service" <<EOF
[Unit]
Description=UrbanFlow $name
After=network-online.target
Wants=network-online.target

[Service]
User=$user
WorkingDirectory=$web
Environment=$environment
Environment=$yarn_environment
Environment=$home
ExecStart=$command
Restart=on-failure
RestartSec=3
UMask=0027
NoNewPrivileges=true
PrivateTmp=true
$sandbox

[Install]
WantedBy=multi-user.target
EOF
}

python="$web/venv/bin/python"
make_unit public urbanflow-web "$python -m uvicorn portal.app:app --host 0.0.0.0 --port 8080 --no-proxy-headers" \
  "URBANFLOW_HOME=$web" $'ProtectSystem=strict\nProtectHome=true'
make_unit admin hadoop "$python -m uvicorn portal.app:app --host 127.0.0.1 --port 8090 --no-proxy-headers" \
  "URBANFLOW_ADMIN=1" 'Environment=URBANFLOW_REPLAY_ENGINE=1'
make_unit worker hadoop "$python -m portal.worker" "URBANFLOW_HOME=$app" ""
make_unit city hadoop "$python -m portal.city_service" "URBANFLOW_HOME=$app" ""
systemctl daemon-reload
for unit in public admin; do
  systemctl enable "urbanflow-$unit.service"
  systemctl restart "urbanflow-$unit.service"
done
systemctl enable "urbanflow-worker.service"
systemctl restart "urbanflow-worker.service"
systemctl enable urbanflow-city.service
systemctl restart urbanflow-city.service
curl --fail --silent --show-error --retry 8 --retry-connrefused http://127.0.0.1:8080/health
curl --fail --silent --show-error --retry 8 --retry-connrefused http://127.0.0.1:8090/health
echo
