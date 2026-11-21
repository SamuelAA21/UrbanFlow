#!/usr/bin/env bash
set -euo pipefail
source_dir=${1:?Falta el directorio del paquete}
months=${2:?Faltan los meses de TLC}
app=/home/hadoop/urbanflow
web=/opt/urbanflow
runtime=/var/lib/urbanflow
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
  "URBANFLOW_ADMIN=1" ""
make_unit worker hadoop "$python -m portal.worker" "URBANFLOW_HOME=$app" ""
systemctl daemon-reload
for unit in public admin; do
  systemctl enable "urbanflow-$unit.service"
  systemctl restart "urbanflow-$unit.service"
done
systemctl enable "urbanflow-worker.service"
systemctl is-active --quiet "urbanflow-worker.service" || systemctl start "urbanflow-worker.service"
curl --fail --silent --show-error --retry 8 --retry-connrefused http://127.0.0.1:8080/health
curl --fail --silent --show-error --retry 8 --retry-connrefused http://127.0.0.1:8090/health
echo
