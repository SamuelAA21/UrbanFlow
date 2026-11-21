[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('menu', 'portal', 'admin', 'respaldo', 'todo', 'desplegar', 'procesar', 'paso', 'demo', 'streaming', 'resultados', 'dashboard', 'estado', 'conectar', 'eliminar')]
    [string]$Accion = 'menu',
    [string]$Region = 'us-east-1',
    [string]$PortalCidr = '',
    [string]$Meses = '2025-01,2025-02,2025-03,2025-04,2025-05,2025-06',
    [ValidateSet('cargar', 'etl', 'analisis', 'sql', 'grafos', 'modelo', 'streaming')][string]$Paso = 'etl',
    [string]$TipoMaestro = 'm7i-flex.large',
    [string]$TipoWorker = 'c7i-flex.large',
    [ValidateRange(1, 12)][int]$HorasMaximas = 4
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$env:AWS_PAGER = ''
$Raiz = Split-Path -Parent $MyInvocation.MyCommand.Path
$MotorScript = Join-Path $Raiz 'motor-cluster.ps1'
$EstadoDir = Join-Path $Raiz 'estado'
$ResultadosDir = Join-Path $Raiz 'resultados'
$Cluster = 'urbanflow-portal'
$AppRemota = '/home/hadoop/urbanflow'

function Step([string]$Texto) { Write-Host "`n==> $Texto" -ForegroundColor Cyan }
function Require([string]$Comando) {
    if (-not (Get-Command $Comando -ErrorAction SilentlyContinue)) { throw "Falta el comando '$Comando'." }
}
function Run-Aws([string[]]$ArgsList) {
    $old = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $raw = & aws @ArgsList 2>&1
        $code = $LASTEXITCODE
    } finally { $ErrorActionPreference = $old }
    if ($code -ne 0) { throw "AWS CLI falló: aws $($ArgsList -join ' ')`n$($raw -join "`n")" }
    return ($raw -join "`n")
}
function Test-Login {
    Require 'aws'
    try { $identity = (Run-Aws @('sts', 'get-caller-identity', '--region', $Region, '--output', 'json')) | ConvertFrom-Json }
    catch { throw "No hay una sesión válida de AWS. Ejecuta 'aws login' y vuelve a intentarlo." }
    Write-Host "Cuenta AWS: $($identity.Account) | Región: $Region"
}
function State-Path { Join-Path $EstadoDir "$Cluster.json" }
function Read-State {
    if (-not (Test-Path (State-Path))) { throw 'No hay un clúster desplegado. Usa primero: .\urbanflow.ps1 desplegar' }
    Get-Content -LiteralPath (State-Path) -Raw | ConvertFrom-Json
}
function Ensure-Key {
    Require 'ssh-keygen'
    New-Item -ItemType Directory -Path $EstadoDir -Force | Out-Null
    $key = Join-Path $EstadoDir "$Cluster-eic"
    if (-not (Test-Path $key)) {
        # Windows PowerShell 5.1 descarta los argumentos vacíos ('') al llamar programas nativos:
        # allí la contraseña vacía se escribe '""'. PowerShell 7 sí pasa '' correctamente.
        $sinClave = if ($PSVersionTable.PSVersion.Major -ge 7) { '' } else { '""' }
        & ssh-keygen -q -t ed25519 -N $sinClave -C "$Cluster-eic" -f $key
        if ($LASTEXITCODE -ne 0) { throw 'No se pudo crear la llave SSH temporal.' }
    }
    return $key
}
function Get-MasterInfo {
    $state = Read-State
    $json = Run-Aws @('ec2', 'describe-instances', '--instance-ids', $state.Instances.maestro, '--region', $Region, '--output', 'json')
    return (($json | ConvertFrom-Json).Reservations[0].Instances[0])
}
function Grant-Key($Info, [string]$Key) {
    # EC2 Instance Connect a veces responde ServiceException transitorio: se reintenta.
    for ($i = 1; $i -le 5; $i++) {
        try { Grant-KeyOnce $Info $Key; return } catch { if ($i -eq 5) { throw }; Start-Sleep -Seconds (3 * $i) }
    }
}
function Grant-KeyOnce($Info, [string]$Key) {
    Run-Aws @('ec2-instance-connect', 'send-ssh-public-key', '--instance-id', $Info.InstanceId,
        '--availability-zone', $Info.Placement.AvailabilityZone, '--instance-os-user', 'ec2-user',
        '--ssh-public-key', "file://$Key.pub", '--region', $Region, '--output', 'json') | Out-Null
}
$SshOpts = @('-o', 'StrictHostKeyChecking=accept-new', '-o', 'ServerAliveInterval=30', '-o', 'LogLevel=ERROR')
function Send-Files([string[]]$Locales, [string]$Remoto) {
    Require 'scp'
    $info = Get-MasterInfo; $key = Ensure-Key; Grant-Key $info $key
    & scp -q @SshOpts -i $key @Locales "ec2-user@$($info.PublicIpAddress):$Remoto"
    if ($LASTEXITCODE -ne 0) { throw "No se pudieron copiar archivos al maestro." }
}
function Receive-File([string]$Remoto, [string]$Local) {
    Require 'scp'
    $info = Get-MasterInfo; $key = Ensure-Key
    for ($i = 1; $i -le 4; $i++) {
        Grant-Key $info $key
        & scp -q @SshOpts -i $key "ec2-user@$($info.PublicIpAddress):$Remoto" $Local
        if ($LASTEXITCODE -eq 0 -and (Test-Path $Local)) { return }
        Start-Sleep -Seconds (2 * $i)
    }
    throw "No se pudo descargar $Remoto."
}
function Run-Remote([string]$Command, [switch]$Interactive) {
    Require 'ssh'
    $info = Get-MasterInfo; $key = Ensure-Key; Grant-Key $info $key
    $sshArgs = @()
    if ($Interactive) { $sshArgs += '-t' }
    $sshArgs += $SshOpts + @('-i', $key, "ec2-user@$($info.PublicIpAddress)")
    if ($Command) { $sshArgs += $Command }
    & ssh @sshArgs
    if ($LASTEXITCODE -ne 0) { throw "El comando remoto falló (código $LASTEXITCODE)." }
}
function Run-Motor([string]$Action) {
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $MotorScript $Action -Region $Region -Cluster $Cluster `
        -TipoMaestro $TipoMaestro -TipoWorker $TipoWorker -HorasMaximas $HorasMaximas
    if ($LASTEXITCODE -ne 0) { throw "La acción '$Action' del clúster falló." }
}
function Cluster-Activo {
    if (-not (Test-Path (State-Path))) { return $false }
    $state = Read-State
    if ($state.Status -ne 'activo' -or -not $state.Instances.maestro) { return $false }
    # La autodestrucción termina las instancias sin tocar el estado local: se confirma con AWS.
    try {
        $json = Run-Aws @('ec2', 'describe-instances', '--instance-ids', $state.Instances.maestro, '--region', $Region, '--output', 'json')
        $maestro = (($json | ConvertFrom-Json).Reservations[0].Instances[0]).State.Name
    } catch { $maestro = 'inexistente' }
    if ($maestro -eq 'running') { return $true }
    Write-Host "El estado local dice 'activo', pero el maestro está '$maestro' (probable autodestrucción a las $($state.AutoDeleteHours) h)." -ForegroundColor Yellow
    return $false
}
function Assert-Activo {
    if (-not (Cluster-Activo)) { throw 'No hay un clúster activo. Ejecuta primero: .\urbanflow.ps1 desplegar (o todo, que despliega y procesa).' }
}

function Deploy {
    Test-Login
    if (Cluster-Activo) { Write-Host 'El clúster ya está activo; se reutiliza.' -ForegroundColor Green }
    else {
        Step "Desplegando Hadoop (HDFS + YARN): 1 maestro $TipoMaestro + 3 workers $TipoWorker"
        Run-Motor 'desplegar'
    }
    Step 'Instalando Apache Spark 3.5 sobre YARN'
    Send-Files @((Join-Path $Raiz 'cluster\instalar-spark.sh')) '/tmp/'
    Run-Remote "sed -i 's/\r$//' /tmp/instalar-spark.sh; bash /tmp/instalar-spark.sh"
    Write-Host "`nCLÚSTER URBANFLOW LISTO." -ForegroundColor Green
}

function Upload-Code {
    Step 'Subiendo el código PySpark al maestro'
    $archivos = @(Get-ChildItem (Join-Path $Raiz 'spark') -Filter *.py | ForEach-Object FullName) + (Join-Path $Raiz 'cluster\pipeline.sh')
    Run-Remote 'rm -rf /tmp/uf-codigo; mkdir -p /tmp/uf-codigo'
    Send-Files $archivos '/tmp/uf-codigo/'
    # sed elimina retornos de carro de Windows por si algún archivo los trae.
    Run-Remote ("sudo -iu hadoop mkdir -p $AppRemota/spark; sudo sed -i 's/\r$//' /tmp/uf-codigo/*; " +
        "sudo install -o hadoop -g hadoop -m 644 /tmp/uf-codigo/*.py $AppRemota/spark/; " +
        "sudo install -o hadoop -g hadoop -m 755 /tmp/uf-codigo/pipeline.sh $AppRemota/pipeline.sh")
}
function Run-Step([string]$Paso, [string]$Argumento = '') {
    Run-Remote "sudo -iu hadoop bash $AppRemota/pipeline.sh $Paso $Argumento"
}
function Download-Results {
    Step 'Descargando resultados al computador'
    Run-Step 'empaquetar'
    New-Item -ItemType Directory -Path $ResultadosDir -Force | Out-Null
    $tgz = Join-Path $EstadoDir 'urbanflow-resultados.tgz'
    Receive-File '/tmp/urbanflow-resultados.tgz' $tgz
    & tar -xzf $tgz -C $Raiz
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo descomprimir el paquete de resultados.' }
    $estado = Read-State
    [ordered]@{
        cluster = "1 maestro $TipoMaestro + 3 workers $TipoWorker"; region = $Region
        meses = $Meses -split ','; descargado = (Get-Date).ToString('s'); ip_maestro = $estado.MasterPublicIp
    } | ConvertTo-Json | Set-Content -Encoding UTF8 (Join-Path $ResultadosDir 'ejecucion.json')
    try { & python (Join-Path $Raiz 'presentacion\generar_datos.py') } catch { Write-Warning 'No se pudieron actualizar los datos de la presentación.' }
    Write-Host "Resultados en $ResultadosDir" -ForegroundColor Green
}

function Process-All {
    Test-Login
    Assert-Activo
    $ini = Get-Date
    Upload-Code
    Step "1/7 Ingesta: descargando los meses $Meses de NYC TLC directamente a HDFS"
    Run-Step 'cargar' $Meses
    Step '2/7 ETL con PySpark: limpieza y agregación zona x hora'
    Run-Step 'etl' $Meses
    Step '3/7 Análisis: patrones, RDD vs DataFrame y plan de Catalyst'
    Run-Step 'analisis'
    Step '4/7 Spark SQL: vistas, funciones de ventana y SQL vs DataFrame'
    Run-Step 'sql'
    Step '5/7 Grafos con GraphFrames/GraphX: PageRank, patrones y comunidades'
    Run-Step 'grafos'
    Step '6/7 Modelo: Random Forest y GBT con Spark MLlib'
    Run-Step 'modelo'
    Step '7/7 Structured Streaming con watermark'
    Run-Step 'streaming'
    Download-Results
    Write-Host ("`nPIPELINE COMPLETO en {0:N1} minutos." -f ((Get-Date) - $ini).TotalMinutes) -ForegroundColor Green
}
# Demo para exponer en vivo: los pasos cortos y vistosos sobre los datos ya procesados.
# Requiere haber ejecutado antes «todo» o «procesar» (ETL y modelo, los pasos largos).
function Live-Demo {
    Test-Login
    Assert-Activo
    $ini = Get-Date
    Upload-Code
    Step 'Demo en vivo 1/3: Spark SQL (vistas, funciones de ventana y SQL vs DataFrame)'
    Run-Step 'sql'
    Step 'Demo en vivo 2/3: grafos con GraphX (PageRank, patrones y comunidades)'
    Run-Step 'grafos'
    Step 'Demo en vivo 3/3: Structured Streaming con watermark'
    Run-Step 'streaming'
    Download-Results
    Write-Host ("`nDEMO EN VIVO COMPLETA en {0:N1} minutos." -f ((Get-Date) - $ini).TotalMinutes) -ForegroundColor Green
}
function Stream-Demo {
    Test-Login
    Assert-Activo
    Upload-Code
    Step 'Demostración de Structured Streaming'
    Run-Step 'streaming'
    Download-Results
}

function Start-Dashboard {
    $venv = Join-Path $Raiz '.venv'
    $py = Join-Path $venv 'Scripts\python.exe'
    if (-not (Test-Path $py)) {
        Step 'Preparando el entorno del dashboard (solo la primera vez)'
        # Se usa el Python del sistema: Smart App Control bloquea intérpretes descargados sin firma.
        & python -m venv $venv
        & $py -m pip install --quiet -r (Join-Path $Raiz 'dashboard\requirements.txt')
        if ($LASTEXITCODE -ne 0) { throw 'No se pudieron instalar las dependencias del dashboard.' }
    }
    if (-not (Test-Path (Join-Path $ResultadosDir 'kpis.json'))) { throw 'Todavía no hay resultados. Ejecuta: .\urbanflow.ps1 procesar' }
    Step 'Abriendo el dashboard en http://localhost:8501'
    # Streamlit lee el tema de .streamlit\config.toml en la carpeta actual: se ejecuta desde la raíz.
    Push-Location $Raiz
    try { & $py -m streamlit run (Join-Path $Raiz 'dashboard\app.py') --server.headless false --browser.gatherUsageStats false }
    finally { Pop-Location }
}

function Show-State {
    Test-Login
    if (Test-Path (State-Path)) {
        Run-Motor 'estado'
        $state = Read-State
        if ($state.CloudFrontDomain) { Write-Host "Portal HTTPS: https://$($state.CloudFrontDomain)/" -ForegroundColor Green }
    } else { Write-Host 'No hay clúster desplegado.' }
}
function Remove-All {
    Test-Login
    $edgeScript = Join-Path $Raiz 'scripts\cloudfront-portal.ps1'
    if (Test-Path (State-Path) -and (Test-Path $edgeScript)) {
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $edgeScript delete -Region $Region
        if ($LASTEXITCODE -ne 0) { throw 'No se pudo retirar CloudFront; se detiene la limpieza para no dejar un recurso huérfano.' }
    }
    if (Test-Path (State-Path)) { Run-Motor 'eliminar' }
    else { Write-Host 'No hay estado local; se verifica en AWS por etiquetas.'; Run-Motor 'eliminar' }
    Write-Host 'Los resultados locales se conservan en la carpeta resultados\ para el dashboard.' -ForegroundColor Green
}

function Menu {
    Write-Host @'

URBANFLOW - Predicción de demanda de taxis con Apache Spark en AWS
1. TODO: desplegar clúster + procesar + descargar resultados
2. Desplegar clúster (Hadoop + Spark, 1 maestro + 3 workers)
3. Procesar (ingesta, ETL, análisis, SQL, grafos, modelo, streaming)
4. Solo demo de Structured Streaming
5. Abrir dashboard
6. Ver estado y enlaces (YARN, HDFS, Spark History)
7. Conectar por SSH al maestro
8. ELIMINAR TODO EN AWS (deja la cuenta sin costos)
9. Demo en vivo: Spark SQL + grafos + streaming (requiere haber procesado antes)
10. Respaldar la base del portal en este computador
0. Salir
'@
    switch (Read-Host 'Elige una opción') {
        '1' { Deploy; Process-All }
        '2' { Deploy }
        '3' { Process-All }
        '4' { Stream-Demo }
        '5' { Start-Dashboard }
        '6' { Show-State }
        '7' { Run-Remote '' -Interactive }
        '8' { Remove-All }
        '9' { Live-Demo }
        '10' { Backup-PortalDatabase }
        '0' { return }
        default { throw 'Opción no válida.' }
    }
}

. (Join-Path $Raiz 'cluster\portal-actions.ps1')

switch ($Accion) {
    'portal' { Deploy-Portal }
    'admin' { Open-PortalAdmin }
    'respaldo' { Backup-PortalDatabase }
    'menu' { Menu }
    'todo' { Deploy; Process-All }
    'desplegar' { Deploy }
    'procesar' { Process-All }
    'streaming' { Stream-Demo }
    'demo' { Live-Demo }
    'paso' { Test-Login; Assert-Activo; Upload-Code; Run-Step $Paso $(if ($Paso -in 'cargar', 'etl') { $Meses } else { '' }); Download-Results }
    'resultados' { Test-Login; Assert-Activo; Download-Results }
    'dashboard' { Start-Dashboard }
    'estado' { Show-State }
    'conectar' { Run-Remote '' -Interactive }
    'eliminar' { Remove-All }
}
