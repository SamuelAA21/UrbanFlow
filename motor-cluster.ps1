[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('menu', 'desplegar', 'estado', 'conectar', 'eliminar')]
    [string]$Accion = 'menu',

    [string]$Region = 'us-east-1',
    [string]$Cluster = 'urbanflow',
    [string]$Perfil = '',
    [ValidateSet('maestro', 'worker1', 'worker2', 'worker3')]
    [string]$Nodo = 'maestro',
    [string]$TipoMaestro = 'm7i-flex.large',
    [string]$TipoWorker = 'c7i-flex.large',
    [string]$HadoopVersion = '3.5.0',
    [ValidateRange(1, 12)]
    [int]$HorasMaximas = 4
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$env:AWS_PAGER = ''
$env:AWS_DEFAULT_REGION = $Region
if ($Perfil) { $env:AWS_PROFILE = $Perfil }

$Raiz = Split-Path -Parent $MyInvocation.MyCommand.Path
$EstadoDir = Join-Path $Raiz 'estado'
$EstadoArchivo = Join-Path $EstadoDir ("{0}.json" -f $Cluster)
$ProyectoTag = 'urbanflow'

function Write-Step([string]$Mensaje) {
    Write-Host "`n==> $Mensaje" -ForegroundColor Cyan
}

function Require-Command([string]$Nombre) {
    if (-not (Get-Command $Nombre -ErrorAction SilentlyContinue)) {
        throw "No se encontró '$Nombre'. Instálalo y vuelve a ejecutar el script."
    }
}

function Invoke-Aws {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Argumentos)
    $preferenciaAnterior = $ErrorActionPreference
    try {
        # Windows PowerShell 5.1 convierte stderr de programas nativos en ErrorRecord.
        # Se captura como texto para poder mostrar el mensaje real de AWS CLI.
        $ErrorActionPreference = 'Continue'
        $salidaCruda = & aws @Argumentos 2>&1
        $codigo = $LASTEXITCODE
        $salida = @($salidaCruda | ForEach-Object { $_.ToString() })
    } finally {
        $ErrorActionPreference = $preferenciaAnterior
    }
    if ($codigo -ne 0) {
        throw "AWS CLI falló: aws $($Argumentos -join ' ')`n$($salida -join "`n")"
    }
    return ($salida -join "`n")
}

function Invoke-AwsJson {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Argumentos)
    $texto = Invoke-Aws @Argumentos
    if ([string]::IsNullOrWhiteSpace($texto)) { return $null }
    return ($texto | ConvertFrom-Json)
}

function Save-State($Estado) {
    New-Item -ItemType Directory -Force -Path $EstadoDir | Out-Null
    $Estado | ConvertTo-Json -Depth 12 | Set-Content -Encoding UTF8 $EstadoArchivo
}

function Set-StateValue($Estado, [string]$Nombre, $Valor) {
    if ($Estado -is [Collections.IDictionary]) {
        $Estado[$Nombre] = $Valor
    } else {
        $Estado | Add-Member -NotePropertyName $Nombre -NotePropertyValue $Valor -Force
    }
}

function Get-InstanceIdsFromState($Estado) {
    if ($Estado.Instances -is [Collections.IDictionary]) {
        return @($Estado.Instances.Values)
    }
    return @($Estado.Instances.PSObject.Properties | ForEach-Object { $_.Value })
}

function Load-State {
    if (-not (Test-Path $EstadoArchivo)) {
        throw "No existe estado para '$Cluster'. Ejecuta primero: .\urbanflow.ps1 desplegar"
    }
    return (Get-Content -Raw $EstadoArchivo | ConvertFrom-Json)
}

function Write-JsonTemp($Objeto, [string]$Nombre) {
    New-Item -ItemType Directory -Force -Path $EstadoDir | Out-Null
    $ruta = Join-Path $EstadoDir $Nombre
    $json = $Objeto | ConvertTo-Json -Depth 15 -Compress
    [IO.File]::WriteAllText($ruta, $json, [Text.UTF8Encoding]::new($false))
    return $ruta
}

function ConvertTo-Base64([string]$Texto) {
    return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Texto))
}

function Get-CallerIp {
    return ((Invoke-RestMethod -Uri 'https://checkip.amazonaws.com' -TimeoutSec 15).Trim())
}

function Test-Login {
    Require-Command 'aws'
    try {
        return (Invoke-AwsJson sts get-caller-identity --output json)
    } catch {
        throw "No hay una sesión válida de AWS. Ejecuta 'aws login' y vuelve a intentarlo.`n$($_.Exception.Message)"
    }
}

function Get-CommonAz {
    $ofertas = Invoke-AwsJson ec2 describe-instance-type-offerings `
        --location-type availability-zone `
        --filters "Name=instance-type,Values=$TipoMaestro,$TipoWorker" `
        --region $Region --output json
    $porZona = $ofertas.InstanceTypeOfferings | Group-Object Location
    $necesarios = @(@($TipoMaestro, $TipoWorker) | Sort-Object -Unique).Count
    $zona = $porZona | Where-Object { @($_.Group.InstanceType | Sort-Object -Unique).Count -eq $necesarios } | Select-Object -First 1
    if (-not $zona) {
        throw "No encontré una zona de $Region que ofrezca $TipoMaestro y $TipoWorker. Cambia los parámetros de tipo."
    }
    return $zona.Name
}

function New-UserData {
    param(
        [ValidateSet('maestro', 'worker')][string]$Rol,
        [string]$Nombre,
        [string]$IpMaestro
    )
    $minutos = $HorasMaximas * 60
    $esMaestro = if ($Rol -eq 'maestro') { 'true' } else { 'false' }
    $script = @'
#!/bin/bash
set -euxo pipefail
exec > >(tee -a /var/log/hadoop-bootstrap.log /dev/console) 2>&1
shutdown -h +__MINUTOS__
hostnamectl set-hostname __NOMBRE__

dnf install -y java-17-amazon-corretto-devel tar gzip procps-ng python3
dnf install -y python3-numpy || { dnf install -y python3-pip && python3 -m pip install 'numpy<2'; }
id hadoop >/dev/null 2>&1 || useradd -m hadoop

cd /opt
curl -fL --retry 3 --retry-delay 5 --connect-timeout 20 \
  -o hadoop.tgz "https://dlcdn.apache.org/hadoop/common/hadoop-__VERSION__/hadoop-__VERSION__.tar.gz" \
  || curl -fL --retry 5 --retry-delay 5 \
  -o hadoop.tgz "https://archive.apache.org/dist/hadoop/common/hadoop-__VERSION__/hadoop-__VERSION__.tar.gz"
tar xzf hadoop.tgz
rm -f hadoop.tgz
ln -sfn "/opt/hadoop-__VERSION__" /opt/hadoop

JAVA_HOME_VALUE=$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")
cat >/etc/profile.d/hadoop.sh <<ENV
export JAVA_HOME=$JAVA_HOME_VALUE
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export PATH=\$PATH:/opt/hadoop/bin:/opt/hadoop/sbin
ENV
echo "export JAVA_HOME=$JAVA_HOME_VALUE" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

write_conf() {
  local file=$1; shift
  {
    echo '<?xml version="1.0"?>'
    echo '<configuration>'
    while [ "$#" -gt 0 ]; do
      printf '<property><name>%s</name><value>%s</value></property>\n' "$1" "$2"
      shift 2
    done
    echo '</configuration>'
  } > "/opt/hadoop/etc/hadoop/$file"
}

write_conf core-site.xml \
  fs.defaultFS hdfs://__MASTER_IP__:9000

write_conf hdfs-site.xml \
  dfs.replication 3 \
  dfs.namenode.name.dir file:///data/hdfs/nn \
  dfs.datanode.data.dir file:///data/hdfs/dn \
  dfs.namenode.datanode.registration.ip-hostname-check false

write_conf mapred-site.xml \
  mapreduce.framework.name yarn \
  mapreduce.application.classpath '$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*' \
  yarn.app.mapreduce.am.env HADOOP_MAPRED_HOME=/opt/hadoop \
  mapreduce.map.env HADOOP_MAPRED_HOME=/opt/hadoop \
  mapreduce.reduce.env HADOOP_MAPRED_HOME=/opt/hadoop \
  mapreduce.jobhistory.address __MASTER_IP__:10020 \
  mapreduce.jobhistory.webapp.address 0.0.0.0:19888

write_conf yarn-site.xml \
  yarn.resourcemanager.hostname __MASTER_IP__ \
  yarn.nodemanager.aux-services mapreduce_shuffle \
  yarn.nodemanager.resource.memory-mb 3072 \
  yarn.scheduler.minimum-allocation-mb 512 \
  yarn.scheduler.maximum-allocation-mb 3072 \
  yarn.nodemanager.vmem-check-enabled false \
  yarn.nodemanager.env-whitelist JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME

mkdir -p /data/hdfs/nn /data/hdfs/dn
chown -R hadoop:hadoop "/opt/hadoop-__VERSION__" /data/hdfs /home/hadoop

if __ES_MAESTRO__; then
  sudo -iu hadoop bash -lc 'hdfs namenode -format -nonInteractive -clusterId cluster-portable; hdfs --daemon start namenode; yarn --daemon start resourcemanager; mapred --daemon start historyserver'
else
  sudo -iu hadoop env HADOOP_HEAPSIZE_MAX=256 HDFS_DATANODE_OPTS=-Xmx256m bash -lc 'hdfs --daemon start datanode'
  sudo -iu hadoop env HADOOP_HEAPSIZE_MAX=256 YARN_NODEMANAGER_OPTS=-Xmx256m bash -lc 'yarn --daemon start nodemanager'
fi

touch /var/log/hadoop-ready
echo HADOOP_BOOTSTRAP_OK
'@
    $script = $script.Replace('__MINUTOS__', "$minutos").Replace('__NOMBRE__', $Nombre)
    $script = $script.Replace('__VERSION__', $HadoopVersion).Replace('__MASTER_IP__', $IpMaestro)
    return $script.Replace('__ES_MAESTRO__', $esMaestro)
}

function New-Instance {
    param(
        [string]$Nombre,
        [string]$Tipo,
        [string]$SubnetId,
        [string]$SecurityGroupId,
        [string]$Ami,
        [string]$UserData,
        [string]$RunId,
        [string]$PrivateIp
    )
    $tags = @(
        @{ Key = 'Name'; Value = $Nombre },
        @{ Key = 'Proyecto'; Value = $ProyectoTag },
        @{ Key = 'Cluster'; Value = $Cluster },
        @{ Key = 'RunId'; Value = $RunId },
        @{ Key = 'ManagedBy'; Value = 'urbanflow.ps1' }
    )
    $solicitud = @{
        ImageId = $Ami
        InstanceType = $Tipo
        MinCount = 1
        MaxCount = 1
        ClientToken = "$RunId-$Nombre"
        NetworkInterfaces = @(@{
            DeviceIndex = 0
            SubnetId = $SubnetId
            Groups = @($SecurityGroupId)
            AssociatePublicIpAddress = $true
            DeleteOnTermination = $true
        })
        BlockDeviceMappings = @(@{
            DeviceName = '/dev/xvda'
            Ebs = @{ VolumeSize = 20; VolumeType = 'gp3'; Encrypted = $true; DeleteOnTermination = $true }
        })
        MetadataOptions = @{ HttpTokens = 'required'; HttpEndpoint = 'enabled'; HttpPutResponseHopLimit = 1 }
        InstanceInitiatedShutdownBehavior = 'terminate'
        UserData = ConvertTo-Base64 $UserData
        TagSpecifications = @(
            @{ ResourceType = 'instance'; Tags = $tags },
            @{ ResourceType = 'volume'; Tags = $tags },
            @{ ResourceType = 'network-interface'; Tags = $tags }
        )
    }
    if ($PrivateIp) { $solicitud.NetworkInterfaces[0].PrivateIpAddress = $PrivateIp }
    $archivo = Write-JsonTemp $solicitud ("run-{0}.json" -f $Nombre)
    $respuesta = Invoke-AwsJson ec2 run-instances --cli-input-json "file://$archivo" --region $Region --output json
    return $respuesta.Instances[0].InstanceId
}

function New-Network($Estado, [string]$Az, [string]$CallerIp) {
    Write-Step 'Creando una red aislada para el laboratorio'
    $vpc = (Invoke-AwsJson ec2 create-vpc --cidr-block 10.78.0.0/16 --region $Region --output json).Vpc.VpcId
    $Estado.VpcId = $vpc; Save-State $Estado
    Invoke-Aws ec2 wait vpc-available --vpc-ids $vpc --region $Region | Out-Null
    $valorTrue = Write-JsonTemp @{ Value = $true } 'vpc-attribute-true.json'
    Invoke-Aws ec2 modify-vpc-attribute --vpc-id $vpc --enable-dns-support "file://$valorTrue" --region $Region | Out-Null
    Invoke-Aws ec2 modify-vpc-attribute --vpc-id $vpc --enable-dns-hostnames "file://$valorTrue" --region $Region | Out-Null
    Invoke-Aws ec2 create-tags --resources $vpc --tags "Key=Name,Value=$Cluster-vpc" "Key=Proyecto,Value=$ProyectoTag" "Key=Cluster,Value=$Cluster" --region $Region | Out-Null

    $subnet = (Invoke-AwsJson ec2 create-subnet --vpc-id $vpc --cidr-block 10.78.1.0/24 --availability-zone $Az --region $Region --output json).Subnet.SubnetId
    $Estado.SubnetId = $subnet; Save-State $Estado
    Invoke-Aws ec2 create-tags --resources $subnet --tags "Key=Name,Value=$Cluster-subnet" "Key=Proyecto,Value=$ProyectoTag" "Key=Cluster,Value=$Cluster" --region $Region | Out-Null

    $igw = (Invoke-AwsJson ec2 create-internet-gateway --region $Region --output json).InternetGateway.InternetGatewayId
    $Estado.InternetGatewayId = $igw; Save-State $Estado
    Invoke-Aws ec2 attach-internet-gateway --internet-gateway-id $igw --vpc-id $vpc --region $Region | Out-Null
    Invoke-Aws ec2 create-tags --resources $igw --tags "Key=Name,Value=$Cluster-igw" "Key=Proyecto,Value=$ProyectoTag" "Key=Cluster,Value=$Cluster" --region $Region | Out-Null

    $rt = (Invoke-AwsJson ec2 create-route-table --vpc-id $vpc --region $Region --output json).RouteTable.RouteTableId
    $Estado.RouteTableId = $rt; Save-State $Estado
    Invoke-Aws ec2 create-route --route-table-id $rt --destination-cidr-block 0.0.0.0/0 --gateway-id $igw --region $Region | Out-Null
    $assoc = (Invoke-AwsJson ec2 associate-route-table --route-table-id $rt --subnet-id $subnet --region $Region --output json).AssociationId
    $Estado.RouteAssociationId = $assoc; Save-State $Estado
    Invoke-Aws ec2 create-tags --resources $rt --tags "Key=Name,Value=$Cluster-routes" "Key=Proyecto,Value=$ProyectoTag" "Key=Cluster,Value=$Cluster" --region $Region | Out-Null

    $sg = (Invoke-AwsJson ec2 create-security-group --group-name "$Cluster-spark-sg" `
        --description 'UrbanFlow: trafico interno, SSH y consolas web desde la IP del alumno' `
        --vpc-id $vpc --region $Region --output json).GroupId
    $Estado.SecurityGroupId = $sg; Save-State $Estado
    Invoke-Aws ec2 create-tags --resources $sg --tags "Key=Name,Value=$Cluster-spark-sg" "Key=Proyecto,Value=$ProyectoTag" "Key=Cluster,Value=$Cluster" --region $Region | Out-Null

    $permisos = @(
        @{ IpProtocol = '-1'; UserIdGroupPairs = @(@{ GroupId = $sg; Description = 'Trafico entre nodos del cluster' }) },
        @{ IpProtocol = 'tcp'; FromPort = 22; ToPort = 22; IpRanges = @(@{ CidrIp = "$CallerIp/32"; Description = 'SSH desde el alumno' }) }
    )
    $permFile = Write-JsonTemp $permisos 'ingress.json'
    Invoke-Aws ec2 authorize-security-group-ingress --group-id $sg --ip-permissions "file://$permFile" --region $Region | Out-Null
}

function Wait-Cluster($Estado) {
    Write-Step 'Esperando la instalación de Hadoop y el registro de los tres workers (puede tardar 10-25 minutos)'
    $limite = (Get-Date).AddMinutes(35)
    Require-Command 'ssh'
    $key = Ensure-SshKey
    $info = Get-NodeInfo 'maestro'
    do {
        Start-Sleep -Seconds 15
        try {
            Grant-TemporarySsh $info "$key.pub"
            $report = & ssh -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=accept-new -i $key "ec2-user@$($info.PublicIpAddress)" 'curl -fsS --max-time 5 "http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"'
            if ($LASTEXITCODE -ne 0) { throw 'HDFS aún no está disponible.' }
            $jmx = ($report -join [Environment]::NewLine) | ConvertFrom-Json
            $vivos = [int]$jmx.beans[0].NumLiveDataNodes
            Write-Host "    DataNodes registrados: $vivos / 3"
            if ($vivos -eq 3) { return }
        } catch {
            Write-Host '    Hadoop todavía está instalándose...'
        }
    } while ((Get-Date) -lt $limite)
    throw 'El clúster no registró los tres DataNodes dentro de 35 minutos. Revisa /var/log/hadoop-bootstrap.log.'
}

function Deploy-Cluster {
    $identidad = Test-Login
    if (Test-Path $EstadoArchivo) {
        $anterior = Load-State
        if ($anterior.Status -ne 'eliminado') {
            $instanciasAnteriores = Get-TaggedInstances
            $vpcsAnteriores = Invoke-AwsJson ec2 describe-vpcs --filters `
                "Name=tag:Proyecto,Values=$ProyectoTag" "Name=tag:Cluster,Values=$Cluster" `
                --region $Region --output json
            if ($instanciasAnteriores.Count -eq 0 -and @($vpcsAnteriores.Vpcs).Count -eq 0) {
                Write-Host 'El estado anterior estaba incompleto, pero AWS no conserva recursos. Se reiniciará de forma segura.' -ForegroundColor Yellow
                Set-StateValue $anterior 'Status' 'eliminado'
                Set-StateValue $anterior 'DeletedAt' (Get-Date).ToString('o')
                Save-State $anterior
            } elseif ($instanciasAnteriores.Count -eq 0) {
                # Sin instancias vivas pero con red: el clúster anterior se autodestruyó (apagado a las N horas
                # = terminación) y su red quedó. Se limpia antes de desplegar de nuevo.
                Write-Host 'El clúster anterior ya no tiene instancias (probable autodestrucción), pero su red sigue en AWS. Se limpia antes de desplegar.' -ForegroundColor Yellow
                Remove-Cluster -Silent
            } else {
                throw "Ya existe un despliegue activo para '$Cluster'. Usa 'estado' o 'eliminar' antes de desplegar otra vez."
            }
        }
    }
    $runId = (Get-Date -Format 'yyyyMMddHHmmss')
    $estado = [ordered]@{
        Version = 1; Status = 'creando'; Cluster = $Cluster; Proyecto = $ProyectoTag
        AccountId = $identidad.Account; Region = $Region; RunId = $runId
        CreatedAt = (Get-Date).ToString('o'); AutoDeleteHours = $HorasMaximas
        Instances = [ordered]@{}
    }
    Save-State $estado

    try {
        $ip = Get-CallerIp
        $az = Get-CommonAz
        $estado.CallerIp = $ip; $estado.AvailabilityZone = $az; Save-State $estado
        Write-Host "Cuenta: $($identidad.Account) | Región: $Region | Zona: $az | Tu IP: $ip"
        New-Network $estado $az $ip

        $ami = (Invoke-AwsJson ssm get-parameter --name '/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64' --region $Region --output json).Parameter.Value
        $estado.Ami = $ami; Save-State $estado

        Write-Step "Lanzando el maestro ($TipoMaestro)"
        $masterUd = New-UserData -Rol maestro -Nombre "$Cluster-maestro" -IpMaestro '10.78.1.10'
        $master = New-Instance -Nombre "$Cluster-maestro" -Tipo $TipoMaestro -SubnetId $estado.SubnetId `
            -SecurityGroupId $estado.SecurityGroupId -Ami $ami -UserData $masterUd -RunId $runId -PrivateIp '10.78.1.10'
        $estado.Instances.maestro = $master; Save-State $estado
        Invoke-Aws ec2 wait instance-running --instance-ids $master --region $Region | Out-Null
        $masterInfo = Invoke-AwsJson ec2 describe-instances --instance-ids $master --region $Region --output json
        $masterObj = $masterInfo.Reservations[0].Instances[0]
        $estado.MasterPrivateIp = $masterObj.PrivateIpAddress
        $estado.MasterPublicIp = $masterObj.PublicIpAddress
        Save-State $estado

        Write-Step "Lanzando tres workers ($TipoWorker)"
        1..3 | ForEach-Object {
            $nombre = "worker$_"
            $ud = New-UserData -Rol worker -Nombre "$Cluster-$nombre" -IpMaestro $estado.MasterPrivateIp
            $workerIp = "10.78.1.$(10 + $_)"
            $iid = New-Instance -Nombre "$Cluster-$nombre" -Tipo $TipoWorker -SubnetId $estado.SubnetId `
                -SecurityGroupId $estado.SecurityGroupId -Ami $ami -UserData $ud -RunId $runId -PrivateIp $workerIp
            $estado.Instances.$nombre = $iid; Save-State $estado
            Write-Host "    ${nombre}: $iid ($workerIp)"
        }
        $todos = Get-InstanceIdsFromState $estado
        Invoke-Aws ec2 wait instance-running --instance-ids @todos --region $Region | Out-Null
        Wait-Cluster $estado

        Set-StateValue $estado 'Status' 'activo'
        Set-StateValue $estado 'ReadyAt' (Get-Date).ToString('o')
        Save-State $estado
        Write-Host "`nCLÚSTER LISTO" -ForegroundColor Green
        Show-State
    } catch {
        Write-Host "`nEl despliegue falló. Ejecutando limpieza automática..." -ForegroundColor Red
        try { Remove-Cluster -Silent } catch { Write-Warning "La limpieza automática también falló: $($_.Exception.Message)" }
        throw
    }
}

function Get-TaggedInstances {
    $r = Invoke-AwsJson ec2 describe-instances --filters `
        "Name=tag:Proyecto,Values=$ProyectoTag" "Name=tag:Cluster,Values=$Cluster" `
        "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down" `
        --region $Region --output json
    return @($r.Reservations | ForEach-Object { $_.Instances } | ForEach-Object { $_ })
}

function Show-State {
    Test-Login | Out-Null
    $estado = Load-State
    $instancias = Get-TaggedInstances
    Write-Host "`nClúster: $Cluster | Cuenta: $($estado.AccountId) | Región: $Region"
    if ($instancias.Count -eq 0) {
        Write-Host 'No hay instancias activas.' -ForegroundColor Yellow
    } else {
        $instancias | ForEach-Object {
            $nombre = ($_.Tags | Where-Object Key -eq 'Name').Value
            [pscustomobject]@{ Nodo = $nombre; Estado = $_.State.Name; Tipo = $_.InstanceType; IPPrivada = $_.PrivateIpAddress; IPPublica = $_.PublicIpAddress }
        } | Format-Table -AutoSize
    }
    Write-Host 'Los servicios de Hadoop y Spark quedan en la red del clúster.'
    Write-Host "Autoeliminación: $($estado.AutoDeleteHours) horas después del despliegue."
}

function Ensure-SshKey {
    Require-Command 'ssh-keygen'
    $key = Join-Path $EstadoDir "$Cluster-eic"
    if (-not (Test-Path $key)) {
        # Windows PowerShell 5.1 descarta los argumentos vacíos ('') al llamar programas nativos:
        # allí la contraseña vacía se escribe '""'. PowerShell 7 sí pasa '' correctamente.
        $sinClave = if ($PSVersionTable.PSVersion.Major -ge 7) { '' } else { '""' }
        & ssh-keygen -q -t ed25519 -N $sinClave -C "$Cluster-eic" -f $key
        if ($LASTEXITCODE -ne 0) { throw 'No se pudo generar la llave SSH temporal.' }
        if ($IsLinux -or $IsMacOS) { & chmod 600 $key }
    }
    return $key
}

function Get-NodeInfo([string]$NombreNodo) {
    $estado = Load-State
    $iid = $estado.Instances.$NombreNodo
    if (-not $iid) { throw "No encontré el nodo '$NombreNodo' en el estado." }
    $r = Invoke-AwsJson ec2 describe-instances --instance-ids $iid --region $Region --output json
    return $r.Reservations[0].Instances[0]
}

function Grant-TemporarySsh($Info, [string]$PublicKeyPath) {
    $ultimoError = $null
    for ($intento = 1; $intento -le 4; $intento++) {
        try {
            Invoke-Aws ec2-instance-connect send-ssh-public-key `
                --instance-id $Info.InstanceId --availability-zone $Info.Placement.AvailabilityZone `
                --instance-os-user ec2-user --ssh-public-key "file://$PublicKeyPath" `
                --region $Region | Out-Null
            return
        } catch {
            $ultimoError = $_.Exception.Message
            if ($intento -lt 4) { Start-Sleep -Seconds (2 * $intento) }
        }
    }
    throw "No se pudo autorizar la llave SSH temporal: $ultimoError"
}

function Connect-Node {
    Test-Login | Out-Null
    Require-Command 'ssh'
    $key = Ensure-SshKey
    $info = Get-NodeInfo $Nodo
    if ($info.State.Name -ne 'running') { throw "El nodo $Nodo está $($info.State.Name), no running." }
    Grant-TemporarySsh $info "$key.pub"
    Write-Host "Entrando a $Nodo. Usa 'sudo -iu hadoop' para trabajar con Hadoop."
    & ssh -o StrictHostKeyChecking=accept-new -i $key "ec2-user@$($info.PublicIpAddress)"
}

function Invoke-DeleteWithRetry {
    param([scriptblock]$Operacion, [string]$Descripcion, [int]$Intentos = 18)
    $ultimo = $null
    for ($i = 1; $i -le $Intentos; $i++) {
        try {
            & $Operacion | Out-Null
            Write-Host "    Eliminado: $Descripcion"
            return $true
        } catch {
            $ultimo = $_.Exception.Message
            # Si AWS dice que el recurso ya no existe (o ya no está conectado), el objetivo está cumplido.
            if ($ultimo -match '\.NotFound|Gateway\.NotAttached') {
                Write-Host "    Ya no existía: $Descripcion"
                return $true
            }
            if ($i -lt $Intentos) { Start-Sleep -Seconds 5 }
        }
    }
    Write-Warning "No se pudo eliminar $Descripcion después de $Intentos intentos: $ultimo"
    return $false
}

# La red del clúster se busca por etiquetas además de leerla del estado local: así se limpia
# aunque el estado falte o esté desactualizado (por ejemplo, después de la autodestrucción).
function Get-TaggedNetwork($Estado) {
    $f1 = "Name=tag:Proyecto,Values=$ProyectoTag"
    $f2 = "Name=tag:Cluster,Values=$Cluster"
    $unir = { param($a, $b) @(@($a) + @($b) | Where-Object { $_ } | Sort-Object -Unique) }
    $vpcs = (Invoke-AwsJson ec2 describe-vpcs --filters $f1 $f2 --region $Region --output json).Vpcs
    $subredes = (Invoke-AwsJson ec2 describe-subnets --filters $f1 $f2 --region $Region --output json).Subnets
    $tablas = (Invoke-AwsJson ec2 describe-route-tables --filters $f1 $f2 --region $Region --output json).RouteTables
    $gateways = (Invoke-AwsJson ec2 describe-internet-gateways --filters $f1 $f2 --region $Region --output json).InternetGateways
    $grupos = (Invoke-AwsJson ec2 describe-security-groups --filters $f1 $f2 --region $Region --output json).SecurityGroups
    $conexiones = @($gateways | ForEach-Object { $g = $_.InternetGatewayId; @($_.Attachments) | ForEach-Object { "$g|$($_.VpcId)" } })
    if ($Estado.InternetGatewayId -and $Estado.VpcId) { $conexiones += "$($Estado.InternetGatewayId)|$($Estado.VpcId)" }
    return [pscustomobject]@{
        Vpcs           = & $unir @($vpcs | ForEach-Object VpcId) $Estado.VpcId
        Subredes       = & $unir @($subredes | ForEach-Object SubnetId) $Estado.SubnetId
        TablasRutas    = & $unir @($tablas | ForEach-Object RouteTableId) $Estado.RouteTableId
        Asociaciones   = & $unir @($tablas | ForEach-Object { @($_.Associations) | Where-Object { -not $_.Main } | ForEach-Object RouteTableAssociationId }) $Estado.RouteAssociationId
        Gateways       = & $unir @($gateways | ForEach-Object InternetGatewayId) $Estado.InternetGatewayId
        Conexiones     = @($conexiones | Sort-Object -Unique)
        Grupos         = & $unir @($grupos | ForEach-Object GroupId) $Estado.SecurityGroupId
    }
}

function Remove-Cluster {
    param([switch]$Silent)
    $identidad = Test-Login
    $estado = if (Test-Path $EstadoArchivo) { Load-State } else { [pscustomobject]@{} }
    if ($estado.AccountId -and $estado.AccountId -ne $identidad.Account) {
        throw "El estado pertenece a la cuenta $($estado.AccountId), pero la sesión actual es $($identidad.Account). Inicia sesión en la cuenta correcta."
    }
    $instancias = Get-TaggedInstances
    if ($instancias.Count -gt 0) {
        Write-Step "Terminando $($instancias.Count) instancia(s)"
        $ids = @($instancias.InstanceId)
        Invoke-Aws ec2 terminate-instances --instance-ids @ids --region $Region | Out-Null
        Invoke-Aws ec2 wait instance-terminated --instance-ids @ids --region $Region | Out-Null
    }

    Write-Step 'Eliminando red y recursos auxiliares'
    $red = Get-TaggedNetwork $estado
    $fallos = @()
    foreach ($id in $red.Grupos) {
        if (-not (Invoke-DeleteWithRetry { Invoke-Aws ec2 delete-security-group --group-id $id --region $Region } "grupo de seguridad $id")) { $fallos += "grupo de seguridad $id" }
    }
    foreach ($id in $red.Asociaciones) {
        if (-not (Invoke-DeleteWithRetry { Invoke-Aws ec2 disassociate-route-table --association-id $id --region $Region } "asociación de rutas $id" 3)) { $fallos += "asociación de rutas $id" }
    }
    foreach ($id in $red.TablasRutas) {
        if (-not (Invoke-DeleteWithRetry { Invoke-Aws ec2 delete-route-table --route-table-id $id --region $Region } "tabla de rutas $id")) { $fallos += "tabla de rutas $id" }
    }
    foreach ($par in $red.Conexiones) {
        $igw, $vpc = $par -split '\|'
        if (-not (Invoke-DeleteWithRetry { Invoke-Aws ec2 detach-internet-gateway --internet-gateway-id $igw --vpc-id $vpc --region $Region } "conexión del Internet Gateway $igw" 3)) { $fallos += "conexión del Internet Gateway $igw" }
    }
    foreach ($id in $red.Gateways) {
        if (-not (Invoke-DeleteWithRetry { Invoke-Aws ec2 delete-internet-gateway --internet-gateway-id $id --region $Region } "Internet Gateway $id")) { $fallos += "Internet Gateway $id" }
    }
    foreach ($id in $red.Subredes) {
        if (-not (Invoke-DeleteWithRetry { Invoke-Aws ec2 delete-subnet --subnet-id $id --region $Region } "subred $id")) { $fallos += "subred $id" }
    }
    foreach ($id in $red.Vpcs) {
        if (-not (Invoke-DeleteWithRetry { Invoke-Aws ec2 delete-vpc --vpc-id $id --region $Region } "VPC $id")) { $fallos += "VPC $id" }
    }
    # Verificación de la red: no debe quedar nada etiquetado (antes solo se revisaban instancias y discos)
    $quedan = Get-TaggedNetwork ([pscustomobject]@{})
    @('Vpcs', 'Subredes', 'Gateways', 'Grupos', 'TablasRutas') | ForEach-Object {
        foreach ($id in $quedan.$_) { if (-not ($fallos | Where-Object { $_ -like "*$id*" })) { $fallos += "sigue en AWS: $id" } }
    }

    $restantes = Get-TaggedInstances
    $vols = $null
    for ($i = 1; $i -le 24; $i++) {
        $vols = Invoke-AwsJson ec2 describe-volumes --filters "Name=tag:Proyecto,Values=$ProyectoTag" "Name=tag:Cluster,Values=$Cluster" --region $Region --output json
        if (@($vols.Volumes).Count -eq 0) { break }
        Start-Sleep -Seconds 5
    }
    if ($restantes.Count -gt 0 -or @($vols.Volumes).Count -gt 0) {
        throw "La verificación encontró recursos restantes: instancias=$($restantes.Count), volúmenes=$(@($vols.Volumes).Count)."
    }
    if ($fallos.Count -gt 0) {
        throw "Las instancias y discos se eliminaron, pero quedaron recursos de red por revisar: $($fallos -join ', ')."
    }
    if (Test-Path $EstadoArchivo) {
        Set-StateValue $estado 'Status' 'eliminado'
        Set-StateValue $estado 'DeletedAt' (Get-Date).ToString('o')
        Save-State $estado
    }
    if (-not $Silent) { Write-Host "`nLIMPIEZA COMPLETA: no quedan instancias, discos ni red del clúster." -ForegroundColor Green }
}

function Show-Menu {
    Write-Host @'

CLÚSTER URBANFLOW EN AWS
1. Desplegar (1 maestro + 3 workers)
2. Ver estado y enlaces
3. Conectar por SSH
5. Eliminar absolutamente todo
0. Salir
'@
    $opcion = Read-Host 'Elige una opción'
    switch ($opcion) {
        '1' { Deploy-Cluster }
        '2' { Show-State }
        '3' { Connect-Node }
        '5' { Remove-Cluster }
        '0' { return }
        default { throw 'Opción no válida.' }
    }
}

switch ($Accion) {
    'menu'      { Show-Menu }
    'desplegar' { Deploy-Cluster }
    'estado'    { Show-State }
    'conectar'  { Connect-Node }
    'eliminar'  { Remove-Cluster }
}
