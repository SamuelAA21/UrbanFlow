function Assert-PortalMonths {
    $parts = $Meses -split ','
    if ($parts.Count -gt 6) { throw 'Elige de uno a seis meses consecutivos.' }
    $last = $null
    foreach ($part in $parts) {
        if ($part -notmatch '^[0-9]{4}-(0[1-9]|1[0-2])$') { throw 'Usa meses con formato YYYY-MM.' }
        $date = [datetime]::ParseExact($part, 'yyyy-MM', [Globalization.CultureInfo]::InvariantCulture)
        if ($last -and $date -ne $last.AddMonths(1)) { throw 'Los meses deben ser consecutivos y estar en orden.' }
        $last = $date
    }
}

function Resolve-PortalCidr {
    $value = $PortalCidr
    if (-not $value) {
        $value = (Invoke-RestMethod 'https://checkip.amazonaws.com' -TimeoutSec 10).Trim() + '/32'
    }
    if ($value -notmatch '^([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})/([0-9]{1,2})$') {
        throw 'Usa un CIDR IPv4 para el portal.'
    }
    $ip = $Matches[1]
    $mask = [int]$Matches[2]
    if ($mask -gt 32 -or @($ip.Split('.') | Where-Object { [int]$_ -gt 255 }).Count) {
        throw 'CIDR inválido.'
    }
    return $value
}

function Deploy-Portal {
    Test-Login
    Assert-PortalMonths
    $cidr = Resolve-PortalCidr
    $alreadyActive = Cluster-Activo
    Require 'tar'
    $archive = Join-Path ([IO.Path]::GetTempPath()) ("urbanflow-" + [guid]::NewGuid().ToString('N') + '.tgz')
    $remote = $null
    try {
        # El envío contiene solo fuentes. El estado local de AWS nunca entra al paquete.
        $files = @('portal/requirements.txt','portal/settings.py','portal/db.py','portal/app.py',
            'portal/cluster_status.py','portal/worker.py','portal/bootstrap.py','portal/web',
            'spark','cluster/pipeline.sh','cluster/instalar-portal.sh','dashboard/taxi_zones.geojson')
        & tar -czf $archive --exclude '__pycache__' -C $Raiz @files
        if ($LASTEXITCODE -ne 0) { throw 'No se pudo empaquetar el portal.' }
        if (-not $alreadyActive) { Deploy }
        $remote = Run-Remote 'mktemp -d /tmp/urbanflow.XXXXXXXX'
        $remote = ($remote | Select-Object -Last 1).Trim()
        if ($remote -notmatch '^/tmp/urbanflow\.[A-Za-z0-9]+$') { throw 'Ruta temporal inesperada.' }
        Send-Files @($archive) "$remote/release.tgz"
        Run-Remote "tar -xzf $remote/release.tgz -C $remote && sed -i 's/\r$//' $remote/cluster/*.sh && sudo bash $remote/cluster/instalar-portal.sh $remote $Meses"
        $state = Read-State
        try {
            Run-Aws @('ec2','authorize-security-group-ingress','--group-id',$state.SecurityGroupId,
                '--protocol','tcp','--port','8080','--cidr',$cidr,'--region',$Region) | Out-Null
        } catch {
            if ($_.Exception.Message -notmatch 'InvalidPermission.Duplicate') { throw }
        }
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Raiz 'scripts\cloudfront-portal.ps1') deploy -Region $Region
        if ($LASTEXITCODE -ne 0) { throw 'No se pudo crear la entrada HTTPS de CloudFront.' }
        Write-Host 'La preparación corre en AWS. Usa .\urbanflow.ps1 admin para seguirla.'
    } catch {
        if (-not $alreadyActive -and (Test-Path (State-Path))) {
            try { Remove-All } catch { Write-Warning 'La limpieza automática falló. Ejecuta eliminar y verifica el resultado.' }
        }
        throw
    } finally {
        Remove-Item -LiteralPath $archive -ErrorAction SilentlyContinue
        if ($remote -match '^/tmp/urbanflow\.[A-Za-z0-9]+$') {
            try { Run-Remote "rm -rf -- $remote" } catch { Write-Warning 'No se pudo retirar el paquete temporal del maestro.' }
        }
    }
}

function Open-PortalAdmin {
    Assert-Activo
    $info = Get-MasterInfo
    $key = Ensure-Key
    Grant-Key $info $key
    Write-Host 'Abre http://127.0.0.1:8090/ y deja esta terminal abierta.'
    & ssh @SshOpts -o ExitOnForwardFailure=yes -i $key -N -L '127.0.0.1:8090:127.0.0.1:8090' "ec2-user@$($info.PublicIpAddress)"
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo abrir el túnel de administración.' }
}

function Backup-PortalDatabase {
    Test-Login
    Assert-Activo
    $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $remoteDatabase = "/tmp/urbanflow-$stamp.sqlite3"
    $remoteHelper = '/tmp/urbanflow-backup-sqlite.py'
    $backupDirectory = Join-Path $EstadoDir 'respaldos'
    $localDatabase = Join-Path $backupDirectory "urbanflow-$stamp.sqlite3"
    New-Item -ItemType Directory -Path $backupDirectory -Force | Out-Null

    Step 'Creando una copia consistente de SQLite sin detener el portal'
    Send-Files @((Join-Path $Raiz 'scripts\backup_sqlite.py')) $remoteHelper
    try {
        Run-Remote ("sudo -u hadoop /opt/urbanflow/venv/bin/python $remoteHelper " +
            "/var/lib/urbanflow/urbanflow.sqlite3 $remoteDatabase; sudo chmod 0644 $remoteDatabase")
        Receive-File $remoteDatabase $localDatabase
        $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $localDatabase).Hash
        Write-Host "Respaldo guardado: $localDatabase" -ForegroundColor Green
        Write-Host "SHA-256: $hash"
    } finally {
        try { Run-Remote "sudo rm -f -- $remoteDatabase $remoteHelper" } catch {
            Write-Warning 'El respaldo local se completo, pero no se pudieron retirar los temporales del maestro.'
        }
    }
}
