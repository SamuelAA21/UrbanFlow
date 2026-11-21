[CmdletBinding()]
param(
    [ValidateSet('deploy', 'delete', 'status')]
    [string]$Action = 'deploy',
    [string]$Region = 'us-east-1'
)

$ErrorActionPreference = 'Stop'
$env:AWS_PAGER = ''
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$StatePath = Join-Path $Root 'estado\urbanflow-portal.json'

function Invoke-Aws([string[]]$Arguments) {
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & aws @Arguments 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($code -ne 0) {
        throw "AWS CLI falló: aws $($Arguments -join ' ')`n$($output -join "`n")"
    }
    return ($output -join "`n")
}

function Read-PortalState {
    if (-not (Test-Path -LiteralPath $StatePath)) {
        throw 'No existe estado de urbanflow-portal. Despliega primero el portal.'
    }
    return Get-Content -Raw -LiteralPath $StatePath | ConvertFrom-Json
}

function Save-PortalState($State) {
    $json = $State | ConvertTo-Json -Depth 12
    [IO.File]::WriteAllText($StatePath, $json, [Text.UTF8Encoding]::new($false))
}

function Set-StateValue($State, [string]$Name, $Value) {
    if ($State.PSObject.Properties[$Name]) { $State.$Name = $Value }
    else { $State | Add-Member -MemberType NoteProperty -Name $Name -Value $Value }
}

function Write-JsonTemp($Value) {
    $path = [IO.Path]::GetTempFileName()
    # -InputObject conserva los arreglos de un solo elemento; el pipeline los
    # desempaqueta y AWS CLI deja de recibir una lista JSON válida.
    $json = ConvertTo-Json -InputObject $Value -Depth 20 -Compress
    [IO.File]::WriteAllText($path, $json, [Text.Encoding]::ASCII)
    return $path
}

function Get-Distribution($State) {
    if (-not $State.CloudFrontId) { return $null }
    try {
        return (Invoke-Aws @('cloudfront', 'get-distribution', '--id', $State.CloudFrontId, '--output', 'json')) | ConvertFrom-Json
    } catch {
        if ($_.Exception.Message -match 'NoSuchDistribution') { return $null }
        throw
    }
}

function Grant-CloudFrontOriginAccess($State) {
    $prefix = (Invoke-Aws @('ec2', 'describe-managed-prefix-lists', '--region', $Region,
        '--filters', 'Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing',
        '--query', 'PrefixLists[0].PrefixListId', '--output', 'text')).Trim()
    if (-not $prefix -or $prefix -eq 'None') { throw 'AWS no devolvió la lista administrada de orígenes CloudFront.' }
    $permission = @(@{
        IpProtocol = 'tcp'; FromPort = 8080; ToPort = 8080
        PrefixListIds = @(@{ PrefixListId = $prefix; Description = 'HTTPS via CloudFront' })
    })
    $file = Write-JsonTemp $permission
    try {
        try {
            Invoke-Aws @('ec2', 'authorize-security-group-ingress', '--group-id', $State.SecurityGroupId,
                '--ip-permissions', "file://$file", '--region', $Region) | Out-Null
        } catch {
            if ($_.Exception.Message -notmatch 'InvalidPermission.Duplicate') { throw }
        }
    } finally { Remove-Item -LiteralPath $file -Force -ErrorAction SilentlyContinue }
    Set-StateValue $State 'CloudFrontPrefixListId' $prefix
}

function Deploy-Distribution {
    $state = Read-PortalState
    $existing = Get-Distribution $state
    if ($existing) {
        Write-Host "Portal HTTPS: https://$($existing.Distribution.DomainName)/"
        Write-Host "CloudFront: $($existing.Distribution.Status)"
        return
    }

    Grant-CloudFrontOriginAccess $state
    $master = (Invoke-Aws @('ec2', 'describe-instances', '--instance-ids', $state.Instances.maestro,
        '--region', $Region, '--query', 'Reservations[0].Instances[0]', '--output', 'json')) | ConvertFrom-Json
    if ($master.State.Name -ne 'running' -or -not $master.PublicDnsName) {
        throw 'El maestro no está activo o no tiene nombre DNS público.'
    }

    $config = @{
        CallerReference = "urbanflow-$([guid]::NewGuid().ToString('N'))"
        Comment = 'UrbanFlow temporal HTTPS portal'
        Enabled = $true
        PriceClass = 'PriceClass_100'
        HttpVersion = 'http2and3'
        IsIPV6Enabled = $true
        Origins = @{ Quantity = 1; Items = @(@{
            Id = 'urbanflow-origin'; DomainName = $master.PublicDnsName; OriginPath = ''
            ConnectionAttempts = 3; ConnectionTimeout = 10; CustomHeaders = @{ Quantity = 0 }
            CustomOriginConfig = @{
                HTTPPort = 8080; HTTPSPort = 443; OriginProtocolPolicy = 'http-only'
                OriginSslProtocols = @{ Quantity = 1; Items = @('TLSv1.2') }
                OriginReadTimeout = 30; OriginKeepaliveTimeout = 5
            }
        }) }
        DefaultCacheBehavior = @{
            TargetOriginId = 'urbanflow-origin'; ViewerProtocolPolicy = 'redirect-to-https'
            Compress = $true; SmoothStreaming = $false
            AllowedMethods = @{ Quantity = 2; Items = @('GET', 'HEAD'); CachedMethods = @{ Quantity = 2; Items = @('GET', 'HEAD') } }
            TrustedSigners = @{ Enabled = $false; Quantity = 0 }
            TrustedKeyGroups = @{ Enabled = $false; Quantity = 0 }
            CachePolicyId = '4135ea2d-6df8-44a3-9df3-4b5a84be39ad'
            OriginRequestPolicyId = 'b689b0a8-53d0-40ab-baf2-68738e2966ac'
            ResponseHeadersPolicyId = '67f7725c-6f97-4210-82d7-5512b31e9d03'
            LambdaFunctionAssociations = @{ Quantity = 0 }
            FunctionAssociations = @{ Quantity = 0 }
            FieldLevelEncryptionId = ''
        }
        Aliases = @{ Quantity = 0 }
        CacheBehaviors = @{ Quantity = 0 }
        CustomErrorResponses = @{ Quantity = 0 }
        Logging = @{ Enabled = $false; IncludeCookies = $false; Bucket = ''; Prefix = '' }
        ViewerCertificate = @{ CloudFrontDefaultCertificate = $true; MinimumProtocolVersion = 'TLSv1'; CertificateSource = 'cloudfront' }
        Restrictions = @{ GeoRestriction = @{ RestrictionType = 'none'; Quantity = 0 } }
    }
    $file = Write-JsonTemp $config
    try {
        $created = (Invoke-Aws @('cloudfront', 'create-distribution', '--distribution-config', "file://$file", '--output', 'json')) | ConvertFrom-Json
    } finally { Remove-Item -LiteralPath $file -Force -ErrorAction SilentlyContinue }
    Set-StateValue $state 'CloudFrontId' $created.Distribution.Id
    Set-StateValue $state 'CloudFrontDomain' $created.Distribution.DomainName
    Save-PortalState $state
    Write-Host "Portal HTTPS: https://$($created.Distribution.DomainName)/"
    Write-Host 'CloudFront se está propagando; normalmente tarda varios minutos.'
}

function Delete-Distribution {
    $state = Read-PortalState
    $current = Get-Distribution $state
    if (-not $current) {
        Write-Host 'No hay una distribución CloudFront activa para este portal.'
        return
    }
    $id = $current.Distribution.Id
    if ($current.Distribution.Status -ne 'Deployed') {
        Write-Host 'Esperando que CloudFront termine la propagación antes de retirarlo...'
        Invoke-Aws @('cloudfront', 'wait', 'distribution-deployed', '--id', $id) | Out-Null
    }
    $configResponse = (Invoke-Aws @('cloudfront', 'get-distribution-config', '--id', $id, '--output', 'json')) | ConvertFrom-Json
    if ($configResponse.DistributionConfig.Enabled) {
        $configResponse.DistributionConfig.Enabled = $false
        $file = Write-JsonTemp $configResponse.DistributionConfig
        try {
            Invoke-Aws @('cloudfront', 'update-distribution', '--id', $id, '--if-match', $configResponse.ETag,
                '--distribution-config', "file://$file", '--output', 'json') | Out-Null
        } finally { Remove-Item -LiteralPath $file -Force -ErrorAction SilentlyContinue }
        Invoke-Aws @('cloudfront', 'wait', 'distribution-deployed', '--id', $id) | Out-Null
    }
    $latest = (Invoke-Aws @('cloudfront', 'get-distribution-config', '--id', $id, '--output', 'json')) | ConvertFrom-Json
    Invoke-Aws @('cloudfront', 'delete-distribution', '--id', $id, '--if-match', $latest.ETag) | Out-Null
    Set-StateValue $state 'CloudFrontId' $null
    Set-StateValue $state 'CloudFrontDomain' $null
    Save-PortalState $state
    Write-Host 'Distribución CloudFront eliminada.'
}

switch ($Action) {
    'deploy' { Deploy-Distribution }
    'delete' { Delete-Distribution }
    'status' {
        $state = Read-PortalState
        $distribution = Get-Distribution $state
        if ($distribution) {
            [pscustomobject]@{
                Id = $distribution.Distribution.Id
                Status = $distribution.Distribution.Status
                Url = "https://$($distribution.Distribution.DomainName)/"
            } | Format-List
        } else { Write-Host 'No hay una distribución CloudFront activa para este portal.' }
    }
}
