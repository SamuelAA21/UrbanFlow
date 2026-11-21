[CmdletBinding()]
param(
    [switch]$SelfTest,
    [int]$AutoCloseSeconds = 0
)

$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

$projectRoot = Split-Path -Parent $PSScriptRoot
$urbanFlowScript = Join-Path $projectRoot 'urbanflow.ps1'

if ($SelfTest) {
    if (-not (Test-Path -LiteralPath $urbanFlowScript)) { throw 'No se encontro urbanflow.ps1.' }
    Write-Output 'UrbanFlow Launcher: OK'
    return
}

function Show-LauncherError([string]$Message) {
    [Windows.Forms.MessageBox]::Show($Message, 'UrbanFlow', 'OK', 'Error') | Out-Null
}

function Start-UrbanFlow([string]$Arguments) {
    if (-not (Test-Path -LiteralPath $urbanFlowScript)) {
        Show-LauncherError 'No se encontro urbanflow.ps1. Conserva el lanzador dentro de la carpeta del proyecto.'
        return
    }
    $quotedScript = '"' + $urbanFlowScript.Replace('"', '\"') + '"'
    Start-Process -FilePath 'powershell.exe' -WorkingDirectory $projectRoot -ArgumentList `
        "-NoLogo -NoProfile -ExecutionPolicy Bypass -NoExit -File $quotedScript $Arguments"
}

function Start-AwsLogin {
    $command = "aws login; if (`$?) { aws sts get-caller-identity }; Write-Host ''; Write-Host 'Puedes cerrar esta ventana cuando aparezca tu cuenta.' -ForegroundColor Green"
    Start-Process -FilePath 'powershell.exe' -WorkingDirectory $projectRoot -ArgumentList `
        "-NoLogo -NoProfile -ExecutionPolicy Bypass -NoExit -Command `"$command`""
}

function Open-Url([string]$Url) {
    try { Start-Process $Url }
    catch { Show-LauncherError $_.Exception.Message }
}

function Open-PublicPortal {
    $statePath = Join-Path $projectRoot 'estado\urbanflow-portal.json'
    if (-not (Test-Path -LiteralPath $statePath)) {
        [Windows.Forms.MessageBox]::Show(
            'Todavia no hay un despliegue registrado. Usa primero uno de los botones de despliegue.',
            'Portal no disponible', 'OK', 'Information') | Out-Null
        return
    }
    try { $state = Get-Content -Raw -LiteralPath $statePath | ConvertFrom-Json }
    catch { Show-LauncherError 'No se pudo leer el estado local del despliegue.'; return }
    if (-not $state.CloudFrontDomain) {
        [Windows.Forms.MessageBox]::Show(
            'El estado local todavia no contiene una direccion publica. Usa Ver estado actual.',
            'Portal no disponible', 'OK', 'Information') | Out-Null
        return
    }
    Open-Url "https://$($state.CloudFrontDomain)/"
}

$form = New-Object Windows.Forms.Form
$form.Text = 'UrbanFlow - Lanzador'
$form.StartPosition = 'CenterScreen'
$form.FormBorderStyle = 'FixedDialog'
$form.MaximizeBox = $false
$form.ClientSize = New-Object Drawing.Size(660, 570)
$form.BackColor = [Drawing.Color]::FromArgb(243, 247, 250)
$form.Font = New-Object Drawing.Font('Segoe UI', 10)

function Add-Label([string]$Text, [int]$X, [int]$Y, [Drawing.Font]$Font, [Drawing.Color]$Color) {
    $label = New-Object Windows.Forms.Label
    $label.Text = $Text
    $label.Font = $Font
    $label.ForeColor = $Color
    $label.AutoSize = $true
    $label.Location = New-Object Drawing.Point($X, $Y)
    $form.Controls.Add($label)
}

function Add-Section([string]$Text, [int]$Y) {
    Add-Label $Text.ToUpperInvariant() 32 $Y `
        (New-Object Drawing.Font('Segoe UI Semibold', 9, [Drawing.FontStyle]::Bold)) `
        ([Drawing.Color]::FromArgb(0, 102, 119))
}

function Add-Button([string]$Text, [int]$X, [int]$Y, [int]$Width, [scriptblock]$Action, [switch]$Primary) {
    $button = New-Object Windows.Forms.Button
    $button.Text = $Text
    $button.Location = New-Object Drawing.Point($X, $Y)
    $button.Size = New-Object Drawing.Size($Width, 42)
    $button.FlatStyle = 'Flat'
    $button.Cursor = [Windows.Forms.Cursors]::Hand
    if ($Primary) {
        $button.BackColor = [Drawing.Color]::FromArgb(0, 104, 120)
        $button.ForeColor = [Drawing.Color]::White
        $button.FlatAppearance.BorderColor = [Drawing.Color]::FromArgb(0, 104, 120)
    } else {
        $button.BackColor = [Drawing.Color]::White
        $button.ForeColor = [Drawing.Color]::FromArgb(19, 53, 67)
        $button.FlatAppearance.BorderColor = [Drawing.Color]::FromArgb(181, 199, 208)
    }
    $button.Add_Click($Action)
    $form.Controls.Add($button)
    return $button
}

Add-Label 'UrbanFlow' 28 20 `
    (New-Object Drawing.Font('Segoe UI Semibold', 24, [Drawing.FontStyle]::Bold)) `
    ([Drawing.Color]::FromArgb(0, 74, 88))
Add-Label 'Despliega y administra el portal sin escribir comandos.' 32 67 `
    (New-Object Drawing.Font('Segoe UI', 10)) ([Drawing.Color]::FromArgb(65, 82, 94))

Add-Section 'Preparacion' 108
Add-Button '1. Iniciar sesion en AWS' 32 142 286 { Start-AwsLogin } | Out-Null
Add-Button 'Ver estado actual' 338 142 286 { Start-UrbanFlow 'estado' } | Out-Null

Add-Section 'Despliegue' 205
Add-Button 'Desplegar rapido (1 mes)' 32 239 286 {
    Start-UrbanFlow 'portal -Meses 2025-06 -HorasMaximas 4'
} -Primary | Out-Null
Add-Button 'Desplegar completo (6 meses)' 338 239 286 {
    Start-UrbanFlow 'portal -Meses 2025-01,2025-02,2025-03,2025-04,2025-05,2025-06 -HorasMaximas 4'
} -Primary | Out-Null

Add-Section 'Operacion' 302
Add-Button 'Abrir tunel de administracion' 32 336 286 { Start-UrbanFlow 'admin' } | Out-Null
Add-Button 'Abrir panel administrativo' 338 336 286 { Open-Url 'http://127.0.0.1:8090/' } | Out-Null
Add-Button 'Abrir portal publico' 32 386 286 { Open-PublicPortal } | Out-Null
Add-Button 'Respaldar base de datos' 338 386 286 { Start-UrbanFlow 'respaldo' } | Out-Null

$remove = Add-Button 'Eliminar recursos de AWS' 32 457 592 {
    $answer = [Windows.Forms.MessageBox]::Show(
        "Esto eliminara CloudFront, las instancias y la red de UrbanFlow en AWS. El respaldo local se conserva.`n`n¿Continuar?",
        'Confirmar eliminacion', 'YesNo', 'Warning', 'Button2')
    if ($answer -eq [Windows.Forms.DialogResult]::Yes) { Start-UrbanFlow 'eliminar' }
}
$remove.BackColor = [Drawing.Color]::FromArgb(157, 46, 52)
$remove.ForeColor = [Drawing.Color]::White
$remove.FlatAppearance.BorderColor = [Drawing.Color]::FromArgb(157, 46, 52)

$note = New-Object Windows.Forms.Label
$note.Text = 'Las consolas permanecen abiertas para mostrar el progreso. Cierra la consola del tunel para salir del panel admin.'
$note.ForeColor = [Drawing.Color]::FromArgb(85, 96, 105)
$note.Size = New-Object Drawing.Size(596, 42)
$note.Location = New-Object Drawing.Point(32, 515)
$form.Controls.Add($note)

if ($AutoCloseSeconds -gt 0) {
    $timer = New-Object Windows.Forms.Timer
    $timer.Interval = $AutoCloseSeconds * 1000
    $timer.Add_Tick({ $timer.Stop(); $form.Close() })
    $timer.Start()
}

[void]$form.ShowDialog()
