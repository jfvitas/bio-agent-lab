Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$appDir = Join-Path $repoRoot "apps\PbdataWinUI"
$projectFile = Join-Path $appDir "PbdataWinUI.csproj"
$buildDir = Join-Path $appDir "bin\x64\Release\net8.0-windows10.0.19041.0\win-x64"
$exePath = Join-Path $buildDir "PbdataWinUI.exe"
$localDotnetDir = Join-Path $repoRoot ".tools\dotnet"
$localDotnetExe = Join-Path $localDotnetDir "dotnet.exe"
$dotnetInstallScript = Join-Path $repoRoot ".tools\dotnet-install.ps1"
$buildLogPath = Join-Path $repoRoot "logs\winui_launcher_build.log"

function Write-Step([string]$message) {
    Write-Host ""
    Write-Host "==> $message" -ForegroundColor Cyan
}

function Test-Command([string]$name) {
    return [bool](Get-Command $name -ErrorAction SilentlyContinue)
}

function Get-DotnetCommand() {
    if (Test-Path $localDotnetExe) {
        return $localDotnetExe
    }
    if (Test-Command "dotnet") {
        return "dotnet"
    }
    return $null
}

function Ensure-LocalDotnetSdk() {
    Write-Step "Installing a local .NET 8 SDK into the repo"
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $localDotnetExe) | Out-Null
    if (-not (Test-Path $dotnetInstallScript)) {
        Invoke-WebRequest -Uri "https://dot.net/v1/dotnet-install.ps1" -OutFile $dotnetInstallScript
    }
    & powershell -NoProfile -ExecutionPolicy Bypass -File $dotnetInstallScript -Version 8.0.419 -InstallDir $localDotnetDir -NoPath
    if (-not (Test-Path $localDotnetExe)) {
        throw "Local .NET SDK install did not produce $localDotnetExe"
    }
    return $localDotnetExe
}

function Ensure-Dotnet() {
    $dotnet = Get-DotnetCommand
    if ($null -ne $dotnet) {
        return $dotnet
    }
    return Ensure-LocalDotnetSdk
}

function Test-WindowsAppRuntimeInstalled() {
    $packages = Get-AppxPackage Microsoft.WindowsAppRuntime.1.8 -ErrorAction SilentlyContinue
    return $null -ne $packages
}

function Ensure-WindowsAppRuntime() {
    if (Test-WindowsAppRuntimeInstalled) {
        return
    }
    if (-not (Test-Command "winget")) {
        throw "Windows App Runtime 1.8 is missing and winget is unavailable for automatic installation."
    }
    Write-Step "Installing Windows App Runtime 1.8"
    & winget install --id Microsoft.WindowsAppRuntime.1.8 --exact --accept-package-agreements --accept-source-agreements --disable-interactivity
    if ($LASTEXITCODE -ne 0) {
        throw "winget failed to install Windows App Runtime 1.8."
    }
    if (-not (Test-WindowsAppRuntimeInstalled)) {
        throw "Windows App Runtime 1.8 still appears to be missing after install."
    }
}

function Get-LatestWriteUtc([string[]]$paths) {
    $latest = [datetime]::MinValue
    foreach ($path in $paths) {
        if (-not (Test-Path $path)) {
            continue
        }
        $candidate = Get-ChildItem $path -Recurse -File | Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
        if ($null -ne $candidate -and $candidate.LastWriteTimeUtc -gt $latest) {
            $latest = $candidate.LastWriteTimeUtc
        }
    }
    return $latest
}

function Ensure-ParentDirectory([string]$path) {
    $parent = Split-Path -Parent $path
    if ($parent -and -not (Test-Path $parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
}

function Test-BuildIsCurrent() {
    if (-not (Test-Path $exePath)) {
        return $false
    }
    $buildWrite = (Get-Item $exePath).LastWriteTimeUtc
    $sourceWrite = Get-LatestWriteUtc @(
        (Join-Path $appDir "App.xaml"),
        (Join-Path $appDir "App.xaml.cs"),
        (Join-Path $appDir "Assets"),
        (Join-Path $appDir "Models"),
        (Join-Path $appDir "ViewModels"),
        (Join-Path $appDir "Views")
    )
    return $buildWrite -ge $sourceWrite
}

function Test-BuildLooksUsable() {
    return (Test-Path $exePath) -and (Get-Item $exePath).Length -gt 0
}

function Build-App([string]$dotnetCmd) {
    Write-Step "Building pbdata WinUI"
    Ensure-ParentDirectory $buildLogPath
    $attempts = 2
    for ($attempt = 1; $attempt -le $attempts; $attempt++) {
        if (Test-Path $buildLogPath) {
            Remove-Item $buildLogPath -Force -ErrorAction SilentlyContinue
        }
        & $dotnetCmd build $projectFile -c Release -p:Platform=x64 -p:UseSharedCompilation=false -nodeReuse:false *>&1 |
            Tee-Object -FilePath $buildLogPath
        if ($LASTEXITCODE -eq 0 -and (Test-BuildLooksUsable)) {
            return
        }

        if ($attempt -lt $attempts) {
            Start-Sleep -Seconds (2 * $attempt)
        }
    }

    if (Test-BuildLooksUsable) {
        Write-Warning "Build hit a transient compiler/lock issue. Launching the most recent successful executable instead."
        return
    }
    throw "dotnet build failed."
}

function Start-App() {
    Write-Step "Launching pbdata WinUI"
    Start-Process -FilePath $exePath | Out-Null
}

$dotnetCmd = Ensure-Dotnet
Ensure-WindowsAppRuntime
if (-not (Test-BuildIsCurrent)) {
    Build-App $dotnetCmd
}
Start-App
