param(
    [string]$PythonExe = "D:\New_ANACONDA\envs\whisper_env\python.exe",
    [string]$RepoRoot = "D:\videoToMarkdownTest2",
    [switch]$CheckDeps,
    [switch]$EnableRedis,
    [switch]$SkipRedis,
    [string]$RedisComposeFile = "docker-compose.yml",
    [string]$RedisService = "redis",
    [string]$RedisContainerName = "v2m-redis",
    [string]$RedisUrl = "redis://127.0.0.1:6379/0",
    [string]$RedisPrefix = "rt",
    [int]$RedisStartupTimeoutSec = 60,
    [string]$DockerDesktopExe = "C:\Program Files\Docker\Docker\Docker Desktop.exe",
    [int]$DockerStartupTimeoutSec = 180,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ServerArgs
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $PythonExe)) {
    throw "Python executable not found: $PythonExe"
}

if (-not (Test-Path $RepoRoot)) {
    throw "Repo root not found: $RepoRoot"
}

Set-Location $RepoRoot

$RedisRuntimeRequested = $EnableRedis -and -not $SkipRedis
if ($EnableRedis -and $SkipRedis) {
    throw "EnableRedis and SkipRedis cannot be used together"
}

$RedisComposeFilePath = Join-Path $RepoRoot $RedisComposeFile
if ($RedisRuntimeRequested -and -not (Test-Path $RedisComposeFilePath)) {
    throw "Redis compose file not found: $RedisComposeFilePath"
}

# 固定启动环境隔离，避免本地运行时被 user-site 包污染依赖解析。
$env:PYTHONNOUSERSITE = "1"
$env:PYTHONIOENCODING = "utf-8"

if ($RedisRuntimeRequested -and -not $PSBoundParameters.ContainsKey("RedisUrl") -and -not [string]::IsNullOrWhiteSpace($env:TASK_RUNTIME_REDIS_URL)) {
    $RedisUrl = $env:TASK_RUNTIME_REDIS_URL
}
if ($RedisRuntimeRequested -and -not $PSBoundParameters.ContainsKey("RedisPrefix") -and -not [string]::IsNullOrWhiteSpace($env:TASK_RUNTIME_REDIS_PREFIX)) {
    $RedisPrefix = $env:TASK_RUNTIME_REDIS_PREFIX
}

function Invoke-DockerCompose {
    param(
        [string[]]$ComposeArgs
    )

    & docker compose -f $RedisComposeFilePath @ComposeArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

function Test-DockerEngineReady {
    try {
        & docker info *> $null
        return $LASTEXITCODE -eq 0
    } catch {
        return $false
    }
}

function Ensure-DockerReady {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "docker command not found; cannot auto-start Redis container"
    }
    if (Test-DockerEngineReady) {
        return
    }
    if (-not (Test-Path $DockerDesktopExe)) {
        throw "Docker engine is not ready and Docker Desktop executable was not found: $DockerDesktopExe"
    }

    Write-Host "Starting Docker Desktop:" $DockerDesktopExe
    Start-Process -FilePath $DockerDesktopExe | Out-Null

    $deadline = (Get-Date).AddSeconds([Math]::Max(30, $DockerStartupTimeoutSec))
    while ((Get-Date) -lt $deadline) {
        if (Test-DockerEngineReady) {
            Write-Host "Docker engine is ready."
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "Docker engine did not become ready in ${DockerStartupTimeoutSec}s"
}

function Get-RedisRuntimeState {
    try {
        $state = & docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $RedisContainerName 2>$null
        if ($LASTEXITCODE -ne 0) {
            return ""
        }
        return (($state | Select-Object -First 1) -as [string]).Trim()
    } catch {
        return ""
    }
}

function Test-TcpEndpoint {
    param(
        [string]$HostName,
        [int]$Port
    )

    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $asyncResult = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $asyncResult.AsyncWaitHandle.WaitOne(2000, $false)) {
            return $false
        }
        $client.EndConnect($asyncResult) | Out-Null
        return $true
    } catch {
        return $false
    } finally {
        $client.Dispose()
    }
}

function Wait-RedisReady {
    param(
        [string]$Url,
        [int]$TimeoutSec
    )

    $uri = [System.Uri]$Url
    $hostName = if ([string]::IsNullOrWhiteSpace($uri.Host)) { "127.0.0.1" } else { $uri.Host }
    $port = if ($uri.Port -gt 0) { $uri.Port } else { 6379 }
    $deadline = (Get-Date).AddSeconds([Math]::Max(5, $TimeoutSec))

    while ((Get-Date) -lt $deadline) {
        $runtimeState = Get-RedisRuntimeState
        if ((Test-TcpEndpoint -HostName $hostName -Port $port) -and ($runtimeState -eq "healthy" -or $runtimeState -eq "running")) {
            return
        }
        Start-Sleep -Seconds 1
    }

    $runtimeState = Get-RedisRuntimeState
    throw "Redis container did not become ready in ${TimeoutSec}s. container=$RedisContainerName state=$runtimeState url=$Url"
}

function Start-RedisForRuntimeRecovery {
    Ensure-DockerReady
    Write-Host "Starting Redis container:" $RedisService
    Invoke-DockerCompose -ComposeArgs @("up", "-d", $RedisService)
    Wait-RedisReady -Url $RedisUrl -TimeoutSec $RedisStartupTimeoutSec

    $env:TASK_RUNTIME_REDIS_ENABLED = "1"
    $env:TASK_RUNTIME_REDIS_URL = $RedisUrl
    $env:TASK_RUNTIME_REDIS_PREFIX = $RedisPrefix

    Write-Host "TASK_RUNTIME_REDIS_ENABLED=" $env:TASK_RUNTIME_REDIS_ENABLED
    Write-Host "TASK_RUNTIME_REDIS_URL=" $env:TASK_RUNTIME_REDIS_URL
    Write-Host "TASK_RUNTIME_REDIS_PREFIX=" $env:TASK_RUNTIME_REDIS_PREFIX
}

Write-Host "Using Python:" $PythonExe
Write-Host "Repo root:" $RepoRoot
Write-Host "PYTHONNOUSERSITE=" $env:PYTHONNOUSERSITE

if ($RedisRuntimeRequested) {
    Start-RedisForRuntimeRecovery
} else {
    $env:TASK_RUNTIME_REDIS_ENABLED = "0"
    Remove-Item Env:TASK_RUNTIME_REDIS_URL -ErrorAction SilentlyContinue
    Remove-Item Env:TASK_RUNTIME_REDIS_PREFIX -ErrorAction SilentlyContinue
    Write-Host "TASK_RUNTIME_REDIS_ENABLED=" $env:TASK_RUNTIME_REDIS_ENABLED
    Write-Host "Redis runtime mirror is disabled by default. Pass -EnableRedis to opt in."
}

if ($CheckDeps) {
    & $PythonExe -X utf8 .\apps\grpc-server\main.py --check-deps
    if ($LASTEXITCODE -ne 0) {
        throw "Dependency preflight failed with exit code $LASTEXITCODE"
    }
    return
}

& $PythonExe -X utf8 .\apps\grpc-server\main.py @ServerArgs
