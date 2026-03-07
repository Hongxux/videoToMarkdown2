param(
    [switch]$SkipOpen,
    [switch]$NoBuild,
    [int]$WaitSeconds = 180
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "../..")
Set-Location $repoRoot

function Test-CommandExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Test-DockerEngineRunning {
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        docker info *> $null
        return $LASTEXITCODE -eq 0
    } finally {
        $ErrorActionPreference = $previousErrorAction
    }
}

if (-not (Test-CommandExists -Name "docker")) {
    Write-Error "未检测到 docker，请先安装并启动 Docker Desktop。"
    exit 1
}

if (-not (Test-DockerEngineRunning)) {
    Write-Error "Docker Engine 未启动，请先打开 Docker Desktop。"
    exit 1
}

$envTemplatePath = Join-Path $repoRoot "deploy/docker/.env.example"
$envPath = Join-Path $repoRoot ".env"

if (-not (Test-Path $envPath)) {
    Copy-Item $envTemplatePath $envPath
    Write-Host "[quick-start] 已从 deploy/docker/.env.example 生成 .env 模板。"
    Write-Host "[quick-start] 通常只需要填写 DEEPSEEK_API_KEY 和 DASHSCOPE_API_KEY。"
    Write-Host "[quick-start] 其余参数已对齐作者截至 2026-03-07 的本机最佳效果配置。"
}

$composeArgs = @("compose", "up", "-d")
if (-not $NoBuild) {
    $composeArgs += "--build"
}

Write-Host ("[quick-start] docker " + ($composeArgs -join " "))
& docker @composeArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker Compose 启动失败，请先查看容器日志。"
    exit $LASTEXITCODE
}

$healthUrl = "http://localhost:8080/api/health"
$webDemoUrl = "http://localhost:8080"
$deadline = (Get-Date).AddSeconds($WaitSeconds)
$healthy = $false

Write-Host "[quick-start] 正在等待 http://localhost:8080/api/health 就绪..."
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 5
    try {
        Invoke-RestMethod -Method Get -Uri $healthUrl -TimeoutSec 5 *> $null
        $healthy = $true
        break
    } catch {
    }
}

if (-not $healthy) {
    Write-Warning "健康检查在限定时间内未通过，请执行日志命令排查。"
    Write-Host "[quick-start] 查看日志：powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action logs"
    Write-Host "[quick-start] 查看状态：powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action ps"
    exit 0
}

Write-Host "[quick-start] Web Demo 已就绪。"
Write-Host "[quick-start] Web Demo: http://localhost:8080"
Write-Host "[quick-start] Health API: http://localhost:8080/api/health"
Write-Host "[quick-start] 查看日志：powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action logs"
Write-Host "[quick-start] Android 模拟器安装示例：.\gradlew.bat :app:installDebug -PmobileApiBaseUrl=http://10.0.2.2:8080/api/mobile"

if (-not $SkipOpen) {
    try {
        Start-Process $webDemoUrl | Out-Null
    } catch {
        Write-Warning "浏览器未能自动打开，请手动访问 http://localhost:8080。"
    }
}
