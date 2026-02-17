param(
    [ValidateSet("up", "down", "logs", "ps", "restart")]
    [string]$Action = "up"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "../..")
Set-Location $repoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "docker command not found. Install Docker Desktop first."
    exit 1
}

$dockerInfoOk = $true
$prevErrorAction = $ErrorActionPreference
try {
    $ErrorActionPreference = "Continue"
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        $dockerInfoOk = $false
    }
} finally {
    $ErrorActionPreference = $prevErrorAction
}

if (-not $dockerInfoOk) {
    Write-Error "Docker engine is not running. Start Docker Desktop first."
    exit 1
}

if ($Action -eq "up") {
    Write-Host "[release] docker compose up -d --build"
    docker compose up -d --build
    exit $LASTEXITCODE
}

if ($Action -eq "down") {
    Write-Host "[release] docker compose down"
    docker compose down
    exit $LASTEXITCODE
}

if ($Action -eq "logs") {
    Write-Host "[release] docker compose logs -f --tail=200"
    docker compose logs -f --tail=200
    exit $LASTEXITCODE
}

if ($Action -eq "ps") {
    Write-Host "[release] docker compose ps"
    docker compose ps
    exit $LASTEXITCODE
}

Write-Host "[release] docker compose restart"
docker compose restart
exit $LASTEXITCODE
