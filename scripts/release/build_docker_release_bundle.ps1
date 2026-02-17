param(
    [string]$Version = "",
    [string]$OutputRoot = "var/releases",
    [switch]$NoZip
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($Version)) {
    $Version = Get-Date -Format "yyyyMMdd-HHmm"
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "../..")
$releaseName = "videoToMarkdown-docker-release-$Version"
$releaseDir = Join-Path $repoRoot $OutputRoot
$stageDir = Join-Path $releaseDir $releaseName
$zipPath = Join-Path $releaseDir "$releaseName.zip"

function New-ParentDir {
    param([string]$Path)
    $parent = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($parent) -and -not (Test-Path $parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
}

function Copy-RepoFile {
    param(
        [string]$SourceRelativePath,
        [string]$TargetRelativePath
    )
    $sourcePath = Join-Path $repoRoot $SourceRelativePath
    if (-not (Test-Path $sourcePath)) {
        throw "Missing source file: $SourceRelativePath"
    }
    $targetPath = Join-Path $stageDir $TargetRelativePath
    New-ParentDir -Path $targetPath
    Copy-Item -Force -Path $sourcePath -Destination $targetPath
}

function Write-SanitizedConfigFile {
    param(
        [string]$SourceRelativePath,
        [string]$TargetRelativePath
    )
    $sourcePath = Join-Path $repoRoot $SourceRelativePath
    if (-not (Test-Path $sourcePath)) {
        throw "Missing config file: $SourceRelativePath"
    }

    $content = Get-Content -Path $sourcePath -Raw -Encoding UTF8
    $content = [regex]::Replace($content, '(?m)^(\s*bearer_token:\s*)".*?"(\s*(#.*)?)$', '$1""$2')
    $content = [regex]::Replace($content, '(?m)^(\s*api_key:\s*)".*?"(\s*(#.*)?)$', '$1""$2')

    $targetPath = Join-Path $stageDir $TargetRelativePath
    New-ParentDir -Path $targetPath
    Set-Content -Path $targetPath -Value $content -Encoding UTF8
}

if (-not (Test-Path $releaseDir)) {
    New-Item -ItemType Directory -Force -Path $releaseDir | Out-Null
}

if (Test-Path $stageDir) {
    Remove-Item -Recurse -Force $stageDir
}
New-Item -ItemType Directory -Force -Path $stageDir | Out-Null

Copy-RepoFile -SourceRelativePath "docker-compose.yml" -TargetRelativePath "docker-compose.yml"
Copy-RepoFile -SourceRelativePath "deploy/docker/python-grpc.Dockerfile" -TargetRelativePath "deploy/docker/python-grpc.Dockerfile"
Copy-RepoFile -SourceRelativePath "deploy/docker/java-orchestrator.Dockerfile" -TargetRelativePath "deploy/docker/java-orchestrator.Dockerfile"
Copy-RepoFile -SourceRelativePath "deploy/docker/.env.example" -TargetRelativePath ".env.example"
Copy-RepoFile -SourceRelativePath "scripts/release/docker_release.ps1" -TargetRelativePath "scripts/release/docker_release.ps1"
Copy-RepoFile -SourceRelativePath "README.DockerRelease.md" -TargetRelativePath "README.md"

Write-SanitizedConfigFile -SourceRelativePath "config/video_config.yaml" -TargetRelativePath "config/video_config.yaml"
Write-SanitizedConfigFile -SourceRelativePath "config/module2_config.yaml" -TargetRelativePath "config/module2_config.yaml"
Copy-RepoFile -SourceRelativePath "config/fault_detection_config.yaml" -TargetRelativePath "config/fault_detection_config.yaml"
Copy-RepoFile -SourceRelativePath "config/dictionaries.yaml" -TargetRelativePath "config/dictionaries.yaml"

if (-not $NoZip) {
    if (Test-Path $zipPath) {
        Remove-Item -Force $zipPath
    }
    $items = Get-ChildItem -Force $stageDir
    if ($items.Count -gt 0) {
        Compress-Archive -Path $items.FullName -DestinationPath $zipPath -CompressionLevel Optimal
    }
}

Write-Host "release_name=$releaseName"
Write-Host "release_dir=$stageDir"
if (-not $NoZip) {
    Write-Host "release_zip=$zipPath"
}
