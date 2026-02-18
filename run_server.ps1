param(
    [string]$PythonExe = "D:\New_ANACONDA\envs\whisper_env\python.exe",
    [string]$RepoRoot = "D:\videoToMarkdownTest2",
    [switch]$CheckDeps,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ServerArgs
)

$ErrorActionPreference = "Stop"

# 固定使用目标解释器，避免 PATH/conda 状态导致落到系统 Python。
if (-not (Test-Path $PythonExe)) {
    throw "Python executable not found: $PythonExe"
}

if (-not (Test-Path $RepoRoot)) {
    throw "Repo root not found: $RepoRoot"
}

Set-Location $RepoRoot

# 禁用 user-site，避免用户目录包污染运行时依赖解析。
$env:PYTHONNOUSERSITE = "1"
$env:PYTHONIOENCODING = "utf-8"

Write-Host "Using Python:" $PythonExe
Write-Host "Repo root:" $RepoRoot
Write-Host "PYTHONNOUSERSITE=" $env:PYTHONNOUSERSITE

if ($CheckDeps) {
    & $PythonExe -X utf8 .\apps\grpc-server\main.py --check-deps
    if ($LASTEXITCODE -ne 0) {
        throw "Dependency preflight failed with exit code $LASTEXITCODE"
    }
}

& $PythonExe -X utf8 .\apps\grpc-server\main.py @ServerArgs
