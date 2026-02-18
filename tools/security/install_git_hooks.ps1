param()

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path
$hookFile = Join-Path $repoRoot ".githooks\\pre-commit"

if (-not (Test-Path $hookFile)) {
    throw "Hook file not found: $hookFile"
}

git -C $repoRoot config core.hooksPath ".githooks"
Write-Output "[hook-guard] core.hooksPath is set to .githooks"
Write-Output "[hook-guard] pre-commit secret + encoding scan is now enabled."
