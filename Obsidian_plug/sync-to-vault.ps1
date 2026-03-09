param(
  [string]$VaultPluginDir = 'D:\云库\OneDrive\文档\Obsidian Vault\.obsidian\plugins\Obsidian_plug',
  [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'

$pluginDir = $PSScriptRoot
$runtimeFiles = @('manifest.json', 'main.js')

$stylesPath = Join-Path $pluginDir 'styles.css'
if (Test-Path $stylesPath) {
  $runtimeFiles += 'styles.css'
}

if (-not $SkipBuild) {
  Push-Location $pluginDir
  try {
    & npm.cmd run build
    if ($LASTEXITCODE -ne 0) {
      throw "npm run build failed with exit code $LASTEXITCODE"
    }
  }
  finally {
    Pop-Location
  }
}

if (-not (Test-Path $VaultPluginDir)) {
  New-Item -ItemType Directory -Force -Path $VaultPluginDir | Out-Null
}

foreach ($file in $runtimeFiles) {
  $sourcePath = Join-Path $pluginDir $file
  if (-not (Test-Path $sourcePath)) {
    throw "missing runtime file: $sourcePath"
  }
  $targetPath = Join-Path $VaultPluginDir $file
  Copy-Item -LiteralPath $sourcePath -Destination $targetPath -Force
}

Write-Output ("Synced runtime files to {0}" -f $VaultPluginDir)
Write-Output ("Files: {0}" -f ($runtimeFiles -join ', '))