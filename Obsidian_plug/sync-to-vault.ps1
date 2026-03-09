param(
  [string]$VaultPluginDir,
  [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'

function Get-DefaultVaultPluginDir {
  $documentsDir = [Environment]::GetFolderPath([Environment+SpecialFolder]::MyDocuments)
  if ([string]::IsNullOrWhiteSpace($documentsDir)) {
    throw 'Cannot resolve MyDocuments folder for default Vault path.'
  }

  return (Join-Path $documentsDir 'Obsidian Vault\.obsidian\plugins\Obsidian_plug')
}

$pluginDir = $PSScriptRoot
$resolvedVaultPluginDir = if ([string]::IsNullOrWhiteSpace($VaultPluginDir)) {
  Get-DefaultVaultPluginDir
} else {
  $VaultPluginDir
}

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

if (-not (Test-Path $resolvedVaultPluginDir)) {
  New-Item -ItemType Directory -Force -Path $resolvedVaultPluginDir | Out-Null
}

foreach ($file in $runtimeFiles) {
  $sourcePath = Join-Path $pluginDir $file
  if (-not (Test-Path $sourcePath)) {
    throw "missing runtime file: $sourcePath"
  }
  $targetPath = Join-Path $resolvedVaultPluginDir $file
  Copy-Item -LiteralPath $sourcePath -Destination $targetPath -Force
}

Write-Output ("Synced runtime files to {0}" -f $resolvedVaultPluginDir)
Write-Output ("Files: {0}" -f ($runtimeFiles -join ', '))