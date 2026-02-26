param(
    [Parameter(Mandatory = $true)]
    [string]$ApkPath,
    [Parameter(Mandatory = $true)]
    [int]$VersionCode,
    [Parameter(Mandatory = $true)]
    [string]$VersionName,
    [int]$MinSupportedVersionCode = 0,
    [switch]$ForceUpdate,
    [string]$ReleaseNotes = "",
    [string]$ApiBaseUrl = "",
    [string]$AdminToken = "",
    [switch]$UseBearerToken,
    [switch]$NoPublish,
    [switch]$SkipVerify,
    [int]$CheckClientVersionCode = -1,
    [string]$CheckClientVersionName = "",
    [int]$TimeoutSeconds = 120,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ApiBaseUrl)) {
    $ApiBaseUrl = $env:MOBILE_API_BASE_URL
}
if ([string]::IsNullOrWhiteSpace($ApiBaseUrl)) {
    $ApiBaseUrl = "http://localhost:8080"
}
$ApiBaseUrl = $ApiBaseUrl.TrimEnd("/")

if ([string]::IsNullOrWhiteSpace($AdminToken)) {
    $AdminToken = $env:MOBILE_UPDATE_ADMIN_TOKEN
}
if ([string]::IsNullOrWhiteSpace($AdminToken)) {
    throw "Admin token is required. Set -AdminToken or env MOBILE_UPDATE_ADMIN_TOKEN."
}

if (-not [System.IO.Path]::IsPathRooted($ApkPath)) {
    $ApkPath = Join-Path (Get-Location) $ApkPath
}
$ApkPath = [System.IO.Path]::GetFullPath($ApkPath)
if (-not (Test-Path -Path $ApkPath -PathType Leaf)) {
    throw "APK file not found: $ApkPath"
}

$commitScript = Join-Path $PSScriptRoot "commit_android_update.ps1"
if (-not (Test-Path -Path $commitScript -PathType Leaf)) {
    throw "Missing script: $commitScript"
}

Write-Host "[android-update] step=commit-upload"
Write-Host "[android-update] apiBaseUrl=$ApiBaseUrl"
Write-Host "[android-update] versionCode=$VersionCode versionName=$VersionName"
Write-Host "[android-update] publishNow=$($NoPublish.IsPresent -eq $false)"

$commitArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $commitScript,
    "-ApkPath", $ApkPath,
    "-VersionCode", "$VersionCode",
    "-VersionName", $VersionName,
    "-ApiBaseUrl", $ApiBaseUrl,
    "-AdminToken", $AdminToken,
    "-TimeoutSeconds", "$TimeoutSeconds"
)
if ($MinSupportedVersionCode -gt 0) {
    $commitArgs += @("-MinSupportedVersionCode", "$MinSupportedVersionCode")
}
if ($ForceUpdate.IsPresent) {
    $commitArgs += "-ForceUpdate"
}
if (-not [string]::IsNullOrWhiteSpace($ReleaseNotes)) {
    $commitArgs += @("-ReleaseNotes", $ReleaseNotes)
}
if (-not $NoPublish.IsPresent) {
    $commitArgs += "-PublishNow"
}
if ($UseBearerToken.IsPresent) {
    $commitArgs += "-UseBearerToken"
}
if ($DryRun.IsPresent) {
    $commitArgs += "-DryRun"
}

& powershell @commitArgs
if ($LASTEXITCODE -ne 0) {
    throw "Commit/upload step failed with exit code $LASTEXITCODE"
}

if ($DryRun.IsPresent) {
    Write-Host "[android-update] dry-run complete."
    exit 0
}

if ($NoPublish.IsPresent) {
    Write-Host "[android-update] publish skipped by -NoPublish. Verify skipped."
    exit 0
}

if ($SkipVerify.IsPresent) {
    Write-Host "[android-update] verify skipped by -SkipVerify."
    exit 0
}

$verifyClientVersionCode = $CheckClientVersionCode
if ($verifyClientVersionCode -le 0) {
    if ($VersionCode -gt 1) {
        $verifyClientVersionCode = $VersionCode - 1
    } else {
        $verifyClientVersionCode = 1
    }
}

$headers = @{}
$timeoutSec = [Math]::Max(10, $TimeoutSeconds)
$encodedClientVersionName = [System.Uri]::EscapeDataString($CheckClientVersionName)
$oldCheckUri = "$ApiBaseUrl/api/mobile/app/update/check?versionCode=$verifyClientVersionCode"
if (-not [string]::IsNullOrWhiteSpace($CheckClientVersionName)) {
    $oldCheckUri = "$oldCheckUri&versionName=$encodedClientVersionName"
}

$encodedCurrentVersionName = [System.Uri]::EscapeDataString($VersionName)
$newCheckUri = "$ApiBaseUrl/api/mobile/app/update/check?versionCode=$VersionCode&versionName=$encodedCurrentVersionName"

Write-Host "[android-update] step=verify-check oldClientVersionCode=$verifyClientVersionCode"
$oldResult = Invoke-RestMethod -Method Get -Uri $oldCheckUri -Headers $headers -TimeoutSec $timeoutSec -ErrorAction Stop
if (-not $oldResult.success) {
    throw "Verify failed: old client check returned success=false"
}
if ([int]$oldResult.latestVersionCode -ne $VersionCode) {
    throw "Verify failed: latestVersionCode=$($oldResult.latestVersionCode), expected $VersionCode"
}
if ($verifyClientVersionCode -lt $VersionCode -and -not [bool]$oldResult.hasUpdate) {
    throw "Verify failed: hasUpdate=false for old client versionCode=$verifyClientVersionCode"
}

Write-Host "[android-update] step=verify-check currentClientVersionCode=$VersionCode"
$newResult = Invoke-RestMethod -Method Get -Uri $newCheckUri -Headers $headers -TimeoutSec $timeoutSec -ErrorAction Stop
if (-not $newResult.success) {
    throw "Verify failed: current client check returned success=false"
}
if ([int]$newResult.latestVersionCode -ne $VersionCode) {
    throw "Verify failed: latestVersionCode=$($newResult.latestVersionCode), expected $VersionCode"
}
if ([bool]$newResult.hasUpdate) {
    throw "Verify failed: hasUpdate=true for current versionCode=$VersionCode"
}

Write-Host "[android-update] verify success."
[pscustomobject]@{
    uploadedVersionCode = $VersionCode
    uploadedVersionName = $VersionName
    oldClientCheck = $oldResult
    currentClientCheck = $newResult
} | ConvertTo-Json -Depth 12
