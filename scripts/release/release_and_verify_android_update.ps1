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

function Has-Property {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name
    )
    if ($null -eq $Object) {
        return $false
    }
    return $Object.PSObject.Properties.Name -contains $Name
}

function Read-RequiredIntProperty {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Context
    )
    if (-not (Has-Property -Object $Object -Name $Name)) {
        throw "Verify failed: missing property '$Name' in $Context"
    }
    return [int]$Object.$Name
}

function Read-OptionalBoolProperty {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        [bool]$DefaultValue = $false
    )
    if (-not (Has-Property -Object $Object -Name $Name)) {
        return $DefaultValue
    }
    return [bool]$Object.$Name
}

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
$apiHost = ""
try {
    $apiHost = ([System.Uri]$ApiBaseUrl).Host
} catch {
    $apiHost = ""
}
if (-not [string]::IsNullOrWhiteSpace($apiHost) -and $apiHost.ToLowerInvariant().Contains("ngrok")) {
    $headers["ngrok-skip-browser-warning"] = "1"
}
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
if ($oldResult -is [string]) {
    throw "Verify failed: old client check returned non-JSON response. body-prefix=$($oldResult.Substring(0, [Math]::Min(160, $oldResult.Length)))"
}
$oldSuccess = Read-OptionalBoolProperty -Object $oldResult -Name "success" -DefaultValue $true
if (-not $oldSuccess) {
    throw "Verify failed: old client check returned success=false"
}
$oldLatestVersionCode = Read-RequiredIntProperty -Object $oldResult -Name "latestVersionCode" -Context "old client check"
if ($oldLatestVersionCode -ne $VersionCode) {
    throw "Verify failed: latestVersionCode=$oldLatestVersionCode, expected $VersionCode"
}
$oldHasUpdate = Read-OptionalBoolProperty -Object $oldResult -Name "hasUpdate" -DefaultValue $false
if ($verifyClientVersionCode -lt $VersionCode -and -not $oldHasUpdate) {
    throw "Verify failed: hasUpdate=false for old client versionCode=$verifyClientVersionCode"
}

Write-Host "[android-update] step=verify-check currentClientVersionCode=$VersionCode"
$newResult = Invoke-RestMethod -Method Get -Uri $newCheckUri -Headers $headers -TimeoutSec $timeoutSec -ErrorAction Stop
if ($newResult -is [string]) {
    throw "Verify failed: current client check returned non-JSON response. body-prefix=$($newResult.Substring(0, [Math]::Min(160, $newResult.Length)))"
}
$newSuccess = Read-OptionalBoolProperty -Object $newResult -Name "success" -DefaultValue $true
if (-not $newSuccess) {
    throw "Verify failed: current client check returned success=false"
}
$newLatestVersionCode = Read-RequiredIntProperty -Object $newResult -Name "latestVersionCode" -Context "current client check"
if ($newLatestVersionCode -ne $VersionCode) {
    throw "Verify failed: latestVersionCode=$newLatestVersionCode, expected $VersionCode"
}
$newHasUpdate = Read-OptionalBoolProperty -Object $newResult -Name "hasUpdate" -DefaultValue $false
if ($newHasUpdate) {
    throw "Verify failed: hasUpdate=true for current versionCode=$VersionCode"
}

Write-Host "[android-update] verify success."
[pscustomobject]@{
    uploadedVersionCode = $VersionCode
    uploadedVersionName = $VersionName
    oldClientCheck = $oldResult
    currentClientCheck = $newResult
} | ConvertTo-Json -Depth 12
