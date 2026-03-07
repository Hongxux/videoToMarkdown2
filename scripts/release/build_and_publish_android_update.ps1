param(
    [int]$VersionCode = 0,
    [string]$VersionName = "",
    [switch]$AutoBumpPatch,
    [switch]$UpdateGradleVersion,
    [string]$ApkPath = "",
    [switch]$SkipBuild,
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
    [int]$TimeoutSeconds = 180,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-AppVersionFromGradleFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    $content = Get-Content -Path $Path -Raw -Encoding UTF8
    $versionCodeMatch = [regex]::Match($content, '(?m)^\s*versionCode\s*=\s*(\d+)\s*$')
    $versionNameMatch = [regex]::Match($content, '(?m)^\s*versionName\s*=\s*"([^"]+)"\s*$')
    if (-not $versionCodeMatch.Success -or -not $versionNameMatch.Success) {
        throw "Cannot parse versionCode/versionName from $Path"
    }
    return [pscustomobject]@{
        VersionCode = [int]$versionCodeMatch.Groups[1].Value
        VersionName = [string]$versionNameMatch.Groups[1].Value
    }
}

function Get-NextPatchVersionName {
    param([string]$CurrentVersionName)

    $raw = [string]$CurrentVersionName
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return "1.0.1"
    }

    $parts = $raw.Split(".")
    for ($i = $parts.Length - 1; $i -ge 0; $i -= 1) {
        $value = 0
        if ([int]::TryParse($parts[$i], [ref]$value)) {
            $parts[$i] = [string]($value + 1)
            return ($parts -join ".")
        }
    }
    return "$raw.1"
}

function Set-AppVersionInGradleFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][int]$NextVersionCode,
        [Parameter(Mandatory = $true)][string]$NextVersionName
    )

    $content = Get-Content -Path $Path -Raw -Encoding UTF8
    $afterCode = [regex]::Replace(
        $content,
        '(?m)^(\s*versionCode\s*=\s*)\d+(\s*)$',
        { param($m) $m.Groups[1].Value + [string]$NextVersionCode + $m.Groups[2].Value },
        1
    )
    if ($afterCode -eq $content) {
        throw "Failed to update versionCode in $Path"
    }

    $afterName = [regex]::Replace(
        $afterCode,
        '(?m)^(\s*versionName\s*=\s*)"[^"]*"(\s*)$',
        { param($m) $m.Groups[1].Value + '"' + $NextVersionName + '"' + $m.Groups[2].Value },
        1
    )
    if ($afterName -eq $afterCode) {
        throw "Failed to update versionName in $Path"
    }

    Set-Content -Path $Path -Value $afterName -Encoding UTF8
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$gradleFilePath = Join-Path $repoRoot "app/build.gradle.kts"
$releaseScriptPath = Join-Path $PSScriptRoot "release_and_verify_android_update.ps1"

if (-not (Test-Path -Path $gradleFilePath -PathType Leaf)) {
    throw "Missing gradle file: $gradleFilePath"
}
if (-not (Test-Path -Path $releaseScriptPath -PathType Leaf)) {
    throw "Missing script: $releaseScriptPath"
}

$currentVersion = Get-AppVersionFromGradleFile -Path $gradleFilePath
$resolvedVersionCode = if ($VersionCode -gt 0) { $VersionCode } elseif ($AutoBumpPatch.IsPresent) { $currentVersion.VersionCode + 1 } else { $currentVersion.VersionCode }
$resolvedVersionName = if (-not [string]::IsNullOrWhiteSpace($VersionName)) { $VersionName.Trim() } elseif ($AutoBumpPatch.IsPresent) { Get-NextPatchVersionName -CurrentVersionName $currentVersion.VersionName } else { $currentVersion.VersionName }

if ($resolvedVersionCode -le 0) {
    throw "Invalid resolved versionCode: $resolvedVersionCode"
}
if ([string]::IsNullOrWhiteSpace($resolvedVersionName)) {
    throw "Invalid resolved versionName: $resolvedVersionName"
}

if ($UpdateGradleVersion.IsPresent) {
    if ($currentVersion.VersionCode -ne $resolvedVersionCode -or $currentVersion.VersionName -ne $resolvedVersionName) {
        Set-AppVersionInGradleFile -Path $gradleFilePath -NextVersionCode $resolvedVersionCode -NextVersionName $resolvedVersionName
        Write-Host "[android-release] updated app/build.gradle.kts versionCode=$resolvedVersionCode versionName=$resolvedVersionName"
    } else {
        Write-Host "[android-release] gradle version already up to date."
    }
}

if (-not $SkipBuild.IsPresent) {
    Push-Location $repoRoot
    try {
        Write-Host "[android-release] building release apk..."
        & .\gradlew.bat :app:assembleRelease -q
        if ($LASTEXITCODE -ne 0) {
            throw "assembleRelease failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
}

if ([string]::IsNullOrWhiteSpace($ApkPath)) {
    $ApkPath = Join-Path $repoRoot "app/build/outputs/apk/release/app-release.apk"
}
if (-not [System.IO.Path]::IsPathRooted($ApkPath)) {
    $ApkPath = Join-Path $repoRoot $ApkPath
}
$ApkPath = [System.IO.Path]::GetFullPath($ApkPath)
if (-not (Test-Path -Path $ApkPath -PathType Leaf)) {
    throw "APK file not found: $ApkPath"
}

Write-Host "[android-release] apkPath=$ApkPath"
Write-Host "[android-release] versionCode=$resolvedVersionCode"
Write-Host "[android-release] versionName=$resolvedVersionName"
Write-Host "[android-release] publishNow=$($NoPublish.IsPresent -eq $false)"
Write-Host "[android-release] skipVerify=$($SkipVerify.IsPresent)"
Write-Host "[android-release] dryRun=$($DryRun.IsPresent)"

$releaseArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $releaseScriptPath,
    "-ApkPath", $ApkPath,
    "-VersionCode", "$resolvedVersionCode",
    "-VersionName", $resolvedVersionName,
    "-TimeoutSeconds", "$TimeoutSeconds"
)
if ($MinSupportedVersionCode -gt 0) {
    $releaseArgs += @("-MinSupportedVersionCode", "$MinSupportedVersionCode")
}
if ($ForceUpdate.IsPresent) {
    $releaseArgs += "-ForceUpdate"
}
if (-not [string]::IsNullOrWhiteSpace($ReleaseNotes)) {
    $releaseArgs += @("-ReleaseNotes", $ReleaseNotes)
}
if (-not [string]::IsNullOrWhiteSpace($ApiBaseUrl)) {
    $releaseArgs += @("-ApiBaseUrl", $ApiBaseUrl)
}
if (-not [string]::IsNullOrWhiteSpace($AdminToken)) {
    $releaseArgs += @("-AdminToken", $AdminToken)
}
if ($UseBearerToken.IsPresent) {
    $releaseArgs += "-UseBearerToken"
}
if ($NoPublish.IsPresent) {
    $releaseArgs += "-NoPublish"
}
if ($SkipVerify.IsPresent) {
    $releaseArgs += "-SkipVerify"
}
if ($CheckClientVersionCode -gt 0) {
    $releaseArgs += @("-CheckClientVersionCode", "$CheckClientVersionCode")
}
if (-not [string]::IsNullOrWhiteSpace($CheckClientVersionName)) {
    $releaseArgs += @("-CheckClientVersionName", $CheckClientVersionName)
}
if ($DryRun.IsPresent) {
    $releaseArgs += "-DryRun"
}

& powershell @releaseArgs
exit $LASTEXITCODE
