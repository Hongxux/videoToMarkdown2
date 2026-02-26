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
    [switch]$PublishNow,
    [switch]$DryRun,
    [int]$TimeoutSeconds = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not [System.IO.Path]::IsPathRooted($ApkPath)) {
    $ApkPath = Join-Path (Get-Location) $ApkPath
}
$ApkPath = [System.IO.Path]::GetFullPath($ApkPath)

if (-not (Test-Path -Path $ApkPath -PathType Leaf)) {
    throw "APK file not found: $ApkPath"
}
if (-not $ApkPath.ToLowerInvariant().EndsWith(".apk")) {
    throw "ApkPath must end with .apk"
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

$endpoint = "$ApiBaseUrl/api/mobile/app/update/admin/upload"
$publishValue = if ($PublishNow.IsPresent) { "true" } else { "false" }
$forceUpdateValue = if ($ForceUpdate.IsPresent) { "true" } else { "false" }

Write-Host "[android-update] endpoint=$endpoint"
Write-Host "[android-update] apk=$ApkPath"
Write-Host "[android-update] versionCode=$VersionCode versionName=$VersionName publishNow=$publishValue"

if ($DryRun.IsPresent) {
    Write-Host "[android-update] dry run enabled, request not sent."
    exit 0
}

Add-Type -AssemblyName System.Net.Http

$handler = [System.Net.Http.HttpClientHandler]::new()
$client = [System.Net.Http.HttpClient]::new($handler)
$client.Timeout = [TimeSpan]::FromSeconds([Math]::Max(10, $TimeoutSeconds))

$fileStream = $null
$multipart = $null

try {
    $multipart = [System.Net.Http.MultipartFormDataContent]::new()

    $fileStream = [System.IO.File]::OpenRead($ApkPath)
    $fileContent = [System.Net.Http.StreamContent]::new($fileStream)
    $fileContent.Headers.ContentType = [System.Net.Http.Headers.MediaTypeHeaderValue]::Parse(
        "application/vnd.android.package-archive"
    )
    $multipart.Add($fileContent, "apk", [System.IO.Path]::GetFileName($ApkPath))

    $multipart.Add([System.Net.Http.StringContent]::new("$VersionCode"), "versionCode")
    $multipart.Add([System.Net.Http.StringContent]::new($VersionName), "versionName")
    if ($MinSupportedVersionCode -gt 0) {
        $multipart.Add(
            [System.Net.Http.StringContent]::new("$MinSupportedVersionCode"),
            "minSupportedVersionCode"
        )
    }
    $multipart.Add([System.Net.Http.StringContent]::new($forceUpdateValue), "forceUpdate")
    $multipart.Add([System.Net.Http.StringContent]::new($publishValue), "publish")
    if (-not [string]::IsNullOrWhiteSpace($ReleaseNotes)) {
        $multipart.Add([System.Net.Http.StringContent]::new($ReleaseNotes), "releaseNotes")
    }

    if ($UseBearerToken.IsPresent) {
        $client.DefaultRequestHeaders.Authorization =
            [System.Net.Http.Headers.AuthenticationHeaderValue]::new("Bearer", $AdminToken)
    } else {
        $null = $client.DefaultRequestHeaders.Remove("X-Update-Admin-Token")
        $null = $client.DefaultRequestHeaders.Add("X-Update-Admin-Token", $AdminToken)
    }

    $response = $client.PostAsync($endpoint, $multipart).GetAwaiter().GetResult()
    $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    if (-not $response.IsSuccessStatusCode) {
        throw "Upload failed: HTTP $([int]$response.StatusCode) $($response.ReasonPhrase)`n$body"
    }

    Write-Host "[android-update] upload success."
    try {
        $json = $body | ConvertFrom-Json
        $json | ConvertTo-Json -Depth 10
    } catch {
        $body
    }
} finally {
    if ($multipart -ne $null) {
        $multipart.Dispose()
    }
    if ($fileStream -ne $null) {
        $fileStream.Dispose()
    }
    $client.Dispose()
    $handler.Dispose()
}

