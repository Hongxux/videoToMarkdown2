param(
    [Parameter(Mandatory = $true)]
    [int]$VersionCode,
    [string]$ApiBaseUrl = "",
    [string]$AdminToken = "",
    [switch]$UseBearerToken,
    [switch]$DryRun,
    [int]$TimeoutSeconds = 60
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

$endpoint = "$ApiBaseUrl/api/mobile/app/update/admin/publish?versionCode=$VersionCode"
Write-Host "[android-update] endpoint=$endpoint"
Write-Host "[android-update] versionCode=$VersionCode"

if ($DryRun.IsPresent) {
    Write-Host "[android-update] dry run enabled, request not sent."
    exit 0
}

$headers = @{}
if ($UseBearerToken.IsPresent) {
    $headers["Authorization"] = "Bearer $AdminToken"
} else {
    $headers["X-Update-Admin-Token"] = $AdminToken
}

$params = @{
    Method      = "Post"
    Uri         = $endpoint
    Headers     = $headers
    TimeoutSec  = [Math]::Max(10, $TimeoutSeconds)
    ErrorAction = "Stop"
}

try {
    $response = Invoke-RestMethod @params
    Write-Host "[android-update] publish success."
    $response | ConvertTo-Json -Depth 10
} catch {
    $errorMessage = $_.Exception.Message
    $resp = $_.Exception.Response
    if ($resp -ne $null) {
        try {
            $stream = $resp.GetResponseStream()
            $reader = [System.IO.StreamReader]::new($stream)
            $rawBody = $reader.ReadToEnd()
            $reader.Dispose()
            throw "Publish failed: $errorMessage`n$rawBody"
        } catch {
            throw "Publish failed: $errorMessage"
        }
    }
    throw
}

