param(
    [int]$TargetVersionCode = 0,
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

$endpoint = "$ApiBaseUrl/api/mobile/app/update/admin/rollback"
if ($TargetVersionCode -gt 0) {
    $endpoint = "$endpoint?targetVersionCode=$TargetVersionCode"
}

Write-Host "[android-update] endpoint=$endpoint"
if ($TargetVersionCode -gt 0) {
    Write-Host "[android-update] targetVersionCode=$TargetVersionCode"
} else {
    Write-Host "[android-update] targetVersionCode=auto(previous published)"
}

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
    Write-Host "[android-update] rollback success."
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
            throw "Rollback failed: $errorMessage`n$rawBody"
        } catch {
            throw "Rollback failed: $errorMessage"
        }
    }
    throw
}

