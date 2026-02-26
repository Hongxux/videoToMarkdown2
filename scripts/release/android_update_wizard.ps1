param(
    [string]$ApiBaseUrl = "",
    [string]$AdminToken = "",
    [ValidateSet("header", "bearer")]
    [string]$AuthMode = "",
    [switch]$DryRun,
    [switch]$PromptGlobalSettings
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-TextWithDefault {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prompt,
        [string]$DefaultValue = ""
    )
    while ($true) {
        if ([string]::IsNullOrWhiteSpace($DefaultValue)) {
            $value = Read-Host $Prompt
        } else {
            $value = Read-Host "$Prompt [$DefaultValue]"
            if ([string]::IsNullOrWhiteSpace($value)) {
                $value = $DefaultValue
            }
        }
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            return $value.Trim()
        }
        Write-Host "Input cannot be empty. Please try again." -ForegroundColor Yellow
    }
}

function Read-OptionalText {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prompt,
        [string]$DefaultValue = ""
    )
    if ([string]::IsNullOrWhiteSpace($DefaultValue)) {
        $value = Read-Host $Prompt
    } else {
        $value = Read-Host "$Prompt [$DefaultValue]"
        if ([string]::IsNullOrWhiteSpace($value)) {
            $value = $DefaultValue
        }
    }
    if ([string]::IsNullOrWhiteSpace($value)) {
        return ""
    }
    return $value.Trim()
}

function Read-IntValue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prompt,
        [int]$DefaultValue = 0,
        [switch]$AllowZero
    )
    while ($true) {
        $raw = ""
        if ($DefaultValue -gt 0 -or $AllowZero.IsPresent) {
            $raw = Read-Host "$Prompt [$DefaultValue]"
            if ([string]::IsNullOrWhiteSpace($raw)) {
                return $DefaultValue
            }
        } else {
            $raw = Read-Host $Prompt
        }

        $parsed = 0
        if ([int]::TryParse($raw, [ref]$parsed)) {
            if ($AllowZero.IsPresent -and $parsed -ge 0) {
                return $parsed
            }
            if ($parsed -gt 0) {
                return $parsed
            }
        }
        Write-Host "Please input a valid integer." -ForegroundColor Yellow
    }
}

function Read-YesNo {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prompt,
        [bool]$DefaultValue = $false
    )
    $defaultText = if ($DefaultValue) { "Y" } else { "N" }
    while ($true) {
        $raw = Read-Host "$Prompt [Y/N, default $defaultText]"
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return $DefaultValue
        }
        $v = $raw.Trim().ToLowerInvariant()
        if ($v -eq "y" -or $v -eq "yes" -or $v -eq "1") {
            return $true
        }
        if ($v -eq "n" -or $v -eq "no" -or $v -eq "0") {
            return $false
        }
        Write-Host "Please input Y or N." -ForegroundColor Yellow
    }
}

function Confirm-OrExit {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$SummaryLines
    )
    Write-Host ""
    Write-Host "Please confirm the parameters:" -ForegroundColor Cyan
    foreach ($line in $SummaryLines) {
        Write-Host "  - $line"
    }
    $ok = Read-YesNo -Prompt "Continue?" -DefaultValue $true
    if (-not $ok) {
        Write-Host "Cancelled." -ForegroundColor Yellow
        exit 0
    }
}

Write-Host "Android Update Wizard" -ForegroundColor Cyan
Write-Host "Actions: 1) Commit upload  2) Publish  3) Rollback  4) Commit and Publish"
Write-Host ""

if (-not [string]::IsNullOrWhiteSpace($ApiBaseUrl)) {
    Write-Host "API base url fixed from parameter."
} elseif (-not [string]::IsNullOrWhiteSpace($env:MOBILE_API_BASE_URL)) {
    $ApiBaseUrl = $env:MOBILE_API_BASE_URL
    Write-Host "API base url loaded from env MOBILE_API_BASE_URL."
}

if (-not [string]::IsNullOrWhiteSpace($AdminToken)) {
    Write-Host "Admin token fixed from parameter."
} elseif (-not [string]::IsNullOrWhiteSpace($env:MOBILE_UPDATE_ADMIN_TOKEN)) {
    $AdminToken = $env:MOBILE_UPDATE_ADMIN_TOKEN
    Write-Host "Admin token loaded from env MOBILE_UPDATE_ADMIN_TOKEN."
}

if (-not [string]::IsNullOrWhiteSpace($AuthMode)) {
    $AuthMode = $AuthMode.Trim().ToLowerInvariant()
    Write-Host "Auth mode fixed from parameter: $AuthMode"
} elseif (-not [string]::IsNullOrWhiteSpace($env:MOBILE_UPDATE_AUTH_MODE)) {
    $AuthMode = $env:MOBILE_UPDATE_AUTH_MODE.Trim().ToLowerInvariant()
    Write-Host "Auth mode loaded from env MOBILE_UPDATE_AUTH_MODE: $AuthMode"
}

if ($AuthMode -ne "header" -and $AuthMode -ne "bearer") {
    $AuthMode = "header"
}

if (-not $DryRun.IsPresent -and -not [string]::IsNullOrWhiteSpace($env:MOBILE_UPDATE_DRY_RUN)) {
    $dryRunEnv = $env:MOBILE_UPDATE_DRY_RUN.Trim().ToLowerInvariant()
    if ($dryRunEnv -eq "1" -or $dryRunEnv -eq "true" -or $dryRunEnv -eq "yes" -or $dryRunEnv -eq "y") {
        $DryRun = $true
        Write-Host "DryRun enabled from env MOBILE_UPDATE_DRY_RUN."
    } elseif ($dryRunEnv -eq "0" -or $dryRunEnv -eq "false" -or $dryRunEnv -eq "no" -or $dryRunEnv -eq "n") {
        Write-Host "DryRun disabled from env MOBILE_UPDATE_DRY_RUN."
    }
}

if ($PromptGlobalSettings.IsPresent -or [string]::IsNullOrWhiteSpace($ApiBaseUrl)) {
    $ApiBaseUrl = Read-TextWithDefault -Prompt "API base url" -DefaultValue (
        if ([string]::IsNullOrWhiteSpace($ApiBaseUrl)) { "http://localhost:8080" } else { $ApiBaseUrl }
    )
}
if ($PromptGlobalSettings.IsPresent -or [string]::IsNullOrWhiteSpace($AdminToken)) {
    $AdminToken = Read-TextWithDefault -Prompt "Admin token" -DefaultValue $AdminToken
}
if ($PromptGlobalSettings.IsPresent -or [string]::IsNullOrWhiteSpace($AuthMode)) {
    $modeInput = Read-TextWithDefault -Prompt "Auth mode (header/bearer)" -DefaultValue (
        if ([string]::IsNullOrWhiteSpace($AuthMode)) { "header" } else { $AuthMode }
    )
    $AuthMode = $modeInput.Trim().ToLowerInvariant()
}
while ($AuthMode -ne "header" -and $AuthMode -ne "bearer") {
    Write-Host "Auth mode must be header or bearer." -ForegroundColor Yellow
    $modeInput = Read-TextWithDefault -Prompt "Auth mode (header/bearer)" -DefaultValue "header"
    $AuthMode = $modeInput.Trim().ToLowerInvariant()
}
$useBearerToken = $AuthMode -eq "bearer"

if ($PromptGlobalSettings.IsPresent -or -not $DryRun.IsPresent) {
    $dryRunSelected = Read-YesNo -Prompt "DryRun only (do not send request)?" -DefaultValue $false
    if ($dryRunSelected) {
        $DryRun = $true
    } elseif ($PromptGlobalSettings.IsPresent) {
        $DryRun = $false
    }
} else {
    Write-Host "DryRun fixed: true"
}

Write-Host ""
Write-Host "Global settings confirmed:" -ForegroundColor Cyan
Write-Host "  - ApiBaseUrl=$ApiBaseUrl"
Write-Host "  - AuthMode=$AuthMode"
Write-Host "  - DryRun=$($DryRun.IsPresent)"
Write-Host ""

$action = Read-IntValue -Prompt "Select action (1/2/3/4)" -DefaultValue 1
while ($action -lt 1 -or $action -gt 4) {
    Write-Host "Action must be 1, 2, 3 or 4." -ForegroundColor Yellow
    $action = Read-IntValue -Prompt "Select action (1/2/3/4)" -DefaultValue 1
}

$commitScript = Join-Path $PSScriptRoot "commit_android_update.ps1"
$publishScript = Join-Path $PSScriptRoot "publish_android_update.ps1"
$rollbackScript = Join-Path $PSScriptRoot "rollback_android_update.ps1"

if ($action -eq 1 -or $action -eq 4) {
    $apkPath = Read-TextWithDefault -Prompt "APK file path"
    while (-not (Test-Path -Path $apkPath -PathType Leaf)) {
        Write-Host "File not found. Please input again." -ForegroundColor Yellow
        $apkPath = Read-TextWithDefault -Prompt "APK file path"
    }
    $versionCode = Read-IntValue -Prompt "versionCode"
    $versionName = Read-TextWithDefault -Prompt "versionName"
    $minSupported = Read-IntValue -Prompt "minSupportedVersionCode (0 means use versionCode)" -DefaultValue 0 -AllowZero
    $forceUpdate = Read-YesNo -Prompt "forceUpdate?" -DefaultValue $false
    $releaseNotes = Read-OptionalText -Prompt "releaseNotes (optional)"
    $publishNow = $action -eq 4

    Confirm-OrExit -SummaryLines @(
        ("Action=" + ($(if ($action -eq 4) { "Commit and Publish" } else { "Commit upload" }))),
        "ApiBaseUrl=$ApiBaseUrl",
        "ApkPath=$apkPath",
        "VersionCode=$versionCode",
        "VersionName=$versionName",
        "MinSupportedVersionCode=$minSupported",
        "ForceUpdate=$forceUpdate",
        "PublishNow=$publishNow",
        "AuthMode=$AuthMode",
        "DryRun=$($DryRun.IsPresent)"
    )

    $args = @(
        "-File", $commitScript,
        "-ApkPath", $apkPath,
        "-VersionCode", "$versionCode",
        "-VersionName", $versionName,
        "-ApiBaseUrl", $ApiBaseUrl,
        "-AdminToken", $AdminToken
    )
    if ($minSupported -gt 0) {
        $args += @("-MinSupportedVersionCode", "$minSupported")
    }
    if ($forceUpdate) {
        $args += "-ForceUpdate"
    }
    if (-not [string]::IsNullOrWhiteSpace($releaseNotes)) {
        $args += @("-ReleaseNotes", $releaseNotes)
    }
    if ($publishNow) {
        $args += "-PublishNow"
    }
    if ($useBearerToken) {
        $args += "-UseBearerToken"
    }
    if ($DryRun.IsPresent) {
        $args += "-DryRun"
    }

    & powershell -NoProfile -ExecutionPolicy Bypass @args
    exit $LASTEXITCODE
}

if ($action -eq 2) {
    $versionCode = Read-IntValue -Prompt "versionCode to publish"
    Confirm-OrExit -SummaryLines @(
        "Action=Publish",
        "ApiBaseUrl=$ApiBaseUrl",
        "VersionCode=$versionCode",
        "AuthMode=$AuthMode",
        "DryRun=$($DryRun.IsPresent)"
    )

    $args = @(
        "-File", $publishScript,
        "-VersionCode", "$versionCode",
        "-ApiBaseUrl", $ApiBaseUrl,
        "-AdminToken", $AdminToken
    )
    if ($useBearerToken) {
        $args += "-UseBearerToken"
    }
    if ($DryRun.IsPresent) {
        $args += "-DryRun"
    }

    & powershell -NoProfile -ExecutionPolicy Bypass @args
    exit $LASTEXITCODE
}

$targetVersionCode = Read-IntValue -Prompt "targetVersionCode for rollback (0 means auto previous)" -DefaultValue 0 -AllowZero
Confirm-OrExit -SummaryLines @(
    "Action=Rollback",
    "ApiBaseUrl=$ApiBaseUrl",
    "TargetVersionCode=$targetVersionCode",
    "AuthMode=$AuthMode",
    "DryRun=$($DryRun.IsPresent)"
)

$rollbackArgs = @(
    "-File", $rollbackScript,
    "-ApiBaseUrl", $ApiBaseUrl,
    "-AdminToken", $AdminToken
)
if ($targetVersionCode -gt 0) {
    $rollbackArgs += @("-TargetVersionCode", "$targetVersionCode")
}
if ($useBearerToken) {
    $rollbackArgs += "-UseBearerToken"
}
if ($DryRun.IsPresent) {
    $rollbackArgs += "-DryRun"
}

& powershell -NoProfile -ExecutionPolicy Bypass @rollbackArgs
exit $LASTEXITCODE
