param(
    [switch]$SkipDotEnv,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

# 统一定位仓库根目录，确保从任意位置执行都能正确启动。
$RepoRoot = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $RepoRoot ".env"

function Set-EnvIfMissing {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Value
    )
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($Name, "Process"))) {
        [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    }
}

function Import-DotEnvIfExists {
    param(
        [Parameter(Mandatory = $true)][string]$Path
    )
    if (-not (Test-Path $Path)) {
        Write-Host "[run_wecom_bot] 未检测到 .env，继续使用默认值/当前环境变量。"
        return
    }

    # 只处理 KEY=VALUE 的简单格式，避免把复杂 shell 语法误注入到进程环境。
    Get-Content -Path $Path -Encoding UTF8 | ForEach-Object {
        $line = $_.Trim()
        if ([string]::IsNullOrWhiteSpace($line)) { return }
        if ($line.StartsWith("#")) { return }
        $idx = $line.IndexOf("=")
        if ($idx -le 0) { return }
        $key = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1).Trim()
        if ($value.Length -ge 2 -and (
            ($value.StartsWith("'") -and $value.EndsWith("'")) -or
            ($value.StartsWith('"') -and $value.EndsWith('"'))
        )) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        if (-not [string]::IsNullOrWhiteSpace($key)) {
            [Environment]::SetEnvironmentVariable($key, $value, "Process")
        }
    }
    Write-Host "[run_wecom_bot] 已加载 .env 到当前进程环境。"
}

if (-not $SkipDotEnv) {
    Import-DotEnvIfExists -Path $EnvFile
}

# 统一注入运行必需参数（已存在时不覆盖）。
Set-EnvIfMissing -Name "PYTHONNOUSERSITE" -Value "1"
Set-EnvIfMissing -Name "WECOM_LISTEN_HOST" -Value "0.0.0.0"
Set-EnvIfMissing -Name "WECOM_LISTEN_PORT" -Value "5000"
Set-EnvIfMissing -Name "WECOM_CALLBACK_PATH" -Value "/wechat/callback"
Set-EnvIfMissing -Name "WECOM_CALLBACK_TOKEN" -Value "videoToMarkdown"
Set-EnvIfMissing -Name "WECOM_ENCODING_AES_KEY" -Value "M7Hd77kWpF6vqfyzS1rIGCE7QYIRzK0RHtE62T6B1BS"
Set-EnvIfMissing -Name "ORCHESTRATOR_API_URL" -Value "http://127.0.0.1:8080/api"
Set-EnvIfMissing -Name "WECOM_CORP_ID" -Value "wwf114dd6affa3f385"
Set-EnvIfMissing -Name "WECOM_AGENT_ID" -Value "1000002"
Set-EnvIfMissing -Name "WECOM_CORP_SECRET" -Value "bfoecCz17wooCLH71wirKO7D96KoGUgyU09K7Ctb83Q"
Set-EnvIfMissing -Name "WECOM_MAX_RETRIES" -Value "2"
Set-EnvIfMissing -Name "WECOM_TASK_POLL_INTERVAL_SEC" -Value "10"
Set-EnvIfMissing -Name "WECOM_RETRY_BACKOFF_BASE_SEC" -Value "30"
Set-EnvIfMissing -Name "WECOM_MSG_DEDUPE_TTL_SEC" -Value "600"
Set-EnvIfMissing -Name "WECOM_HTTP_TIMEOUT_SEC" -Value "8"

$mustHave = @("WECOM_CALLBACK_TOKEN", "WECOM_ENCODING_AES_KEY", "WECOM_LISTEN_PORT")
foreach ($k in $mustHave) {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($k, "Process"))) {
        throw "[run_wecom_bot] 缺少必填环境变量: $k"
    }
}

# 回消息到个人聊天依赖以下三个值；缺失时仍可接收消息并触发任务，但无法主动回执。
$sendDeps = @("WECOM_CORP_ID", "WECOM_CORP_SECRET", "WECOM_AGENT_ID")
$missingSendDeps = @()
foreach ($k in $sendDeps) {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($k, "Process"))) {
        $missingSendDeps += $k
    }
}
if ($missingSendDeps.Count -gt 0) {
    Write-Warning "[run_wecom_bot] 以下变量缺失，状态可能无法回传到个人聊天: $($missingSendDeps -join ', ')"
}

Write-Host "[run_wecom_bot] 即将启动 WeCom Bot，配置摘要："
Write-Host "  - listen: $([Environment]::GetEnvironmentVariable('WECOM_LISTEN_HOST','Process')):$([Environment]::GetEnvironmentVariable('WECOM_LISTEN_PORT','Process'))"
Write-Host "  - callback: $([Environment]::GetEnvironmentVariable('WECOM_CALLBACK_PATH','Process'))"
Write-Host "  - orchestrator: $([Environment]::GetEnvironmentVariable('ORCHESTRATOR_API_URL','Process'))"
Write-Host "  - retries: $([Environment]::GetEnvironmentVariable('WECOM_MAX_RETRIES','Process'))"

if ($DryRun) {
    Write-Host "[run_wecom_bot] DryRun 模式，不启动进程。"
    exit 0
}

Push-Location $RepoRoot
try {
    python -X utf8 apps/wecom-bot/main.py
}
finally {
    Pop-Location
}

