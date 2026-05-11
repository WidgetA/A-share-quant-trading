# === SCRIPT PURPOSE ===
# 一键: 本地 kimi login -> 上传 OAuth credentials 到线上 trading-service
#
# 用法 (PowerShell):
#   .\kimi_login_and_upload.ps1
#   .\kimi_login_and_upload.ps1 -ServiceUrl http://8.133.23.9:8000
#   .\kimi_login_and_upload.ps1 -ApiKey "your-trading-api-key" -ForceLogin
#
# 配置: 默认从环境变量读
#   $env:KIMI_UPLOAD_SERVICE_URL  默认 http://8.133.23.9:8000
#   $env:KIMI_UPLOAD_API_KEY      trading 端的 X-API-Key (若未配置, 服务端会让通过)
#
# 流程:
#   1. 检查 kimi-cli 是否安装 + 版本
#   2. 检查 credentials 文件是否已存在且未过期: 若已存在且 refresh_token
#      看起来还能用, 默认 skip kimi login (用 -ForceLogin 强制重新登录)
#   3. 调 kimi login (会拉浏览器完成 OAuth)
#   4. 把 ~/.kimi/credentials/kimi-code.json POST 到 /api/audit/kimi-credentials/upload
#   5. 打印服务端返回的"access_token 剩余秒数"

[CmdletBinding()]
param(
    [string]$ServiceUrl = $(if ($env:KIMI_UPLOAD_SERVICE_URL) { $env:KIMI_UPLOAD_SERVICE_URL } else { 'http://8.133.23.9:8000' }),
    [string]$ApiKey = $env:KIMI_UPLOAD_API_KEY,
    [switch]$ForceLogin,
    [switch]$SkipLogin
)

$ErrorActionPreference = 'Stop'

function Write-Step($msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}

function Write-OK($msg) {
    Write-Host "[OK] $msg" -ForegroundColor Green
}

function Write-Warn2($msg) {
    Write-Host "[WARN] $msg" -ForegroundColor Yellow
}

function Write-ErrLine($msg) {
    Write-Host "[ERROR] $msg" -ForegroundColor Red
}

# ---------------------------------------------------------------------------
# 1. kimi-cli 安装 + 版本检查
# ---------------------------------------------------------------------------
Write-Step "检查 kimi-cli"
$kimiPath = Get-Command kimi -ErrorAction SilentlyContinue
if (-not $kimiPath) {
    Write-ErrLine "未找到 kimi 命令。请先安装 kimi-cli: pip install -U kimi-cli"
    exit 1
}
$kimiVer = (& kimi --version 2>&1 | Out-String).Trim()
Write-OK "kimi 已安装: $kimiVer"

# ---------------------------------------------------------------------------
# 2. 决定是否需要 kimi login
# ---------------------------------------------------------------------------
$credPath = Join-Path $env:USERPROFILE ".kimi\credentials\kimi-code.json"
$needLogin = $true

if (-not $ForceLogin) {
    if (Test-Path $credPath) {
        try {
            $cred = Get-Content $credPath -Raw -Encoding UTF8 | ConvertFrom-Json
            $expiresAt = [double]$cred.expires_at
            $nowEpoch  = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
            $accessRemain = [int]($expiresAt - $nowEpoch)

            # access_token 过期可以靠 refresh_token 续。refresh_token 本身
            # 大约 7-30 天有效, 服务端续不到时会 401, 这时再 -ForceLogin。
            # 若 SkipLogin, 不管 access 状态, 直接上传现有 credentials。
            if ($SkipLogin) {
                Write-OK "已找到 credentials, 跳过 login (-SkipLogin)"
                $needLogin = $false
            } elseif ($accessRemain -gt 60) {
                Write-OK "credentials 仍有 ~$accessRemain s 有效, 跳过 login"
                $needLogin = $false
            } else {
                Write-Warn2 "credentials access_token 已过期或即将过期 (剩 $accessRemain s),  尝试 kimi login 续期"
            }
        } catch {
            Write-Warn2 "现有 credentials 解析失败: $_, 重新 kimi login"
        }
    } else {
        Write-Warn2 "未找到 credentials, 需要登录"
    }
}

if ($needLogin) {
    Write-Step "运行 kimi login (会弹浏览器, 完成后回到这里)"
    & kimi login
    if ($LASTEXITCODE -ne 0) {
        Write-ErrLine "kimi login 失败 (exit $LASTEXITCODE)"
        exit 1
    }
    if (-not (Test-Path $credPath)) {
        Write-ErrLine "kimi login 跑完了但 $credPath 不存在, 异常"
        exit 1
    }
    Write-OK "kimi login 完成"
}

# ---------------------------------------------------------------------------
# 3. 上传 credentials 到线上
# ---------------------------------------------------------------------------
Write-Step "上传 credentials 到 $ServiceUrl"

$uploadUrl = "$ServiceUrl/api/audit/kimi-credentials/upload"

# Manually construct multipart/form-data so this works on Windows PowerShell 5.1
# (its Invoke-RestMethod has no -Form switch; that's PS 7+ only).
$fileBytes = [System.IO.File]::ReadAllBytes($credPath)
$fileEncoded = [System.Text.Encoding]::GetEncoding('iso-8859-1').GetString($fileBytes)
$boundary = [System.Guid]::NewGuid().ToString()
$LF = "`r`n"
$body = (
    "--$boundary$LF" +
    "Content-Disposition: form-data; name=`"file`"; filename=`"kimi-code.json`"$LF" +
    "Content-Type: application/json$LF$LF" +
    $fileEncoded + $LF +
    "--$boundary--$LF"
)

$headers = @{}
if ($ApiKey) {
    $headers['X-API-Key'] = $ApiKey
}

try {
    $resp = Invoke-RestMethod `
        -Uri $uploadUrl `
        -Method Post `
        -Headers $headers `
        -ContentType "multipart/form-data; boundary=$boundary" `
        -Body $body `
        -ErrorAction Stop
} catch {
    $statusCode = $null
    if ($_.Exception.Response) {
        $statusCode = [int]$_.Exception.Response.StatusCode
    }
    Write-ErrLine "上传失败: $($_.Exception.Message) (HTTP $statusCode)"
    if ($statusCode -eq 401) {
        Write-Warn2 '服务端要求 X-API-Key. 在脚本参数 -ApiKey 或环境变量 $env:KIMI_UPLOAD_API_KEY 里填上'
    }
    exit 1
}

if ($resp.success) {
    Write-OK $resp.message
    Write-Host ("       目标路径: " + $resp.destination)
    if ($resp.access_token_seconds_remaining) {
        $secs = [int]$resp.access_token_seconds_remaining
        $mins = [math]::Round($secs / 60, 1)
        $line = "       access_token 剩余: " + $secs + " s (约 " + $mins + " 分钟). "
        $line += "refresh_token 自动续期, 通常 7-30 天后才需要重新跑这个脚本"
        Write-Host $line
    }
} else {
    Write-ErrLine "服务端返回 success=false"
    $resp | ConvertTo-Json -Depth 4
    exit 1
}

Write-Host ""
Write-OK "完成"
