# Android 更新发布 SOP

本文档用于说明如何使用仓库内现有脚本完成 Android APK 的上传、发布、校验与回滚。

## 1. 脚本清单

- `scripts/release/commit_android_update.ps1`：上传 APK，并可选择是否立即发布。
- `scripts/release/publish_android_update.ps1`：将指定 `versionCode` 发布为最新版本。
- `scripts/release/rollback_android_update.ps1`：回滚到指定版本或自动回滚到上一个版本。
- `scripts/release/android_update_wizard.ps1`：交互式向导，适合手工操作。
- `scripts/release/release_and_verify_android_update.ps1`：一键上传 + 发布 + 校验（推荐）。

## 2. 前置条件

1. 启动 `java-orchestrator` 服务，并确保外部可访问。
2. 已准备好待发布 APK 文件。
3. 拿到管理口令（`X-Update-Admin-Token` 或 Bearer Token）。
4. 在仓库根目录执行命令（`D:\videoToMarkdownTest2`）。

可选环境变量：

- `MOBILE_API_BASE_URL`：例如 `http://localhost:8080`
- `MOBILE_UPDATE_ADMIN_TOKEN`：更新管理口令

## 3. 推荐流程（一条命令）

以下命令会自动完成：上传 -> 发布 -> 双向校验。

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts/release/release_and_verify_android_update.ps1 `
  -ApkPath D:\path\to\app-release.apk `
  -VersionCode 101 `
  -VersionName 1.0.1 `
  -MinSupportedVersionCode 100 `
  -ForceUpdate `
  -ReleaseNotes "mandatory update" `
  -ApiBaseUrl "http://localhost:8080" `
  -AdminToken "your-admin-token"
```

成功后会输出两段校验结果：

1. 旧版本客户端检查：应看到 `hasUpdate=true`。
2. 当前版本客户端检查：应看到 `hasUpdate=false`。

## 4. 分步流程（可手动拆分）

### 4.1 仅上传（不立即发布）

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts/release/commit_android_update.ps1 `
  -ApkPath D:\path\to\app-release.apk `
  -VersionCode 101 `
  -VersionName 1.0.1 `
  -MinSupportedVersionCode 100 `
  -ReleaseNotes "hotfix" `
  -ApiBaseUrl "http://localhost:8080" `
  -AdminToken "your-admin-token"
```

### 4.2 发布指定版本

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts/release/publish_android_update.ps1 `
  -VersionCode 101 `
  -ApiBaseUrl "http://localhost:8080" `
  -AdminToken "your-admin-token"
```

### 4.3 回滚

```powershell
# 回滚到指定版本
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts/release/rollback_android_update.ps1 `
  -TargetVersionCode 100 `
  -ApiBaseUrl "http://localhost:8080" `
  -AdminToken "your-admin-token"
```

```powershell
# 自动回滚到上一个发布版本
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts/release/rollback_android_update.ps1 `
  -ApiBaseUrl "http://localhost:8080" `
  -AdminToken "your-admin-token"
```

## 5. 发布后手工校验

```powershell
# 旧版本视角，应返回 hasUpdate=true
Invoke-RestMethod -Method Get `
  -Uri "http://localhost:8080/api/mobile/app/update/check?versionCode=100&versionName=1.0"
```

```powershell
# 新版本视角，应返回 hasUpdate=false
Invoke-RestMethod -Method Get `
  -Uri "http://localhost:8080/api/mobile/app/update/check?versionCode=101&versionName=1.0.1"
```

## 6. 强制更新字段说明

- `forceUpdate=true`：客户端应进入强制升级路径。
- `minSupportedVersionCode`：低于该版本的客户端应被判定为必须升级。

建议策略：

1. 灰度阶段使用 `forceUpdate=false`，先观察下载与安装成功率。
2. 全量阶段再切 `forceUpdate=true`，并同步提升 `minSupportedVersionCode`。

## 7. 常见问题

1. 上传成功但客户端无更新提示：
   - 检查是否执行了发布（`publishNow` 或单独调用 `publish`）。
   - 检查 `latest.json` 是否已切到目标 `versionCode`。
2. 客户端下载失败：
   - 检查 `downloadUrl` 是否可访问。
   - 检查服务端 `apkFile` 路径是否存在对应文件。
3. 返回 401：
   - 校验 `AdminToken` 与服务端配置一致。
   - 或改用 `-UseBearerToken` 与网关认证策略对齐。

## 8. 安全建议

1. 不要将真实管理口令写入脚本文件。
2. 优先使用环境变量注入 `MOBILE_UPDATE_ADMIN_TOKEN`。
3. 线上环境请替换默认弱口令，并纳入密钥管理系统。
