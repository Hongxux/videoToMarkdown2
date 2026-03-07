# Android App Update 发布说明

本文档用于说明如何把 Android App Demo 发布为可更新版本，以及如何让体验者最快安装并升级。

## 推荐交付方式

- 对普通体验者：把 APK 放到 GitHub Releases，README 中给下载链接。
- 对开发者：提供 `gradlew` 构建命令，并允许通过 `mobileApiBaseUrl` 注入后端地址。
- 对你自己的演示环境：在后端启用 Android 更新接口，再用仓库里的 PowerShell 脚本上传和发布新版本。

## 前置条件

1. `java-orchestrator` 已启动并可从外部访问。
2. 你已经构建好了待发布的 APK。
3. 你已经准备好了更新管理口令。

可选环境变量：

- `MOBILE_API_BASE_URL`：例如 `http://localhost:8080`
- `MOBILE_APP_API_BASE_URL`：例如 `http://localhost:8080/api/mobile`
- `MOBILE_UPDATE_ADMIN_TOKEN`：更新管理口令

## 一条命令上传 + 发布 + 校验

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

## 本地开发安装示例

### Android 模拟器

```powershell
.\gradlew.bat :app:installDebug -PmobileApiBaseUrl=http://10.0.2.2:8080/api/mobile
```

### Android 真机

```powershell
.\gradlew.bat :app:installDebug -PmobileApiBaseUrl=http://<你的局域网IP>:8080/api/mobile
```

## 推荐的 GitHub 发布组合

### 方案 A：最省心

- 代码推到 GitHub。
- APK 传到 GitHub Releases。
- README 顶部给出下载和安装说明。

### 方案 B：可热更新演示

- 后端部署到稳定域名。
- 用本仓库的更新脚本发布 APK。
- App 连接稳定的 `/api/mobile` 地址。

## 常见问题

### 看不到更新提示

- 先确认已经执行了发布，而不只是上传。
- 再确认客户端连接的是正确后端，不是历史临时地址。

### APK 能安装，但连不上后端

- 确认 `mobileApiBaseUrl` 是否传了完整 `/api/mobile`，或者至少传了主机根地址以便构建时自动补齐。
- 模拟器请使用 `10.0.2.2`，真机请使用你电脑的局域网 IP。

### 是否应该把 APK 直接提交到仓库

- 不建议。
- 推荐使用 GitHub Releases 存放 APK，源码仓库只保留构建脚本和说明文档。
