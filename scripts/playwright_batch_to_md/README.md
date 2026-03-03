# playwright_batch_to_md 使用说明

## 1. 目标

`scripts/playwright_batch_to_md.py` 用于批量抓取网页正文并导出为 Markdown，重点支持知乎（登录态复用、签名请求、CDP 模式、图片下载并回填 Markdown）。

## 2. 目录结构

```text
scripts/
  playwright_batch_to_md.py            # 主脚本
  playwright_batch_to_md/
    zhihu_sign.js                      # 知乎签名算法脚本
    stealth.min.js                     # 反检测脚本
    requirements.txt                   # 本脚本最小依赖清单
    README.md                          # 本文档
```

## 3. 安装依赖

建议先进入仓库根目录再执行。

```powershell
pip install -r scripts/playwright_batch_to_md/requirements.txt
python -m playwright install chromium
```

注意事项：

1. `PyExecJS` 需要本机可用的 JavaScript 运行时（通常是 Node.js）。
2. 若你只抓非知乎页面，`PyExecJS` 不是强依赖，但安装后最省心。

## 4. 快速开始

单链接抓取（默认下载图片并写回 Markdown）：

```powershell
python scripts/playwright_batch_to_md.py "https://www.zhihu.com/question/1990889997868500131/answer/2011732273976844742"
```

批量抓取（URL 文件每行一个地址）：

```powershell
python scripts/playwright_batch_to_md.py --urls-file var/zhihu_urls.txt
```

## 5. 输出与落盘参数

脚本会在输出目录下创建批次子目录，默认形如 `extract_YYYYMMDD_HHMMSS`。

常用参数：

1. `--out-dir`：输出根目录（默认 `var/playwright_md`）
2. `--output-name`：Markdown 文件名（默认 `article.md`）
3. `--save-images`：下载图片到本地（默认开启）
4. `--no-save-images`：关闭图片下载
5. `--max-images`：最多下载图片数（`0` 表示不限制）
6. `--save-html`：额外保存 `article.html`

示例：自定义目录、文件名、最多下载 10 张图。

```powershell
python scripts/playwright_batch_to_md.py ^
  "https://www.zhihu.com/question/1990889997868500131/answer/2011732273976844742" ^
  --out-dir var/custom_md ^
  --output-name answer.md ^
  --max-images 10
```

## 6. 知乎相关参数（默认已对齐 MindSpider 风格）

1. 默认有头模式：`--headed`（可用 `--headless` 覆盖）
2. 默认启用登录兜底：`--auto-login-fallback`（可用 `--no-auto-login-fallback` 关闭）
3. 默认启用 CDP：`--zhihu-cdp-mode`（可用 `--disable-zhihu-cdp-mode` 关闭）
4. 默认请求间隔：`--zhihu-request-interval-sec 2.0`

常见参数：

1. `--zhihu-cdp-browser-path`：指定 Chrome/Edge 可执行文件
2. `--zhihu-cdp-user-data-dir`：指定 CDP 专用数据目录
3. `--zhihu-user-data-dir`：标准 persistent context 数据目录
4. `--storage-state` / `--save-storage-state`：导入/导出登录态

## 7. 常见问题

1. 报错 `Google Chrome 无法对其数据目录执行读写操作`
原因：浏览器数据目录被占用或损坏。  
处理：关闭所有 Chrome 实例，或换一个目录，例如：

```powershell
python scripts/playwright_batch_to_md.py "URL" --zhihu-cdp-user-data-dir var/zhihu_user_data_cdp_2
```

2. 报 `status=403`
原因：风控、登录态失效、IP 风险、浏览器环境异常。  
处理：优先保持有头模式、确保登录态有效、避免过高并发、保留默认 2 秒节流。

3. `Get-Content` 查看中文乱码
通常是终端编码显示问题，不代表文件内容损坏。建议用 UTF-8 编辑器查看。

