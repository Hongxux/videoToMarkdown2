# Obsidian Phase2B Structured Rewrite 插件

## 目标

这个插件用于在 Obsidian 中选中一段文本后，按 `Ctrl+Shift+S`（macOS 为 `Cmd+Shift+S`）调用现有后端接口：

- `POST /api/mobile/cards/phase2b/structured-markdown`

插件会把返回的结构化 Markdown 直接替换当前选区，并对后续行做上下文感知缩进，尽量保证在以下场景中正常渲染：

- 普通段落
- 引用块 `>`
- Callout `> [!note]`
- 列表项 `- / * / 1.`
- 任务列表 `- [ ]`

## 文件结构

- `manifest.json`：Obsidian 插件元数据
- `main.ts`：插件主逻辑
- `package.json`：构建脚本与依赖
- `tsconfig.json`：TypeScript 配置
- `esbuild.config.mjs`：打包配置
- `versions.json`：Obsidian 版本映射
- `sync-to-vault.ps1`：一键同步到指定 Vault 插件目录

## 已实现能力

- 注册编辑器命令：`结构化改写选中文本`
- 默认快捷键：`Mod+Shift+S`
- 调用后端 `/phase2b/structured-markdown`
- 在接口等待期间，即使选区高亮消失或光标移动，仍会按启动时缓存的原选区进行替换
- 当原范围失效时，会按“原始偏移 -> 上下文锚点 -> 唯一原文匹配”的顺序重新定位后再替换
- 提供稳定的多状态反馈：
  - `空闲`
  - `正在执行改写`
  - `已接收结构化返回文本`
  - `完成改写`
  - `改写失败`
- 支持插件设置中配置：
  - 后端基础地址
  - 接口路径
  - 自动剥离外层 Markdown 代码块包裹
- 自动适配多行结果的续行缩进
- 支持一键构建并同步到固定 Vault 插件目录

## 工作方式

插件遵循“后端负责结构化，前端负责编辑器上下文适配”的分层原则：

1. 在发起请求前冻结一份选区快照：原文本、起止位置、上下文缩进信息。
2. 状态栏切到 `正在执行改写`，同时发起后端请求：

```json
{
  "bodyText": "选中的原文",
  "sourceText": "选中的原文"
}
```

3. 接收到结构化结果后，状态栏切到 `已接收结构化返回文本`。
4. 按缓存下来的 Markdown 容器上下文，给后续行补齐正确前缀。
5. 优先按原始范围替换；如果原始范围失效，则按“原始偏移 -> 上下文锚点 -> 唯一原文匹配”逐级回退定位后再替换。
6. 替换完成后，状态栏切到 `完成改写`，稍后自动回到 `空闲`。

## 构建

先进入插件目录：

```bash
cd Obsidian_plug
```

安装依赖：

```bash
npm install
```

开发模式：

```bash
npm run dev
```

生产构建：

```bash
npm run build
```

构建后会生成：

- `main.js`

## 一键同步到 Vault

这个仓库已经内置一键同步脚本，默认会：

1. 先执行 `npm run build`
2. 再把运行时文件同步到：
   - `D:\云库\OneDrive\文档\Obsidian Vault\.obsidian\plugins\Obsidian_plug`
3. 默认只覆盖运行时文件：
   - `manifest.json`
   - `main.js`
   - `styles.css`（如果存在）
4. 不会删除 `data.json`、`node_modules` 等其他本地文件

直接执行：

```bash
npm run sync:vault
```

如果你刚刚已经构建过，想跳过构建阶段：

```bash
npm run sync:vault:skip-build
```

如果以后要同步到别的 Vault，也可以直接运行脚本并覆盖目标目录：

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\sync-to-vault.ps1 -VaultPluginDir "D:\你的路径\.obsidian\plugins\Obsidian_plug"
```

## 安装到 Obsidian

将以下文件复制到你的 vault 中：

- `manifest.json`
- `main.js`

目标目录通常为：

```text
<你的 Vault>/.obsidian/plugins/phase2b-structured-rewrite/
```

然后在 Obsidian 中：

1. 打开“设置” -> “第三方插件”。
2. 打开社区插件支持。
3. 启用 `Phase2B Structured Rewrite`。
4. 进入插件设置，填写后端地址，例如：`http://127.0.0.1:8080`。
5. 启用后可在 Obsidian 底部状态栏看到当前改写状态。

## 使用方式

1. 在笔记中选中一段文本。
2. 按 `Ctrl+Shift+S`。
3. 即使等待期间选区取消，插件仍会记住原文本与原位置。
4. 你可以在状态栏依次看到：`正在执行改写` -> `已接收结构化返回文本` -> `完成改写`。
5. 接口返回后，插件会优先替换原范围；若范围失效，会优先按原始偏移和前后文锚点重定位，最后才回退到唯一原文匹配。

## 推荐使用习惯

为了获得最稳定的渲染结果，建议：

- 优先选中“正文内容”，不要把外层列表符号或引用符号一起选进去。
- 优先按块级内容选择，不要只选中一行中间的一小段碎片文本。
- 如果后端经常返回标题、表格、代码块，优先在普通段落或独立块中使用。
- 在等待接口返回期间，尽量不要主动改写原段落内容；这样可以最大化命中原范围直替换。

## 当前限制

- 当前是“状态型反馈”，不是流式字级回显；也就是说你能看到阶段状态，但还不会逐字展示模型生成过程。
- 如果你在等待期间手动修改了原文，且缓存文本在文档中出现了多个重复副本，插件会停止替换并提示你重新选择，以避免误替换。
- 如果后端返回非常复杂的嵌套结构，例如多层表格加代码块，最终渲染仍取决于当前选区所在的 Markdown 容器是否合法。
- 当前版本使用一次性请求，没有接入 `progressChannel` 流式进度展示。

## 后续建议

如果你下一步要继续增强，这个目录可以继续往下扩：

- 增加“测试连接”按钮
- 如后端补齐 `filterRequirement` 生效链路，再把该字段暴露为可配置项
- 接入 `progressChannel/requestId` 做流式预览
- 增加“仅替换当前段落”与“替换完整块”两种模式
- 为列表、引用、Callout 分别补更细的缩进策略测试
- 如果你确实希望支持“等待期间文档继续被大量编辑”，再升级为 offset/transaction 级别的更强定位策略

## 与现有后端的对齐关系

这个插件没有复制你的结构化逻辑，只复用现有接口能力：

- Java 控制器入口：`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/controller/MobileCardController.java`
- 路由：`/api/mobile/cards/phase2b/structured-markdown`

这样做的收益是：

- Prompt 与结构化策略只维护一份
- Obsidian 侧保持轻量
- 后续你升级后端策略时，插件无需大改