# var 目录约定

`var/` 用于承载运行态数据，不应提交到 Git。

- `var/storage/`：任务输出、中间产物、素材。
- `var/cache/`：缓存与临时运行文件。
- `var/artifacts/`：诊断与性能报告。
- `var/models/`：本地模型与大文件。

注意：
- 该目录下内容默认由 `.gitignore` 过滤。
- 如需提交样例数据，请放在 `docs/` 下的小样本目录并显式说明来源。

