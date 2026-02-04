# 错误修正记录

> 目的：记录错误修正的完整信息，避免同类问题重复发生。

## 记录字段
- 日期
- 现象与影响范围
- 触发条件
- 根因定位
- 修复措施
- 验证方式
- 预防方案（测试/监控/校验/回滚）
- 相关文件/接口
- 复盘要点

## 2026-02-04 初始化
- 本文件创建，等待首条错误修正记录。

## 2026-02-04 知识分类 actionId 回写丢失
- 日期：2026-02-04
- 现象与影响范围：知识分类结果无法回写到 action_units，导致语义单元缺少 knowledge_type/reasoning。
- 触发条件：action_units 的 id 未赋值或重复，分类返回 action_id=0/冲突。
- 根因定位：Java 侧未统一分配 ActionSegmentResult.id，Python 分类按 action_id 关联失败。
- 修复措施：Java 编排为 action_units 统一补齐/去重 id，并确保分类输入与 CV 结果共用同一 id。
- 验证方式：跑一条本地视频主链路，检查 semantic_units 中 action_units 的 id 非零且分类字段已回写。
- 预防方案（测试/监控/校验/回滚）：增加 action_id 非零与唯一性校验；缓存签名纳入 action_id；必要时记录告警并回退到仅保留 unit 级分类。
- 相关文件/接口：MVP_Module2_HEANCING/enterprise_services/java_orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java、python_grpc_server.py
- 复盘要点：action_id 是跨阶段关联键，必须在编排层统一治理。

## 2026-02-04 Phase2A 初始素材请求被忽略
- 日期：2026-02-04
- 现象与影响范围：Phase2A 召回的截图/切片在后续 FFmpeg 阶段被丢弃。
- 触发条件：GenerateMaterialRequests 返回结果覆盖 Phase2A 请求。
- 根因定位：Java 编排仅使用生成请求，未合并 Phase2A 初始请求。
- 修复措施：在 FFmpeg 提取前合并 Phase2A 与生成请求并去重。
- 验证方式：跑含 Phase2A 初始请求的视频，检查输出素材数量与请求一致。
- 预防方案（测试/监控/校验/回滚）：增加素材请求合并的回归用例；日志记录合并前后数量；必要时增加开关快速回退。
- 相关文件/接口：MVP_Module2_HEANCING/enterprise_services/java_orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java
- 复盘要点：上游召回与下游策略要统一合并，避免覆盖。

## 2026-02-04 screenshot_selector 缩进错误
- 日期：2026-02-04
- 现象与影响范围：python_grpc_server 启动时报 IndentationError，模块导入失败。
- 触发条件：加载 screenshot_selector.py 时遇到异常缩进的三引号文本。
- 根因定位：遗留的占位文档块缩进不合法，破坏了函数缩进结构。
- 修复措施：移除异常三引号块，改为规范的中文注释与方法实现。
- 验证方式：重新导入 python_grpc_server.py 通过；运行服务启动不再报错。
- 预防方案（测试/监控/校验/回滚）：引入 lint/格式化检查；合并前运行 py_compile 级别语法检查。
- 相关文件/接口：MVP_Module2_HEANCING/module2_content_enhancement/screenshot_selector.py
- 复盘要点：文档占位不应影响语法结构，注释必须使用合法格式。

## 2026-02-04 Java 编译缺少 Comparator/LinkedHashMap 导入
- 日期：2026-02-04
- 现象与影响范围：maven-compiler-plugin 编译失败，KnowledgeClassificationOrchestrator 报找不到符号。
- 触发条件：在类中使用 Comparator/LinkedHashMap 但未导入。
- 根因定位：新增排序与签名构造逻辑后遗漏 java.util 导入。
- 修复措施：补充 java.util.Comparator 与 java.util.LinkedHashMap 导入。
- 验证方式：重新执行 mvn 编译通过。
- 预防方案（测试/监控/校验/回滚）：合并前运行编译检查；IDE 启用缺失导入提示。
- 相关文件/接口：MVP_Module2_HEANCING/enterprise_services/java_orchestrator/src/main/java/com/mvp/module2/fusion/service/KnowledgeClassificationOrchestrator.java
- 复盘要点：签名与排序改动要同步检查 import。

## 2026-02-04 material_requests 未写回 semantic_units_phase2a.json
- 日期：2026-02-04
- 现象与影响范围：semantic_units_phase2a.json 中所有 material_requests 为空，导致后续对齐困难。
- 触发条件：文件结构为 dict 或更新逻辑未覆盖到 semantic_units 列表。
- 根因定位：写回逻辑仅按列表遍历，未兼容 {semantic_units: [...]} 结构，导致更新未生效。
- 修复措施：写回时兼容列表/字典结构，统一更新 semantic_units 列表。
- 验证方式：重新生成素材请求后检查 material_requests 中截图/切片条目。
- 预防方案（测试/监控/校验/回滚）：为语义单元文件增加结构校验；写回后记录条目数量。
- 相关文件/接口：python_grpc_server.py
- 复盘要点：跨阶段文件格式必须显式兼容或统一规范。
