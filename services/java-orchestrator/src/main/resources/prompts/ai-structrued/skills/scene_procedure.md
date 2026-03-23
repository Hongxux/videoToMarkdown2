# Skill: 流程步骤 / 操作指南场景（scene_procedure）

当 section 涉及操作指南、安装部署、具体执行步骤序列时，适用本 skill。

---

## 精修规则

### 1. 有序列表表达步骤序列
- 使用标准的有序列表（`1. 2. 3.`）表达严格的步骤时序
- 步骤编号必须连续，不得跳跃
- 如果某些步骤可以并行执行，用标签标注：`（可与步骤 N 并行执行）`

### 2. 代码/参数/异常嵌入步骤子节点
- **必须**将对应的代码块、参数解释、异常警告，作为对应步骤的**子节点（4空格缩进）**嵌入
- **绝对禁止**将代码块脱离步骤单独拎出来罗列
- 参数格式建议：`- **参数 `--flag`**：{说明}`

### 3. 前置条件与后置验证
- 如果步骤流程有**前置条件**（如：确保已安装 JDK 17），在步骤列表前用 Callout 标注
- 如果有**后置验证方式**（如：运行 `java -version` 确认），在最后一步后追加验证步骤

### 正向示例
```markdown
> [!warning] 前置条件
> 请确保已安装 JDK 17+ 和 Maven 3.8+。

1. **克隆仓库**：
    ```bash
    git clone https://github.com/example/project.git
    ```
2. **配置环境变量**：
    - **参数 `JAVA_HOME`**：指向 JDK 安装目录。
    - **参数 `MAVEN_HOME`**：指向 Maven 安装目录。
3. **编译并运行**：
    ```bash
    mvn clean package -DskipTests
    java -jar target/app.jar
    ```
    - **异常排查**：若出现 `OutOfMemoryError`，增加 JVM 堆内存参数 `-Xmx2g`。
4. **后置验证**：
    - 访问 `http://localhost:8080/health`，确认返回 `{"status":"UP"}`。
```
