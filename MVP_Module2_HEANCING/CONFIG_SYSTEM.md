# Configuration System Summary

## 已创建的配置文件

### 1. `config/fault_detection_config.yaml`
完整的断层检测配置文件,包含:

**第1类断层关键词** (6个分类, 80+关键词):
- 缺失定义: 14个关键词
- 缺失因果: 19个关键词
- 缺失推导: 12个关键词
- 指代模糊: 30个关键词 (代词/模糊限定词/省略主语)
- 缺失逻辑关联: 19个关键词
- 缺失量化信息: 22个关键词

**第2类断层关键词** (5个分类, 120+关键词):
- 空间结构: 48个关键词 (架构/位置关系/UI元素)
- 动态过程: 56个关键词 (过程描述/动作词/算法执行)
- 复杂关系: 42个关键词 (对比/关系类型/数据可视化)
- 数学公式: 27个关键词
- 代码示例: 17个关键词

**检测参数**:
- context_window_before/after: 5
- llm_temperature: 0.3
- min_confidence: 0.5
- max_concurrent_segments: 10

**领域特定配置**:
- 算法: 动态过程权重+0.2
- AI框架: 空间结构权重+0.2
- 数学: 数学公式权重+0.3

### 2. `module2_content_enhancement/config_loader.py`
配置加载器,功能:
- YAML文件加载
- 默认配置 + 自定义配置深度合并
- 提取关键词和参数的便捷方法
- 领域特定配置获取
- 全局单例模式

## 使用方式

### 基本用法
```python
from fault_detector import FaultDetector
from llm_client import create_llm_client

# 使用默认配置
llm = create_llm_client()
detector = FaultDetector(llm, domain="算法")

# 使用自定义配置
detector = FaultDetector(
    llm,
    domain="算法",
    config_path="path/to/custom_config.yaml"
)
```

### 修改配置
直接编辑 `config/fault_detection_config.yaml`:
```yaml
# 添加新的关键词
class1_indicators:
  缺失定义:
    - "什么是"
    - "新关键词"  # 直接添加

# 调整参数
detection_params:
  context_window_before: 10  # 修改上下文窗口
  min_confidence: 0.7  # 提高置信度阈值
```

### 自定义配置文件
创建自己的配置文件,只需要包含要覆盖的部分:
```yaml
# custom_config.yaml
class1_indicators:
  缺失定义:
    - "额外关键词1"
    - "额外关键词2"

detection_params:
  llm_temperature: 0.5
```

加载时会自动深度合并:
```python
detector = FaultDetector(
    llm,
    config_path="custom_config.yaml"
)
```

## 优势

1. **易于维护**: 关键词在YAML文件中,不需要修改Python代码
2. **灵活扩展**: 添加新关键词或参数无需重启
3. **领域适配**: 支持不同领域的特殊配置
4. **可追踪**: 配置文件可以版本控制
5. **用户友好**: YAML格式人类可读,支持注释

## 配置文件位置

```
MVP_Module2_HEANCING/
├── config/
│   └── fault_detection_config.yaml  # 默认配置
└── module2_content_enhancement/
    └── config_loader.py  # 加载器
```

用户可以将自定义配置放在任何位置,通过`config_path`参数指定。
