# Stage1：视频文字稿处理流程（v7）

原材料：完整的视频和转录的文字稿

---

## 零、时间戳存储策略

### 本地存储结构
```
/local_storage/
├── sentence_timestamps.json    # 步骤4后生成，步骤8a/8b/9-11查询
├── segment_timestamps.json     # 步骤7后生成，步骤19/23查询
├── kp_timestamps.json          # 步骤7c后生成，知识点级时间戳
└── temp_frames/                # 步骤12-15临时存储
```

---

## 一、前期准备

### 步骤1：原材料确认与主题识别
- **类型**：Tool + LLM

#### 核心动作
1. 校验视频和字幕文件有效性
2. 读取字幕样本，推断视频领域和主题

#### 实现流程
```
1. [Tool] 检查 video_path 文件存在且可读
2. [Tool] 检查 subtitle_path 文件存在且可读
3. [Tool] 读取字幕文件前20条作为样本
4. [Tool] 从文件名提取视频标题（可选）
5. [LLM] 基于样本字幕推断 domain 和 main_topic
6. [输出] 返回确认结果
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `video_path` | string | 用户输入 | 视频文件路径/URL |
| `subtitle_path` | string | 用户输入 | 原始细切字幕稿路径 |

#### LLM推断Prompt
```
请根据以下视频字幕样本，推断视频的领域和主题：

【字幕样本】
{sample_subtitles}

【视频标题（如有）】
{video_title}

【输出要求】
1. domain：视频所属领域，如"计算机科学"、"数学"、"物理"、"经济学"等
2. main_topic：核心主题，20字以内，概括视频讲解的核心内容

【输出格式】
{"domain": "string", "main_topic": "string"}
```

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `is_valid` | boolean | 流程控制 | 有效性校验结果 |
| `video_path` | string | →步骤12,23 | 视频路径透传 |
| `domain` | string | →步骤2 | LLM推断的视频领域 |
| `main_topic` | string | →步骤5 | LLM推断的核心主题 |

---

## 二、文字稿预处理

### 步骤2：DeepSeek智能纠错
- **类型**：LLM

#### 核心动作
基于领域上下文，修正ASR语音识别的同音字错误

#### 纠错原则（硬编码）
1. **只纠正明显的同音字错误**：根据上下文语境判断
2. **不确定时保留原文**：避免过度纠正
3. **不纠正语法错误或标点错误**：这些留给后续步骤处理
4. **基于领域推断专业术语**：利用domain信息修正专业词汇

#### 纠错标注方式
- **正文处理**：在正文中直接使用纠正后的词
- **纠错记录**：在输出中统一列出所有纠错，包含判断依据

#### 常见同音误判示例（按领域分类）
| 领域 | 常见误判 | 正确写法 | 判断依据 |
|-----|---------|---------|---------|
| 哲学 | 维新者 | 唯心者 | 上下文出现"唯物主义" |
| 哲学 | 行而上学 | 形而上学 | 哲学术语 |
| 哲学 | 辩正 | 辩证 | 哲学术语 |
| 哲学 | 意园论 | 一元论 | 哲学术语 |
| 计算机 | 栈针 | 栈帧 | 计算机术语 |
| 计算机 | 进程/线程混淆 | 根据上下文判断 | 语义分析 |
| 通用 | 的/得/地 | 根据语法判断 | 助词用法 |
| 通用 | 在/再 | 根据语义判断 | 副词/介词区分 |

#### 实现流程
```
1. [输入] 接收原始字幕列表 + domain
2. [LLM] 逐批处理字幕（每批50条）
   a. 基于domain推断领域术语
   b. 识别并修正同音字错误
3. [输出] 返回纠错后字幕，保留原始时间戳，附带纠错记录
```

#### LLM纠错Prompt
```
你是一个专业的ASR纠错助手，请修正以下字幕中的同音字错误。

【视频领域】{domain}

【字幕列表】
{subtitles}

【纠错范围】
仅纠正同音字错误，如：
- "维新者"→"唯心者"（哲学领域，上下文有"唯物主义"）
- "行而上学"→"形而上学"
- "的/得/地"、"在/再"混用

【纠错原则】
- 只纠正明显的同音字错误
- 不确定时保留原文
- 不纠正语法错误或标点错误
- 基于领域推断专业术语

【输出要求】
对每条字幕输出：
- subtitle_id：原ID
- corrected_text：纠错后文本
- corrections：纠错记录列表 [{"original": "原文", "corrected": "纠正", "timestamp": "时间戳", "reason": "判断依据"}]
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `input_subtitles[].subtitle_id` | string | 原始字幕文件 | 字幕唯一标识 |
| `input_subtitles[].text` | string | 原始字幕文件 | 原始字幕文本 |
| `input_subtitles[].start_sec` | number | 原始字幕文件 | 开始时间(秒) |
| `input_subtitles[].end_sec` | number | 原始字幕文件 | 结束时间(秒) |
| `domain` | string | ←步骤1 | 视频领域 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `corrected_subtitles[].subtitle_id` | string | →步骤3 | 字幕ID透传 |
| `corrected_subtitles[].corrected_text` | string | →步骤3 | 纠错后文本 |
| `corrected_subtitles[].start_sec` | number | →步骤3 | 时间戳透传 |
| `corrected_subtitles[].end_sec` | number | →步骤3 | 时间戳透传 |
| `corrected_subtitles[].corrections` | array | 调试记录 | 纠错记录列表 |

#### 输出JSON结构示例
```json
{
  "corrected_subtitles": [
    {
      "subtitle_id": "SUB001",
      "corrected_text": "唯心者和唯物主义的区别",
      "start_sec": 10.5,
      "end_sec": 15.2,
      "corrections": [
        {
          "original": "维新者",
          "corrected": "唯心者",
          "timestamp": "00:00:10",
          "reason": "根据上下文'唯物主义'，此处应为哲学术语'唯心者'而非历史术语'维新者'"
        }
      ]
    }
  ],
  "correction_summary": [
    {
      "timestamp": "00:00:10",
      "original": "维新者",
      "corrected": "唯心者",
      "reason": "根据上下文'唯物主义'，此处应为哲学术语'唯心者'而非历史术语'维新者'"
    }
  ]
}
```

#### 纠错记录格式说明
| 字段 | 说明 | 示例 |
|-----|------|------|
| `timestamp` | 纠错发生的时间戳 | "00:02:10" |
| `original` | 原始错误文本 | "维新者" |
| `corrected` | 纠正后文本 | "唯心者" |
| `reason` | 判断依据（必须说明为什么这样纠正） | "根据上下文'唯物主义'，此处应为哲学术语" |

---

### 步骤3：自然语义合并
- **类型**：LLM
- **约束**：单句不超过80字

#### 核心动作
将ASR细切字幕碎片句合并为语法完整、语义通顺的句子

#### 实现流程
```
1. [输入] 接收纠错后字幕列表
2. [LLM] 滑动窗口处理（窗口大小10条）
   - 判断相邻字幕是否属于同一语义单元
   - 基于标点、语气词、语义完整性判断句子边界
3. [合并] 将属于同一语义单元的字幕合并
   - 合并文本
   - 时间戳取首尾（start_sec取第一条，end_sec取最后一条）
4. [约束检查] 单句超过80字则强制分割
5. [输出] 返回合并后句子列表
```

#### LLM合并Prompt
```
请将以下ASR细切字幕合并为语法完整、语义通顺的句子。

【字幕列表】
{subtitles}

【合并规则】
1. 相邻字幕如果语义连贯，应合并为一句
2. 遇到句号、问号、感叹号等结束标点，作为句子边界
3. 遇到明显的话题转换，作为句子边界
4. 单句不超过80字

【输出要求】
对每个合并后的句子输出：
- sentence_id：新生成的句子ID（格式：S001, S002...）
- text：合并后的完整句子
- source_subtitle_ids：来源字幕ID列表
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `corrected_subtitles[].subtitle_id` | string | ←步骤2 | 字幕ID |
| `corrected_subtitles[].corrected_text` | string | ←步骤2 | 纠错后文本 |
| `corrected_subtitles[].start_sec` | number | ←步骤2 | 开始时间 |
| `corrected_subtitles[].end_sec` | number | ←步骤2 | 结束时间 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `merged_sentences[].sentence_id` | string | →步骤4,5,6,8a | 句子唯一标识 |
| `merged_sentences[].text` | string | →步骤4 | 合并后文本 |
| `merged_sentences[].start_sec` | number | →步骤4 | 合并后开始时间 |
| `merged_sentences[].end_sec` | number | →步骤4 | 合并后结束时间 |
| `merged_sentences[].source_subtitle_ids` | string[] | 调试追溯 | 来源字幕ID |

---

### 步骤4：局部冗余删除 + 时间戳存储
- **类型**：LLM + 本地存储
- **注意力范围**：局部上下文（单句内）

#### 核心动作
清理单句内无价值冗余，并将时间戳存储到本地

#### 局部冗余分类（硬编码，全部直接删除）
| 冗余类型 | 定义 | 示例 | 处理方式 |
|---------|------|------|---------|
| 结巴类 | 单句内连续重复词汇/音节，无语义价值 | "我我我想说"→"我想说" | 直接删除 |
| 单句内无意义重复 | 单句内重复词汇无语义增量 | "这个这个方案可行"→"这个方案可行" | 直接删除 |
| 口语填充词 | 无实义的语气词/过渡词 | "那个"、"就是说"、"然后"、"嗯"、"啊"、"呃" | 直接删除 |
| 同音/近音词误判 | ASR将同一词汇误判为多个同音词 | "产品的的质量"→"产品的质量" | 直接删除 |
| 背景噪音误判 | 环境杂音被误判为有意义词汇 | "人工智能趋势呃嘶"→"人工智能趋势" | 直接删除 |
| 单句语义赘述 | 单句内同义叠加表述，无新增信息 | "我个人认为我觉得"→"我认为" | 直接删除 |

#### 实现流程
```
1. [输入] 接收合并后句子列表
2. [LLM] 逐句处理（仅需单句内信息，无需跨句上下文）
   - 删除结巴（连续重复词汇/音节）
   - 删除填充词（无实义语气词/过渡词）
   - 删除单句内无意义重复
   - 删除同音词误判冗余
   - 删除噪音误判冗余
   - 删除单句语义赘述
3. [存储] 将时间戳存储到本地 sentence_timestamps.json
4. [输出] 返回清理后句子（不含时间戳）
```

#### LLM清理Prompt
```
请清理以下句子中的无价值冗余内容。

【句子列表】
{sentences}

【清理类型（全部直接删除）】
1. 结巴类：单句内连续重复词汇/音节，如"我我我想说"→"我想说"
2. 单句内无意义重复：如"这个这个方案可行"→"这个方案可行"
3. 口语填充词：如"那个"、"就是说"、"然后"、"嗯"、"啊"、"呃"
4. 同音/近音词误判：如"产品的的质量"→"产品的质量"
5. 背景噪音误判：如句末的"呃嘶"等无意义音
6. 单句语义赘述：如"我个人认为我觉得"→"我认为"

【注意】
- 保留有意义的重复（如强调性重复）
- 保留有表达作用的语气词
- 仅处理单句内的冗余，不考虑跨句关系

【输出要求】
对每个句子输出：
- sentence_id：原ID
- cleaned_text：清理后文本
- removed_items：删除的冗余内容列表（调试用）
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `merged_sentences[].sentence_id` | string | ←步骤3 | 句子ID |
| `merged_sentences[].text` | string | ←步骤3 | 合并后文本 |
| `merged_sentences[].start_sec` | number | ←步骤3 | 开始时间 |
| `merged_sentences[].end_sec` | number | ←步骤3 | 结束时间 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `cleaned_sentences[].sentence_id` | string | →步骤5,6,8a | 句子ID透传 |
| `cleaned_sentences[].cleaned_text` | string | →步骤5,6,8a | 清理后文本 |

#### 本地存储（sentence_timestamps.json）
```json
{
  "S001": {"start_sec": 10.5, "end_sec": 15.2},
  "S002": {"start_sec": 15.2, "end_sec": 20.1}
}
```

---

### 步骤5：跨句冗余删除
- **类型**：LLM
- **注意力范围**：跨句上下文（滑动窗口3-5句）

#### 核心动作
删除跨句无价值冗余（无增量信息，直接删除）

#### 跨句冗余删除分类（硬编码，全部直接删除）
| 冗余类型 | 定义 | 判断依据 | 处理方式 |
|---------|------|---------|---------|
| 跨句完全重复 | 相邻句子完全复述且无任何增量信息 | 相似度>0.95 | 直接删除重复句 |
| 离题性冗余 | 与核心主题无关的内容 | 基于main_topic判断 | 直接删除 |
| 高频口头禅（无意义） | 局部窗口内高频重复且与语义无关的口头禅 | 3-5句窗口内频率统计 | 直接删除 |

#### 实现流程
```
1. [输入] 接收清理后句子列表 + main_topic
2. [LLM] 滑动窗口处理（窗口大小3-5句）
   - 检测完全重复句（相似度>0.95，无增量信息）
   - 检测离题句（与main_topic无关，基于主题一致性验证）
   - 检测高频无意义口头禅（窗口内频率统计）
3. [标记] 标记需要删除的句子
4. [输出] 返回非冗余句子列表
```

#### LLM删除Prompt
```
请识别以下句子中需要直接删除的跨句冗余内容。

【核心主题】{main_topic}

【句子列表】
{sentences}

【冗余类型（全部直接删除，无增量价值）】
1. 跨句完全重复：与前面句子内容完全相同或高度相似（相似度>0.95），无任何新信息
   - 示例："我要去超市。我要去超市。" → 删除第二句
2. 离题性冗余：与核心主题无关的内容
   - 示例：主题"Python数据分析" → "我昨天用Python写了个小游戏" → 删除
3. 高频口头禅（无意义）：局部窗口内高频重复且与语义无关
   - 示例："方案可行，你知道吧？成本很低，你知道吧？" → 删除"你知道吧"

【判断原则】
- 删除的内容必须是"无增量价值"的
- 如果句子虽然重复但有新信息补充，不要删除（留给步骤6合并）
- 离题判断需结合核心主题，不要误删相关内容

【输出要求】
输出需要保留的句子ID列表，并说明删除原因
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `cleaned_sentences[].sentence_id` | string | ←步骤4 | 句子ID |
| `cleaned_sentences[].cleaned_text` | string | ←步骤4 | 清理后文本 |
| `main_topic` | string | ←步骤1 | 核心主题（离题判断依据） |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `non_redundant_sentences[].sentence_id` | string | →步骤6 | 句子ID透传 |
| `non_redundant_sentences[].cleaned_text` | string | →步骤6 | 文本透传 |

---

### 步骤6：跨句冗余合并
- **类型**：LLM
- **注意力范围**：跨句上下文（滑动窗口5-8句）

#### 核心动作
整合跨句语义重叠且含增量信息的内容（有增量价值，合并保留）

#### 跨句冗余合并分类（硬编码，合并而非删除）
| 冗余类型 | 定义 | 判断依据 | 处理方式 |
|---------|------|---------|---------|
| 断句错误重复 | ASR断句错误导致的句子拆分，后句含增量信息 | 文本重叠度高但后句有新内容 | 合并重复部分+增量信息 |
| 跨句同义转述 | 相邻句子语义重叠但存在不同表述角度 | 相似度0.6-0.9，各有增量 | 合并核心语义+补充信息 |
| 跨句部分重复 | 句子部分内容重复，其余部分为有效补充 | 部分重叠+部分新增 | 合并重复部分，保留补充 |

#### 删除 vs 合并的判断标准
```
核心判断：冗余片段是否包含可提取的有效语义增量？

无增量 → 步骤5直接删除
  - 完全重复句（相似度>0.95，无新信息）
  - 离题句
  - 无意义口头禅

有增量 → 步骤6合并处理
  - 断句错误重复（后句有新内容）
  - 同义转述（不同角度的补充）
  - 部分重复（有新增信息）
```

#### 实现流程
```
1. [输入] 接收非冗余句子列表
2. [LLM] 滑动窗口处理（窗口大小5-8句）
   - 检测语义重叠句（相似度0.6-0.9）
   - 判断是否含增量信息
   - 如有增量，合并为一个段落
3. [合并] 生成段落，记录来源句子ID
4. [输出] 返回段落列表
```

#### LLM合并Prompt
```
请将以下句子中语义重叠但含增量信息的内容合并为段落。

【句子列表】
{sentences}

【需要合并的冗余类型（有增量价值）】
1. 断句错误重复：ASR断句错误导致的拆分，后句有新内容
   - 示例："我今天要去超市。我今天要去超市买水果" → "我今天要去超市买水果"
2. 跨句同义转述：语义重叠但有不同角度的补充
   - 示例："我们需要优化算法效率。我们的核心目标是提升算法的运行速度" 
   → "我们的核心目标是优化算法效率，提升运行速度"
3. 跨句部分重复：部分内容重复，其余为有效补充
   - 示例："这个方法的优点是高效。这个方法的优点是稳定" 
   → "这个方法的优点是高效且稳定"

【合并规则】
1. 语义重叠但各有增量信息的句子，合并为一个段落
2. 保留所有增量信息，不丢失细节
3. 合并后的段落应语义连贯
4. 保持讲解者的表达风格

【输出要求】
对每个段落输出：
- paragraph_id：新生成的段落ID（格式：P001, P002...）
- text：合并后的段落文本
- source_sentence_ids：来源句子ID列表
- merge_type：合并类型（断句错误重复/同义转述/部分重复/无合并）
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `non_redundant_sentences[].sentence_id` | string | ←步骤5 | 句子ID |
| `non_redundant_sentences[].cleaned_text` | string | ←步骤5 | 文本 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `pure_text_script[].paragraph_id` | string | →步骤7 | 段落唯一标识 |
| `pure_text_script[].text` | string | →步骤7 | 合并后段落文本 |
| `pure_text_script[].source_sentence_ids` | string[] | →步骤7(时间戳计算) | 来源句子ID列表 |

---

## 三、分片与断层识别

### 步骤7：知识点细粒度分片 + 时间戳存储
- **类型**：LLM + 本地存储

#### 核心动作
按DARPA八问框架进行细粒度分片，每个片段只对应一个问题的一个语义维度

#### 实现流程
```
1. [输入] 接收段落列表
2. [宏观分段] 识别主题转换点，初步拆分
3. [语义判断] 对每个片段列出候选DARPA问题
4. [上下文敲定] 
   - 检查前后句关键词（参考下方关键词映射表）
   - 如关键词不明确，分析整体谈论内容
   - 确定最终DARPA问题和语义维度
5. [微观提取] 在每个片段内提取：
   - 例子（用于说明知识点的具体案例、场景描述）
   - 类比（"像...一样"、"类似于"、"好比"等表达）
   - 具象词语（区别于抽象术语的具体、可感知的动词和名词）
   - 个人理解/核心洞察（"我认为"、"其实"、"本质上"、"关键在于"等标记）
6. [继续细分] 如片段涉及多个语义维度，继续拆分直到单一维度
7. [存储] 计算并存储段落级时间戳
8. [输出] 返回知识点片段列表
```

#### 上下文敲定关键词映射（参考，不限于此）
| DARPA问题      | 常见关键词示例             |
| ------------ | ------------------- |
| Q1 要解决什么问题   | 为什么需要、痛点、问题、困难、挑战   |
| Q2 旧方法及局限    | 以前、传统方式、局限、过去、原来    |
| Q3 创新之处与核心原理 | 创新、改进、核心机制、原理、本质    |
| Q4 应用场景与价值   | 应用场景、适合、用在、谁用、什么时候用 |
| Q5 代价与风险     | 代价、成本、约束、缺点、风险、局限   |
| Q6 最小验证案例    | 验证、案例、示例、演示、举例      |
| Q7 与其他知识的关联  | 关联、依赖、对比、联系、类似      |
| Q8 易误解之处     | 误区、容易混淆、注意、常见错误、陷阱  |

**注意**：以上关键词仅为参考示例，实际判断时应根据上下文语义灵活判断，不必拘泥于特定词语。

#### DARPA八问框架
| 问题编号 | 问题名称 | 所属层级 | 关键词示例 |
|---------|---------|---------|-----------|
| Q1 | 要解决什么问题 | 目标与创新 | 为什么需要、痛点、问题 |
| Q2 | 旧方法及局限 | 目标与创新 | 以前、传统方式、局限、过去 |
| Q3 | 创新之处与核心原理 | 目标与创新 | 创新、改进、核心机制、原理 |
| Q4 | 应用场景与价值 | 价值与风险 | 应用场景、适合、用在、谁用 |
| Q5 | 代价与风险 | 价值与风险 | 代价、成本、约束、缺点 |
| Q6 | 最小验证案例 | 验证与边界 | 验证、案例、示例、演示 |
| Q7 | 与其他知识的关联 | 验证与边界 | 关联、依赖、对比、联系 |
| Q8 | 易误解之处 | 验证与边界 | 误区、容易混淆、注意、常见错误 |

#### 语义维度类型
| 维度类型 | 说明 | 可选值 |
|---------|------|--------|
| 逻辑关系 | 内容之间的逻辑连接 | 因果/对比/递进/并列/条件 |
| 分类分层 | 内容的层次定位 | 定义层/原理层/实现层/应用层/边界层 |

#### LLM分片Prompt
```
请将以下文本按DARPA八问框架进行细粒度分片。

【文本内容】
{paragraphs}

【DARPA八问】
Q1: 要解决什么问题
Q2: 旧方法及局限
Q3: 创新之处与核心原理
Q4: 应用场景与价值
Q5: 代价与风险
Q6: 最小验证案例
Q7: 与其他知识的关联
Q8: 易误解之处

【分片要求】
1. 每个片段只对应一个DARPA问题的一个语义维度
2. 识别知识点名称
3. 确定逻辑关系（因果/对比/递进/并列/条件）
4. 确定分类分层（定义层/原理层/实现层/应用层/边界层）
5. 提取例子、类比、具象词语、个人洞察

【输出格式】
对每个片段输出完整的JSON结构
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `pure_text_script[].paragraph_id` | string | ←步骤6 | 段落ID |
| `pure_text_script[].text` | string | ←步骤6 | 段落文本 |
| `pure_text_script[].source_sentence_ids` | string[] | ←步骤6 | 来源句子ID |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `knowledge_segments[].segment_id` | string | →全链路 | 片段唯一标识 |
| `knowledge_segments[].full_text` | string | →步骤8a,16,18,20,21 | 完整文本 |
| `knowledge_segments[].knowledge_point` | string | →步骤23,24 | 所属知识点名称 |
| `knowledge_segments[].darpa_question` | string | →步骤8a,16,17,18 | Q1-Q8 |
| `knowledge_segments[].darpa_question_name` | string | →步骤23 | 问题名称 |
| `knowledge_segments[].semantic_dimension.logic_relation` | string | →步骤8a,17 | 逻辑关系 |
| `knowledge_segments[].semantic_dimension.hierarchy_type` | string | →步骤8a,17 | 分类分层 |
| `knowledge_segments[].semantic_dimension.description` | string | →步骤16,21 | 自然语言描述 |
| `knowledge_segments[].core_semantic.summary` | string | →步骤16 | 20-50字摘要 |
| `knowledge_segments[].core_semantic.label` | string | →步骤23,24 | 8字以内命名标签 |
| `knowledge_segments[].extracted_elements` | object | →步骤21 | 提取的元素 |
| `knowledge_segments[].source_paragraph_ids` | string[] | 调试追溯 | 来源段落ID |

#### 输出JSON结构
```json
{
  "knowledge_segments": [
    {
      "segment_id": "SEG001",
      "full_text": "string",
      "knowledge_point": "分布式锁",
      "darpa_question": "Q3",
      "darpa_question_name": "创新之处与核心原理",
      "semantic_dimension": {
        "logic_relation": "因果关系",
        "hierarchy_type": "原理层",
        "description": "Q3-Redis实现分布式锁的核心机制"
      },
      "core_semantic": {
        "summary": "string（20-50字摘要）",
        "label": "string（8字以内标签）"
      },
      "extracted_elements": {
        "examples": [{"content": "...", "position": "第X句", "sub_dimension": "..."}],
        "analogies": [{"content": "...", "position": "第X句", "sub_dimension": "..."}],
        "concrete_words": [{"word": "...", "full_expression": "...", "abstract_concept": "...", "position": "第X句"}],
        "insights": [{"content": "...", "insight_type": "个人理解/核心洞察/独特视角", "position": "第X句", "sub_dimension": "..."}]
      },
      "source_paragraph_ids": ["P001", "P002"]
    }
  ]
}
```

#### 本地存储（segment_timestamps.json）
```json
{
  "SEG001": {
    "start_sec": 10.5,
    "end_sec": 45.3,
    "source_sentence_ids": ["S001", "S002", "S003"]
  }
}
```

---

### 步骤7b：可视化场景识别（与步骤8a并行）
- **类型**：LLM

#### 核心动作
识别文字稿中"适合可视化"的内容（独立于断层判断）

#### 设计理念
> **"文字定逻辑、截图定具象"**
> 
> 某些内容用视觉呈现比文字更直观高效，即使文字稿没有断层，也值得截图展示

#### 5种可视化场景（LLM语义分析判定）
| 场景类型 | 判定条件（AI分析，非关键词触发） | 可视化优势 |
|---------|-------------------------------|-----------|
| 层级/结构类 | 涉及架构分层、目录结构、数据结构组成 | 一眼看清层级关系 |
| 流程/流转类 | 逻辑有分支、闭环或顺序依赖 | 直观呈现全流程 |
| 实操/界面类 | 涉及软件操作、命令输出、界面配置 | 还原实操场景 |
| 对比/差异类 | 需区分不同状态、方案的差异 | 快速抓差异点 |
| 复杂逻辑关系类 | 多元素关联（ER图、依赖关系图等） | 完整呈现关联逻辑 |

#### 实现流程
```
1. [输入] 接收知识点片段列表
2. [LLM] 对每个片段进行语义分析
   - 判断是否属于5种可视化场景之一
   - 如属于，识别预期可视化形态
   - 提取截图必须包含的关键元素
3. [输出] 返回可视化候选列表
```

#### LLM Prompt
```
请分析以下知识点片段是否适合用可视化（截图/视频）呈现。

【片段信息】
- 片段ID：{segment_id}
- 内容：{full_text}
- DARPA问题：{darpa_question}

【5种可视化场景】
1. 层级/结构类：涉及架构分层、目录结构、数据结构组成
2. 流程/流转类：逻辑有分支、闭环或顺序依赖
3. 实操/界面类：涉及软件操作、命令输出、界面配置
4. 对比/差异类：需区分不同状态、方案的差异
5. 复杂逻辑关系类：多元素关联

【判断原则】
- 判断依据是"可视化效果是否优于纯文字"
- 不需要关键词触发，基于语义理解判断
- 即使文字描述完整，如果图更直观也应标记

【输出格式】
{
  "is_visualization_candidate": true/false,
  "scene_type": "5种场景之一/null",
  "expected_visual_forms": ["可视化形态1", "可视化形态2"],
  "key_elements": ["截图必须包含的关键元素1", "关键元素2"],
  "min_completeness": 0.7
}
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `knowledge_segments[].segment_id` | string | ←步骤7 | 片段ID |
| `knowledge_segments[].full_text` | string | ←步骤7 | 完整文本 |
| `knowledge_segments[].darpa_question` | string | ←步骤7 | DARPA问题 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `visualization_candidates[].segment_id` | string | →步骤9,14,15b,24 | 关联知识片段 |
| `visualization_candidates[].scene_type` | string | →步骤9,15b,24 | 5种场景之一 |
| `visualization_candidates[].expected_visual_forms` | string[] | →步骤14 Vision校验 | 预期可视化形态（多种候选） |
| `visualization_candidates[].key_elements` | string[] | →步骤14 Vision校验 | 截图必含元素 |
| `visualization_candidates[].min_completeness` | number | →步骤14 Vision校验 | 最低完整度要求(0-1)，默认0.7 |

#### 输出JSON结构
```json
{
  "visualization_candidates": [
    {
      "segment_id": "SEG001",
      "scene_type": "流程/流转类",
      "expected_visual_forms": ["流程图", "步骤示意图", "PPT流程页"],
      "key_elements": ["步骤1名称", "步骤2名称", "流转箭头"],
      "min_completeness": 0.7,
      "judgment_basis": "内容描述算法执行的多个步骤，有明确的先后顺序"
    }
  ]
}
```

---

### 步骤7c：知识点合并与过渡语识别
- **类型**：LLM + 本地存储
- **执行时机**：步骤7后，步骤7b/8a前

#### 核心动作
将细粒度segments合并为knowledge_points（知识点），识别并提取原文中的过渡语

#### 设计理念
> **"减少冗余处理，保持结构完整"**
> 
> 步骤7产生的细粒度segments按DARPA问题拆分，但同一知识点的多个维度应合并处理，减少后续步骤的重复工作。

#### 合并规则
| 合并条件 | 判断依据 | 合并结果 |
|---------|---------|---------|
| 相同知识点 | `knowledge_point` 字段相同 | 合并为一个knowledge_point |
| 连续DARPA问题 | Q1→Q2→Q3等连续问题 | 合并，保留问题列表 |
| 共享语义维度 | 逻辑关系或层次相近 | 优先合并 |

#### 过渡语识别规则
| 过渡类型 | 常见标记词 | 处理方式 |
|---------|-----------|---------|
| 主题过渡 | "接下来"、"下面我们来看"、"那么" | 提取为transition字段 |
| 层次过渡 | "首先"、"其次"、"最后"、"第一个" | 提取为transition字段 |
| 总结过渡 | "总结一下"、"综上所述"、"所以" | 提取为transition字段 |

> **优先原则**：如原文中有过渡语则直接采用，无需AI生成；仅当原文无过渡语时才在后续步骤生成

#### 实现流程
```
1. [输入] 接收知识点片段列表（from步骤7）
2. [分组] 按knowledge_point字段分组segments
3. [LLM分析] 对每组进行：
   a. 识别过渡语句（主题间过渡标记词）
   b. 提取过渡语及其位置
   c. 确定该知识点的时间范围
   d. 合并DARPA问题列表
   e. 聚合extracted_elements
4. [存储] 计算并存储知识点级时间戳（kp_timestamps.json）
5. [输出] 返回合并后的knowledge_points列表
```

#### LLM合并Prompt
```
请分析以下同属一个知识点的片段，进行合并并识别过渡语。

【知识点名称】{knowledge_point}

【片段列表】
{segments}

【过渡语识别规则】
识别以下类型的过渡语句：
1. 主题过渡："接下来"、"下面我们来看"、"那么"、"现在我们讨论"
2. 层次过渡："首先"、"其次"、"最后"、"第一个方面"
3. 总结过渡："总结一下"、"综上所述"、"所以说"

【注意】
- 过渡语只存在于知识点与知识点之间
- 如果原文有过渡语句，直接提取；如果没有，transition设为null
- 合并时保留所有extracted_elements

【输出格式】
{
  "kp_id": "KP001",
  "knowledge_point": "知识点名称",
  "transition": "原文过渡语/null",
  "transition_source": "original/null",
  "darpa_questions": ["Q1", "Q3", "Q6"],
  "merged_segments": ["SEG001", "SEG003", "SEG006"],
  "full_text": "合并后完整文本",
  "extracted_elements": {...聚合后的元素...},
  "start_sec": 10.5,
  "end_sec": 120.3
}
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `knowledge_segments[].segment_id` | string | ←步骤7 | 片段ID |
| `knowledge_segments[].knowledge_point` | string | ←步骤7 | 知识点名称 |
| `knowledge_segments[].darpa_question` | string | ←步骤7 | DARPA问题 |
| `knowledge_segments[].full_text` | string | ←步骤7 | 完整文本 |
| `knowledge_segments[].extracted_elements` | object | ←步骤7 | 提取的元素 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `knowledge_points[].kp_id` | string | →全链路 | 知识点唯一标识 |
| `knowledge_points[].knowledge_point` | string | →步骤21,23,24 | 知识点名称 |
| `knowledge_points[].transition` | string/null | →步骤21 | 原文过渡语（优先采用） |
| `knowledge_points[].transition_source` | string | →步骤21 | "original"或"null" |
| `knowledge_points[].darpa_questions` | string[] | →步骤16,17,21 | 涉及的DARPA问题列表 |
| `knowledge_points[].merged_segments` | string[] | →调试追溯 | 合并的片段ID列表 |
| `knowledge_points[].full_text` | string | →步骤16,18,20,21 | 合并后完整文本 |
| `knowledge_points[].extracted_elements` | object | →步骤21 | 聚合后的元素 |

#### 输出JSON结构
```json
{
  "knowledge_points": [
    {
      "kp_id": "KP001",
      "knowledge_point": "顺序查找算法",
      "transition": "接下来我们来看顺序查找算法",
      "transition_source": "original",
      "darpa_questions": ["Q1", "Q3", "Q6"],
      "merged_segments": ["SEG001", "SEG003", "SEG006"],
      "full_text": "合并后的完整文本...",
      "extracted_elements": {
        "examples": [...],
        "analogies": [...],
        "concrete_words": [...],
        "insights": [...]
      }
    }
  ]
}
```

#### 本地存储（kp_timestamps.json）
```json
{
  "KP001": {
    "start_sec": 10.5,
    "end_sec": 120.3,
    "merged_segments": ["SEG001", "SEG003", "SEG006"]
  }
}
```

---

### 步骤8a：断层分类与粗定位
- **类型**：LLM

#### 核心动作
识别知识点片段中的语义断层类型，粗略定位断层位置

#### 实现流程
```
1. [输入] 接收清理后句子 + 知识点片段
2. [LLM] 对每个片段扫描10类断层特征
   - 基于DARPA问题和语义维度，判断断层类型
   - 识别触发断层的句子
3. [输出] 返回断层候选列表，含上下文信息
```

#### 10类断层详细定义（硬编码）

##### 1. 显性指引类断层
- **特征**：文字稿出现「如图所示/看这个PPT/动画里的步骤/黑板上的公式」等显性指引词，且指引词后**无任何核心内容描述**
- **触发词示例**：看这个、如图所示、PPT上、动画里、黑板上
- **可视化形态**：PPT静态页/动画定格帧/板书完整页
- **形态特征**：文字清晰、无人物遮挡、内容居中展示

##### 2. 结论无推导类断层
- **特征**：文字稿突然给出公式/定理/结论，但**无推导步骤、实验依据、逻辑链条**，且推导过程仅存在于PPT/动画中
- **触发词示例**：所以公式是、最终结论是、由此得出
- **可视化形态**：PPT推导步骤页/动画推演帧/公式手写步骤
- **形态特征**：含公式字符+推导箭头/步骤编号

##### 3. 概念无定义类断层
- **特征**：文字稿提及专业术语/陌生概念，但**无定义、无适用场景、无具象化例子**，且解释信息仅存在于可视化内容中
- **触发词示例**：我们用XX、这里的XX、所谓的XX
- **可视化形态**：PPT术语卡片/概念动画演示帧/定义板书
- **形态特征**：含术语名称+定义文字+适用场景标签

##### 4. 实操步骤断裂类断层
- **特征**：软件/实验实操讲解中，步骤描述**跳步、缺失关键界面/参数/操作反馈**，且关键细节仅存在于操作动画/界面演示中
- **触发词示例**：点击、设置、输入、选择、运行
- **可视化形态**：软件操作界面/实验器材摆放/步骤演示动画
- **形态特征**：含关键按钮位置/参数设置值/操作反馈

##### 5. 分层分类无内容类断层
- **特征**：文字稿提及「分X层/X类/X模块」，但**未列出具体名称/功能/逻辑关系**，且层级内容仅存在于PPT框架图/分类表中
- **触发词示例**：分为三层、有四种类型、包含五个模块
- **可视化形态**：PPT层级框架图/分类对比表/模块关系图
- **形态特征**：含层级名称+功能描述+逻辑箭头

##### 6. 量化数据缺失类断层
- **特征**：文字稿提及「数据对比/效果提升/参数最优」等定性描述，但**无具体数值/表格/图表支撑**，且量化数据仅存在于PPT数据可视化内容中
- **触发词示例**：效率提升了、性能更好、数据显示
- **可视化形态**：PPT柱状图/折线图/数据对比表
- **形态特征**：含具体数值+对比维度+图例

##### 7. 指代模糊类断层
- **特征**：文字稿出现「这个结构/那个方法/该模型」等模糊指代词，但**无明确指代对象**，且指代内容仅存在于PPT/动画中
- **触发词示例**：这个、那个、该、此
- **可视化形态**：PPT结构示意图/模型动画/方法流程图
- **形态特征**：含结构名称+核心组件标注

##### 8. 动态过程空白类断层
- **特征**：文字稿提及「参数变化/系统演变/算法迭代」等动态过程，但**无关键节点/触发条件/结果差异**，且过程仅存在于动画演示中
- **触发词示例**：变化过程、演变、迭代、流动、传递
- **可视化形态**：参数变化动画/算法迭代步骤帧/系统演变演示
- **形态特征**：含状态变化节点+结果对比

##### 9. 符号编号缺失类断层
- **特征**：文字稿提及「公式(1)/图3-2/表2」等带编号的可视化内容，但**未给出编号对应的具体内容**，且内容仅存在于PPT/板书中
- **触发词示例**：公式(X)、图X-X、表X、如式X所示
- **可视化形态**：PPT编号公式页/编号图表页/板书编号内容
- **形态特征**：含清晰的编号标注+对应内容

##### 10. 对比逻辑缺失类断层
- **特征**：文字稿提及「A与B对比/两种方案差异」，但**无对比维度/差异点/适用场景**，且对比内容仅存在于PPT对比表/动画效果对比中
- **触发词示例**：对比、差异、区别、不同、相比
- **可视化形态**：PPT对比表格/方案差异图/动画效果对比帧
- **形态特征**：含对比对象+至少2个对比维度

#### LLM断层识别Prompt
```
请识别以下知识点片段中的语义断层。

【知识点片段】
{segment}

【片段上下文】
- 知识点：{knowledge_point}
- DARPA问题：{darpa_question} - {darpa_question_name}
- 语义维度：{semantic_dimension}

【10类断层类型及特征】
1. 显性指引类：出现"看这个PPT/动画/如图所示"等指引词，后无内容描述
2. 结论无推导类：突然给出公式/结论，无推导过程
3. 概念无定义类：提及专业术语，无定义解释
4. 实操步骤断裂类：步骤描述跳步，缺失关键界面/参数
5. 分层分类无内容类：提及"分X层/类"，未列出具体内容
6. 量化数据缺失类：定性描述无具体数值支撑
7. 指代模糊类："这个结构/那个方法"等模糊指代
8. 动态过程空白类：提及动态过程，无关键节点描述
9. 符号编号缺失类：提及"公式(1)/图3-2"，无具体内容
10. 对比逻辑缺失类：提及对比，无对比维度/差异点

【输出要求】
对每个识别到的断层输出：
- fault_id：断层ID
- fault_type：断层类型（1-10）
- trigger_sentence_id：触发断层的句子ID
- trigger_text：触发断层的原文
- trigger_keywords：触发断层的关键词
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `cleaned_sentences[]` | array | ←步骤4 | 清理后句子 |
| `knowledge_segments[]` | array | ←步骤7 | 知识点片段 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `fault_candidates[].fault_id` | string | →全链路 | 断层唯一标识 |
| `fault_candidates[].segment_id` | string | →步骤8b | 所属片段ID |
| `fault_candidates[].fault_type` | string | →步骤8b,9,17 | 断层类型 |
| `fault_candidates[].trigger_sentence_id` | string | →步骤8b | 触发句子ID |
| `fault_candidates[].trigger_text` | string | →步骤8b | 触发原文 |
| `fault_candidates[].fault_context` | object | →步骤8b | 断层上下文 |

---

### 步骤8b：断层精确定位与补全分析
- **类型**：LLM + 本地查询

#### 核心动作
基于粗定位结果，精确定位断层时间范围，分析缺失内容，标注可视化形态

#### 实现流程
```
1. [输入] 接收断层候选 + 句子时间戳
2. [查询] 从本地存储获取触发句子的时间戳
3. [LLM] 精确定位断层时间范围
   - 根据断层类型应用时间锚点规则
   - 分析缺失内容（必须补全 + 次要补全）
   - 判断可视化形态
4. [输出] 返回精确定位的断层列表
```

#### 时间锚点规则（硬编码）
| 断层类型 | 时间锚点规则 | 说明 |
|---------|-------------|------|
| 1.显性指引类 | 指引词后0.5-2秒 | 讲解者需时间切换PPT/动画并停留 |
| 2.结论无推导类 | 结论前1-3秒 | 推导过程通常在结论前展示 |
| 3.概念无定义类 | 概念出现后0-1秒 | 讲解者通常会同步展示概念解释 |
| 4.实操步骤断裂类 | 同步截帧 | 操作演示与讲解同步 |
| 5.分层分类无内容类 | 提及后0.5-1.5秒 | 框架图通常同步展示 |
| 6.量化数据缺失类 | 定性描述前1-2秒 | 数据图表通常先展示再口头总结 |
| 7.指代模糊类 | 前2-3秒至后1秒 | 指代对象通常在前后展示 |
| 8.动态过程空白类 | 全程区间 | 覆盖从初始状态到最终状态 |
| 9.符号编号缺失类 | 提及后0-1秒 | 编号对应内容通常同步展示 |
| 10.对比逻辑缺失类 | 提及后0.5-1.5秒 | 对比表通常同步展示 |

#### 可视化形态标注（硬编码）
| 断层类型 | 可视化形态 | 形态特征 |
|---------|-----------|---------|
| 1.显性指引类 | PPT静态页/动画定格帧/板书完整页 | 文字清晰、无人物遮挡、内容居中 |
| 2.结论无推导类 | PPT推导步骤页/动画推演帧/公式手写步骤 | 含公式字符+推导箭头/步骤编号 |
| 3.概念无定义类 | PPT术语卡片/概念动画演示帧/定义板书 | 含术语名称+定义文字+适用场景 |
| 4.实操步骤断裂类 | 软件操作界面/实验器材摆放/步骤演示动画 | 含关键按钮位置/参数设置值/操作反馈 |
| 5.分层分类无内容类 | PPT层级框架图/分类对比表/模块关系图 | 含层级名称+功能描述+逻辑箭头 |
| 6.量化数据缺失类 | PPT柱状图/折线图/数据对比表 | 含具体数值+对比维度+图例 |
| 7.指代模糊类 | PPT结构示意图/模型动画/方法流程图 | 含结构名称+核心组件标注 |
| 8.动态过程空白类 | 参数变化动画/算法迭代步骤帧/系统演变演示 | 含状态变化节点+结果对比 |
| 9.符号编号缺失类 | PPT编号公式页/编号图表页/板书编号内容 | 含清晰的编号标注+对应内容 |
| 10.对比逻辑缺失类 | PPT对比表格/方案差异图/动画效果对比帧 | 含对比对象+至少2个对比维度 |

#### 分层校验标准（硬编码，用于步骤14）
| 断层类型 | 核心必含项 | 次要加分项 |
|---------|-----------|-----------|
| 1.显性指引类 | 指引词指向的核心内容 | 内容的功能描述/关联标注 |
| 2.结论无推导类 | 公式完整形式+至少1个推导关键步 | 推导依据（如"由勾股定理得"） |
| 3.概念无定义类 | 术语名称+核心定义 | 适用场景/典型例子 |
| 4.实操步骤断裂类 | 关键操作界面（按钮/参数位置清晰） | 操作后的正确结果展示 |
| 5.分层分类无内容类 | 所有层级/类别的名称 | 各层级的功能描述/关联关系 |
| 6.量化数据缺失类 | 关键对比数值+对比维度 | 数据结论标注（如"A方案最优"） |
| 7.指代模糊类 | 指代词对应的对象名称+核心特征 | 对象的优势/适用场景 |
| 8.动态过程空白类 | 至少2个关键变化节点+最终结果 | 变化的触发条件 |
| 9.符号编号缺失类 | 编号+对应内容的完整形式 | 编号内容的推导依据/应用场景 |
| 10.对比逻辑缺失类 | 对比对象+至少1个核心差异维度 | 差异点对应的适用场景建议 |

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `fault_candidates[]` | array | ←步骤8a | 断层候选 |
| `sentence_timestamps` | object | ←本地存储 | 句子时间戳 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `semantic_faults[].fault_id` | string | →全链路 | 断层ID透传 |
| `semantic_faults[].segment_id` | string | →全链路 | 片段ID透传 |
| `semantic_faults[].fault_type` | string | →步骤9,16,17,18 | 断层类型 |
| `semantic_faults[].fault_location` | object | →步骤9,10 | 精确时间范围 |
| `semantic_faults[].visual_form` | string | →步骤17 | 可视化形态 |
| `semantic_faults[].missing_content` | object | →步骤11,21 | 缺失内容分析 |

---

## 四、截图指令生成

### 步骤9：截帧策略匹配
- **类型**：代码规则（硬编码映射表）

#### 核心动作
根据断层类型匹配截帧策略

#### 实现流程
```
1. [输入] 接收断层列表
2. [映射] 根据fault_type查询映射表
3. [输出] 返回策略匹配结果
```

#### 映射规则（硬编码）
| 断层类型 | 截帧策略 | 时间锚点规则 | 采样模式 | 帧数 | 图像增强 |
|---------|---------|-------------|---------|------|---------|
| 1.显性指引类 | 显性指引 | 指引词后0.5-2秒 | 多帧采样 | 3 | 默认 |
| 2.结论无推导类 | 过程公式 | 结论前1-3秒 | 多帧采样 | 5 | 默认 |
| 3.概念无定义类 | 显性指引 | 概念出现后0-1秒 | 单帧精准 | 1 | 默认 |
| 4.实操步骤断裂类 | 显性指引 | 同步截帧 | 多帧采样 | 3 | 默认 |
| 5.分层分类无内容类 | 显性指引 | 提及后0.5-1.5秒 | 双帧采样 | 2 | 默认 |
| 6.量化数据缺失类 | 量化数据 | 定性描述前1-2秒 | 单帧精准 | 1 | 锐化处理 |
| 7.指代模糊类 | 显性指引 | 前2-3秒至后1秒 | 多帧采样 | 3 | 默认 |
| 8.动态过程空白类 | 过程公式 | 全程区间 | 多帧采样 | 5 | 默认 |
| 9.符号编号缺失类 | 符号编号 | 提及后0-1秒 | 单帧精准 | 1 | 局部放大 |
| 10.对比逻辑缺失类 | 量化数据 | 提及后0.5-1.5秒 | 双帧采样 | 2 | 默认 |

#### 代码实现
```python
STRATEGY_MAP = {
    "显性指引类": {
        "strategy": "显性指引", 
        "anchor": "后0.5-2秒", 
        "mode": "多帧采样", 
        "count": 3,
        "enhance": "default"
    },
    "结论无推导类": {
        "strategy": "过程公式", 
        "anchor": "前1-3秒", 
        "mode": "多帧采样", 
        "count": 5,
        "enhance": "default"
    },
    "概念无定义类": {
        "strategy": "显性指引", 
        "anchor": "后0-1秒", 
        "mode": "单帧精准", 
        "count": 1,
        "enhance": "default"
    },
    "实操步骤断裂类": {
        "strategy": "显性指引", 
        "anchor": "同步", 
        "mode": "多帧采样", 
        "count": 3,
        "enhance": "default"
    },
    "分层分类无内容类": {
        "strategy": "显性指引", 
        "anchor": "后0.5-1.5秒", 
        "mode": "双帧采样", 
        "count": 2,
        "enhance": "default"
    },
    "量化数据缺失类": {
        "strategy": "量化数据", 
        "anchor": "前1-2秒", 
        "mode": "单帧精准", 
        "count": 1,
        "enhance": "sharpen"  # 数据图表文字小，需增强清晰度
    },
    "指代模糊类": {
        "strategy": "显性指引", 
        "anchor": "前2-3秒至后1秒", 
        "mode": "多帧采样", 
        "count": 3,
        "enhance": "default"
    },
    "动态过程空白类": {
        "strategy": "过程公式", 
        "anchor": "全程区间", 
        "mode": "多帧采样", 
        "count": 5,
        "enhance": "default"
    },
    "符号编号缺失类": {
        "strategy": "符号编号", 
        "anchor": "后0-1秒", 
        "mode": "单帧精准", 
        "count": 1,
        "enhance": "local_zoom"  # 编号内容位置固定，放大后提升识别率
    },
    "对比逻辑缺失类": {
        "strategy": "量化数据", 
        "anchor": "后0.5-1.5秒", 
        "mode": "双帧采样", 
        "count": 2,
        "enhance": "default"
    }
}

def match_strategy(fault_type):
    return STRATEGY_MAP.get(fault_type)
```

#### 可视化场景→截图策略映射（用于无断层场景）
| 场景类型 | 截帧策略 | 采样模式 | 帧数 | 说明 |
|---------|---------|---------|------|------|
| 层级/结构类 | 显性指引 | 单帧精准 | 1 | 静态结构图一帧足够 |
| 流程/流转类 | 过程公式 | 多帧采样 | 3-5 | 捕捉流程多个阶段 |
| 实操/界面类 | 显性指引 | 多帧采样 | 3 | 捕捉操作关键步骤 |
| 对比/差异类 | 量化数据 | 双帧采样 | 2 | 捕捉对比前后状态 |
| 复杂逻辑关系类 | 显性指引 | 单帧精准 | 1 | 完整关系图一帧展示 |

#### 代码实现（可视化场景映射）
```python
VISUALIZATION_STRATEGY_MAP = {
    "层级/结构类": {
        "strategy": "显性指引", 
        "mode": "单帧精准", 
        "count": 1,
        "enhance": "default"
    },
    "流程/流转类": {
        "strategy": "过程公式", 
        "mode": "多帧采样", 
        "count": 5,
        "enhance": "default"
    },
    "实操/界面类": {
        "strategy": "显性指引", 
        "mode": "多帧采样", 
        "count": 3,
        "enhance": "default"
    },
    "对比/差异类": {
        "strategy": "量化数据", 
        "mode": "双帧采样", 
        "count": 2,
        "enhance": "default"
    },
    "复杂逻辑关系类": {
        "strategy": "显性指引", 
        "mode": "单帧精准", 
        "count": 1,
        "enhance": "default"
    }
}

def match_strategy_combined(fault_type=None, scene_type=None):
    """合并断层和可视化场景的策略匹配"""
    # 优先使用断层类型策略
    if fault_type and fault_type in STRATEGY_MAP:
        return STRATEGY_MAP[fault_type]
    # 其次使用可视化场景策略
    if scene_type and scene_type in VISUALIZATION_STRATEGY_MAP:
        return VISUALIZATION_STRATEGY_MAP[scene_type]
    return None
```

#### 输入（增加可视化候选）
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `semantic_faults[]` | array | ←步骤8b | 断层列表 |
| `visualization_candidates[]` | array | ←步骤7b | 可视化候选列表 |

#### 输出（增加可视化相关字段）
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `strategy_match[].expected_visual_forms` | string[] | →步骤14 | 预期可视化形态（复数） |
| `strategy_match[].key_elements` | string[] | →步骤14 | 截图必含元素 |
| `strategy_match[].min_completeness` | number | →步骤14 | 最低完整度要求（透传） |

---

### 步骤10：截帧时间计算
- **类型**：代码规则 + 本地查询

#### 核心动作
根据时间锚点规则计算精确截帧时间（支持断层和可视化场景两条路径）

#### 实现流程
```
1. [输入] 接收策略匹配结果 + 断层位置/可视化候选
2. [查询] 对于可视化场景，从本地存储查询 segment_timestamps
3. [计算] 根据时间锚点规则计算截帧时间
   - 断层场景：使用 fault_location
   - 可视化场景：使用 segment_timestamps 关联的时间范围
   - 单帧精准：计算单个preferred_sec
   - 多帧采样：计算多个preferred_sec
   - 双帧采样：计算首尾两个preferred_sec
4. [备选] 计算fallback_range
5. [输出] 返回时间参数

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `strategy_match[]` | array | ←步骤9 | 策略匹配结果 |
| `fault_location` | object | ←步骤8b | 断层时间范围（断层场景） |
| `visualization_candidates[]` | array | ←步骤7b | 可视化候选（可视化场景） |
| `segment_timestamps` | object | ←本地存储 | 片段时间戳（可视化场景）|
```

#### 代码实现
```python
def calculate_capture_times(strategy_match, fault_location):
    start = fault_location["start_sec"]
    end = fault_location["end_sec"]
    mode = strategy_match["capture_mode"]
    count = strategy_match["frame_count"]
    
    if mode == "单帧精准截":
        preferred = [start + 0.5]
    elif mode == "双帧采样":
        preferred = [start + 0.5, end - 0.5]
    elif mode == "多帧采样":
        step = (end - start) / (count + 1)
        preferred = [start + step * (i + 1) for i in range(count)]
    
    fallback = {"start_sec": start, "end_sec": end, "step_sec": 0.5}
    return {"capture_times": preferred, "fallback_range": fallback}
```

---

### 步骤11：标准化JSON指令生成
- **类型**：代码规则

#### 核心动作
生成含执行层和校验层的截帧任务包

#### 实现流程
```
1. [输入] 接收时间参数 + 策略匹配 + 断层信息
2. [生成] 构建截帧指令
   - opencv_params：截帧执行参数
   - validation_questions：校验问题（基于missing_content生成）
3. [输出] 返回截帧指令列表
```

#### 校验问题生成规则
```python
def generate_validation_questions(missing_content=None, fault_type=None, 
                                   key_elements=None, scene_type=None):
    """支持断层和可视化场景两种来源"""
    questions = []
    
    # 路径1：断层场景 - 基于missing_content
    if missing_content:
        must = missing_content["must_supplement"]
        questions.append({
            "question_id": "Q1",
            "question": f"图中是否包含'{must}'？",
            "is_core": True
        })
        secondary = missing_content.get("secondary_supplement")
        if secondary:
            questions.append({
                "question_id": "Q2",
                "question": f"图中是否包含'{secondary}'？",
                "is_core": False
            })
    
    # 路径2：可视化场景 - 基于key_elements
    if key_elements:
        for i, elem in enumerate(key_elements):
            questions.append({
                "question_id": f"V{i+1}",
                "question": f"图中是否清晰展示'{elem}'？",
                "is_core": (i == 0)  # 第一个元素为核心
            })
    
    return questions
```

#### 输入（增加可视化场景支持）
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `capture_times` | object | ←步骤10 | 截帧时间参数 |
| `strategy_match[]` | array | ←步骤9 | 策略匹配结果 |
| `semantic_faults[]` | array | ←步骤8b | 断层信息（断层场景） |
| `visualization_candidates[]` | array | ←步骤7b | 可视化候选（可视化场景） |

---

## 五、截帧执行与质控

### 步骤12：截帧策略执行
- **类型**：Tool(OpenCV)

#### 核心动作
调用OpenCV执行截帧策略

#### 实现流程
```
1. [输入] 接收截帧指令 + 视频路径
2. [执行] 对每个指令：
   - 打开视频文件
   - 定位到primary_times中的每个时间点
   - 截取帧并保存
   - 应用图像增强（如果配置）
3. [输出] 返回截取的帧列表
```

#### 代码实现
```python
import cv2

def capture_frames(instruction, video_path):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    frames = []
    
    for i, time_sec in enumerate(instruction["opencv_params"]["primary_times"]):
        frame_num = int(time_sec * fps)
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
        ret, frame = cap.read()
        
        if ret:
            # 应用图像增强
            if instruction["opencv_params"]["enhance_params"]["sharpen"]:
                frame = apply_sharpen(frame)
            
            frame_path = f"temp_frames/{instruction['instruction_id']}_{i}.png"
            cv2.imwrite(frame_path, frame)
            frames.append({
                "frame_id": f"F_{instruction['instruction_id']}_{i}",
                "timestamp": time_sec,
                "frame_path": frame_path
            })
    
    cap.release()
    return frames
```

---

### 步骤13：帧校验（黑屏/过渡帧检测 + 帧去重）
- **类型**：Tool(OpenCV)

#### 核心动作
1. 过滤黑屏、模糊过渡帧
2. 基于感知哈希(pHash)去除相似帧

#### 实现流程
```
1. [输入] 接收截取的帧列表
2. [检测] 对每帧：
   - 计算平均亮度
   - 计算清晰度（拉普拉斯方差）
3. [过滤] 亮度<3或清晰度<50的帧标记为无效
4. [去重] 基于感知哈希计算帧指纹
   - 计算每帧的pHash
   - 比较汉明距离，距离<8视为相似
   - 相似帧只保留第一张
5. [输出] 返回去重后的有效帧列表
```

#### 帧去重算法
```python
def deduplicate_frames(frames, threshold=8):
    """基于感知哈希去除相似帧"""
    import cv2
    import numpy as np
    
    def compute_phash(image_path, hash_size=8):
        """计算感知哈希 (pHash)"""
        img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
        resized = cv2.resize(img, (hash_size + 1, hash_size))
        dct = cv2.dct(np.float32(resized))
        dct_low = dct[:hash_size, :hash_size]
        median = np.median(dct_low)
        return (dct_low > median).flatten()
    
    def hamming_distance(hash1, hash2):
        return int(np.sum(hash1 != hash2))
    
    unique_frames = []
    seen_hashes = []
    
    for frame in frames:
        phash = compute_phash(frame["frame_path"])
        is_duplicate = any(
            hamming_distance(phash, h) < threshold 
            for h in seen_hashes
        )
        if not is_duplicate:
            unique_frames.append(frame)
            seen_hashes.append(phash)
    
    return unique_frames
```

#### 阈值说明
| 参数 | 值 | 说明 |
|------|-----|------|
| `min_brightness` | 3 | 适配深色背景教学视频 |
| `min_sharpness` | 50 | 过滤明显模糊帧 |
| `hash_threshold` | 8 | 汉明距离阈值，越小越严格 |


### 步骤14：AI Vision问答校验
- **类型**：LLM(Vision)

#### 核心动作
通过问答方式校验帧内容是否满足断层补全需求

#### 实现流程
```
1. [输入] 接收有效帧 + 校验问题
2. [Vision] 对每帧调用Vision模型
   - 输入：帧图片 + 校验问题列表
   - 输出：每个问题的回答（是/否）+ 提取的内容
3. [分级] 根据回答结果分级
   - A级：核心问题全"是" + 次要≥50%"是"
   - B级：核心问题全"是" + 次要<50%"是"
   - C级：核心问题全"是" + 次要全"否"
   - 不合格：任一核心问题"否"
4. [输出] 返回合格帧和不合格帧
```

#### 分层校验标准（基于步骤8b的定义）
| 断层类型 | 核心必含项（必须全部满足） | 次要加分项（满足越多等级越高） |
|---------|-------------------------|------------------------------|
| 1.显性指引类 | 指引词指向的核心内容 | 内容的功能描述/关联标注 |
| 2.结论无推导类 | 公式完整形式+至少1个推导关键步 | 推导依据（如"由勾股定理得"） |
| 3.概念无定义类 | 术语名称+核心定义 | 适用场景/典型例子 |
| 4.实操步骤断裂类 | 关键操作界面（按钮/参数位置清晰） | 操作后的正确结果展示 |
| 5.分层分类无内容类 | 所有层级/类别的名称 | 各层级的功能描述/关联关系 |
| 6.量化数据缺失类 | 关键对比数值+对比维度 | 数据结论标注（如"A方案最优"） |
| 7.指代模糊类 | 指代词对应的对象名称+核心特征 | 对象的优势/适用场景 |
| 8.动态过程空白类 | 至少2个关键变化节点+最终结果 | 变化的触发条件 |
| 9.符号编号缺失类 | 编号+对应内容的完整形式 | 编号内容的推导依据/应用场景 |
| 10.对比逻辑缺失类 | 对比对象+至少1个核心差异维度 | 差异点对应的适用场景建议 |

#### 可视化场景校验标准（用于无断层场景）
| 场景类型 | 核心校验项 | 说明 |
|---------|-----------|------|
| 层级/结构类 | 层级关系是否清晰可辨 | 能看出层次嵌套关系 |
| 流程/流转类 | 流转步骤是否完整 | 包含开始、关键节点、结束 |
| 实操/界面类 | 操作位置是否清晰 | 关键按钮/输入框可识别 |
| 对比/差异类 | 对比项是否同时可见 | 至少2个对比对象同框 |
| 复杂逻辑关系类 | 关联关系是否可识别 | 元素间连线/箭头可见 |

#### 输入（增加可视化相关）
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `deduplicated_frames[]` | array | ←步骤13 | 去重后有效帧 |
| `validation_questions[]` | array | ←步骤11 | 校验问题 |
| `expected_visual_forms` | string[] | ←步骤9 | 预期可视化形态（多种候选） |
| `key_elements` | string[] | ←步骤9 | 截图必含元素 |
| `min_completeness` | number | ←步骤7b(透传) | 最低完整度要求，默认0.7 |

#### Vision Prompt
```
请根据以下图片回答问题，并提取相关内容。

【断层类型】{fault_type}

【校验问题】
{questions}

【回答要求】
对每个问题：
1. 回答"是"或"否"
2. 如果回答"是"，提取图中对应的具体内容
3. 如果回答"否"，说明缺失的原因

【输出格式】
{
  "answers": [
    {
      "question_id": "Q1",
      "answer": "是/否",
      "extracted_content": "从图中提取的具体内容",
      "missing_reason": "如果否，说明缺失原因"
    }
  ]
}
```

#### 分级标准
| 等级 | 条件 | 说明 |
|-----|------|------|
| A级 | 核心问题全部"是" + 次要问题≥50%"是" | 优质截图，完全满足补全需求 |
| B级 | 核心问题全部"是" + 次要问题<50%"是" | 合格截图，基本满足补全需求 |
| C级 | 核心问题全部"是" + 次要问题全部"否" | 最低合格，仅满足核心需求 |
| **C+级** | **key_elements完整度≥min_completeness** | **完整度达标采用（断层/可视化通用）** |
| 不合格 | 任一核心问题"否" 且 完整度<min_completeness | 需要重试或人工处理 |

#### 完整度计算
```python
def calculate_completeness(key_elements, extracted_elements):
    """计算key_elements的覆盖完整度"""
    matched = sum(1 for elem in key_elements if elem in extracted_elements)
    return matched / len(key_elements) if key_elements else 1.0

# 判定逻辑
completeness = calculate_completeness(key_elements, extracted_elements)
if completeness >= min_completeness:
    grade = "C+"  # 完整度达标，可采用
```

---

### 步骤15：智能重试（校验失败时）
- **类型**：Tool + LLM

#### 核心动作
对不合格帧，基于历史截图和指令，生成更精确的重试指令

#### 实现流程
```
1. [输入] 接收不合格帧 + 原始指令 + 历史截图信息
2. [分析] LLM分析失败原因
   - 输入：当前截图 + 失败问题 + 历史所有轮次的截图和指令
   - 输出：失败原因分析 + 优化建议
3. [生成] 基于分析结果生成新的截帧指令
   - 调整时间偏移
   - 调整图像增强参数
   - 调整截帧区间
4. [执行] 重新截帧并校验
5. [迭代] 最多重试3次
6. [输出] 返回重试结果
```

#### 重试历史记录结构
```json
{
  "instruction_id": "INS001",
  "retry_history": [
    {
      "round": 1,
      "capture_params": {
        "preferred_sec": 10.5,
        "enhance_params": {...}
      },
      "frame_path": "temp_frames/INS001_r1.png",
      "validation_result": {
        "grade": "不合格",
        "failed_questions": ["Q1"],
        "answers": [...]
      },
      "failure_analysis": "截图时间过早，PPT尚未切换完成"
    },
    {
      "round": 2,
      "capture_params": {
        "preferred_sec": 11.0,
        "enhance_params": {...}
      },
      "frame_path": "temp_frames/INS001_r2.png",
      "validation_result": {...}
    }
  ]
}
```

#### LLM重试分析Prompt
```
请分析截图校验失败的原因，并提供优化建议。

【当前截图信息】
- 截图路径：{current_frame_path}
- 截帧时间：{capture_time}秒
- 失败的校验问题：{failed_questions}
- Vision回答：{answers}

【历史重试记录】
{retry_history}

【原始断层信息】
- 断层类型：{fault_type}
- 断层时间范围：{fault_location}
- 需要补全的内容：{missing_content}

【分析要求】
1. 分析当前截图为什么没有包含所需内容
2. 对比历史截图，找出规律
3. 提供具体的优化建议：
   - 时间调整方向（提前/延后多少秒）
   - 是否需要调整图像增强参数
   - 是否需要扩大/缩小截帧区间

【输出格式】
{
  "failure_reason": "具体失败原因",
  "optimization": {
    "time_adjustment": "+0.5秒/-0.5秒/不变",
    "new_preferred_sec": 11.5,
    "enhance_adjustment": {
      "sharpen": true,
      "contrast_boost": 1.3
    },
    "range_adjustment": "扩大/缩小/不变"
  },
  "confidence": "高/中/低",
  "suggestion": "如果置信度低，建议的替代方案"
}
```

#### 重试策略
```python
def smart_retry(unqualified_frame, original_instruction, retry_history, video_path):
    max_retries = 3
    
    for round_num in range(1, max_retries + 1):
        # 1. LLM分析失败原因并生成优化建议
        analysis = llm_analyze_failure(
            current_frame=unqualified_frame,
            retry_history=retry_history,
            original_instruction=original_instruction
        )
        
        # 2. 根据分析结果调整参数
        new_params = apply_optimization(
            original_instruction["opencv_params"],
            analysis["optimization"]
        )
        
        # 3. 重新截帧
        new_frame = capture_single_frame(video_path, new_params)
        
        # 4. 重新校验
        validation_result = vision_validate(
            new_frame,
            original_instruction["validation_questions"]
        )
        
        # 5. 记录本轮结果
        retry_history.append({
            "round": round_num,
            "capture_params": new_params,
            "frame_path": new_frame["frame_path"],
            "validation_result": validation_result,
            "failure_analysis": analysis["failure_reason"]
        })
        
        # 6. 检查是否成功
        if validation_result["grade"] in ["A", "B", "C"]:
            return {
                "status": "success",
                "final_frame": new_frame,
                "retry_count": round_num
            }
        
        # 7. 如果置信度低，提前终止
        if analysis["confidence"] == "低":
            break
    
    # 重试失败
    return {
        "status": "failed",
        "retry_count": max_retries,
        "best_frame": select_best_failed_frame(retry_history),
        "fallback_action": "使用最佳失败帧/标记人工处理"
    }
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `unqualified_frames[]` | array | ←步骤14 | 不合格帧 |
| `screenshot_instructions[]` | array | ←步骤11 | 原始指令 |
| `retry_history` | object | 累积记录 | 历史重试记录 |
| `video_path` | string | ←步骤1 | 视频路径 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `retry_results[].instruction_id` | string | 调试记录 | 指令ID |
| `retry_results[].retry_count` | number | 调试记录 | 重试次数 |
| `retry_results[].final_status` | string | 流程控制 | success/failed |
| `retry_results[].final_frame` | object | 合并到qualified_frames | 成功的帧 |
| `retry_results[].retry_history` | array | 调试记录 | 完整重试历史 |
| `permanently_failed[]` | array | 人工处理队列 | 永久失败的指令 |

---

### 步骤15b：截图后处理（裁剪）
- **类型**：Tool(PIL)（可配置）

#### 核心动作
**裁剪去冗余**：只保留核心区域，剔除空白边、无关元素

#### 设计理念
> 截图**精准对应文字稿核心节点**，裁剪去冗余聚焦记忆点

#### 配置项
| 配置项 | 默认值 | 说明 |
|-------|-------|------|
| `enable_ai_crop` | `false` | 是否启用AI生成裁剪描述 |
| `enable_fixed_crop` | `false` | 是否启用固定裁剪规则 |

#### 实现流程
```
1. [输入] 接收合格帧 + 可视化场景类型
2. [判断] 检查配置项是否启用裁剪
3. [裁剪] 如启用裁剪
   a. enable_ai_crop=true: LLM分析裁剪区域
   b. enable_fixed_crop=true: 应用固定规则裁剪
   c. PIL执行裁剪
4. [输出] 返回处理后截图（或原图）
```

#### 固定裁剪规则（enable_fixed_crop=true时使用）
| 场景类型 | 裁剪策略 | 说明 |
|---------|---------|------|
| 层级/结构类 | 裁剪边缘10%空白 | 保留结构主体 |
| 流程/流转类 | 裁剪边缘10%空白 | 保留流程主体 |
| 实操/界面类 | 保留完整界面 | 仅裁无关窗口 |
| 对比/差异类 | 中心对齐裁剪 | 保证对比项同框 |
| 复杂逻辑关系类 | 不裁剪 | 保留完整关系图 |

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `qualified_frames[]` | array | ←步骤14/15 | 合格截图 |
| `visualization_candidates[]` | array | ←步骤7b | 可视化场景信息 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `processed_frames[].frame_id` | string | →步骤20,22,24 | 帧ID |
| `processed_frames[].processed_path` | string | →步骤22,24 | 处理后路径（或原路径） |

---

## 六、可视化判定与语义重构

### 步骤16：可视化必要性判定
- **类型**：LLM

#### 核心动作
判断**断层信息**是否需要可视化补充（区别于步骤7b的场景识别）

> **说明**：步骤7b识别"适合可视化"的内容，步骤16判断"断层是否需要可视化补全"

#### 实现流程
```
1. [输入] 接收知识点片段 + 断层信息 + 已有合格帧（含可视化场景截图）
2. [LLM] 针对断层信息判断
   - 断层缺失内容是否需要视觉呈现
   - 已有截图是否已覆盖断层补全需求
   - 评估截图覆盖率
3. [输出] 返回判断结果
```

#### LLM判断Prompt
```
请判断以下知识点片段是否需要可视化补充。

【片段信息】
- 内容摘要：{summary}
- DARPA问题：{darpa_question}
- 语义维度：{semantic_dimension}

【断层信息】
{faults}

【已有截图】
{frames}

【判断依据】
1. 内容复杂度是否需要视觉辅助
2. 是否存在需要补全的断层
3. 已有截图是否足够

【输出】
{
  "need_visualization": true/false,
  "judgment_basis": ["理由1", "理由2"]
}
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `knowledge_segments[]` | array | ←步骤7 | 知识点片段 |
| `semantic_faults[]` | array | ←步骤8b | 断层信息 |
| `qualified_frames[]` | array | ←步骤14/15 | 已有合格截图 |
| `visualization_candidates[]` | array | ←步骤7b | 可视化场景信息 |

---

### 步骤17：可视化形式选择
- **类型**：代码规则

#### 核心动作
根据断层类型和可视化形态选择适配形式

#### 实现流程
```
1. [输入] 接收可视化判断结果 + 断层信息 + 可视化场景
2. [映射] 根据断层类型/scene_type查询映射表
3. [输出] 返回可视化形式
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `visualization_necessity` | object | ←步骤16 | 可视化判断结果 |
| `semantic_faults[]` | array | ←步骤8b | 断层信息 |
| `visualization_candidates[]` | array | ←步骤7b | 可视化场景（含scene_type） |

#### 映射规则（硬编码）
| 断层类型 | 可视化形式 |
|---------|-----------|
| 动态过程空白(8)/实操步骤断裂(4) | **视频片段** |
| 显性指引(1)/概念无定义(3)/分层分类(5)/量化数据(6)/符号编号(9)/对比逻辑(10) | **关键截图** |
| 结论无推导(2)/指代模糊(7) | **视频+截图** |

---

### 步骤18：核心内容判定
- **类型**：LLM

#### 核心动作
识别需保留的核心视频片段类型

#### 实现流程
```
1. [输入] 接收可视化形式 + 知识点片段 + 断层信息
2. [LLM] 判断是否为核心内容
   - 核心知识点可视化
   - 实操连贯步骤
   - 动画展现核心机制
3. [输出] 返回核心内容判定结果
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `visualization_form` | string | ←步骤17 | 可视化形式 |
| `knowledge_segments[]` | array | ←步骤7 | 知识点片段 |
| `semantic_faults[]` | array | ←步骤8b | 断层信息 |

---

### 步骤19：边界精细化检测与辅助信息生成
- **类型**：LLM(Vision) + Tool(OpenCV) + 本地查询

#### 核心动作
1. **边界精细化**：基于SSIM采样和Vision AI多轮迭代，精确判定视频片段的起止边界（Coarse-to-Fine策略）
2. **辅助信息生成**：生成从文字到视频/截图的衔接引导语

#### 实现流程
```
1. [输入] 接收核心内容判定 + 知识点片段 + 断层信息 + 可视化形式
2. [粗定位] 获取初始时间范围（来自字幕或AVP）
3. [精细化] 进入多轮迭代（最大3轮）：
   a. [SSIM采样] 在当前范围内自适应步长采样帧（过滤静态重复帧）
   b. [上下文] 获取时间范围内的字幕文本，构建语义上下文
   c. [Vision判定] 发送帧序列+字幕给Vision AI
      - 判断边界是否准确
      - 返回状态：found / need_resample / need_expand_*
   d. [重采样] 如果 Vision 返回 points，使用 OpenCV 在精确时间点重采样
   e. [更新] 根据 Vision 建议调整时间范围
4. [生成] 基于精细化后的边界，生成衔接引导语
   - video/video_screenshot → 生成 video_transition
   - screenshot/video_screenshot → 生成 screenshot_transition
   - 都生成 post_media_summary
5. [输出] 返回辅助信息 + 精确视频时间范围
```

#### Vision AI 边界判定Prompt
```
请从以下截图序列中，判断"{title}"的视频片段的精确起止边界。

【知识点信息】
- 标题：{title}
{subtitle_section} [字幕上下文]

【提供的帧序列】
- frame_0@10.5s
- frame_1@11.2s
...

【判定标准】
- 动画场景：从画面开始变化的瞬间到画面静止的瞬间
- 实操场景：从鼠标/手部动作开始到动作结束
...

【输出格式】
{
  "boundary_status": "found / need_resample / need_expand_start / need_expand_end",
  "start_frame_label": "frame_X@Y.Ys",
  "end_frame_label": "frame_Z@W.Ws",
  "resample_points": [10.5, 12.0, 13.5] (仅 need_resample 时提供)
}
```

#### LLM 引导语生成Prompt
```
请为以下知识点生成媒体衔接引导语。
... (同原Step 19)
```

---

## 七、语义重构与最终输出

### 步骤20：素材整合
- **类型**：代码规则

#### 核心动作
按知识点整合文字、视频、截图（断层截图 + 可视化场景截图）、辅助信息

#### 实现流程
```
1. [输入] 接收所有上游数据
2. [整合] 按segment_id关联所有素材
   - 文本内容
   - 视频信息
   - 断层截图（来自步骤8a/8b驱动的截帧）
   - 可视化场景截图（来自步骤7b驱动的截帧）
   - 处理后截图（来自步骤15b）
   - 辅助信息
3. [输出] 返回整合后的素材列表
```

#### 代码实现
```python
def integrate_materials(knowledge_segments, core_content_judgment, 
                        fault_frames, visualization_frames,
                        processed_frames, auxiliary_information):
    integrated = []
    
    for segment in knowledge_segments:
        seg_id = segment["segment_id"]
        
        # 关联核心内容判定
        core_judgment = find_by_segment_id(core_content_judgment, seg_id)
        
        # 关联断层截图
        fault_screenshots = filter_by_segment_id(fault_frames, seg_id)
        
        # 关联可视化场景截图（来自步骤7b识别的场景）
        viz_screenshots = filter_by_segment_id(visualization_frames, seg_id)
        
        # 关联处理后截图
        processed = filter_by_segment_id(processed_frames, seg_id)
        
        # 关联辅助信息
        aux_info = find_by_segment_id(auxiliary_information, seg_id)
        
        integrated.append({
            "segment_id": seg_id,
            "text_content": segment["full_text"],
            "video_info": {
                "needed": core_judgment["video_needed"],
                "time_range": aux_info["video_time_range"] if aux_info else None
            },
            "screenshot_info": {
                "fault_screenshots": fault_screenshots,      # 断层补全截图
                "viz_screenshots": viz_screenshots,          # 可视化场景截图
                "processed_screenshots": processed           # 处理后截图
            },
            "auxiliary_info": aux_info
        })
    
    return integrated
```

---

### 步骤21：语义重构（结构化笔记生成）
- **类型**：LLM（三阶段流程）

#### 核心动作
1. 将原文按教学逻辑重新组织（识别前置知识铺垫）
2. 生成层级标识符（总/核心机制/补充机制/支撑知识/分N）
3. 嵌入例子/类比/个人理解到合适位置
4. 在断层位置插入媒体占位符和认知过渡语

#### 核心约束
> **不能修改、删除、增加原文内容**——仅按讲解者的组织方式重组织
> 
> **参考格式**：类似 `发布-订阅架构风格.md` 的教学笔记风格

#### 实现流程（三阶段）
```
【阶段1：句子与时间戳匹配】
1. [输入] 接收知识片段（含full_text、source_sentence_ids）
2. [拆分] 将full_text拆分为句子列表
3. [匹配] 通过source_sentence_ids查询sentence_timestamps.json
4. [输出] 返回带时间戳的句子列表

【阶段2：LLM结构化分析】
1. [输入] 接收带时间戳的句子 + extracted_elements + 断层信息 + 媒体信息
2. [LLM] 结构化分析（核心步骤）
   a. 识别前置知识铺垫（定义、概念、基础知识）
      - 标注为 **支撑知识N：名称**
      - 放在需要用到的主要内容之前
   b. 生成层级标识符（自动推断，不生硬套用）
      - **总**：总体概括
      - **核心机制：名称**：主要机制（包含：作用、解决的问题、引入的新问题）
      - **补充机制：名称**：辅助机制
      - **分N：维度名称**：并列维度
      - **协同组件**：具体实现细节
   c. 嵌入例子/类比/洞察
      - 例子嵌入正文：在解释机制后，用"例子："引入
      - 类比集中在 **记忆串联** 部分：使用"概念 = 生活化类比"格式
      - 个人理解用括号标注：（常见理解：xxx）或（个人洞察：xxx）
   d. 插入媒体占位符
      - 在断层位置或需要可视化的地方插入 {{MEDIA:type:id}}
      - 媒体前添加引导语：> 请观看以下视频片段...
      - 媒体后添加总结语：>  总结要点
   e. 添加认知过渡语
      - 在知识点结束处添加 {{TRANSITION}}
      - 格式：> **认知过渡**：前面的内容总结 + 引出下一个问题
3. [输出] 返回结构化文本（含占位符）

【阶段3：媒体占位符替换】
1. [输入] 接收结构化文本 + 媒体信息
2. [替换] 将 {{MEDIA:type:id}} 替换为Obsidian嵌入代码
   - video_clip → ![[video.mp4#t=start,end]]
   - screenshot → ![[screenshot.png]]\n*截图 @timestamp*
   - video_screenshot → 视频嵌入 + 截图嵌入
3. [生成] 使用LLM生成认知过渡语，替换 {{TRANSITION}}
4. [输出] 返回最终的结构化文本
```

#### 输出格式示例（参考 `发布-订阅架构风格.md`）
```markdown
- 问题：核心机制是什么？
    - **支撑知识1：基础概念**
    - **定义**：...
    - **为什么重要**：...
    - **影响因素**：...
    - **数据参考**：...
    
    - **核心机制：机制名称**
        - **总**：实现XX功能（总体说明）
        - **核心机制：具体机制名称**
            - 作用：中间件将消息写入磁盘
            - 解决的问题：订阅者临时下线导致消息丢失
            - 引入的新问题：如何确认订阅者真的处理了消息？
            - 例子：订单服务每秒1万订单，库存服务只能处理1000个...（嵌入正文）
                - > 请观看以下视频片段，注意观察推导过程...
                - ![[video.mp4#t=20,54]]
                - > 💡 通过上述演示，我们可以看到...
        - **补充机制：辅助机制名称**
            - 作用：订阅者处理完消息后发送ACK
            - 解决的问题：消息持久化了，但不知道订阅者是否成功处理
            - 完整闭环：三个机制协同工作，实现可靠的时间解耦
        - **协同组件**（RabbitMQ三层可靠性保证）：生产者确认 + 消息持久化 + 队列持久化
    - **记忆串联**：
        - 核心机制 = 邮局代收包裹（你不在家时，邮局帮你保管）
        - 补充机制 = 签收回执（确认你收到了）
        - 协同组件 = 快递单号记录（可以随时查询）

> **认知过渡**：核心机制已经明确，但这些机制如何转化为实际的系统价值？
```

#### LLM Prompt（阶段2核心）
```
你是一个专业的教学笔记整理助手，请将以下知识片段按教学逻辑重新组织。

【原文】
{full_text}

【已提取的元素】
- 例子：{examples}
- 类比：{analogies}
- 具象词语：{concrete_words}
- 个人理解：{insights}

【语义信息】
- DARPA问题：{darpa_question}
- 逻辑关系：{logic_relation}
- 层次类型：{hierarchy_type}

【断层信息】
{fault_info}

【媒体信息】
{media_info}

【输出要求】

1. **核心核心要要求：严格遵循 note-format.xml 规范**
   - **格式规则 (Format Rules)**
     - **强制列表标记**：严禁只有缩进没有标记。所有缩进层级必须使用 `-` 或 `1.` 开头。
     - **标题限制**：内部仅使用 `###` (子章节/钩子) 和 `####` (支撑/细节)。严禁使用 `#####`。
     - **禁用表格**：Markdown表格在缩进中渲染不兼容，**绝对禁止使用表格**。如有对比，请使用“维度列表”形式（**维度名**：内容）。
     - **语义标签**：使用明确的语义标签（如 `原因：`、`后果：`、`机制：`、`场景：`），而非泛泛的描述。

2. **逻辑规则 (Logic Rules)**
   - **消灭简单并列**：如果不清楚逻辑关系，必须推断分类维度。
   - **层次清晰**：使用缩进表达逻辑从属关系。

3. **内容规则 (Content Rules)**
   - **加粗规范**：仅加粗定义性核心概念。
   - **完整性**：保留原文所有知识点，不增不删核心信息。

4. **媒体占位符**：
   - 在断层位置或需要可视化的地方插入 {{MEDIA}}
   - **后端会自动替换为带缩进(Tab + -)的媒体块**嵌入到对应的逻辑层级下

5. **认知过渡语**：
   - 在知识点结束处添加 {{TRANSITION}}

【输出格式】
使用Markdown格式，参考以下结构：

- 问题：{darpa_question_name}
    - **支撑知识1：概念名称**（如果需要前置知识）
    - **定义**：...
    - **为什么重要**：...
    
    - **核心机制：机制名称**
        - **总**：总体说明
        - **核心机制：具体机制**
            - 作用：...
            - 解决的问题：...
            - 引入的新问题：...
            - 例子：具体案例（保持原文表述）
                - {{MEDIA:screenshot:001}}
        - **补充机制：辅助机制**
            - ...
        - **协同组件**：具体实现细节
    - **记忆串联**：
        - 核心机制 = 生活化类比
        - 补充机制 = 生活化类比
    
    {{TRANSITION}}
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `integrated_materials[].segment_id` | string | ←步骤20 | 片段ID |
| `integrated_materials[].text_content` | string | ←步骤20 | 完整文本 |
| `knowledge_segments[].extracted_elements` | object | ←步骤7 | 提取的元素 |
| `knowledge_segments[].semantic_dimension` | object | ←步骤7 | 语义维度 |
| `semantic_faults[]` | array | ←步骤8b | 断层信息 |
| `integrated_materials[].video_info` | object | ←步骤20 | 视频信息 |
| `integrated_materials[].screenshot_info` | object | ←步骤20 | 截图信息 |

#### 输出
| 字段 | 类型 | 去向 | 说明 |
|-----|------|------|------|
| `reconstructed_segments[].segment_id` | string | →步骤22 | 片段ID透传 |
| `reconstructed_segments[].structured_text` | string | →步骤22 | 结构化文本（含媒体嵌入） |
| `reconstructed_segments[].transition_text` | string | →步骤22 | 认知过渡语 |

---

### 步骤22：最终Markdown生成（Obsidian兼容）
- **类型**：代码规则 + LLM

#### 核心动作
1. 生成认知地图（文档顶部导航）
2. 按章节组织结构化内容
3. 输出Obsidian兼容的Markdown文件
4. 复制视频和截图到输出目录

#### 实现流程
```
1. [输入] 接收重构素材 + 整合素材 + 视频路径
2. [生成认知地图] 使用LLM生成文档顶部的认知地图
   - 按DARPA问题分组，概括各问题的核心内容
   - 生成章节结构流程图
3. [组织章节] 按DARPA问题分组，生成章节标题
   - 格式：### 一、核心定义与价值：时空解耦的本质
   - 每个章节包含多个知识点
4. [创建目录] 创建 notes/ 目录
5. [复制媒体] 复制视频和截图到 notes/ 目录
6. [生成Markdown] 按Obsidian格式生成Markdown
   - 认知地图（顶部）
   - 章节标题 + 结构化内容
   - 认知过渡语（章节之间）
   - 视频嵌入：![[video.mp4#t=start,end]]
   - 截图嵌入：![[screenshot.png]]
7. [命名] 文件名使用主题：{main_topic}.md
8. [输出] 返回Markdown文件路径
```

#### 认知地图生成Prompt
```
请为以下知识点生成认知地图，用于文档顶部导航。
... (同上)
```

... (输出目录结构 同上)

... (Obsidian Markdown模板 同上)

... (完整示例 同上)

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `reconstructed_segments[]` | array | ←步骤21 | 重构后的片段 |
| `main_topic` | string | ←步骤1 | 核心主题 |
| `domain` | string | ←步骤1 | 视频领域 |
| `video_path` | string | ←步骤1 | 视频路径 |

#### 输出
| 字段 | 类型 | 说明 |
|-----|------|------|
| `markdown_file_path` | string | 生成的Markdown文件路径 |
| `notes_directory` | string | notes目录路径 |

---

### 步骤22b：可视化决策总结（新增）
- **类型**：代码规则
- **执行时机**：步骤22后，步骤23前

#### 核心动作
生成可视化选择的详细报告，记录截图和视频片段的选择原因，用于质量监控和追溯。

#### 报告内容
1. **总体统计**：知识点数、截图数、视频数
2. **明细记录**（按知识点）：
   - **截图选择**：文件名、时间点、断层类型、缺失内容、采样策略、质量等级
   - **视频片段**：时间范围、核心内容判定类型、原因、包含的断层类型

#### 输出格式 (visualization_summary.md)
```markdown
# 可视化决策总结

## 总体统计
- 知识点数量: 5
- 截图总数: 12
- 视频片段数: 2

## 1. 顺序查找算法
**可视化形式**: `video_screenshot`

### 📸 截图选择
#### 截图 1: F_INS001_0.png
- **时间点**: 10.5s
- **断层类型**: 结论无推导类 (Type 2)
- **缺失内容**: 需要补充公式推导过程
- **采样策略**: 过程公式，锚点：结论前1-3秒，多帧采样
- **质量等级**: A

### 🎬 视频片段选择
**时间范围**: 10.0s - 35.0s (25.0s)

**选择原因**:
- **核心内容判定**: 核心知识点可视化
- **断层类型**: 结论无推导类, 动态过程空白类

**边界检测过程**:
- 最终边界: 10.0s - 35.0s
- 经过 Vision AI 边界精细化
```

---

## 八、归档规范执行

### 步骤23：视频片段命名
- **类型**：Tool(FFmpeg) + 本地查询

#### 核心动作
按规范命名核心视频片段

#### 实现流程
```
1. [输入] 接收核心内容判定 + 知识点片段 + 辅助信息
2. [查询] 从本地存储获取段落时间戳
3. [截取] 使用FFmpeg截取视频片段
4. [命名] 按规范命名：{label}-{core_type}-{timestamp}.mp4
5. [输出] 返回命名后的视频列表
```

#### FFmpeg命令
```bash
ffmpeg -i {video_path} -ss {start_sec} -to {end_sec} -c copy {output_path}
```

---

### 步骤24：截图文件命名
- **类型**：代码规则

#### 核心动作
按规范命名合格截图文件，关联文字稿知识点

#### 命名格式
```
{knowledge_point}_{scene_type}_{sequence}.png
```

**示例**：
- `快速排序_流程流转类_1.png`
- `分布式锁_层级结构类_1.png`
- `CRUD操作_实操界面类_2.png`

#### 实现流程
```
1. [输入] 接收处理后截图 + 可视化场景信息 + 知识点片段
2. [命名] 按规范命名：{knowledge_point}_{scene_type}_{sequence}.png
3. [复制] 将处理后截图复制到最终目录并重命名
4. [输出] 返回命名后的截图列表
```

#### 输入
| 字段 | 类型 | 来源 | 说明 |
|-----|------|------|------|
| `processed_frames[]` | array | ←步骤15b | 处理后截图 |
| `visualization_candidates[]` | array | ←步骤7b | 可视化场景信息 |
| `knowledge_segments[]` | array | ←步骤7 | 知识点信息 |

---

## 九、步骤总览

| 阶段 | 步骤号 | 步骤名称 | 类型 | 核心动作 |
|-----|-------|---------|------|---------|
| 前期准备 | 1 | 原材料确认与主题识别 | Tool+LLM | 校验文件+推断主题 |
| 文字稿预处理 | 2 | 智能纠错 | LLM | 修正ASR识别误差 |
| | 3 | 自然语义合并 | LLM | 碎片句合并为完整句 |
| | 4 | 局部冗余删除 | LLM+存储 | 清理单句冗余+存储时间戳 |
| | 5 | 跨句冗余删除 | LLM | 删除重复句/离题句 |
| | 6 | 跨句冗余合并 | LLM | 合并语义重叠内容 |
| 分片与断层 | 7 | 知识点分片 | LLM+存储 | DARPA八问细粒度分片 |
| | **7b** | **可视化场景识别** | **LLM** | **识别5类可视化场景** |
| | 8a | 断层分类粗定位 | LLM | 识别10类断层 |
| | 8b | 断层精确定位 | LLM+查询 | 精确时间+缺失分析 |
| 截图指令 | 9 | 截帧策略匹配 | 代码规则 | 断层+可视化→策略映射 |
| | 10 | 截帧时间计算 | 代码规则 | 计算精确截帧时间 |
| | 11 | JSON指令生成 | 代码规则 | 生成截帧任务包 |
| 截帧执行 | 12 | 截帧执行 | Tool(OpenCV) | 执行截帧 |
| | 13 | 帧基础校验 | Tool(OpenCV) | 过滤黑屏/模糊帧 |
| | 14 | Vision问答校验 | LLM(Vision) | 问答式内容+形态校验 |
| | 15 | 智能重试 | Tool+LLM | 基于历史的智能重试 |
| | **15b** | **截图后处理** | **Tool(PIL)+LLM** | **裁剪去冗余+极简标注** |
| 可视化判定 | 16 | 可视化必要性 | LLM | 判断是否需要可视化 |
| | 17 | 可视化形式选择 | 代码规则 | 选择视频/截图/混合 |
| | 18 | 核心内容判定 | LLM | 判断核心内容类型 |
| | 19 | 辅助信息生成 | LLM+查询 | 生成学习辅助信息 |
| 输出生成 | 20 | 素材整合 | 代码规则 | 整合所有素材 |
| | 21 | 语义重构 | LLM | 融合截图内容重构原文 |
| | 22 | Markdown生成 | 代码规则 | 生成最终文档 |
| 归档 | 23 | 视频片段命名 | Tool(FFmpeg) | 截取并命名视频 |
| | 24 | 截图文件命名 | 代码规则 | 知识点+场景类型命名 |

---

## 十、字段全链路追踪表

### 核心ID字段
| 字段名 | 产生步骤 | 使用步骤 | 说明 |
|-------|---------|---------|------|
| `subtitle_id` | 原始字幕 | 2,3 | 字幕唯一标识 |
| `sentence_id` | 3 | 4,5,6,7,8a,8b | 句子唯一标识 |
| `paragraph_id` | 6 | 7 | 段落唯一标识 |
| `segment_id` | 7 | 8a-24(全链路) | 知识点片段唯一标识 |
| `fault_id` | 8a | 8b-15,24 | 断层唯一标识 |
| `instruction_id` | 11 | 12-15 | 截帧指令唯一标识 |
| `frame_id` | 12 | 13-24 | 帧唯一标识 |

### 时间戳字段
| 字段名 | 产生步骤 | 存储位置 | 查询步骤 |
|-------|---------|---------|---------|
| `sentence_timestamps` | 4 | 本地存储 | 8b,9-11 |
| `segment_timestamps` | 7 | 本地存储 | 19,23 |
| `fault_location` | 8b | 传递 | 9,10 |
| `video_time_range` | 19 | 传递 | 20,23 |

### 语义字段
| 字段名 | 产生步骤 | 使用步骤 | 说明 |
|-------|---------|---------|------|
| `domain` | 1 | 2 | 视频领域 |
| `main_topic` | 1 | 5 | 核心主题 |
| `knowledge_point` | 7 | 8a,23,24 | 知识点名称 |
| `darpa_question` | 7 | 8a,16,17,18 | DARPA问题编号 |
| `semantic_dimension` | 7 | 8a,16,17,21 | 语义维度 |
| `extracted_elements` | 7 | 21 | 提取的例子/类比等 |
| `core_semantic` | 7 | 16,23,24 | 摘要和标签 |
| `missing_content` | 8b | 11,15,21 | 缺失内容 |
| `extracted_content` | 14 | 15,21 | 截图提取内容 |
| `retry_history` | 15 | 15(累积) | 重试历史记录 |

### 可视化字段（新增）
| 字段名 | 产生步骤 | 使用步骤 | 说明 |
|-------|---------|---------|------|
| `visualization_candidates` | 7b | 9,10,11,14,15b,16,17,24 | 可视化候选列表 |
| `scene_type` | 7b | 9,15b,17,24 | 5种可视化场景类型 |
| `expected_visual_forms` | 7b | 9,14 | 预期可视化形态（复数） |
| `key_elements` | 7b | 11,14,15b | 截图必含元素 |
| `min_completeness` | 7b | 9,14 | 最低完整度要求 |
| `processed_frames` | 15b | 20,22,24 | 处理后截图 |
你是一个基于【第一性原理】的 AI 知识架构师。你的任务是透过表面的关键词（形式），洞察字幕产生的根本动机（语义）。

## 核心原则：去形式化，重语义
⚠️ **严禁** 仅凭“点击”、“因为”、“首先”等关键词进行机械分类。
✅ **必须** 结合上下文，问自己：如果把这句话删掉，用户失去的是什么？（是失去了一个概念？失去了一个操作步骤？还是失去了一个逻辑证明？）
1.如果是非讲解型的片段，就输出建议的视频截取的范围，范围截取的依据是不同类型的片段的本质目的
2.对于所有片段，提出建议截取的具象性知识：
判定当前截图是否包含**具象性知识**：

**阳性（存在具象性知识）** - 满足任一条件：
- 截图中存在和教学知识点强相关的：实物照片、标本图、实验装置、解剖图、结构图、地图、实操界面、具体事物示意图，抽象框图、逻辑流程图、思维导图等
- 截图中存在**数学公式**（包括方程式、推导过程、符号表达式等）
- 该图形是用于讲解知识点的功能性元素，非装饰、水印、花边、无关插画
- 图形能明确对应现实中的具体事物、现象、操作步骤、数学关系
- 截图是否存在图表显示数据或者变化
- 截图是否能帮助初学者直观认知某个事物的视觉形式
- 截图是否能作为记忆点，用于学习者后续回顾和复习
**阴性（不存在具象性知识）**：
- 纯文字、无功能性图形
- 仅有装饰图片，无教学用具象图形或数学公式
- 仅仅是讲解者的人物图片

## 一、五大本质公理 (Mutually Exclusive)

1. **【讲解型】 (Explanation)**
   - **失去它，用户失去了什么**：失去对事物定义的知晓，或看不到最终效果。
   - **本质**：静态信息传递、概念定义、或 **最终效果展示 (Demo)**。
   - **陷阱**：如果不涉及具体“怎么做”或“为什么”，仅仅是“看那里”，就是讲解型。

2. **【环境配置】 (Configuration)**
   - **失去它，用户失去了什么**：无法搭建起程序运行的舞台。
   - **本质**：对依赖、参数、系统的设置。
   - **特征**：对象通常是静态的文件、变量或系统服务。

3. **【过程性知识】 (Process)**
   - **失去它，用户失去了什么**：搞不懂事物内部是如何流转/运作的。
   - **本质**：揭示 **机制、算法或逻辑的动态执行流**。
   - **辨析**：它描述客观规律（如“数据包会经过路由器...”），而非主观操作（如“我去点击路由器的开关...”）。

4. **【实操】 (Practical)**
   - **失去它，用户失去了什么**：无法复刻具体的交互动作。
   - **本质**：人与计算机的直接交互指令集。
   - **特征**：必须包含明确的动作施加者（人）和操作对象。

5. **【推演】 (Deduction)**
   - **失去它，用户失去了什么**：只知其然，不知其所以然。
   - **本质**：逻辑闭环的构建、设计哲学的论证。
   - **辨析**：如果是在解释“为什么要这样设计”或“导致Bug的根本原因”，就是推演。

## 二、认知优先级 (Cognitive Hierarchy)
当内容混合时，按认知价值排序：
**推演 (Why) > 实操/配置 (How to do) > 过程性知识 (How it works) > 讲解型 (What is it)**

    ## 输出格式 (JSON Array)
[ { "id": 0, "knowledge_type": "讲解型", "confidence": 0.92, "reasoning": "核心是静态传递Clawdbot的定义和最终效果，通过网站界面和功能描述展示其作为'7x24小时待命的AI agent'的核心特性，未涉及具体操作步骤或设计原理。", "key_evidence": "一个可以7成24小时待命，可以使用聊天工具去操控的AI agent", "clip_start_sec": 12.2, "clip_end_sec": 18.2, "suggested_screenshoot_timestamps": [12.2, 15.2] }, { "id": 1, "knowledge_type": "推演", "confidence": 0.88, "reasoning": "通过作者创业故事构建逻辑闭环，解释'为什么开发Clawdbot'——从财富自由后的空虚感推导出'写代码'动机，最终引出项目诞生的必然性，属于设计哲学论证。", "key_evidence": "这腐化的生活太让人空虚了，我要写代码", "clip_start_sec": 55.2, "clip_end_sec": 61.2, "suggested_screenshoot_timestamps": [57.2] }, { "id": 2, "knowledge_type": "环境配置", "confidence": 0.95, "reasoning": "聚焦系统参数设置，详细说明安装脚本的执行逻辑、模型供应商选择及认证方式配置，对象是静态的配置文件和系统服务，符合'搭建运行舞台'的本质。", "key_evidence": "推荐使用Quickstart模式，然后呢选择一个大模型的供应商", "clip_start_sec": 123.2, "clip_end_sec": 148.2, "suggested_screenshoot_timestamps": [125.2, 135.2, 145.2] }, { "id": 3, "knowledge_type": "实操", "confidence": 0.97, "reasoning": "提供可复刻的终端操作指令（curl命令+参数配置），包含明确的动作施加者（用户）和操作对象（安装脚本），用户可直接执行复现安装过程。", "key_evidence": "curl -fsSL https://clawd.bot/install.sh | bash", "clip_start_sec": 115.2, "clip_end_sec": 122.2, "suggested_screenshoot_timestamps": [116.2, 120.2] }, { "id": 4, "knowledge_type": "过程性知识", "confidence": 0.85, "reasoning": "揭示架构内部动态流转机制，描述'Gateway Daemon统一管理会话→Agent Runtime调用模型→Channel执行操作'的客观数据流，而非主观操作步骤。", "key_evidence": "最外层呢是各种的接入，比如呢像各种的channel，还有呢TUI，Web UI或者是Mac的APP", "clip_start_sec": 415.2, "clip_end_sec": 462.2, "suggested_screenshoot_timestamps": [418.2, 425.2, 435.2, 450.2] }, { "id": 5, "knowledge_type": "推演", "confidence": 0.9, "reasoning": "论证Clawdbot的核心价值逻辑：从'7x24小时服务'概念出发，推导长期记忆机制如何主动形成任务，构建'AI持续工作+用户偏好积累'的闭环设计哲学。", "key_evidence": "Clawdbot强调这种服务的概念，让AI呢7*24小时的工作", "clip_start_sec": 488.2, "clip_end_sec": 517.2, "suggested_screenshoot_timestamps": [490.2, 505.2, 510.2] } ]