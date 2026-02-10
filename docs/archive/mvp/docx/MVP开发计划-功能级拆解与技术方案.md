# MVP开发计划：视频转图文并茂Markdown笔记（功能级拆解与技术方案）

## 整体目标
在1-2周内完成端到端功能验证，生成一份包含文字+截图+视频片段的完整笔记，自己觉得"比纯文字稿好用"。

## 核心原则
- **快速验证优先**：先跑通流程，再优化质量
- **功能可独立验证**：每个模块都有明确的输入/输出和验证标准
- **代码可复用**：相同逻辑（如帧差异率计算）封装为公共函数

---

## 模块拆解与技术方案

### 模块0：环境准备与依赖安装（预计时间：0.5天）

#### 功能0.1：Python环境配置
**任务描述**：配置Python开发环境，安装所有依赖库

**技术方案**：
```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装核心依赖
pip install opencv-python==4.8.0  # 视频处理
pip install pillow==10.0.0  # 图像处理
pip install openai==1.0.0  # LLM API调用
pip install python-dotenv==1.0.0  # 环境变量管理
pip install pytesseract==0.3.10  # OCR（可选）
```

**验证标准**：
- [ ] 所有库导入无报错：`python -c "import cv2, PIL, openai"`
- [ ] OpenCV版本正确：`cv2.__version__ == '4.8.0'`

**输入**：无
**输出**：可用的Python环境

---

### 模块1：断层数据加载（已有，优化接口）（预计时间：0.5天）

#### 功能1.1：解析deepseek的断层识别结果
**任务描述**：读取deepseek输出的断层JSON，提取必要字段

**技术方案**：
```python
import json

def load_fault_data(json_path):
    """
    输入：断层识别结果JSON文件路径
    输出：标准化的断层列表
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    faults = []
    for item in data['faults']:
        fault = {
            'fault_id': item['id'],
            'fault_text': item['text'],  # 断层的文字描述
            'timestamp_start': item['start_time'],  # 秒
            'timestamp_end': item['end_time'],  # 秒
            'fault_type': item['type'],  # 1/2/3类断层
            'context_before': item['context_before'],  # 前3句
            'context_after': item['context_after']  # 后3句
        }
        faults.append(fault)
    return faults
```

**验证标准**：
- [ ] 能正确解析测试JSON（字段完整、类型正确）
- [ ] 时间戳数据类型为float，范围在0-视频时长内

**输入**：deepseek输出的JSON文件
**输出**：Python字典列表

---

### 模块2：LLM断层补全方式判断（预计时间：1天）

#### 功能2.1：设计Prompt模板
**任务描述**：根据断层类型和上下文，设计LLM Prompt

**技术方案**：
```python
def build_fault_prompt(fault):
    """
    根据断层数据构建LLM Prompt
    """
    prompt = f"""你是知识笔记生成助手。请分析以下视频文字稿的断层，判断最佳补全方式。

【断层类型】
{_get_fault_type_desc(fault['fault_type'])}

【断层文字】
{fault['fault_text']}

【上下文】
前文：{fault['context_before']}
后文：{fault['context_after']}

【任务】
1. 判断该断层应该用哪种方式补全（文字/截图/视频片段）
2. 给出判断的置信度（0-1之间）
3. 简要说明判断理由

【输出格式（严格JSON）】
{{
  "补全方式": "文字" | "截图" | "视频片段",
  "置信度": 0.85,
  "理由": "该断层需要展示空间结构..."
}}
"""
    return prompt

def _get_fault_type_desc(fault_type):
    type_map = {
        1: "第1类：无法根据上下文补全，需要视觉信息",
        2: "第2类：可以上下文补全，但可视化效果更好",
        3: "第3类：可以上下文补全，且可视化无意义"
    }
    return type_map.get(fault_type, "未知类型")
```

**验证标准**：
- [ ] Prompt包含所有必要信息（断层类型、文字、上下文）
- [ ] 输出格式明确，要求返回JSON

**输入**：断层字典
**输出**：Prompt字符串

---

#### 功能2.2：调用LLM API
**任务描述**：调用deepseek/GPT-4 API，获取判断结果

**技术方案**：
```python
import os
from openai import OpenAI

def call_llm_api(prompt, model="deepseek-chat"):
    """
    调用LLM API，返回补全方式判断
    """
    client = OpenAI(
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        base_url="https://api.deepseek.com"
    )
    
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "你是知识笔记生成助手，专注于判断视频断层的补全方式。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3,  # 降低随机性，提高一致性
        response_format={"type": "json_object"}  # 强制JSON输出
    )
    
    return response.choices[0].message.content
```

**验证标准**：
- [ ] API调用成功，无报错
- [ ] 返回结果为有效JSON字符串

**输入**：Prompt字符串
**输出**：LLM返回的JSON字符串

---

#### 功能2.3：解析LLM返回结果
**任务描述**：解析JSON，提取补全方式和置信度

**技术方案**：
```python
import json

def parse_llm_response(response_text):
    """
    解析LLM返回的JSON，提取关键信息
    """
    try:
        data = json.loads(response_text)
        return {
            'method': data['补全方式'],  # "文字"/"截图"/"视频片段"
            'confidence': float(data['置信度']),
            'reason': data.get('理由', '')
        }
    except (json.JSONDecodeError, KeyError) as e:
        # 容错：LLM返回格式不对，默认使用文字补全
        return {
            'method': '文字',
            'confidence': 0.5,
            'reason': f'解析失败，使用默认方式: {str(e)}'
        }
```

**验证标准**：
- [ ] 能正确解析标准格式的JSON
- [ ] 解析失败时有兜底逻辑，不会崩溃
- [ ] 置信度在0-1范围内

**输入**：LLM返回的JSON字符串
**输出**：标准化的补全方式字典

---

#### 模块2整体验证
**验证数据**：准备3个测试断层（对应文字/截图/视频三种类型）

**验证标准**：
- [ ] 对3个测试断层的判断结果，至少2个与预期一致（准确率≥67%）
- [ ] 所有断层都返回有效结果（无崩溃）

---

### 模块3：视频素材提取（预计时间：2-3天）

#### 功能3.1：视频读取与帧提取
**任务描述**：根据时间戳范围，提取视频帧序列

**技术方案**：
```python
import cv2
import numpy as np

def extract_frames(video_path, start_time, end_time, fps=30):
    """
    提取指定时间范围内的所有帧
    
    参数：
    - video_path: 视频文件路径
    - start_time: 起始时间（秒）
    - end_time: 结束时间（秒）
    - fps: 帧率（默认30fps）
    
    返回：
    - frames: 帧列表，每个元素是numpy数组
    - frame_times: 每帧对应的时间戳列表
    """
    cap = cv2.VideoCapture(video_path)
    video_fps = cap.get(cv2.CAP_PROP_FPS)
    
    start_frame = int(start_time * video_fps)
    end_frame = int(end_time * video_fps)
    
    frames = []
    frame_times = []
    
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    
    for frame_idx in range(start_frame, end_frame):
        ret, frame = cap.read()
        if not ret:
            break
        frames.append(frame)
        frame_times.append(frame_idx / video_fps)
    
    cap.release()
    return frames, frame_times
```

**验证标准**：
- [ ] 提取的帧数 = (end_time - start_time) * fps
- [ ] 每帧的shape为(height, width, 3)
- [ ] frame_times的时间戳准确（误差<0.1秒）

**输入**：视频路径、时间范围
**输出**：帧列表、时间戳列表

---

#### 功能3.2：计算帧差异率（核心公共函数）
**任务描述**：计算相邻帧的MSE，用于后续静态/动态判断

**技术方案**：
```python
def calculate_frame_diff(frame1, frame2):
    """
    计算两帧之间的均方误差（MSE）
    
    返回：
    - mse: 均方误差值
    - diff_rate: 差异率百分比（0-100）
    """
    # 转为灰度图，减少计算量
    gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY)
    
    mse = np.mean((gray1.astype(float) - gray2.astype(float)) ** 2)
    
    # 差异率 = MSE / 最大可能MSE * 100
    # 最大MSE = 255^2 = 65025（黑到白）
    diff_rate = (mse / 65025) * 100
    
    return mse, diff_rate

def calculate_all_diffs(frames):
    """
    计算帧序列中所有相邻帧的差异率
    
    返回：
    - mse_list: MSE值列表（长度=len(frames)-1）
    - diff_rate_list: 差异率列表
    """
    mse_list = []
    diff_rate_list = []
    
    for i in range(len(frames) - 1):
        mse, diff_rate = calculate_frame_diff(frames[i], frames[i+1])
        mse_list.append(mse)
        diff_rate_list.append(diff_rate)
    
    return mse_list, diff_rate_list
```

**验证标准**：
- [ ] 同一帧与自身的MSE=0
- [ ] 差异率在0-100范围内
- [ ] 列表长度 = len(frames) - 1

**输入**：帧列表
**输出**：MSE列表、差异率列表

---

#### 功能3.3：截图选择——画面稳定性评分
**任务描述**：根据MSE计算每帧的稳定性得分（S1）

**技术方案**：
```python
def score_stability(mse_list, frame_idx, threshold_stable=100, threshold_unstable=300):
    """
    计算指定帧的稳定性得分（0-10分）
    
    参数：
    - mse_list: 所有帧的MSE列表
    - frame_idx: 要评分的帧索引
    - threshold_stable: 稳定帧MSE阈值（默认100）
    - threshold_unstable: 不稳定帧MSE阈值（默认300）
    
    返回：
    - score: 稳定性得分（0-10）
    """
    if frame_idx == 0:
        mse = mse_list[0]
    elif frame_idx >= len(mse_list):
        mse = mse_list[-1]
    else:
        mse = mse_list[frame_idx]
    
    # 基础分
    if mse <= threshold_stable:
        base_score = 10
    elif mse <= threshold_unstable:
        base_score = 5
    else:
        base_score = 0
    
    # 区间加分：统计该帧前后的连续稳定帧数
    stable_count = 1
    # 向前统计
    for i in range(frame_idx - 1, -1, -1):
        if i < len(mse_list) and mse_list[i] <= threshold_stable:
            stable_count += 1
        else:
            break
    # 向后统计
    for i in range(frame_idx, len(mse_list)):
        if mse_list[i] <= threshold_stable:
            stable_count += 1
        else:
            break
    
    # 连续稳定15帧（0.5秒@30fps）加1分，30帧加2分
    if stable_count >= 30:
        bonus = 2
    elif stable_count >= 15:
        bonus = 1
    else:
        bonus = 0
    
    return min(10, base_score + bonus)
```

**验证标准**：
- [ ] 稳定帧（MSE<100）得分≥8
- [ ] 不稳定帧（MSE>300）得分≤3
- [ ] 得分范围在0-10

**输入**：MSE列表、帧索引
**输出**：稳定性得分（0-10）

---

#### 功能3.4：截图选择——无遮挡评分
**任务描述**：检测画面中心区域是否有鼠标/弹窗遮挡

**技术方案**：
```python
def score_no_occlusion(frame, core_region_ratio=0.6):
    """
    计算无遮挡得分（0-10分）
    
    参数：
    - frame: 输入帧（numpy数组）
    - core_region_ratio: 核心区域占比（默认0.6，即60%）
    
    返回：
    - score: 无遮挡得分（0-10）
    """
    h, w = frame.shape[:2]
    
    # 定义核心区域（中间60%x60%）
    core_h_start = int(h * (1 - core_region_ratio) / 2)
    core_h_end = int(h * (1 + core_region_ratio) / 2)
    core_w_start = int(w * (1 - core_region_ratio) / 2)
    core_w_end = int(w * (1 + core_region_ratio) / 2)
    
    core_region = frame[core_h_start:core_h_end, core_w_start:core_w_end]
    
    score = 10  # 默认满分
    
    # 检测1：鼠标光标（亮色小连通域）
    gray = cv2.cvtColor(core_region, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY)
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    for contour in contours:
        area = cv2.contourArea(contour)
        if area < 50:  # 小连通域，可能是鼠标
            score -= 5
            break
    
    # 检测2：弹窗（大矩形边框）
    edges = cv2.Canny(gray, 50, 150)
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    core_area = core_region.shape[0] * core_region.shape[1]
    for contour in contours:
        area = cv2.contourArea(contour)
        if area > core_area * 0.1:  # 面积>核心区域10%
            score -= 10
            break
    
    return max(0, score)
```

**验证标准**：
- [ ] 干净画面得分=10
- [ ] 有鼠标的画面得分≤5
- [ ] 有弹窗的画面得分=0

**输入**：单帧图像
**输出**：无遮挡得分（0-10）

---

#### 功能3.5：截图选择——加权评分与最优帧选择
**任务描述**：综合S1、S4得分，选择最优帧

**技术方案**：
```python
def select_best_frame_for_screenshot(frames, mse_list):
    """
    从帧序列中选择最适合作为截图的帧
    
    参数：
    - frames: 帧列表
    - mse_list: MSE列表
    
    返回：
    - best_frame_idx: 最优帧索引
    - best_frame: 最优帧图像
    - score_details: 评分明细
    """
    scores = []
    
    for idx, frame in enumerate(frames):
        s1 = score_stability(mse_list, idx)
        s4 = score_no_occlusion(frame)
        
        # 加权总分（稳定性40% + 无遮挡40%）
        # MVP阶段暂不启用信息密度（S2）
        total_score = s1 * 0.5 + s4 * 0.5
        
        scores.append({
            'frame_idx': idx,
            'stability': s1,
            'no_occlusion': s4,
            'total': total_score
        })
    
    # 选择总分最高的帧
    best = max(scores, key=lambda x: x['total'])
    
    return best['frame_idx'], frames[best['frame_idx']], best
```

**验证标准**：
- [ ] 返回的帧索引在有效范围内
- [ ] 总分≥8的帧优先被选中
- [ ] 分数明细包含s1、s4、total字段

**输入**：帧列表、MSE列表
**输出**：最优帧索引、最优帧图像、评分明细

---

#### 功能3.6：保存截图
**任务描述**：将选中的帧保存为PNG文件

**技术方案**：
```python
import os
from datetime import datetime

def save_screenshot(frame, fault_id, output_dir='./output/screenshots'):
    """
    保存截图到指定目录
    
    参数：
    - frame: 帧图像
    - fault_id: 断层ID（用于命名）
    - output_dir: 输出目录
    
    返回：
    - file_path: 保存的文件路径
    """
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"screenshot_{fault_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    file_path = os.path.join(output_dir, filename)
    
    cv2.imwrite(file_path, frame)
    
    return file_path
```

**验证标准**：
- [ ] 文件成功保存，路径存在
- [ ] 图片可正常打开，分辨率与原帧一致

**输入**：帧图像、断层ID
**输出**：文件路径

---

#### 功能3.7：视频片段截取——动作起止点识别
**任务描述**：基于MSE突变，识别动作的起点和终点

**技术方案**：
```python
def detect_action_boundaries(mse_list, frame_times, threshold_change=2.0):
    """
    检测动作起止点
    
    参数：
    - mse_list: MSE列表
    - frame_times: 时间戳列表
    - threshold_change: 突变阈值（后帧MSE/前帧MSE >= threshold_change）
    
    返回：
    - start_time: 动作起始时间（秒）
    - end_time: 动作结束时间（秒）
    """
    # 检测起点：MSE从<100突变到>300
    start_idx = None
    for i in range(len(mse_list) - 2):
        if mse_list[i] < 100 and mse_list[i+1] > 300:
            if mse_list[i+1] / max(mse_list[i], 1) >= threshold_change:
                start_idx = i + 1
                break
    
    # 检测终点：MSE从>300回落到<100并稳定
    end_idx = None
    if start_idx:
        for i in range(start_idx, len(mse_list) - 15):  # 至少稳定15帧
            if mse_list[i] > 300 and mse_list[i+1] < 100:
                # 验证后续15帧都稳定
                is_stable = all(mse < 100 for mse in mse_list[i+1:i+16])
                if is_stable:
                    end_idx = i + 1
                    break
    
    # 转换为时间戳
    if start_idx and end_idx:
        return frame_times[start_idx], frame_times[end_idx]
    else:
        # 未检测到明显动作，返回None
        return None, None
```

**验证标准**：
- [ ] 对有明显动作的视频，能检测到起止点
- [ ] 起止时间间隔在2-8秒范围内
- [ ] 对静态画面，返回None

**输入**：MSE列表、时间戳列表
**输出**：起始时间、结束时间

---

#### 功能3.8：视频片段截取——动态区间扩展
**任务描述**：在未检测到完整动作时，动态扩展搜索范围

**技术方案**：
```python
def extend_search_range(video_path, initial_start, initial_end, max_extend=2.0):
    """
    动态扩展搜索范围，寻找完整动作
    
    参数：
    - video_path: 视频路径
    - initial_start: 初始起始时间
    - initial_end: 初始结束时间
    - max_extend: 最大扩展时长（秒）
    
    返回：
    - extended_start: 扩展后起始时间
    - extended_end: 扩展后结束时间
    """
    # 向前扩展
    extended_start = max(0, initial_start - max_extend)
    frames_before, times_before = extract_frames(video_path, extended_start, initial_start)
    
    if frames_before:
        mse_before, _ = calculate_all_diffs(frames_before)
        # 找到第一个稳定区间
        for i in range(len(mse_before)):
            if all(mse < 100 for mse in mse_before[i:i+3]):
                extended_start = times_before[i]
                break
    
    # 向后扩展
    extended_end = initial_end + max_extend
    frames_after, times_after = extract_frames(video_path, initial_end, extended_end)
    
    if frames_after:
        mse_after, _ = calculate_all_diffs(frames_after)
        # 找到第一个稳定区间
        for i in range(len(mse_after)):
            if all(mse < 100 for mse in mse_after[i:i+3]):
                extended_end = times_after[i]
                break
    
    return extended_start, extended_end
```

**验证标准**：
- [ ] 扩展范围≤max_extend
- [ ] 扩展后的时间范围≥原范围

**输入**：视频路径、初始时间范围
**输出**：扩展后时间范围

---

#### 功能3.9：保存视频片段
**任务描述**：从原视频中截取指定时间段，保存为MP4

**技术方案**：
```python
def save_video_clip(video_path, start_time, end_time, fault_id, output_dir='./output/video_clips'):
    """
    截取并保存视频片段
    
    参数：
    - video_path: 原视频路径
    - start_time: 起始时间（秒）
    - end_time: 结束时间（秒）
    - fault_id: 断层ID
    - output_dir: 输出目录
    
    返回：
    - file_path: 保存的文件路径
    """
    import subprocess
    
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"clip_{fault_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
    file_path = os.path.join(output_dir, filename)
    
    # 使用ffmpeg截取视频（比OpenCV更快更可靠）
    cmd = [
        'ffmpeg',
        '-i', video_path,
        '-ss', str(start_time),
        '-to', str(end_time),
        '-c', 'copy',  # 直接复制流，不重新编码
        '-y',  # 覆盖已存在文件
        file_path
    ]
    
    subprocess.run(cmd, check=True, capture_output=True)
    
    return file_path
```

**验证标准**：
- [ ] 文件成功保存
- [ ] 视频时长 = end_time - start_time（误差<0.5秒）
- [ ] 视频可正常播放

**输入**：视频路径、时间范围、断层ID
**输出**：文件路径

**注意**：需要安装ffmpeg：`pip install ffmpeg-python` 或系统安装ffmpeg

---

#### 模块3整体验证
**验证数据**：准备2个测试视频片段
- 片段A：包含明显动作（如代码高亮变化）
- 片段B：静态架构图展示

**验证标准**：
- [ ] 片段A能正确截取视频片段，时长2-8秒
- [ ] 片段B能正确选择最完整帧作为截图
- [ ] 所有输出文件可正常打开

---

### 模块4：文字补全生成（预计时间：0.5天）

#### 功能4.1：设计文字补全Prompt
**任务描述**：针对"文字补全"类型的断层，设计LLM Prompt

**技术方案**：
```python
def build_text_fill_prompt(fault):
    """
    构建文字补全的Prompt
    """
    prompt = f"""你是知识笔记优化助手。请为以下视频文字稿的断层生成补全文字。

【断层文字】
{fault['fault_text']}

【上下文】
前文：{fault['context_before']}
后文：{fault['context_after']}

【任务】
1. 分析断层缺失的内容（概念定义、逻辑关系、因果链等）
2. 生成结构化的补全文字，使用Markdown格式
3. 保持与上下文的连贯性

【输出要求】
- 使用分级标题（###、####）组织内容
- 用加粗标注核心关键词
- 用有序列表呈现步骤或逻辑链
- 字数控制在100-300字

【输出格式】
直接输出Markdown文字，无需JSON包装。
"""
    return prompt
```

**验证标准**：
- [ ] Prompt明确了输出格式要求
- [ ] 包含上下文信息

**输入**：断层字典
**输出**：Prompt字符串

---

#### 功能4.2：调用LLM生成补全文字
**任务描述**：调用API，获取Markdown格式的补全文字

**技术方案**：
```python
def generate_text_fill(fault):
    """
    生成文字补全内容
    
    返回：
    - fill_text: Markdown格式的补全文字
    """
    prompt = build_text_fill_prompt(fault)
    
    client = OpenAI(
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        base_url="https://api.deepseek.com"
    )
    
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": "你是知识笔记优化助手。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0.5
    )
    
    return response.choices[0].message.content
```

**验证标准**：
- [ ] 返回内容为Markdown格式
- [ ] 包含必要的结构元素（标题、列表等）
- [ ] 字数在100-300范围内

**输入**：断层字典
**输出**：Markdown文字

---

### 模块5：Markdown文档组装（预计时间：1天）

#### 功能5.1：生成文档结构
**任务描述**：根据原文字稿和断层补全，构建Markdown文档框架

**技术方案**：
```python
def build_markdown_structure(original_text, faults_with_fills):
    """
    构建Markdown文档结构
    
    参数：
    - original_text: 原始文字稿（带时间戳）
    - faults_with_fills: 断层及其补全内容列表
    
    返回：
    - markdown_sections: 文档章节列表
    """
    sections = []
    last_pos = 0
    
    for fault in sorted(faults_with_fills, key=lambda x: x['timestamp_start']):
        # 添加断层前的原文
        if last_pos < fault['timestamp_start']:
            sections.append({
                'type': 'original',
                'content': original_text[last_pos:fault['timestamp_start']]
            })
        
        # 添加补全内容
        sections.append({
            'type': 'fill',
            'method': fault['fill_method'],
            'content': fault['fill_content'],
            'metadata': fault.get('metadata', {})
        })
        
        last_pos = fault['timestamp_end']
    
    # 添加最后剩余的原文
    if last_pos < len(original_text):
        sections.append({
            'type': 'original',
            'content': original_text[last_pos:]
        })
    
    return sections
```

**验证标准**：
- [ ] 章节顺序正确（按时间戳排序）
- [ ] 原文和补全内容交替出现
- [ ] 无内容遗漏

**输入**：原文字稿、补全结果列表
**输出**：结构化章节列表

---

#### 功能5.2：嵌入截图和视频
**任务描述**：将截图和视频片段以Markdown语法嵌入文档

**技术方案**：
```python
def embed_media(section):
    """
    根据补全类型，生成对应的Markdown嵌入代码
    
    参数：
    - section: 章节字典
    
    返回：
    - markdown_text: Markdown文本
    """
    if section['type'] == 'original':
        return section['content']
    
    elif section['type'] == 'fill':
        method = section['method']
        
        if method == '文字':
            return section['content']
        
        elif method == '截图':
            file_path = section['content']  # 截图文件路径
            score = section['metadata'].get('score', {})
            return f"""
![架构图]({file_path})
> 📊 截图评分：{score.get('total', 0):.1f}分（稳定：{score.get('stability', 0):.1f}分 | 无遮挡：{score.get('no_occlusion', 0):.1f}分）
"""
        
        elif method == '视频片段':
            file_path = section['content']  # 视频文件路径
            start = section['metadata'].get('start_time', 0)
            end = section['metadata'].get('end_time', 0)
            duration = end - start
            return f"""
[查看动作演示]({file_path})
> 🎬 时长：{duration:.1f}秒 | 时间戳：{start:.1f}s - {end:.1f}s
"""
    
    return ""
```

**验证标准**：
- [ ] 截图使用`![](path)`语法
- [ ] 视频使用`[text](path)`语法
- [ ] 路径为相对路径或绝对路径

**输入**：章节字典
**输出**：Markdown文本

---

#### 功能5.3：生成完整Markdown文档
**任务描述**：整合所有章节，生成最终文档

**技术方案**：
```python
def generate_final_markdown(sections, video_title, output_path):
    """
    生成最终的Markdown笔记
    
    参数：
    - sections: 章节列表
    - video_title: 视频标题
    - output_path: 输出文件路径
    
    返回：
    - output_path: 保存的文件路径
    """
    # 文档头部
    markdown = f"""# {video_title}

> 📹 视频转笔记（自动生成）
> 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

"""
    
    # 拼接所有章节
    for section in sections:
        markdown += embed_media(section)
        markdown += "\n\n"
    
    # 保存文件
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(markdown)
    
    return output_path
```

**验证标准**：
- [ ] 文件成功保存
- [ ] Markdown格式正确（可在Obsidian中正常渲染）
- [ ] 媒体链接可点击打开

**输入**：章节列表、视频标题
**输出**：Markdown文件路径

---

#### 模块5整体验证
**验证数据**：使用模块1-4生成的所有补全内容

**验证标准**：
- [ ] 生成的Markdown文件结构完整
- [ ] 在Obsidian中打开，所有截图和视频可正常显示
- [ ] 主观体验：比纯文字稿更容易理解

---

## 整体集成测试（预计时间：1天）

### 端到端测试流程
**测试数据**：准备3个不同类型的测试视频
- 视频A：算法讲解（包含动态操作）
- 视频B：架构图讲解（静态截图为主）
- 视频C：理论概念讲解（文字补全为主）

**测试步骤**：
1. 运行完整pipeline，生成3份笔记
2. 自己查看每份笔记，评估"是否比纯文字稿好用"
3. 记录问题清单（哪些断层补全不合理、哪些素材质量不够）

**验证标准**：
- [ ] 3个视频都能成功生成笔记
- [ ] 至少2个视频的笔记"明显比纯文字稿好用"（主观评判）
- [ ] 无系统崩溃或报错

---

## 时间安排建议

### 第1周（基础功能）
- Day 1-2：模块0+模块1+模块2（环境+断层加载+LLM判断）
- Day 3-4：模块3.1-3.6（截图选择与保存）
- Day 5：模块3.7-3.9（视频片段截取）
- 周末：调试与优化

### 第2周（集成与验证）
- Day 1：模块4（文字补全生成）
- Day 2-3：模块5（Markdown组装）
- Day 4：端到端测试，记录问题
- Day 5：根据问题调整参数和Prompt
- 周末：准备3-5个真实视频，生成笔记验证效果

---

## 关键风险与应对

### 风险1：LLM判断准确率低（<70%）
**应对**：
- 优化Prompt，增加更多示例
- 调整temperature参数
- 如仍不行，手动标注10个样本，微调模型

### 风险2：截图质量差（遮挡严重、画面模糊）
**应对**：
- 调整MSE阈值（100→120）
- 增加OCR信息密度评分（S2）
- 提供人工选择入口

### 风险3：视频片段截取不完整
**应对**：
- 启用动态区间扩展
- 增加语义-画面双对齐逻辑
- 提供冗余片段选择入口

### 风险4：Markdown格式在Obsidian中渲染异常
**应对**：
- 检查路径格式（相对路径 vs 绝对路径）
- 测试嵌入语法（`![]()`是否正确）
- 参考Obsidian官方文档调整

---

## 下一步行动

### 立即开始（今天）
- [ ] 创建项目目录结构
- [ ] 配置Python环境
- [ ] 准备1个测试视频（5-10分钟）
- [ ] 准备deepseek的断层识别结果JSON样本

### 本周完成
- [ ] 完成模块0-2，能跑通LLM判断
- [ ] 完成模块3截图选择，能生成高质量截图

### 下周完成
- [ ] 完成所有模块，生成第一份完整笔记
- [ ] 验证"比纯文字稿好用"

---

**核心提醒**：
1. MVP阶段追求"能用"而非"完美"，先跑通流程
2. 每个模块独立验证，避免集成时才发现问题
3. 遇到困难时，先用最简单的方案（如固定参数、手动标注）绕过，后续优化
4. 保持主观体验优先的验证标准，技术指标是辅助

---

### 持续迭代
- [ ] 积累用户反馈数据
- [ ] 定期（每月）分析参数优化方向
- [ ] 更新理论框架文档

---

## Phase 2: Physics-First Hardening & Industrialization (已完成)

鉴于 MVP 实测中暴露的 ASR 漂移与素材误删问题，我们在原有计划基础上增加了 **物理层加固阶段**。

### 核心成果
1.  **Dual-Anchor Visual Recalibration (双锚点重标定)**
    -   **问题**：ASR 向后漂移导致视频包含静默定格画面，且易生成损坏文件。
    -   **解决**：放弃盲信 ASR，以 ASR 为中心 ±3s 搜索，使用双向 MSE 极值扫描锁定物理跳变点与定格点。
    -   **落地**：`VideoClipExtractor` 已集成该算法，且增加了 `Physical EOF` 硬约束。

2.  **3-Tier Scene-Centric Clustering (三层几何聚类)**
    -   **问题**：120s 滑动窗口误删早期关键素材，且无法处理跨章节同名模板。
    -   **解决**：
        -   **Tier 1**：dHash + 3D-Hist 锁定物理 PPT 容器（无视时间跨度）。
        -   **Tier 2**：时空语义校准检测“模板重用”。
        -   **Tier 3**：架构完整性打分（Information Superset Scoring）。
    -   **落地**：`GlobalMaterialOptimizer` 已重构为该三层架构，实测保留了所有关键过程帧。

3.  **Metadata Persistence (元数据全链路)**
    -   **问题**：LLM 生成的视频过渡语在最终 JSON 中丢失。
    -   **解决**：重构 E2E 管道，确保 `VideoMetadata` 从提取器透传至最终结果。

### 状态更新
目前系统已从“理论 MVP”进化为“工业级 MVP”，具备了处理长视频、复杂 PPT 动画和高噪声音频的鲁棒能力。
