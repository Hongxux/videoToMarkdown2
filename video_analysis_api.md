# Qwen3-VL-Plus 通过 OpenAI 兼容接口上传视频指南

## 一、核心概念澄清
**Qwen3-VL-Plus** 是阿里云百炼平台（Model Studio）提供的多模态大模型，并非 OpenAI 官方模型。它支持 **OpenAI 兼容接口**，可使用 OpenAI SDK 调用，但需配置阿里云百炼的 **base_url** 和 **API Key**，而非 OpenAI 官方密钥。

## 二、准备工作
1.  **获取阿里云百炼 API Key**
    - 登录[阿里云百炼平台](https://model.aliyun.com/)
    - 进入个人中心 → API Key 管理 → 创建新的 API Key
    - 记录 API Key，用于后续认证

2.  **安装 OpenAI SDK**
    ```bash
    pip install openai>=1.0.0
    ```

3.  **配置环境变量**
    ```bash
    export OPENAI_API_KEY="sk-bee8fc770460451da1cb9d8d0ab5f7a6"
    export OPENAI_BASE_URL="https://dashscope.aliyuncs.com/compatible-mode/v1"
    ```

## 三、视频上传

### 通过 DashScope SDK 获取临时 URL（推荐本地文件）
阿里云提供专门的 SDK 用于获取本地文件的临时 URL，避免 Base64 编码的性能问题。

#### 操作步骤
1.  安装 DashScope SDK
    ```bash
    pip install dashscope>=1.24.6
    ```

2.  上传本地文件获取临时 URL
3.  使用临时 URL 调用 OpenAI 兼容接口

#### 代码示例
```python
import dashscope
from openai import OpenAI

# 配置 DashScope
dashscope.api_key = "sk-bee8fc770460451da1cb9d8d0ab5f7a6"

# 上传本地视频获取临时 URL
def get_video_temp_url(video_path):
    with open(video_path, "rb") as file:
        response = dashscope.File.upload(
            file=file,
            model_name="qwen3-vl-plus"  # 指定用于 qwen3-vl-plus 模型
        )
    if response.status_code == 200:
        return response.output.get("url")
    else:
        raise Exception(f"文件上传失败: {response.message}")

# 获取临时 URL
video_url = get_video_temp_url("local_video.mp4")

# 初始化 OpenAI 客户端
client = OpenAI(
    api_key="sk-bee8fc770460451da1cb9d8d0ab5f7a6",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)

# 构造消息并调用 API
messages = [
    {
        "role": "user",
        "content": [
            {"type": "text", "text": "请描述这段视频的内容"},
            {
                "type": "video_url",
                "video_url": {"url": video_url}
            }
        ]
    }
]

response = client.chat.completions.create(
    model="qwen3-vl-plus",
    messages=messages,
    max_tokens=1024
)

print(response.choices[0].message.content)
```

## 四、关键注意事项
1.  **抽帧频率限制**：使用 OpenAI 兼容 SDK 时，视频默认**每 0.5 秒抽取一帧**，且**不支持修改**。如需自定义抽帧频率（如 1 秒 1 帧），请使用阿里云 DashScope 原生 SDK。

2.  **视频格式与大小限制**
    - 支持格式：MP4、AVI、MOV 等常见格式
    - 推荐大小：不超过 500MB，时长不超过 10 分钟（确保分析速度）
    - 分辨率：建议不超过 1080p（平衡分析效果与资源消耗）

3.  **上下文长度限制**：Qwen3-VL-Plus 支持**256K 上下文窗口**，可处理长视频，但过长视频可能导致推理时间增加。

4.  **流式调用支持**：可通过 `stream=True` 参数实现流式输出，适用于实时视频分析场景。

    ```python
    response = client.chat.completions.create(
        model="qwen3-vl-plus",
        messages=messages,
        max_tokens=1024,
        stream=True
    )

    for chunk in response:
        if chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)
    ```

## 五、错误排查
1.  **认证失败**：检查 API Key 是否正确，base_url 是否设置为阿里云百炼兼容模式地址。
2.  **视频无法访问**：确保视频 URL 可公开访问，或临时 URL 未过期（通常有效期 24 小时）。
3.  **请求超时**：缩短视频时长或降低分辨率，或使用流式调用减少等待时间。
4.  **格式不支持**：转换视频为 MP4 格式后重试。

## 六、总结
Qwen3-VL-Plus 通过 OpenAI 兼容接口上传视频的核心是**构造包含视频 URL 或 Base64 编码的多模态消息**，并使用阿里云百炼的 API Key 和 base_url 进行认证。推荐优先使用**视频 URL 方式**（公开视频）或**DashScope SDK 临时 URL 方式**（本地视频），以获得更好的性能和稳定性。

需要我提供一个完整的可直接运行的示例（含环境配置、文件上传和视频分析），你只需替换API Key和视频路径即可？