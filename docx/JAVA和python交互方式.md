

## 核心通信方案（按推荐优先级排序）
### 方案1：HTTP/REST接口（最推荐，通用解耦）
这是Java和Python之间最常用、最易落地的通信方式：Python封装AI能力为HTTP接口（如FastAPI/Flask），Java通过HTTP客户端调用该接口。
**核心优势**：跨机器、跨部署、解耦（Java和Python可独立扩容/部署），完全贴合分布式架构，也是你项目的首选。

#### 步骤1：Python端（AI层）编写HTTP接口（Whisper转写）
用FastAPI实现轻量、高性能的AI接口（比Flask更适合生产环境）：
```python
# python_ai_service.py
from fastapi import FastAPI, UploadFile, File
import whisper
import os
import uvicorn

app = FastAPI(title="Whisper转写AI服务")
# 加载Whisper模型（提前下载，建议用base/small模型）
model = whisper.load_model("base")

# 定义转写接口（接收音频文件，返回转写文本）
@app.post("/api/whisper/transcribe")
async def transcribe_audio(file: UploadFile = File(...)):
    try:
        # 1. 保存上传的音频文件（临时文件）
        temp_file = f"temp_{file.filename}"
        with open(temp_file, "wb") as f:
            f.write(await file.read())
        
        # 2. 调用Whisper转写（AI核心逻辑）
        result = model.transcribe(temp_file, language="zh")
        text = result["text"]
        
        # 3. 删除临时文件
        os.remove(temp_file)
        
        # 4. 返回JSON结果
        return {
            "code": 200,
            "msg": "转写成功",
            "data": {"text": text}
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": f"转写失败：{str(e)}",
            "data": None
        }

# 启动服务：监听0.0.0.0，端口8000（允许Java跨机器调用）
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

#### 步骤2：Java端（Web层）调用Python的HTTP接口
用Spring Boot的`WebClient`（非阻塞，适合高并发）调用：
```java
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import org.springframework.stereotype.Service;

@Service
public class PythonAIClient {
    // Python AI服务的地址（可配置在application.properties中）
    private static final String PYTHON_AI_URL = "http://localhost:8000/api/whisper/transcribe";
    private final WebClient webClient;

    public PythonAIClient() {
        this.webClient = WebClient.builder()
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.MULTIPART_FORM_DATA_VALUE)
                .build();
    }

    /**
     * Java调用Python的Whisper转写接口
     * @param audioFilePath 本地音频文件路径（视频提取后的音频）
     * @return 转写文本
     */
    public Mono<String> callWhisperTranscribe(String audioFilePath) {
        // 构建multipart/form-data请求（上传音频文件）
        return webClient.post()
                .uri(PYTHON_AI_URL)
                .body(BodyInserters.fromMultipartData("file", new java.io.File(audioFilePath)))
                .retrieve()
                // 解析Python返回的JSON结果
                .bodyToMono(WhisperResponse.class)
                .map(response -> {
                    if (response.getCode() == 200) {
                        return response.getData().getText();
                    } else {
                        throw new RuntimeException("Python AI服务返回错误：" + response.getMsg());
                    }
                });
    }

    // 定义响应实体类（匹配Python返回的JSON结构）
    static class WhisperResponse {
        private int code;
        private String msg;
        private WhisperData data;

        // getter/setter 省略
    }

    static class WhisperData {
        private String text;

        // getter/setter 省略
    }

    // 测试调用
    public static void main(String[] args) {
        PythonAIClient client = new PythonAIClient();
        // 视频提取后的音频文件路径
        String audioPath = "path/to/audio.mp3";
        client.callWhisperTranscribe(audioPath)
              .subscribe(
                  text -> System.out.println("转写结果：" + text),
                  error -> System.err.println("调用失败：" + error.getMessage())
              );
    }
}
```

#### 核心说明
- **部署灵活**：Python服务可部署在独立服务器/容器，Java通过IP+端口调用，支持分布式扩容；
- **异常处理**：Java端捕获Python接口的错误码，保证调用稳定性；
- **高并发适配**：Python用FastAPI（异步框架）、Java用WebClient（非阻塞），可支撑高并发转写请求。

### 方案2：消息队列（高并发异步场景）
如果你的项目需要处理**高并发视频转写请求**（比如每秒数百个），用消息队列（如RabbitMQ）实现Java和Python的异步通信：Java将转写任务发送到队列，Python消费队列执行转写，再将结果写回队列，Java异步获取结果。
**核心优势**：削峰填谷、异步解耦，避免高并发下请求阻塞。

#### 步骤1：Java端发送转写任务到RabbitMQ
```java
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TranscribeTaskProducer {
    @Autowired
    private RabbitTemplate rabbitTemplate;

    // 发送转写任务（音频文件路径+任务ID）
    public void sendTranscribeTask(String taskId, String audioFilePath) {
        TranscribeTask task = new TranscribeTask(taskId, audioFilePath);
        // 发送到"whisper.transcribe.task"队列
        rabbitTemplate.convertAndSend("whisper.transcribe.task", task);
        System.out.println("任务发送成功：" + taskId);
    }

    // 任务实体类
    static class TranscribeTask {
        private String taskId;
        private String audioFilePath;

        // 构造器、getter/setter 省略
    }
}
```

#### 步骤2：Python端消费队列并执行转写
```python
# python_rabbitmq_consumer.py
import pika
import whisper
import os
import json

# 初始化RabbitMQ连接
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
# 声明队列（和Java端一致）
channel.queue_declare(queue='whisper.transcribe.task', durable=True)
# 加载Whisper模型
model = whisper.load_model("base")

# 消费任务的回调函数
def callback(ch, method, properties, body):
    # 解析Java发送的任务
    task = json.loads(body)
    task_id = task["taskId"]
    audio_path = task["audioFilePath"]
    
    try:
        # 执行转写
        result = model.transcribe(audio_path, language="zh")
        text = result["text"]
        
        # 将结果发送到结果队列（供Java消费）
        result_msg = json.dumps({
            "taskId": task_id,
            "text": text,
            "status": "success"
        })
        channel.basic_publish(exchange='', routing_key='whisper.transcribe.result', body=result_msg)
        print(f"任务{task_id}转写完成，结果已发送")
    except Exception as e:
        # 发送失败结果
        result_msg = json.dumps({
            "taskId": task_id,
            "error": str(e),
            "status": "failed"
        })
        channel.basic_publish(exchange='', routing_key='whisper.transcribe.result', body=result_msg)
    finally:
        # 确认任务已处理
        ch.basic_ack(delivery_tag=method.delivery_tag)

# 消费队列
channel.basic_qos(prefetch_count=4)  # 限制同时处理4个任务（根据CPU核心数调整）
channel.basic_consume(queue='whisper.transcribe.task', on_message_callback=callback)
channel.start_consuming()
```

#### 步骤3：Java端消费转写结果
```java
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class TranscribeResultConsumer {
    // 监听结果队列
    @RabbitListener(queues = "whisper.transcribe.result")
    public void handleTranscribeResult(String resultJson) {
        // 解析JSON结果，更新任务状态、存储转写文本
        System.out.println("收到转写结果：" + resultJson);
        // 后续逻辑：更新数据库、推送通知等
    }
}
```

### 方案3：本地进程调用（简单单机场景）
如果Java和Python部署在同一台机器，且并发量低，可直接通过Java的`ProcessBuilder`调用Python脚本（无需搭建HTTP/消息队列）。
#### 步骤1：Python脚本（whisper_transcribe.py）
```python
import sys
import whisper

def transcribe(audio_path):
    model = whisper.load_model("base")
    result = model.transcribe(audio_path, language="zh")
    return result["text"]

if __name__ == "__main__":
    # 接收Java传递的音频文件路径参数
    audio_path = sys.argv[1]
    try:
        text = transcribe(audio_path)
        print(text)  # 将结果输出到标准输出，供Java读取
    except Exception as e:
        print(f"ERROR:{str(e)}", file=sys.stderr)  # 错误输出到标准错误
```

#### 步骤2：Java调用Python脚本
```java
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class PythonScriptCaller {
    public static String callWhisperScript(String audioFilePath) throws Exception {
        // 构建Python进程调用命令
        ProcessBuilder pb = new ProcessBuilder("python3", "whisper_transcribe.py", audioFilePath);
        pb.redirectErrorStream(true); // 合并标准输出和标准错误
        Process process = pb.start();

        // 读取Python输出结果
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder result = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        process.waitFor();

        // 处理结果
        if (process.exitValue() == 0) {
            return result.toString();
        } else {
            throw new RuntimeException("Python脚本执行失败：" + result);
        }
    }

    public static void main(String[] args) throws Exception {
        String audioPath = "path/to/audio.mp3";
        String text = callWhisperScript(audioPath);
        System.out.println("转写结果：" + text);
    }
}
```

### 方案4：RPC框架（高性能场景）
如果对通信延迟要求极高（比如毫秒级），可使用gRPC（跨语言RPC框架）：定义protobuf接口，Java和Python分别实现客户端/服务端。
**适用场景**：高吞吐、低延迟的AI调用（如实时视频转写），但开发成本高于HTTP。

#### 核心步骤（简化）
1. 定义protobuf接口（描述转写服务）；
2. 用protoc生成Java和Python的代码；
3. Python实现gRPC服务端（Whisper转写）；
4. Java实现gRPC客户端调用。

---

## 各方案选型对比（贴合你的项目）
| 通信方式       | 优点                          | 缺点                          | 适用场景                          |
|----------------|-------------------------------|-------------------------------|-----------------------------------|
| HTTP/REST接口  | 通用、解耦、跨机器、易调试    | 略高于RPC的延迟               | 绝大多数场景（推荐）|
| 消息队列       | 异步、削峰填谷、高并发友好    | 架构稍复杂，需部署MQ          | 高并发视频转写、异步处理          |
| 本地进程调用   | 简单、无额外依赖              | 仅单机、并发低、调试困难      | 单机小流量、快速验证              |
| RPC（gRPC）    | 低延迟、高吞吐、强类型        | 开发成本高、调试复杂          | 实时转写、低延迟要求的场景        |

---

### 总结
1. **首选方案**：HTTP/REST接口（FastAPI+Java WebClient），兼顾易用性、解耦性和扩展性，完全贴合你的分布式/高并发改造目标；
2. **高并发场景**：叠加消息队列（RabbitMQ）实现异步解耦，避免请求阻塞；
3. **核心要点**：
   - Java和Python通信的核心是“数据序列化”（JSON为主）；
   - 生产环境需添加**超时控制**、**重试机制**、**监控告警**；
   - 优先保证解耦，让Java和Python层可独立部署、扩容。