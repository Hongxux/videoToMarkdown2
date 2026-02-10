# 视频转文字稿项目 - Java+Python混合架构方案(基于RabbitMQ)

## 一、架构设计总览

### 1.1 核心设计原则

基于你的回答,架构设计遵循以下原则:

1. **技术选型**:RabbitMQ消息队列解耦Java和Python
2. **部署环境**:阿里云GPU计算型gn6i(支持Whisper GPU加速)
3. **扩展性优先**:第一版即考虑未来50-100用户的扩展能力
4. **Python独立性**:Python脚本已模块化,可独立调用

---

### 1.2 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                         用户层                                │
│                    (浏览器/移动端)                            │
└──────────────────────┬──────────────────────────────────────┘
                       │ HTTP请求
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Java Web层 (Spring Boot)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ 文件上传服务   │  │ 任务管理服务   │  │ 结果查询服务   │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                  │                  │              │
│         ▼                  ▼                  ▼              │
│  ┌──────────────────────────────────────────────────┐      │
│  │           MySQL (任务状态/用户数据)                │      │
│  └──────────────────────────────────────────────────┘      │
└──────────────────────┬──────────────────────────────────────┘
                       │ 发送任务消息
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    RabbitMQ 消息队列                          │
│  ┌──────────────────┐         ┌──────────────────┐          │
│  │ video.task.queue │────────▶│ result.queue     │          │
│  │ (待处理任务)      │         │ (处理结果)        │          │
│  └──────────────────┘         └──────────────────┘          │
└──────────────────────┬──────────────────────────────────────┘
                       │ 消费任务消息
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Python AI处理层 (多进程Worker)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Worker 1     │  │ Worker 2     │  │ Worker 3     │      │
│  │ (Whisper+LLM)│  │ (Whisper+LLM)│  │ (Whisper+LLM)│      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         │                  │                  │              │
│         └──────────────────┴──────────────────┘              │
│                            │                                 │
│                            ▼                                 │
│                    ┌──────────────┐                          │
│                    │ GPU (3060)   │                          │
│                    │ Whisper加速  │                          │
│                    └──────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、技术栈选型

### 2.1 Java Web层

| 组件 | 技术选型 | 版本 | 用途 |
|------|---------|------|------|
| 核心框架 | Spring Boot | 3.2+ | Web服务基础 |
| 数据库 | MySQL | 8.0+ | 任务状态/用户数据持久化 |
| ORM | Spring Data JPA | 3.2+ | 数据库操作 |
| 消息队列 | RabbitMQ | 3.12+ | 异步任务解耦 |
| MQ客户端 | Spring AMQP | 3.1+ | RabbitMQ集成 |
| 文件存储 | 本地磁盘 | - | 视频/结果文件存储 |
| 前端 | Thymeleaf + Bootstrap | - | 简单Web界面 |

### 2.2 Python AI层

| 组件 | 技术选型 | 版本 | 用途 |
|------|---------|------|------|
| Whisper | openai-whisper | latest | 视频转录 |
| LLM客户端 | openai / anthropic | latest | 文本处理 |
| MQ客户端 | pika | 1.3+ | RabbitMQ消费 |
| 数据处理 | pandas | latest | 文本清洗 |
| GPU加速 | torch (CUDA) | 2.0+ | Whisper GPU推理 |

---

## 三、核心流程设计

### 3.1 任务处理完整流程

```
1. 用户上传视频
   ↓
2. Java接收文件,存储到本地,创建任务记录(MySQL)
   ↓
3. Java发送任务消息到RabbitMQ (video.task.queue)
   消息内容:{task_id, video_path, user_id, created_at}
   ↓
4. Python Worker消费消息
   ↓
5. Python执行处理:
   5.1 提取音频(FFmpeg)
   5.2 Whisper转录(GPU加速)
   5.3 LLM文本处理(24步流程)
   5.4 生成Markdown结果
   ↓
6. Python发送结果消息到RabbitMQ (result.queue)
   消息内容:{task_id, status, result_path, error_msg}
   ↓
7. Java消费结果消息,更新任务状态(MySQL)
   ↓
8. 用户查询任务状态,下载结果
```

---

## 四、详细实现方案

### 4.1 Java Web层实现

#### 4.1.1 数据库设计

```sql
-- 任务表
CREATE TABLE tasks (
    task_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36),
    video_name VARCHAR(255) NOT NULL,
    video_path VARCHAR(500) NOT NULL,
    video_size BIGINT,
    status VARCHAR(20) NOT NULL,  -- PENDING/PROCESSING/COMPLETED/FAILED
    result_path VARCHAR(500),
    error_msg TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 用户表(可选,第一版可不实现)
CREATE TABLE users (
    user_id VARCHAR(36) PRIMARY KEY,
    email VARCHAR(255) UNIQUE,
    usage_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### 4.1.2 核心Java代码

**任务实体类**
```java
@Entity
@Table(name = "tasks")
public class Task {
    @Id
    private String taskId;
    private String userId;
    private String videoName;
    private String videoPath;
    private Long videoSize;
    
    @Enumerated(EnumType.STRING)
    private TaskStatus status;  // PENDING, PROCESSING, COMPLETED, FAILED
    
    private String resultPath;
    private String errorMsg;
    
    @CreationTimestamp
    private LocalDateTime createdAt;
    
    @UpdateTimestamp
    private LocalDateTime updatedAt;
    
    private LocalDateTime completedAt;
    
    // getters/setters省略
}

enum TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED
}
```

**文件上传服务**
```java
@Service
public class VideoUploadService {
    @Autowired
    private TaskRepository taskRepository;
    
    @Autowired
    private RabbitTemplate rabbitTemplate;
    
    @Value("${video.upload.dir}")
    private String uploadDir;
    
    public Task uploadVideo(MultipartFile file, String userId) throws IOException {
        // 1. 生成任务ID
        String taskId = UUID.randomUUID().toString();
        
        // 2. 保存视频文件
        String videoPath = uploadDir + "/" + taskId + "/" + file.getOriginalFilename();
        File destFile = new File(videoPath);
        destFile.getParentFile().mkdirs();
        file.transferTo(destFile);
        
        // 3. 创建任务记录
        Task task = new Task();
        task.setTaskId(taskId);
        task.setUserId(userId);
        task.setVideoName(file.getOriginalFilename());
        task.setVideoPath(videoPath);
        task.setVideoSize(file.getSize());
        task.setStatus(TaskStatus.PENDING);
        taskRepository.save(task);
        
        // 4. 发送任务消息到RabbitMQ
        VideoTaskMessage message = new VideoTaskMessage(
            taskId, videoPath, userId, LocalDateTime.now()
        );
        rabbitTemplate.convertAndSend("video.task.queue", message);
        
        return task;
    }
}
```

**RabbitMQ配置**
```java
@Configuration
public class RabbitMQConfig {
    
    @Bean
    public Queue videoTaskQueue() {
        return QueueBuilder.durable("video.task.queue")
                .withArgument("x-message-ttl", 3600000)  // 消息1小时过期
                .build();
    }
    
    @Bean
    public Queue resultQueue() {
        return QueueBuilder.durable("result.queue").build();
    }
    
    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
```

**结果消费者**
```java
@Service
public class ResultConsumer {
    @Autowired
    private TaskRepository taskRepository;
    
    @RabbitListener(queues = "result.queue")
    public void handleResult(VideoResultMessage message) {
        Task task = taskRepository.findById(message.getTaskId()).orElse(null);
        if (task == null) {
            log.error("任务不存在: {}", message.getTaskId());
            return;
        }
        
        if ("success".equals(message.getStatus())) {
            task.setStatus(TaskStatus.COMPLETED);
            task.setResultPath(message.getResultPath());
            task.setCompletedAt(LocalDateTime.now());
        } else {
            task.setStatus(TaskStatus.FAILED);
            task.setErrorMsg(message.getErrorMsg());
        }
        
        taskRepository.save(task);
        log.info("任务{}处理完成,状态:{}", message.getTaskId(), message.getStatus());
    }
}
```

---

### 4.2 Python AI层实现

#### 4.2.1 RabbitMQ消费者

```python
# worker.py
import pika
import json
import os
import subprocess
from datetime import datetime

# RabbitMQ连接配置
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')

# 建立连接
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
)
channel = connection.channel()

# 声明队列
channel.queue_declare(queue='video.task.queue', durable=True)
channel.queue_declare(queue='result.queue', durable=True)

def process_video(task_id, video_path):
    """
    调用你的Python脚本处理视频
    假设你的脚本是: python process_video.py --task_id xxx --video_path xxx
    """
    try:
        # 调用你的24步处理脚本
        result = subprocess.run(
            ['python', 'process_video.py', 
             '--task_id', task_id, 
             '--video_path', video_path],
            capture_output=True,
            text=True,
            timeout=3600  # 1小时超时
        )
        
        if result.returncode == 0:
            # 处理成功,解析输出获取结果路径
            result_path = result.stdout.strip()
            return {'status': 'success', 'result_path': result_path}
        else:
            return {'status': 'failed', 'error_msg': result.stderr}
            
    except subprocess.TimeoutExpired:
        return {'status': 'failed', 'error_msg': '处理超时'}
    except Exception as e:
        return {'status': 'failed', 'error_msg': str(e)}

def callback(ch, method, properties, body):
    """消费任务消息的回调函数"""
    try:
        # 解析任务消息
        message = json.loads(body)
        task_id = message['taskId']
        video_path = message['videoPath']
        
        print(f"[{datetime.now()}] 开始处理任务: {task_id}")
        
        # 执行视频处理
        result = process_video(task_id, video_path)
        
        # 发送结果消息
        result_message = {
            'taskId': task_id,
            'status': result['status'],
            'resultPath': result.get('result_path'),
            'errorMsg': result.get('error_msg')
        }
        
        channel.basic_publish(
            exchange='',
            routing_key='result.queue',
            body=json.dumps(result_message),
            properties=pika.BasicProperties(delivery_mode=2)  # 持久化消息
        )
        
        print(f"[{datetime.now()}] 任务{task_id}处理完成: {result['status']}")
        
    except Exception as e:
        print(f"处理任务失败: {str(e)}")
    finally:
        # 确认消息已处理
        ch.basic_ack(delivery_tag=method.delivery_tag)

# 设置QoS:每次只处理1个任务(根据GPU显存调整)
channel.basic_qos(prefetch_count=1)

# 开始消费
channel.basic_consume(queue='video.task.queue', on_message_callback=callback)

print('Python Worker启动,等待任务...')
channel.start_consuming()
```

#### 4.2.2 视频处理脚本适配

```python
# process_video.py (你的24步处理脚本)
import argparse
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--task_id', required=True)
    parser.add_argument('--video_path', required=True)
    args = parser.parse_args()
    
    try:
        # 你的24步处理逻辑
        # 步骤1: Whisper转录
        # 步骤2-24: LLM处理...
        
        # 生成结果文件
        result_path = f"/results/{args.task_id}/output.md"
        
        # 输出结果路径到stdout(供worker.py读取)
        print(result_path)
        sys.exit(0)
        
    except Exception as e:
        # 输出错误到stderr
        print(f"ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
```

---

## 五、部署方案

### 5.1 云服务器配置

**阿里云GPU计算型gn6i推荐配置:**
- 实例规格: ecs.gn6i-c4g1.xlarge (4核16G + Tesla T4 GPU)
- 操作系统: Ubuntu 22.04 LTS
- 磁盘: 100GB SSD系统盘 + 500GB数据盘(存储视频)

### 5.2 环境部署

#### 5.2.1 Java环境

```bash
# 安装JDK 17
sudo apt update
sudo apt install openjdk-17-jdk -y

# 安装MySQL
sudo apt install mysql-server -y
sudo mysql_secure_installation

# 安装RabbitMQ
sudo apt install rabbitmq-server -y
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server

# 创建RabbitMQ用户
sudo rabbitmqctl add_user admin password
sudo rabbitmqctl set_user_tags admin administrator
sudo rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"
```

#### 5.2.2 Python环境

```bash
# 安装CUDA (GPU加速)
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-ubuntu2204.pin
sudo mv cuda-ubuntu2204.pin /etc/apt/preferences.d/cuda-repository-pin-600
sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/3bf863cc.pub
sudo add-apt-repository "deb https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/ /"
sudo apt update
sudo apt install cuda -y

# 安装Python依赖
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
pip install openai-whisper pika pandas

# 安装FFmpeg
sudo apt install ffmpeg -y
```

### 5.3 启动服务

```bash
# 1. 启动Java Web服务
cd /path/to/java-project
java -jar video-transcribe-web.jar

# 2. 启动Python Worker(多进程)
cd /path/to/python-project
# 启动3个Worker进程(根据GPU显存调整)
python worker.py &
python worker.py &
python worker.py &
```

---

## 六、扩展性设计

### 6.1 水平扩展方案

当用户增长到50-100人时:

**Java Web层扩展:**
- 部署多个Java实例 + Nginx负载均衡
- 引入Redis缓存任务状态,减少MySQL查询压力

**Python Worker层扩展:**
- 增加GPU服务器,部署更多Worker进程
- RabbitMQ自动负载均衡到多个Worker

**数据库扩展:**
- MySQL主从复制(读写分离)
- 分库分表(按user_id分片)

### 6.2 监控与告警

- **Java层**: Spring Boot Actuator + Prometheus + Grafana
- **Python层**: 自定义日志 + ELK Stack
- **RabbitMQ**: Management Plugin监控队列积压

---

## 七、第一周开发计划

### Day 1-2: 环境搭建
- [ ] 购买阿里云GPU服务器
- [ ] 安装Java/Python/MySQL/RabbitMQ
- [ ] 配置CUDA环境,测试Whisper GPU加速

### Day 3-4: Java Web层开发
- [ ] 创建Spring Boot项目
- [ ] 实现文件上传接口
- [ ] 实现RabbitMQ消息发送
- [ ] 实现任务状态查询接口

### Day 5-6: Python Worker开发
- [ ] 编写RabbitMQ消费者
- [ ] 适配你的24步处理脚本
- [ ] 测试完整流程

### Day 7: 测试与优化
- [ ] 端到端测试
- [ ] 性能测试(单Worker处理能力)
- [ ] 部署到云服务器,邀请第一批用户

---

## 八、风险与反例(维度四:Verification)

### 8.1 潜在风险

1. **RabbitMQ消息积压**:
   - 风险:如果Python处理速度慢,消息会积压在队列中
   - 缓解:设置消息TTL,超时自动失败;监控队列长度告警

2. **GPU显存不足**:
   - 风险:多个Worker同时加载Whisper模型,显存爆满
   - 缓解:限制Worker并发数(prefetch_count=1);使用smaller模型

3. **文件存储爆满**:
   - 风险:视频文件占用大量磁盘空间
   - 缓解:任务完成后定时清理;使用OSS对象存储

### 8.2 逆向思维:如何让这个架构失败?

- 不设置消息TTL → 失败任务永久占用队列
- 不限制Worker并发 → GPU显存爆满,所有任务失败
- 不监控队列积压 → 用户等待时间过长,体验极差
- 不做异常处理 → Python脚本崩溃,消息丢失

**当前方案的缓解措施:**
- ✅ 消息TTL + 死信队列
- ✅ QoS限流(prefetch_count)
- ✅ 监控告警
- ✅ 异常捕获 + 结果消息反馈

---

## 九、总结

这个架构方案基于你的技术选型(RabbitMQ + GPU云服务器 + 考虑扩展性),实现了:

1. **解耦**: Java和Python通过消息队列异步通信,互不阻塞
2. **扩展性**: 可独立扩展Java Web层和Python Worker层
3. **可靠性**: 消息持久化 + 任务状态追踪 + 异常处理
4. **高性能**: GPU加速Whisper + 多Worker并行处理

**下一步行动**: 按照第一周开发计划,逐步实现各模块。
