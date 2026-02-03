# 端到端测试指南

本指南帮助你测试完整的Java+Python混合架构系统。

## 前置条件

### 1. 启动MySQL
```bash
# 确认MySQL已启动
mysql -u root -p

# 初始化数据库 (如果还没有)
source d:/videoToMarkdownTest2/database/schema.sql
```

### 2. 启动RabbitMQ
```bash
# 方式A: 使用Windows服务
net start RabbitMQ

# 方式B: 使用Docker (如果Docker可用)
cd d:/videoToMarkdownTest2
docker-compose up -d

# 验证RabbitMQ
# 访问 http://localhost:15672 (guest/guest)
```

### 3. 配置环境变量
```bash
# 复制.env.example为.env
cp .env.example .env

# 编辑.env配置:
# MYSQL_PASSWORD=你的MySQL密码
# RABBITMQ_PASSWORD=admin123
# DEEPSEEK_API_KEY=你的API Key
```

## 启动服务

### Terminal 1: Java Backend
```bash
cd java-backend

# 首次编译
mvn clean install

# 启动
mvn spring-boot:run

# 等待看到:
# ╔═══════════════════════════════════════════════════════╗
# ║     视频转文字稿系统 - 后端服务已启动                  ║
# ╚═══════════════════════════════════════════════════════╝
```

### Terminal 2: Python Worker
```bash
cd videoToMarkdown

# 激活虚拟环境
conda activate videotomd  # 或 source venv/bin/activate

# 安装依赖 (首次)
pip install -r requirements.txt

# 启动Worker
python worker_manager.py --workers 1

# 等待看到:
# ╔═══════════════════════════════════════════════════════╗
# ║     VideoToMarkdown Worker Manager                    ║
# ╚═══════════════════════════════════════════════════════╝
```

## 测试流程

### 测试1: 用户注册

```bash
# 使用curl或Postman
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "inviteCode": "INVITE-TEST-001",
    "email": "test@example.com",
    "password": "123456",
    "username": "测试用户"
  }'

# 预期返回:
# {
#   "token": "eyJhbGciOiJIUzI1NiJ9...",
#   "email": "test@example.com",
#   "username": "测试用户",
#   "role": "USER",
#   "userId": 2
# }

# 保存返回的token,后续请求需要
```

### 测试2: 用户登录

```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "123456"
  }'
```

### 测试3: 提交视频任务

```bash
# 使用测试视频URL
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE" \
  -d '{
    "videoUrl": "https://www.bilibili.com/video/BV1gt421a7Cq"
  }'

# 预期返回:
# {
#   "taskId": "uuid-string",
#   "status": "PENDING",
#   "message": "任务已提交"
# }

# 保存taskId
```

### 测试4: 查询任务状态

```bash
# 循环查询任务状态
curl -X GET http://localhost:8080/api/tasks/YOUR_TASK_ID \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# 返回示例:
# {
#   "taskId": "...",
#   "status": "PROCESSING",  # PENDING -> PROCESSING -> COMPLETED
#   "progress": 0.45,
#   "videoUrl": "...",
#   "createdAt": "...",
#   "resultPath": null
# }
```

### 测试5: 查看配额

```bash
curl -X GET http://localhost:8080/api/tasks/quota \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# 返回:
# {
#   "dailyLimit": 3,
#   "remaining": 2,
#   "used": 1
# }
```

### 测试6: 获取任务列表

```bash
curl -X GET http://localhost:8080/api/tasks \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# 返回数组:
# [
#   {
#     "taskId": "...",
#     "status": "COMPLETED",
#     "videoUrl": "...",
#     ...
#   }
# ]
```

## 监控调试

### 查看Java日志
```bash
# Terminal 1的输出会显示:
# - API请求
# - RabbitMQ消息发送
# - 数据库操作
```

### 查看Python Worker日志
```bash
# Terminal 2的输出会显示:
# - 接收到的任务
# - 处理进度
# - 完成/失败状态
```

### 查看RabbitMQ
访问 http://localhost:15672
- Queues -> `video.task.queue`: 待处理任务数
- Queues -> `result.queue`: 结果消息数

### 查看数据库
```sql
-- 查看用户
SELECT * FROM users;

-- 查看任务
SELECT task_id, status, progress, video_url, created_at 
FROM tasks 
ORDER BY created_at DESC;

-- 查看使用记录
SELECT * FROM usage_logs;

-- 查看邀请码使用情况
SELECT invite_code, is_used, used_at FROM invite_codes;
```

## 常见问题

### 1. Java启动失败 - 端口被占用
```
错误: Web server failed to start. Port 8080 was already in use.
解决: lsof -i :8080 (Mac/Linux) 或 netstat -ano | findstr :8080 (Windows)
杀掉占用进程或修改application.yml中的server.port
```

### 2. Python Worker无法连接RabbitMQ
```
错误: pika.exceptions.AMQPConnectionError
解决: 
1. 检查RabbitMQ是否启动: netstat -ano | findstr :5672
2. 检查.env中的RABBITMQ_HOST和RABBITMQ_PORT
3. 检查RabbitMQ用户名密码
```

### 3. 任务一直PENDING状态
```
原因: Python Worker未启动或已崩溃
解决: 
1. 检查Worker是否运行
2. 查看Worker日志
3. 查看RabbitMQ队列是否有消息积压
```

### 4. DeepSeek API调用失败
```
错误: API key invalid
解决: 
1. 检查.env中的DEEPSEEK_API_KEY
2. 访问 https://platform.deepseek.com/ 确认Key有效
3. 检查API配额
```

### 5. 内存不足
```
Whisper处理视频需要大量内存(2-4GB)
解决:
1. 减少Worker数量: python worker_manager.py --workers 1
2. 使用较小模型: config.yaml中设置model_size: "small"
3. 增加系统内存
```

## 性能基准

### CPU模式 (本地测试)
- 5分钟视频: 15-20分钟
- 内存使用: 2-3GB
- 建议Worker数: 1-2

### GPU模式 (云端部署)
- 5分钟视频: 7-8分钟
- 内存使用: 4-6GB
- 建议Worker数: 2-4

## 下一步

1. ✅ Backend + Worker联调通过
2. 开发前端界面 (Vue 3)
3. 完整端到端测试
4. 部署到云端

## API文档

完整API文档: http://localhost:8080/swagger-ui.html
