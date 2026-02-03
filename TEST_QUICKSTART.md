# 端到端测试 - 快速启动脚本

## 步骤1: 检查前置条件

### MySQL状态
```powershell
Get-Service MySQL* | Select-Object Name, Status
```
✅ MySQL80正在运行

### RabbitMQ状态
```powershell
# 检查RabbitMQ端口
netstat -ano | Select-String ":5672"
netstat -ano | Select-String ":15672"
```

如果没有运行，启动Docker:
```powershell
docker-compose up -d
```

或使用RabbitMQ Windows服务:
```powershell
# 启动RabbitMQ服务
net start RabbitMQ

# 或者手动启动
rabbitmq-server.bat
```

### 数据库Schema
```powershell
# 导入schema (如果还没有)
mysql -u root -p < database/schema.sql
```

## 步骤2: 启动各组件

### Terminal 1: Java Backend
```powershell
cd java-backend
mvn spring-boot:run
```

等待看到:
```
Started VideoToMarkdownApplication in X.XXX seconds
```

访问健康检查: http://localhost:8080/actuator/health

### Terminal 2: Python Worker (需要Conda环境)
```powershell
# 激活conda环境
conda activate whisper_env

# 启动Worker
cd videoToMarkdown
python worker_manager.py
```

等待看到:
```
Worker初始化完成 - RabbitMQ: localhost:5672
DeepSeek API: 已配置
Vision API: 已配置
等待任务消息...
```

### Terminal 3: Frontend
```powershell
cd frontend  
npm install  # 首次运行
npm run dev
```

等待看到:
```
VITE ready in XXX ms
Local: http://localhost:5173/
```

## 步骤3: 开始测试

### 3.1 注册新用户

1. 打开浏览器: http://localhost:5173
2. 点击"注册"
3. 填写表单:
   - Email: test001@example.com
   - Password: Test123456
   - 邀请码: INVITE-TEST-001
4. 点击"注册"

### 3.2 创建测试任务

使用短视频测试(推荐):
- Bilibili短视频(1-2分钟): `https://www.bilibili.com/video/BV1xx411c7mu`

### 3.3 监控处理流程

**RabbitMQ管理界面**: http://localhost:15672
- 用户名: admin
- 密码: admin123

观察队列:
- `video.task.queue`: 任务队列
- `result.queue`: 结果队列

**Worker日志**: 查看Terminal 2输出

**Backend日志**: 查看Terminal 1输出

**Frontend**: 观察进度条和状态更新

## 测试检查清单

- [ ] MySQL运行中
- [ ] RabbitMQ运行中 (端口5672, 15672)
- [ ] Java Backend启动成功 (端口8080)
- [ ] Python Worker连接成功
- [ ] Frontend可访问 (端口5173)
- [ ] 用户注册成功
- [ ] 用户登录成功
- [ ] 任务创建成功
- [ ] Worker接收任务
- [ ] 进度实时更新
- [ ] 任务处理完成
- [ ] 结果文件可下载

## 故障排查

### Backend无法启动
```
# 检查8080端口占用
netstat -ano | Select-String ":8080"

# 检查MySQL连接
mysql -u root -p -e "USE video_to_markdown; SHOW TABLES;"
```

### Worker无法连接RabbitMQ
```
# 检查RabbitMQ是否运行
netstat -ano | Select-String ":5672"

# 查看RabbitMQ日志
docker logs rabbitmq
```

### Frontend请求失败
检查vite.config.js代理配置:
```javascript
proxy: {
  '/api': {
    target: 'http://localhost:8080',
    changeOrigin: true
  }
}
```
