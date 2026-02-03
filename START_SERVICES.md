# 端到端测试 - 服务启动脚本

## 重要提示
- Python Worker 必须在 `conda whisper_env` 环境中运行
- 确保已安装所有依赖

## 启动顺序

### 1. 激活Conda环境
```powershell
conda activate whisper_env
```

### 2. 安装Python依赖（首次运行）
```powershell
cd videoToMarkdown
pip install pika pyyaml -q
cd ..
```

### 3. 检查前置条件
```powershell
python check_prerequisites.py
```

### 4. 启动各服务

**启动RabbitMQ** (如果未运行):
```powershell
# 方式1: Docker
docker-compose up -d

# 方式2: Windows服务
net start RabbitMQ
```

**启动Java Backend** (新终端):
```powershell
cd java-backend
mvn spring-boot:run
```

**启动Python Worker** (新终端，必须先激活conda):
```powershell
conda activate whisper_env
cd videoToMarkdownTest2\videoToMarkdown
python worker_manager.py
```

**启动Frontend** (新终端):
```powershell
cd frontend
npm run dev
```

## 验证服务

- Java Backend: http://localhost:8080/actuator/health
- RabbitMQ管理: http://localhost:15672 (admin/admin123)
- Frontend: http://localhost:5173

## 测试流程

1. 打开 http://localhost:5173
2. 使用邀请码注册: `INVITE-TEST-001`
3. 提交测试视频URL
4. 观察Worker日志和进度更新
5. 下载处理结果

## 快速测试视频

**短视频推荐** (约1-2分钟):
```
https://www.bilibili.com/video/BV1xx411c7mu
```
