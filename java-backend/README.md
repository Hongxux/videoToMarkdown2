# VideoToMarkdown - Java Backend

Java后端服务,提供用户认证、任务管理、限流等功能。

## 快速开始

### 1. 环境准备

**必需软件**:
- JDK 17+
- MySQL 8.0+
- RabbitMQ 3.12+
- Maven 3.6+

### 2. 启动RabbitMQ

**使用Docker (推荐)**:
```bash
# 在项目根目录
cd d:/videoToMarkdownTest2
docker-compose up -d
```

**手动启动** (如果Docker失败):
```bash
# Windows: 下载并安装RabbitMQ
# https://www.rabbitmq.com/install-windows.html

# 启动服务
net start RabbitMQ

# 或使用RabbitMQ命令行工具
rabbitmq-server.bat

# 访问管理界面: http://localhost:15672
# 默认账号: guest / guest
```

### 3. 初始化数据库

```bash
mysql -u root -p < d:/videoToMarkdownTest2/database/schema.sql
```

### 4. 配置环境变量

复制`.env.example`为`.env`并修改:
```bash
cp ../.env.example ../.env
```

### 5. 启动应用

```bash
cd java-backend

# 首次编译
mvn clean install

# 启动
mvn spring-boot:run
```

## API文档

启动后访问: http://localhost:8080/swagger-ui.html

## 主要接口

### 认证
- `POST /api/auth/register` - 注册
- `POST /api/auth/login` - 登录

### 任务
- `POST /api/tasks` - 创建任务
- `GET /api/tasks` - 获取任务列表
- `GET /api/tasks/{id}` - 获取任务详情
- `GET /api/tasks/quota` - 查询配额

## 测试

```bash
# 运行所有测试
mvn test

# 运行指定测试
mvn test -Dtest=AuthServiceTest
```

## 常见问题

### RabbitMQ连接失败
1. 检查RabbitMQ是否启动: `netstat -ano | findstr :5672`
2. 检查用户名密码: application.yml中的配置
3. 查看RabbitMQ日志

### MySQL连接失败
1. 检查MySQL是否启动
2. 检查数据库是否已创建
3. 检查用户名密码

## 项目结构

```
src/main/java/com/videotomd/
├── VideoToMarkdownApplication.java  # 主类
├── config/                           # 配置类
│   ├── RabbitMQConfig.java
│   ├── SecurityConfig.java
│   └── JwtUtil.java
├── controller/                       # 控制器
│   ├── AuthController.java
│   └── TaskController.java
├── service/                          # 服务层
│   ├── AuthService.java
│   ├── TaskService.java
│   └── RateLimitService.java
├── repository/                       # 数据访问层
│   ├── UserRepository.java
│   └── TaskRepository.java
├── entity/                           # 实体类
│   ├── User.java
│   └── Task.java
├── dto/                              # 数据传输对象
│   ├── LoginRequest.java
│   └── AuthResponse.java
├── mq/                               # 消息队列
│   ├── TaskProducer.java
│   └── ResultConsumer.java
└── filter/                           # 过滤器
    └── JwtAuthenticationFilter.java
```
