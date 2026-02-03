# VideoToMarkdown - 快速启动指南

## 1. 启动RabbitMQ

```bash
# 启动RabbitMQ容器
docker-compose up -d

# 查看RabbitMQ日志
docker-compose logs -f rabbitmq

# 访问RabbitMQ管理界面
# URL: http://localhost:15672
# 用户名: admin
# 密码: admin123
```

## 2. 初始化MySQL数据库

```bash
# 进入MySQL
mysql -u root -p

# 执行初始化脚本
source d:/videoToMarkdownTest2/database/schema.sql

# 或者
mysql -u root -p < d:/videoToMarkdownTest2/database/schema.sql
```

## 3. 配置环境变量

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑.env文件，填写实际值
```

## 4. 启动Java后端

```bash
cd java-backend

# 首次运行需要下载依赖
mvn clean install

# 启动应用
mvn spring-boot:run

# 或打包后运行
mvn clean package
java -jar target/backend-0.0.1-SNAPSHOT.jar
```

## 5. 测试连接

```bash
# 健康检查
curl http://localhost:8080/actuator/health

# Swagger UI
# http://localhost:8080/swagger-ui.html
```

## 常用命令

```bash
# 停止RabbitMQ
docker-compose down

# 重启RabbitMQ
docker-compose restart

# 查看RabbitMQ队列
# 访问 http://localhost:15672 -> Queues

# 查看Java日志
tail -f java-backend/logs/app.log
```
