# VideoToMarkdown Frontend

Vue 3前端界面,提供用户注册、登录、任务管理功能。

## 快速开始

### 1. 安装依赖

```bash
cd frontend
npm install
```

### 2. 启动开发服务器

```bash
npm run dev
```

访问: http://localhost:5173

### 3. 构建生产版本

```bash
npm run build
```

构建产物在 `dist/` 目录

## 技术栈

- **框架**: Vue 3 (Composition API)
- **路由**: Vue Router 4
- **状态管理**: Pinia
- **UI库**: Element Plus
- **HTTP**: Axios
- **构建工具**: Vite

## 项目结构

```
src/
├── api/              # API请求层
│   ├── request.js    # Axios实例
│   └── index.js      # API接口定义
├── stores/           # Pinia状态管理
│   ├── auth.js       # 认证状态
│   └── task.js       # 任务状态
├── router/           # 路由配置
│   └── index.js
├── layouts/          # 布局组件
│   └── MainLayout.vue
├── views/            # 页面组件
│   ├── Login.vue     # 登录页
│   ├── Register.vue  # 注册页
│   ├── TaskList.vue  # 任务列表
│   ├── TaskDetail.vue # 任务详情
│   └── Profile.vue   # 个人中心
├── App.vue           # 根组件
└── main.js           # 入口文件
```

## 功能说明

### 1. 用户认证
- ✅ 邀请码注册
- ✅ 邮箱+密码登录
- ✅ JWT Token自动管理
- ✅ 路由守卫

### 2. 任务管理
- ✅ 创建视频处理任务
- ✅ 查看任务列表
- ✅ 实时进度更新 (自动刷新)
- ✅ 任务详情查看
- ✅ 结果文件下载

### 3. 个人中心
- ✅ 用户信息展示
- ✅ 使用统计 (今日剩余次数)
- ✅ 最近任务记录

## 开发指南

### API代理配置

`vite.config.js`中配置了API代理:
```javascript
proxy: {
  '/api': {
    target: 'http://localhost:8080',  // Java后端地址
    changeOrigin: true
  }
}
```

### 状态管理

使用Pinia管理全局状态:
- `useAuthStore()`: 认证相关
- `useTaskStore()`: 任务相关

### 路由守卫

自动检查JWT Token,未登录跳转到登录页。

## 配置说明

### 环境变量 (可选)

创建`.env.local`:
```
VITE_API_BASE_URL=http://localhost:8080
```

### 默认配置

- 开发端口: 5173
- API代理: /api -> http://localhost:8080

## 常见问题

### 1. API请求404
确保Java后端已启动 (http://localhost:8080)

### 2. 跨域问题
使用Vite代理,前端通过 `/api/*` 访问后端

### 3. Token过期
Token有效期24小时,过期后自动跳转登录页

## 后续优化

- [ ] WebSocket实时进度推送 (替代轮询)
- [ ] 更丰富的数据可视化
- [ ] 任务导出功能
- [ ] 暗黑模式支持
