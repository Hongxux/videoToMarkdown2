import api from './request'

export const authAPI = {
  // 注册
  register(data) {
    return api.post('/auth/register', data)
  },
  
  // 登录
  login(data) {
    return api.post('/auth/login', data)
  }
}

export const taskAPI = {
  // 创建任务
  createTask(videoUrl) {
    return api.post('/tasks', { videoUrl })
  },
  
  // 获取任务列表
  getTaskList() {
    return api.get('/tasks')
  },
  
  // 获取任务详情
  getTaskDetail(taskId) {
    return api.get(`/tasks/${taskId}`)
  },
  
  // 获取配额
  getQuota() {
    return api.get('/tasks/quota')
  }
}
