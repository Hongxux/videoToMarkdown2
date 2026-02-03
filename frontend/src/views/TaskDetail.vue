<template>
  <div class="task-detail-container">
    <el-card v-loading="taskStore.loading">
      <template #header>
        <div class="card-header">
          <el-button link @click="$router.back()">
            <el-icon><ArrowLeft /></el-icon> 返回列表
          </el-button>
          <h2>任务详情</h2>
        </div>
      </template>

      <el-descriptions v-if="task" :column="2" border>
        <el-descriptions-item label="任务ID">
          <el-text>{{ task.taskId }}</el-text>
        </el-descriptions-item>

        <el-descriptions-item label="状态">
          <el-tag :type="getStatusType(task.status)" size="large">
            {{ getStatusText(task.status) }}
          </el-tag>
        </el-descriptions-item>

        <el-descriptions-item label="视频URL" :span="2">
          <el-link :href="task.videoUrl" target="_blank" type="primary">
            {{ task.videoUrl }}
          </el-link>
        </el-descriptions-item>

        <el-descriptions-item label="创建时间">
          {{ formatDate(task.createdAt) }}
        </el-descriptions-item>

        <el-descriptions-item label="完成时间">
          {{ task.completedAt ? formatDate(task.completedAt) : '-' }}
        </el-descriptions-item>

        <el-descriptions-item label="处理进度" :span="2">
          <el-progress 
            :percentage="Math.round(task.progress * 100)"
            :status="task.status === 'FAILED' ? 'exception' : (task.status === 'COMPLETED' ? 'success' : undefined)"
          />
        </el-descriptions-item>

        <el-descriptions-item v-if="task.errorMsg" label="错误信息" :span="2">
          <el-alert type="error" :closable="false">
            {{ task.errorMsg }}
          </el-alert>
        </el-descriptions-item>

        <el-descriptions-item v-if="task.resultPath" label="结果文件" :span="2">
          <el-space>
            <el-text>{{ task.resultPath.split('/').pop() }}</el-text>
            <el-button type="primary" size="small" @click="downloadResult">
              <el-icon><Download /></el-icon> 下载
            </el-button>
          </el-space>
        </el-descriptions-item>
      </el-descriptions>

      <!-- 实时日志 (仅处理中状态) -->
      <el-divider v-if="task && task.status === 'PROCESSING'" />
      <div v-if="task && task.status === 'PROCESSING'" class="log-section">
        <h3>处理日志</h3>
        <el-alert type="info" :closable="false">
          <el-text>任务正在处理中,预计需要15-20分钟...</el-text>
          <br>
          <el-text type="info" size="small">页面将自动刷新进度</el-text>
        </el-alert>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import { useTaskStore } from '@/stores/task'

const route = useRoute()
const taskStore = useTaskStore()

const task = ref(null)
const refreshTimer = ref(null)

onMounted(async () => {
  await loadTaskDetail()
  
  // 自动刷新 (仅处理中状态)
  refreshTimer.value = setInterval(async () => {
    if (task.value && (task.value.status === 'PROCESSING' || task.value.status === 'PENDING')) {
      await loadTaskDetail()
    }
  }, 3000) // 每3秒刷新一次
})

onUnmounted(() => {
  if (refreshTimer.value) {
    clearInterval(refreshTimer.value)
  }
})

const loadTaskDetail = async () => {
  try {
    task.value = await taskStore.fetchTaskDetail(route.params.id)
  } catch (error) {
    ElMessage.error('加载任务详情失败')
  }
}

const downloadResult = () => {
  if (!task.value?.taskId) return
  
  // 构造下载URL (后端已支持将 notes 目录打包为 ZIP 下载)
  const downloadUrl = `http://localhost:8080/api/tasks/${task.value.taskId}/download`
  window.open(downloadUrl, '_blank')
}

const getStatusType = (status) => {
  const types = {
    'PENDING': 'info',
    'PROCESSING': 'warning',
    'COMPLETED': 'success',
    'FAILED': 'danger'
  }
  return types[status] || 'info'
}

const getStatusText = (status) => {
  const texts = {
    'PENDING': '等待处理',
    'PROCESSING': '处理中',
    'COMPLETED': '已完成',
    'FAILED': '处理失败'
  }
  return texts[status] || status
}

const formatDate = (dateString) => {
  return new Date(dateString).toLocaleString('zh-CN')
}
</script>

<style scoped>
.task-detail-container {
  max-width: 900px;
  margin: 0 auto;
}

.card-header {
  display: flex;
  align-items: center;
  gap: 16px;
}

.card-header h2 {
  margin: 0;
}

.log-section {
  margin-top: 20px;
}

.log-section h3 {
  margin: 0 0 16px 0;
}
</style>
