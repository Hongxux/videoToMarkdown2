<template>
  <div class="task-list-container">
    <!-- 顶部操作栏 -->
    <el-card class="header-card" shadow="never">
      <div class="header-content">
        <div>
          <h2>我的任务</h2>
          <el-text type="info">
            今日剩余次数: <el-tag type="success">{{ taskStore.quota.remaining }}/{{ taskStore.quota.dailyLimit }}</el-tag>
          </el-text>
        </div>
        <el-button type="primary" size="large" @click="showCreateDialog = true" :disabled="taskStore.quota.remaining === 0">
          <el-icon><Plus /></el-icon>
          创建任务
        </el-button>
      </div>
    </el-card>

    <!-- 任务列表 -->
    <el-card class="list-card" v-loading="taskStore.loading">
      <el-empty v-if="taskStore.tasks.length === 0" description="还没有任务,点击上方按钮创建第一个任务" />
      
      <el-table v-else :data="taskStore.tasks" stripe>
        <el-table-column prop="taskId" label="任务ID" width="120">
          <template #default="{ row }">
            <el-text type="info" size="small">{{ row.taskId.substring(0, 8) }}</el-text>
          </template>
        </el-table-column>

        <el-table-column prop="videoUrl" label="视频URL" min-width="200">
          <template #default="{ row }">
            <el-link :href="row.videoUrl" target="_blank" type="primary">
              {{ row.videoUrl.substring(0, 50) }}...
            </el-link>
          </template>
        </el-table-column>

        <el-table-column prop="status" label="状态" width="120">
          <template #default="{ row }">
            <el-tag :type="getStatusType(row.status)">{{ getStatusText(row.status) }}</el-tag>
          </template>
        </el-table-column>

        <el-table-column prop="progress" label="进度" width="150">
          <template #default="{ row }">
            <el-progress :percentage="Math.round(row.progress * 100)" :status="row.status === 'FAILED' ? 'exception' : undefined" />
          </template>
        </el-table-column>

        <el-table-column prop="createdAt" label="创建时间" width="180">
          <template #default="{ row }">
            {{ formatDate(row.createdAt) }}
          </template>
        </el-table-column>

        <el-table-column label="操作" width="120" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" @click="viewDetail(row.taskId)">
              查看详情
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 创建任务对话框 -->
    <el-dialog v-model="showCreateDialog" title="创建任务" width="500px">
      <el-form :model="createForm" :rules="createRules" ref="createFormRef">
        <el-form-item label="视频URL" prop="videoUrl">
          <el-input
            v-model="createForm.videoUrl"
            placeholder="请输入Bilibili或YouTube视频链接"
            clearable
          />
        </el-form-item>
        <el-alert
          title="支持的平台"
          type="info"
          :closable="false"
          show-icon
        >
          <ul style="margin: 0; padding-left: 20px;">
            <li>Bilibili: https://www.bilibili.com/video/BVxxx</li>
            <li>YouTube: https://www.youtube.com/watch?v=xxx</li>
          </ul>
        </el-alert>
      </el-form>
      <template #footer>
        <el-button @click="showCreateDialog = false">取消</el-button>
        <el-button type="primary" @click="handleCreate" :loading="creating">提交</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { useTaskStore } from '@/stores/task'

const router = useRouter()
const taskStore = useTaskStore()

const showCreateDialog = ref(false)
const creating = ref(false)
const createFormRef = ref(null)

const createForm = ref({
  videoUrl: ''
})

const createRules = {
  videoUrl: [
    { required: true, message: '请输入视频URL', trigger: 'blur' },
    { pattern: /(bilibili|youtube)\.com/, message: '请输入有效的Bilibili或YouTube链接', trigger: 'blur' }
  ]
}

onMounted(async () => {
  await taskStore.fetchTaskList()
  await taskStore.fetchQuota()
  
  // 轮询更新任务状态
  setInterval(() => {
    taskStore.fetchTaskList()
  }, 5000) // 每5秒刷新一次
})

const handleCreate = async () => {
  if (!createFormRef.value) return
  
  await createFormRef.value.validate(async (valid) => {
    if (!valid) return
    
    creating.value = true
    try {
      await taskStore.createTask(createForm.value.videoUrl)
      ElMessage.success('任务创建成功')
      showCreateDialog.value = false
      createForm.value.videoUrl = ''
    } catch (error) {
      ElMessage.error(error.response?.data?.error || '任务创建失败')
    } finally {
      creating.value = false
    }
  })
}

const viewDetail = (taskId) => {
  router.push(`/tasks/${taskId}`)
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
    'PENDING': '等待中',
    'PROCESSING': '处理中',
    'COMPLETED': '已完成',
    'FAILED': '失败'
  }
  return texts[status] || status
}

const formatDate = (dateString) => {
  return new Date(dateString).toLocaleString('zh-CN')
}
</script>

<style scoped>
.task-list-container {
  max-width: 1400px;
  margin: 0 auto;
}

.header-card {
  margin-bottom: 20px;
}

.header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.header-content h2 {
  margin: 0 0 8px 0;
}

.list-card {
  min-height: 400px;
}
</style>
