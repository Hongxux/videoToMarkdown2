<template>
  <div class="profile-container">
    <el-row :gutter="20">
      <!-- 用户信息卡片 -->
      <el-col :span="12">
        <el-card>
          <template #header>
            <h3>用户信息</h3>
          </template>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="用户名">
              {{ authStore.userInfo.username }}
            </el-descriptions-item>
            <el-descriptions-item label="邮箱">
              {{ authStore.userInfo.email }}
            </el-descriptions-item>
            <el-descriptions-item label="角色">
              <el-tag>{{ authStore.userInfo.role === 'ADMIN' ? '管理员' : '普通用户' }}</el-tag>
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>

      <!-- 使用统计 -->
      <el-col :span="12">
        <el-card>
          <template #header>
            <h3>使用统计</h3>
          </template>
          <el-statistic-group direction="horizontal">
            <el-statistic title="今日剩余" :value="taskStore.quota.remaining">
              <template #suffix>/ {{ taskStore.quota.dailyLimit }}</template>
            </el-statistic>
            <el-statistic title="今日已用" :value="taskStore.quota.used" />
            <el-statistic title="总任务数" :value="taskStore.tasks.length" />
          </el-statistic-group>
        </el-card>
      </el-col>
    </el-row>

    <!-- 最近任务 -->
    <el-card style="margin-top: 20px">
      <template #header>
        <h3>最近任务</h3>
      </template>
      <el-table :data="recentTasks" stripe>
        <el-table-column prop="taskId" label="任务ID" width="120">
          <template #default="{ row }">
            <el-text type="info" size="small">{{ row.taskId.substring(0, 8) }}</el-text>
          </template>
        </el-table-column>
        <el-table-column prop="videoUrl" label="视频" min-width="200">
          <template #default="{ row }">
            {{ row.videoUrl.substring(0, 50) }}...
          </template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="getStatusType(row.status)" size="small">
              {{ getStatusText(row.status) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="createdAt" label="创建时间" width="180">
          <template #default="{ row }">
            {{ formatDate(row.createdAt) }}
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { computed, onMounted } from 'vue'
import { useAuthStore } from '@/stores/auth'
import { useTaskStore } from '@/stores/task'

const authStore = useAuthStore()
const taskStore = useTaskStore()

const recentTasks = computed(() => {
  return taskStore.tasks.slice(0, 5) // 最近5个任务
})

onMounted(async () => {
  await taskStore.fetchTaskList()
  await taskStore.fetchQuota()
})

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
.profile-container {
  max-width: 1200px;
  margin: 0 auto;
}

h3 {
  margin: 0;
}
</style>
