import { defineStore } from 'pinia'
import { ref } from 'vue'
import { taskAPI } from '@/api'

export const useTaskStore = defineStore('task', () => {
  // State
  const tasks = ref([])
  const currentTask = ref(null)
  const quota = ref({ dailyLimit: 3, remaining: 3, used: 0 })
  const loading = ref(false)

  // Actions
  async function fetchTaskList() {
    loading.value = true
    try {
      tasks.value = await taskAPI.getTaskList()
    } finally {
      loading.value = false
    }
  }

  async function fetchTaskDetail(taskId) {
    loading.value = true
    try {
      currentTask.value = await taskAPI.getTaskDetail(taskId)
      return currentTask.value
    } finally {
      loading.value = false
    }
  }

  async function createTask(videoUrl) {
    const response = await taskAPI.createTask(videoUrl)
    await fetchTaskList() // 刷新列表
    await fetchQuota() // 刷新配额
    return response
  }

  async function fetchQuota() {
    quota.value = await taskAPI.getQuota()
  }

  return {
    tasks,
    currentTask,
    quota,
    loading,
    fetchTaskList,
    fetchTaskDetail,
    createTask,
    fetchQuota
  }
})
