import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { authAPI } from '@/api'

export const useAuthStore = defineStore('auth', () => {
  // State
  const token = ref(localStorage.getItem('token') || '')
  const userInfo = ref(JSON.parse(localStorage.getItem('userInfo') || '{}'))

  // Getters
  const isAuthenticated = computed(() => !!token.value)
  const username = computed(() => userInfo.value.username || '')
  const email = computed(() => userInfo.value.email || '')

  // Actions
  async function login(credentials) {
    const response = await authAPI.login(credentials)
    token.value = response.token
    userInfo.value = {
      email: response.email,
      username: response.username,
      role: response.role,
      userId: response.userId
    }
    localStorage.setItem('token', response.token)
    localStorage.setItem('userInfo', JSON.stringify(userInfo.value))
    return response
  }

  async function register(data) {
    const response = await authAPI.register(data)
    token.value = response.token
    userInfo.value = {
      email: response.email,
      username: response.username,
      role: response.role,
      userId: response.userId
    }
    localStorage.setItem('token', response.token)
    localStorage.setItem('userInfo', JSON.stringify(userInfo.value))
    return response
  }

  function logout() {
    token.value = ''
    userInfo.value = {}
    localStorage.removeItem('token')
    localStorage.removeItem('userInfo')
  }

  return {
    token,
    userInfo,
    isAuthenticated,
    username,
    email,
    login,
    register,
    logout
  }
})
