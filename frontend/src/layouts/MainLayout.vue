<template>
  <div class="main-layout">
    <el-container>
      <!-- 顶部导航 -->
      <el-header class="header">
        <div class="header-content">
          <div class="logo">
            <el-icon class="logo-icon"><VideoCamera /></el-icon>
            <span>视频转文字稿系统</span>
          </div>
          <div class="header-right">
            <el-space>
              <el-text>{{ authStore.username }}</el-text>
              <el-dropdown @command="handleCommand">
                <el-button circle>
                  <el-icon><User /></el-icon>
                </el-button>
                <template #dropdown>
                  <el-dropdown-menu>
                    <el-dropdown-item command="profile">个人中心</el-dropdown-item>
                    <el-dropdown-item command="logout" divided>退出登录</el-dropdown-item>
                  </el-dropdown-menu>
                </template>
              </el-dropdown>
            </el-space>
          </div>
        </div>
      </el-header>

      <el-container>
        <!-- 侧边栏 -->
        <el-aside width="200px" class="aside">
          <el-menu
            :default-active="$route.path"
            router
            class="sidebar-menu"
          >
            <el-menu-item index="/tasks">
              <el-icon><List /></el-icon>
              <span>任务列表</span>
            </el-menu-item>
            <el-menu-item index="/profile">
              <el-icon><User /></el-icon>
              <span>个人中心</span>
            </el-menu-item>
          </el-menu>
        </el-aside>

        <!-- 主内容区 -->
        <el-main class="main">
          <router-view />
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script setup>
import { useRouter } from 'vue-router'
import { ElMessageBox } from 'element-plus'
import { useAuthStore } from '@/stores/auth'

const router = useRouter()
const authStore = useAuthStore()

const handleCommand = (command) => {
  if (command === 'logout') {
    ElMessageBox.confirm('确定要退出登录吗?', '提示', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning'
    }).then(() => {
      authStore.logout()
      router.push('/login')
    })
  } else if (command === 'profile') {
    router.push('/profile')
  }
}
</script>

<style scoped>
.main-layout {
  height: 100vh;
}

.header {
  background: #fff;
  border-bottom: 1px solid #e4e7ed;
  display: flex;
  align-items: center;
  padding: 0 20px;
}

.header-content {
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.logo {
  display: flex;
  align-items: center;
  font-size: 18px;
  font-weight: bold;
  color: #303133;
}

.logo-icon {
  font-size: 24px;
  margin-right: 8px;
  color: #409eff;
}

.aside {
  background: #f5f7fa;
  border-right: 1px solid #e4e7ed;
}

.sidebar-menu {
  border: none;
  background: #f5f7fa;
}

.main {
  background: #f0f2f5;
  padding: 20px;
}
</style>
