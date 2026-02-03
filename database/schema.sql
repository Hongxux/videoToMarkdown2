-- 视频转文字稿系统 - 数据库初始化脚本
-- 创建日期: 2026-01-22
-- 数据库: MySQL 8.0+

-- 创建数据库
CREATE DATABASE IF NOT EXISTS video_to_markdown 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE video_to_markdown;

-- ============================================================================
-- 用户表
-- ============================================================================
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '用户ID',
    email VARCHAR(255) UNIQUE NOT NULL COMMENT '邮箱',
    password_hash VARCHAR(255) NOT NULL COMMENT '密码哈希',
    username VARCHAR(100) COMMENT '用户名',
    role ENUM('USER', 'ADMIN') DEFAULT 'USER' COMMENT '角色',
    invite_code VARCHAR(50) COMMENT '使用的邀请码',
    email_verified BOOLEAN DEFAULT TRUE COMMENT '邮箱是否验证(v1跳过验证)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_email (email),
    INDEX idx_role (role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';

-- ============================================================================
-- 任务表
-- ============================================================================
CREATE TABLE tasks (
    task_id VARCHAR(36) PRIMARY KEY COMMENT '任务ID(UUID)',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    video_url VARCHAR(1000) NOT NULL COMMENT '视频URL',
    video_title VARCHAR(500) COMMENT '视频标题',
    status ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED') NOT NULL DEFAULT 'PENDING' COMMENT '任务状态',
    progress FLOAT DEFAULT 0.0 COMMENT '处理进度(0.0-1.0)',
    result_path VARCHAR(500) COMMENT '结果文件路径',
    error_msg TEXT COMMENT '错误信息',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    completed_at TIMESTAMP NULL COMMENT '完成时间',
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    INDEX idx_completed_at (completed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务表';

-- ============================================================================
-- 邀请码表
-- ============================================================================
CREATE TABLE invite_codes (
    code_id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '邀请码ID',
    invite_code VARCHAR(50) UNIQUE NOT NULL COMMENT '邀请码',
    created_by BIGINT COMMENT '创建者ID',
    used_by BIGINT COMMENT '使用者ID',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    used_at TIMESTAMP NULL COMMENT '使用时间',
    is_used BOOLEAN DEFAULT FALSE COMMENT '是否已使用',
    FOREIGN KEY (created_by) REFERENCES users(user_id) ON DELETE SET NULL,
    FOREIGN KEY (used_by) REFERENCES users(user_id) ON DELETE SET NULL,
    INDEX idx_code (invite_code),
    INDEX idx_is_used (is_used)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='邀请码表';

-- ============================================================================
-- 使用记录表 (用于限流统计)
-- ============================================================================
CREATE TABLE usage_logs (
    log_id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '日志ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    task_id VARCHAR(36) COMMENT '任务ID',
    action_date DATE NOT NULL COMMENT '操作日期',
    action_count INT DEFAULT 1 COMMENT '当日操作次数',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_date (user_id, action_date),
    INDEX idx_user_date (user_id, action_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='使用记录表';

-- ============================================================================
-- 初始化数据: 创建管理员账号
-- ============================================================================
-- 密码: admin123 (BCrypt加密后的哈希)
INSERT INTO users (email, password_hash, username, role, email_verified) VALUES
('admin@example.com', '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lIYDGTAQ8LGfU7ywC', 'Admin', 'ADMIN', TRUE);

-- ============================================================================
-- 初始化数据: 生成10个测试邀请码
-- ============================================================================
INSERT INTO invite_codes (invite_code, created_by) VALUES
('INVITE-TEST-001', 1),
('INVITE-TEST-002', 1),
('INVITE-TEST-003', 1),
('INVITE-TEST-004', 1),
('INVITE-TEST-005', 1),
('INVITE-TEST-006', 1),
('INVITE-TEST-007', 1),
('INVITE-TEST-008', 1),
('INVITE-TEST-009', 1),
('INVITE-TEST-010', 1);

-- ============================================================================
-- 查询测试: 验证表结构
-- ============================================================================
-- 查看所有表
SHOW TABLES;

-- 查看用户表结构
DESC users;

-- 查看可用邀请码
SELECT invite_code, is_used FROM invite_codes WHERE is_used = FALSE;
