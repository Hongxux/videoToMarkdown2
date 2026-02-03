package com.mvp.module2.fusion.websocket;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket 处理器
 * 
 * 功能：
 * 1. 管理客户端连接
 * 2. 推送任务状态更新
 * 3. 接收客户端命令（取消任务等）
 */
@Component
public class TaskWebSocketHandler extends TextWebSocketHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(TaskWebSocketHandler.class);
    
    // 用户会话映射：userId -> session
    private final ConcurrentHashMap<String, WebSocketSession> userSessions = new ConcurrentHashMap<>();
    
    // 任务订阅映射：taskId -> sessions
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> taskSubscribers = new ConcurrentHashMap<>();
    
    @Autowired
    private TaskQueueManager taskQueueManager;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String userId = getUserIdFromSession(session);
        if (userId != null) {
            userSessions.put(userId, session);
            logger.info("WebSocket connected: user={}, session={}", userId, session.getId());
        }
    }
    
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String userId = getUserIdFromSession(session);
        if (userId != null) {
            userSessions.remove(userId);
            
            // 清理该用户的任务订阅
            for (ConcurrentHashMap<String, WebSocketSession> subscribers : taskSubscribers.values()) {
                subscribers.remove(session.getId());
            }
            
            logger.info("WebSocket disconnected: user={}", userId);
        }
    }
    
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            Map<String, Object> payload = objectMapper.readValue(message.getPayload(), Map.class);
            String action = (String) payload.get("action");
            
            switch (action) {
                case "subscribe":
                    handleSubscribe(session, payload);
                    break;
                case "unsubscribe":
                    handleUnsubscribe(session, payload);
                    break;
                case "cancel":
                    handleCancel(session, payload);
                    break;
                case "ping":
                    sendMessage(session, Map.of("type", "pong"));
                    break;
                default:
                    logger.warn("Unknown action: {}", action);
            }
        } catch (Exception e) {
            logger.error("Error handling message", e);
        }
    }
    
    /**
     * 处理订阅请求
     */
    private void handleSubscribe(WebSocketSession session, Map<String, Object> payload) {
        String taskId = (String) payload.get("taskId");
        if (taskId != null) {
            taskSubscribers.computeIfAbsent(taskId, k -> new ConcurrentHashMap<>())
                .put(session.getId(), session);
            
            // 立即发送当前状态
            TaskQueueManager.TaskEntry task = taskQueueManager.getTask(taskId);
            if (task != null) {
                sendTaskUpdate(session, task);
            }
            
            logger.debug("Session {} subscribed to task {}", session.getId(), taskId);
        }
    }
    
    /**
     * 处理取消订阅请求
     */
    private void handleUnsubscribe(WebSocketSession session, Map<String, Object> payload) {
        String taskId = (String) payload.get("taskId");
        if (taskId != null) {
            ConcurrentHashMap<String, WebSocketSession> subscribers = taskSubscribers.get(taskId);
            if (subscribers != null) {
                subscribers.remove(session.getId());
            }
        }
    }
    
    /**
     * 处理取消任务请求
     */
    private void handleCancel(WebSocketSession session, Map<String, Object> payload) {
        String taskId = (String) payload.get("taskId");
        if (taskId != null) {
            boolean cancelled = taskQueueManager.cancelTask(taskId);
            sendMessage(session, Map.of(
                "type", "cancelResult",
                "taskId", taskId,
                "success", cancelled
            ));
        }
    }
    
    /**
     * 广播任务状态更新
     */
    public void broadcastTaskUpdate(TaskQueueManager.TaskEntry task) {
        // 1. 推送给订阅该任务的所有会话
        ConcurrentHashMap<String, WebSocketSession> subscribers = taskSubscribers.get(task.taskId);
        if (subscribers != null) {
            for (WebSocketSession session : subscribers.values()) {
                sendTaskUpdate(session, task);
            }
        }
        
        // 2. 推送给任务所属用户
        WebSocketSession userSession = userSessions.get(task.userId);
        if (userSession != null && userSession.isOpen()) {
            sendTaskUpdate(userSession, task);
        }
    }
    
    /**
     * 广播任务状态更新 (使用参数)
     */
    public void broadcastTaskUpdate(String taskId, String status, double progress, String message, String resultPath) {
        Map<String, Object> update = Map.of(
            "type", "taskUpdate",
            "taskId", taskId,
            "status", status,
            "progress", progress,
            "message", message != null ? message : "",
            "resultPath", resultPath != null ? resultPath : ""
        );
        
        // 广播给所有订阅者
        ConcurrentHashMap<String, WebSocketSession> subscribers = taskSubscribers.get(taskId);
        if (subscribers != null) {
            for (WebSocketSession session : subscribers.values()) {
                sendMessage(session, update);
            }
        }
        
        // 广播给所有连接的用户（简化处理）
        for (WebSocketSession session : userSessions.values()) {
            if (session.isOpen()) {
                sendMessage(session, update);
            }
        }
    }
    
    /**
     * 发送任务状态更新
     */
    private void sendTaskUpdate(WebSocketSession session, TaskQueueManager.TaskEntry task) {
        Map<String, Object> update = Map.of(
            "type", "taskUpdate",
            "taskId", task.taskId,
            "status", task.status.name(),
            "progress", task.progress,
            "message", task.statusMessage != null ? task.statusMessage : "",
            "resultPath", task.resultPath != null ? task.resultPath : "",
            "errorMessage", task.errorMessage != null ? task.errorMessage : ""
        );
        sendMessage(session, update);
    }
    
    /**
     * 发送消息 (同步以避免并发写入)
     */
    private synchronized void sendMessage(WebSocketSession session, Map<String, Object> payload) {
        if (session.isOpen()) {
            try {
                String json = objectMapper.writeValueAsString(payload);
                session.sendMessage(new TextMessage(json));
            } catch (IOException e) {
                logger.error("Error sending message", e);
            }
        }
    }
    
    /**
     * 从会话中获取用户ID
     */
    private String getUserIdFromSession(WebSocketSession session) {
        // 从URI参数或header中获取用户ID
        // 例如: ws://localhost:8080/ws?userId=xxx
        String query = session.getUri().getQuery();
        if (query != null) {
            for (String param : query.split("&")) {
                String[] pair = param.split("=");
                if (pair.length == 2 && "userId".equals(pair[0])) {
                    return pair[1];
                }
            }
        }
        return session.getId(); // fallback
    }
    
    /**
     * 获取当前连接数
     */
    public int getConnectionCount() {
        return userSessions.size();
    }
}
