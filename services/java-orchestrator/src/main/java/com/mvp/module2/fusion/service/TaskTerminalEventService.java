package com.mvp.module2.fusion.service;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class TaskTerminalEventService {

    private static final int MAX_REPLAY_EVENTS = 100;

    private final TaskTerminalEventRepository repository;

    public TaskTerminalEventService(TaskTerminalEventRepository repository) {
        this.repository = repository;
    }

    public Map<String, Object> enqueue(TaskQueueManager.TaskEntry task, Map<String, Object> taskPayload) {
        if (task == null || taskPayload == null || !isReplayableTerminalStatus(task.status)) {
            return null;
        }
        LinkedHashMap<String, Object> payload = new LinkedHashMap<>(taskPayload);
        payload.put("terminalStatus", task.status.name());
        TaskTerminalEventRepository.TerminalEventRecord event = repository.appendEvent(
                task.userId,
                task.taskId,
                task.status.name(),
                payload
        );
        return event != null ? new LinkedHashMap<>(event.payload()) : null;
    }

    public List<Map<String, Object>> replayPendingEvents(String userId, long lastAckedEventId) {
        String normalizedUserId = normalize(userId);
        if (normalizedUserId.isEmpty()) {
            return List.of();
        }
        acknowledge(normalizedUserId, lastAckedEventId);
        List<TaskTerminalEventRepository.TerminalEventRecord> records =
                repository.listEventsAfter(normalizedUserId, Math.max(0L, lastAckedEventId), MAX_REPLAY_EVENTS);
        if (records.isEmpty()) {
            return List.of();
        }
        List<Map<String, Object>> payloads = new ArrayList<>(records.size());
        for (TaskTerminalEventRepository.TerminalEventRecord record : records) {
            payloads.add(new LinkedHashMap<>(record.payload()));
        }
        return payloads;
    }

    public void acknowledge(String userId, long eventId) {
        repository.acknowledgeThrough(userId, eventId);
    }

    private boolean isReplayableTerminalStatus(TaskQueueManager.TaskStatus status) {
        if (status == null) {
            return false;
        }
        return status == TaskQueueManager.TaskStatus.COMPLETED
                || status == TaskQueueManager.TaskStatus.FAILED;
    }

    private String normalize(String rawValue) {
        return rawValue == null ? "" : rawValue.trim();
    }
}
