package com.mvp.module2.fusion.queue;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskQueueManagerStateMachineTest {

    @Test
    void repeatedCompleteShouldBeIdempotent() throws Exception {
        TaskQueueManager queueManager = new TaskQueueManager();
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_state_machine_complete",
                "https://example.com/state-machine-complete",
                "var/tmp-state-machine-complete",
                TaskQueueManager.Priority.NORMAL
        );

        TaskQueueManager.TaskEntry polled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(polled);

        TaskQueueManager.TaskTransitionResult first = queueManager.completeTask(task.taskId, "out-first.md");
        TaskQueueManager.TaskTransitionResult second = queueManager.completeTask(task.taskId, "out-second.md");

        assertTrue(first.isApplied());
        assertTrue(second.isNoOp());
        assertEquals(TaskQueueManager.TaskStatus.COMPLETED, queueManager.getTask(task.taskId).status);
        assertEquals("out-first.md", queueManager.getTask(task.taskId).resultPath);
    }

    @Test
    void repeatedCancelAndFinalizeShouldBeIdempotent() {
        TaskQueueManager queueManager = new TaskQueueManager();
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_state_machine_cancel",
                "https://example.com/state-machine-cancel",
                "var/tmp-state-machine-cancel",
                TaskQueueManager.Priority.NORMAL
        );

        TaskQueueManager.TaskTransitionResult cancelFirst = queueManager.cancelTaskTransition(task.taskId);
        TaskQueueManager.TaskTransitionResult cancelSecond = queueManager.cancelTaskTransition(task.taskId);
        TaskQueueManager.TaskTransitionResult finalizeFirst = queueManager.finalizeCancelledTask(task.taskId, "任务已取消，处理已停止");
        TaskQueueManager.TaskTransitionResult finalizeSecond = queueManager.finalizeCancelledTask(task.taskId, "任务已取消，处理已停止");

        assertTrue(cancelFirst.isApplied());
        assertTrue(cancelSecond.isNoOp());
        assertTrue(finalizeFirst.isApplied());
        assertTrue(finalizeSecond.isNoOp());
        assertEquals(TaskQueueManager.TaskStatus.CANCELLED, queueManager.getTask(task.taskId).status);
    }

    @Test
    void terminalTaskShouldIgnoreProgressUpdate() throws Exception {
        TaskQueueManager queueManager = new TaskQueueManager();
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_state_machine_progress",
                "https://example.com/state-machine-progress",
                "var/tmp-state-machine-progress",
                TaskQueueManager.Priority.NORMAL
        );

        TaskQueueManager.TaskEntry polled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(polled);
        TaskQueueManager.TaskTransitionResult completed = queueManager.completeTask(task.taskId, "out-progress.md");

        boolean progressApplied = queueManager.updateProgress(task.taskId, 0.33, "should be ignored");

        assertTrue(completed.isApplied());
        assertFalse(progressApplied);
        assertEquals(1.0, queueManager.getTask(task.taskId).progress);
        assertEquals("处理完成", queueManager.getTask(task.taskId).statusMessage);
    }
}
