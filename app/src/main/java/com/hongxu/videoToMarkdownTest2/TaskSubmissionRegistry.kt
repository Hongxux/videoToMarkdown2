package com.hongxu.videoToMarkdownTest2

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update

enum class SubmissionEventType {
    STARTED,
    SUCCEEDED,
    FAILED,
    CANCELLED
}

data class SubmissionEvent(
    val submissionId: String,
    val taskId: String?,
    val title: String,
    val type: SubmissionEventType,
    val message: String
)

/**
 * App 内提交态注册表。
 * Foreground Service 写入，Compose 页面订阅显示骨架、胶囊和调度中心。
 */
object TaskSubmissionRegistry {
    private val _activeHints = MutableStateFlow<Map<String, ActiveSubmissionHint>>(emptyMap())
    val activeHints: StateFlow<Map<String, ActiveSubmissionHint>> = _activeHints.asStateFlow()

    private val _events = MutableSharedFlow<SubmissionEvent>(
        replay = 0,
        extraBufferCapacity = 32
    )
    val events: SharedFlow<SubmissionEvent> = _events.asSharedFlow()

    fun upsert(hint: ActiveSubmissionHint) {
        _activeHints.update { old ->
            old + (hint.workId to hint)
        }
    }

    fun remove(submissionId: String) {
        _activeHints.update { old ->
            old - submissionId
        }
    }

    fun clearAll() {
        _activeHints.value = emptyMap()
    }

    fun tryEmitEvent(event: SubmissionEvent) {
        _events.tryEmit(event)
    }
}
