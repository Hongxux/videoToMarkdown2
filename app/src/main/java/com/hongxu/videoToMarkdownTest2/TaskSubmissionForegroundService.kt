package com.hongxu.videoToMarkdownTest2

import android.app.NotificationChannel
import android.app.ForegroundServiceStartNotAllowedException
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.json.JSONObject
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.roundToInt

class TaskSubmissionForegroundService : Service() {

    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val submissionJobs = ConcurrentHashMap<String, Job>()
    private val submissionTaskIds = ConcurrentHashMap<String, String>()
    private val mobileUserId by lazy { MobileClientIdentity.resolveUserId(applicationContext) }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        ensureNotificationChannels()
        ensureServiceForeground()
        when (intent?.action) {
            ACTION_START_URL -> {
                val videoUrl = intent.getStringExtra(EXTRA_VIDEO_URL)?.trim().orEmpty()
                if (videoUrl.isNotEmpty()) {
                    val submissionId = intent.getStringExtra(EXTRA_SUBMISSION_ID)
                        ?.takeIf { it.isNotBlank() }
                        ?: UUID.randomUUID().toString()
                    val preferredTitle = intent.getStringExtra(EXTRA_PREFERRED_TITLE).orEmpty()
                    startSubmission(
                        submissionId = submissionId,
                        mode = SubmissionMode.URL,
                        preferredTitle = preferredTitle,
                        videoUrl = videoUrl,
                        uploadUri = null
                    )
                }
            }

            ACTION_START_UPLOAD -> {
                val rawUri = intent.getStringExtra(EXTRA_UPLOAD_URI)?.trim().orEmpty()
                if (rawUri.isNotEmpty()) {
                    val submissionId = intent.getStringExtra(EXTRA_SUBMISSION_ID)
                        ?.takeIf { it.isNotBlank() }
                        ?: UUID.randomUUID().toString()
                    val preferredTitle = intent.getStringExtra(EXTRA_PREFERRED_TITLE).orEmpty()
                    startSubmission(
                        submissionId = submissionId,
                        mode = SubmissionMode.UPLOAD,
                        preferredTitle = preferredTitle,
                        videoUrl = null,
                        uploadUri = Uri.parse(rawUri)
                    )
                }
            }

            ACTION_TRACK_TASK -> {
                val rawTaskId = intent.getStringExtra(EXTRA_TASK_ID)?.trim().orEmpty()
                val preferredTitle = intent.getStringExtra(EXTRA_PREFERRED_TITLE).orEmpty()
                val submissionId = intent.getStringExtra(EXTRA_SUBMISSION_ID)
                    ?.takeIf { it.isNotBlank() }
                    ?: UUID.randomUUID().toString()
                if (rawTaskId.isNotEmpty()) {
                    trackExistingTask(
                        submissionId = submissionId,
                        taskId = rawTaskId,
                        preferredTitle = preferredTitle
                    )
                }
            }

            ACTION_CANCEL -> {
                val submissionId = intent.getStringExtra(EXTRA_SUBMISSION_ID)?.trim().orEmpty()
                if (submissionId.isNotEmpty()) {
                    cancelSubmission(submissionId)
                }
            }
        }
        return START_STICKY
    }

    override fun onDestroy() {
        submissionJobs.values.forEach { it.cancel() }
        serviceScope.coroutineContext[Job]?.cancel()
        super.onDestroy()
    }

    private fun startSubmission(
        submissionId: String,
        mode: SubmissionMode,
        preferredTitle: String,
        videoUrl: String?,
        uploadUri: Uri?
    ) {
        if (submissionJobs.containsKey(submissionId)) {
            return
        }
        val title = preferredTitle.ifBlank {
            if (mode == SubmissionMode.UPLOAD) "本地视频任务" else "链接视频任务"
        }
        TaskSubmissionRegistry.upsert(
            ActiveSubmissionHint(
                workId = submissionId,
                taskId = null,
                title = title,
                phaseText = if (mode == SubmissionMode.UPLOAD) {
                    "正在上传视频..."
                } else {
                    "正在提交任务..."
                },
                progressPercent = null,
                running = true,
                failed = false,
                failedMessage = ""
            )
        )
        TaskSubmissionRegistry.tryEmitEvent(
            SubmissionEvent(
                submissionId = submissionId,
                taskId = null,
                title = title,
                type = SubmissionEventType.STARTED,
                message = "任务已转入后台队列"
            )
        )
        val job = serviceScope.launch {
            runSubmission(
                submissionId = submissionId,
                mode = mode,
                preferredTitle = title,
                videoUrl = videoUrl,
                uploadUri = uploadUri
            )
        }
        submissionJobs[submissionId] = job
    }

    /**
     * Probe 路径专用：taskId 已由 ViewModel 提交到服务器，
     * 此处直接注册骨架卡片 + 启动 WS 监听终态 + 发完成通知。
     */
    private fun trackExistingTask(
        submissionId: String,
        taskId: String,
        preferredTitle: String
    ) {
        if (submissionJobs.containsKey(submissionId)) return
        val title = preferredTitle.ifBlank { "视频处理任务" }
        TaskSubmissionRegistry.upsert(
            ActiveSubmissionHint(
                workId = submissionId,
                taskId = taskId,
                title = title,
                phaseText = "服务端处理中...",
                progressPercent = null,
                running = true,
                failed = false,
                failedMessage = ""
            )
        )
        val job = serviceScope.launch {
            val completionNotifier = TaskCompletionNotifier(applicationContext)
            val notificationId = progressNotificationId(submissionId)
            notifyProgress(
                notificationId = notificationId,
                title = title,
                phaseText = "服务端处理中，请稍候...",
                progressPercent = null,
                taskId = taskId
            )
            try {
                val terminalState = awaitTaskTerminalState(
                    submissionId = submissionId,
                    taskId = taskId,
                    title = title,
                    notificationId = notificationId
                )
                when {
                    isCompletedStatus(terminalState.status) -> {
                        completionNotifier.notifyTaskCompleted(taskId = taskId, taskTitle = title)
                        TaskSubmissionRegistry.remove(submissionId)
                        TaskSubmissionRegistry.tryEmitEvent(
                            SubmissionEvent(
                                submissionId = submissionId,
                                taskId = taskId,
                                title = title,
                                type = SubmissionEventType.SUCCEEDED,
                                message = "任务已完成"
                            )
                        )
                    }
                    isFailedStatus(terminalState.status) -> {
                        TaskSubmissionRegistry.remove(submissionId)
                        TaskSubmissionRegistry.tryEmitEvent(
                            SubmissionEvent(
                                submissionId = submissionId,
                                taskId = taskId,
                                title = title,
                                type = SubmissionEventType.FAILED,
                                message = terminalState.statusMessage.ifBlank { "任务处理失败" }
                            )
                        )
                    }
                    isCancelledStatus(terminalState.status) -> {
                        TaskSubmissionRegistry.remove(submissionId)
                        TaskSubmissionRegistry.tryEmitEvent(
                            SubmissionEvent(
                                submissionId = submissionId,
                                taskId = taskId,
                                title = title,
                                type = SubmissionEventType.CANCELLED,
                                message = "任务已取消"
                            )
                        )
                    }
                    else -> {}
                }
            } catch (_: CancellationException) {
                TaskSubmissionRegistry.remove(submissionId)
            } catch (_: Throwable) {
                // WS 断连或超时：骨架卡片静默移除，不发 FAILED 事件
                // 下次 refreshTasks 会从服务器拉到最新状态
                TaskSubmissionRegistry.remove(submissionId)
            } finally {
                NotificationManagerCompat.from(applicationContext).cancel(notificationId)
                submissionJobs.remove(submissionId)
                if (submissionJobs.isEmpty()) {
                    stopForeground(STOP_FOREGROUND_REMOVE)
                    stopSelf()
                }
            }
        }
        submissionJobs[submissionId] = job
    }

    private suspend fun runSubmission(

        submissionId: String,
        mode: SubmissionMode,
        preferredTitle: String,
        videoUrl: String?,
        uploadUri: Uri?
    ) {
        val taskApi = HttpMobileTaskApi(MobileApiEndpointStore.resolveApiBaseUrl(applicationContext), mobileUserId)
        val completionNotifier = TaskCompletionNotifier(applicationContext)
        val progressNotificationId = progressNotificationId(submissionId)
        var taskId: String? = null
        try {
            val submitResult = when (mode) {
                SubmissionMode.URL -> taskApi.submitVideoUrl(videoUrl.orEmpty())
                SubmissionMode.UPLOAD -> {
                    val uri = requireNotNull(uploadUri) { "missing upload uri" }
                    taskApi.uploadVideoFile(contentResolver, uri)
                }
            }
            if (!submitResult.success || submitResult.taskId.isBlank()) {
                throw IllegalStateException(submitResult.message.ifBlank { "submit failed" })
            }
            taskId = submitResult.taskId.trim()
            submissionTaskIds[submissionId] = taskId

            TaskSubmissionRegistry.upsert(
                ActiveSubmissionHint(
                    workId = submissionId,
                    taskId = taskId,
                    title = preferredTitle,
                    phaseText = "任务已进入后台队列",
                    progressPercent = 0,
                    running = true,
                    failed = false,
                    failedMessage = ""
                )
            )
            notifyProgress(
                notificationId = progressNotificationId,
                title = preferredTitle,
                phaseText = "任务已提交，等待服务器主动推送状态...",
                progressPercent = null,
                taskId = taskId
            )

            val terminalState = awaitTaskTerminalState(
                submissionId = submissionId,
                taskId = taskId,
                title = preferredTitle,
                notificationId = progressNotificationId
            )
            when {
                isCompletedStatus(terminalState.status) -> {
                    completionNotifier.notifyTaskCompleted(taskId = taskId, taskTitle = preferredTitle)
                    TaskSubmissionRegistry.remove(submissionId)
                    TaskSubmissionRegistry.tryEmitEvent(
                        SubmissionEvent(
                            submissionId = submissionId,
                            taskId = taskId,
                            title = preferredTitle,
                            type = SubmissionEventType.SUCCEEDED,
                            message = "任务已完成"
                        )
                    )
                }

                isFailedStatus(terminalState.status) -> {
                    TaskSubmissionRegistry.remove(submissionId)
                    TaskSubmissionRegistry.tryEmitEvent(
                        SubmissionEvent(
                            submissionId = submissionId,
                            taskId = taskId,
                            title = preferredTitle,
                            type = SubmissionEventType.FAILED,
                            message = terminalState.statusMessage.ifBlank { "任务处理失败" }
                        )
                    )
                }

                isCancelledStatus(terminalState.status) -> {
                    TaskSubmissionRegistry.remove(submissionId)
                    TaskSubmissionRegistry.tryEmitEvent(
                        SubmissionEvent(
                            submissionId = submissionId,
                            taskId = taskId,
                            title = preferredTitle,
                            type = SubmissionEventType.CANCELLED,
                            message = "任务已取消"
                        )
                    )
                }

                else -> {
                    throw IllegalStateException("unexpected terminal status: ${terminalState.status}")
                }
            }
            NotificationManagerCompat.from(applicationContext).cancel(progressNotificationId)
        } catch (cancelled: CancellationException) {
            val activeTaskId = taskId ?: submissionTaskIds[submissionId]
            if (!activeTaskId.isNullOrBlank()) {
                runCatching { taskApi.cancelTask(activeTaskId) }
            }
            TaskSubmissionRegistry.remove(submissionId)
            TaskSubmissionRegistry.tryEmitEvent(
                SubmissionEvent(
                    submissionId = submissionId,
                    taskId = activeTaskId,
                    title = preferredTitle,
                    type = SubmissionEventType.CANCELLED,
                    message = "任务已取消"
                )
            )
            NotificationManagerCompat.from(applicationContext).cancel(progressNotificationId)
        } catch (error: Throwable) {
            TaskSubmissionRegistry.remove(submissionId)
            TaskSubmissionRegistry.tryEmitEvent(
                SubmissionEvent(
                    submissionId = submissionId,
                    taskId = taskId,
                    title = preferredTitle,
                    type = SubmissionEventType.FAILED,
                    message = error.message ?: "任务提交失败"
                )
            )
            NotificationManagerCompat.from(applicationContext).cancel(progressNotificationId)
        } finally {
            submissionJobs.remove(submissionId)
            submissionTaskIds.remove(submissionId)
            if (submissionJobs.isEmpty()) {
                stopForeground(STOP_FOREGROUND_REMOVE)
                stopSelf()
            }
        }
    }

    private suspend fun awaitTaskTerminalState(
        submissionId: String,
        taskId: String,
        title: String,
        notificationId: Int
    ): TaskRealtimeState {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            throw IllegalArgumentException("taskId cannot be empty")
        }
        val normalizedUserId = mobileUserId.trim()
        if (normalizedUserId.isEmpty()) {
            throw IllegalStateException("mobile user id is empty")
        }

        val wsEndpoint = CollectionApiFactory.toWebSocketUrl(
            MobileApiEndpointStore.resolveApiBaseUrl(applicationContext)
        )
        val deferred = CompletableDeferred<TaskRealtimeState>()
        val realtimeClient = ReliableTaskWebSocketClient(
            context = applicationContext,
            wsEndpoint = wsEndpoint,
            userId = normalizedUserId,
            clientLabel = "TaskSubmitRealtime",
            streamKeyProvider = {
                ReliableTaskWebSocketClient.buildStableStreamKey(
                    "android",
                    "foreground",
                    "task",
                    normalizedUserId,
                    normalizedTaskId
                )
            },
            buildReplayActions = {
                listOf(
                    JSONObject()
                        .put("action", "subscribe")
                        .put("taskId", normalizedTaskId)
                )
            },
            onJsonMessage = { payload ->
                if (payload.optString("type").trim() == "taskUpdate"
                    && payload.optString("taskId").trim() == normalizedTaskId
                ) {
                    val status = payload.optString("status").trim()
                    val statusMessage = payload.optString("message").trim()
                    val progress = payload.optDouble("progress", 0.0)

                    val phaseText = resolvePhaseText(
                        status = status,
                        statusMessage = statusMessage,
                        progress = progress
                    )
                    val progressPercent = normalizeProgressPercent(progress)
                    TaskSubmissionRegistry.upsert(
                        ActiveSubmissionHint(
                            workId = submissionId,
                            taskId = normalizedTaskId,
                            title = title,
                            phaseText = phaseText,
                            progressPercent = progressPercent,
                            running = !isTerminalStatus(status),
                            failed = false,
                            failedMessage = ""
                        )
                    )
                    notifyProgress(
                        notificationId = notificationId,
                        title = title,
                        phaseText = phaseText,
                        progressPercent = progressPercent,
                        taskId = normalizedTaskId
                    )
                    if (!deferred.isCompleted && isTerminalStatus(status)) {
                        deferred.complete(
                            TaskRealtimeState(
                                status = status,
                                statusMessage = statusMessage,
                                progress = progress
                            )
                        )
                    }
                }
            }
        )

        realtimeClient.connect()
        try {
            return withTimeout(TASK_REALTIME_AWAIT_TIMEOUT_MS) {
                deferred.await()
            }
        } catch (timeout: TimeoutCancellationException) {
            throw IllegalStateException("wait task terminal status timeout")
        } finally {
            realtimeClient.shutdown()
        }
    }
    private fun cancelSubmission(submissionId: String) {
        val taskId = submissionTaskIds[submissionId]
        if (!taskId.isNullOrBlank()) {
            serviceScope.launch {
                runCatching {
                    HttpMobileTaskApi(MobileApiEndpointStore.resolveApiBaseUrl(applicationContext), mobileUserId).cancelTask(taskId)
                }
            }
        }
        submissionJobs[submissionId]?.cancel(CancellationException("cancelled by user"))
    }

    private fun ensureServiceForeground() {
        val openAppIntent = Intent(applicationContext, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
        }
        val pendingIntent = PendingIntent.getActivity(
            applicationContext,
            SERVICE_NOTIFICATION_ID,
            openAppIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val notification = NotificationCompat.Builder(applicationContext, SERVICE_CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle("任务后台服务运行中")
            .setContentText("任务会在后台持续执行，可随时回到 App 查看。")
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setContentIntent(pendingIntent)
            .build()
        startForeground(SERVICE_NOTIFICATION_ID, notification)
    }

    private fun notifyProgress(
        notificationId: Int,
        title: String,
        phaseText: String,
        progressPercent: Int?,
        taskId: String
    ) {
        val openAppIntent = Intent(applicationContext, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra("task_id", taskId)
        }
        val pendingIntent = PendingIntent.getActivity(
            applicationContext,
            notificationId,
            openAppIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val builder = NotificationCompat.Builder(applicationContext, PROGRESS_CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle("任务后台处理中")
            .setContentText("《$title》$phaseText")
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setContentIntent(pendingIntent)
        if (progressPercent == null) {
            builder.setProgress(100, 0, true)
        } else {
            builder.setProgress(100, progressPercent.coerceIn(0, 100), false)
        }
        NotificationManagerCompat.from(applicationContext).notify(notificationId, builder.build())
    }

    private fun ensureNotificationChannels() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return
        }
        val manager = getSystemService(NotificationManager::class.java) ?: return
        if (manager.getNotificationChannel(SERVICE_CHANNEL_ID) == null) {
            manager.createNotificationChannel(
                NotificationChannel(
                    SERVICE_CHANNEL_ID,
                    "后台任务服务",
                    NotificationManager.IMPORTANCE_LOW
                ).apply {
                    description = "保持任务提交与处理状态在后台持续运行"
                }
            )
        }
        if (manager.getNotificationChannel(PROGRESS_CHANNEL_ID) == null) {
            manager.createNotificationChannel(
                NotificationChannel(
                    PROGRESS_CHANNEL_ID,
                    "任务进度",
                    NotificationManager.IMPORTANCE_LOW
                ).apply {
                    description = "显示任务处理阶段、进度和完成状态"
                }
            )
        }
    }

    private fun progressNotificationId(submissionId: String): Int {
        return submissionId.hashCode()
    }

    private fun normalizeProgressPercent(progress: Double): Int? {
        if (progress <= 0.0) {
            return null
        }
        val normalized = when {
            progress <= 1.0 -> progress * 100.0
            progress <= 100.0 -> progress
            else -> 100.0
        }
        return normalized.roundToInt().coerceIn(0, 100)
    }

    private fun resolvePhaseText(status: String, statusMessage: String, progress: Double): String {
        val normalizedStatus = status.trim().uppercase(Locale.ROOT)
        val normalizedMessage = statusMessage.trim()
        if (normalizedMessage.isNotBlank() && !isGenericStatusMessage(normalizedMessage)) {
            return normalizedMessage
        }
        if (normalizedStatus == "COMPLETED" || normalizedStatus == "SUCCESS") {
            return "处理完成"
        }
        if (normalizedStatus == "FAILED" || normalizedStatus == "ERROR") {
            return "处理失败"
        }
        if (normalizedStatus == "CANCELLED" || normalizedStatus == "CANCELED") {
            return "任务已取消"
        }
        if (normalizedStatus == "QUEUED" || normalizedStatus == "PENDING") {
            return "正在排队等待调度..."
        }
        return when {
            progress < 0.20 -> "正在提取音视频片段..."
            progress < 0.75 -> "AI 正在处理中..."
            else -> "正在生成 Markdown..."
        }
    }

    private fun isGenericStatusMessage(message: String): Boolean {
        val normalized = message.trim().lowercase(Locale.ROOT)
        return normalized == "processing" ||
            normalized == "running" ||
            normalized == "queued" ||
            normalized == "pending" ||
            message.trim() == "处理中" ||
            message.trim() == "排队中"
    }

    private fun isTerminalStatus(status: String): Boolean {
        return isCompletedStatus(status) || isFailedStatus(status) || isCancelledStatus(status)
    }

    private fun isCompletedStatus(status: String): Boolean {
        val upper = status.trim().uppercase(Locale.ROOT)
        return upper == "COMPLETED" || upper == "SUCCESS"
    }

    private fun isFailedStatus(status: String): Boolean {
        val upper = status.trim().uppercase(Locale.ROOT)
        return upper == "FAILED" || upper == "ERROR"
    }

    private fun isCancelledStatus(status: String): Boolean {
        val upper = status.trim().uppercase(Locale.ROOT)
        return upper == "CANCELLED" || upper == "CANCELED"
    }

    private data class TaskRealtimeState(
        val status: String,
        val statusMessage: String,
        val progress: Double
    )

    private enum class SubmissionMode {
        URL,
        UPLOAD
    }

    companion object {
        private const val ACTION_START_URL = "com.hongxu.videoToMarkdownTest2.START_URL_SUBMISSION"
        private const val ACTION_START_UPLOAD = "com.hongxu.videoToMarkdownTest2.START_UPLOAD_SUBMISSION"
        private const val ACTION_TRACK_TASK = "com.hongxu.videoToMarkdownTest2.TRACK_TASK"
        private const val ACTION_CANCEL = "com.hongxu.videoToMarkdownTest2.CANCEL_SUBMISSION"

        private const val EXTRA_SUBMISSION_ID = "submission_id"
        private const val EXTRA_PREFERRED_TITLE = "preferred_title"
        private const val EXTRA_VIDEO_URL = "video_url"
        private const val EXTRA_UPLOAD_URI = "upload_uri"
        private const val EXTRA_TASK_ID = "task_id"

        private const val SERVICE_CHANNEL_ID = "submission_service_channel"
        private const val PROGRESS_CHANNEL_ID = "submission_progress_channel"
        private const val SERVICE_NOTIFICATION_ID = 10401
        private const val TASK_REALTIME_AWAIT_TIMEOUT_MS = 2 * 60 * 60 * 1000L

        fun startUrlSubmission(context: Context, submissionId: String, title: String, videoUrl: String) {
            val intent = Intent(context, TaskSubmissionForegroundService::class.java).apply {
                action = ACTION_START_URL
                putExtra(EXTRA_SUBMISSION_ID, submissionId)
                putExtra(EXTRA_PREFERRED_TITLE, title)
                putExtra(EXTRA_VIDEO_URL, videoUrl)
            }
            startServiceCompat(context, intent)
        }

        /**
         * 注册一个已提交的 taskId 进行监控。
         * 适用于 Probe 路径（由 ViewModel 直接 HTTP 提交后回调）。
         */
        fun startTaskTracking(
            context: Context,
            submissionId: String,
            taskId: String,
            title: String
        ) {
            val intent = Intent(context, TaskSubmissionForegroundService::class.java).apply {
                action = ACTION_TRACK_TASK
                putExtra(EXTRA_SUBMISSION_ID, submissionId)
                putExtra(EXTRA_TASK_ID, taskId)
                putExtra(EXTRA_PREFERRED_TITLE, title)
            }
            startServiceCompat(context, intent)
        }

        fun startUploadSubmission(context: Context, submissionId: String, title: String, uploadUri: Uri) {
            val intent = Intent(context, TaskSubmissionForegroundService::class.java).apply {
                action = ACTION_START_UPLOAD
                putExtra(EXTRA_SUBMISSION_ID, submissionId)
                putExtra(EXTRA_PREFERRED_TITLE, title)
                putExtra(EXTRA_UPLOAD_URI, uploadUri.toString())
            }
            startServiceCompat(context, intent)
        }

        fun cancelSubmission(context: Context, submissionId: String) {
            val intent = Intent(context, TaskSubmissionForegroundService::class.java).apply {
                action = ACTION_CANCEL
                putExtra(EXTRA_SUBMISSION_ID, submissionId)
            }
            startServiceCompat(context, intent)
        }

        private fun startServiceCompat(context: Context, intent: Intent) {
            runCatching {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    context.startForegroundService(intent)
                } else {
                    context.startService(intent)
                }
            }.onFailure { error ->
                val failureMessage = resolveServiceStartFailureMessage(error)
                Log.e(
                    TAG,
                    "前台任务服务启动失败: action=${intent.action}, message=$failureMessage",
                    error
                )

                // 兜底发失败事件，避免 startForegroundService 异常直接打断提交流程并导致界面静默。
                val submissionId = intent.getStringExtra(EXTRA_SUBMISSION_ID)?.trim().orEmpty()
                if (submissionId.isNotEmpty()) {
                    val taskId = intent.getStringExtra(EXTRA_TASK_ID)
                        ?.trim()
                        ?.takeIf { it.isNotEmpty() }
                    val title = intent.getStringExtra(EXTRA_PREFERRED_TITLE)
                        .orEmpty()
                        .ifBlank { "任务提交" }
                    TaskSubmissionRegistry.tryEmitEvent(
                        SubmissionEvent(
                            submissionId = submissionId,
                            taskId = taskId,
                            title = title,
                            type = SubmissionEventType.FAILED,
                            message = failureMessage
                        )
                    )
                }
            }
        }

        private fun resolveServiceStartFailureMessage(error: Throwable): String {
            val detail = error.message?.trim().takeUnless { it.isNullOrBlank() }
            val summary = when {
                error is SecurityException -> "后台任务服务权限不足"
                Build.VERSION.SDK_INT >= Build.VERSION_CODES.S &&
                    error is ForegroundServiceStartNotAllowedException -> {
                    "系统当前不允许启动前台服务"
                }
                else -> "后台任务服务启动失败"
            }
            return if (detail == null) summary else "$summary：$detail"
        }

        private const val TAG = "TaskSubmissionService"
    }
}
