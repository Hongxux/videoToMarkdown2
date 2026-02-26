package com.hongxu.videoToMarkdownTest2

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.roundToInt

/**
 * 前台服务负责提交任务并持续同步后端进度。
 * 该服务在 App 退到后台后依旧存活，直到所有提交任务结束。
 */
class TaskSubmissionForegroundService : Service() {

    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val submissionJobs = ConcurrentHashMap<String, Job>()
    private val submissionTaskIds = ConcurrentHashMap<String, String>()

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

    private suspend fun runSubmission(
        submissionId: String,
        mode: SubmissionMode,
        preferredTitle: String,
        videoUrl: String?,
        uploadUri: Uri?
    ) {
        val taskApi = HttpMobileTaskApi(BuildConfig.MOBILE_API_BASE_URL)
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

            while (kotlinx.coroutines.currentCoroutineContext().isActive) {
                val snapshot = taskApi.getTaskRuntimeSnapshot(taskId)
                val phaseText = resolvePhaseText(
                    status = snapshot.status,
                    statusMessage = snapshot.statusMessage,
                    progress = snapshot.progress
                )
                val progressPercent = normalizeProgressPercent(snapshot.progress)

                TaskSubmissionRegistry.upsert(
                    ActiveSubmissionHint(
                        workId = submissionId,
                        taskId = taskId,
                        title = preferredTitle,
                        phaseText = phaseText,
                        progressPercent = progressPercent,
                        running = true,
                        failed = false,
                        failedMessage = ""
                    )
                )
                notifyProgress(
                    notificationId = progressNotificationId,
                    title = preferredTitle,
                    phaseText = phaseText,
                    progressPercent = progressPercent,
                    taskId = taskId
                )

                when {
                    isCompletedStatus(snapshot.status) -> {
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
                        NotificationManagerCompat.from(applicationContext).cancel(progressNotificationId)
                        return
                    }

                    isFailedStatus(snapshot.status) -> {
                        TaskSubmissionRegistry.remove(submissionId)
                        TaskSubmissionRegistry.tryEmitEvent(
                            SubmissionEvent(
                                submissionId = submissionId,
                                taskId = taskId,
                                title = preferredTitle,
                                type = SubmissionEventType.FAILED,
                                message = snapshot.statusMessage.ifBlank { "任务处理失败" }
                            )
                        )
                        NotificationManagerCompat.from(applicationContext).cancel(progressNotificationId)
                        return
                    }

                    isCancelledStatus(snapshot.status) -> {
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
                        NotificationManagerCompat.from(applicationContext).cancel(progressNotificationId)
                        return
                    }
                }

                delay(POLL_INTERVAL_MS)
            }
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

    private fun cancelSubmission(submissionId: String) {
        val taskId = submissionTaskIds[submissionId]
        if (!taskId.isNullOrBlank()) {
            serviceScope.launch {
                runCatching {
                    HttpMobileTaskApi(BuildConfig.MOBILE_API_BASE_URL).cancelTask(taskId)
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
            .setContentText("任务将持续在后台执行，可随时回到 App 查看。")
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
                    description = "保持任务提交与进度同步在后台持续运行"
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
                    description = "展示任务的阶段进度和完成状态"
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
            progress < 0.20 -> "正在提取音频片段..."
            progress < 0.75 -> "AI正在深度思考（预计1分钟）..."
            else -> "正在进行Markdown排版..."
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

    private enum class SubmissionMode {
        URL,
        UPLOAD
    }

    companion object {
        private const val ACTION_START_URL = "com.hongxu.videoToMarkdownTest2.START_URL_SUBMISSION"
        private const val ACTION_START_UPLOAD = "com.hongxu.videoToMarkdownTest2.START_UPLOAD_SUBMISSION"
        private const val ACTION_CANCEL = "com.hongxu.videoToMarkdownTest2.CANCEL_SUBMISSION"

        private const val EXTRA_SUBMISSION_ID = "submission_id"
        private const val EXTRA_PREFERRED_TITLE = "preferred_title"
        private const val EXTRA_VIDEO_URL = "video_url"
        private const val EXTRA_UPLOAD_URI = "upload_uri"

        private const val SERVICE_CHANNEL_ID = "submission_service_channel"
        private const val PROGRESS_CHANNEL_ID = "submission_progress_channel"
        private const val SERVICE_NOTIFICATION_ID = 10401
        private const val POLL_INTERVAL_MS = 4_000L

        fun startUrlSubmission(context: Context, submissionId: String, title: String, videoUrl: String) {
            val intent = Intent(context, TaskSubmissionForegroundService::class.java).apply {
                action = ACTION_START_URL
                putExtra(EXTRA_SUBMISSION_ID, submissionId)
                putExtra(EXTRA_PREFERRED_TITLE, title)
                putExtra(EXTRA_VIDEO_URL, videoUrl)
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
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
        }
    }
}
