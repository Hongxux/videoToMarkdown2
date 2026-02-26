package com.hongxu.videoToMarkdownTest2

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat

class TaskCompletionNotifier(
    private val context: Context
) {
    fun notifyTaskCompleted(taskId: String, taskTitle: String) {
        if (!canPostNotification()) {
            return
        }
        ensureChannel(
            channelId = TASK_CHANNEL_ID,
            channelName = TASK_CHANNEL_NAME,
            channelDescription = TASK_CHANNEL_DESCRIPTION
        )

        val sanitizedTaskId = taskId.trim()
        val title = taskTitle.trim().ifBlank { sanitizedTaskId }
        val openAppIntent = Intent(context, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra(EXTRA_TASK_ID, sanitizedTaskId)
        }
        val pendingIntent = PendingIntent.getActivity(
            context,
            sanitizedTaskId.hashCode(),
            openAppIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val notification = NotificationCompat.Builder(context, TASK_CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle("Task completed")
            .setContentText("\"$title\" is ready")
            .setStyle(
                NotificationCompat.BigTextStyle()
                    .bigText("\"$title\" is ready. Open the app to view the result.")
            )
            .setAutoCancel(true)
            .setContentIntent(pendingIntent)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
            .build()

        NotificationManagerCompat.from(context).notify(sanitizedTaskId.hashCode(), notification)
    }

    fun notifyVideoProbeCompleted(
        input: String,
        resolvedTitle: String?,
        success: Boolean,
        detail: String? = null
    ) {
        if (!canPostNotification()) {
            return
        }
        ensureChannel(
            channelId = PROBE_CHANNEL_ID,
            channelName = PROBE_CHANNEL_NAME,
            channelDescription = PROBE_CHANNEL_DESCRIPTION
        )

        val normalizedInput = input.trim()
        val title = resolvedTitle?.trim().takeUnless { it.isNullOrBlank() }
            ?: normalizedInput.ifBlank { "video link" }
        val openAppIntent = Intent(context, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
        }
        val requestCode = ("probe:$title").hashCode()
        val pendingIntent = PendingIntent.getActivity(
            context,
            requestCode,
            openAppIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val contentTitle = if (success) "Video probe completed" else "Video probe failed"
        val contentText = if (success) {
            "\"$title\" metadata is ready"
        } else {
            "\"$title\" probe failed"
        }
        val detailText = if (success) {
            "\"$title\" metadata is ready. Open the app to continue."
        } else {
            val normalizedDetail = detail?.trim().takeUnless { it.isNullOrBlank() } ?: "unknown"
            "\"$title\" probe failed: $normalizedDetail"
        }

        val notification = NotificationCompat.Builder(context, PROBE_CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle(contentTitle)
            .setContentText(contentText)
            .setStyle(NotificationCompat.BigTextStyle().bigText(detailText))
            .setAutoCancel(true)
            .setContentIntent(pendingIntent)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
            .build()

        NotificationManagerCompat.from(context).notify(requestCode, notification)
    }

    private fun canPostNotification(): Boolean {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            return true
        }
        return ContextCompat.checkSelfPermission(
            context,
            Manifest.permission.POST_NOTIFICATIONS
        ) == PackageManager.PERMISSION_GRANTED
    }

    private fun ensureChannel(
        channelId: String,
        channelName: String,
        channelDescription: String
    ) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return
        }
        val manager = context.getSystemService(NotificationManager::class.java) ?: return
        val existing = manager.getNotificationChannel(channelId)
        if (existing != null) {
            return
        }
        val channel = NotificationChannel(
            channelId,
            channelName,
            NotificationManager.IMPORTANCE_DEFAULT
        ).apply {
            description = channelDescription
        }
        manager.createNotificationChannel(channel)
    }

    companion object {
        private const val TASK_CHANNEL_ID = "task_completion_channel"
        private const val TASK_CHANNEL_NAME = "Task completion"
        private const val TASK_CHANNEL_DESCRIPTION = "Notify when a video task is completed"
        private const val PROBE_CHANNEL_ID = "video_probe_channel"
        private const val PROBE_CHANNEL_NAME = "Video probe"
        private const val PROBE_CHANNEL_DESCRIPTION = "Notify when video metadata probe completes"
        private const val EXTRA_TASK_ID = "task_id"
    }
}
