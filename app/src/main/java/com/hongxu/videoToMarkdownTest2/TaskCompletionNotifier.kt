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
        ensureChannel()

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

        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
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

    private fun canPostNotification(): Boolean {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            return true
        }
        return ContextCompat.checkSelfPermission(
            context,
            Manifest.permission.POST_NOTIFICATIONS
        ) == PackageManager.PERMISSION_GRANTED
    }

    private fun ensureChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return
        }
        val manager = context.getSystemService(NotificationManager::class.java) ?: return
        val existing = manager.getNotificationChannel(CHANNEL_ID)
        if (existing != null) {
            return
        }
        val channel = NotificationChannel(
            CHANNEL_ID,
            CHANNEL_NAME,
            NotificationManager.IMPORTANCE_DEFAULT
        ).apply {
            description = CHANNEL_DESCRIPTION
        }
        manager.createNotificationChannel(channel)
    }

    companion object {
        private const val CHANNEL_ID = "task_completion_channel"
        private const val CHANNEL_NAME = "Task completion"
        private const val CHANNEL_DESCRIPTION = "Notify when a video task is completed"
        private const val EXTRA_TASK_ID = "task_id"
    }
}
