
import json
import logging
import os
import re
import time
import requests
import threading
from typing import Any, Dict, Optional

import lark_oapi as lark
from lark_oapi.api.im.v1 import *
from lark_oapi.event.client import Event as LarkEvent, EventDispatcherHandler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FeishuBot")

# Configuration
APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
ORCHESTRATOR_API_URL = os.getenv("ORCHESTRATOR_API_URL", "http://localhost:8080/api")

if not APP_ID or not APP_SECRET:
    logger.error("Missing FEISHU_APP_ID or FEISHU_APP_SECRET environment variables.")
    # We don't exit here to allow import for testing, but run will fail.

# Global client
client: Optional[lark.Client] = None

def _submit_task_to_orchestrator(url: str, user_id: str) -> Dict[str, Any]:
    """Submits a task to the local Java Orchestrator."""
    try:
        payload = {
            "userId": f"feishu_{user_id}",
            "videoUrl": url,
            "outputDir": "feishu_bot_downloads", # Simple default
            "priority": "normal"
        }
        resp = requests.post(f"{ORCHESTRATOR_API_URL}/tasks", json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Failed to submit task: {e}")
        return {"success": False, "message": str(e)}

def _get_task_status(task_id: str) -> Dict[str, Any]:
    """Gets task status from Orchestrator."""
    try:
        resp = requests.get(f"{ORCHESTRATOR_API_URL}/tasks/{task_id}", timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Failed to get task status for {task_id}: {e}")
        return {}

def _send_feishu_message(chat_id: str, text: str):
    """Sends a text message to Feishu chat."""
    if not client:
        return
    
    try:
        content = json.dumps({"text": text})
        request = CreateMessageRequest.builder() \
            .receive_id_type("chat_id") \
            .request_body(CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(content)
                .build()) \
            .build()
            
        resp = client.im.v1.message.create(request)
        if not resp.success():
            logger.error(f"Failed to send message: {resp.code} - {resp.msg}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")

def _send_feishu_file(chat_id: str, file_path: str, message: str = ""):
    """Uploads and sends a file."""
    if not client:
        return

    # 1. Upload File
    file_key = ""
    try:
        if not os.path.exists(file_path):
            _send_feishu_message(chat_id, f"Error: Result file not found at {file_path}")
            return

        request = CreateFileRequest.builder() \
            .request_body(CreateFileRequestBody.builder()
                .file_type("stream")
                .file_name(os.path.basename(file_path))
                .file(open(file_path, "rb"))
                .build()) \
            .build()

        resp = client.im.v1.file.create(request)
        if not resp.success():
             logger.error(f"Failed to upload file: {resp.code} - {resp.msg}")
             _send_feishu_message(chat_id, f"Failed to upload result file: {resp.msg}")
             return
        file_key = resp.data.file_key
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        _send_feishu_message(chat_id, f"Error uploading file: {e}")
        return

    # 2. Send File Message
    try:
        content = json.dumps({"file_key": file_key})
        request = CreateMessageRequest.builder() \
            .receive_id_type("chat_id") \
            .request_body(CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("file")
                .content(content)
                .build()) \
            .build()
            
        resp = client.im.v1.message.create(request)
        if not resp.success():
             logger.error(f"Failed to send file message: {resp.code} - {resp.msg}")
    except Exception as e:
        logger.error(f"Error sending file message: {e}")

    if message:
        _send_feishu_message(chat_id, message)


def do_p2_im_message_receive_v1(data: P2ImMessageReceiveV1) -> None:
    """Handles incoming messages."""
    event_id = data.header.event_id
    content = json.loads(data.event.message.content)
    text = content.get("text", "")
    chat_id = data.event.message.chat_id
    user_id = data.event.sender.sender_id.open_id # Use open_id for user identification

    logger.info(f"Received message from {user_id}: {text}")

    # extract URL
    url_match = re.search(r'https?://[^\s]+', text)
    if not url_match:
        # Ignore non-URL messages or maybe send help
        if "help" in text.lower() or "帮助" in text:
             _send_feishu_message(chat_id, "请发送视频链接（如 YouTube, Bilibili），我会自动开始处理。")
        return

    url = url_match.group(0)
    _send_feishu_message(chat_id, f"收到链接: {url}\n正在提交任务...")

    # Submit task
    result = _submit_task_to_orchestrator(url, user_id)
    
    if result.get("success"):
        task_id = result.get("taskId")
        _send_feishu_message(chat_id, f"✅ 任务已提交!\nID: {task_id}\n\n我会定期检查状态并向您汇报。")
        
        # Start monitoring thread for this task
        threading.Thread(target=_monitor_task, args=(task_id, chat_id), daemon=True).start()
    else:
        _send_feishu_message(chat_id, f"❌ 任务提交失败: {result.get('message')}")


def _monitor_task(task_id: str, chat_id: str):
    """Monitors a task until completion."""
    logger.info(f"Started monitoring task {task_id} for chat {chat_id}")
    
    last_status = "PENDING"
    start_time = time.time()
    
    while True:
        task_info = _get_task_status(task_id)
        if not task_info:
            time.sleep(30)
            continue
            
        status = task_info.get("status")
        progress = task_info.get("progress", 0)
        
        # Log status change or every 20% progress
        if status != last_status:
             _send_feishu_message(chat_id, f"🔄 任务状态更新: {status} (进度: {progress}%)")
             last_status = status
        
        if status == "COMPLETED":
            result_path = task_info.get("resultPath")
            duration = int(time.time() - start_time)
            _send_feishu_message(chat_id, f"🎉 任务完成! (耗时: {duration}s)\n正在上传结果...")
            
            # Locate the markdown file
            # resultPath usually points to storage directory. We need to find the .md file.
            # Assuming resultPath is absolute or relative to orchestrator working dir.
            # Since bot runs on same machine, we can access it.
            
            # Search for MD file in output dir if result_path is a directory
            final_file = result_path
            if os.path.isdir(result_path):
                 md_files = [f for f in os.listdir(result_path) if f.endswith(".md")]
                 if md_files:
                     final_file = os.path.join(result_path, md_files[0])
            
            if final_file and os.path.exists(final_file):
                 _send_feishu_file(chat_id, final_file)
            else:
                 _send_feishu_message(chat_id, f"⚠️ 找不到结果文件: {result_path}")
            
            break
            
        if status in ["FAILED", "CANCELLED"]:
            error = task_info.get("errorMessage", "Unknown error")
            _send_feishu_message(chat_id, f"❌ 任务失败/取消: {error}")
            break
            
        time.sleep(10) # Poll every 10 seconds


def main():
    global client
    
    if not APP_ID or not APP_SECRET:
        print("Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables.")
        return

    # Initialize Client
    client = lark.Client.builder() \
        .app_id(APP_ID) \
        .app_secret(APP_SECRET) \
        .log_level(lark.LogLevel.INFO) \
        .build()

    # Register Event Handler
    event_handler = EventDispatcherHandler.builder("", "") \
        .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
        .build()

    # Start WebSocket Client
    ws_client = lark.ws.Client(APP_ID, APP_SECRET, event_handler=event_handler, log_level=lark.LogLevel.INFO)
    
    print("🚀 Feishu Bot started (WebSocket Mode)... Listening for messages.")
    ws_client.start()

if __name__ == "__main__":
    main()
