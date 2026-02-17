# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import hashlib
import logging
import os
import queue
import re
import struct
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import unquote, urlsplit

import requests

_AES_BACKEND: Optional[str] = None
_AES_IMPORT_ERROR: Optional[str] = None

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    _AES_BACKEND = "cryptography"

    def _aes_cbc_decrypt(cipher_bytes: bytes, key: bytes, iv: bytes) -> bytes:
        """使用 cryptography 执行 AES-CBC 解密。"""
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        decryptor = cipher.decryptor()
        return decryptor.update(cipher_bytes) + decryptor.finalize()

except Exception as _cryptography_import_exc:
    try:
        from Crypto.Cipher import AES as _PyCryptoAes

        _AES_BACKEND = "pycryptodome"

        def _aes_cbc_decrypt(cipher_bytes: bytes, key: bytes, iv: bytes) -> bytes:
            """使用 pycryptodome 执行 AES-CBC 解密。"""
            cipher = _PyCryptoAes.new(key, _PyCryptoAes.MODE_CBC, iv)
            return cipher.decrypt(cipher_bytes)

    except Exception as _pycryptodome_import_exc:
        _AES_BACKEND = None
        _AES_IMPORT_ERROR = (
            f"cryptography 导入失败: {_cryptography_import_exc}; "
            f"pycryptodome 导入失败: {_pycryptodome_import_exc}"
        )

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


logger = logging.getLogger("WeComBot")

URL_PATTERN = re.compile(r"https?://[^\s]+", re.IGNORECASE)


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _parse_query_without_plus_decode(query: str) -> dict[str, str]:
    result: dict[str, str] = {}
    if not query:
        return result
    for pair in query.split("&"):
        if not pair:
            continue
        if "=" in pair:
            key, value = pair.split("=", 1)
        else:
            key, value = pair, ""
        result[unquote(key)] = unquote(value)
    return result


def _mask_text_for_log(value: str, keep: int = 8) -> str:
    if not value:
        return ""
    if len(value) <= keep:
        return value
    return f"{value[:keep]}...({len(value)})"


class WeComCrypto:
    """企业微信回调加解密与签名校验工具。"""

    def __init__(self, token: str, encoding_aes_key: str, receive_id: Optional[str]) -> None:
        if not _AES_BACKEND:
            raise RuntimeError(
                "缺少可用 AES 解密依赖，请安装 `cryptography` 或 `pycryptodome`。"
                f" 导入详情: {_AES_IMPORT_ERROR}"
            )
        self._token = token
        self._receive_id = receive_id or ""
        key_text = f"{encoding_aes_key}="
        self._aes_key = base64.b64decode(key_text)
        if len(self._aes_key) != 32:
            raise ValueError("WECOM_ENCODING_AES_KEY 解码后长度必须为 32 字节")

    def verify_signature(self, signature: str, timestamp: str, nonce: str, encrypted: str) -> bool:
        parts = sorted([self._token, timestamp, nonce, encrypted])
        joined = "".join(parts).encode("utf-8")
        digest = hashlib.sha1(joined).hexdigest()
        return digest == signature

    def decrypt(self, encrypted: str) -> str:
        cipher_bytes = base64.b64decode(encrypted)
        iv = self._aes_key[:16]
        padded_plain = _aes_cbc_decrypt(cipher_bytes=cipher_bytes, key=self._aes_key, iv=iv)
        plain = self._pkcs7_unpad(padded_plain)
        if len(plain) < 20:
            raise ValueError("企业微信回调解密后长度异常")
        xml_len = struct.unpack(">I", plain[16:20])[0]
        xml_start = 20
        xml_end = xml_start + xml_len
        xml_bytes = plain[xml_start:xml_end]
        receive_id = plain[xml_end:].decode("utf-8", errors="ignore")
        if self._receive_id and receive_id and receive_id != self._receive_id:
            raise ValueError("企业微信回调 receive_id 不匹配")
        return xml_bytes.decode("utf-8")

    @staticmethod
    def _pkcs7_unpad(data: bytes) -> bytes:
        if not data:
            raise ValueError("PKCS7 数据为空")
        pad_len = data[-1]
        if pad_len <= 0 or pad_len > 32:
            raise ValueError("PKCS7 填充长度非法")
        if data[-pad_len:] != bytes([pad_len]) * pad_len:
            raise ValueError("PKCS7 填充内容非法")
        return data[:-pad_len]


class WeComMessageClient:
    """企业微信消息发送客户端，支持 access_token 缓存。"""

    def __init__(self, corp_id: str, corp_secret: str, agent_id: Optional[int], timeout_sec: int = 8) -> None:
        self._corp_id = corp_id
        self._corp_secret = corp_secret
        self._agent_id = agent_id
        self._timeout_sec = timeout_sec
        self._token: Optional[str] = None
        self._token_expire_at: float = 0.0
        self._lock = threading.Lock()

    @property
    def is_ready(self) -> bool:
        return bool(self._corp_id and self._corp_secret and self._agent_id is not None)

    def send_text(self, user_id: str, content: str) -> bool:
        if not self.is_ready:
            logger.warning("企业微信发送配置缺失，跳过消息发送。")
            return False
        if not user_id:
            return False
        token = self._get_access_token()
        if not token:
            return False
        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"
        payload = {
            "touser": user_id,
            "msgtype": "text",
            "agentid": self._agent_id,
            "text": {"content": content},
            "safe": 0,
        }
        try:
            resp = requests.post(url, json=payload, timeout=self._timeout_sec)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error("发送企业微信消息失败: %s", exc)
            return False
        if int(data.get("errcode", -1)) != 0:
            logger.error("企业微信消息接口返回错误: errcode=%s, errmsg=%s", data.get("errcode"), data.get("errmsg"))
            return False
        return True

    def _get_access_token(self) -> Optional[str]:
        with self._lock:
            now = time.time()
            if self._token and now < self._token_expire_at - 60:
                return self._token
            token_url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
            params = {"corpid": self._corp_id, "corpsecret": self._corp_secret}
            try:
                resp = requests.get(token_url, params=params, timeout=self._timeout_sec)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.error("获取企业微信 access_token 失败: %s", exc)
                return None
            if int(data.get("errcode", -1)) != 0:
                logger.error("获取 access_token 返回错误: errcode=%s, errmsg=%s", data.get("errcode"), data.get("errmsg"))
                return None
            token = data.get("access_token")
            expires_in = int(data.get("expires_in", 7200))
            if not token:
                logger.error("获取 access_token 结果缺少 access_token 字段")
                return None
            self._token = token
            self._token_expire_at = now + max(expires_in, 120)
            return token


class OrchestratorClient:
    """复用 Java Orchestrator REST API 提交与查询任务。"""

    def __init__(self, api_base_url: str, timeout_sec: int = 8, output_dir_prefix: str = "wecom_bot_tasks") -> None:
        self._api_base_url = api_base_url.rstrip("/")
        self._timeout_sec = timeout_sec
        self._output_dir_prefix = output_dir_prefix

    def submit_task(self, url: str, user_id: str, job_id: str) -> tuple[bool, str, str]:
        submit_url = f"{self._api_base_url}/tasks"
        payload = {
            "userId": f"wecom_{user_id}",
            "videoUrl": url,
            "outputDir": f"{self._output_dir_prefix}/{job_id}",
            "priority": "normal",
        }
        try:
            resp = requests.post(submit_url, json=payload, timeout=self._timeout_sec)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            return False, "", f"提交任务请求失败: {exc}"
        if not data.get("success"):
            return False, "", str(data.get("message", "任务提交失败"))
        task_id = str(data.get("taskId", "")).strip()
        if not task_id:
            return False, "", "任务提交返回缺少 taskId"
        return True, task_id, ""

    def query_task(self, task_id: str) -> tuple[bool, dict, str]:
        query_url = f"{self._api_base_url}/tasks/{task_id}"
        try:
            resp = requests.get(query_url, timeout=self._timeout_sec)
            if resp.status_code == 404:
                return False, {}, "任务不存在"
            resp.raise_for_status()
            return True, resp.json(), ""
        except Exception as exc:
            return False, {}, f"查询任务状态失败: {exc}"


@dataclass
class JobRecord:
    job_id: str
    user_id: str
    source_url: str
    state: str
    created_at: float
    updated_at: float
    attempts: int = 0
    orchestrator_task_id: str = ""
    result_path: str = ""
    error_message: str = ""


class SerialJobExecutor:
    """串行任务执行器，内置自动重试。"""

    TERMINAL_ORCHESTRATOR_STATES = {"COMPLETED", "FAILED", "CANCELLED"}

    def __init__(
        self,
        orchestrator: OrchestratorClient,
        messenger: WeComMessageClient,
        max_retries: int,
        poll_interval_sec: int,
        retry_backoff_base_sec: int,
    ) -> None:
        self._orchestrator = orchestrator
        self._messenger = messenger
        self._max_retries = max(0, max_retries)
        self._max_attempts = self._max_retries + 1
        self._poll_interval_sec = max(3, poll_interval_sec)
        self._retry_backoff_base_sec = max(3, retry_backoff_base_sec)
        self._queue: queue.Queue[str] = queue.Queue()
        self._jobs: dict[str, JobRecord] = {}
        self._jobs_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="wecom-serial-job-executor")

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout_sec: float = 3.0) -> None:
        self._stop_event.set()
        self._thread.join(timeout=timeout_sec)

    def enqueue(self, user_id: str, source_url: str) -> JobRecord:
        now = time.time()
        job = JobRecord(
            job_id=uuid.uuid4().hex[:12],
            user_id=user_id,
            source_url=source_url,
            state="QUEUED",
            created_at=now,
            updated_at=now,
        )
        with self._jobs_lock:
            self._jobs[job.job_id] = job
        self._queue.put(job.job_id)
        return job

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return JobRecord(**job.__dict__)

    def _run_loop(self) -> None:
        logger.info("串行任务执行器已启动，max_retries=%s", self._max_retries)
        while not self._stop_event.is_set():
            try:
                job_id = self._queue.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self._execute_job(job_id)
            except Exception:
                logger.exception("执行作业异常: job_id=%s", job_id)
            finally:
                self._queue.task_done()
        logger.info("串行任务执行器已停止")

    def _execute_job(self, job_id: str) -> None:
        for attempt in range(1, self._max_attempts + 1):
            if self._stop_event.is_set():
                return
            self._update_job(job_id, state="RUNNING", attempts=attempt, error_message="")
            job = self.get_job(job_id)
            if job is None:
                return
            logger.info(
                "URL尝试提交: job_id=%s attempt=%s/%s user=%s url=%s",
                job.job_id,
                attempt,
                self._max_attempts,
                job.user_id,
                job.source_url,
            )
            self._messenger.send_text(
                job.user_id,
                f"[任务 {job.job_id}] 开始执行，第 {attempt}/{self._max_attempts} 次尝试。",
            )
            ok, task_id, submit_error = self._orchestrator.submit_task(job.source_url, job.user_id, job.job_id)
            if not ok:
                logger.warning(
                    "URL提交失败: job_id=%s attempt=%s/%s url=%s error=%s",
                    job.job_id,
                    attempt,
                    self._max_attempts,
                    job.source_url,
                    submit_error,
                )
                if self._retry_or_finalize(job_id, attempt, submit_error):
                    continue
                return
            self._update_job(job_id, orchestrator_task_id=task_id)
            job = self.get_job(job_id)
            if job is None:
                return
            self._messenger.send_text(
                job.user_id,
                f"[任务 {job.job_id}] 已提交到编排器，task_id={task_id}。",
            )
            logger.info(
                "URL提交成功: job_id=%s attempt=%s/%s task_id=%s url=%s",
                job.job_id,
                attempt,
                self._max_attempts,
                task_id,
                job.source_url,
            )
            success, message = self._monitor_orchestrator_task(job_id=job.job_id, user_id=job.user_id, task_id=task_id)
            if success:
                return
            if self._retry_or_finalize(job_id, attempt, message):
                continue
            return

    def _monitor_orchestrator_task(self, job_id: str, user_id: str, task_id: str) -> tuple[bool, str]:
        last_state = ""
        while not self._stop_event.is_set():
            ok, payload, error = self._orchestrator.query_task(task_id)
            if not ok:
                return False, error
            status = str(payload.get("status", "")).strip().upper()
            progress = payload.get("progress", 0)
            status_message = str(payload.get("statusMessage", "")).strip()
            if status and status != last_state:
                if status in {"QUEUED", "PROCESSING"}:
                    self._messenger.send_text(
                        user_id,
                        f"[任务 {job_id}] 状态={status}，进度={round(float(progress) * 100)}%，说明={status_message or '无'}",
                    )
                last_state = status
            if status == "COMPLETED":
                result_path = str(payload.get("resultPath", "")).strip()
                self._update_job(job_id, state="SUCCEEDED", result_path=result_path, error_message="")
                self._messenger.send_text(
                    user_id,
                    f"[任务 {job_id}] 执行成功。结果路径：{result_path or '未返回路径'}",
                )
                return True, ""
            if status in {"FAILED", "CANCELLED"}:
                error_message = str(payload.get("errorMessage", "")).strip() or f"编排器返回状态 {status}"
                return False, error_message
            if status in self.TERMINAL_ORCHESTRATOR_STATES:
                return False, f"未知终态: {status}"
            time.sleep(self._poll_interval_sec)
        return False, "服务停止"

    def _retry_or_finalize(self, job_id: str, attempt: int, reason: str) -> bool:
        job = self.get_job(job_id)
        if job is None:
            return False
        if attempt < self._max_attempts:
            backoff = self._retry_backoff_base_sec * (2 ** (attempt - 1))
            self._update_job(job_id, state="RETRYING", error_message=reason)
            self._messenger.send_text(
                job.user_id,
                f"[任务 {job.job_id}] 第 {attempt} 次失败：{reason}\n将在 {backoff}s 后自动重试。",
            )
            for _ in range(backoff):
                if self._stop_event.is_set():
                    return False
                time.sleep(1)
            return True
        self._update_job(job_id, state="FAILED_FINAL", error_message=reason)
        self._messenger.send_text(
            job.user_id,
            f"[任务 {job.job_id}] 最终失败（已重试 {self._max_retries} 次）。\n原因：{reason}",
        )
        return False

    def _update_job(self, job_id: str, **kwargs: object) -> None:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            job.updated_at = time.time()


class WeComBotService:
    """企业微信回调服务，解析命令并投递串行任务。"""

    def __init__(
        self,
        callback_token: str,
        encoding_aes_key: str,
        callback_path: str,
        receive_id: Optional[str],
        job_executor: SerialJobExecutor,
        messenger: WeComMessageClient,
        dedupe_ttl_sec: int = 600,
    ) -> None:
        self.callback_path = callback_path
        self._crypto = WeComCrypto(callback_token, encoding_aes_key, receive_id=receive_id)
        self._executor = job_executor
        self._messenger = messenger
        self._dedupe_ttl_sec = max(60, dedupe_ttl_sec)
        self._seen_msg_ids: dict[str, float] = {}
        self._seen_lock = threading.Lock()

    def start(self) -> None:
        self._executor.start()

    def stop(self) -> None:
        self._executor.stop()

    def verify_url(self, msg_signature: str, timestamp: str, nonce: str, echostr: str) -> tuple[bool, str]:
        if not self._crypto.verify_signature(msg_signature, timestamp, nonce, echostr):
            return False, "签名验证失败"
        try:
            plain = self._crypto.decrypt(echostr)
        except Exception as exc:
            return False, f"echostr 解密失败: {exc}"
        return True, plain

    def handle_callback_post(self, msg_signature: str, timestamp: str, nonce: str, body: bytes) -> tuple[bool, str]:
        encrypt_value = self._extract_encrypt_from_outer_xml(body)
        if not encrypt_value:
            return False, "回调 XML 缺少 Encrypt 字段"
        if not self._crypto.verify_signature(msg_signature, timestamp, nonce, encrypt_value):
            return False, "签名验证失败"
        try:
            inner_xml = self._crypto.decrypt(encrypt_value)
        except Exception as exc:
            return False, f"消息解密失败: {exc}"
        try:
            self._consume_inner_message_xml(inner_xml)
        except Exception as exc:
            logger.exception("消费消息失败")
            return False, f"消息处理失败: {exc}"
        return True, "success"

    @staticmethod
    def _extract_encrypt_from_outer_xml(body: bytes) -> str:
        try:
            root = ET.fromstring(body.decode("utf-8"))
        except Exception:
            return ""
        encrypted = root.findtext("Encrypt")
        return (encrypted or "").strip()

    def _consume_inner_message_xml(self, inner_xml: str) -> None:
        root = ET.fromstring(inner_xml)
        msg_id = (root.findtext("MsgId") or root.findtext("CreateTime") or "").strip()
        if msg_id and self._is_duplicate_msg(msg_id):
            logger.info("忽略重复回调消息: msg_id=%s", msg_id)
            return
        msg_type = (root.findtext("MsgType") or "").strip().lower()
        user_id = (root.findtext("FromUserName") or "").strip()
        if msg_type != "text" or not user_id:
            logger.info("忽略非文本或缺少用户消息: msg_type=%s user_id=%s", msg_type, user_id)
            return
        content = (root.findtext("Content") or "").strip()
        self._handle_text_command(user_id=user_id, content=content)

    def _is_duplicate_msg(self, msg_id: str) -> bool:
        now = time.time()
        with self._seen_lock:
            expired_keys = [key for key, ts in self._seen_msg_ids.items() if now - ts > self._dedupe_ttl_sec]
            for key in expired_keys:
                self._seen_msg_ids.pop(key, None)
            if msg_id in self._seen_msg_ids:
                return True
            self._seen_msg_ids[msg_id] = now
        return False

    def _handle_text_command(self, user_id: str, content: str) -> None:
        normalized = content.strip()
        low = normalized.lower()
        if low.startswith("/status"):
            pieces = normalized.split(maxsplit=1)
            if len(pieces) < 2:
                self._messenger.send_text(user_id, "用法：/status <job_id>")
                return
            job_id = pieces[1].strip()
            job = self._executor.get_job(job_id)
            if not job:
                self._messenger.send_text(user_id, f"未找到任务 {job_id}")
                return
            self._messenger.send_text(
                user_id,
                (
                    f"[任务 {job.job_id}] 状态={job.state}，尝试={job.attempts}，"
                    f"task_id={job.orchestrator_task_id or '无'}，"
                    f"错误={job.error_message or '无'}，"
                    f"结果={job.result_path or '无'}"
                ),
            )
            return

        url_match = URL_PATTERN.search(normalized)
        if not url_match:
            self._messenger.send_text(
                user_id,
                "未识别到 URL。\n发送 `/run <url>` 或直接发送 URL。\n可用 `/status <job_id>` 查询状态。",
            )
            return
        url = url_match.group(0)
        job = self._executor.enqueue(user_id=user_id, source_url=url)
        logger.info("URL入队: job_id=%s user=%s url=%s", job.job_id, user_id, url)
        self._messenger.send_text(
            user_id,
            f"[任务 {job.job_id}] 已接收并入队（严格串行）。\nURL: {url}",
        )


class WeComCallbackHandler(BaseHTTPRequestHandler):
    """HTTP 回调处理器。"""

    service: WeComBotService

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        params = _parse_query_without_plus_decode(parsed.query)
        msg_signature = params.get("msg_signature", "")
        timestamp = params.get("timestamp", "")
        nonce = params.get("nonce", "")
        echostr = params.get("echostr", "")
        logger.info(
            "wechat_callback GET访问: client=%s path=%s sig=%s ts=%s nonce=%s echostr_len=%s",
            self.client_address[0] if self.client_address else "",
            parsed.path,
            _mask_text_for_log(msg_signature),
            timestamp,
            _mask_text_for_log(nonce),
            len(echostr),
        )
        if parsed.path != self.service.callback_path:
            self._write_text(404, "not found")
            return
        if not (msg_signature and timestamp and nonce and echostr):
            logger.warning("wechat_callback GET缺少必要参数: client=%s path=%s", self.client_address[0], parsed.path)
            self._write_text(400, "missing query fields")
            return
        ok, payload = self.service.verify_url(msg_signature, timestamp, nonce, echostr)
        if not ok:
            logger.warning("wechat_callback GET验签/解密失败: client=%s reason=%s", self.client_address[0], payload)
            self._write_text(403, payload)
            return
        logger.info("wechat_callback GET验证通过: client=%s", self.client_address[0])
        self._write_text(200, payload)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        params = _parse_query_without_plus_decode(parsed.query)
        msg_signature = params.get("msg_signature", "")
        timestamp = params.get("timestamp", "")
        nonce = params.get("nonce", "")
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        logger.info(
            "wechat_callback POST访问: client=%s path=%s sig=%s ts=%s nonce=%s content_length=%s",
            self.client_address[0] if self.client_address else "",
            parsed.path,
            _mask_text_for_log(msg_signature),
            timestamp,
            _mask_text_for_log(nonce),
            content_length,
        )
        if parsed.path != self.service.callback_path:
            self._write_text(404, "not found")
            return
        if not (msg_signature and timestamp and nonce):
            logger.warning("wechat_callback POST缺少签名参数: client=%s path=%s", self.client_address[0], parsed.path)
            self._write_text(400, "missing signature query fields")
            return
        body = self.rfile.read(content_length)
        ok, message = self.service.handle_callback_post(msg_signature, timestamp, nonce, body)
        if ok:
            logger.info("wechat_callback POST处理成功: client=%s", self.client_address[0])
            self._write_text(200, "success")
            return
        logger.warning("wechat_callback POST处理失败: client=%s reason=%s", self.client_address[0], message)
        self._write_text(403, message)

    def log_message(self, format: str, *args: object) -> None:
        logger.info("HTTP %s - %s", self.address_string(), format % args)

    def _write_text(self, status: int, payload: str) -> None:
        data = payload.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def run_server(service: WeComBotService, host: str, port: int) -> None:
    class _Handler(WeComCallbackHandler):
        pass

    _Handler.service = service
    server = ThreadingHTTPServer((host, port), _Handler)
    logger.info("企业微信回调服务启动: http://%s:%s%s", host, port, service.callback_path)
    try:
        service.start()
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        logger.info("收到中断信号，开始停止服务...")
    finally:
        server.server_close()
        service.stop()


def build_service_from_env() -> tuple[WeComBotService, str, int]:
    if load_dotenv is not None:
        load_dotenv()

    callback_token = os.getenv("WECOM_CALLBACK_TOKEN", "videoToMarkdown").strip()
    encoding_aes_key = os.getenv("WECOM_ENCODING_AES_KEY", "").strip()
    callback_path = os.getenv("WECOM_CALLBACK_PATH", "/wechat/callback").strip() or "/wechat/callback"
    listen_host = os.getenv("WECOM_LISTEN_HOST", "0.0.0.0").strip() or "0.0.0.0"
    listen_port = _parse_int_env("WECOM_LISTEN_PORT", 5000)

    corp_id = os.getenv("WECOM_CORP_ID", "").strip()
    corp_secret = os.getenv("WECOM_CORP_SECRET", "").strip()
    agent_id_raw = os.getenv("WECOM_AGENT_ID", "").strip()
    receive_id = os.getenv("WECOM_RECEIVE_ID", corp_id).strip() if (os.getenv("WECOM_RECEIVE_ID") or corp_id) else None

    orchestrator_api = os.getenv("ORCHESTRATOR_API_URL", "http://127.0.0.1:8080/api").strip()
    max_retries = _parse_int_env("WECOM_MAX_RETRIES", 2)
    poll_interval = _parse_int_env("WECOM_TASK_POLL_INTERVAL_SEC", 10)
    backoff_base = _parse_int_env("WECOM_RETRY_BACKOFF_BASE_SEC", 30)
    dedupe_ttl_sec = _parse_int_env("WECOM_MSG_DEDUPE_TTL_SEC", 600)
    timeout_sec = _parse_int_env("WECOM_HTTP_TIMEOUT_SEC", 8)

    agent_id: Optional[int] = None
    if agent_id_raw:
        try:
            agent_id = int(agent_id_raw)
        except Exception:
            logger.warning("WECOM_AGENT_ID 不是有效整数，消息发送将不可用。")
            agent_id = None

    if not encoding_aes_key:
        raise RuntimeError("缺少 WECOM_ENCODING_AES_KEY，请在环境变量或 .env 中配置。")

    messenger = WeComMessageClient(
        corp_id=corp_id,
        corp_secret=corp_secret,
        agent_id=agent_id,
        timeout_sec=timeout_sec,
    )
    orchestrator = OrchestratorClient(api_base_url=orchestrator_api, timeout_sec=timeout_sec)
    executor = SerialJobExecutor(
        orchestrator=orchestrator,
        messenger=messenger,
        max_retries=max_retries,
        poll_interval_sec=poll_interval,
        retry_backoff_base_sec=backoff_base,
    )
    service = WeComBotService(
        callback_token=callback_token,
        encoding_aes_key=encoding_aes_key,
        callback_path=callback_path,
        receive_id=receive_id,
        job_executor=executor,
        messenger=messenger,
        dedupe_ttl_sec=dedupe_ttl_sec,
    )
    return service, listen_host, listen_port


def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG if _parse_bool_env("WECOM_DEBUG", False) else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("WeCom AES 解密后端: %s", _AES_BACKEND or "unavailable")
    service, host, port = build_service_from_env()
    run_server(service=service, host=host, port=port)


if __name__ == "__main__":
    main()
