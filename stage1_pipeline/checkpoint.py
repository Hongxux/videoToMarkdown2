"""
SQLite 持久化检查点管理器
支持跨进程断点续跑
"""

import sqlite3
import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from contextlib import contextmanager


class SQLiteCheckpointer:
    """
    SQLite 持久化检查点管理器
    
    特性：
    - 跨进程持久化状态
    - 支持多线程/视频并行处理
    - 自动清理旧检查点
    - 支持查询和恢复历史状态
    """
    
    def __init__(self, db_path: str = "checkpoints.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        
    def _init_db(self):
        """初始化数据库表"""
        with self._get_conn() as conn:
            conn.executescript("""
                -- 主检查点表
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    step_index INTEGER NOT NULL,
                    state_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(thread_id, step_name)
                );
                
                -- 运行元数据表
                CREATE TABLE IF NOT EXISTS runs (
                    thread_id TEXT PRIMARY KEY,
                    video_path TEXT,
                    subtitle_path TEXT,
                    output_dir TEXT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated_at TIMESTAMP,
                    status TEXT DEFAULT 'running',
                    last_step TEXT,
                    total_steps INTEGER DEFAULT 24,
                    completed_steps INTEGER DEFAULT 0
                );
                
                -- 索引
                CREATE INDEX IF NOT EXISTS idx_checkpoints_thread 
                    ON checkpoints(thread_id);
                CREATE INDEX IF NOT EXISTS idx_checkpoints_step 
                    ON checkpoints(thread_id, step_index);
            """)
    
    @contextmanager
    def _get_conn(self):
        """获取数据库连接"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
    
    # ========== 运行管理 ==========
    
    def start_run(
        self,
        thread_id: str,
        video_path: str,
        subtitle_path: str,
        output_dir: str
    ):
        """开始新运行"""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO runs 
                (thread_id, video_path, subtitle_path, output_dir, started_at, status, last_step, completed_steps)
                VALUES (?, ?, ?, ?, datetime('now'), 'running', NULL, 0)
            """, (thread_id, video_path, subtitle_path, output_dir))
    
    def get_run_info(self, thread_id: str) -> Optional[Dict[str, Any]]:
        """获取运行信息"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE thread_id = ?",
                (thread_id,)
            ).fetchone()
            
            if row:
                return dict(row)
            return None
    
    def update_run_status(
        self,
        thread_id: str,
        status: str,
        last_step: str,
        completed_steps: int
    ):
        """更新运行状态"""
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE runs 
                SET status = ?, last_step = ?, completed_steps = ?, 
                    last_updated_at = datetime('now')
                WHERE thread_id = ?
            """, (status, last_step, completed_steps, thread_id))
    
    # ========== 检查点管理 ==========
    
    def save_checkpoint(
        self,
        thread_id: str,
        step_name: str,
        step_index: int,
        state: Dict[str, Any]
    ):
        """保存检查点"""
        state_json = json.dumps(state, ensure_ascii=False, default=str)
        
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO checkpoints 
                (thread_id, step_name, step_index, state_json, created_at)
                VALUES (?, ?, ?, ?, datetime('now'))
            """, (thread_id, step_name, step_index, state_json))
            
            # 更新运行状态
            conn.execute("""
                UPDATE runs 
                SET last_step = ?, completed_steps = ?, last_updated_at = datetime('now')
                WHERE thread_id = ?
            """, (step_name, step_index, thread_id))
    
    def load_checkpoint(
        self,
        thread_id: str,
        step_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        加载检查点
        
        Args:
            thread_id: 线程ID
            step_name: 步骤名（可选，默认加载最新）
        """
        with self._get_conn() as conn:
            if step_name:
                row = conn.execute("""
                    SELECT state_json FROM checkpoints 
                    WHERE thread_id = ? AND step_name = ?
                """, (thread_id, step_name)).fetchone()
            else:
                # 加载最新检查点
                row = conn.execute("""
                    SELECT state_json FROM checkpoints 
                    WHERE thread_id = ?
                    ORDER BY step_index DESC
                    LIMIT 1
                """, (thread_id,)).fetchone()
            
            if row:
                return json.loads(row[0])
            return None
    
    def get_last_completed_step(self, thread_id: str) -> Optional[str]:
        """获取最后完成的步骤"""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT step_name FROM checkpoints 
                WHERE thread_id = ?
                ORDER BY step_index DESC
                LIMIT 1
            """, (thread_id,)).fetchone()
            
            return row[0] if row else None
    
    def list_checkpoints(self, thread_id: str) -> List[Dict[str, Any]]:
        """列出所有检查点"""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT step_name, step_index, created_at 
                FROM checkpoints 
                WHERE thread_id = ?
                ORDER BY step_index
            """, (thread_id,)).fetchall()
            
            return [dict(row) for row in rows]
    
    # ========== 清理 ==========
    
    def cleanup_old_runs(self, days: int = 7):
        """清理旧运行记录"""
        with self._get_conn() as conn:
            conn.execute("""
                DELETE FROM checkpoints 
                WHERE thread_id IN (
                    SELECT thread_id FROM runs 
                    WHERE started_at < datetime('now', ?)
                )
            """, (f'-{days} days',))
            
            conn.execute("""
                DELETE FROM runs 
                WHERE started_at < datetime('now', ?)
            """, (f'-{days} days',))
    
    def delete_run(self, thread_id: str):
        """删除运行记录"""
        with self._get_conn() as conn:
            conn.execute("DELETE FROM checkpoints WHERE thread_id = ?", (thread_id,))
            conn.execute("DELETE FROM runs WHERE thread_id = ?", (thread_id,))


# ============================================================================
# 步骤索引映射
# ============================================================================

STEP_INDEX_MAP = {
    "step1_validate": 1,
    "step2_correction": 2,
    "step3_merge": 3,
    "step4_clean_local": 4,
    "step5_clean_cross": 5,
    "step6_merge_cross": 6,
    "step7_segment": 7,
    "step7c_kp_merge": 8,
    "step7b_viz_scene": 9,
    "step8a_fault_detect": 10,
    "step8b_fault_locate": 11,
    "step9_strategy": 12,
    "step10_timing": 13,
    "step11_instruction": 14,
    "step12_capture": 15,
    "step13_validate_frame": 16,
    "step14_vision_qa": 17,
    "step15_retry": 18,
    "step15b_postprocess": 19,
    "step16_viz_need": 20,
    "step17_viz_form": 21,
    "step18_core_content": 22,
    "step19_auxiliary": 23,
    "step20_integrate": 24,
    "step21_reconstruct": 25,
    "step22_markdown": 26,
    "step23_video_name": 27,
    "step24_screenshot_name": 28
}


def generate_thread_id(video_path: str, subtitle_path: str) -> str:
    """根据输入文件生成唯一 thread_id"""
    content = f"{video_path}:{subtitle_path}"
    return hashlib.md5(content.encode()).hexdigest()[:12]
