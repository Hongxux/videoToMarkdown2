"""
模块说明：阶段1流水线 checkpoint 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import sqlite3
import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from contextlib import contextmanager


class SQLiteCheckpointer:
    """
    类说明：封装 SQLiteCheckpointer 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。
    补充说明：
    特性：
    - 跨进程持久化状态
    - 支持多线程/视频并行处理
    - 自动清理旧检查点
    - 支持查询和恢复历史状态"""
    
    def __init__(self, db_path: str = "checkpoints.db"):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - db_path: 文件路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        
    def _init_db(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - thread_id: 标识符（类型：str）。
        - video_path: 文件路径（类型：str）。
        - subtitle_path: 文件路径（类型：str）。
        - output_dir: 目录路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO runs 
                (thread_id, video_path, subtitle_path, output_dir, started_at, status, last_step, completed_steps)
                VALUES (?, ?, ?, ?, datetime('now'), 'running', NULL, 0)
            """, (thread_id, video_path, subtitle_path, output_dir))
    
    def get_run_info(self, thread_id: str) -> Optional[Dict[str, Any]]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：row
        依据来源（证据链）：
        输入参数：
        - thread_id: 标识符（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
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
        """
        执行逻辑：
        1) 校验输入值。
        2) 更新内部状态或持久化。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：集中更新状态，保证一致性。
        输入参数：
        - thread_id: 标识符（类型：str）。
        - status: 函数入参（类型：str）。
        - last_step: 函数入参（类型：str）。
        - completed_steps: 函数入参（类型：int）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
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
        """
        执行逻辑：
        1) 组织输出结构与格式。
        2) 写入目标路径并处理异常。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：统一输出格式，降低落盘与格式错误。
        输入参数：
        - thread_id: 标识符（类型：str）。
        - step_name: 函数入参（类型：str）。
        - step_index: 函数入参（类型：int）。
        - state: 函数入参（类型：Dict[str, Any]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
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
        执行逻辑：
        1) 校验输入路径与参数。
        2) 读取并解析为结构化对象。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：将外部数据转为内部结构，统一输入口径。
        决策逻辑：
        - 条件：step_name
        - 条件：row
        依据来源（证据链）：
        - 输入参数：step_name。
        输入参数：
        - thread_id: 标识符（类型：str）。
        - step_name: 函数入参（类型：Optional[str]）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
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
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：row
        依据来源（证据链）：
        输入参数：
        - thread_id: 标识符（类型：str）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT step_name FROM checkpoints 
                WHERE thread_id = ?
                ORDER BY step_index DESC
                LIMIT 1
            """, (thread_id,)).fetchone()
            
            return row[0] if row else None
    
    def list_checkpoints(self, thread_id: str) -> List[Dict[str, Any]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - thread_id: 标识符（类型：str）。
        输出参数：
        - Dict[str, Any] 列表（与输入或处理结果一一对应）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - days: 函数入参（类型：int）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - thread_id: 标识符（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
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
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - video_path: 文件路径（类型：str）。
    - subtitle_path: 文件路径（类型：str）。
    输出参数：
    - 字符串结果。"""
    content = f"{video_path}:{subtitle_path}"
    return hashlib.md5(content.encode()).hexdigest()[:12]
