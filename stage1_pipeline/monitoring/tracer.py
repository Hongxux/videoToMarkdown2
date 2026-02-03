"""
执行追踪器
实现：
- 步骤执行时间追踪
- 状态变化追踪
- 可视化执行流程
- Mermaid 图导出
"""

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path
from enum import Enum


class EventType(str, Enum):
    """事件类型"""
    STEP_START = "step_start"
    STEP_END = "step_end"
    STEP_ERROR = "step_error"
    LLM_CALL = "llm_call"
    TOOL_CALL = "tool_call"
    STATE_UPDATE = "state_update"
    CHECKPOINT = "checkpoint"


@dataclass
class TraceEvent:
    """追踪事件"""
    event_id: str
    step_name: str
    event_type: EventType
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)
    duration_ms: Optional[float] = None
    parent_event_id: Optional[str] = None
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["event_type"] = self.event_type.value
        return d


@dataclass
class StepMetrics:
    """步骤指标"""
    step_name: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_ms: float = 0
    llm_calls: int = 0
    tool_calls: int = 0
    total_tokens: int = 0
    errors: int = 0
    status: str = "pending"  # pending/running/success/error


class PipelineTracer:
    """
    管道执行追踪器
    
    特性：
    - 实时追踪步骤执行
    - 收集性能指标
    - 导出多种格式（JSON、Mermaid、HTML）
    """
    
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("output/traces")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.events: List[TraceEvent] = []
        self.step_metrics: Dict[str, StepMetrics] = {}
        self._event_counter = 0
        self._start_time = datetime.now()
        
        # 步骤定义（用于可视化）
        self.step_order = [
            "step1_validate", "step2_correction", "step3_merge",
            "step4_clean_local", "step5_clean_cross", "step6_merge_cross",
            "step7_segment", "step8a_fault_detect", "step8b_fault_locate",
            "step9_strategy", "step10_timing", "step11_instruction",
            "step12_capture", "step13_validate_frame", "step14_vision_qa", "step15_retry",
            "step16_viz_need", "step17_viz_form", "step18_core_content", "step19_auxiliary",
            "step20_integrate", "step21_reconstruct", "step22_markdown",
            "step23_video_name", "step24_screenshot_name"
        ]
        
    def _generate_event_id(self) -> str:
        """生成事件ID"""
        self._event_counter += 1
        return f"evt_{self._event_counter:06d}"
    
    def trace_step_start(self, step_name: str, input_data: Optional[Dict] = None) -> str:
        """追踪步骤开始"""
        event_id = self._generate_event_id()
        event = TraceEvent(
            event_id=event_id,
            step_name=step_name,
            event_type=EventType.STEP_START,
            timestamp=datetime.now(),
            data={"input_summary": self._summarize(input_data) if input_data else {}}
        )
        self.events.append(event)
        
        # 更新指标
        if step_name not in self.step_metrics:
            self.step_metrics[step_name] = StepMetrics(step_name=step_name)
        self.step_metrics[step_name].start_time = event.timestamp
        self.step_metrics[step_name].status = "running"
        
        return event_id
    
    def trace_step_end(
        self, 
        step_name: str, 
        output_data: Optional[Dict] = None,
        success: bool = True,
        parent_event_id: Optional[str] = None
    ) -> str:
        """追踪步骤结束"""
        event_id = self._generate_event_id()
        now = datetime.now()
        
        # 计算耗时
        duration_ms = 0
        if step_name in self.step_metrics and self.step_metrics[step_name].start_time:
            duration_ms = (now - self.step_metrics[step_name].start_time).total_seconds() * 1000
        
        event = TraceEvent(
            event_id=event_id,
            step_name=step_name,
            event_type=EventType.STEP_END,
            timestamp=now,
            data={"output_summary": self._summarize(output_data) if output_data else {}},
            duration_ms=duration_ms,
            parent_event_id=parent_event_id
        )
        self.events.append(event)
        
        # 更新指标
        if step_name in self.step_metrics:
            self.step_metrics[step_name].end_time = now
            self.step_metrics[step_name].duration_ms = duration_ms
            self.step_metrics[step_name].status = "success" if success else "error"
        
        return event_id
    
    def trace_step_error(self, step_name: str, error: Exception, context: Optional[Dict] = None):
        """追踪步骤错误"""
        event_id = self._generate_event_id()
        event = TraceEvent(
            event_id=event_id,
            step_name=step_name,
            event_type=EventType.STEP_ERROR,
            timestamp=datetime.now(),
            data={
                "error_type": type(error).__name__,
                "error_message": str(error),
                "context": context
            }
        )
        self.events.append(event)
        
        if step_name in self.step_metrics:
            self.step_metrics[step_name].errors += 1
            self.step_metrics[step_name].status = "error"
            
        return event_id
    
    def trace_llm_call(
        self, 
        step_name: str,
        model: str,
        tokens: int,
        latency_ms: float
    ):
        """追踪LLM调用"""
        event_id = self._generate_event_id()
        event = TraceEvent(
            event_id=event_id,
            step_name=step_name,
            event_type=EventType.LLM_CALL,
            timestamp=datetime.now(),
            data={"model": model, "tokens": tokens, "latency_ms": latency_ms}
        )
        self.events.append(event)
        
        if step_name in self.step_metrics:
            self.step_metrics[step_name].llm_calls += 1
            self.step_metrics[step_name].total_tokens += tokens
            
        return event_id
    
    def trace_tool_call(self, step_name: str, tool_name: str, duration_ms: float):
        """追踪工具调用"""
        event_id = self._generate_event_id()
        event = TraceEvent(
            event_id=event_id,
            step_name=step_name,
            event_type=EventType.TOOL_CALL,
            timestamp=datetime.now(),
            data={"tool_name": tool_name, "duration_ms": duration_ms}
        )
        self.events.append(event)
        
        if step_name in self.step_metrics:
            self.step_metrics[step_name].tool_calls += 1
            
        return event_id
    
    def checkpoint(self, name: str, data: Optional[Dict] = None):
        """创建检查点"""
        event = TraceEvent(
            event_id=self._generate_event_id(),
            step_name="checkpoint",
            event_type=EventType.CHECKPOINT,
            timestamp=datetime.now(),
            data={"checkpoint_name": name, "data": data}
        )
        self.events.append(event)
    
    def _summarize(self, data: Dict[str, Any]) -> Dict:
        """创建数据摘要"""
        summary = {}
        for key, value in data.items():
            if isinstance(value, list):
                summary[key] = f"[{len(value)} items]"
            elif isinstance(value, dict):
                summary[key] = f"{{...}}"
            elif isinstance(value, str) and len(value) > 100:
                summary[key] = f"{value[:100]}..."
            else:
                summary[key] = value
        return summary
    
    def get_timeline(self) -> List[Dict]:
        """获取执行时间线"""
        return [event.to_dict() for event in self.events]
    
    def get_metrics_summary(self) -> Dict:
        """获取指标汇总"""
        total_duration = (datetime.now() - self._start_time).total_seconds() * 1000
        total_tokens = sum(m.total_tokens for m in self.step_metrics.values())
        total_llm_calls = sum(m.llm_calls for m in self.step_metrics.values())
        
        return {
            "total_duration_ms": total_duration,
            "total_tokens": total_tokens,
            "total_llm_calls": total_llm_calls,
            "steps_completed": sum(1 for m in self.step_metrics.values() if m.status == "success"),
            "steps_failed": sum(1 for m in self.step_metrics.values() if m.status == "error"),
            "step_details": {
                name: {
                    "duration_ms": m.duration_ms,
                    "llm_calls": m.llm_calls,
                    "tokens": m.total_tokens,
                    "status": m.status
                }
                for name, m in self.step_metrics.items()
            }
        }
    
    def export_json(self, filename: str = "execution_trace.json"):
        """导出JSON格式"""
        filepath = self.output_dir / filename
        export_data = {
            "start_time": self._start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "metrics_summary": self.get_metrics_summary(),
            "events": self.get_timeline()
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        return filepath
    
    def export_mermaid(self) -> str:
        """导出Mermaid图"""
        lines = ["graph TD"]
        
        # 添加节点
        for i, step in enumerate(self.step_order):
            if step in self.step_metrics:
                metrics = self.step_metrics[step]
                status_icon = "✓" if metrics.status == "success" else "✗" if metrics.status == "error" else "⋯"
                duration = f"{metrics.duration_ms:.0f}ms" if metrics.duration_ms else "pending"
                lines.append(f'    {step}["{step}<br/>{status_icon} {duration}"]')
                
                # 设置颜色
                if metrics.status == "success":
                    lines.append(f'    style {step} fill:#90EE90')
                elif metrics.status == "error":
                    lines.append(f'    style {step} fill:#FFB6C1')
                elif metrics.status == "running":
                    lines.append(f'    style {step} fill:#87CEEB')
        
        # 添加边
        for i in range(len(self.step_order) - 1):
            current = self.step_order[i]
            next_step = self.step_order[i + 1]
            if current in self.step_metrics and next_step in self.step_metrics:
                lines.append(f'    {current} --> {next_step}')
        
        return "\n".join(lines)
    
    def save(self):
        """保存追踪数据"""
        self.export_json()
        
        # 保存Mermaid图
        mermaid_path = self.output_dir / "execution_flow.mmd"
        with open(mermaid_path, "w", encoding="utf-8") as f:
            f.write(self.export_mermaid())
        
        # 保存指标汇总
        metrics_path = self.output_dir / "metrics_summary.json"
        with open(metrics_path, "w", encoding="utf-8") as f:
            json.dump(self.get_metrics_summary(), f, ensure_ascii=False, indent=2)
