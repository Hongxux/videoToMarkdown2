"""
模块说明：阶段监控 tracer 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。
补充说明：
实现：
- 步骤执行时间追踪
- 状态变化追踪
- 可视化执行流程
- Mermaid 图导出"""

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path
from enum import Enum


class EventType(str, Enum):
    """
    类说明：封装 EventType 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    STEP_START = "step_start"
    STEP_END = "step_end"
    STEP_ERROR = "step_error"
    LLM_CALL = "llm_call"
    TOOL_CALL = "tool_call"
    STATE_UPDATE = "state_update"
    CHECKPOINT = "checkpoint"


@dataclass
class TraceEvent:
    """
    类说明：封装 TraceEvent 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    event_id: str
    step_name: str
    event_type: EventType
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)
    duration_ms: Optional[float] = None
    parent_event_id: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["event_type"] = self.event_type.value
        return d


@dataclass
class StepMetrics:
    """
    类说明：封装 StepMetrics 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
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
    类说明：封装 PipelineTracer 的职责与行为。
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
    - 实时追踪步骤执行
    - 收集性能指标
    - 导出多种格式（JSON、Mermaid、HTML）"""
    
    def __init__(self, output_dir: Optional[Path] = None):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：output_dir
        依据来源（证据链）：
        - 输入参数：output_dir。
        输入参数：
        - output_dir: 目录路径（类型：Optional[Path]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 字符串结果。"""
        self._event_counter += 1
        return f"evt_{self._event_counter:06d}"
    
    def trace_step_start(self, step_name: str, input_data: Optional[Dict] = None) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：step_name not in self.step_metrics
        - 条件：input_data
        依据来源（证据链）：
        - 输入参数：input_data, step_name。
        - 对象内部状态：self.step_metrics。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - input_data: 函数入参（类型：Optional[Dict]）。
        输出参数：
        - 字符串结果。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：step_name in self.step_metrics and self.step_metrics[step_name].start_time
        - 条件：step_name in self.step_metrics
        - 条件：success
        依据来源（证据链）：
        - 输入参数：output_data, step_name, success。
        - 对象内部状态：self.step_metrics。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - output_data: 函数入参（类型：Optional[Dict]）。
        - success: 函数入参（类型：bool）。
        - parent_event_id: 标识符（类型：Optional[str]）。
        输出参数：
        - 字符串结果。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：step_name in self.step_metrics
        依据来源（证据链）：
        - 输入参数：step_name。
        - 对象内部状态：self.step_metrics。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - error: 函数入参（类型：Exception）。
        - context: 函数入参（类型：Optional[Dict]）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：step_name in self.step_metrics
        依据来源（证据链）：
        - 输入参数：step_name。
        - 对象内部状态：self.step_metrics。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - model: 模型/推理配置（类型：str）。
        - tokens: 函数入参（类型：int）。
        - latency_ms: 函数入参（类型：float）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：step_name in self.step_metrics
        依据来源（证据链）：
        - 输入参数：step_name。
        - 对象内部状态：self.step_metrics。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - tool_name: 函数入参（类型：str）。
        - duration_ms: 函数入参（类型：float）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - name: 函数入参（类型：str）。
        - data: 数据列表/集合（类型：Optional[Dict]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        event = TraceEvent(
            event_id=self._generate_event_id(),
            step_name="checkpoint",
            event_type=EventType.CHECKPOINT,
            timestamp=datetime.now(),
            data={"checkpoint_name": name, "data": data}
        )
        self.events.append(event)
    
    def _summarize(self, data: Dict[str, Any]) -> Dict:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：isinstance(value, list)
        - 条件：isinstance(value, dict)
        - 条件：isinstance(value, str) and len(value) > 100
        依据来源（证据链）：
        输入参数：
        - data: 数据列表/集合（类型：Dict[str, Any]）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
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
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - 无。
        输出参数：
        - Dict 列表（与输入或处理结果一一对应）。"""
        return [event.to_dict() for event in self.events]
    
    def get_metrics_summary(self) -> Dict:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - filename: 函数入参（类型：str）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：step in self.step_metrics
        - 条件：current in self.step_metrics and next_step in self.step_metrics
        - 条件：metrics.status == 'success'
        依据来源（证据链）：
        - 对象内部状态：self.step_metrics。
        输入参数：
        - 无。
        输出参数：
        - 字符串结果。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.export_json()
        
        # 保存Mermaid图
        mermaid_path = self.output_dir / "execution_flow.mmd"
        with open(mermaid_path, "w", encoding="utf-8") as f:
            f.write(self.export_mermaid())
        
        # 保存指标汇总
        metrics_path = self.output_dir / "metrics_summary.json"
        with open(metrics_path, "w", encoding="utf-8") as f:
            json.dump(self.get_metrics_summary(), f, ensure_ascii=False, indent=2)
