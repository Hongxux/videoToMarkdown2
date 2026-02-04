"""
模块说明：阶段监控 metrics 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
from collections import defaultdict


@dataclass
class TokenUsage:
    """
    类说明：封装 TokenUsage 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    
    @property
    def total_tokens(self) -> int:
        """
        执行逻辑：
        1) 读取对象内部状态。
        2) 返回属性值。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：对外提供统一读路径，便于维护与扩展。
        输入参数：
        - 无。
        输出参数：
        - 数值型计算结果。"""
        return self.prompt_tokens + self.completion_tokens
    
    def add(self, prompt: int, completion: int):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - prompt: 文本内容（类型：int）。
        - completion: 函数入参（类型：int）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.prompt_tokens += prompt
        self.completion_tokens += completion


@dataclass 
class StepStats:
    """
    类说明：封装 StepStats 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    executions: int = 0
    successes: int = 0
    failures: int = 0
    total_duration_ms: float = 0
    avg_duration_ms: float = 0
    min_duration_ms: float = float('inf')
    max_duration_ms: float = 0
    token_usage: TokenUsage = field(default_factory=TokenUsage)


class MetricsCollector:
    """
    类说明：封装 MetricsCollector 的职责与行为。
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
    收集：
    - Token使用量（按步骤、按模型）
    - 执行时间
    - 成功/失败率
    - LLM调用统计"""
    
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
        self.output_dir = Path(output_dir) if output_dir else Path("output/metrics")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self._step_stats: Dict[str, StepStats] = defaultdict(StepStats)
        self._model_usage: Dict[str, TokenUsage] = defaultdict(TokenUsage)
        self._llm_latencies: List[float] = []
        self._start_time = datetime.now()
        
    def record_step_execution(
        self,
        step_name: str,
        duration_ms: float,
        success: bool = True
    ):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：success
        依据来源（证据链）：
        - 输入参数：success。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - duration_ms: 函数入参（类型：float）。
        - success: 函数入参（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        stats = self._step_stats[step_name]
        stats.executions += 1
        stats.total_duration_ms += duration_ms
        stats.avg_duration_ms = stats.total_duration_ms / stats.executions
        stats.min_duration_ms = min(stats.min_duration_ms, duration_ms)
        stats.max_duration_ms = max(stats.max_duration_ms, duration_ms)
        
        if success:
            stats.successes += 1
        else:
            stats.failures += 1
            
    def record_llm_usage(
        self,
        step_name: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        latency_ms: float
    ):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - model: 模型/推理配置（类型：str）。
        - prompt_tokens: 函数入参（类型：int）。
        - completion_tokens: 函数入参（类型：int）。
        - latency_ms: 函数入参（类型：float）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        # 按步骤统计
        self._step_stats[step_name].token_usage.add(prompt_tokens, completion_tokens)
        
        # 按模型统计
        self._model_usage[model].add(prompt_tokens, completion_tokens)
        
        # 延迟统计
        self._llm_latencies.append(latency_ms)
        
    def get_summary(self) -> Dict:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：self._llm_latencies
        - 条件：stats.executions > 0
        - 条件：stats.min_duration_ms != float('inf')
        依据来源（证据链）：
        - 对象内部状态：self._llm_latencies。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        total_duration = (datetime.now() - self._start_time).total_seconds() * 1000
        
        # Token汇总
        total_tokens = sum(s.token_usage.total_tokens for s in self._step_stats.values())
        total_prompt = sum(s.token_usage.prompt_tokens for s in self._step_stats.values())
        total_completion = sum(s.token_usage.completion_tokens for s in self._step_stats.values())
        
        # LLM延迟统计
        avg_latency = sum(self._llm_latencies) / len(self._llm_latencies) if self._llm_latencies else 0
        
        return {
            "pipeline": {
                "total_duration_ms": total_duration,
                "start_time": self._start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            },
            "tokens": {
                "total": total_tokens,
                "prompt": total_prompt,
                "completion": total_completion,
                "by_model": {
                    model: {
                        "prompt": usage.prompt_tokens,
                        "completion": usage.completion_tokens,
                        "total": usage.total_tokens
                    }
                    for model, usage in self._model_usage.items()
                }
            },
            "llm": {
                "total_calls": len(self._llm_latencies),
                "avg_latency_ms": avg_latency,
                "min_latency_ms": min(self._llm_latencies) if self._llm_latencies else 0,
                "max_latency_ms": max(self._llm_latencies) if self._llm_latencies else 0
            },
            "steps": {
                name: {
                    "executions": stats.executions,
                    "successes": stats.successes,
                    "failures": stats.failures,
                    "success_rate": stats.successes / stats.executions if stats.executions > 0 else 0,
                    "avg_duration_ms": stats.avg_duration_ms,
                    "min_duration_ms": stats.min_duration_ms if stats.min_duration_ms != float('inf') else 0,
                    "max_duration_ms": stats.max_duration_ms,
                    "tokens": stats.token_usage.total_tokens
                }
                for name, stats in self._step_stats.items()
            }
        }
    
    def save(self, filename: str = "metrics.json"):
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
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.get_summary(), f, ensure_ascii=False, indent=2)
        return filepath
    
    def print_summary(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：stats['failures'] == 0
        依据来源（证据链）：
        - 配置字段：failures。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        summary = self.get_summary()
        
        print("\n" + "="*60)
        print("Pipeline Metrics Summary")
        print("="*60)
        
        print(f"\n⏱  Total Duration: {summary['pipeline']['total_duration_ms']:.0f}ms")
        print(f"🔤 Total Tokens: {summary['tokens']['total']:,}")
        print(f"   - Prompt: {summary['tokens']['prompt']:,}")
        print(f"   - Completion: {summary['tokens']['completion']:,}")
        print(f"🤖 LLM Calls: {summary['llm']['total_calls']}")
        print(f"   - Avg Latency: {summary['llm']['avg_latency_ms']:.0f}ms")
        
        print("\n📊 By Step:")
        for name, stats in summary['steps'].items():
            status = "✓" if stats['failures'] == 0 else "⚠"
            print(f"   {status} {name}: {stats['avg_duration_ms']:.0f}ms, {stats['tokens']} tokens")
        
        print("\n📦 By Model:")
        for model, usage in summary['tokens']['by_model'].items():
            print(f"   {model}: {usage['total']:,} tokens")
        
        print("="*60 + "\n")
