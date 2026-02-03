"""
指标收集器
收集和聚合管道执行指标
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
from collections import defaultdict


@dataclass
class TokenUsage:
    """Token使用量"""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    
    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens
    
    def add(self, prompt: int, completion: int):
        self.prompt_tokens += prompt
        self.completion_tokens += completion


@dataclass 
class StepStats:
    """步骤统计"""
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
    指标收集器
    
    收集：
    - Token使用量（按步骤、按模型）
    - 执行时间
    - 成功/失败率
    - LLM调用统计
    """
    
    def __init__(self, output_dir: Optional[Path] = None):
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
        """记录步骤执行"""
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
        """记录LLM使用"""
        # 按步骤统计
        self._step_stats[step_name].token_usage.add(prompt_tokens, completion_tokens)
        
        # 按模型统计
        self._model_usage[model].add(prompt_tokens, completion_tokens)
        
        # 延迟统计
        self._llm_latencies.append(latency_ms)
        
    def get_summary(self) -> Dict:
        """获取指标汇总"""
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
        """保存指标"""
        filepath = self.output_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.get_summary(), f, ensure_ascii=False, indent=2)
        return filepath
    
    def print_summary(self):
        """打印摘要"""
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
