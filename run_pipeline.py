#!/usr/bin/env python
"""
Stage1 Pipeline Runner
支持：
- SQLite 持久化检查点（断点续跑）
- 可配置的步骤中间产物输出
- 可配置的日志级别
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from stage1_pipeline import run_pipeline, create_pipeline_graph
from stage1_pipeline.checkpoint import SQLiteCheckpointer, generate_thread_id


def main():
    parser = argparse.ArgumentParser(
        description="Stage1 视频文字稿处理流程",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 基本运行
  python run_pipeline.py -v video.mp4 -s subs.txt

  # 输出所有步骤中间产物
  python run_pipeline.py -v video.mp4 -s subs.txt --output-all-steps

  # 仅输出关键步骤
  python run_pipeline.py -v video.mp4 -s subs.txt --output-steps step1_validate step7_segment

  # 断点续跑
  python run_pipeline.py -v video.mp4 -s subs.txt --resume

  # 查看检查点状态
  python run_pipeline.py --list-checkpoints --output my_output
        """
    )
    
    # 输入文件
    parser.add_argument("--video", "-v", help="视频文件路径")
    parser.add_argument("--subtitle", "-s", help="字幕文件路径")
    parser.add_argument("--output", "-o", default="output", help="输出目录 (默认: output)")
    
    # 检查点选项
    checkpoint_group = parser.add_argument_group("检查点选项")
    checkpoint_group.add_argument(
        "--enable-sqlite", action="store_true", default=True,
        help="启用 SQLite 持久化检查点 (默认开启)"
    )
    checkpoint_group.add_argument(
        "--no-sqlite", action="store_true",
        help="禁用 SQLite 持久化检查点"
    )
    checkpoint_group.add_argument(
        "--resume", action="store_true",
        help="从上次中断处继续运行"
    )
    checkpoint_group.add_argument(
        "--thread-id", type=str,
        help="指定线程ID (用于检查点，默认自动生成)"
    )
    checkpoint_group.add_argument(
        "--list-checkpoints", action="store_true",
        help="列出所有检查点并退出"
    )
    
    # 中间产物输出选项
    output_group = parser.add_argument_group("中间产物输出")
    output_group.add_argument(
        "--output-all-steps", action="store_true",
        help="输出所有步骤的中间产物"
    )
    output_group.add_argument(
        "--output-steps", nargs="+", type=str,
        help="指定输出哪些步骤的中间产物 (如: step1_validate step7_segment)"
    )
    output_group.add_argument(
        "--no-intermediate", action="store_true",
        help="禁用所有中间产物输出"
    )
    
    # 日志选项
    log_group = parser.add_argument_group("日志选项")
    log_group.add_argument(
        "--log-io", action="store_true", default=True,
        help="记录每步的输入/输出 (默认开启)"
    )
    log_group.add_argument(
        "--no-log-io", action="store_true",
        help="禁用输入/输出日志"
    )
    log_group.add_argument(
        "--debug", action="store_true",
        help="启用调试模式（详细日志）"
    )
    
    # 其他
    parser.add_argument("--show-graph", action="store_true", help="显示流程图并退出")
    parser.add_argument("--no-checkpoints", action="store_true", help="禁用内存检查点")
    
    args = parser.parse_args()
    
    # 显示流程图
    if args.show_graph:
        from stage1_pipeline.graph import get_graph_mermaid
        print(get_graph_mermaid())
        return
    
    # 列出检查点
    if args.list_checkpoints:
        db_path = Path(args.output) / "checkpoints.db"
        if not db_path.exists():
            print(f"未找到检查点数据库: {db_path}")
            return
        
        checkpointer = SQLiteCheckpointer(str(db_path))
        print("\n📋 检查点列表")
        print("=" * 60)
        
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        
        runs = conn.execute("SELECT * FROM runs ORDER BY started_at DESC").fetchall()
        for run in runs:
            print(f"\n🔹 Thread: {run['thread_id']}")
            print(f"   状态: {run['status']}")
            print(f"   最后步骤: {run['last_step']}")
            print(f"   完成步骤: {run['completed_steps']}/25")
            print(f"   开始时间: {run['started_at']}")
            
            # 列出检查点
            checkpoints = checkpointer.list_checkpoints(run['thread_id'])
            if checkpoints:
                print(f"   检查点: {', '.join([c['step_name'] for c in checkpoints])}")
        
        conn.close()
        return
    
    # 验证必需参数
    if not args.video or not args.subtitle:
        parser.error("--video 和 --subtitle 参数是必需的")
    
    # 验证输入文件
    video_path = Path(args.video)
    subtitle_path = Path(args.subtitle)
    
    if not video_path.exists():
        print(f"错误: 视频文件不存在: {video_path}")
        sys.exit(1)
        
    if not subtitle_path.exists():
        print(f"错误: 字幕文件不存在: {subtitle_path}")
        sys.exit(1)
    
    # 创建输出目录
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("Stage1 视频文字稿处理流程")
    print("=" * 60)
    print(f"视频: {video_path}")
    print(f"字幕: {subtitle_path}")
    print(f"输出: {output_dir}")
    
    # 配置选项
    enable_sqlite = args.enable_sqlite and not args.no_sqlite
    output_steps = None
    output_all = args.output_all_steps
    
    if args.no_intermediate:
        output_steps = []
        output_all = False
    elif args.output_steps:
        output_steps = args.output_steps
    
    if enable_sqlite:
        print(f"检查点: SQLite 持久化")
    if args.resume:
        print(f"模式: 断点续跑")
    if output_all:
        print(f"中间产物: 全部输出")
    elif output_steps:
        print(f"中间产物: {', '.join(output_steps)}")
    
    print("=" * 60)
    
    # 运行管道
    try:
        result = asyncio.run(run_pipeline(
            video_path=str(video_path),
            subtitle_path=str(subtitle_path),
            output_dir=str(output_dir),
            enable_checkpoints=not args.no_checkpoints,
            enable_sqlite=enable_sqlite,
            resume=args.resume,
            output_steps=output_steps,
            output_all_steps=output_all,
            thread_id=args.thread_id
        ))
        
        print("\n" + "=" * 60)
        print("处理完成!")
        print("=" * 60)
        
        # 打印摘要
        print(f"\n📊 处理结果摘要:")
        print(f"  - 领域: {result.get('domain', 'N/A')}")
        print(f"  - 主题: {result.get('main_topic', 'N/A')}")
        print(f"  - 知识片段数: {len(result.get('knowledge_segments', []))}")
        print(f"  - 识别断层数: {len(result.get('semantic_faults', []))}")
        
        # 打印输出文件位置
        print(f"\n📁 输出文件:")
        print(f"  - Markdown: {result.get('output_markdown_path', 'N/A')}")
        print(f"  - 日志: {output_dir}/logs/")
        print(f"  - 中间产物: {output_dir}/intermediates/")
        print(f"  - 检查点: {output_dir}/checkpoints.db")
        
    except KeyboardInterrupt:
        print("\n\n⚠ 用户中断 (检查点已保存，可使用 --resume 继续)")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n❌ 处理失败: {str(e)}")
        if args.debug:
            import traceback
            traceback.print_exc()
        print("\n💡 提示: 可使用 --resume 从上次中断处继续")
        sys.exit(1)


if __name__ == "__main__":
    main()
