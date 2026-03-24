"""
模块说明：Stage1 恢复辅助元数据。
执行逻辑：
1) 聚合 step 索引与线程标识生成逻辑。
2) 为 graph/streaming executor 提供一致的恢复序号。
实现方式：通过轻量纯函数与常量映射实现。
核心价值：避免 Stage1 再维护一套专用 checkpoint 持久化。"""

from services.python_grpc.src.common.utils.hash_policy import md5_short_text_compat

STEP_INDEX_MAP = {
    "step1_validate": 1,
    "step2_correction": 2,
    "step3_merge": 3,
    "step3_5_translate": 4,
    "step4_clean_local": 5,
    "step5_clean_cross": 5,
    "step6_merge_cross": 6,
    "step5_6_dedup_merge": 6,
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
    return md5_short_text_compat(content)
