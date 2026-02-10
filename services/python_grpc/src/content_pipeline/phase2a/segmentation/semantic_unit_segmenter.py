"""
模块说明：Module2 语义单元切分（SemanticUnitSegmenter）。
执行逻辑：
1) 从 step6_merge_cross_output.json 读取段落输入。
2) 调用 LLM 完成语义单元切分与结构化映射。
核心价值：将“段落到语义单元”的规则化处理集中封装，降低主流程复杂度。
"""

import os
import json
import logging
import asyncio
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
# 统一 LLM 调用入口
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt, render_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys

logger = logging.getLogger(__name__)


SEGMENTATION_MAX_OUTPUT_TOKENS = 8192
KNOWLEDGE_TYPE_CODE_MAP = {
    0: "abstract",
    1: "concrete",
    2: "process",
}
DEFAULT_UNIT_CONFIDENCE = 0.8


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class SemanticUnit:
    """类说明：SemanticUnit 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    unit_id: str                          # SU001, SU002, ...
    knowledge_type: str                   # abstract | concrete | process
    knowledge_topic: str                  # 鏍稿績鐭ヨ瘑鐐规爣绛?
    full_text: str                        # 瀹屾暣鏂囨湰
    source_paragraph_ids: List[str]       # 鏉ユ簮娈佃惤ID (P001, P002, ...)
    source_sentence_ids: List[str]        # 鏉ユ簮鍙ュ瓙ID (S001, S002, ...)
    start_sec: float = 0.0                # 璧峰鏃堕棿
    end_sec: float = 0.0                  # 缁撴潫鏃堕棿
    confidence: float = 0.0               # LLM鍒ゅ畾缃俊搴?
    mult_steps: bool = False              # 鏄惁涓哄姝ラ鍗曞厓锛堟帹鐞?閰嶇疆/瀹炴搷锛?
    action_segments: List[Dict] = None    # V7.x: 鍔ㄤ綔鍖洪棿璇︽儏 [{start, end, type}]
    stable_islands: List[Dict] = None     # V7.x: 绋冲畾宀涘尯闂?[{start, end, mid, duration}]
    materials: Any = None                 # V7.x: 鐢熸垚鐨勭礌鏉愰泦鍚?(MaterialSet)
    instructional_steps: List[Dict] = None # V8.0: 璇︾粏鐨勬搷浣滄楠?(for tutorial_stepwise)

    def __post_init__(self):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃鍐呴儴鏂规硶璋冪敤/鐘舵€佹洿鏂板疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        鍐崇瓥閫昏緫锛?
        - 鏉′欢锛歴elf.action_segments is None
        - 鏉′欢锛歴elf.stable_islands is None
        渚濇嵁鏉ユ簮锛堣瘉鎹摼锛夛細
        - 瀵硅薄鍐呴儴鐘舵€侊細self.action_segments, self.stable_islands銆?
        杈撳叆鍙傛暟锛?
        - 鏃犮€?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?"""
        if self.action_segments is None:
            self.action_segments = []
        if self.stable_islands is None:
            self.stable_islands = []


@dataclass
class SegmentationResult:
    """类说明：SegmentationResult 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    semantic_units: List[SemanticUnit]
    total_paragraphs_input: int
    total_units_output: int
    llm_token_usage: int = 0
    processing_time_ms: float = 0.0


# =============================================================================
# LLM Prompts - v1.3: Knowledge Type Based Segmentation
# =============================================================================

SYSTEM_PROMPT = """浣犳槸璇箟鍗曞厓鍒掑垎鍔╂墜銆?
鍙緭鍑轰弗鏍?JSON锛屼笖鍙厑璁告渶灏忓瓧娈甸泦鍚堬細
- 椤跺眰锛歴emantic_units
- 鍗曞厓瀛楁锛歱ids, k, m

瀛楁瀹氫箟锛?- pids: 娈佃惤ID鏁扮粍锛屼緥濡?[\"P001\", \"P002\"]
- k: 鐭ヨ瘑绫诲瀷缂栫爜锛?=abstract, 1=concrete, 2=process
- m: 鏄惁澶氭楠わ紝0=false, 1=true

绂佹杈撳嚭浠讳綍鍏朵粬瀛楁锛屽挨鍏舵槸 reasoning / confidence / action / text / full_text銆?"""

USER_PROMPT_TEMPLATE = """璇峰杈撳叆娈佃惤杩涜璇箟鍗曞厓鍒掑垎銆?
杈撳嚭瑕佹眰锛?1) 鍙緭鍑?JSON
2) 姣忎釜鍗曞厓浠呬繚鐣?pids + k + m 涓変釜瀛楁
3) k 鍙兘鏄?0/1/2锛?=abstract,1=concrete,2=process锛?4) m 鍙兘鏄?0/1
5) 涓嶅緱杈撳嚭 reasoning / confidence / action / text / full_text

杈撳叆娈佃惤锛?{paragraphs_json}

杈撳嚭妯℃澘锛?{{
  "semantic_units": [
    {{"pids": ["P001", "P002"], "k": 2, "m": 1}}
  ]
}}
"""


# =============================================================================
# LLM Prompts - Phase 3: Cross-modal Conflict Resolution
# =============================================================================

RESEGMENT_SYSTEM_PROMPT = """浣犳槸璇箟鍗曞厓鍒囧垎涓撳锛屾搮闀垮鐞嗚法妯℃€佸啿绐併€?
CV妯″潡妫€娴嬪埌鏌愪簺璇箟鍗曞厓瀛樺湪瑙嗚涓庢枃鏈殑涓嶄竴鑷达紝浣犻渶瑕佸喅瀹氬浣曞鐞嗚繖浜涘啿绐併€?

鍐崇瓥鍘熷垯锛?
1. 濡傛灉鏂囨湰璇箟纭疄璺ㄨ秺浜嗕笉鍚岀煡璇嗙被鍨嬭竟鐣?鈫?鎷嗗垎
2. 濡傛灉鍙槸棣栧熬鍖呭惈鍐椾綑鐢婚潰浣嗘牳蹇冭涔夊畬鏁?鈫?寰皟杈圭晫
3. 濡傛灉瑙嗚鍙樺寲鍙槸鍔ㄧ敾鏁堟灉鎴栨棤鍏冲共鎵?鈫?淇濇寔鍘熷垽"""

RESEGMENT_USER_PROMPT = """CV妯″潡妫€娴嬪埌浠ヤ笅璇箟鍗曞厓瀛樺湪"璺ㄦā鎬佸啿绐?锛岃缁撳悎瑙嗚閿氱偣淇℃伅閲嶆柊瀹¤銆?

## 冲突单元信息
- **Unit ID**: {unit_id}
- **褰撳墠鏂囨湰**: "{text}"
- **褰撳墠鏃跺簭**: {start_sec:.1f}s - {end_sec:.1f}s
- **棰勫垽绫诲瀷**: {llm_type}
- **瑙嗚缁熻**: 绋冲畾宀?{s_stable:.0%}, 鍔ㄤ綔={s_action:.0%}, 鍐椾綑={s_redundant:.0%}
- **瑙嗚閿氱偣**: {anchors} (杩欎簺鏃堕棿鐐瑰彂鐢熶簡鏄捐憲鐨勮瑙夌姸鎬佸垏鎹?
- **鍐茬獊鍘熷洜**: {reason}

## 决策选项

### 1. Split (强制拆分)
- 鍦烘櫙: 鏂囧瓧鏄庢樉瀵瑰簲浜嗕笉鍚岀殑瑙嗚鐘舵€?濡傚墠鍗婃槸姒傚康璁茶В锛屽悗鍗婃槸鎿嶄綔婕旂ず)
- 瑙嗚閿氱偣纭疄鏄煡璇嗙偣鐨勮嚜鐒跺垎鐣岀嚎
- 杩斿洖鎷嗗垎鐐规椂闂存埑(蹇呴』鎺ヨ繎鏌愪釜瑙嗚閿氱偣)

### 2. Adjust (边界微调)
- 鍦烘櫙: 鍗曞厓鏍稿績璇箟瀹屾暣锛屼絾棣栧熬鍖呭惈浜嗘棤鍏崇殑杞満/鍐椾綑鐢婚潰
- 鏀剁缉鎴栨墿灞?start_sec/end_sec 浠ラ伩寮€鍐椾綑

### 3. Keep (保持原判)
- 鍦烘櫙: 瑙嗚鍙樺寲浠呮槸PPT鍔ㄧ敾鎴栨棤鍏冲共鎵帮紝鏂囨湰璇箟鏄笉鍙垎鍓茬殑鏁翠綋
- 缁存寔鍘熸椂搴忥紝鏍囪涓?璺ㄦā鎬佸瓨鐤?

## 输出格式 (JSON)
```json
{{
    "decision": "split" | "adjust" | "keep",
    "rationale": "鍐崇瓥鐞嗙敱(20瀛椾互鍐?",
    "split_point": 12.5,
    "new_timeline": [10.0, 25.0]
}}
```

娉ㄦ剰锛?
- split鏃跺繀椤绘彁渚泂plit_point(绉?
- adjust鏃跺繀椤绘彁渚沶ew_timeline([start, end])
- keep鏃朵袱鑰呴兘涓嶉渶瑕?

璇疯緭鍑篔SON鍐崇瓥:"""




# =============================================================================
# Main Segmenter Class
# =============================================================================

class SemanticUnitSegmenter:
    """类说明：SemanticUnitSegmenter 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, llm_client=None):
        """
        鎵ц閫昏緫锛?
        1) 瑙ｆ瀽閰嶇疆鎴栦緷璧栵紝鍑嗗杩愯鐜銆?
        2) 鍒濆鍖栧璞＄姸鎬併€佺紦瀛樹笌渚濊禆瀹㈡埛绔€?
        瀹炵幇鏂瑰紡锛氶€氳繃鍐呴儴鏂规硶璋冪敤/鐘舵€佹洿鏂板疄鐜般€?
        鏍稿績浠峰€硷細鍦ㄥ垵濮嬪寲闃舵鍥哄寲渚濊禆锛屼繚璇佽繍琛岀ǔ瀹氭€с€?
        杈撳叆鍙傛暟锛?
        - llm_client: 瀹㈡埛绔疄渚嬶紙绫诲瀷锛氭湭鏍囨敞锛夈€?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?"""
        self.llm_client = llm_client
        self._ensure_llm_client()
        self._segment_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM,
            fallback=SYSTEM_PROMPT,
        )
        self._segment_user_template = get_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_USER,
            fallback=USER_PROMPT_TEMPLATE,
        )
        self._resegment_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_SYSTEM,
            fallback=RESEGMENT_SYSTEM_PROMPT,
        )
        self._resegment_user_template = get_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_USER,
            fallback=RESEGMENT_USER_PROMPT,
        )
    
    def _ensure_llm_client(self):
        """方法说明：SemanticUnitSegmenter._ensure_llm_client 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if self.llm_client is None:
            self.llm_client = llm_gateway.get_deepseek_client()
            logger.info("SemanticUnitSegmenter: LLM client initialized via gateway")
    
    async def segment(
        self,
        paragraphs: List[Dict[str, Any]],
        sentence_timestamps: Dict[str, Dict[str, float]] = None,
        batch_size: int = 10,
        cache_path: str = None
    ) -> SegmentationResult:
        """方法说明：SemanticUnitSegmenter.segment 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        import time
        start_time = time.time()

        if cache_path and os.path.exists(cache_path):
            try:
                cached_result = self._load_from_cache(cache_path)
                logger.info(f"Loaded cached segmentation result from {cache_path}")
                return cached_result
            except Exception as e:
                logger.warning(f"Failed to load cache from {cache_path}: {e}, re-computing...")

        if not paragraphs:
            return SegmentationResult(
                semantic_units=[],
                total_paragraphs_input=0,
                total_units_output=0,
                llm_token_usage=0,
                processing_time_ms=0.0,
            )

        paragraphs_for_llm = [
            {
                "paragraph_id": p.get("paragraph_id", f"P{idx + 1:03d}"),
                "text": p.get("text", ""),
                "source_sentence_ids": p.get("source_sentence_ids", []),
            }
            for idx, p in enumerate(paragraphs)
        ]

        prompt = render_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_USER,
            context={
                "paragraphs_json": json.dumps(paragraphs_for_llm, ensure_ascii=False, indent=2)
            },
            fallback=self._segment_user_template,
        )
        logger.info(f"Sending single LLM request for {len(paragraphs)} paragraphs")

        all_units: List[SemanticUnit] = []
        total_tokens = 0
        unit_counter = 1

        try:
            try:
                result_json, metadata, _ = await llm_gateway.deepseek_complete_json(
                    prompt=prompt,
                    system_message=self._segment_system_prompt,
                    max_tokens=SEGMENTATION_MAX_OUTPUT_TOKENS,
                    client=self.llm_client,
                )
            except TypeError:
                result_json, metadata, _ = await llm_gateway.deepseek_complete_json(
                    prompt=prompt,
                    system_message=self._segment_system_prompt,
                    client=self.llm_client,
                )

            total_tokens = getattr(metadata, "total_tokens", 0)
            units_data = result_json.get("semantic_units", []) if isinstance(result_json, dict) else []
            logger.debug(f"LLM returned {len(units_data)} semantic units")

            for raw_unit in units_data:
                parsed = self._parse_min_schema_unit(raw_unit, paragraphs)
                if parsed is None:
                    raise ValueError("Invalid unit schema under strict mode")

                paragraph_ids = parsed["pids"]
                knowledge_type = self._decode_knowledge_type(parsed["k"])
                mult_steps = parsed["m"] == 1
                full_text = self._collect_text_by_paragraph_ids(paragraph_ids, paragraphs)
                source_sentence_ids = self._collect_sentence_ids(paragraph_ids, paragraphs)
                start_sec, end_sec = self._calculate_timestamps(
                    paragraph_ids,
                    paragraphs,
                    sentence_timestamps,
                )

                unit = SemanticUnit(
                    unit_id=f"SU{unit_counter:03d}",
                    knowledge_type=knowledge_type,
                    knowledge_topic=self._build_topic_from_text(full_text),
                    full_text=full_text,
                    source_paragraph_ids=paragraph_ids,
                    source_sentence_ids=source_sentence_ids,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    confidence=DEFAULT_UNIT_CONFIDENCE,
                    mult_steps=mult_steps,
                )
                all_units.append(unit)
                unit_counter += 1

        except Exception as e:
            logger.error(f"LLM call failed or strict parse failed: {e}")
            all_units = []
            total_tokens = 0
            unit_counter = 1

            for p in paragraphs:
                paragraph_id = p.get("paragraph_id", f"P{unit_counter:03d}")
                text = p.get("text", "")
                source_sentence_ids = p.get("source_sentence_ids", [])
                start_sec, end_sec = self._calculate_timestamps(
                    [paragraph_id],
                    paragraphs,
                    sentence_timestamps,
                )

                unit = SemanticUnit(
                    unit_id=f"SU{unit_counter:03d}",
                    knowledge_type="abstract",
                    knowledge_topic=self._build_topic_from_text(text),
                    full_text=text,
                    source_paragraph_ids=[paragraph_id],
                    source_sentence_ids=source_sentence_ids,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    confidence=0.5,
                    mult_steps=False,
                )
                all_units.append(unit)
                unit_counter += 1

        elapsed_ms = (time.time() - start_time) * 1000

        result = SegmentationResult(
            semantic_units=all_units,
            total_paragraphs_input=len(paragraphs),
            total_units_output=len(all_units),
            llm_token_usage=total_tokens,
            processing_time_ms=elapsed_ms,
        )

        logger.info(
            f"Segmentation complete: {result.total_paragraphs_input} paragraphs 鈫?"
            f"{result.total_units_output} units, {total_tokens} tokens, {elapsed_ms:.0f}ms"
        )

        if cache_path:
            try:
                self._save_to_cache(result, cache_path)
                logger.info(f"Saved segmentation result to cache: {cache_path}")
            except Exception as e:
                logger.warning(f"Failed to save cache to {cache_path}: {e}")

        return result
    
    def _save_to_cache(self, result: SegmentationResult, path: str):
        """方法说明：SemanticUnitSegmenter._save_to_cache 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        data = {
            "semantic_units": [asdict(u) for u in result.semantic_units],
            "total_paragraphs_input": result.total_paragraphs_input,
            "total_units_output": result.total_units_output,
            "llm_token_usage": result.llm_token_usage,
            "processing_time_ms": result.processing_time_ms
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_from_cache(self, path: str) -> SegmentationResult:
        """方法说明：SemanticUnitSegmenter._load_from_cache 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        units = []
        for u_data in data.get("semantic_units", []):
            # 重建 SemanticUnit 对象
            # 处理 dataclass 字段差异 (向后兼容)
            valid_keys = SemanticUnit.__dataclass_fields__.keys()
            filtered_data = {k: v for k, v in u_data.items() if k in valid_keys}
            units.append(SemanticUnit(**filtered_data))
            
        return SegmentationResult(
            semantic_units=units,
            total_paragraphs_input=data.get("total_paragraphs_input", 0),
            total_units_output=data.get("total_units_output", 0),
            llm_token_usage=data.get("llm_token_usage", 0),
            processing_time_ms=data.get("processing_time_ms", 0.0)
        )
    
    def _calculate_timestamps(
        self,
        paragraph_ids: List[str],
        paragraphs: List[Dict],
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> Tuple[float, float]:
        """方法说明：SemanticUnitSegmenter._calculate_timestamps 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not sentence_timestamps:
            return 0.0, 0.0
        
        # 鏀堕泦鎵€鏈夌浉鍏冲彞瀛怚D
        sentence_ids = self._collect_sentence_ids(paragraph_ids, paragraphs)
        
        if not sentence_ids:
            return 0.0, 0.0
        
        min_start = float('inf')
        max_end = 0.0
        
        for sid in sentence_ids:
            if sid in sentence_timestamps:
                ts = sentence_timestamps[sid]
                min_start = min(min_start, ts.get("start_sec", float('inf')))
                max_end = max(max_end, ts.get("end_sec", 0.0))
        
        if min_start == float('inf'):
            min_start = 0.0
        
        return min_start, max_end
    
    def _collect_sentence_ids(
        self, 
        paragraph_ids: List[str], 
        paragraphs: List[Dict]
    ) -> List[str]:
        """方法说明：SemanticUnitSegmenter._collect_sentence_ids 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        sentence_ids = []
        pid_set = set(paragraph_ids)
        
        for p in paragraphs:
            if p.get("paragraph_id") in pid_set:
                sentence_ids.extend(p.get("source_sentence_ids", []))
        
        return sentence_ids

    def _parse_min_schema_unit(
        self,
        raw_unit: Any,
        paragraphs: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """方法说明：SemanticUnitSegmenter._parse_min_schema_unit 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not isinstance(raw_unit, dict):
            return None

        allowed_keys = {"pids", "k", "m"}
        if set(raw_unit.keys()) != allowed_keys:
            return None

        normalized_pids = self._normalize_paragraph_ids(raw_unit.get("pids"), paragraphs)
        if not normalized_pids:
            return None

        k_value = raw_unit.get("k")
        if not isinstance(k_value, int) or k_value not in KNOWLEDGE_TYPE_CODE_MAP:
            return None

        m_value = raw_unit.get("m")
        if not isinstance(m_value, int) or m_value not in (0, 1):
            return None

        return {
            "pids": normalized_pids,
            "k": k_value,
            "m": m_value,
        }

    def _normalize_paragraph_ids(
        self,
        paragraph_ids: Any,
        paragraphs: List[Dict[str, Any]],
    ) -> List[str]:
        """方法说明：SemanticUnitSegmenter._normalize_paragraph_ids 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not isinstance(paragraph_ids, list):
            return []

        paragraph_order = [p.get("paragraph_id") for p in paragraphs if p.get("paragraph_id")]
        paragraph_set = set(paragraph_order)
        requested_set = {
            pid for pid in paragraph_ids
            if isinstance(pid, str) and pid in paragraph_set
        }
        if not requested_set:
            return []

        return [pid for pid in paragraph_order if pid in requested_set]

    def _collect_text_by_paragraph_ids(
        self,
        paragraph_ids: List[str],
        paragraphs: List[Dict[str, Any]],
    ) -> str:
        """方法说明：SemanticUnitSegmenter._collect_text_by_paragraph_ids 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        id_set = set(paragraph_ids)
        texts: List[str] = []
        for paragraph in paragraphs:
            paragraph_id = paragraph.get("paragraph_id")
            if paragraph_id in id_set:
                text = paragraph.get("text", "")
                if isinstance(text, str):
                    texts.append(text)
        return "\n".join(texts)

    def _decode_knowledge_type(self, k_value: int) -> str:
        """方法说明：SemanticUnitSegmenter._decode_knowledge_type 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return KNOWLEDGE_TYPE_CODE_MAP[k_value]

    def _build_topic_from_text(self, text: str) -> str:
        """方法说明：SemanticUnitSegmenter._build_topic_from_text 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not isinstance(text, str):
            return "鏈煡涓婚"
        topic = text.strip().replace("\n", " ")
        if not topic:
            return "鏈煡涓婚"
        return topic[:20] + "..." if len(topic) > 20 else topic

    async def _resolve_conflicts(
        self,
        conflict_packages: List,
        units: List[SemanticUnit],
        cv_result_map: Dict,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """方法说明：SemanticUnitSegmenter._resolve_conflicts 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import ConflictPackage
        
        unit_map = {u.unit_id: u for u in units}
        updated_units = []
        processed_ids = set()
        
        for pkg in conflict_packages:
            if not isinstance(pkg, ConflictPackage):
                continue
            
            unit = unit_map.get(pkg.conflict_unit_id)
            if not unit:
                continue
            
            processed_ids.add(pkg.conflict_unit_id)
            cv_result = cv_result_map.get(pkg.conflict_unit_id)
            
            # 调用LLM获取决策
            try:
                decision = await self._call_llm_for_decision(unit, pkg, cv_result)
                
                # 执行决策
                result_units = self._execute_decision(
                    decision, unit, sentence_timestamps)
                updated_units.extend(result_units)
                
                logger.info(f"Conflict resolved: {unit.unit_id} -> {decision.get('decision', 'keep')}")
                
            except Exception as e:
                logger.warning(f"Conflict resolution failed for {unit.unit_id}: {e}, keeping original")
                unit.__dict__['cross_modal_suspected'] = True
                updated_units.append(unit)
        
        # 保留未处理的单元
        for unit in units:
            if unit.unit_id not in processed_ids:
                updated_units.append(unit)
        
        # 鎸夋椂闂存帓搴?
        updated_units.sort(key=lambda u: u.start_sec)
        
        return updated_units
    
    async def _call_llm_for_decision(
        self,
        unit: SemanticUnit,
        pkg,  # ConflictPackage
        cv_result  # CVValidationResult
    ) -> Dict[str, Any]:
        """方法说明：SemanticUnitSegmenter._call_llm_for_decision 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        # 构建prompt参数
        vision_stats = cv_result.vision_stats if cv_result else None
        
        prompt = render_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_USER,
            context={
                "unit_id": unit.unit_id,
                "text": unit.full_text[:200] + "..." if len(unit.full_text) > 200 else unit.full_text,
                "start_sec": unit.start_sec,
                "end_sec": unit.end_sec,
                "llm_type": unit.knowledge_type,
                "s_stable": vision_stats.s_stable if vision_stats else 0,
                "s_action": vision_stats.s_action if vision_stats else 0,
                "s_redundant": vision_stats.s_redundant if vision_stats else 0,
                "anchors": pkg.vision_anchors if pkg else [],
                "reason": pkg.conflict_reason if pkg else "unknown",
            },
            fallback=self._resegment_user_template,
        )

        try:
            result, metadata, _ = await llm_gateway.deepseek_complete_json(
                prompt=prompt,
                system_message=self._resegment_system_prompt,
                client=self.llm_client,
            )
            return result
        except Exception as e:
            logger.error(f"LLM decision call failed: {e}")
            return {"decision": "keep", "rationale": f"LLM璋冪敤澶辫触: {e}"}
    
    def _execute_decision(
        self,
        decision: Dict[str, Any],
        unit: SemanticUnit,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """方法说明：SemanticUnitSegmenter._execute_decision 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        action = decision.get("decision", "keep").lower()
        
        if action == "split":
            return self._execute_split(decision, unit, sentence_timestamps)
        elif action == "adjust":
            return self._execute_adjust(decision, unit)
        else:  # keep
            unit.__dict__['cross_modal_suspected'] = True
            unit.__dict__['cv_abnormal_reason'] = decision.get("rationale", "淇濇寔鍘熷垽")
            return [unit]
    
    def _execute_split(
        self,
        decision: Dict[str, Any],
        unit: SemanticUnit,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """方法说明：SemanticUnitSegmenter._execute_split 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        split_point = decision.get("split_point")
        if not split_point or not sentence_timestamps:
            # 无法拆分,标记存疑
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        # 根据split_point分割sentence_ids
        before_ids = []
        after_ids = []
        
        for sid in unit.source_sentence_ids:
            ts = sentence_timestamps.get(sid, {})
            end_sec = ts.get("end_sec", 0)
            if end_sec <= split_point:
                before_ids.append(sid)
            else:
                after_ids.append(sid)
        
        # 濡傛灉鍒嗗壊鏃犳晥(涓€杈逛负绌?
        if not before_ids or not after_ids:
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        # 鍒涘缓涓や釜瀛愬崟鍏?
        unit1 = SemanticUnit(
            unit_id=f"{unit.unit_id}_1",
            knowledge_type=unit.knowledge_type,
            knowledge_topic=unit.knowledge_topic,
            full_text=self._collect_text_by_ids(before_ids, sentence_timestamps),
            source_paragraph_ids=unit.source_paragraph_ids,
            source_sentence_ids=before_ids,
            start_sec=unit.start_sec,
            end_sec=split_point,
            display_form=unit.display_form,
            confidence=unit.confidence * 0.9
        )
        
        unit2 = SemanticUnit(
            unit_id=f"{unit.unit_id}_2",
            knowledge_type=unit.knowledge_type,
            knowledge_topic=unit.knowledge_topic,
            full_text=self._collect_text_by_ids(after_ids, sentence_timestamps),
            source_paragraph_ids=unit.source_paragraph_ids,
            source_sentence_ids=after_ids,
            start_sec=split_point,
            end_sec=unit.end_sec,
            display_form=unit.display_form,
            confidence=unit.confidence * 0.9
        )
        
        logger.info(f"Split {unit.unit_id} at {split_point}s -> {unit1.unit_id}, {unit2.unit_id}")
        return [unit1, unit2]
    
    def _execute_adjust(
        self,
        decision: Dict[str, Any],
        unit: SemanticUnit
    ) -> List[SemanticUnit]:
        """方法说明：SemanticUnitSegmenter._execute_adjust 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        new_timeline = decision.get("new_timeline", [])
        if len(new_timeline) != 2:
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        new_start, new_end = new_timeline
        
        # 楠岃瘉鍚堢悊鎬?
        if new_start >= new_end:
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        # 更新时序
        old_start, old_end = unit.start_sec, unit.end_sec
        unit.start_sec = max(0, new_start)
        unit.end_sec = new_end
        
        logger.info(f"Adjusted {unit.unit_id}: [{old_start:.1f}, {old_end:.1f}] -> [{unit.start_sec:.1f}, {unit.end_sec:.1f}]")
        return [unit]
    
    def _collect_text_by_ids(
        self,
        sentence_ids: List[str],
        sentence_timestamps: Dict[str, Dict[str, float]]
    ) -> str:
        """方法说明：SemanticUnitSegmenter._collect_text_by_ids 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        # 绠€鍖栧疄鐜? 杩斿洖ID鍒楄〃鍗犱綅
        # 瀹屾暣瀹炵幇闇€瑕佷粠step2_correction_output鑾峰彇text
        return f"[Sentences: {', '.join(sentence_ids)}]"

    


