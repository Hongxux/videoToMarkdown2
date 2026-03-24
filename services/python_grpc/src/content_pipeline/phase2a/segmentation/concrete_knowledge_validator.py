"""Concrete knowledge validator used by content_pipeline segmentation stage."""

import os
import cv2
import json
import base64
import logging
import httpx
import numpy as np
import time
import re
import uuid
import threading
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any
from dataclasses import dataclass

from services.python_grpc.src.common.utils.hash_policy import fast_digest_text, fast_hasher
from services.python_grpc.src.config_paths import load_yaml_dict, resolve_video_config_path
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys

logger = logging.getLogger(__name__)

_PPSTRUCTURE_IR_PATCH_LOCK = threading.Lock()
_PPSTRUCTURE_IR_PATCH_APPLIED = False

VISION_DESCRIPTION_ONLY_SYSTEM_PROMPT = """You are an educational image description assistant. Return JSON only. Single image: {\"img_description\":\"...\"}. Multi image: [{\"img_description\":\"...\"}]. Describe only visible facts."""

CONCRETE_KNOWLEDGE_PROMPT = """You are an educational image analysis assistant. Return strict JSON with fields: has_concrete_knowledge, confidence, img_description."""


@dataclass
class ConcreteKnowledgeResult:
    has_concrete: bool
    has_formula: bool
    confidence: float
    concrete_type: str
    reason: str
    is_mixed: bool
    non_text_ratio: float
    should_include: bool
    img_description: str = ""


class ConcreteKnowledgeValidator:
    """Docstring omitted."""
    def __init__(self, config_path: Optional[str] = None, output_dir: Optional[str] = None):
        """Docstring omitted."""
        self._concrete_knowledge_system_prompt = get_prompt(
            PromptKeys.VISION_AI_CONCRETE_KNOWLEDGE_SYSTEM,
            fallback=CONCRETE_KNOWLEDGE_PROMPT,
        )
        self._concrete_knowledge_legacy_user_prompt = get_prompt(
            PromptKeys.VISION_AI_CONCRETE_KNOWLEDGE_USER,
            fallback=self._concrete_knowledge_system_prompt,
        )
        if not str(self._concrete_knowledge_system_prompt or "").strip():
            self._concrete_knowledge_system_prompt = self._concrete_knowledge_legacy_user_prompt
        self._concrete_knowledge_system_prompt = VISION_DESCRIPTION_ONLY_SYSTEM_PROMPT

        from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import VisionAIClient, VisionAIConfig, PerceptualHasher, HashCacheManager
        
        self._vision_enabled = False
        self._vision_client: Optional[VisionAIClient] = None
        self._cache_signature = ""
        self._persistent_cache_path: Optional[Path] = None
        
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鍨鹃幇浣圭稁缂傚倷鐒﹁摫闁告瑥绻橀弻鐔碱敍閿濆洣姹楅悷婊呭鐢帡鎮欐繝鍐︿簻闁瑰搫绉烽崗宀勬煕濡濮嶉柟顔筋殜閻涱噣宕归鐓庮潛婵犵數鍋涢惇浼村礉閹存繍鍤曢柟闂寸绾惧吋绻涢幋锝夊摵妞ゆ梹妫冨铏圭磼濡搫顫戦梺绯曟櫆閻楁粌鈽夐崹顐犲亝闁告劏鏅濋崢閬嶆⒑闂堟侗鐒鹃柛搴ゆ珪缁傛帒鈽夐姀锛勫幍婵炴挻鑹鹃悘婵囦繆閸ф鐓冪憸婊堝礈閵娧呯闁糕剝顭囬々鍙夌節婵犲倻澧遍柡?VisionAIClient
        vision_config = self._load_vision_config(config_path)
        if vision_config:
            self._vision_client = VisionAIClient(vision_config)
            self._vision_enabled = vision_config.enabled
            self._cache_signature = self._build_cache_signature(vision_config)
        
        # 闂傚倸鍊搁崐鐑芥嚄閸撲礁鍨濇い鏍仦閸嬧剝銇勯弽顐粶缂佲偓閸喓绠鹃柟瀛樼懃閻忣亪鏌涚€ｎ剙鏋戠紒缁樼洴楠炲鈻庤箛鏇氱棯闂備胶顭堟鍝ョ矓瑜版帒钃熸繛鎴欏灩閸楁娊鏌曟繛鍨姎妞ゎ偀鏅滅换婵堝枈濡搫鈷夋繝鐢靛仜閿曨亜顕ｆ繝姘櫜濠㈣埖蓱閺咃絽鈹戦悩璇у伐闁哥噥鍋呴幈銊︽償閵婏腹鎷虹紒缁㈠幖閹冲酣藟瀹ュ悿鐟邦煥鎼粹€斥拫閻庢鍣崑濠囥€佸▎鎾村殐闁冲搫鍊归悾鐑芥⒒娓氣偓閳ь剛鍋涢懟顖涙櫠閻楀牅绻嗛柛娆忣槸婵秹鏌℃担绋库偓鍧楀蓟閵娾晩鏁嶆慨姗嗗亜椤?(闂傚倸鍊搁崐鐑芥倿閿曗偓椤啴宕归鍛姺闂佺鍕垫當缂佲偓婢舵劖鍊甸柨婵嗛婢ф彃鈹戦鎸庣彧闁靛洤瀚伴獮鎺楀箣濠垫劒鐥俊?Vision API 闂傚倸鍊搁崐椋庢濮橆剦鐒界紓浣姑肩换鍡涙煕閵夘喖澧痪鎯ф健閺屾洟宕煎┑鍥ф珴闂?
        similarity_threshold = vision_config.similarity_threshold if vision_config else 0.99
        self._hash_cache = HashCacheManager(similarity_threshold=similarity_threshold)
        self._hasher = PerceptualHasher

        # 闂傚倸鍊搁崐椋庣矆娴ｈ櫣绀婂┑鐘叉搐閻ら箖鏌熺紒妯轰刊婵炴挸顭烽弻鐔兼倻濡儵鎷绘繛鎴炴尭缁夌數鎹㈠☉銏犲耿婵☆垵顕х喊宥囩磽娴ｆ彃浜鹃悗鍏夊亾闁告洦鍓涢崢鎼佹倵楠炲灝鍔氬Δ鐘叉啞缁傚秹鎮欏顔惧數闁荤姴鎼幖顐︻敂椤撱垺鐓欐い鏃傜摂濞堟棃鏌嶉挊澶樻Ц闁宠楠搁埢搴ょ疀閹惧磭绋荤紓鍌氬€搁崐椋庣矆娴ｅ浜瑰鑸靛姇绾偓闂佺懓澧庨弲顐㈢暤?url_hash + 闂傚倸鍊搁崐鐑芥嚄閸洍鈧箓宕奸姀鈥冲簥闂佺懓顕崑鐔虹不閹€鏀介柣妯哄级閹兼劙鏌＄€ｂ晝鍔嶉柕鍥у楠炴﹢宕￠悙鍏哥棯闂備胶绮幐鎾磻閹剧粯鐓熼幖杈剧磿閻ｎ參鏌涙惔鈥宠埞閻撱倝鏌曟繛鐐珔闁?婵犵數濮烽弫鍛婃叏娴兼潙鍨傞柣鎾崇岸閺嬫牗绻涢幋鐐寸殤闁活厽鎹囬弻娑㈩敃閿濆棛顦ラ梺?VisionAI 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸婂潡鏌ㄩ弴鐐测偓鍝ョ不閺夊簱鏀介柣妯虹－椤ｆ煡鏌?
        if output_dir:
            intermediates_dir = Path(output_dir) / "intermediates"
            intermediates_dir.mkdir(parents=True, exist_ok=True)
            suffix = self._cache_signature[:12] if self._cache_signature else "default"
            self._persistent_cache_path = intermediates_dir / f"vision_ai_cache_{suffix}.json"
            self._load_persistent_cache()
        
        # V2: 闂傚倸鍊搁崐鎼佸磹閹间礁纾圭紒瀣嚦濞戞鏃堝焵椤掑啰浜辨繝鐢靛仜濡瑩骞愰幖浣稿瀭?ThreadSafeMathOCR 闂傚倸鍊风粈渚€骞栭位鍥敃閿曗偓閻ょ偓绻濇繝鍌涘櫧闁活厽鐟╅弻鈥愁吋鎼粹€崇闂侀€炲苯鍘哥紒鑸靛哺閻涱喚鈧綆鍠楅崑鎰版煙缂佹ê绗уù婊€鍗冲缁樻媴缁涘娈愰梺鎼炲妼闁帮絽鐣峰鈧崺锟犲礃閵婎煈鍞归梻鍌氬€搁崐鐑芥嚄閸洍鈧箓宕煎婵堟嚀椤繄鎹勯搹璇℃Ф婵犵數鍋涘Λ娆撳垂閸洩缍栭柡鍥ュ灪閻撳啴鏌曟径娑橆洭缂佹劗鍋炵换娑㈠箣濞嗗繒浠奸梺缁樺笒閻忔岸濡甸崟顖氱闁糕剝銇炴竟鏇㈡⒒?(闂傚倸鍊搁崐鐑芥倿閿曗偓椤啴宕归鍛姺闂佺鍕垫當缂佲偓婢跺备鍋撻獮鍨姎妞わ富鍨跺浼村Ψ閿斿墽顔曢梺鐟邦嚟閸庢垶绗熷☉銏＄厱闁靛牆鎳愭晶鏇㈡懚閺嶎厽鐓ラ柡鍐ㄥ€婚幗鍌炴煕鎼存稑鍔﹂柡?
        self._math_ocr = None
        self._math_ocr_init_attempted = False
        self._math_ocr_init_error: Optional[str] = None
        self._math_ocr_init_lock = threading.Lock()
        math_ocr_cfg = self._load_math_ocr_config(config_path=config_path)
        self._math_ocr_enabled = bool(math_ocr_cfg.get("enabled", False))
        raw_math_ocr_min_score = math_ocr_cfg.get("min_score", 0.7)
        try:
            parsed_math_ocr_min_score = float(raw_math_ocr_min_score)
        except Exception:
            parsed_math_ocr_min_score = 0.7
        self._math_ocr_min_score = max(0.0, min(1.0, parsed_math_ocr_min_score))
        self._ocr_extractor = None
        self._ocr_extractor_init_error: Optional[str] = None

        # PP-StructureV3 婵犵數濮烽弫鎼佸磻濞戙垺鍋ら柕濞у啫鐏婇悗鍏夊亾闁告洦鍓欏▓锝咁渻閵堝棛澧遍柛瀣仱瀹曟洟骞嬮敂鐣屽幗濠德板€愰崑鎾绘煟濡ゅ啫浠ф俊鍙夊姍閺佹劖寰勭€ｎ剙骞堥梻浣告惈濞层垽宕濈仦鍓ь洸闁兼亽鍎扮换鍡涙煛鐏炶鍔欓柟鏌ョ畺閺屸€崇暆鐎ｎ剛袦闂佽鍠楅悷鈺佺暦閿濆棗绶炴俊顖欑串缁辨姊婚崒娆戭槮闁圭寽銈呯稑闂備胶顭堥敃銉┿€冮崼婢綁骞囬弶璺ㄧ潉闂佸壊鍋掗崑鍛村疾椤忓牊鈷戠紒顖涙礀婢ф煡鏌ｉ悢婵嗘硽閸ヮ剙鐓涢柛娑卞枓閹峰搫鈹戦悙璺虹毢缂侇噮鍨辩换娑㈠炊椤掍胶鍘遍梺鍝勫€归娆撳磿閹达附鐓犳繛宸簷閹茬偓銇勯姀鈽嗘疁鐎规洜鍠栭、鏇㈠Χ鎼粹剝鍊涙繝纰夌磿閸嬫垿宕愰弴鐘冲床闁归偊鍠掗崑鎾愁潩閻撳骸绠荤紓渚囧枛椤戝鐣锋總绋课ㄩ柨鏃€鍎抽獮宥夋煟鎼达絾鍤€閻庢凹鍠楅弲璺何旈崥钘夘槸椤劑宕奸悢鍝勫箞闂備線娼ч…鍫ュ磿閻㈢鐭楅柛鏇ㄥ灡閻?
        structure_cfg = self._load_structure_preprocess_config(
            config_path=config_path,
            default_similarity_threshold=similarity_threshold,
        )
        self._structure_preprocess_enabled = bool(structure_cfg.get("enabled", False))
        self._structure_disable_after_backend_error = bool(
            structure_cfg.get("disable_ppstructure_after_backend_error", True)
        )
        self._structure_dedup_similarity_threshold = float(
            structure_cfg.get("dedup_similarity_threshold", similarity_threshold)
        )
        self._structure_bbox_overlap_merge_threshold = max(
            0.0,
            min(1.0, float(structure_cfg.get("bbox_overlap_merge_threshold", 0.9) or 0.9)),
        )
        self._structure_skip_split_bbox_coverage_threshold = max(
            0.0,
            min(
                1.0,
                float(
                    structure_cfg.get("skip_split_bbox_coverage_threshold", 0.0) or 0.0
                ),
            ),
        )
        self._structure_crop_margin_px = max(0, int(structure_cfg.get("crop_margin_px", 4) or 4))
        self._structure_context_nearby_px = max(
            0, int(structure_cfg.get("context_nearby_px", 18) or 0)
        )
        self._structure_target_types = {
            str(item or "").strip()
            for item in structure_cfg.get(
                "target_types",
                [
                    "algorithm",
                    "formula",
                    "image",
                    "figure",
                    "figure caption",
                    "figure title",
                    "table",
                    "table caption",
                ],
            )
            if str(item or "").strip()
        }
        self._structure_context_types = {
            str(item or "").strip()
            for item in structure_cfg.get(
                "context_types",
                ["text", "code"],
            )
            if str(item or "").strip()
        }
        self._structure_force_disable_ir_optim = bool(
            structure_cfg.get("force_disable_ir_optim", True)
        )
        self._structure_backend_compat_retry_enabled = bool(
            structure_cfg.get("backend_compat_retry_enabled", True)
        )
        self._structure_backend_compat_retry_attempted = False
        self._structure_engine = None
        self._structure_engine_init_error: Optional[str] = None
        self._structure_paddlex_layout_model = None
        self._structure_paddlex_model_init_error: Optional[str] = None
    
    def _load_vision_config(self, config_path: Optional[str] = None):
        """Load VisionAIConfig from YAML/environment."""
        from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import VisionAIConfig
        
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍡椾粡濡炪倖鍔х粻鎴犲閸ф鐓曟繛鍡楁禋濡叉椽鏌ｅ┑濠傜厫濞ｅ洤锕、娑樷攽閸℃洘鐫忕紓浣哄亾閸庢娊濡堕幖浣歌摕闁挎繂顦Λ姗€鏌涢…鎴濇珮闁哄棎鍨归—鍐Χ閸涱垳顔掓繛瀛樼矎濞夋盯锝炶箛鎾佹椽顢旈崟顓㈢崜闂佽崵濮垫禍浠嬪礈闁秴围濠㈣泛顑囬崢浠嬫⒑闂堟稓澧曢柟鍐茬箻钘熼柣妯肩帛閻撴瑩鏌ｉ悢鍛婄凡妞ゅ浚鍘介妵鍕閿涘嫧妲堝銈庡亝缁诲啫顭囪箛娑樼厸闁告劑鍔庨妶?
        if config_path is None:
            config_path = resolve_video_config_path(anchor_file=__file__)
        else:
            config_path = resolve_video_config_path(str(config_path), anchor_file=__file__)
        
        if not config_path or not config_path.exists():
             logger.warning(f"config.yaml not found at {config_path} (checked unified config path), using CV-only mode")
             return None
        
        try:
            config = load_yaml_dict(config_path)
            
            vision_config = config.get("vision_ai", {})
            
            if not vision_config.get("enabled", False):
                logger.info("Vision AI disabled in config, using CV-only mode")
                return None
            
            base_url = str(
                vision_config.get("base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions") or ""
            ).strip()
            normalized_base_url = base_url.lower()
            is_qianfan = ("qianfan.baidubce.com" in normalized_base_url) or ("aistudio.baidu.com" in normalized_base_url)
            default_model = "ernie-4.5-turbo-vl-32k" if is_qianfan else "qwen-vl-max-2025-08-13"

            api_key = str(vision_config.get("api_key", "") or "").strip()
            bearer_token = str(vision_config.get("bearer_token", "") or "").strip()
            auth_token = api_key or bearer_token

            api_key_env = str(vision_config.get("api_key_env", "") or "").strip()
            bearer_token_env = str(vision_config.get("bearer_token_env", "") or "").strip()
            if not api_key_env:
                api_key_env = "VISION_AI_BEARER_TOKEN" if is_qianfan else "DASHSCOPE_API_KEY"
            if not bearer_token_env:
                bearer_token_env = "VISION_AI_BEARER_TOKEN"

            if not auth_token:
                env_candidates = [api_key_env, bearer_token_env]
                if is_qianfan:
                    env_candidates.append("QIANFAN_BEARER_TOKEN")
                seen_env_names = set()
                for env_name in env_candidates:
                    normalized_env_name = str(env_name or "").strip()
                    if not normalized_env_name or normalized_env_name in seen_env_names:
                        continue
                    seen_env_names.add(normalized_env_name)
                    candidate = str(os.getenv(normalized_env_name, "") or "").strip()
                    if candidate:
                        auth_token = candidate
                        api_key_env = normalized_env_name
                        break
            if not auth_token:
                logger.warning("vision_ai auth token is empty, using CV-only mode")
                return None

            batch_cfg = vision_config.get("batch", {}) if isinstance(vision_config.get("batch"), dict) else {}
            person_cfg = (
                vision_config.get("person_subject_filter", {})
                if isinstance(vision_config.get("person_subject_filter"), dict)
                else {}
            )
            force_include_raw = person_cfg.get("force_include_patterns", []) or []
            if isinstance(force_include_raw, str):
                force_include_raw = [force_include_raw]
              
            # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敂缁樻櫈闂佸憡渚楅崹顏堝磻閹炬剚娼╅柣鎾抽椤偆绱?VisionAIConfig
            ai_config = VisionAIConfig(
                enabled=True,
                api_key=auth_token,
                bearer_token=auth_token,
                api_key_env=api_key_env,
                bearer_token_env=bearer_token_env,
                base_url=base_url,
                model=vision_config.get("model", vision_config.get("vision_model", default_model)),
                temperature=vision_config.get("temperature", 0.3),
                rate_limit_per_minute=vision_config.get("rate_limit_per_minute", 60),
                duplicate_detection_enabled=vision_config.get("duplicate_detection", True),
                similarity_threshold=vision_config.get("similarity_threshold", 0.99),
                batch_enabled=bool(batch_cfg.get("enabled", False)),
                batch_max_size=max(1, int(batch_cfg.get("max_size", 4) or 4)),
                batch_flush_ms=max(0, int(batch_cfg.get("flush_ms", 20) or 20)),
                batch_max_inflight_batches=max(1, int(batch_cfg.get("max_inflight_batches", 2) or 2)),
                person_subject_filter_enabled=bool(person_cfg.get("enabled", True)),
                person_mask_area_threshold=max(
                    0.0, min(1.0, float(person_cfg.get("area_threshold", 0.3) or 0.3))
                ),
                person_mask_binary_threshold=max(
                    0.0, min(1.0, float(person_cfg.get("mask_threshold", 0.5) or 0.5))
                ),
                person_mask_high_conf_threshold=max(
                    0.0, min(1.0, float(person_cfg.get("high_conf_threshold", 0.8) or 0.8))
                ),
                person_mask_high_conf_min_area=max(
                    0.0, min(1.0, float(person_cfg.get("high_conf_min_area", 0.08) or 0.08))
                ),
                person_prefilter_force_include_patterns=[
                    str(item).strip()
                    for item in force_include_raw
                    if str(item).strip()
                ],
                person_model_selection=0 if int(person_cfg.get("model_selection", 1) or 1) <= 0 else 1,
            )
            
            logger.info(
                f"Vision API enabled: model={ai_config.model}, base_url={ai_config.base_url}, "
                f"duplicate_detection={ai_config.duplicate_detection_enabled}"
            )
            return ai_config
            
        except Exception as e:
            logger.error(f"Failed to load vision config: {e}")
            return None

    def _load_structure_preprocess_config(
        self,
        config_path: Optional[str] = None,
        default_similarity_threshold: float = 0.99,
    ) -> Dict[str, Any]:
        """Docstring omitted."""
        try:
            normalized_default_threshold = float(default_similarity_threshold)
        except Exception:
            normalized_default_threshold = 0.95
        normalized_default_threshold = max(0.0, min(1.0, normalized_default_threshold))
        # 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸婂潡鏌ㄩ弴鐐测偓鍝ョ不閺夊簱鏀介柣妯虹－椤ｆ煡鏌涙繝鍌滀粵闁靛洤瀚板浠嬵敃椤厾鎹曠紓鍌欒閸嬫挾鈧厜鍋撻柛鏇ㄥ墰閸樻悂鎮楅獮鍨姎闁哥噥鍋呮穱濠囧礂缁楄桨绨婚柟鍏肩暘閸ㄨ櫣绮堢€ｎ喗鐓涢悘鐐插⒔濞叉潙鈹戦敍鍕幋闁轰礁鍟撮崺鈧い鎺戝缁€澶愭煏閸繃锛嶆繛鍫滅矙閺岋綁骞囬鍌涙喖閻庤娲栭惌鍌炲蓟閿涘嫪娌柛濠勫枎椤忣厼顪冮妶鍡樺碍闁告艾顑呴銉╁礋椤撴繃鍕冮梺浼欑到閻牓宕戣濮婄粯鎷呴挊澶婃優婵犳鍠楁繛濠傤潖閽樺鍚嬪璺猴功閿涙瑩姊洪崫鍕枆闁告ü绮欏畷鏇熺節閸愶缚绨婚梺鍝勬处椤ㄥ懏绂嶆ィ鍐╁€甸悷娆忓缁€鍐┿亜閵娿儻鏀诲ǎ鍥э躬楠炴牗鎷呴懖婵勫妿閹插憡鎯旈妸銉ь唵闂備緡鍓欑粔鐢稿煕閹达附鐓曟繛鎴炃氭慨鍥ㄣ亜韫囨挾澧愰柍褜鍓ㄧ粻鎾诲箖濠婂牊瀵犲鑸靛姈閿涙姊绘担鍝ョШ婵☆偉娉曠划鍫熺瑹閳ь剙鐣烽敓鐘茬闁兼亽鍎抽崢鍛婄節閵忥絾纭炬い鎴濇嚇閹偞銈ｉ崘鈺冨幈闂佺粯妫冮ˉ鎾存櫏闂備礁鎼径鍥礈濠靛棭鍤楅柛鏇ㄥ幐閸嬫捇鏁愭惔鈥茬盎闂佺粯绻嶆禍鐐垫閹惧瓨濯村ù鐘差儏閹界數绱撴担绛嬪殭闁哥喍鍗抽崺鈧い鎺戝€归弳鈺呮煕濡姴妫涙禍浠嬫⒒娴ｅ憡鍟炵紒璇插€婚埀顒佸嚬閸撴瑩顢氶敐鍛傛棃宕ㄩ瑙勫闂備礁鎲＄粙鎴︽晝閵婏妇鐝舵慨妞诲亾闁哄矉绲借灃闁逞屽墮铻炴繝闈涱儏缁犵娀鏌ｉ弬鍨倯闁稿瀚伴弻娑滅疀閺囩偛浠橀梺鍛婃惄閸犳牠鍩為幋锔绘晩闁伙絽鐬奸悾鐢告⒑濞茶澧查柛瀣姉閸掓帗绻濆鍏兼櫔闂侀€炲苯澧寸€殿喛顕ч埥澶愬閻樻牓鍔戦弻鏇熷緞閸繂濮夐梺鍦焾閸燁垳鎹㈠┑瀣厱闁逞屽墴瀹曠喖顢橀悜鍡樻缂傚倸鍊风粈渚€宕愰崷顓熸殰闁斥晛鍟╃换鍡涙煟閹达絾顥夐崬顖炴⒑闂堟侗妾ч悗闈涙湰缁傛帡顢橀姀鈾€鎷洪柣鐘叉穿鐏忔瑧绮婚幍顔剧＜缂備焦锚閻忓鈧娲栫紞濠囧箖閻ｅ瞼鐭欓悹鎭掑妽閻掗箖姊绘担鐑樺殌妞ゆ洦鍙冨畷鎴︽倷閺夋垵鐏侀柣搴㈢⊕椤洨绮绘ィ鍐╃厱闁斥晛鍘鹃鍡欑當鐟滃海鎹㈠☉銏犻唶婵犻潧鐗嗛悡鐔兼⒑閸濆嫮鐒跨紓宥勭窔閺佹劙鎮欐笟顖涙櫌闂侀€炲苯澧ǎ鍥э躬楠炴牗鎷呴崷顓炲笚?
        structure_default_dedup_threshold = min(0.88, normalized_default_threshold)

        defaults: Dict[str, Any] = {
            "enabled": False,
            "disable_ppstructure_after_backend_error": True,
            "force_disable_ir_optim": True,
            "backend_compat_retry_enabled": True,
            "dedup_similarity_threshold": float(structure_default_dedup_threshold),
            "bbox_overlap_merge_threshold": 0.9,
            "skip_split_bbox_coverage_threshold": 0.0,
            "crop_margin_px": 4,
            "context_nearby_px": 18,
            "target_types": [
                "algorithm",
                "formula",
                "image",
                "figure",
                "figure caption",
                "figure title",
                "table",
                "table caption",
            ],
            "context_types": [
                "text",
                "code",
            ],
        }
        try:
            if config_path is None:
                config_file = resolve_video_config_path(anchor_file=__file__)
            else:
                config_file = resolve_video_config_path(str(config_path), anchor_file=__file__)
            if not config_file or not config_file.exists():
                return defaults
            config = load_yaml_dict(config_file)
            vision_cfg = config.get("vision_ai", {}) if isinstance(config, dict) else {}
            structure_cfg = (
                vision_cfg.get("structure_preprocess", {})
                if isinstance(vision_cfg, dict)
                else {}
            )
            if not isinstance(structure_cfg, dict):
                return defaults

            merged = dict(defaults)
            merged["enabled"] = bool(structure_cfg.get("enabled", defaults["enabled"]))
            merged["disable_ppstructure_after_backend_error"] = bool(
                structure_cfg.get(
                    "disable_ppstructure_after_backend_error",
                    defaults["disable_ppstructure_after_backend_error"],
                )
            )
            merged["force_disable_ir_optim"] = bool(
                structure_cfg.get(
                    "force_disable_ir_optim",
                    defaults["force_disable_ir_optim"],
                )
            )
            merged["backend_compat_retry_enabled"] = bool(
                structure_cfg.get(
                    "backend_compat_retry_enabled",
                    defaults["backend_compat_retry_enabled"],
                )
            )
            merged["dedup_similarity_threshold"] = float(
                structure_cfg.get(
                    "dedup_similarity_threshold",
                    defaults["dedup_similarity_threshold"],
                )
            )
            merged["dedup_similarity_threshold"] = max(
                0.0,
                min(1.0, merged["dedup_similarity_threshold"]),
            )
            merged["bbox_overlap_merge_threshold"] = float(
                structure_cfg.get(
                    "bbox_overlap_merge_threshold",
                    defaults["bbox_overlap_merge_threshold"],
                )
            )
            merged["bbox_overlap_merge_threshold"] = max(
                0.0,
                min(1.0, merged["bbox_overlap_merge_threshold"]),
            )
            merged["skip_split_bbox_coverage_threshold"] = float(
                structure_cfg.get(
                    "skip_split_bbox_coverage_threshold",
                    defaults["skip_split_bbox_coverage_threshold"],
                )
            )
            merged["skip_split_bbox_coverage_threshold"] = max(
                0.0,
                min(1.0, merged["skip_split_bbox_coverage_threshold"]),
            )
            merged["crop_margin_px"] = max(
                0, int(structure_cfg.get("crop_margin_px", defaults["crop_margin_px"]) or 0)
            )
            merged["context_nearby_px"] = max(
                0, int(structure_cfg.get("context_nearby_px", defaults["context_nearby_px"]) or 0)
            )
            raw_types = structure_cfg.get("target_types", defaults["target_types"])
            if isinstance(raw_types, str):
                raw_types = [part.strip() for part in raw_types.split(",")]
            if isinstance(raw_types, list):
                normalized_types = [
                    str(item or "").strip()
                    for item in raw_types
                    if str(item or "").strip()
                ]
                if normalized_types:
                    merged["target_types"] = normalized_types
            raw_context_types = structure_cfg.get("context_types", defaults["context_types"])
            if isinstance(raw_context_types, str):
                raw_context_types = [part.strip() for part in raw_context_types.split(",")]
            if isinstance(raw_context_types, list):
                normalized_context_types = [
                    str(item or "").strip()
                    for item in raw_context_types
                    if str(item or "").strip()
                ]
                if normalized_context_types:
                    merged["context_types"] = normalized_context_types
            return merged
        except Exception as exc:
            logger.warning(f"Failed to load structure preprocess config, using defaults: {exc}")
            return defaults

    def _load_math_ocr_config(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load math OCR config with safe defaults."""
        defaults: Dict[str, Any] = {
            "enabled": False,
            "min_score": 0.7,
        }
        try:
            if config_path is None:
                config_file = resolve_video_config_path(anchor_file=__file__)
            else:
                config_file = resolve_video_config_path(str(config_path), anchor_file=__file__)
            if not config_file or not config_file.exists():
                return defaults
            config = load_yaml_dict(config_file)
            vision_cfg = config.get("vision_ai", {}) if isinstance(config, dict) else {}
            math_cfg = (
                vision_cfg.get("math_ocr", {})
                if isinstance(vision_cfg, dict)
                else {}
            )
            if not isinstance(math_cfg, dict):
                return defaults

            merged = dict(defaults)
            merged["enabled"] = bool(math_cfg.get("enabled", defaults["enabled"]))
            raw_min_score = math_cfg.get("min_score", defaults["min_score"])
            try:
                merged["min_score"] = float(raw_min_score)
            except Exception:
                merged["min_score"] = float(defaults["min_score"])
            merged["min_score"] = max(0.0, min(1.0, merged["min_score"]))
            return merged
        except Exception as exc:
            logger.warning(f"Failed to load math ocr config, using defaults: {exc}")
            return defaults

    @staticmethod
    def _normalize_structure_type(raw_type: Any) -> str:
        token = str(raw_type or "").strip().lower()
        if not token:
            return ""
        token = token.replace("-", " ").replace("_", " ")
        token = re.sub(r"\s+", " ", token).strip()
        alias_map = {
            "algorithm": "algorithm",
            "formula": "formula",
            "display formula": "formula",
            "inline formula": "formula",
            "formula number": "formula",
            "image": "image",
            "chart": "figure",
            "figure": "figure",
            "figure caption": "figure_caption",
            "figure title": "figure_title",
            "figure table chart title": "figure_title",
            "table": "table",
            "table caption": "table_caption",
            "table title": "table_caption",
            "text": "text",
            "paragraph": "text",
            "plain text": "text",
            "reference": "text",
            "code": "code",
            "code block": "code",
            "program code": "code",
        }
        return alias_map.get(token, token.replace(" ", "_"))

    @staticmethod
    def _is_structure_backend_runtime_error(exc: Exception) -> bool:
        message = str(exc or "").lower()
        if not message:
            return False
        keywords = (
            "onednncontext does not have the input filter",
            "operator < fused_conv2d > error",
            "fused_conv2d",
            "onednn",
            "mkldnn",
        )
        return any(token in message for token in keywords)

    @staticmethod
    def _summarize_exception(exc: Exception, max_lines: int = 3, max_chars: int = 320) -> str:
        message = str(exc or "").strip()
        if not message:
            return type(exc).__name__
        lines = [line.strip() for line in message.splitlines() if line.strip()]
        if not lines:
            return type(exc).__name__
        summary = " | ".join(lines[:max(1, int(max_lines))])
        if len(lines) > max_lines:
            summary += " | ..."
        if len(summary) > max_chars:
            summary = summary[: max_chars - 3] + "..."
        return summary

    def _attempt_structure_backend_compat_recovery(self, exc: Exception) -> bool:
        if not bool(getattr(self, "_structure_backend_compat_retry_enabled", True)):
            return False
        if self._structure_backend_compat_retry_attempted:
            return False
        if not self._is_structure_backend_runtime_error(exc):
            return False
        self._structure_backend_compat_retry_attempted = True

        # 一次性兼容重试：禁用 mkldnn，避免已知 oneDNN fused_conv2d 导出链路异常。
        os.environ["FLAGS_use_mkldnn"] = "0"
        self._structure_engine = None
        self._structure_engine_init_error = None
        recovered_engine = self._get_structure_engine()
        if recovered_engine is None:
            return False
        logger.warning(
            "PP-Structure backend compatibility retry enabled: FLAGS_use_mkldnn=0, engine reinitialized."
        )
        return True

    def _patch_ppstructure_ir_optim_if_needed(self) -> None:
        if not bool(getattr(self, "_structure_force_disable_ir_optim", True)):
            return
        if os.name != "nt":
            return
        global _PPSTRUCTURE_IR_PATCH_APPLIED
        if _PPSTRUCTURE_IR_PATCH_APPLIED:
            return
        with _PPSTRUCTURE_IR_PATCH_LOCK:
            if _PPSTRUCTURE_IR_PATCH_APPLIED:
                return
            try:
                from paddle import inference  # type: ignore
            except Exception as exc:
                logger.warning(
                    "PP-Structure IR compatibility patch skipped: paddle inference unavailable: %s",
                    exc,
                )
                return
            config_cls = getattr(inference, "Config", None)
            if config_cls is None or not hasattr(config_cls, "switch_ir_optim"):
                logger.warning(
                    "PP-Structure IR compatibility patch skipped: inference.Config.switch_ir_optim missing."
                )
                return
            original_switch_ir_optim = config_cls.switch_ir_optim

            # 做什么：强制关闭 IR 优化；为什么：规避 Windows + PaddleOCR2.x 下布局模型触发 fused_conv2d/oneDNN 崩溃。
            # 权衡：可能牺牲少量推理性能，但可换取 PP-Structure 主后端可用性，避免长期退化到 fallback。
            def _force_disable_ir_optim(self, _enabled):  # type: ignore[no-untyped-def]
                return original_switch_ir_optim(self, False)

            try:
                config_cls.switch_ir_optim = _force_disable_ir_optim
                _PPSTRUCTURE_IR_PATCH_APPLIED = True
                logger.warning(
                    "PP-Structure compatibility patch enabled: force inference.Config.switch_ir_optim(False) on Windows."
                )
            except Exception as exc:
                logger.warning(
                    "PP-Structure IR compatibility patch failed: %s",
                    exc,
                )

    def _get_paddlex_layout_model(self):
        if self._structure_paddlex_layout_model is not None:
            return self._structure_paddlex_layout_model
        if self._structure_paddlex_model_init_error:
            return None
        try:
            import sys
            import types
            os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
            # paddleocr 2.x 婵犵數濮烽弫鎼佸磻閻愬樊鐒芥繛鍡樻尭鐟欙箓鎮楅敐搴℃灍闁搞倕鐭傞弻鐔碱敍閸℃啸闁逞屽墯鐎笛囧Φ閸曨喚鐤€闁圭偓娼欏▍銈嗙箾鐎涙ê娈犻柛濠冪箞瀵鎮㈤崗鑲╁姺闂佹寧娲嶉崑鎾绘煟韫囨稐鎲鹃柡宀嬬節閸┾偓妞ゆ帊鑳堕々鐑芥倵閿濆骸浜為柛妯绘倐濮婃椽宕ㄦ繝鍌毿曢梺鍛婎焽閺咁偄鈽夐崹顐犲亝闁告劏鏅濋崢?sys.path 濠电姷鏁告慨鐑藉极閹间礁纾绘繛鎴旀嚍閸ヮ剦鏁囬柕蹇曞Х椤︻噣鎮楅崗澶婁壕闂佸憡娲﹂崑鍡涙偂閹达附鈷戠紒顖涙礀婢ф煡鏌ㄥ杈╃＜闁告挷绀佹禒锕傛煙娓氬灝濡兼い顐ｇ矒瀹曞崬顪冮幆褎鐎抽梻鍌欑劍閹爼宕愰弴銏╂晞濠㈣埖鍔曠粻鏍ㄤ繆閵堝倸浜鹃梺瀹犳椤︻垶鍩㈡惔銈囩杸闁瑰灝鍟紞鎴︽⒒閸屾瑧鍔嶆俊鐐叉健瀹曘垺绂掔€ｎ亞锛熼梺鐟板閻℃棃寮€ｎ喗鐓忓璺烘濞呭懐绱掗埀顒傗偓锝庡枟閻撴洟鏌熼幑鎰敿闁稿繐鏈妵鍕晜鐠囧弶鐝濆┑顔硷攻濡炶棄鐣烽妸锔剧瘈闁告洦鍓欏▍褔姊绘担瑙勫仩闁告柨閰ｅ顐ゆ嫚閼碱剚娈惧┑掳鍊愰崑鎾淬亜椤愶絿鐭掗柛鈹惧亾濡炪倖甯掔€氼參宕戝Ο姹囦簻闁哄洦顨呮禍楣冩倵鐟欏嫭绌跨紒鍙夊劤椤曘儵宕熼鍌滅槇闁瑰吋鐣崺鍕?paddlex 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁斥晛鍟伴幊鍛存煟椤撶儐妯€闁哄本鐩崺鍕礃椤忓懎娅楅梻浣告憸婵潙螞濠靛钃熼柕濞垮劗閺€浠嬫煕閳╁喛渚涙俊顐犲€濆娲焻閻愯尪瀚伴柛妯烘憸缁辨帡顢欓悾灞惧櫗婵?
            # 闂傚倸鍊风粈渚€骞栭位鍥敃閿曗偓閻ょ偓绻濇繝鍌滃闁稿绻濋弻宥夊传閸曨剙娅ｉ梻鍌氬亞閸ㄥ爼寮婚敐澶婄闁挎繂妫Λ鍕⒑閸︻厽鍤€闁绘牕銈稿濠氬即閻旇櫣顔曢梺鍓茬厛閸犳牗鎱ㄩ崼鏇熲拺闁硅偐鍋涢埀顒佸灴瀹曟劕鈹戠€ｎ亣鎽?paddlex 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼唶闂佸憡绺块崕鎶芥儗閹剧粯鐓ｉ煫鍥风到娴滄粎绱掗崜浣镐户闁逞屽墮缁犲秹宕曢柆宓ュ洭顢涘鍛殫閻庡箍鍎遍ˇ浼存偂閺囥垺鐓欓柟瑙勫姇閻ㄦ椽鏌熼绗哄仮闁哄苯绉瑰畷顐﹀礋椤掆偓濞呫倝姊洪幎鑺ユ暠婵☆偄鍟村濠氬即閵忕娀鍞跺┑鐘绘涧閻楀棝鍩呮潏鈺冪＝濞达絽鎼暩闂佺娅曢幑鍥э耿娴ｇ硶鏀介柣妯款嚋瀹搞儵鎮楀鐓庢珝闁糕晛锕鎾閿涘嫬骞愬┑鐐舵彧缁蹭粙骞栭锝囶浄閺夊牃鏂侀崑鎾舵喆閸曨剛顦ㄥ銈冨妼閿曘儲绌辨繝鍥ㄥ€婚柦妯猴級閳哄啯鍠愰煫鍥ㄧ☉閼稿綊鏌熺紒銏犳灍闁稿﹦鏁婚弻娑⑩€﹂幋婵呯凹缂備浇鍩栭悡锟犲箖濡も偓椤繈顢楁担鐟版瀾婵犵鈧啿绾ч柛鏃€娲熼崺鐐哄箣閿曗偓闁卞洦鎱ㄥ鍡楀幐濠㈣娲熷鍝勑ч崶褏浼堝┑鐐板尃閸涱喗娈伴梺鍓插亝濞叉﹢鎮￠弴鐔虹闁糕剝顨堢粻鍙夈亜閵夛絽鐏柍褜鍓濋～澶娒鸿箛娑樼？闁哄被鍎辩粻姘舵煛閸愩劎澧曢柣蹇斿▕閺屟嗙疀閿濆懍绨奸梺缁樼箖鐢€愁潖缂佹ɑ濯寸紒娑橆儏濞堟劙姊洪崨濞掝亞绱炴繝鍌滄殾缁绢厼鎳屾禍褰掓煙楠炲灝鈧洖煤椤撶儐鍤曞┑鐘宠壘鍞梺?
            original_sys_path = list(sys.path)
            filtered_sys_path = [
                p
                for p in original_sys_path
                if "site-packages\\paddleocr" not in str(p).lower().replace("/", "\\")
            ]
            sys.path[:] = filtered_sys_path
            injected_modelscope_stub = False
            if "modelscope" not in sys.modules:
                modelscope_stub = types.ModuleType("modelscope")

                def _snapshot_download(*args, **kwargs):
                    raise RuntimeError("modelscope snapshot_download is unavailable in offline mode")

                modelscope_stub.snapshot_download = _snapshot_download  # type: ignore[attr-defined]
                modelscope_stub.__version__ = "0.0.0-stub"
                sys.modules["modelscope"] = modelscope_stub
                injected_modelscope_stub = True
            try:
                import paddlex as pdx  # type: ignore
            finally:
                sys.path[:] = original_sys_path
                if injected_modelscope_stub:
                    sys.modules.pop("modelscope", None)

            self._structure_paddlex_layout_model = pdx.create_model("PP-DocLayoutV3")
            logger.info("PP-Structure preprocess paddlex fallback enabled: model=PP-DocLayoutV3")
            return self._structure_paddlex_layout_model
        except Exception as exc:
            self._structure_paddlex_model_init_error = str(exc)
            logger.warning(f"PP-Structure preprocess paddlex fallback disabled: {exc}")
            return None

    def _collect_structure_blocks_via_paddlex(self, image_path: str) -> Optional[List[Dict[str, Any]]]:
        model = self._get_paddlex_layout_model()
        if model is None:
            return None

        image = cv2.imread(image_path)
        if image is None or image.size == 0:
            return []
        image_h, image_w = image.shape[:2]

        try:
            results = list(model.predict(image_path))
        except Exception as exc:
            logger.warning(f"PaddleX layout inference failed for {Path(image_path).name}: {exc}")
            return None

        blocks: List[Dict[str, Any]] = []
        for item in results:
            if not isinstance(item, dict):
                continue
            boxes = item.get("boxes", [])
            if not isinstance(boxes, list):
                continue
            for box in boxes:
                if not isinstance(box, dict):
                    continue
                block_type = self._normalize_structure_type(box.get("label"))
                bbox = self._normalize_bbox(
                    box.get("coordinate") or box.get("bbox") or box.get("box"),
                    image_w=image_w,
                    image_h=image_h,
                )
                if block_type and bbox:
                    blocks.append({"type": block_type, "bbox": bbox})
        return blocks

    def _get_structure_engine(self):
        if not bool(getattr(self, "_structure_preprocess_enabled", False)):
            return None
        if self._structure_engine is not None:
            return self._structure_engine
        if self._structure_engine_init_error:
            return None
        try:
            self._patch_ppstructure_ir_optim_if_needed()
            from paddleocr import PPStructure  # type: ignore
            try:
                from paddleocr import PPStructureV3  # type: ignore
            except Exception:
                PPStructureV3 = None  # type: ignore

            engine_cls = PPStructureV3 or PPStructure
            if engine_cls is None:
                self._structure_engine_init_error = "ppstructure_class_not_found"
                logger.warning("PP-Structure preprocessor disabled: class not found")
                return None
            try:
                init_kwargs: Dict[str, Any] = {"show_log": False}
                if bool(getattr(self, "_structure_force_disable_ir_optim", True)):
                    init_kwargs["ir_optim"] = False
                self._structure_engine = engine_cls(**init_kwargs)
            except TypeError:
                self._structure_engine = engine_cls()
            logger.info(
                "PP-Structure preprocessor enabled: engine=%s",
                getattr(engine_cls, "__name__", "unknown"),
            )
            return self._structure_engine
        except Exception as exc:
            self._structure_engine_init_error = str(exc)
            logger.warning(f"PP-Structure preprocessor disabled: {exc}")
            return None

    @staticmethod
    def _normalize_bbox(raw_bbox: Any, image_w: int, image_h: int) -> Optional[Tuple[int, int, int, int]]:
        if raw_bbox is None:
            return None

        x1 = y1 = x2 = y2 = None
        if isinstance(raw_bbox, dict):
            keys = {str(k).lower(): v for k, v in raw_bbox.items()}
            if all(key in keys for key in ("x1", "y1", "x2", "y2")):
                x1, y1, x2, y2 = keys["x1"], keys["y1"], keys["x2"], keys["y2"]
        elif isinstance(raw_bbox, (list, tuple)):
            if len(raw_bbox) == 4 and all(isinstance(v, (int, float)) for v in raw_bbox):
                x1, y1, x2, y2 = raw_bbox
            elif len(raw_bbox) >= 4 and all(
                isinstance(v, (list, tuple)) and len(v) >= 2 for v in raw_bbox[:4]
            ):
                xs = [float(v[0]) for v in raw_bbox[:4]]
                ys = [float(v[1]) for v in raw_bbox[:4]]
                x1, y1, x2, y2 = min(xs), min(ys), max(xs), max(ys)

        if x1 is None or y1 is None or x2 is None or y2 is None:
            return None

        x1 = max(0, min(int(round(float(x1))), max(0, image_w - 1)))
        y1 = max(0, min(int(round(float(y1))), max(0, image_h - 1)))
        x2 = max(0, min(int(round(float(x2))), image_w))
        y2 = max(0, min(int(round(float(y2))), image_h))
        if x2 <= x1 or y2 <= y1:
            return None
        return x1, y1, x2, y2

    @staticmethod
    def _union_bboxes(
        bboxes: List[Tuple[int, int, int, int]],
        image_w: int,
        image_h: int,
    ) -> Optional[Tuple[int, int, int, int]]:
        if not bboxes:
            return None
        x1 = min(item[0] for item in bboxes)
        y1 = min(item[1] for item in bboxes)
        x2 = max(item[2] for item in bboxes)
        y2 = max(item[3] for item in bboxes)
        x1 = max(0, min(x1, max(0, image_w - 1)))
        y1 = max(0, min(y1, max(0, image_h - 1)))
        x2 = max(0, min(x2, image_w))
        y2 = max(0, min(y2, image_h))
        if x2 <= x1 or y2 <= y1:
            return None
        return x1, y1, x2, y2

    @staticmethod
    def _bbox_overlap_ratio_by_smaller_area(
        bbox_a: Tuple[int, int, int, int],
        bbox_b: Tuple[int, int, int, int],
    ) -> float:
        """Docstring omitted."""
        ax1, ay1, ax2, ay2 = bbox_a
        bx1, by1, bx2, by2 = bbox_b
        inter_x1 = max(ax1, bx1)
        inter_y1 = max(ay1, by1)
        inter_x2 = min(ax2, bx2)
        inter_y2 = min(ay2, by2)
        inter_w = max(0, inter_x2 - inter_x1)
        inter_h = max(0, inter_y2 - inter_y1)
        inter_area = inter_w * inter_h
        if inter_area <= 0:
            return 0.0
        area_a = max(0, (ax2 - ax1) * (ay2 - ay1))
        area_b = max(0, (bx2 - bx1) * (by2 - by1))
        smaller_area = min(area_a, area_b)
        if smaller_area <= 0:
            return 0.0
        return float(inter_area) / float(smaller_area)

    @staticmethod
    def _bbox_area(bbox: Tuple[int, int, int, int]) -> int:
        x1, y1, x2, y2 = bbox
        return max(0, x2 - x1) * max(0, y2 - y1)

    @staticmethod
    def _bbox_union_area(bboxes: List[Tuple[int, int, int, int]]) -> int:
        if not bboxes:
            return 0
        xs = sorted({coord for x1, _, x2, _ in bboxes for coord in (x1, x2)})
        if len(xs) <= 1:
            return 0

        total_area = 0
        for idx in range(len(xs) - 1):
            left = xs[idx]
            right = xs[idx + 1]
            if right <= left:
                continue

            y_intervals: List[Tuple[int, int]] = []
            for x1, y1, x2, y2 in bboxes:
                if x1 < right and x2 > left:
                    y_intervals.append((y1, y2))
            if not y_intervals:
                continue

            y_intervals.sort(key=lambda item: (item[0], item[1]))
            merged_y_length = 0
            current_start, current_end = y_intervals[0]
            for start, end in y_intervals[1:]:
                if start <= current_end:
                    current_end = max(current_end, end)
                else:
                    merged_y_length += max(0, current_end - current_start)
                    current_start, current_end = start, end
            merged_y_length += max(0, current_end - current_start)
            total_area += (right - left) * merged_y_length
        return int(max(0, total_area))

    @staticmethod
    def _bbox_edge_gap(
        bbox_a: Tuple[int, int, int, int],
        bbox_b: Tuple[int, int, int, int],
    ) -> Tuple[int, int]:
        ax1, ay1, ax2, ay2 = bbox_a
        bx1, by1, bx2, by2 = bbox_b
        gap_x = max(0, max(bx1 - ax2, ax1 - bx2))
        gap_y = max(0, max(by1 - ay2, ay1 - by2))
        return int(gap_x), int(gap_y)

    def _should_attach_context_bbox(
        self,
        target_bbox: Tuple[int, int, int, int],
        context_bbox: Tuple[int, int, int, int],
        nearby_px: int,
    ) -> bool:
        if self._bbox_overlap_ratio_by_smaller_area(target_bbox, context_bbox) > 0.0:
            return True
        if nearby_px <= 0:
            return False
        gap_x, gap_y = self._bbox_edge_gap(target_bbox, context_bbox)
        if gap_x == 0 and gap_y <= nearby_px:
            return True
        if gap_y == 0 and gap_x <= nearby_px:
            return True
        return (gap_x * gap_x + gap_y * gap_y) <= (nearby_px * nearby_px)

    def _expand_bbox_with_context(
        self,
        seed_bbox: Tuple[int, int, int, int],
        context_bboxes: List[Tuple[int, int, int, int]],
        nearby_px: int,
        image_w: int,
        image_h: int,
    ) -> Tuple[int, int, int, int]:
        if not context_bboxes:
            return seed_bbox
        current = seed_bbox
        changed = True
        while changed:
            changed = False
            for context_bbox in context_bboxes:
                if not self._should_attach_context_bbox(current, context_bbox, nearby_px=nearby_px):
                    continue
                union_bbox = self._union_bboxes([current, context_bbox], image_w=image_w, image_h=image_h)
                if union_bbox is None or union_bbox == current:
                    continue
                current = union_bbox
                changed = True
        return current

    @staticmethod
    def _crop_group_priority(group_type: str) -> int:
        mapping = {
            "figure_bundle": 0,
            "table_bundle": 1,
            "algorithm": 2,
            "formula": 3,
            "image": 4,
        }
        token = str(group_type or "").strip().lower()
        if token in mapping:
            return mapping[token]
        if token.endswith("_bundle"):
            return 5
        return 50

    def _merge_group_type(self, kept_group: str, incoming_group: str) -> str:
        kept_token = str(kept_group or "").strip().lower()
        incoming_token = str(incoming_group or "").strip().lower()
        if kept_token.endswith("_bundle"):
            return kept_group
        if incoming_token.endswith("_bundle"):
            return incoming_group
        if self._crop_group_priority(incoming_token) < self._crop_group_priority(kept_token):
            return incoming_group
        return kept_group

    def _merge_crop_plan_by_overlap(
        self,
        crop_plan: List[Tuple[str, Tuple[int, int, int, int]]],
        image_w: int,
        image_h: int,
        overlap_threshold: float,
    ) -> List[Tuple[str, Tuple[int, int, int, int]]]:
        if len(crop_plan) <= 1:
            return list(crop_plan)

        threshold = max(0.0, min(1.0, float(overlap_threshold or 0.0)))
        ordered = sorted(
            crop_plan,
            key=lambda item: (
                self._crop_group_priority(item[0]),
                -self._bbox_area(item[1]),
            ),
        )
        kept: List[Tuple[str, Tuple[int, int, int, int]]] = []
        for incoming_group, incoming_bbox in ordered:
            merged = False
            for idx, (kept_group, kept_bbox) in enumerate(kept):
                overlap_ratio = self._bbox_overlap_ratio_by_smaller_area(incoming_bbox, kept_bbox)
                if overlap_ratio < threshold:
                    continue
                union_bbox = self._union_bboxes([incoming_bbox, kept_bbox], image_w=image_w, image_h=image_h)
                if union_bbox is None:
                    continue
                merged_group = self._merge_group_type(kept_group, incoming_group)
                kept[idx] = (merged_group, union_bbox)
                merged = True
                break
            if not merged:
                kept.append((incoming_group, incoming_bbox))
        return kept

    def _merge_bboxes_by_overlap(
        self,
        bboxes: List[Tuple[int, int, int, int]],
        image_w: int,
        image_h: int,
        overlap_threshold: float,
    ) -> List[Tuple[int, int, int, int]]:
        """Docstring omitted."""
        if len(bboxes) <= 1:
            return list(bboxes)
        threshold = max(0.0, min(1.0, float(overlap_threshold or 0.0)))
        merged: List[Tuple[int, int, int, int]] = []
        for bbox in bboxes:
            current = bbox
            merged_changed = True
            while merged_changed:
                merged_changed = False
                next_merged: List[Tuple[int, int, int, int]] = []
                for existing in merged:
                    overlap_ratio = self._bbox_overlap_ratio_by_smaller_area(current, existing)
                    if overlap_ratio >= threshold:
                        union_bbox = self._union_bboxes([current, existing], image_w=image_w, image_h=image_h)
                        if union_bbox is None:
                            next_merged.append(existing)
                            continue
                        current = union_bbox
                        merged_changed = True
                    else:
                        next_merged.append(existing)
                merged = next_merged
            merged.append(current)
        return merged

    def _collect_structure_blocks(self, image_path: str) -> Optional[List[Dict[str, Any]]]:
        engine = self._get_structure_engine()
        if engine is None:
            return self._collect_structure_blocks_via_paddlex(image_path)

        image = cv2.imread(image_path)
        if image is None or image.size == 0:
            return []
        image_h, image_w = image.shape[:2]

        try:
            raw_result = engine(image)
            return self._parse_structure_blocks_from_raw_result(raw_result, image_w=image_w, image_h=image_h)
        except Exception as exc:
            # 一次性尝试兼容重试，成功后继续走 PP-Structure 主路径。
            if self._attempt_structure_backend_compat_recovery(exc):
                recovered_engine = self._get_structure_engine()
                if recovered_engine is not None:
                    try:
                        raw_result = recovered_engine(image)
                        logger.info(
                            "PP-Structure backend recovered for %s after compatibility retry.",
                            Path(image_path).name,
                        )
                        return self._parse_structure_blocks_from_raw_result(
                            raw_result,
                            image_w=image_w,
                            image_h=image_h,
                        )
                    except Exception as retry_exc:
                        logger.warning(
                            "PP-Structure compatibility retry failed for %s: %s",
                            Path(image_path).name,
                            self._summarize_exception(retry_exc),
                        )
                        logger.debug(
                            "PP-Structure compatibility retry traceback for %s",
                            Path(image_path).name,
                            exc_info=retry_exc,
                        )
                        exc = retry_exc
            if bool(getattr(self, "_structure_disable_after_backend_error", True)) and self._is_structure_backend_runtime_error(exc):
                self._structure_engine = None
                self._structure_engine_init_error = f"runtime_backend_error:{type(exc).__name__}"
                logger.warning(
                    "PP-Structure disabled after backend runtime error; subsequent images use paddlex fallback."
                )
            logger.warning(
                "PP-Structure inference failed for %s, trying paddlex fallback: %s",
                Path(image_path).name,
                self._summarize_exception(exc),
            )
            logger.debug(
                "PP-Structure inference traceback for %s",
                Path(image_path).name,
                exc_info=exc,
            )
            return self._collect_structure_blocks_via_paddlex(image_path)

    def _parse_structure_blocks_from_raw_result(
        self,
        raw_result: Any,
        image_w: int,
        image_h: int,
    ) -> List[Dict[str, Any]]:
        blocks: List[Dict[str, Any]] = []
        queue: List[Any] = [raw_result]
        while queue:
            node = queue.pop(0)
            if isinstance(node, list):
                queue.extend(node)
                continue
            if not isinstance(node, dict):
                continue

            block_type = self._normalize_structure_type(
                node.get("type") or node.get("label") or node.get("layout")
            )
            bbox = self._normalize_bbox(
                node.get("bbox") or node.get("box") or node.get("points"),
                image_w=image_w,
                image_h=image_h,
            )
            if block_type and bbox:
                blocks.append({"type": block_type, "bbox": bbox})

            for key in ("res", "result", "layout_result", "children", "items"):
                value = node.get(key)
                if isinstance(value, (list, dict)):
                    queue.append(value)
        return blocks

    def extract_structured_screenshots(
        self,
        image_path: str,
        source_id: str = "",
        timestamp_sec: Optional[float] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐鐑芥嚄閸撲焦鍏滈柛顐ｆ礀閻ら箖鏌ｉ幇顒佲枙婵炲瓨鐗犻弻鏇熺箾瑜嶅Λ妤€鐨?PP-StructureV3 闂傚倸鍊搁崐鐑芥嚄閸洖绠犻柟鎹愵嚙鎼村﹪鏌熺紒妯哄瀭婵炴垶顭傞弮鍫濆窛妞ゆ挾鍋熼埀顒夊弮閺岀喖宕楅懖鈺傛闂佸憡鏌ㄩ惉鑲╁垝婵犳碍鏅查柛鈩冨姃缁ㄥ妫呴銏″闁瑰憡鎮傞、鏃堫敂閸℃瑧锛滈梺绋挎湰濮樸劍鏅堕敃鍌涚厸?
        # 闂傚倸鍊风粈渚€骞栭位鍥敃閿曗偓閻ょ偓绻濇繝鍌滃闁藉啰鍠栭弻鏇熺箾閸喖澹勫┑鐐叉▕娴滄粓宕橀埀顒€顪冮妶鍡樺暗闁稿鍠栭弫宥呪枎閹剧补鎷洪梺缁樻尭濞撮绮斿ú顏呯厱閻庯綆浜烽煬顒傗偓瑙勬礈閺佽顕ｉ浣瑰劅闁圭偓鎯屽鏃堟⒒娴ｅ憡鎯堥柛濠傜秺椤㈡牕鈻庤箛锝団偓鍓佹喐韫囨洘顫?
        # - None: 婵犵數濮烽。钘壩ｉ崨鏉戠；闁告侗鍙庨悢鍡樹繆椤栨氨姣為柛瀣尭椤繈鎮℃惔鈾€鎷梺鑺ド戠换鍫ュ蓟瀹ュ浼犻柛鏇ㄥ墮濞呫倝姊虹粙娆惧剳濠电偐鍋撻梺鍝勬湰閻╊垶宕洪崟顖氱闁冲搫鍠氬Σ鍫曟⒒娴ｈ棄鍚归柛鐘冲姉閹广垽宕奸妷銉ㄦ憰濠电偞鍨惰彜闁哄瀛╂穱濠囶敍濠靛浂浠╂繛瀵稿閸ㄨ泛顫忛悜妯诲濞寸厧鐡ㄩ鏍⒑缁嬪尅鏀荤紒璇茬墕椤曪綁顢曢姀鈺佹倯闂佸憡渚楅崹鍗炩枔閺囥垺鈷戦柛娑橈工婵箑霉濠婂嫮绠炴い銏＄懃椤撳吋寰勭€Ｑ勫婵犵數鍋犵亸娆戝垝椤栨稓绠鹃柛銉墯閻撴洟鏌￠崒婵囩《閺佸牓鎮楀▓鍨灈妞ゎ厾鍏橀獮鍐閵堝棗浜楅柟鑹版彧缂嶅棝宕捄琛℃斀闁绘﹩鍠栭悘杈ㄧ箾婢跺娲存い銏＄墵瀹曞崬鈽夊▎鎰彨闂佽鍑界紞鍡涘窗濡ゅ懎鐓曢柟瀵稿亼娴滄粓鏌熼弶鍨暢缂佸绮妵鍕敃閿濆孩鐣肩紓浣介哺鐢鏁嶉幇顑芥斀闁糕剝娲滈弫鏍⒒娴ｅ憡璐℃い顐㈩儔瀹曟儼顦村Δ鐘叉喘濮婃椽宕滈懠顒€甯ラ梺鍝ュУ椤ㄥ﹤顕ｉ崨濞垮亝闁告劏鏂侀幏娲⒑绾懎浜归柛瀣洴瀹曟繂鈻庨幘瀵稿幈闂佸搫娲ㄩ崳锕傛嚀閸ф鐓曢柍瑙勫劤娴?
        # - []: 婵犵數濮烽。钘壩ｉ崨鏉戠；闁告侗鍙庨悢鍡樹繆椤栨氨姣為柛瀣尭椤繈鎮℃惔鈾€鎷梺鑺ド戠换鍫ュ蓟瀹ュ浼犻柛鏇ㄥ墮濞呫倝姊虹粙娆惧剳濠电偐鍋撻梺鍝勬湰閻╊垶宕洪崟顖氱闁绘劦鍓涢弳銉╂⒑閼姐倕小闁绘帪绠撻幃锟犳晸閻樿尪鎽曢梺鎸庣箓椤︻垶锝為崨瀛樼厓闁告繂瀚弳娆撴偨椤栨ê濡跨紒杈ㄦ尰閹峰懏绂掔€ｎ亝鎳欑紓鍌欐祰椤曆囧疮閹绢喚宓侀煫鍥ㄧ☉瀹告繈鏌℃径瀣仴闁兼澘鐏濋埞鎴﹀煡閸℃浠村銈嗘肠閸涱噮娼熼梺鎸庢礀閸婂綊鎮￠悢鍏肩厽婵☆垰鎼痪褔鏌熼崗鐓庡濞ｅ洤锕、鏇㈡晲鎼淬垻鏉归柣搴ゎ潐濞叉ê顪冮挊澶屾殾闁告鍋愬Σ鍫熶繆椤栨壕鎷℃俊鍙夋尦濮婄粯鎷呯粵瀣秷濡炪倖姊归悧鐘茬暦閺夎鏃堝礃椤忓棗绠垫繝寰锋澘鈧洟骞婅箛娑欏亗婵炴垶鍩冮崑鎾荤嵁閸喖濮庨梺鐟板暱缁绘劙顢欒箛鎾斀閻庯綆鍋勬禒鈺佲攽椤旂瓔鐒介柛妯犲嫮顩查柟娈垮枓閸嬫挾鎲撮崟顒傤槬濠电姭鍋撻梺顒€绉撮悞鍨亜閹烘埊鍔熼柛鎺撴緲椤儻顦叉繛鎾棑閸掓帡宕奸悢椋庣獮闂佸綊鍋婇崢鍏肩椤栫偞鐓熼幖鎼灣缁夐潧霉濠婂嫮鐭掔€殿喗濞婇幃銏ゅ礂閼测晛骞愬┑鐐舵彧缂嶄礁顭囪閸┾偓妞ゆ帊鑳剁粻鎾淬亜閺囶亞绉い銏☆殜瀹曠喖顢楅崒姘ュ┑鐘垫暩閸嬬偤宕圭捄渚僵闁靛ň鏅涚粻鐔烘喐閻楀牆绗氶柣鎾寸☉椤法鎹勯悜姗嗘！濠电偛鎳庡Λ娑氭閹烘柡鍋撻敐搴′簻闁诲繐顕埀?Vision闂?
        # - List: 闂傚倸鍊搁崐鐑芥嚄閸洖绠犻柟鎹愵嚙鎼村﹪鏌熺紒妯哄瀭婵炴垶顭傞弮鍫濆窛妞ゆ挾鍋熼埀顒夊弮閺岀喖宕楅懖鈺傛闂佸憡鏌ㄩ惉濂稿箟閹绢喗鏅濋柛灞炬皑椤旀劙鏌℃径濠勫濠⒀呮櫕缁瑦绻濆顓犲幗闂佹寧绻傞ˇ顕€骞夋ィ鍐╃厸鐎光偓閳ь剟宕伴弽顓犲祦闁哄稁鍙庨弫鍥ㄧ節闂堟稓澧㈡繛澶嬪缁绘繈鎮介棃娑楃捕闂佸鏉垮闁轰礁鍟存慨鈧柣妯虹仛濞堟儳鈹戦鏂や緵闁稿海鏁诲顕€宕煎┑鍫Ч闂備礁鐤囬～澶愬磿閾忣偆顩插Δ锝呭暞閻撴洟鏌熸导瀛樻锭闁哄绋掑?
        """Docstring omitted."""
        if not bool(getattr(self, "_structure_preprocess_enabled", False)):
            return None
        if self._get_structure_engine() is None and self._get_paddlex_layout_model() is None:
            return None
        if not image_path or not os.path.exists(image_path):
            return []

        image = cv2.imread(image_path)
        if image is None or image.size == 0:
            return []
        image_h, image_w = image.shape[:2]
        blocks = self._collect_structure_blocks(image_path)
        if blocks is None:
            # 闂傚倸鍊搁崐宄懊归崶顒婄稏濠㈣泛顑囬々鎻捗归悩宸剰缂佲偓婢舵劕绠规繛锝庡墮婵¤偐绱掗悩宕囧ⅹ闁宠鍨块幃鈺呭矗婢跺妲遍梻浣瑰▕閺€閬嶅箲閸ヮ剙钃熼柕鍫濐槸瀹告繂鈹戦悩鏌ヮ€楅柡鍛翠憾濮婃椽宕妷銉ゆ埛闂佸搫鎷嬮崑濠囧春閳ь剚銇勯幒宥囪窗闁哥喎绻橀弻娑㈡偐瀹曞洤鈷岄悗瑙勬礃閸旀瑩鐛弽銊﹀闁荤喖顣﹂崙浠嬫⒒娴ｇ顥忛柛瀣噹鐓ゆ慨妞诲亾鐎殿喗鐓￠、妤佹媴閻熸澘浼庢繝娈垮枟椤ㄥ懎螞濡ゅ懐宓侀柡宥庡幗閻撶喖鏌ㄩ弴妤€浜惧銈庡幘閸忔ê顕ｇ拠宸悑濠㈣泛谩閵娾晜鐓ラ柡鍌氱仢閳锋棃鏌ｉ幘鎰佹Ц妞ゎ亜鍟存俊鎯扮疀閺囩偟鐓楅梻浣告惈閹冲繒鍒掑畝鍕垫晪闁靛鏅涚粈瀣亜閺嶃劍鐨戞い锔芥緲椤啴濡堕崱妯烘殫闂佸摜濮撮柊锝夊春閳ь剚銇勯幒鎴濃偓鍛婄濠婂牊鐓冮柦妯侯樈濡偓濡ょ姷鍋為敋闁伙絿鍏樺畷鍫曞幢閹邦剛浼勯梺鐟板级閹倿骞冭瀹曞ジ濮€閻樿尙鍝楅梻鍌欐祰椤曟牠宕伴弴鐘插灊婵炲棙鍔掔换鍡涙煙闂傚顦﹂柣鎾村灴閺屽秵娼悧鍫▊婵犮垼顫夊ú鏍煘閹达附鍋愮紓浣股戦柨顓炩攽閳藉棗浜濋柣鐔濆懎鍨濋柛顐犲劚閻掑灚銇勯幒鎴濐仾闁抽攱甯掗妴鎺戭潩椤掍焦鎮欐繛瀛樼矋閸旀瑩寮诲☉姘ｅ亾閿濆骸浜濈€规洖鐭傞弻锛勪沪鐠囨彃顬堥梺瀹犳椤︻垵鐏掓繛鎾村嚬閸ㄩ亶顢欓崟顓犵＝闁稿本鐟ч崝宥嗐亜閵娿儲鍤囬挊婵嬫⒒閸喓銆掗柡鍡畵閺岀喓鈧稒顭囩粻銉︾箾鐏忔牗娅婇柡灞诲妼閳规垿宕卞璇蹭壕闁告稑锕﹂々鏌ユ煟閹伴潧澧扮紒鈾€鍋撳┑鐘垫暩婵挳宕愰幖渚囨晜妞ゅ繐鐗婇悡銉╂煟閺冣偓濞兼瑩宕濋敃鍌涚厸鐎光偓鐎ｎ剛袦闂佺硶鏂侀崜婵堟崲濠靛纾兼繛鎴炵煯閹查箖姊婚崒娆掑厡缂侇噮鍨堕幃褔顢橀悩鑼瓘闂佺鍕垫畷闁稿鐗犻弻娑㈠箻濡も偓鐎氼剟鎮垫导瀛樷拺闁兼祴鏂侀幏锟犳煕韫囨棑鑰跨€规洘鍨块獮妯肩磼濡　鍋撴繝姘參婵☆垯璀﹀Σ鍝劽归悡搴剰闁宠鍨块幃鈺冪磼濡鏁繝鐢靛仜閻即宕濋幋锕€绠栭柡澶婄氨閺€浠嬫煕閵夛絽濡肩€殿喛娉曠槐鎺旂磼閵忕姴绠归梺鐟板暱闁帮綁骞冮悙瀵割浄閻庯綆鍋嗛崢?
            return None
        if not blocks:
            return []

        target_types = {
            self._normalize_structure_type(item)
            for item in getattr(self, "_structure_target_types", set())
            if self._normalize_structure_type(item)
        }
        if not target_types:
            target_types = {
                "algorithm",
                "formula",
                "image",
                "figure",
                "figure_caption",
                "figure_title",
                "table",
                "table_caption",
            }
        context_types = {
            self._normalize_structure_type(item)
            for item in getattr(self, "_structure_context_types", set())
            if self._normalize_structure_type(item)
        }
        if not context_types:
            context_types = {"text", "code"}
        candidate_types = set(target_types) | set(context_types)

        grouped_bboxes: Dict[str, List[Tuple[int, int, int, int]]] = {}
        all_recognized_bboxes: List[Tuple[int, int, int, int]] = []
        for block in blocks:
            block_type = self._normalize_structure_type(block.get("type"))
            bbox = block.get("bbox")
            if not isinstance(bbox, tuple):
                continue
            if block_type:
                all_recognized_bboxes.append(bbox)
            if block_type not in candidate_types:
                continue
            grouped_bboxes.setdefault(block_type, []).append(bbox)

        if not grouped_bboxes:
            return []

        overlap_threshold = float(
            getattr(self, "_structure_bbox_overlap_merge_threshold", 0.9) or 0.9
        )
        for type_name, type_bboxes in list(grouped_bboxes.items()):
            grouped_bboxes[type_name] = self._merge_bboxes_by_overlap(
                type_bboxes,
                image_w=image_w,
                image_h=image_h,
                overlap_threshold=overlap_threshold,
            )

        figure_types = {"figure", "figure_caption", "figure_title"}
        table_types = {"table", "table_caption"}

        crop_plan: List[Tuple[str, Tuple[int, int, int, int]]] = []
        figure_boxes: List[Tuple[int, int, int, int]] = []
        for type_name in figure_types:
            figure_boxes.extend(grouped_bboxes.get(type_name, []))
        figure_bbox = self._union_bboxes(figure_boxes, image_w=image_w, image_h=image_h)
        if figure_bbox:
            crop_plan.append(("figure_bundle", figure_bbox))

        table_boxes: List[Tuple[int, int, int, int]] = []
        for type_name in table_types:
            table_boxes.extend(grouped_bboxes.get(type_name, []))
        table_bbox = self._union_bboxes(table_boxes, image_w=image_w, image_h=image_h)
        if table_bbox:
            crop_plan.append(("table_bundle", table_bbox))

        for type_name in ("algorithm", "formula", "image"):
            for bbox in grouped_bboxes.get(type_name, []):
                crop_plan.append((type_name, bbox))

        if not crop_plan:
            return []

        context_bboxes: List[Tuple[int, int, int, int]] = []
        for context_type in context_types:
            context_bboxes.extend(grouped_bboxes.get(context_type, []))
        if context_bboxes:
            nearby_px = int(getattr(self, "_structure_context_nearby_px", 18) or 0)
            crop_plan = [
                (
                    group_type,
                    self._expand_bbox_with_context(
                        seed_bbox=bbox,
                        context_bboxes=context_bboxes,
                        nearby_px=nearby_px,
                        image_w=image_w,
                        image_h=image_h,
                    ),
                )
                for group_type, bbox in crop_plan
            ]

        pre_merge_count = len(crop_plan)
        crop_plan = self._merge_crop_plan_by_overlap(
            crop_plan=crop_plan,
            image_w=image_w,
            image_h=image_h,
            overlap_threshold=overlap_threshold,
        )
        if len(crop_plan) < pre_merge_count:
            logger.info(
                "Structure preprocess merged %d overlapping crop(s) into bundle/primary crop: source=%s",
                pre_merge_count - len(crop_plan),
                source_id or Path(image_path).name,
            )

        skip_split_threshold = float(
            getattr(self, "_structure_skip_split_bbox_coverage_threshold", 0.0) or 0.0
        )
        if skip_split_threshold > 0.0:
            planned_bboxes = [bbox for _, bbox in crop_plan]
            required_type_bbox_area_sum = sum(
                self._bbox_area(item_bbox) for item_bbox in planned_bboxes
            )
            recognized_enclosing_bbox = self._union_bboxes(
                all_recognized_bboxes,
                image_w=image_w,
                image_h=image_h,
            )
            if recognized_enclosing_bbox is not None:
                denominator_area = max(1, self._bbox_area(recognized_enclosing_bbox))
            else:
                denominator_area = max(1, int(image_w) * int(image_h))
            coverage_ratio = float(required_type_bbox_area_sum) / float(denominator_area)
            if coverage_ratio >= skip_split_threshold:
                logger.info(
                    "Structure preprocess fallback to raw screenshot: required_area_sum=%d, recognized_enclosing_area=%d, coverage=%.4f >= threshold=%.4f, source=%s",
                    int(required_type_bbox_area_sum),
                    int(denominator_area),
                    coverage_ratio,
                    skip_split_threshold,
                    source_id or Path(image_path).name,
                )
                return None

        margin = int(getattr(self, "_structure_crop_margin_px", 4) or 0)
        src_path = Path(image_path)
        output_items: List[Dict[str, Any]] = []
        for index, (group_type, bbox) in enumerate(crop_plan, start=1):
            x1, y1, x2, y2 = bbox
            x1 = max(0, x1 - margin)
            y1 = max(0, y1 - margin)
            x2 = min(image_w, x2 + margin)
            y2 = min(image_h, y2 + margin)
            if x2 <= x1 or y2 <= y1:
                continue
            crop_img = image[y1:y2, x1:x2]
            if crop_img is None or crop_img.size == 0:
                continue

            crop_name = (
                f"{src_path.stem}__ppstructure_{group_type}_{index:02d}_{uuid.uuid4().hex[:8]}.png"
            )
            crop_path = src_path.parent / crop_name
            try:
                ok = cv2.imwrite(str(crop_path), crop_img)
            except Exception:
                ok = False
            if not ok:
                continue
            output_items.append(
                {
                    "image_path": str(crop_path),
                    "group_type": group_type,
                    "source_id": source_id,
                    "timestamp_sec": float(timestamp_sec) if timestamp_sec is not None else None,
                    "parent_image_path": image_path,
                    "parent_key": str(src_path.resolve()),
                    "is_structured_crop": True,
                    "crop_index": index,
                    "bbox_xyxy": [int(x1), int(y1), int(x2), int(y2)],
                    "bbox_normalized_xyxy": [
                        float(x1) / float(max(1, image_w)),
                        float(y1) / float(max(1, image_h)),
                        float(x2) / float(max(1, image_w)),
                        float(y2) / float(max(1, image_h)),
                    ],
                    "parent_image_size": [int(image_w), int(image_h)],
                }
            )
        return output_items

    def _compute_exact_image_signature(self, image_path: str) -> Optional[str]:
        """Docstring omitted."""
        if not image_path:
            return None
        try:
            image = cv2.imread(image_path, cv2.IMREAD_UNCHANGED)
            if image is None:
                return None
            hasher = fast_hasher()
            hasher.update(str(image.shape).encode("utf-8"))
            hasher.update(str(image.dtype).encode("utf-8"))
            hasher.update(image.tobytes())
            return hasher.hexdigest()
        except Exception:
            return None

    def dedupe_structured_candidates_keep_latest(
        self,
        candidates: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        # 闂傚倷娴囧畷鐢稿窗閹邦喖鍨濋幖娣灪濞呯姵淇婇妶鍛櫣缂佺姳鍗抽弻娑樷槈濮楀牊鏁惧┑鐐叉噽婵炩偓闁哄矉绲借灒闁稿繐鍚嬪В鍕繆閻愰娼愬┑鐐诧躬瀵鍨鹃幇浣告倯闁硅偐琛ラ埀顒冨皺閸戣绻濋悽闈涗粶闁绘鍋熺槐鐐寸瑹閳ь剙顕ｆ繝姘╅柕澶堝灪椤秴鈹戦悙鍙夘棡閻㈩垱甯¤棢婵犻潧顑嗛埛鎴︽煙閼测晛浠滃┑陇鍋愮槐鎾愁吋閸滃啫浼愬銈庡亜缁绘垹绮嬮幒鏂哄亾閿濆簶鍋撻濠傛处閻撴洟鎮橀悙浣冩婵炲牊鏌ㄩ湁婵犲﹤鎳庢禍鍓х磼缂佹绠栫紒缁樼箞瀹曟帒顫濋鐕佸敼闂傚倷鑳堕…鍫ユ晝閿曞倸鏋佸┑鐘宠壘閽冪喐绻涢幋娆忕仼缂佺媴缍侀弻銊モ攽閸℃﹫绱炴繛瀵稿Л閺呮稖鐏冮梺缁橈耿濞佳勭濠婂嫮绠鹃悹鍥囧喚娲紓渚囧枟閻燂箑顕ラ崟顖氱疀妞ゆ帊绶″Λ鐔兼⒑閼姐倕校濞存粈绮欏畷婵嗩吋閸涱亝鐏佸銈嗘磵閸嬫挻鎱ㄦ繝鍛仩缂侇喗鐟ч幑鍕Ω瑜忛弶浠嬫⒒娴ｅ憡鍟為柡灞诲姂椤㈡牠宕堕埡浣哥亰闂佸啿鎼幊搴ｇ不缂佹ǜ浜滈柡鍐ㄦ处椤ュ绻涢崼鐔虹煂缂佽鲸鎸荤粭鐔煎炊瑜庨悘鍫ユ⒑閸涘鎴犲垝閹惧磭鏆﹂柟杈剧畱缁犲鏌￠崒妯哄姕闁哄倵鍋撻梻鍌欑婢瑰﹪宕戦崱娑樼獥闁规崘顕уЧ鏌ユ煟濡偐甯涢柣鎾跺枛閺岋絽螣閸濆嫬绗梺鐓庡娴滎亪寮诲☉銏犳闁秆勵殔瀵即鎮楃憴鍕闁圭⒈鍋夐悘鍐⒑閸涘﹣绶遍柛姗€绠栭、姗€宕崟鍨瘜闂侀潧鐗嗛幊鎰不娴煎瓨鍊垫慨妯煎帶楠炴绱掗纰卞剶鐎规洖銈告俊鐑芥晜鐟欏嫬顏归梻鍌欑濠€杈╁垝椤栨粍鏆滈柣鎰皺閹姐儱鈹戦敍鍕杭闁稿ě鍥ㄢ挃闁告洦鍨遍崑鍌炴煃閵夈儳锛嶉柡鍡畵閺屾盯顢曢敐鍡欘槬缂備胶濮撮…鐑藉蓟濞戞ǚ妲堥柛妤冨仧娴狀垰顪冮妶蹇氬悅闁哄懐濮撮～蹇撁洪鍕獩婵犵數濮撮幊搴ㄋ夊┑瀣拺闁告繂瀚晶閬嶆煕閹惧鎳囬柍銉︽瀹曟﹢顢欓懖鈺嬬床闂備胶绮崝妯间焊濞嗘拲鍥敃閿旇В鎷虹紓鍌欑劍閿曗晠鎮為悾宀€纾兼い鏇炴噹閻忥箓鏌熼鐐毈闁诡喓鍨藉畷妤呮偂鎼存ê浜鹃柣銏犳啞閻撶喖鏌曡箛鏇炐ュù鐘崇洴閺屾盯濡堕崒姘缂傚倸鍊搁崐鐑芥⒔瀹ュ绀夌€广儱顦伴崑鍌炴煃閵夛箑澧柛銈嗘礃閵囧嫰骞掗幋婵愪患闂佺粯鍔曢敃顏堝蓟閵娾晛绫嶉柛灞剧矋閹叉﹢姊虹粙娆惧剭闁告梹鍨甸～蹇撁洪鍕唶闁硅壈鎻徊鍝勎ｉ崼婵愭富闁靛牆绻愰惁婊堟煕濞嗗繐鏆ｇ€规洘妞介崺鈧?
        if not candidates:
            return []

        indexed = list(enumerate(candidates))

        def _sort_key(item: Tuple[int, Dict[str, Any]]) -> Tuple[int, float, int]:
            idx, payload = item
            ts = payload.get("timestamp_sec")
            try:
                if ts is None:
                    return (0, float(idx), idx)
                return (1, float(ts), idx)
            except Exception:
                return (0, float(idx), idx)

        ordered = [payload for _, payload in sorted(indexed, key=_sort_key)]
        kept: List[Dict[str, Any]] = []
        accepted_signatures: List[Tuple[str, str]] = []
        duplicate_count = 0
        deleted_count = 0

        for payload in ordered:
            if not bool(payload.get("is_structured_crop", False)):
                kept.append(payload)
                continue

            image_path = str(payload.get("image_path", "") or "")
            parent_key = str(payload.get("parent_key", "") or "")
            current_signature = self._compute_exact_image_signature(image_path)
            if not current_signature:
                kept.append(payload)
                continue

            is_duplicate = False
            for accepted_signature, accepted_parent_key in accepted_signatures:
                if not accepted_signature:
                    continue
                if accepted_parent_key == parent_key:
                    continue
                if current_signature == accepted_signature:
                    is_duplicate = True
                    duplicate_count += 1
                    break

            if is_duplicate:
                try:
                    if os.path.exists(image_path):
                        os.remove(image_path)
                        deleted_count += 1
                except Exception as delete_error:
                    logger.warning(
                        "Structured screenshot dedupe failed to delete duplicate file: path=%s, err=%s",
                        image_path,
                        delete_error,
                    )
                continue

            kept.append(payload)
            accepted_signatures.append((current_signature, parent_key))
        if duplicate_count > 0:
            logger.info(
                "Structured screenshot dedupe removed %d candidate(s), deleted=%d, kept=%d",
                duplicate_count,
                deleted_count,
                len(kept),
            )
        return kept

    def _build_cache_signature(self, vision_config) -> str:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊搁崐宄懊归崶顒婄稏濠㈣埖鍔曠壕鍧楁煟閹惧啿顔傛繛鎴欏灩缁€瀣亜閺冨洦銆冩慨瑙勵殜濮婃椽骞栭悙鎻掑Ф闂佸憡鎸婚悷褏鍒掗崼銉ョ＜闁绘劕顕崢鐢告⒑鐠団€崇仭婵犮垺顭堥。鍧楁⒒娴ｅ憡鎯堟俊顐ｇ懇閹嫰顢涘鍏肩稁濠电偛妯婃禍婊呯棯瑜旈幃褰掑箒閹烘垵顥庨梺闈涚墕椤︿即鎮￠悢鍏肩厪闁割偅绻冮崳鐑樸亜閵壯冣枅闁哄苯绉烽¨渚€鏌涢幘瀛樼殤缂侇喗鐟╅獮鎺戭渻鐏忔牕浜鹃柛鎰靛枛楠炪垺绻涢幋锝夊摵闁哄應鏅犻幃妤呭礂婢跺﹣澹曢梻浣告啞閸斞呯磽濮橆儷褍螖閸涱喒鎷洪梻鍌氱墛缁嬫挾绮诲鑸电厓鐟滄粓宕滃▎鎴濐棜妞ゆ挾鍠撻々鏌ユ煥濠靛棙宸濈痪鎹愭闇夐柨婵嗘噹椤ュ繘鏌涙惔锛勭闁哄本鐩幃銏ゆ煥鐎ｎ亙娣梻浣筋嚃閸ｏ絿绮婚弽褏鏆︽繝濠傛－濡茬兘姊虹粙娆惧剱闁瑰憡鎮傞崺銉﹀緞婵炵偓鐎诲┑鈽嗗灥瀹曠敻寮抽妷鈺傗拻濞达絽鎲￠崯鐐烘煕閳轰礁鏆ｇ€规洘鍨块獮妯肩磼濡攱瀚?
        # 2) 闂傚倸鍊峰ù鍥х暦閸偅鍙忕€规洖娲﹂浠嬫煏閸繃澶勬い顐ｆ礋閺岋繝宕堕妷銉т痪闂?SHA256 缂傚倸鍊搁崐鎼佸磹閻戣姤鍊块柨鏂垮⒔閻瑩鏌熼悜姗嗘當缁炬儳娼￠弻鐔煎箚閻楀牜妫勭紓浣哄У閻楃娀寮婚敓鐘茬倞闁靛鍎虫禒楣冩⒑缁嬫鍎嶉柛鏃€鍨垮濠氬即閻旇櫣顔曢梺鍓茬厛閸犳帡宕戦幘婢勬棃宕ㄩ鍏肩杽闂備礁鎲￠幐鍡涘川椤栥倗闂梻鍌欒兌椤牓寮甸鍌滅煓闁圭儤鏌ч悞濠冦亜閹惧崬鐏柣鎾跺枛楠炴帡骞庨挊澶岊槷闂佺懓鐡ㄧ换宥咁焽閺嶎灛鏃堟晲閸涱厽娈紓渚囧亜缁夊綊寮诲☉姘勃閻犲洦绁撮崑鎾澄旈崨顓犲姦濡炪倖甯婇懗鍫曞矗閳ь剟姊?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘偓浣肝旈崥钘夋搐椤﹪寮?dumps + hashlib.sha256闂?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堟偂閻斿吋鐓熸俊顖濇硶缁ㄦ寧銇勯弬娆炬█闁哄矉绻濆畷銊╊敊閸撗呮毉婵犵鈧啿绾ч柛鏃€鐟╁顐﹀磼閻愬鍙嗗銈嗙墬閻喗绔熼弴鐐╂斀闁绘劖娼欓悘锔姐亜韫囷絼閭柟铏墵閸╋繝宕橀鍡樻闂備浇顕х换鎺楀磻閻樺磭绠鹃柛銉墯閸嬪嫰鏌ｉ幘铏崳濞寸厧顑夐幃妤冩喆閸曨剛顦ュ┑鐐跺皺婵炩偓鐎?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸閻ゎ喗銇勯幇鍫曟闁稿顑夐弻娑㈠焺閸愵亖妲堥梺缁樺笒閻忔岸濡甸崟顖氱闁瑰瓨绻冨▓顓熺箾鐎涙鐭婃繝鈧潏鈺傤潟闁圭儤顨忛弫瀣煕閳╁喚娈㈡繛宸弮濮婃椽鏌呴悙鑼跺濠⒀冨级閵囧嫰顢曢姀鈺傗枅闂佽鍠撻崹浠嬨€佸Δ鍛＜婵炴垼椴哥紞妤佺節閻㈤潧鈻堟繛浣冲浂鏁勯柛鈩冪⊕閸婂灝鈹戦崒姘暈闁绘挻娲橀妵鍕敇閻旈浠撮梺鍝勵儐閻╊垶骞冩禒瀣垫晬婵炴垶菤閺嬫瑩姊虹拠鈥虫灈缂傚秴锕悰顔界瑹閳ь剟鐛幒妤€绠ｆ繝鍨姈缂嶅棝姊婚崒娆戭槮闁圭寽銈呯稑闂備胶顭堥敃銉┿€冮崼婢綁骞囬弶璺ㄧ潉闂佸壊鍋掗崑鍛村疾閳哄懏鈷戠紒瀣皡閸旂喖鏌涢悩鍐插妞ゎ厼娼″浠嬵敇閻斿弶瀚奸梻浣告啞缁诲倻鈧凹鍘奸敃銏″鐎涙鍘电紓浣割儓濞夋洟宕洪敐澶嬬厱闁宠鍎虫禍?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - vision_config: VisionAIConfig 闂傚倸鍊搁崐鐑芥嚄閸洖绠犻柟鍓х帛閸嬨倝鏌曟繛鐐珕闁稿顑夐弻锟犲炊閵夈儳浠肩紓浣哄У閻楃娀寮婚悢鐓庣畾鐟滃秹銆傚畷鍥╂／闁诡垎浣镐划闂佸搫鐬奸崰鏍嵁閸℃凹妾ㄩ梺鎼炲€楅崰鏍蓟瀹ュ牜妾ㄩ梺鍛婃惈缁犳挸鐣疯ぐ鎺戠闁芥ê顦遍崝锕€顪冮妶鍡楃瑨闁稿﹤缍婂鎶藉煛閸屾ü绨婚梺闈涚箳婵挳鐓鍕厵缂佸瀵ч崵鍥┾偓瑙勬礈閸犳牠銆佸☉妯?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - 闂傚倸鍊搁崐鎼佸磹閻戣姤鍊块柨鏇楀亾妞ゎ厼鐏濊灒闁兼祴鏅濋悡瀣⒑閸撴彃浜濇繛鍙夛耿瀹曟垿顢旈崼鐔哄幈闂佹枼鏅涢崯浼村箠閸涱収娓婚悷娆忓閳绘洟鏌″畝鈧崰鏍€佸☉銏犲耿婵°倐鍋撴い蹇婃櫊閺屽秷顧侀柛鎾寸缁绘稒绻濋崶褏鐣哄┑掳鍊愰崑鎾绘煃閽樺妲搁柍璇查铻ｉ柤濮愬€栧В澶娾攽閿涘嫬浜奸柛濠冪墵瀹曞綊骞庨懞銉モ偓鍧楁煥閺傚灝鈷旈柣顓熺懃椤法鎹勯悜妯绘嫳闁诲孩纰嶅銊╁箟濮濆瞼鐤€闁哄洨濮烽悞濂告⒑閸涘﹥灏柡鈧—?56 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鍙傦箓鏌ｉ幇顔芥毄闁活厼妫楅妴鎺戭潩閿濆懍澹曢柣搴ゎ潐濞测晝寰婃禒瀣剁稏婵犻潧顑愰弫鍥煟閺冨洤鐓愰柛鐐差槹缁绘繈鎮介棃娑楁勃闂佹悶鍔岄悥濂稿极鐎ｎ喗鈷戠紓浣股戠亸浼存煕閵娿儲鍋ョ€殿喖顭锋俊鎼佸Ψ閵忊槅娼旀繝娈垮枟椤ㄥ懎螞濞嗘帒鈧挳姊?""
        try:
            payload = {
                "model": getattr(vision_config, "model", ""),
                "temperature": getattr(vision_config, "temperature", 0.0),
                "vision_mode": "description_only_v1",
                "duplicate_detection": getattr(vision_config, "duplicate_detection_enabled", True),
                "similarity_threshold": getattr(vision_config, "similarity_threshold", 0.99),
                "person_subject_filter_enabled": getattr(vision_config, "person_subject_filter_enabled", True),
                "person_mask_area_threshold": getattr(vision_config, "person_mask_area_threshold", 0.3),
                "person_mask_binary_threshold": getattr(vision_config, "person_mask_binary_threshold", 0.5),
                "person_mask_high_conf_threshold": getattr(vision_config, "person_mask_high_conf_threshold", 0.8),
                "person_mask_high_conf_min_area": getattr(vision_config, "person_mask_high_conf_min_area", 0.08),
                "person_prefilter_force_include_patterns": getattr(
                    vision_config, "person_prefilter_force_include_patterns", []
                ),
                "person_model_selection": getattr(vision_config, "person_model_selection", 1),
            }
            raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
            return fast_digest_text(raw)
        except Exception as e:
            logger.warning(f"Failed to build cache signature: {e}")
            return ""

    def _load_persistent_cache(self):
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊峰ù鍥х暦閸偅鍙忛柡澶嬪殮濞差亜鐓涢柛婊€鐒﹂弲顏堟偡濠婂嫬鐏村┑锛勬暬楠炲洭寮剁捄銊モ偓鐐差渻閵堝棗鍧婇柛瀣崌閹顫濋澶嬓ч梺闈涙搐鐎氫即鐛幒妤€骞㈡俊鐐村劤椤ユ艾鈹戦悩鍨毄濠殿噮鍙冨畷鎴﹀箻缂佹鍘电紓鍌欓檷閸ㄥ綊寮稿☉姘辩＜闁绘娅曠欢鏌ユ婢舵劖鐓ユ繝闈涙－濡牊淇婇锝呯伌闁哄矉缍侀弫鎰板礋椤撶姷鍘梻浣告惈閺堫剟鎯勯姘煎殨濞寸姴顑呮儫閻熸粌绉归崺娑㈠醇閵忋垻锛濇繛杈剧悼閹虫捇寮幆褉鏀介柍銉ㄦ珪閸犳﹢鏌涢埞鎯т壕婵＄偑鍊栫敮濠囨嚄閸洖鐓€闁哄洢鍨虹€氬懘姊洪鈧粔鐢告偂濞戙垺鍊甸柨婵嗙凹缁ㄥ鏌￠崱娆忎户缂佽鲸甯″畷鎺戔槈濡槒鐧侀梻浣虹帛閹尖晠宕滃▎鎾寸畳闂佽鍑界紞鍡樼濞嗘劗鐝?
        # 2) 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厯闂佸憡娲﹂崢钘夌暦閸欏绡€闂傚牊渚楅崕鎰版煕鐎ｎ倖鎴犳崲濞戙垹骞㈡俊顖濇娴犲吋淇婇悙棰濇綈濠电偛锕濠氭偄绾拌鲸鏅梺绯曞墲椤ㄥ懘顢旈埡鍛厓鐟滄粓宕滃▎鎾崇柈闁秆勵殔閻撯€愁熆閼搁潧濮囩紒鐘电帛閵囧嫰寮介妸褏鍙濇繝銏ｎ潐濞茬喎顫忕紒妯诲闁硅偐鍋涙禒顕€姊虹悰鈥充壕闂佸憡顨堥崕鎰版倿閼恒儳绠鹃柛鈩兩戠亸顓犵磼閻樺磭澧ǎ鍥э躬婵″爼宕ㄩ鍏碱仩缂傚倷璁查崑鎾愁熆閼搁潧濮堥柣鎾跺枑娣囧﹪顢涘鍐ㄤ粯闂佸憡鏌ㄩ鍥╂閹烘挻濯寸€瑰嫭婢樼粊顕€姊?hash_cache闂?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘偓渚€寮介‖顒佹⒒瀵板﹪宕?闂傚倸鍊峰ù鍥х暦閻㈢绐楅柟鎵閸嬶繝寮堕崼姘珔缂佽翰鍊曡灃闁挎繂鎳庨弳鐐烘煕?+ HashCacheManager.load_results闂?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堟偂閻斿吋鐓熸俊顖氭惈鐢娀鏌ら崜韫偗闁哄本鐩顒€鈻庨幆褍澹庢俊銈囧Х閸嬬偤宕归幐搴㈠弿闁逞屽墴閺屾洟宕煎┑鍥ф濡炪倧瀵岄崳锝咁潖濞差亝鍊烽柦妯侯槸婵洟姊洪崨濠冨鞍闁艰鍎冲畵鍕節閻㈤潧校缁炬澘绉归幃?Vision AI 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸婂潡鏌ㄩ弴鐐测偓鍝ョ不閺夊簱鏀介柣妯虹－椤ｆ煡鏌涙繝鍛【妞ゎ厼娼￠幊婊堟濞戞﹩娼旈梻浣告惈閹峰宕滈悢鐓庣畺婵せ鍋撻柟顔界懇閹虫粌鈻撻幐搴濠电姷鏁搁崑娑㈠箠瀹€鍕９婵犻潧顑呯粻鏍ㄤ繆椤栨繃纭堕柣銈傚亾闂備礁鎼崐钘夆枖閺囩喎顕辩€光偓閸曨兘鎷洪梻鍌氱墐閺呮繈宕氭导瀛樼厵婵炴潙顑傞崑鎾诲箛娴ｅ憡顔傞梻浣告啞濞诧箓宕戦幒妤€瑙﹂悗锝庡枟閻撴洘銇勯幇顔夹㈤柣蹇ｄ簼閵囧嫰寮捄銊愌勬叏婵犲倹鎯堥悡銈囩磽娴ｅ顏堝煝韫囨稒鈷戦柛娑橈攻閻撱儵鏌ㄩ弴銊ら偗鐎规洘妞介崺鈧?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻傞锝団偓?self._persistent_cache_path or not self._persistent_cache_path.exists()
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻堝鍫曞礆閻?_cache_signature and meta.get('signature') != self._cache_signature
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻堝鍫曞礆閻?_hash_cache and items
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鎲搁悧鍫濈瑨缂佺姳鍗抽弻鐔兼⒒鐎电濡介梺绋款儍閸婃繈寮婚弴鐔虹闁绘劦鍓氶悵鏂库攽閳藉棗浜濋柨鏇樺灲瀵鈽夐姀鐘栥劍銇勯弽顐沪妞ゅ骸绉撮—鍐Χ閸℃顫堢紓渚囧枟閻熲晛顕ｆ繝姘╅柕澶堝灪椤秴鈹戦悙鍙夘棡闁挎艾鈹戦敍鍕儓ta.signature闂?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲㈤崹鍦不閻樿櫕鍙忔俊鐐额嚙娴滈箖鎮楃憴鍕婵炶尙鍠栭悰顔碱潨閳ь剟銆佸▎鎾村癄濠㈣泛顦伴惈蹇涙⒒閸屾瑨鍏岄柛妯犲洤绠烘繝濠傜墑閳ь剙鍊圭粋鎺斺偓锝庝簽閿涙盯姊虹憴鍕姢妞ゆ洦鍙冮幃鐑芥偡閹佃櫕鏂€闂佺粯蓱瑜板啴顢旈锔界厽婵炴垼椴搁鐖€lf._cache_signature闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟犻崑鎾垛偓鍨緲鐎氫即骞冮悙鍝勭疅闁逞呮儗._hash_cache闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟犻崑鎾垛偓鍨緲鐎氫即骞冮悙鍝勭疅闁逞呮儗._persistent_cache_path闂?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧悿顕€鏌ｅΔ鈧悧濠囧矗韫囨拋褰掑礂閸忚偐绋囬梺?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧悿顕€鏌ｅΔ鈧悧鍡涘垂閺冨牊鍊甸梻鍫熺⊕閸熺偞銇勯锝嗙缂佺粯鐩畷鍗炍熼崫鍕垫綍婵＄偑鍊愰弲婵嬪礂濮椻偓瀵鈽夊Ο閿嬵潔闂佸憡顨堥崑鐐烘倶瀹ュ洨纾藉ù锝夌細濡炬悂鏌涢妸銉у煟鐎殿喖顭烽弫鎾绘偐閼碱剨绱叉繝鐢靛仦閸ㄥ爼鎮疯瀹曟瑩鏁撻悩鏂ユ嫼闂佸憡绺块崕杈ㄧ閿曞倹鐓欓柛娑橈工閳绘洜鈧鍠栭…宄扮暦婵傜唯闁靛绨堕弲婵嬪箟濮濆瞼鐤€闁哄倽娉曠粣鐐烘⒑閸涘﹥澶勯柛銊╀憾瀹曟劙宕归銈囶啎闂佺懓顕崑鐔煎箠閺囥垺鐓曢煫鍥风到婢ф壆绱掓潏銊ョ瑲婵炵厧绻樻俊鎼佸Ψ閵夛附鍤堟繝?""
        if not self._persistent_cache_path or not self._persistent_cache_path.exists():
            return
        try:
            with open(self._persistent_cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            meta = data.get("meta", {})
            if self._cache_signature and meta.get("signature") != self._cache_signature:
                logger.info("Persistent cache signature mismatch, skip loading")
                return
            items = data.get("items", {})
            if self._hash_cache and items:
                self._hash_cache.load_results(items)
                logger.info(f"Persistent cache loaded: {len(items)} items")
        except Exception as e:
            logger.warning(f"Failed to load persistent cache: {e}")

    def _save_persistent_cache(self):
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 婵?hash_cache 闂傚倸鍊峰ù鍥敋瑜嶉湁闁绘垼妫勯弸渚€鏌熼梻瀵割槮闁稿被鍔庨幉鎼佸棘鐠恒劍娈鹃梺姹囧灩婢瑰﹪寮崶顒佺厽婵☆垰鍚嬮弳鈺呮煥濞戞瑧鐭掓慨濠呮閹叉挳宕熼銏犘戞俊鐐€栧ú锕傚储閻ｅ瞼鐭夌€广儱顦介弫宥夋煟閹邦剦鍤熼柛?
        # 2) 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫绋侀崢浠嬪磻閿熺姵鐓忓璺烘濞呭懘鏌ｉ鐕佹疁闁哄本鐩崺鍕礃椤忎焦顫嶉梺璇插閼归箖藝娴兼潙桅闁告洦鍨扮粻鎶芥煕閳╁啨浠﹀瑙勬礃缁绘繈鎮介棃娴舵盯鏌?JSON 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮煡鏌涘☉鍙樼凹闁诲骸顭峰娲濞戞氨鐤勯梺绋匡攻椤ㄥ懘鎮鹃崹顐ょ懝闁逞屽墴瀵鈽夐姀鈺傛櫇闂佹寧绻傚Λ娑⑺囬妸銉富闁靛牆鎳橀妤冪磼婢跺﹦绉虹€殿喛顕ч埥澶愬閻樼數娼夐梻浣侯焾閺堫剟顢氶幘顔嘉ㄩ柨鏃囨〃缁ㄥ姊洪崷顓炲妺闁哄被鍔戝畷銏⑩偓鐢电《閸嬫挾鎲撮崟顒傦紭闂佺閰ｆ禍鍫曘€佸Ο鑽ら檮缂佸娉曢ˇ鏉款渻閵堝棙灏靛┑顔炬暬瀹曟澘鈽夐姀鈾€鎷绘繛杈剧到閹诧繝骞嗛崼鐔虹閻忕偛鍊搁埀顒佺箞楠炲啫煤椤忓嫀鈺呮煃鏉炵増绁伴柟鑺ユ礃缁绘繈鎮介棃娴躲垺绻涚仦鍌氣偓妤呮偖閹屽悑濠㈣泛顑囬崢鍗烆渻閵堝棗濮х紒鎻掑⒔缁牓宕橀鐣屽弳濠电偞鍨堕懝楣冨几濞戙垺鐓?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘偓渚€寮介‖顒佹⒒瀵板﹪宕?闂傚倸鍊烽懗鍫曞箠閹捐瑙﹂悗锝庝憾閻掕姤绻涢崱妯虹仸鐎规洘鐓￠弻鐔兼偋閸喓鍑￠梺鎼炲妼閸婂綊濡甸崟顔剧杸闁规崘娉涢。铏圭磽娴ｆ彃浜?+ 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮煡鏌涘☉鍙樼凹闁诲骸顭峰娲濞戞氨鐤勯梺鎼炲妼濠€閬嶅焵椤掆偓閻忔岸鎮￠敓鐘茶摕闁靛牆妫欓崣蹇涙煙闁箑鍘撮柛瀣尭铻栭柛娑卞幗濡差剟姊虹紒姗嗙劷缂侇噮鍨堕幃锟犲即閵忥紕鍘甸梻渚囧弿缁犳垿鐛鈧弻鐔煎礃閹绘帗娈婚梺鍝勬湰缁嬫垿鍩㈡惔銈囩杸闁哄啯鍨堕敍鍡椻攽?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堟偂閻斿吋鐓熸俊顖濇硶缁ㄦ寧銇勯弬娆炬█闁哄矉绻濆畷銊╊敊閸撗呮毉婵犵鈧啿绾ч柛鏃€鐟╁顐﹀磼閻愬鍙嗗銈嗙墬閻喗绔熼弴鐐╂斀闁绘劖娼欓悘锔姐亜韫囷絼閭い銏℃瀹曞爼濡烽妶鍡欐綎闂傚倷娴囬崑鎰板煕閸儱绀堟慨姗嗗墰閺嗭箑顭块懜闈涘闁绘挸绻愰埞鎴︽倷閼碱兛铏庢繝寰枫倖纭剁紒杈ㄥ浮閹晠宕崟顐ｅ劒缂?闂傚倸鍊峰ù鍥х暦閸偅鍙忛柛顭戝亞椤╂彃霉閻樺樊鍎忕紒鈧径鎰€甸柨婵嗛閺嬫稓绱掗悩宕囧弨闁哄本娲濈粻娑氣偓锝庝簴閸嬫捇寮撮悩鐢电劶婵犮垼鍩栭崝鏍偂閸愵喗鐓忓鑸电〒閻ｅ崬顭胯閸ㄨ鲸绌辨繝鍥х闁圭儤绻勯崥瀣⒑閸濆嫮鐒跨紓宥勭窔閺佹劙鎮欐笟顖涙櫌闂侀€炲苯澧ǎ鍥э躬楠炴牗鎷呴崷顓炲笚?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻傞锝団偓?self._persistent_cache_path or not self._hash_cache
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - 闂傚倸鍊峰ù鍥敋瑜嶉湁闁绘垼妫勯弸渚€鏌熼梻鎾闁逞屽厸閻掞妇鎹㈠┑瀣倞闁靛鍎冲Ο渚€姊绘担鍛婂暈婵炶绠撳畷褰掑箥椤斿墽鐒奸梺鍛婂姦閸犳鎮￠弴鐔虹闁瑰鍋熼幊鍕磽瀹ュ棗鐏撮柡灞剧洴瀵剛鎷犻幓鎺懶曢梻浣告惈閻寰婇崐鐔轰航闂備礁澹婇悡鍫ュ窗濮樿泛鍌ㄦい鎰堕檮閳锋垿姊洪銈呬粶闁兼椿鍨跺畷銏ゅ传閵壯咃紲闂傚鍓氳ぐ鍐焵椤掍緡娈旈崡閬嶆煙閹殿喖顣奸柛搴＄Т閳规垿宕掑鍛唹f._hash_cache, self._persistent_cache_path闂?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧悿顕€鏌ｅΔ鈧悧濠囧矗韫囨拋褰掑礂閸忚偐绋囬梺?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧悿顕€鏌ｅΔ鈧悧鍡涘垂閺冨牊鍊甸梻鍫熺⊕閸熺偞銇勯锝嗙缂佺粯鐩畷鍗炍熼崫鍕垫綍婵＄偑鍊愰弲婵嬪礂濮椻偓瀵鈽夊Ο閿嬵潔闂佸憡顨堥崑鐐烘倶瀹ュ洨纾藉ù锝夌細濡炬悂鏌涢妸銉у煟鐎殿喖顭烽弫鎾绘偐閼碱剨绱叉繝鐢靛Т閿曘倝宕板璺烘辈妞ゆ挾鍋愰弨浠嬫煟濮楀棗浜滃ù婊呭亾缁绘盯寮堕幋婵囧€┑鐐插悑閻熲晠鐛幋锕€閱囬柡鍥╁枔閸橀潧鈹戦悙鑼闁诲繑绻堝鎼佸Χ閸滀胶鍞甸柣鐘叉惈閹碱偊顢旈銏＄厵妞ゆ梻鐡斿▓鏃堟煃閽樺妲搁柍璇茬Ч閹煎綊顢曢姀顫礉闁诲孩顔栭崰妤呭箖閸屾氨鏆︽慨妞诲亾闁糕晪绻濆畷姗€鎮欓鍌樺亽闂傚倷娴囧畷鍨叏瀹曞洨鐭嗗ù锝夋交閼板潡寮堕崼姘珖闁活厼妫濋弻娑㈠焺閸愵亖濮囬梺?""
        if not self._persistent_cache_path or not self._hash_cache:
            return
        try:
            items = self._hash_cache.export_results()
            url_hash = ""
            try:
                url_hash = self._persistent_cache_path.parent.parent.name
            except Exception:
                url_hash = ""
            data = {
                "meta": {
                    "signature": self._cache_signature,
                    "url_hash": url_hash,
                    "updated_at": int(time.time())
                },
                "items": items
            }
            with open(self._persistent_cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Failed to save persistent cache: {e}")
    
    @property
    def enabled(self) -> bool:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊风粈渚€骞栭位鍥敃閿曗偓閻ょ偓绻濇繝鍌滃闁藉啰鍠栭弻鏇熺箾閸喖澹勫┑鐐叉▕娴滄粓宕橀埀顒€顪冮妶鍡楃仴鐟滄壆鍋涙晥婵°倕鎳忛埛鎴︽偡濞嗗繐顏╅柛鏂诲€濋弻娑㈡偄闁垮浠撮悹渚灦閺屾稑鈽夐崡鐐茬闂佸搫鍟悧濠囧磻閹扮増鐓犵痪鏉垮船婢т即鏌涢弮鎴濇珝婵﹦鍎ょ€电厧鈻庨幋鐘虫婵＄偑鍊х粻鎾愁焽瑜旈幃楣冩倻閽樺顔呴梺鑺ッ敍澶愬煛閸愨晛鏋戦柟鍏肩暘閸斿秹宕戦埡鍛拺妞ゆ巻鍋撶紒澶屾暬閹繝寮撮悢绋垮伎濠碘槅鍨抽崢褏鏁崼鏇熺厽闊洢鍎崇粔顕€鏌＄仦绋垮⒉闁瑰嘲鎳樺Λ鍐ㄢ槈閹烘垳澹曞┑鐘绘涧椤戝懐绮婚弽顓熺厓闁告繂瀚崳娲煕閵堝拋鍎戦柣銉邯閹囧春椤愵偂绨肩紒鍌氱Ч楠炲棝鈥栭垾宕囩Ш闁轰焦鍔欏畷鍫曟嚋閸偆妲ｉ梻?
        # 2) 婵犵數濮烽弫鎼佸磿閹寸姴绶ら柦妯侯棦濞差亝鏅滈柣鎰靛墮鎼村﹪姊洪崨濠傚Е濞存粍鐗犲畷鎴﹀箻鐠囨彃鐎銈嗗姧缂嶅棗螞閸愩剮鏃堟偐闂堟稐娌柣銏╁灙閸撴繃绌辨繝鍥ㄥ€婚柦妯猴級閵娾晜鐓忓璇″灠閹冲寮搁弽銊х瘈闁汇垽娼ф禒锕傛煕閵娿儳鍩ｆ鐐村姍楠炴﹢顢欓懖鈺嬬幢闂備胶鎳撴晶鐣屽垝椤栫偞鍋傞柣妯碱暯閸嬫捇鎮烽弶娆句痪婵犮垻鎳撳Λ婵嬬嵁濡ゅ懏鍋愮紓浣诡焽閸樻捇鎮峰鍕煉鐎规洘娲滅划娆忊枎閹勫€梻浣规偠閸庮噣寮查埡浣勶綁鏌嗗鍡欏幗闂佺粯鏌ㄩ幗婊堟儗鐏炵瓔鐔嗙憸搴ㄣ€冩繝鍥╁祦闁圭増婢樺婵嗏攽閻樻彃鏆為柣婵堝厴濮婅櫣绱掑鍡欏姺濡炪倧绲肩划娆忕暦閹达箑绠婚悹鍥у级瀹撳秴顪冮妶鍡樺暗闁稿锕㈤獮瀣暋閹锋梹妫冮幃鈺呮濞戞鎹曟繝鐢靛仜濡﹪宕㈡總鍛婂仼闁绘垼妫勯柋鍥煛閸モ晛浠掔紒鍗炵埣濮婃椽宕ㄦ繝鍐槱闂佸憡顭堝Λ鍕暰闁瑰吋鐣崹鑽ょ不?True闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁幒妤嬬稏婵犻潧顑呮儫闂侀潧顦崯顖氼渻閽樺鏆︾紒瀣嚦閺冨牆鐒垫い鎺戝€绘稉宥夋煙閹澘袚闁绘挸绻愰…璺ㄦ崉閾忕懓顣烘繝寰枫倖纭剁紒杈ㄥ浮閹晛鐣烽崶褍缁╅梻浣告惈閺堫剟鎯勯娑楃箚闁汇値鍨煎Σ缁樼箾鐎电校缂侇喖绉堕幑銏犫槈濮橈絽浜炬繛鎴烆仾椤忓懏鍙忓鑸靛姈閻撳啰鎲稿鍫濈婵犻潧顑戠紞鏍ь熆閼搁潧濮堥柛銈呰嫰铻栭柨婵嗘噹閺嗘瑩鏌ｉ幒鎴犱粵闁靛洤瀚伴獮鎺楀箣濠靛懐鏁栭梻浣侯焾濞撮攱绻涢埀顒勬煛?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顧€婵″洨绮绘ィ鍐╃厵閻庣數顭堟禒锔锯偓瑙勬礀鐎氼厽绌辨繝鍌ゆ富闁绘挸楠搁‖瀣磽娓氬洤娅橀柛娆忓暙閻ｇ兘骞掗幋顓犲弳闂侀€炲苯澧柍璁崇矙椤㈡棃宕奸悢鍝勫箞闂佽绻掗崑娑欐櫠娴犲鐓″璺虹灱绾惧ジ鏌ょ喊鍗炲闁搞倐鍋撻梻渚€娼уΛ娆戞暜閹烘缍栨繝闈涱儛閺佸洭鏌ｅ鍡楁灀闁稿鎹囬獮鎺楀籍閸屾粣绱叉俊鐐€栧褰掑磿閹惰棄绀夐悗锝庡墰绾惧ジ鎮楅敐搴濇喚婵☆垪鍋撻梻浣虹《閺備線宕戦幘鎰佹富闁靛牆妫楃粭鎺楁倵濮樼厧寮€规洘娲栭鍏煎緞鐎ｎ剙骞堥梻浣告惈濞层垽宕濈仦鍓ь洸闁绘劗鏁稿Λ顖炴煙椤栧棗鐬奸崥瀣⒑閸濆嫭婀扮紒瀣墱缁鈽夐姀鐘殿啋闂佺厧鎽滈弫鎼佸焵椤掑鏋涙い顏勫暣婵″爼宕卞Ο纭风喘闂備焦鎮堕崝蹇撯枍閿濆洤鍨濇繛鍡樺姉缁♀偓闂佸憡鍔戦崝搴ｇ玻閻愮儤鈷戦柛鎾村絻娴滄繈鏌ら崷顓炰壕缂?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧悿顕€鏌ｅΔ鈧悧濠囧矗韫囨拋褰掑礂閸忚偐绋囬梺?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - bool闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熼悜妯荤叆闁哄鐗忛埀顒€绠嶉崕閬嶅箟濮椻偓閺佹劖寰勬繝鍕靛晣濠电偠鎻徊楣冩偤閺団槅鏉洪梻鍌氬€烽悞锔锯偓绗涘厾鍝勵吋婢跺﹦锛涢梺鐟板⒔缁垶鎮″▎鎰╀簻闁哄秲鍔岄崫娲煕濞嗗繐顏柡宀€鍠栭、娆撴嚒閵堝洨鍘┑鐘殿暯閸撴繈骞冮崒鐐叉槬闁跨喓濮寸粈鍐煏婵炑冩噹楠炩偓闂傚倸鍊峰ù鍥х暦閸偅鍙忛柡澶嬪殮濞差亝鏅查柛銉㈡櫇閻掑吋绻涙潏鍓ф偧缁绢厼鐖煎绋款吋婢跺鍘甸梺缁樺灦閿曗晛鈻撻弮鈧?""
        return True
    
    def validate(
        self,
        image_path: str,
        ocr_text: str = "",
        skip_duplicate_check: bool = False,
    ) -> ConcreteKnowledgeResult:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厯闂佸憡娲﹂崢钘夌暦閸欏绡€闂傚牊渚楅崕鎰版煕鐎ｎ倖鎴犳崲濞戙垹骞㈡俊顖濐嚙绾板秹姊虹紒妯哄闁挎洦浜璇测槈閵忕姷顔掑┑掳鍊愰崑鎾绘煕閻樺啿鍝虹€殿喗濞婇、妤呭礋椤掑倸骞楅梺鐟板悑閻ｎ亪宕规繝姘厐闁哄洢鍨洪悡銉︽叏濮楀棗骞樼紒鈾€鍋撻梻渚€鈧偛鑻晶瀛樸亜閵忊剝顥堢€规洜鍘ч埞鎴﹀醇閵忊剝顔忛梻浣藉吹閸犳劗鍒掑鍥ㄥ床闁告洦鍊犻敐澶婄疀妞ゆ垼濮ら弬鈧梻浣稿閸嬩線宕规繝姘櫖婵犻潧顑嗛埛鎴︽煕濞戞﹫鍔熺紒鐘虫崌閹顫濋妷銉ヮ瀴缂備礁鍊哥粔鐢电紦娴犲宸濆┑鐘插楠炲牓姊绘担鍛婅础闁稿簺鍊濋獮鎰偅閸愩劎鐤囬梺鍛婁緱閸犳氨绮绘ィ鍐╃厵缂備焦锚缁楁岸鏌涚€ｃ劌鍔滄い銊ｅ劦閹瑩寮堕幋鐐剁檨濠电姷顣槐鏇㈠礂濡绻嗘慨婵嗙焾濡叉儳顪冮妶鍡樼叆妞わ箓娼ч～蹇撁洪鍕啇闂佺粯鍔栬ぐ鍐╂叏閵忋倖鈷戠紒瀣儥閸庡秹鏌涙繝鍐炬疁鐎规洘宀搁獮鎺懳旀繝鍐╂珫婵犵數濮撮敃銈団偓姘煎墴椤㈡挸鈽夐姀鈾€鎷洪梻鍌氱墛缁嬫挻鏅堕弴鐔虹閻忕偛鍊告俊濂告煃缂佹ɑ顥堝┑顔瑰亾闂侀潧鐗嗗Λ娑㈠储閹间焦鐓熼幖鎼灣缁夌敻鏌涚€ｎ亜顏╅柍缁樻崌閹晝绱掑Ο鐓庡笚?
        # 2) 闂傚倸鍊搁崐鎼佸磹妞嬪孩顐介柨鐔哄Т绾惧鏌涘☉鍗炴灓闁崇懓绉归弻褑绠涘鍏肩秷閻?pHash 濠电姷鏁告慨鐑姐€傞挊澹╋綁宕ㄩ弶鎴狅紱闂侀€炲苯澧撮柡灞剧〒閳ь剨缍嗛崑鍛焊閹殿喚纾肩紓浣诡焽濞插鈧鍠栭悥濂哥嵁鐎ｎ噮鏁囬柣鎰絻椤ユ捇姊婚崒娆戠獢婵炶壈宕靛濠冪鐎ｎ亞鐤呴梺鐐藉劜閸撴岸宕靛Δ鍛厱闁哄洢鍔屾禍浠嬫煕濮橆剦鍎旈柡灞剧☉閳藉宕￠悙瀵镐邯婵＄偑鍊ら崑鍛村箲閸パ屾綎婵炲樊浜滅粻浼村箹鏉堝墽宀涙俊鎻掔墦濮婃椽宕楅崗绗轰户闂佹悶鍔岄悥濂告偘椤旂晫绡€闁搞儜鍛Ф闂備礁鎲￠崝锔界閺嶎厼鍑犻柡宓本瀵岄梺闈涚墕濡瑧浜搁悽鍛婄厱閻庯綆鍋嗗ú鎾煙椤旀枻鑰块柟顔哄灪缁鸿姤寰勭仦鑺ユ毄闂備浇宕垫繛鈧紓鍌涜壘閳诲秹鏁愰崼婵冨亾閹绢喗鈷掑ù锝堝Г閵嗗啴姊婚崟顐ばх€规洘鍔曡灃闁告劏鏅╁鐔兼⒑鐟欏嫬绀冩い鏇嗗懐涓嶉柤濮愬€愰崑鎾舵喆閸曨剛顦ュ┑鐐额嚋缁犳捇鐛径鎰殐闁冲搫鍟伴敍婵嬫⒑缁嬫寧婀伴柣鐔濆洤绀夌€广儱顦伴悡娆戠磼鐎ｎ偒鍎ラ柣鎾炽偢閺岀喖顢欑紓瀛樺灥椤曘儵宕熼姘辩杸濡?
        # 3) 闂傚倸鍊风粈渚€骞栭位鍥敃閿曗偓閻ょ偓绻濇繝鍌涘櫧闁活厽鐟╅弻鈥愁吋鎼粹€崇闂侀€炲苯鍘哥紒鑸靛哺閻涱喚鈧綆鍠楅弲婵嬫煃瑜滈崜娑氬垝閸儱纾兼繛鎴炵墧缁ㄥ姊洪悷鐗堟儓婵☆偅顨嗙粋宥夊礈瑜忕壕钘壝归敐鍥ㄥ殌妞わ絾濞婇弻宥堫檨闁告挻姘ㄥ▎銏ゆ倷濞村顫嶉梺闈涚箳婵兘宕濋敃鈧—鍐Χ閸℃娼戦梺绋款儐閹瑰洤螞娴ｇ懓绶為柟閭﹀幖閳ь剛鏁婚弻銊モ攽閸℃ê娅ｅ銈忕到绾绢厾妲愰幒妤€鐒垫い鎺嶇缁剁偤鏌熼柇锕€骞橀柛妯哄船閳规垿鎮欓弶鎴犱桓闂佽崵鍠嗛崕鐢稿箖濡崵绡€闁搞儯鍔庨崢楣冩⒑閸撹尙鍘涢柛瀣閹鈧數纭堕崑鎾舵喆閸曨剛顦ㄩ梺鐓庣秺缁犳牠宕洪姀鈩冨劅闁靛鍎抽娲⒑缂佹ê濮嶆繛浣冲洤鍚归梺顒€绉甸埛鎺懨归敐鍥╂憘婵炲吋鍔曢湁婵犲ň鍋撶紒顔界懃閻ｇ兘寮撮姀鐘殿吋濡炪倖妫佸Λ鍕償婵犲倵鏀芥い鏃傜摂閻掗箖鏌涢埡鍌滃⒌鐎规洘鍨块崺锟犲磼濠婂拋鍟庨梻浣虹《閸撴繈鏁嬫繝娈垮枛濞差參寮婚敐澶婄闁规惌鍘鹃崥瀣⒑閸濆嫭婀伴柣鈺婂灡娣囧﹪鎮滈挊澹┭囨煕濞戝崬骞樼憸鐗堝灴濮?
        # 4) 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块弶鍫氭櫅閸ㄦ繃銇勯弽顐粶缂佲偓婢舵劖鐓涢柛銈呯埣椤ｏ箑效濡ゅ懏鈷戦柟鑲╁仜閸旀挳鏌涢幘瀵告噰闁诡喗鐟︾换婵嬪炊閵娧冨箥婵＄偑鍊栭悧鏇炍涘Δ鈧灋婵°倕鎳忛崑鍌炴煕瀹€鈧崑鐐哄煕閹达附鐓曟繛鎴烇公閸旂喎顭胯娴滎亪寮婚悢鍝勬瀳闁告鍋橀崰濠囨⒑鐠団€虫灀闁逞屽墯閺嬪ジ寮告惔銊︾厵闁诡垎鍛喖濠电偛鎳忛悧鐘诲蓟閿濆牏鐤€闁哄洨鍋樼划璺侯渻閵堝骸浜濇繛鍙夘焽缁顓奸崱娆撴闂佸憡绋戦敃銈嗙椤栫偞鈷戦悷娆忓椤ユ劙鏌￠崨顔炬创鐎规洘绻傞～婵嬫嚋閻㈤潧骞堟俊鐐€ら崢浠嬪垂閻㈠憡鍊堕柨鏃堟暜閸嬫挾鎲撮崟顒傤槰閻庡厜鍋撻柛娑橈梗缁诲棝鏌涢锝嗙缂佲偓鐎ｎ偁浜滈柡鍐ㄥ€藉鍛婁繆閹绘帗璐＄紒杈ㄦ崌瀹曟帒鈻庨幒鎴濆腐闂備焦鍎崇换鎰版煀閿濆懐鏆︾憸鐗堝笚閻掕偐鈧箍鍎遍幊搴ㄥ吹閹达附鈷戦柛娑橈工婵箑霉濠婂嫮澧电€规洘鍨块獮姗€寮妷锔绘綌婵犵數鍋涘Λ娆掓懌闂佹眹鍊撶欢姘潖濞差亝顥堟繛鎴炵懐濡偛顪冮妶蹇擃洭闁轰礁顭烽獮鍐╁閹碱厽鏅╅梺缁樺姦閸撴瑧澹曢鐐粹拺闁告稑锕︾粻鎾绘倵濮樷偓閸パ呯厬婵°倧绲介崯顖炲煕閹烘鐓曢悘鐐村礃婢规ɑ銇勮箛瀣姦闁哄本绋掔换婵嬪礃閵娧傜礉闂備胶纭堕弬渚€宕?
        # 5) Vision AI 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鍐蹭画濡炪倖鐗滈崑娑㈠垂閸岀偞鐓欓柟顖滃椤ュ鏌＄€ｂ晝绐旈柡灞炬礋瀹曠厧鈹戦幇顓夛妇绱撴笟鍥т簻缂佸缍婂濠氭偄绾拌鲸鏅┑鐐村灦閿曗晛螞閸℃せ鏀介柣鎰皺婢ф洟鏌ｉ弽褋鍋㈢€?_vision_validate_v3闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熸潏楣冩闁搞倖鍔栭妵鍕冀椤愵澀绮剁紓浣哄У閼归箖濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬値娼愭繛鍙夛耿閹虫繃銈ｉ崘銊у幒闁瑰吋鐣崝宀€绮婚幎鑺ョ厸闁搞儮鏅涢弸搴ㄦ煕?CV-only 婵犵數濮烽弫鎼佸磿閹寸姴绶ら柦妯侯棦濞差亝鏅滈柣鎰靛墮鎼村﹪姊虹粙璺ㄧ伇闁稿鍋ゅ畷鎴﹀煛閸愨晛鏋戦柟鑹版彧缁插潡鎯屽鑸电厱婵°倕鍟禍褰掓煛閳ь剚绂掔€ｎ偆鍘卞銈嗗姂閸婃洟寮搁幋锔界厽闁挎繂楠告晶瀛樻叏?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘偓渚€寮介鐐茬彉缂備礁宕崺鍖攁cheManager + OCR/闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮繈鏌嶈閸撶喖寮崘顔碱潊闁靛牆鎳愰鐓庮渻閵堝棙顥嗘俊顐㈠瀵悂宕掗悙绮规嫼闂佸憡绋戦敃銉﹀緞閸曨垱鐓曢悗锝庝簻椤忣參鏌?+ OpenCV 闂傚倸鍊搁崐鐑芥倿閿曞倸绠栭柛顐ｆ礀绾惧潡鏌＄仦璇插姎缁炬儳娼￠弻鐔煎箚閺夊晝鎾绘煕閵堝拋鍎忛棁澶愭煕韫囨挸鎮戠紓宥嗗灩缁辨帡鍩€椤掆偓閻ｆ繈宕熼鍌氬箞婵犵數濞€濞佳兾涘Δ鍜佹晜妞ゆ劧闄勯悡鏇熶繆椤栨艾鎮戦柡鍡╁墴閺屸€崇暆閳ь剟宕伴幘鑸殿潟闁圭儤鍤﹂悢鐓庝紶闁告洖鐏氱紞瀣⒑?+ VisionAIClient闂?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堝煕閹达附鐓曟繛鎴烇公瀹搞儵鏌ｈ箛瀣姢缂佽鲸甯炵槐鎺懳熼崫鍕垫綒闂備浇顕栭崹浼村疮閹绢喓鈧礁鈽夐姀鈥斥偓鐑芥倵閻㈡鐒鹃悽顖涙崌閺岋絾鎯旈敍鍕殯闂佺閰ｆ禍鍫曠嵁韫囨洍鍋撻敐搴樺亾椤撱劎绋荤紒缁樼箓椤繈顢橀悙鏉垮闂備浇顕ч崙鐣岀礊閸℃顩叉繝濠傜墕濮规煡鏌ｉ弬鍨倯闁绘挻绋戦湁闁挎繂顦板☉褔鏌嶈閸撴氨鎹㈤崼銉ョ畺濞村吋鎯岄弫瀣煃瑜滈崜鐔肩嵁婵犲偆鐓ラ柛顐ゅ櫏濡啫鈹戦悙鏉戠仸闁煎綊绠栭崺鈧い鎺嶇缁楁艾菐閸パ嶈含濠碘€崇埣瀹曘劑顢樺☉妯瑰閻熸粎澧楃敮妤呭疾椤忓牊鈷掑ù锝呮啞閸熺偤鏌ｉ悢鏉戝闁崇粯妫冨鎾閻樻鍞堕梻浣告贡閸庛倝銆冮崱娑樼柧妞ゆ帒瀚悡鏇㈡煛閸ャ儱濡兼鐐寸墱閳ь剚顔栭崰鎾诲礉閹达箑钃熼柨婵嗩槹閸嬫劙鏌涘▎蹇ｆЧ闁诡喗鐟ラ埞鎴﹀煡閸℃ぞ绨奸梺鐑╂櫓閸ㄥ爼鐛崘顭嬫椽顢斿鍡樻珖闂備焦瀵у濠氬疾椤愶箑绀夐柛顭戝亞缁♀偓闂侀潧楠忕徊鍓ф兜妤ｅ啯鐓ラ柡鍥埀顒佹礋閸┿垹顓奸崨顏呯€婚梺鐟邦嚟婵敻鏁嶈箛鎿冩富闁靛牆妫欑€垫瑩鏌涢幇銊︽珖濞存粠浜铏规嫚閸欏鏀銈庡亜椤︽壆鎹㈠☉娆戠瘈闁告剬鍛暰?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻傞锝団偓?os.path.exists(image_path)
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻堝鍫曞礆閻?_hash_cache 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁嶉崟顓犵厯闂佸湱鍎ら〃鍛村垂閸屾稓绡€闂傚牊渚楅崕鎰版煕閵堝懐澧﹂柡宀€鍠愬蹇斻偅閸愨晩鈧秹姊虹紒妯诲碍闁稿﹤顭烽崺鈧い鎺嗗亾缂佺姴绉瑰畷鏇㈡焼瀹ュ懐顔嗛柣搴秵閳ь剦鍙忕徊鍓ф崲濠靛棭娼╂い鎾跺仒缂?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻堝畷鑼姳婵傜棞ormula
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绮庡Σ鎰板磽椤＄ic_region is None
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻堝鍫曞礆閻?_vision_enabled and self._vision_client
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮煡鏌涘☉鍙樼凹闁诲骸顭峰娲濞戞氨鐤勯梺鎼炲妼濠€閬嶅焵椤掆偓閻忔岸鎮￠敓鐘茶摕闁靛牆妫欓崣蹇涙煙闁箑鍘撮柛瀣尭铻栭柛娑卞幗濡差剟姊虹紒姗嗙劷缂侇噮鍨堕幃锟犳偄闂€鎰畾濡炪倖鐗楀銊︾閵忋倖鐓熼柟鐐綑婵秹鏌＄仦鐣屝ユい褌绶氶弻娑滅疀閺冨倶鈧帗绻涢崱鎰伈濠碘€崇埣瀹曞爼鈥﹂幋鐐电◥闂傚倷娴囬惃顐﹀川椤愮喎浜鹃柛銏㈩嚢e_path 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍌氫壕婵ê宕崢瀵糕偓瑙勬礉椤鈧潧銈稿鍫曞箣閻樺灚姣庢繝鐢靛仩閹活亞绱為埀顒勬煏閸″繐浜鹃梻浣侯焾椤戝棝骞愰幖浣哥叀濠㈣泛艌閺嬪秵銇勯幘妤€鍊婚惄搴ㄦ⒑閹稿孩纾搁柛銊ょ矙閵嗕線寮崼婵嬪敹濠?
        # - 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鎲搁悧鍫濈瑨缂佺姳鍗抽弻鐔兼⒒鐎电濡介梺绋款儍閸婃繈寮婚弴鐔虹闁绘劦鍓氶悵鏇㈡⒑閹惰姤鏁遍柟鐟版喘瀵鈽夐姀鈺傛櫇闂佹寧绻傚ú銈夈€呴鍕拺缂佸顑欓崕鎰版煙閸涘﹤鍔ら柍钘夘樀瀹曞綊顢曢锛把囨煙閼圭増褰х紒鎻掔仢閻ｉ绮斿鍧檉._hash_cache.threshold闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熸潏楣冩闁稿顑夐悡顐﹀炊閵婏箑顎涢柣鐔哥懃鐎氼剛娆㈤悙鐑樺€甸柨婵嗙凹閹查箖鏌涢妶鍐ㄢ偓婵嗩潖閾忓湱纾兼俊顖濐嚙绾板秵淇婇妶鍥ｉ柟顔煎€块妴浣糕枎閹邦喚鐦堥梺鎼炲劘閸斿骞忔繝姘拺闁告稑锕ゆ慨鍌炴煕閺傚灝顒㈤柡鍛埣瀵挳濮€閿涘嫬甯楅柣鐔哥矋缁挸鐣峰鍐ｆ闁靛繒濮堥妷鈺傚€甸柨婵嗛婢ь垱銇勯锝嗙闁逛究鍔岃灒闁圭娴烽妴鎰板级?
        # - OCR/闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮繈鏌嶈閸撶喖寮崘顔碱潊闁炽儲鍓氶崵銈夋煟鎼淬垻鈯曢拑杈ㄧ箾閸涱喚澧柍瑙勫灴椤㈡瑩鎮惧畝鈧悾鐓庘攽閻愬弶鍣虹痪顓熸埧text 婵?_detect_math_formula 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸婂潡鏌ㄩ弴鐐测偓鍝ョ不閺夊簱鏀介柣妯虹－椤ｆ煡鏌涙繝鍛【妞ゎ厼娼￠幊婊堟濞戞﹩娼撻柡?
        # - 闂傚倸鍊搁崐鎼佸磹閻戣姤鍊块柨鏇楀亾妞ゎ厼鐏濊灒闁兼祴鏅濋悡瀣⒑閸撴彃浜濇繛鍙夛耿瀹曟垿顢旈崼鐔哄幈闂佹枼鏅涢崯浼村煀閺囥垺鐓曢悗锝庡亜婵秹鏌＄仦璇测偓婵嗙暦椤愶箑唯妞ゆ棁濮ら惁婊堟⒒娴ｄ警鐒炬い鎴濇楠炴劙鎳￠妶鍥╃暥闂佺粯鏌ㄩ幗婊呭姬閳ь剟姊洪崨濠冨闁告ɑ鍎抽悾椋庣矓濞撳獱ion_ai.enabled 婵?bearer_token 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍌氫壕婵ê宕崢瀵糕偓瑙勬礉椤鈧潧銈稿鍫曞箣閻樺灚姣庢繝鐢靛仦閹稿宕洪崘顔肩；闁瑰墽绮悡娆愩亜閺冨洤鍚归柣顓熺懇閺岀喖顢欓崫鍕紙閻庤娲忛崝宥囨崲濠靛绀冩い蹇撳缁嬪洭姊?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - image_path: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮煡鏌涘☉鍙樼凹闁诲骸顭峰娲濞戞氨鐤勯梺鎼炲妼閹碱偊鎮鹃柨瀣窞閻庯絻鍔嬬花濠氭⒑閸濆嫮袪闁告柨绉归幆鍐箣閻樼數锛滈梺褰掑亰閸犳牗绂掗柆宥嗙厸閻忕偠顕ф俊濂告煃閽樺妲搁摶锝夋煟閹惧磭宀搁柛娆欑節濮婃椽鎳￠妶鍛勃闂佸憡鍨电紞濠傜暦閹寸偟绡€闁稿本绮嶅▓楣冩⒑闂堚晛鐦滈柛妯绘倐瀹曟垿骞樼紒妯绘珳闂佸憡渚楅崣搴ㄥ汲閵忋垻纾藉ù锝囨嚀婵牓鏌嶉鍡╂r闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # - ocr_text: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮繈鏌嶈閸撶喖寮崘顔碱潊闁炽儲鍓氶崵銈夋⒑閸濆嫷妲归柛銊ョ秺钘濋柕濞炬櫆閳锋垿鏌涘☉姗堟缂佸爼浜堕弻娑樷枎韫囨稑寮伴悗瑙勬处閸ㄨ泛鐣烽崼鏇ㄦ晢濞达絽鎼铏節閻㈤潧浠﹂柛銊ョ埣閳ワ箓宕堕妸锔界彿闂侀潧绻堥崐鏍磻閻樿櫕鍙忔俊顖滃帶鐢泛霉濠婂嫬顥嬮柍褜鍓濋～澶娒洪弽褝鑰块梺顒€绉撮悞鍨亜閹烘埊鍔熼柛鎺撴緲椤儻顦叉繛鎾棑閸掓帡鏁愰崪浣圭稁濡炪伇鍛闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - ConcreteKnowledgeResult闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熸潏楣冩闁稿顑夐弻娑㈠焺閸愵亝鍠涢梺绋款儐閹瑰洤鐣疯ぐ鎺濇晝妞ゆ帒鍟犻崑鎾舵崉閵娿垹浜鹃悷娆忓绾炬悂鏌涙惔锝嗘毈闁靛棔绶氬鎾閻欌偓濞煎﹪姊虹紒姗嗙劸閻忓繑鐟ヨ灒濠电姴娲﹂埛鎴︽煠婵劕鈧洖鏆╅梻浣侯焾椤戝棝宕濆▎鎾虫槬闁逞屽墯閵囧嫰骞掗崱妞惧闂備礁鎽滈崰搴☆焽閿熺姴绠栨慨妞诲亾鐎殿喖顭锋俊鐑芥晝閳ь剟鍩€椤掑倹鏆柟顔煎槻閳诲氦绠涢幙鍐ф偅闂備胶鎳撻崥瀣箖閸岀偛钃熺€广儱鐗滃銊╂⒑缁嬭法绠查柣鈺婂灦楠炲﹤螖閸涱參鍞堕梺鍝勬川閸犲孩绂掓ィ鍐┾拺缂侇垱娲栨晶鍙夈亜椤撶偞鍠橀柛鈺冨仜椤撳吋寰勭€ｎ剙甯楅梻渚€娼чˇ顓㈠磿闁秴姹查弶鍫涘妸娴滄粓鏌￠崒姘殹闂婎剦鍓熼弻锛勪沪閻愵剛顦伴悗瑙勬礋娴滆泛顕ｉ幘顔肩伋闁告劖褰冮懙鎰版⒒娴ｈ棄鍚归柛瀣仱瀹曟瑨銇愰幒鎴狅紱婵犵數濮撮崐濠氬汲閿曞倹鐓曢柨鏃囶嚙楠炴劙鏌涚€ｎ偅宕岄柡浣瑰姍瀹曟﹢濡搁妷搴涘姂濮婅櫣绮欏▎鎯у壉闂佸湱顭堥…鐑界嵁韫囨稑宸濋悗娑櫭埀顒傜帛娣囧﹪顢涘┑鍕ㄥ亾閳ь剟鏌涚€ｎ偅宕岄柟顔界懇閹粌螣閻撳骸绠洪梻鍌欑窔濞佳呮崲閹烘挻鍙忛悗闈涙憸缁€濠偯归敐鍫燁仩缁炬儳鍚嬫穱濠囧Χ閸屾矮澹曢梻浣告惈閻楁粓宕滃☉姘灊婵娉涚涵鈧梺缁樺姀閺呮粓寮埀顒勬⒒娴ｇ顥忛柛瀣瀹曚即骞囬悧鍫濅患濡炪倖甯掔€氼參宕愰悽鍛婄厽闁归偊鍠楅崵鈧梺閫炲苯澧紒璇插濡叉劙骞橀幇浣瑰兊婵☆偆澧楃换鍐疾閳哄懏鈷戦柛娑橈功閳藉鏌ｆ幊閸旀垵顕ｉ弻銉晢闁逞屽墴閳ワ妇鎹勯妸锕€纾梺鎯х箳閹虫捇銆傞悽鍛娾拺?""
        if not os.path.exists(image_path):
            logger.warning(f"Image not found: {image_path}")
            return self._default_result(False)
        
        # 濠电姷顣藉Σ鍛村磻閹捐泛绶ゅù鐘差儏閻ゎ喗銇勯弽顐沪闁?Step 0: 闂傚倸鍊搁崐宄懊归崶顒婄稏濠㈣泛顑囬悵鍫曟煕閳╁啰鈽夌紒鎰殜閺岀喖宕滆鐢盯鏌涚€ｎ亜鈧潡寮婚妸鈺傚亜闁告繂瀚呴姀鐘嗗綊鎳栭埡浣叉瀰闂佸搫鏈惄顖炪€侀弴銏℃櫜闁搞儮鏅濋弳顓㈡⒒娴ｈ棄鍚归柛鐘冲姍閹囨偐瀹割喗缍庡┑鐐叉▕娴滄粎绮婚悽鍛婄厵闁绘垶蓱閻忔盯鏌曞娑㈩暒缁ㄥ姊洪幐搴㈢；缂佲偓娓氣偓閹啫煤椤忓懐鍘告繛鎾磋壘濞层倝寮搁敂濮愪簻闁挎洑鐒﹂悵顏堟煏閸パ冾伃濠碉紕鍏樻俊鐑芥晝閳ь剙鈻嶉敐澶嬧拻?(闂傚倸鍊搁崐鎼佲€﹂鍕；闁告洦鍊嬭ぐ鎺戠＜闁绘劘灏欓敍娑欑節閻㈤潧孝婵炲眰鍊濋幃鐐哄垂椤愮姳绨婚梺鍦劋閸ㄧ敻鍩€椤掍焦鍊愮€殿喚顭堥埢搴ㄥ箻鐎电骞楅梻浣筋潐閹矂宕圭捄渚€舵い鏃傛櫕缁犻箖鏌ｉ幘鍐茬槰闁绘捁鍋愰埀顒冾潐濞叉牠鎮ユ總绋挎槬闁跨喓濮寸壕鍏肩節瑜嶉悘婵囩濠婂嫮绡€闁汇垽娼ф禒褎銇勯妷锕€濮嶇€规洘鍨块獮鍥级閸喖浜堕柣鐔哥矋濡啴鎮?
        if self._hash_cache and not skip_duplicate_check:
            is_duplicate, cached_result = self._hash_cache.check_duplicate(image_path)
            if is_duplicate and cached_result:
                logger.info(f"Duplicate frame skipped (pHash): {Path(image_path).name}")
                # 闂傚倸鍊峰ù鍥敋瑜忛幑銏ゅ箛椤旇棄搴婇梺褰掑亰閸庨潧鈽夊Ο閿嬵潔濠殿喗锕╅崣搴☆焽閻斿吋鈷戦柟绋垮椤ュ牓鏌℃笟鍥ф珝鐎规洘鍨块獮姗€寮妷锔绘綌婵犵數鍋涘Λ娆撯€﹂崶顒€绀夌€瑰嫭澹嬮弨浠嬫煟閹邦垰鐨哄褎鐩弻娑㈠Ω閵壯呅ㄩ梺绯曟杹閸嬫挸顪冮妶鍡楃瑐闁煎啿鐖奸崺濠囧即閻旂繝绨婚梺闈涱焾閸庢煡宕甸崶鈹惧亾鐟欏嫭纾搁柛搴ｆ暬瀹曟椽鍩€椤掍降浜滈柟鐑樺灥椤忊晝绱掗悪娆忔处閻撶喖鏌熺€涙绠栨い銉︾矊椤儻顧侀柛銊﹀▕閸┾偓妞ゆ帒鍠氬鎰箾閸欏澧い鏇秮瀹曟粍鎷呴搹鍦幆?ConcreteKnowledgeResult
                if isinstance(cached_result, dict):
                    result = ConcreteKnowledgeResult(
                        has_concrete=cached_result.get("has_concrete", True),
                        has_formula=cached_result.get("has_formula", False),
                        confidence=cached_result.get("confidence", 0.9),
                        concrete_type=cached_result.get("concrete_type", "cached_result"),
                        reason=f"闂傚倸鍊搁崐鎼佸磹閻戣姤鍊块柨鏇氶檷娴滃綊鏌涢幇鍏哥敖闁活厽鎹囬弻鐔虹磼閵忕姵娈欓梺鎼炲労閸撴岸宕甸幋鐐簻闁瑰搫绉堕崝宥嗙箾?(pHash闂傚倸鍊搁崐鐑芥嚄閸洖纾块柣銏㈩焾閻ら箖鏌ｅΟ娆惧殭闁汇倝绠栭弻锛勪沪鐠囨彃濮庣紓浣哄閸ｏ綁寮诲鍫闂佸憡鎸鹃崰鏍偘?{self._hash_cache.threshold:.0%})",
                        is_mixed=cached_result.get("is_mixed", False),
                        non_text_ratio=cached_result.get("non_text_ratio", 0.0),
                        should_include=cached_result.get("should_include", True),
                        img_description=cached_result.get("img_description", cached_result.get("img_desription", ""))
                    )
                    return self._finalize_validation_result(image_path, result, cache_result=False)
                elif isinstance(cached_result, ConcreteKnowledgeResult):
                    return self._finalize_validation_result(image_path, cached_result, cache_result=False)
        
        # Step 1: 濠电姷鏁告慨鐑姐€傞挊澹╋綁宕ㄩ弶鎴狅紱闂侀€炲苯澧撮柡灞剧〒閳ь剨缍嗛崑鍛焊閹殿喚纾肩紓浣诡焽濞插鈧鍠栭悥濂哥嵁鐎ｎ噮鏁囬柣鎰絻椤ュ酣姊婚崒娆戭槮闁圭⒈鍋勮灋婵炴垯鍨圭粣妤呮煙閹规劦鍤欓柣鎺戠仛閵囧嫰骞掑鍥舵М闂佸摜濮靛ú婊呮閹烘鍋愰柛鎰皺娴煎矂鎮楃憴鍕┛缂傚秳绀侀锝夊箻椤旇棄鈧鏌涢埄鍐炬畷妞?(闂傚倸鍊搁崐鎼佸磹閹间礁纾圭紒瀣紩濞差亝鍋愰悹鍥皺閿涙盯姊洪悷鏉库挃缂侇噮鍨跺畷?conda whisper_env 婵犵數濮烽弫鎼佸磻閻愬搫鍨傞柛顐ｆ礀缁犲綊鏌嶉崫鍕櫣闁稿被鍔戦弻鐔碱敍閸″繐浜鹃梺?PaddleOCR)
        has_formula = self._detect_math_formula(ocr_text)
        if has_formula:
            logger.info(f"Formula detected in {Path(image_path).name}, including screenshot")
            result = ConcreteKnowledgeResult(
                has_concrete=True,
                has_formula=True,
                confidence=0.9,
                concrete_type="formula",
                reason="detected formula, include screenshot",
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=True
            )
            return self._finalize_validation_result(image_path, result, cache_result=True)
        
        # Step 2: Vision 婵犵數濮撮惀澶愬级鎼存挸浜炬俊銈勭劍閸欏繘鏌熺紒銏犳灍闁稿孩顨呴妴鎺戭潩閿濆懍澹曢梻浣筋嚃閸垶鎮為敃鈧銉╁礋椤栨氨鐤€濡炪倖鎸鹃崰鎰版偟椤愶附鈷掑ù锝囶焾椤ュ繘鏌涚€ｎ亝鍣介柛鎺撳笚閹棃濡搁敃鈧禒顓炩攽閻樼粯娑фい鎴濇閹繝寮撮姀锛勫幍闂備緡鍙忕粻鎴炴櫠娴兼潙鍐€闁斥晛鍟扮弧鈧梺鍐茬殱閸嬫捇鏌涘☉鍗炴灍闁绘繍浜?_extract_graphic_region 婵犵數濮烽。钘壩ｉ崨鏉戠；闁告侗鍙庨悢鍡樹繆椤栨氨姣為柛瀣尭椤繈鎮℃惔锝勫寲闂備胶鎳撶粻宥夊垂閽樺鏆︽慨妞诲亾闁炽儻绠撻獮瀣偐閹绘帗杈堥梻鍌氬€风欢姘焽瑜旈幃褔宕卞▎鎰簥闂佸湱鍎ゅ濠氭儗?        if self._vision_enabled and self._vision_client:
            result = self._vision_validate_v3(image_path=image_path, graphic_region=None)
            return self._finalize_validation_result(image_path, result, cache_result=True)

        # Fallback: 闂?Vision API 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧悿顔姐亜閹板爼妾柛瀣樀閺屻倕霉鐎ｎ偅鐝旈梺鎼炲妽缁诲啴濡甸崟顖氬唨闁靛ě鍛帒濠电姵顔栭崹浼村Χ閹间礁钃熼柡鍥ュ灩闁卞洦绻濋棃娑欙紞濞村皷鍋撶紓鍌氬€烽懗鑸垫叏瀹曞洤鍨濇繛鍡樻尭閽冪喖鏌ㄥ┑鍡╂Ч闁哄懏鐓￠弻娑㈠焺閸愵亝鍣梺闈╂€ラ崶銊у幗闂侀潧绻嗛弲娑㈡倶闁秵鐓曢悗锝庝悍瀹搞儵鏌?        logger.info(f"Vision API disabled, including raw screenshot {Path(image_path).name}")
        text_page_desc = self._extract_text_page_description(image_path=image_path, ocr_text=ocr_text)
        result = ConcreteKnowledgeResult(
            has_concrete=True,
            has_formula=False,
            confidence=0.6,
            concrete_type="formula",
            reason="detected formula, include screenshot",
            is_mixed=False,
            non_text_ratio=0.0,
            should_include=True,
            img_description=text_page_desc or "raw screenshot kept",
        )
        return self._finalize_validation_result(image_path, result, cache_result=True)

    def _finalize_validation_result(
        self,
        image_path: str,
        result: ConcreteKnowledgeResult,
        cache_result: bool = True,
    ) -> ConcreteKnowledgeResult:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉埀顒婄畵瀹曞ジ濡风€ｎ亝鍠樻俊顐㈠暙閳藉寮介妶鍛闂佸湱鍎ら〃鍛存煁閸ヮ剚鐓涢柛銉㈡櫅娴犙勩亜閿斿搫濡奸柍瑙勫灴閹瑩宕ｆ径妯活棧缂傚倷鑳剁划顖炲礉閺囥埄鏁嬮柨婵嗩槸缁狀噣鏌ら幁鎺戝姢闁告﹢浜跺Λ鍛搭敃閵忊€愁槱濠电偛寮堕敃銏ゅ箖閻愬搫閿ゆ俊銈勮兌閸欏啴姊洪崫鍕檨闁告劕褰炵槐鏃堟煟鎼淬値娼愭繛鍙壝悾婵嬪川婵犱胶绠氶梺鍓插亝濞叉牕顔忓┑鍥ヤ簻闁哄秲鍔庨埊鏇㈡煟閿旀儳浠х紒杈ㄦ崌瀹曟帒顫濋钘変壕闁归棿绀佺壕鍦喐閻楀牆绗掔紒鐘卞嵆閺岀喖姊荤€电濡介梺绋款儍閸婃繈寮婚弴鐔虹闁绘劦鍓氶悵鏇㈡⒑閸濆嫭濯奸柛鎾跺枛楠炲啫顫滈埀顒勫箖濞嗘垶宕夐弶鐐靛閻繒绱撻崒娆掑厡濠殿喖纾崚鎺戔枎閹惧磭鐣洪悷婊呭鐢帡鎮欐繝鍥ㄧ叆闁哄倸鐏濋埛鏃堟煟濞戞牕鐏叉慨濠冩そ濡啴鍩￠崘銊ョ厒闂備浇顫夌粊鎾焵椤掍礁澧柛銈嗩殜閺屾盯寮撮妸銉よ埅闂佸憡鑹鹃澶愬蓟濞戞ǚ妲堥柛妤冨仦閻忓牆顪冮妶搴′簼闁规瓕宕甸幑銏犫攽鐎ｎ亶娼婇梺鎸庣箓濡盯濡撮幇顔剧＝濞达絽澹婂Σ鎼佹煟閺嶎偄甯舵い鏇秮椤㈡洟鏁冮埀顒€娲块梻浣规偠閸庮噣寮插┑瀣亗闁瑰墽绮埛鎴︽煕濞戞﹫宸ュ┑顔肩焸閹绠涢弮鍌涘櫚閻庢鍠涘▍鏇犫偓浣冨亹閳ь剚绋掗…鍥储?
        # 2) 闂傚倸鍊风粈渚€骞栭位鍥敃閿曗偓閻ょ偓绻濇繝鍌滃闁藉啰鍠栭弻鏇熺箾閸喖澹勫┑鐐叉▕娴滄粓宕橀埀顒€顪冮妶鍡樺暗闁稿鐩畷鎴濐吋婢跺鎷虹紓鍌欑劍钃遍柣鎾卞劜閵囧嫰濡搁妷鈺娾偓妤併亜椤撶偞鎼愰摶鏍煕濞戝崬鏋ら柛姗€浜跺娲偡閻楀牊鍎撳銈嗗灥閹虫﹢宕洪埀顒併亜閹哄秶鍔嶇紒鈧€ｎ喗鐓欐い鏇楀亾缂佺姵鐗曢悾宄邦煥閸♀晜鞋缂備胶鍋撻崕鎶藉Χ閹间礁钃熼柣鏃傚帶缁犳氨鎲稿鍫濆惞闁绘棁顔栭悷閭︾叆閹艰揪绱曟禒顖滅磽娴ｄ粙鍝洪柟绋款煼楠炲繘宕ㄩ弶鎴炲祶濡炪倖鎸鹃崰搴♀枍濞嗘挻鈷掑ù锝呮贡濠€浠嬫煕閺傚潡鍙勭€规洑鍗冲浠嬵敇閻愮數鏆┑鐐差嚟婵挳顢栭崨顔煎姅闂傚倷鐒︾€笛冾潖婵犳艾鐤炬い鎰剁畱閸屻劑鏌ｉ姀鐘冲暈闁抽攱鍨块弻鐔虹矙閹稿孩宕抽梺瀹犳椤︻垶鍩為幋锕€纾兼繛鎴炵憿閹疯崵绱撴笟鍥ф珮闁告妫勯銉╁礋椤栨氨鐤€濡炪倖甯掗崑鍡涘触鐎ｎ喗鐓熼柣鏂挎憸閻鏌涢妸銉﹀仴濠碘€崇摠缁楃喖鍩€椤掑嫮宓侀悗锝庡枟閸婇攱绻涢崼鐔奉嚋缂佷緡鍋婂娲嚒閵堝憛锛勭磼閳ь剚鎷呴崫鍕垫濡炪倖鎸炬慨鐑芥儗閹剧粯鐓曠憸搴ㄣ€冮崱娑欏亗闁绘棃鏅茬换鍡涙煟閹邦喗鏆╅悽顖涚⊕缁绘盯宕ㄩ鐔锋灎闂佸搫鐬奸崰鏍€佸▎鎴炲厹闁汇値鍨伴幆鍫ユ煟鎼淬値娼愭繛鍙壝悾婵嬪箹娴ｅ摜鐣洪梺鍐叉惈閸熸壆澹曢崗闂寸箚妞ゆ牗绮岀敮鍓佺磽瀹ュ拋妯€婵?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘茬疇婵犻潧娲㈤崑鍛存煕韫囷絽鐏犳繛宸幖椤曪絾绻濆顑┿劑鏌ㄩ弴妤€浜炬繛?_cache_result闂?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堝煕閹寸姷妫い鎾跺仦閸ｈ櫣鐥銏㈢暫闁硅櫕绻傞悾婵嬪礋椤掑倸甯楅柣鐔哥矋缁挸鐣峰鍐炬僵闁告鍋涢悘濠囨倵閸忓浜鹃梺鍛婃处閸嬪嫰鎮楅銏♀拺濞村吋鐟ч崚鏉款熆鐠虹儤鍠樼€规洘鍨块獮姗€鎳滈棃娑欑€梻浣告啞濞诧箓宕滃☉妯锋灁闁靛牆娲ㄧ壕钘夈€掑顒佹悙闁哄妫冮弻娑㈠籍閳ь剟宕归悽鍓叉晪闁挎繂娲ㄩ惌娆撳箹鐎涙ɑ灏伴柡浣哄█濮婄粯鎷呯拠鈥冲妼闂佹悶鍔庢晶妤冪矉瀹ュ鍊锋い鎺戝€婚惁鍫ユ⒒閸屾氨澧涚紒瀣浮瀹曡櫕绂掔€ｎ偆鍘介梺缁樻⒐濞兼瑩鎮橀弻銉︾厸閻忕偟鏅晥婵犵绱曢崗姗€寮崒鐐茬鐟滃繘宕㈤幋锔解拻濞达絽鎲￠幆鍫熴亜閹存繃鍤囩€规洦鍨堕、娑橆煥閸涱垳鍔归梻浣告贡閸庛倝銆冮崨顖滅焼闁割偅绶疯ぐ鎺撴櫜闁割偒鍋呯紞鍫濃攽閻愬弶鍣烽柛銊ㄦ椤繐煤椤忓嫬绐涙繝鐢靛Т鐎氱兘鎮橀崡鐐╂斀闁宠棄妫楁禍婊堟倵濮橆偄宓嗛柣娑卞枟缁绘繈宕惰閼板灝鈹戦悙鏉戠仸闁挎岸鏌ｅ┑鍥疯€挎慨濠呮缁辨帒螣閻戔晜瀚荤紓鍌欐閻掞箓骞愰崘鑼殾闁硅揪绠戠粻濠氭煛閸屾ê鍔滈柡鍌楀亾婵犵數濮烽弫鎼佸磿閹达附鈷旈柛鏇ㄥ幗閺嗘粓鏌ㄩ悢鍝勑㈢紒鐘靛█閺岀喖宕滆鐢盯鏌￠崨顔剧煉闁哄被鍊曢湁閻庯綆鍋呴悵鏍ь渻閵堝繐鐦滈柛锝忕到椤繒绱掑Ο鑲╂嚌闂佹悶鍎滈崘鐐氦闂傚倷娴囬褏鎹㈤幋鐘冲床闁割偅绻勯弳锕傛煏韫囧鈧洖娲垮┑鐘灱濞夋盯鏁冮敃鍋瑰洭顢橀悜鍡樺瘜闂侀潧鐗嗗Λ娑欐櫠椤掑嫭鐓熼柣鏃€绻傞幊鎰版儗濞嗗繆鏀介柣妯哄级婢跺嫰鏌ｉ幘璺烘瀾闁靛洤瀚伴、鏇㈩敃閵忕媭娼诲┑鐘殿暜缁辨洟宕㈣閸╃偤骞嬮敃鈧壕鍏肩箾閹寸偠澹橀柍宄扮焸閹鎲撮崟顒€顦╅梺绋款儏鐎氫即銆佸Ο鑽ら檮缂佸娉曢ˇ鏉款渻閵堝棙灏靛┑顔炬暬瀹曨垳鈧稒顭囩弧鈧梺姹囧灲濞佳勭閿曞倹鐓欓柟闂磋兌閻ｆ椽鏌嶉妷顖滅暤鐎规洖銈告俊鐑藉Ψ閵夈儰鎲鹃梻浣藉吹閸犳劙鎮烽妷褉鍋撳鐓庡籍鐎规洘娲熼幃銏ゅ礂閼测晛骞愰梻浣规偠閸庮垶宕曢柆宥嗗€舵い蹇撶墛閻撴瑧鈧娲栧ú銈夊煝閸儲鐓欐い鏃€顑欏鎰磼濡ゅ啫鏋涙い銏＄☉椤繈宕ｅΟ鍏煎礋闂?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悢婵嗘搐缁犵儤銇勮濠€鐞眅_result
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕ｆ繝姘╅柕澶堝灪椤秴鈹戦悙鍙夘棡闁挎艾鈹戦敍鍕┾偓鍢篻e_path闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟崐鍦偓姘煼閺岋綁鎮╅崹顐㈢５ult闂?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - image_path: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮煡鏌涘☉鍙樼凹闁诲骸顭峰娲濞戞氨鐤勯梺鎼炲妼閹碱偊鎮鹃柨瀣窞閻庯絻鍔嬬花濠氭⒑閸濆嫮袪闁告柨绉归幆鍐箣閻樼數锛滈梺褰掑亰閸犳牗绂掗柆宥嗙厸閻忕偠顕ф俊濂告煃閽樺妲搁摶锝夋煟閹惧磭宀搁柛娆欑節濮婃椽鎳￠妶鍛勃闂佸憡鍨电紞濠傜暦閹寸偟绡€闁稿本绮嶅▓楣冩⒑闂堚晛鐦滈柛妯绘倐瀹曟垿骞樼紒妯绘珳闂佸憡渚楅崣搴ㄥ汲閵忋垻纾藉ù锝囨嚀婵牓鏌嶉鍡╂r闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # - result: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁嶉崟顒佹闂佹悶鍎洪崜娆戝瑜版帗鐓涢柛銉ｅ劚閻忣亪鏌涚€Ｑ勬珚闁哄本鐩獮妯何旈埀顒勫箠閹扮増鏅繝濠傜墛閳锋垿鎮峰▎蹇擃仼闁告柣鍊栭妵鍕即閵娿儱绠虹紓浣稿€哥粔褰掋€佸☉銏″€烽悗鐢殿焾瀵櫕绻濋悽闈涗沪闁搞劌鐖奸垾锕傚炊閵婏附鐝烽梺闈涚箞閸婃牠宕戦悩铏弿婵☆垳鍘х敮璺好瑰鍕棆闁逞屽墲椤煤閺嵮嶈€块梺顒€绉撮悞鍨亜閹烘埊鍔熼柛鎺撴緲椤儻顦叉繛鎾棑閸掓帡鏁愰崪浣圭闂佺粯甯╅幗鐥梤eteKnowledgeResult闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # - cache_result: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍌氫壕婵ê宕崢瀵糕偓瑙勬礉椤鈧潧銈稿鍫曞箣閻樺灚姣庢繝鐢靛仦閹稿宕洪崘顔肩；闁规儳鐏堥崑鎾舵喆閸曨剛顦ㄩ梺鎸庡哺閺屽秶鎷犻崣澶婃敪缂備胶濮甸惄顖炵嵁濮椻偓閹煎綊顢曢妷褍褰傜紓鍌氬€搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鎲搁悧鍫濈瑨缂佺姳鍗抽弻鐔兼⒒鐎电濡介梺绋款儍閸婃繈寮婚弴鐔虹闁绘劦鍓氶悵鏇㈡⒑閸濆嫭濯奸柛鎾跺枛瀵鈽夐姀鈺傛櫇闂佺粯顭堢亸娆愬閸パ屾富闁靛牆楠告禍鍓х磼閻樿櫕宕岀€殿喛顕ч埥澶愬閻樻牑鏅犻弻鏇熷緞濡儤鐏堥梺鍛婃煛閸嬫挾绱撻崒姘偓宄懊归崶銊ь洸妞ゆ巻鍋撻柍璇茬Т椤垽鎯冩稊鎾⒒閸屾艾鈧兘鎮為敂閿亾缁楁稑鎳愰惌娆撴煙濞喡ゎ嚙濞差參銆佸鈧慨鈧柣姗€娼ф慨?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - ConcreteKnowledgeResult闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熼悜姗嗘畷闁搞倕鐭傞弻娑㈠箻濡も偓鐎氼厼鈻撻弶娆炬富闁靛牆妫楁慨褏绱掗悩鍐茬仼缂侇喖鐗撳畷鍗炩槈濞嗘垵甯鹃梻浣稿閸嬪懐鎹㈤崘顔奸棷濞寸姴顑嗛悡娑㈡煕閳╁厾顏埶夌€ｎ剛纾奸弶鍫涘妽鐏忎即鏌熷畡鐗堝櫧闁瑰弶鎸冲畷鐔碱敃閿旈敮鍋撳ú顏呪拻濞达絿鐡旈崵娆戠磼缂佹ê濮嶉挊婵嬫煕椤垵浜楃紒?""
        if cache_result:
            self._cache_result(image_path, result)

        return result
    
    def _cache_result(self, image_path: str, result: ConcreteKnowledgeResult):
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊峰ù鍥敋瑜忛幑銏ゅ箛椤旇棄搴婇梺褰掑亰閸庨潧鈽夊Ο閿嬵潔濠殿喗锕╅崜娆撴倶婵犲洦鈷戦梻鍫熷崟閸儱鐤炬繛鎴欏灩缁€鍕煟濡櫣锛嶇紒鈾€鍋撳┑鐘垫暩婵挳宕愯ぐ鎹ゅ鎮欓悽鐢碉紲婵炴挻鑹惧ú銈呪枍瀹ュ鐓涚€光偓閳ь剟宕伴幘璇茬闁绘顕ч悞娲煕閹板吀绨介柣娑栧灲濮婃椽鎳￠妶鍛咃綁鏌涢弮鈧〃濠傜暦娴兼潙鍐€鐟滃繘寮抽敃鍌涚叆婵犻潧妫Ο鍫ユ煛娴ｅ摜校闁逛究鍔岃灒濠㈠墎顭堥幆鍫濐渻閵堝繒绋婚柣鎾偓鎰佹綎婵炲樊浜滃婵嗏攽閻樻彃鈧鎮烽幓鎺濇富闁靛牆鍟俊濂告煙閾忣偄濮嶉柛鈹惧亾濡炪倖鍨煎▔鏇犵矈閻戣姤鍊堕煫鍥ㄦ⒒閹冲洭鏌涢埞鎯т壕婵＄偑鍊栫敮濠囨嚄閸洖鐓€闁哄洨鍠嗘禍婊堟煏婵炲灝鍔滈柛銈呮川閳ь剝顫夊ú蹇涘磿閻㈢鏄ラ柍褜鍓氶妵鍕箳閹存繍浠鹃梺?
        # 2) 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫绋侀崢浠嬪磻閿熺姵鐓忓璺烘濞呭懘鏌?hash_cache 濠电姴鐥夐弶搴撳亾濡や焦鍙忛柣鎴ｆ绾惧鏌ｅΟ鑽ゃ偞闁哄鐗楃换娑㈠箣濞嗗繒浠肩紓浣哄У閻楃娀寮婚悢鍏煎€锋い鎺嗗亾濠⒀屽灡缁绘盯姊婚弶鎴濈ギ闂佸搫鏈ú妯侯嚗閸曨垰閱囨繝闈涙椤捇姊绘担铏瑰笡闁瑰憡鎮傚畷鎰板锤濡も偓閽冪喖鏌ｉ弬鍨倯闁抽攱鍔欓弻鐔兼焽閿曗偓瀵偓淇婇姘偓鑽ゆ閹惧瓨濯撮柛婵嗗珔閿濆鐓熼柣鏂垮级濞呭懘鏌ｉ敐鍥у幋闁诡喒鏅濋幏鐘诲箵閹烘垶鏆忓┑锛勫亼閸婃牠宕濋幋锕€纾归柡鍥ｆ嚍閸ヮ剙鐓涢柛娑卞枤閸樹粙鏌熼崗鑲╂殬闁搞劌顭烽崺濠囧即閵忊€虫閻熸粍妫冨?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘偓渚€寮介鐐茬彉缂備礁宕崺鍖攁cheManager.store_result + 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敂钘変罕濠电姴锕ら悧鍡欑矆閸喓绠鹃柛鈩冾殜閻涙粓鏌?JSON闂?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堟偂閻斿吋鐓熸俊顖氭惈鐢娀鏌ら崜韫偗闁哄本鐩顒傛崉閵娧冨П婵°倗濮烽崑娑欘殽閹间緤缍栨繝闈涱儛閺佸洭鏌ｉ幇顔芥毄闁绘侗鍣ｉ弻锝嗘償閿涘嫮鏆涢梺绋块瀹曨剛鍙呴梺鎸庢礀閸婄效閺屻儳鍙撻柛銉ｅ妿閳洟鏌ｉ幘瀛樼闁绘搩鍋婂畷鍫曞Ω閿旂虎妲伴梻浣虹帛鐢帒顭囬敓鐘茶摕闁挎洖鍊搁崘鈧悷婊冪Ч椤㈡棃顢橀悩顐壕閻熸瑥瀚粈鍐偓鍏夊亾闁告稑锕ｇ换鍡涙煕椤愶絾绀€鐎瑰憡绻冮妵鍕冀閵娧€濮囬梺浼欏閸嬨倕顫忕紒妯诲闁荤喖鍋婇崵瀣磽娴ｅ壊鍎愰柛銊ユ健楠炲﹪鎮㈢喊杈ㄦ櫖闂佺粯鍔栭悡鈥澄涢悜鑺モ拺闁诡垎鍛啋闂侀€炲苯澧紒瀣崌瀹曟粓濡搁埡鍌楁嫼缂佺虎鍘奸幊蹇氥亹瑜忕槐鎺楁偐閸愯尙浠撮梺瀹狀嚙缁夎鎱ㄩ埀顒勬煟濞嗗苯浜鹃梺缁樻煥濡繈骞冪憴鍕╁亰闁圭瀵掓禒鈺呮煕閻斿憡鍊愭慨濠傛惈鏁堥柛銉戝喚鐎烽梻浣告憸婵敻鎮ч悩鑽ゅ祦闁告劏鏅濋々鐑芥倵閿濆簶鍋撻娑欐珚闁哄被鍔岄埞鎴﹀幢濡炶浜鹃柛娑橈功椤?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻堝鍫曞礆閻?_hash_cache
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - 闂傚倸鍊峰ù鍥敋瑜嶉湁闁绘垼妫勯弸渚€鏌熼梻鎾闁逞屽厸閻掞妇鎹㈠┑瀣倞闁靛鍎冲Ο渚€姊绘担鍛婂暈婵炶绠撳畷褰掑箥椤斿墽鐒奸梺鍛婂姦閸犳鎮￠弴鐔虹闁瑰鍋熼幊鍕磽瀹ュ棗鐏撮柡灞剧洴瀵剛鎷犻幓鎺懶曢梻浣告惈閻寰婇崐鐔轰航闂備礁澹婇悡鍫ュ窗濮樿泛鍌ㄦい鎰堕檮閳锋垿姊洪銈呬粶闁兼椿鍨跺畷銏ゅ传閵壯咃紲闂傚鍓氳ぐ鍐焵椤掍緡娈旈崡閬嶆煙閹殿喖顣奸柛搴＄Т閳规垿宕掑鍛唹f._hash_cache闂?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - image_path: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮煡鏌涘☉鍙樼凹闁诲骸顭峰娲濞戞氨鐤勯梺鎼炲妼閹碱偊鎮鹃柨瀣窞閻庯絻鍔嬬花濠氭⒑閸濆嫮袪闁告柨绉归幆鍐箣閻樼數锛滈梺褰掑亰閸犳牗绂掗柆宥嗙厸閻忕偠顕ф俊濂告煃閽樺妲搁摶锝夋煟閹惧磭宀搁柛娆欑節濮婃椽鎳￠妶鍛勃闂佸憡鍨电紞濠傜暦閹寸偟绡€闁稿本绮嶅▓楣冩⒑闂堚晛鐦滈柛妯绘倐瀹曟垿骞樼紒妯绘珳闂佸憡渚楅崣搴ㄥ汲閵忋垻纾藉ù锝囨嚀婵牓鏌嶉鍡╂r闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # - result: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鎻掔€梺绋跨箰閸氬宕ｈ箛娑欑厪闁割偅绻嶅Σ鍛婃叏鐟欏嫮鍙€闁哄矉缍佸顕€宕掑顑跨帛缂傚倷璁查崑鎾绘煕瀹€鈧崑鐐烘偂韫囨搩鐔嗛悹楦挎婢ф洟鏌涢弮鈧喊宥嗙┍婵犲浂鏁冮柨婵嗙箺閳ь剙娼￠弻锛勪沪鐠囨彃顬堥梺瀹犳椤︻垵鐏掗梺缁樻尭缁ㄥ爼宕ｉ敓鐘斥拺闁煎鍊曟禒锕傛煕閹存繄绉虹€规洘鍨剁换婵嬪磼濠婂嫭顔曢梻渚€娼荤€靛矂宕㈤幖浣哥；闁瑰墽绮弲鏌ユ煕濞戝崬寮鹃柡鍡愬灮缁辨挻鎷呴悷鏉款潔濡炪們鍩勫Σ妾攃reteKnowledgeResult闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧悿顕€鏌ｅΔ鈧悧鍡涘垂閺冨牊鍊甸梻鍫熺⊕閸熺偞銇勯锝嗙缂佺粯鐩畷鍗炍熼崫鍕垫綍婵＄偑鍊愰弲婵嬪礂濮椻偓瀵鈽夊Ο閿嬵潔闂佸憡顨堥崑鐐烘倶瀹ュ洨纾藉ù锝夌細濡炬悂鏌涢妸銉у煟鐎殿喖顭烽弫鎾绘偐閼碱剨绱叉繝鐢靛仦閸ㄥ爼鏁冮埡浣勬盯宕卞▎鎴狅紳婵炶揪绲块幊鎾诲极閹€鏀介柍銉ㄦ珪閸犳﹢鏌涢埞鎯т壕婵＄偑鍊栫敮濠囨嚄閸洖鐓€闁哄洢鍨洪悡銉╂煛閸ャ儱濡煎ù婊呭仦娣囧﹪宕ｆ径濠傤潓濡炪値鍋呯换鍕箲閸曨剚濯?""
        if self._hash_cache:
            result_dict = {
                "has_concrete": result.has_concrete,
                "has_formula": result.has_formula,
                "confidence": result.confidence,
                "concrete_type": result.concrete_type,
                "is_mixed": result.is_mixed,
                "non_text_ratio": result.non_text_ratio,
                "should_include": result.should_include,
                "img_description": result.img_description,
                "img_desription": result.img_description
            }
            self._hash_cache.store_result(image_path, result_dict)
            self._save_persistent_cache()

    def _vision_flag_to_bool(self, value: Any) -> bool:
        """Docstring omitted."""
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value > 0
        token = str(value or "").strip().lower()
        return token in {"true", "1", "yes", "y"}

    def _build_result_from_vision_payload(self, api_result: Dict[str, Any]) -> ConcreteKnowledgeResult:
        """Docstring omitted."""
        payload = dict(api_result or {})
        if "raw_response" in payload and "has_concrete_knowledge" not in payload:
            raw = str(payload.get("raw_response") or "")
            clean_raw = raw.replace("```json", "").replace("```", "").strip()
            try:
                parsed = json.loads(clean_raw)
                if isinstance(parsed, dict):
                    payload.update(parsed)
            except Exception:
                pass

        try:
            confidence = float(payload.get("confidence", 1.0))
        except Exception:
            confidence = 1.0
        concrete_type = str(payload.get("concrete_type", "visual_description") or "visual_description")
        reason = str(payload.get("reason", "Vision AI description") or "Vision AI description")
        reason_lower = reason.lower()
        prefilter_source = str(payload.get("prefilter_source", "") or "").strip()
        person_prefilter_hit = (
            bool(prefilter_source)
            or concrete_type.strip().lower() == "person_subject"
            or "person_subject_prefilter" in reason_lower
            or "person-subject prefilter" in reason_lower
            or "预过滤" in reason
        )
        has_concrete_flag = self._vision_flag_to_bool(
            payload.get("has_concrete_knowledge", payload.get("has_concrete", True))
        )
        should_include_flag = self._vision_flag_to_bool(payload.get("should_include", True))
        img_description = str(
            payload.get("img_description")
            or payload.get("img_desription")
            or payload.get("description")
            or payload.get("caption")
            or payload.get("raw_response")
            or reason
            or ""
        ).strip()
        if not img_description:
            img_description = "description unavailable"
        return ConcreteKnowledgeResult(
            has_concrete=has_concrete_flag if person_prefilter_hit else True,
            has_formula=False,
            confidence=confidence,
            concrete_type=concrete_type,
            reason=reason,
            is_mixed=False,
            non_text_ratio=0.0,
            should_include=should_include_flag if person_prefilter_hit else True,
            img_description=img_description,
        )

    def _build_structured_group_prompt(
        self,
        *,
        parent_image_path: str,
        parent_image_size: Optional[Tuple[int, int]],
        normalized_items: List[Dict[str, Any]],
        ocr_text: str = "",
    ) -> str:
        """Build layout-aware prompt for multi-crop images from one parent screenshot."""
        prompt_lines: List[str] = [
            "You are analyzing multiple cropped images extracted from the SAME original screenshot.",
            "Use bbox layout metadata to understand each crop's relative position in the original screenshot.",
            "Coordinate origin is top-left; bbox format is [x1, y1, x2, y2].",
        ]
        if parent_image_path:
            prompt_lines.append(f"parent_image_path={parent_image_path}")
        if parent_image_size:
            prompt_lines.append(f"parent_image_size=[{int(parent_image_size[0])}, {int(parent_image_size[1])}]")

        prompt_lines.append("inputs_in_order:")
        for idx, item in enumerate(normalized_items, start=1):
            group_type = str(item.get("group_type", "") or "unknown")
            bbox_xyxy = item.get("bbox_xyxy")
            bbox_norm = item.get("bbox_normalized_xyxy")

            bbox_text = "[]"
            if isinstance(bbox_xyxy, (list, tuple)) and len(bbox_xyxy) == 4:
                try:
                    bbox_text = f"[{int(bbox_xyxy[0])}, {int(bbox_xyxy[1])}, {int(bbox_xyxy[2])}, {int(bbox_xyxy[3])}]"
                except Exception:
                    bbox_text = str(list(bbox_xyxy))

            bbox_norm_text = "[]"
            if isinstance(bbox_norm, (list, tuple)) and len(bbox_norm) == 4:
                try:
                    bbox_norm_text = (
                        f"[{float(bbox_norm[0]):.4f}, {float(bbox_norm[1]):.4f}, "
                        f"{float(bbox_norm[2]):.4f}, {float(bbox_norm[3]):.4f}]"
                    )
                except Exception:
                    bbox_norm_text = str(list(bbox_norm))

            prompt_lines.append(
                f"{idx}. group_type={group_type}; bbox_xyxy={bbox_text}; bbox_normalized_xyxy={bbox_norm_text}"
            )

        ocr_hint = str(ocr_text or "").strip()
        if ocr_hint:
            prompt_lines.append(f"ocr_hint={ocr_hint[:600]}")

        prompt_lines.extend(
            [
                f"Return ONLY JSON array with exactly {len(normalized_items)} objects in the same order.",
                "Each object must contain: {\"img_description\":\"...\"}",
                "Do not output markdown.",
            ]
        )
        return "\n".join(prompt_lines)

    def validate_structured_group(
        self,
        *,
        parent_image_path: str,
        items: List[Dict[str, Any]],
        ocr_text: str = "",
    ) -> List[ConcreteKnowledgeResult]:
        """Validate multiple structured crops from one parent screenshot in a single Vision call."""
        if not items:
            return []
        if not (self._vision_enabled and self._vision_client):
            return [self._default_result(True) for _ in items]

        indexed_payloads: List[Tuple[int, Dict[str, Any], str]] = []
        final_results: List[ConcreteKnowledgeResult] = [self._default_result(True) for _ in items]
        for idx, payload in enumerate(items):
            image_path = str(payload.get("image_path", "") or "").strip()
            if not image_path or not os.path.exists(image_path):
                continue
            indexed_payloads.append((idx, payload, image_path))

        if not indexed_payloads:
            return final_results

        parent_image_size: Optional[Tuple[int, int]] = None
        first_payload = indexed_payloads[0][1]
        raw_parent_size = first_payload.get("parent_image_size")
        if isinstance(raw_parent_size, (list, tuple)) and len(raw_parent_size) == 2:
            try:
                parent_image_size = (int(raw_parent_size[0]), int(raw_parent_size[1]))
            except Exception:
                parent_image_size = None
        if parent_image_size is None and parent_image_path and os.path.exists(parent_image_path):
            try:
                parent_img = cv2.imread(parent_image_path)
                if parent_img is not None and parent_img.size > 0:
                    parent_h, parent_w = parent_img.shape[:2]
                    parent_image_size = (int(parent_w), int(parent_h))
            except Exception:
                parent_image_size = None

        normalized_items: List[Dict[str, Any]] = [payload for _, payload, _ in indexed_payloads]
        prompt = self._build_structured_group_prompt(
            parent_image_path=parent_image_path,
            parent_image_size=parent_image_size,
            normalized_items=normalized_items,
            ocr_text=ocr_text,
        )

        image_paths = [image_path for _, _payload, image_path in indexed_payloads]
        batch_size = max(1, len(image_paths))
        config_obj = getattr(self._vision_client, "config", None)
        prev_batch_enabled = getattr(config_obj, "batch_enabled", None) if config_obj is not None else None
        prev_batch_max_size = getattr(config_obj, "batch_max_size", None) if config_obj is not None else None

        try:
            if config_obj is not None and prev_batch_enabled is not None:
                config_obj.batch_enabled = True
            if config_obj is not None and prev_batch_max_size is not None:
                config_obj.batch_max_size = max(int(prev_batch_max_size or 1), batch_size)

            api_results = llm_gateway.vision_validate_images_sync(
                image_paths=image_paths,
                prompt=prompt,
                system_prompt=self._concrete_knowledge_system_prompt,
                client=self._vision_client,
                max_batch_size=batch_size,
                timeout=120,
            )
        finally:
            if config_obj is not None and prev_batch_enabled is not None:
                config_obj.batch_enabled = prev_batch_enabled
            if config_obj is not None and prev_batch_max_size is not None:
                config_obj.batch_max_size = prev_batch_max_size

        if not isinstance(api_results, list):
            raise RuntimeError("Vision grouped validation returned non-list payload")
        if len(api_results) < len(indexed_payloads):
            api_results = list(api_results) + [{} for _ in range(len(indexed_payloads) - len(api_results))]
        elif len(api_results) > len(indexed_payloads):
            api_results = list(api_results)[: len(indexed_payloads)]

        for (origin_idx, _payload, image_path), api_result in zip(indexed_payloads, api_results):
            parsed_payload = api_result if isinstance(api_result, dict) else {"raw_response": str(api_result)}
            concrete_result = self._build_result_from_vision_payload(parsed_payload)
            final_results[origin_idx] = self._finalize_validation_result(
                image_path,
                concrete_result,
                cache_result=True,
            )
        return final_results

    def _validate_batch_with_vision_api(self, tasks: List[Dict]) -> Optional[List[ConcreteKnowledgeResult]]:
        """Docstring omitted."""
        # 闂?Vision 闂傚倸鍊搁崐椋庣矆娴ｈ櫣绀婂┑鐘插亞閻掔晫鎲歌箛鏇燁潟闁绘劕顕弧鈧梺鎼炲劀閸ヮ煉绱┑鐘垫暩閸嬫稑螣婵犲啰顩叉繝闈涱儐閸嬪倿鏌ㄥ┑鍡╂Ч闁绘挻鐟╅幃宄扳枎韫囨搩浠兼繝娈垮枛椤︾敻寮婚敐澶婃闁圭楠稿▓妤呮⒑閸濆嫮鐏遍柛鐘虫尵閸掓帡顢橀悙鈺傤潔濠碘槅鍨槐顔炬閻愮儤鈷戦悶娑掆偓鍏呭濠电偛顕慨鎾敄閸℃稒鍋傛繛鍡樻尰閻撶喖鏌曡箛鏇炐ュù鐘崇洴閺屾盯濡堕崒姘婵犵绱曢崑鎴﹀磹瑜旈幊娆掋亹閹烘垹鏌ч梺缁橆焾椤曆呯不閺嶎厽鐓曟い鎰剁稻缁€鈧Δ鐘靛亼閸ㄤ粙寮婚悢鍏煎殝妞ゆ巻鍋撳┑锛勫帶闇?
        # 婵犵數濮烽弫鍛婃叏娴兼潙鍨傛繛宸簻绾惧潡鏌ゅù瀣珔闁搞劍绻堥弻娑㈠箻濡も偓鐎氼剟寮搁崒鐐粹拺闁圭瀛╃粈鈧梺绋匡工閹芥粓鎮幆褜鍚嬪璺侯儑閸橀潧顪冮妶鍡欏缂佸鍨胯棢閻庯綆鍓涚壕濂告煟濡櫣锛嶉柛娆屽亾闂?None闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熸潏楣冩闁稿孩鏌ㄩ埞鎴﹀磼濠婂海鍔搁梺鍝勵儎缁舵艾顕ｉ崼鏇為唶婵﹩鍘介悵鏇㈡煟鎼淬垹鍤柛鎾跺枛瀵鈽夐姀鐘电杸闂佺绻愰幗婊堝礄瑜版帗鈷戦柛婵嗗椤ユ粎绱掔紒姗堣€跨€殿喖顭烽弫鎾绘偐閼碱剨绱叉繝鐢靛Т閿曘倗鈧凹鍓欓埢鎾朵沪鐟欙絾鏂€闂佺粯鍔栬ぐ鍐棯瑜旈弻锝呂旈崘銊㈡瀰閻庢鍠栭…宄扮暦閵娾晛绾ч柟瀵稿Т婵附淇婇悙顏勨偓鏍暜閹烘柡鍋撳鐓庡缂侇喖鐗撳畷鍗炩槈濞嗘垵骞楁繝寰锋澘鈧劙宕戦幘缁樼厽婵°倐鍋撻柨鏇ㄤ邯瀵偄顓奸崶锝呬壕闁挎繂楠告晶鎵棯閹佸仮闁哄瞼鍠撻幉鎾礋椤愩埄娼旂紓浣鸿檸閸樺ジ鎮ユ總绋胯摕鐎广儱顦伴崵鎺楁煏閸繃鍣瑰ù鐘筹耿閺屸剝鎷呴崫銉モ叺闂佸搫鐭夌徊鍊熺亽闂佸壊鐓堥崰姘掗姀銈嗏拺闁告繂瀚峰Σ椋庣磼椤旂晫鎳囩€殿喖顭烽弫鎰板椽娴ｅ搫寮抽梻浣虹《閸撴繈銆冮崼銉ョ闁绘垼濮ら埛?
        """Docstring omitted."""
        if not tasks:
            return []
        if not (self._vision_enabled and self._vision_client):
            return None
        if not getattr(self._vision_client.config, "batch_enabled", False):
            return None

        results: List[Optional[ConcreteKnowledgeResult]] = [None] * len(tasks)
        pending_for_vision: List[Tuple[int, str, str]] = []
        try:
            for idx, task in enumerate(tasks):
                image_path = str(task.get("image_path", "") or "")
                ocr_text = str(task.get("ocr_text", "") or "")
                if not image_path or not os.path.exists(image_path):
                    results[idx] = self._default_result(False)
                    continue

                if self._hash_cache:
                    is_duplicate, cached_result = self._hash_cache.check_duplicate(image_path)
                    if is_duplicate and cached_result:
                        if isinstance(cached_result, dict):
                            cached_obj = ConcreteKnowledgeResult(
                                has_concrete=bool(cached_result.get("has_concrete", True)),
                                has_formula=bool(cached_result.get("has_formula", False)),
                                confidence=float(cached_result.get("confidence", 0.9)),
                                concrete_type="formula",
                                reason="detected formula, include screenshot",
                                is_mixed=bool(cached_result.get("is_mixed", False)),
                                non_text_ratio=float(cached_result.get("non_text_ratio", 0.0)),
                                should_include=bool(cached_result.get("should_include", True)),
                                img_description=str(
                                    cached_result.get("img_description", cached_result.get("img_desription", ""))
                                ),
                            )
                            results[idx] = self._finalize_validation_result(
                                image_path, cached_obj, cache_result=False
                            )
                        elif isinstance(cached_result, ConcreteKnowledgeResult):
                            results[idx] = self._finalize_validation_result(
                                image_path, cached_result, cache_result=False
                            )
                        else:
                            results[idx] = self._default_result(True)
                        continue

                has_formula = self._detect_math_formula(ocr_text)
                if has_formula:
                    results[idx] = self._finalize_validation_result(
                        image_path,
                        ConcreteKnowledgeResult(
                            has_concrete=True,
                            has_formula=True,
                            confidence=0.9,
                            concrete_type="formula",
                            reason="detected formula, include screenshot",
                            is_mixed=False,
                            non_text_ratio=0.0,
                            should_include=True,
                        ),
                        cache_result=True,
                    )
                    continue

                pending_for_vision.append((idx, image_path, image_path))

            if pending_for_vision:
                api_results = llm_gateway.vision_validate_images_sync(
                    image_paths=[item[2] for item in pending_for_vision],
                    prompt="",
                    system_prompt=self._concrete_knowledge_system_prompt,
                    client=self._vision_client,
                    max_batch_size=getattr(self._vision_client.config, "batch_max_size", None),
                    timeout=120,
                )
                if len(api_results) != len(pending_for_vision):
                    raise RuntimeError(
                        f"Vision batch response size mismatch: expected={len(pending_for_vision)}, got={len(api_results)}"
                    )

                for (idx, cache_image_path, _request_path), api_result in zip(pending_for_vision, api_results):
                    concrete_result = self._build_result_from_vision_payload(
                        api_result if isinstance(api_result, dict) else {"raw_response": str(api_result)}
                    )
                    results[idx] = self._finalize_validation_result(cache_image_path, concrete_result, cache_result=True)

            finalized: List[ConcreteKnowledgeResult] = []
            for idx, value in enumerate(results):
                if isinstance(value, ConcreteKnowledgeResult):
                    finalized.append(value)
                else:
                    fallback_path = str(tasks[idx].get("image_path", "") or "")
                    fallback_result = self._default_result(True)
                    if fallback_path:
                        fallback_result = self._finalize_validation_result(
                            fallback_path,
                            fallback_result,
                            cache_result=False,
                        )
                    finalized.append(fallback_result)
            return finalized

        except Exception as e:
            logger.warning(f"Vision batch validation failed, fallback to thread pool path: {e}")
            return None
    
    def _vision_validate_v3(
        self, image_path: str, graphic_region: Optional[np.ndarray] = None
    ) -> ConcreteKnowledgeResult:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?        1) 闂傚倸鍊搁崐鐑芥嚄閸洖纾块柣銏㈩焾閻ょ偓绻涢幋娆忕仾闁稿鍊濋弻鏇熺箾瑜嶇€氼厼鈻撴导瀛樼厽閹兼番鍨婚埊鏇㈡嫅閸楃偐鏀芥い鏍ㄧ⊕鐏忥箓鏌熼鑲╃Ш妤犵偘绶氶獮鎺楀箣濠垫劗鈧挳姊绘担鍛婃儓婵☆偅顨堥幑銏狀潨閳ь剙顕ｇ拠娴嬫婵☆垱绮庨崰鏍箖閳╁啯鍎熼柕蹇婃噰閸嬫挾鎷犲ù瀣杸闂佺粯鍔樼亸娆撳箺閻樼偨浜滄い鎾跺仦閸犳ɑ顨ラ悙鑼妞ゃ垺娲熼弫鍐焵椤掑倻鐭嗛柛鎰ㄦ杺娴滄粓鐓崶銊﹀鞍闁革絿鍎ょ换?VisionAIClient.validate_image闂?        2) 闂傚倸鍊峰ù鍥х暦閻㈢绐楅柟鎵閸嬶繝寮堕崼姘珔缂佽翰鍊曡灃闁挎繂鎳庨弳鐐烘煕?has_concrete_knowledge/confidence/concrete_type/reason 濠电姴鐥夐弶搴撳亾濡や焦鍙忛柣鎴ｆ绾剧粯绻涢幋鐐垫噭婵炲懐濞€楠炴牕菐椤掆偓婵″ジ鏌＄€ｂ晝绐旈柡宀€鍠栭獮鎴﹀箛闂堟稒顔勭紓鍌欒兌婵绱炴笟鈧濠氭偄閸忚偐鍔烽梺鎸庢磵閸嬫捇鏌＄€ｃ劌鈧牜鎹㈠☉娆忕窞闁割偅绻勬导鍫ユ⒑閸濆嫮鐒跨紒杈ㄦ礋閸┾偓妞ゆ帒锕︾粔鐢告煕鐎ｎ亜顏╅柍缁樻崌閹晝绱掑Ο鐓庡笚?        3) 闂傚倷娴囬褏鈧稈鏅犻、娆撳冀椤撶偟鐛ラ梺鍝勭▉閸樿偐澹曡ぐ鎺撶厵闂傚倸顕崝宥夋煕鐎ｎ亶妯€闁哄本鐩、鏇㈡晲閸℃瑯妲紓鍌欑贰閸嬪嫮绮旇ぐ鎺戣摕闁绘梻鈷堥弫宥嗙箾閹寸儑渚涙俊顐㈡缁辨挻鎷呴崫鍕戯綁鏌ｉ幙鍕瘈鐎殿喛顕ч埥澶愬閻樻牓鍔庨幉绋款吋閸℃瑯娴勯梺鍦劋濮婅崵澹曢懖鈺冪＝濞达綀鍋傞幋锔藉剭闁硅揪闄勯悡鍐喐濠婂牆绀堟繛鎴欏灩缁犵娀鏌熼崜褏甯涢柛瀣儔閻擃偊宕堕妸锔绢槬闂佹悶鍔嶇换鍫ュ蓟閺囩喓绠鹃柣鎰靛墯閻濇棃姊洪崨濠傜仼濠殿喚鏁搁幑銏犫攽婵犲孩些缂傚倷闄嶉崝鎴炵鐠鸿櫣鏆?        闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘偓浣肝旈崨顐熷亾閸涙潙绠い鈺佸储ateway.vision_validate_image_sync闂?        闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧锛忛崨顖ょ闯濠电偠鎻徊鑺ョ珶婵犲伣锝夊川婵炲じ绨诲銈嗘尵閸嬫稑危婵犳碍鐓?CV 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槱閻熸粎澧楃敮鎺楀垂閸岀偞鐓欑€瑰嫭濯介～锕€霉濠婂啰绉洪柡灞剧〒娴狅箓骞嗚濮ｅ牓姊洪幖鐐测偓鏍洪悢鍏煎亗妞ゆ劧绠戦悙濠囨煏婵炲灝鈧鎹㈡笟鈧娲传閸曨剦妫″┑鐐跺皺閸犲酣顢氶敐澶婇唶闁哄洨鍋熼崢鍛婄箾鏉堝墽鎮奸柟铏崌钘濋柍鍝勬噺閳锋垹鐥鐐村櫧闁割偒浜弻娑欑節閸愵亞鐤勯悗娈垮櫘閸嬪嫰顢樻總绋垮窛妞ゆ牗绋掗悾鐑芥⒒娴ｅ憡鍟炴繛璇ч檮缁傚秹鎮欓崹顐綗濠殿喗顭堥崺鏍煕閹寸姷纾奸悗锝庡幗绾墎绱掗悩鍐差棆缂佽鲸甯楀鍕沪閽樺绠ｆ俊鐐€戦崹鍝勭暆閸涘﹣绻嗛柤鎼佹涧缁剁偛鈹戦悩鎻掝伌婵″弶鎸冲缁樼瑹閳ь剙顭囪閹囧幢濞嗗苯浜炬慨妯煎帶楠炴绱掔€ｎ亶妯€濠殿喒鍋撻梺闈涚墕濡盯宕㈤柆宥嗏拺闂傚牊渚楀Σ鍫曟煕婵犲啯绀堥柍褜鍓氶懝楣冩煀閿濆钃?        闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?        - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻傞悾锟犳儉閻?is_running()
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻堝畷鑼姳婵傜棗oncrete闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熸潏楣冩闁稿顑夐弻娑樷槈閸楃偟浠╅梺鎼炲妽缁诲牓鐛弽顬ュ酣顢楅埀顒佷繆閼测晝纾奸柍褜鍓熷畷鎺楁倷鐎电骞楅梻渚€娼ч悧鍡橆殽閹间焦鍊堕柕澶嗘櫆閻撴稑顭跨捄渚Ш闁活厽鐟╅弻鈥崇暆閳ь剟宕伴弽顓犲祦闁哄稁鍙庨弫鍥ㄧ節闂堟稓澧㈡繛澶嬪缁绘繈鎮介棃娑楃捕闂佸鏉垮闁圭瓔鍋嗙槐鎾存媴濮濆苯顏悗瑙勬处閸撶喖宕洪悙鍝勭闁挎洍鍋撻梺鍗炴喘閺岋綁寮幐搴㈠枑闂佸搫妫欏畝绋款潖?        婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - 闂傚倸鍊风粈渚€骞栭位鍥敃閿曗偓閻ょ偓绻濇繝鍌滃缂佲偓婢跺鍙忔俊鐐额嚙娴滈箖姊洪棃娑欘棛缂佽埖宀搁悰顔锯偓锝庡枟閺呮粓鏌﹀Ο渚Х婵顨婂缁樻媴閸濆嫬浠橀梺纭呭Г缁捇濡撮崒娑氼浄閻庯綆浜為敍娑㈡⒑鐟欏嫬鍔ゆい鏇ㄥ弮閹兘鎮烽幍铏杸闂佺粯蓱瑜板啴顢旈锔界厽婵炴垼椴搁悵顤箉ncio 婵犵數濮烽弫鎼佸磻濞戙垺鍋ら柕濞у啫鐏婇悗鍏夊亾闁告洖鐏氶弲鐐烘⒑閸涘﹥澶勯柛瀣у亾闂佸搫顑呴柊锝夊蓟閺囷紕鐤€闁哄洨鍎愰埀顒€鐭傞弻娑㈠Χ閸♀晜鐣烽梺闈涙搐鐎氼垳绮诲☉銏犵闁圭⒈鍘介ˉ锝囩磽閸屾瑨鍏屽┑顕€娼ч悾婵嬪箹娴ｆ瓕鎽曢梺璺ㄥ枔婵绮堢€ｎ喗鈷掗柛顐ゅ枔閵嗘帒霉閻橆偅娅婃慨濠呮閹风娀鍨惧畷鍥︽埛缂傚倷娴囬崺鏍嚌妤ｅ啯鍋╅柣鎴ｅГ閸婂鏌﹀Ο鐚寸礆闁冲搫鎳忛悡蹇撯攽閻愭垵鍟刊濂告煕濮橆剦鍎旀慨濠冩そ閹筹繝濡堕崨顒佸媰缂傚倷绀侀鍡涘垂瑜版帒绠栨い鏇楀亾妞ゃ垺娲熼弫鍐焵椤掑嫭鍊?
        # - API 闂傚倸鍊峰ù鍥敋瑜忛埀顒佺▓閺呮繄鍒掑▎鎾崇婵＄偛鐨烽崑鎾诲礃椤旂厧鑰垮┑鐐村灱妞存悂寮查埡鍛€甸柛蹇擃槸娴滈箖姊洪崨濠冨闁告挻鑹鹃埢宥夊冀瑜夐弨鑺ャ亜閺冨倹娅曞┑鈥虫喘閺岋繝宕卞Δ浣规珗_concrete_knowledge闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟悧鍡欑不閹烘鐓熼柣妯活問閻撶idence闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟悧鍡欑不閹烘鐓熼柣妯活問閻撶rete_type闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟崐鍦偓姘煼閺岋綁鎮╅崹顐㈢毆son闂?        闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?        - image_path: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮煡鏌涘☉鍙樼凹闁诲骸顭峰娲濞戞氨鐤勯梺鎼炲妼閹碱偊鎮鹃柨瀣窞閻庯絻鍔嬬花濠氭⒑閸濆嫮袪闁告柨绉归幆鍐箣閻樼數锛滈梺褰掑亰閸犳牗绂掗柆宥嗙厸閻忕偠顕ф俊濂告煃閽樺妲搁摶锝夋煟閹惧磭宀搁柛娆欑節濮婃椽鎳￠妶鍛勃闂佸憡鍨电紞濠傜暦閹寸偟绡€闁稿本绮嶅▓楣冩⒑闂堚晛鐦滈柛妯绘倐瀹曟垿骞樼紒妯绘珳闂佸憡渚楅崣搴ㄥ汲閵忋垻纾藉ù锝囨嚀婵牓鏌嶉鍡╂r闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?        - graphic_region: 闂傚倸鍊搁崐鐑芥嚄閸洍鈧箓宕奸姀鈥冲簥闂佸湱鍎ら〃鍛村磼閵娧勫枑闁哄啫鐗勯埀顑跨閳诲酣骞樺畷鍥╂澑闂備礁鎼ˇ鍐测枖閺囥垺鍋傞柟杈鹃檮閳锋垹绱掔€ｎ偄顕滄繝鈧幍顔剧＜閻庯綆鍋勭粭鎺楁煃缂佹ɑ宕岀€规洖銈稿鎾偄閸濆嫬姹插┑鐘垫暩閸嬬偤宕归崼鏇椻偓锕傚醇閳垛晛浜炬慨姗嗗亜瀹撳棝鏌＄仦鍓ф创妤犵偛顑夊顒勫垂椤旇瀚熺紓鍌氬€风粈渚€顢栭崨姝ゅ洭顢涢悜鍡樻櫍婵犻潧鍊婚…鍫濐啅濠靛洢浜滈柟鎹愭硾濞呭繑淇婇銈呮瀻闁宠鍨块幃娆撳级閹寸姳妗撻梻浣烘嚀閹芥粓鈥﹂崼銉稏婵犻潧顑嗛弲婵嬫煕鐏炶鈧牠寮查鍕垫富闁靛牆妫楅崸濠囨煕鐎ｎ偅宕岀€殿噮鍋婂畷濂稿Ψ閿旀儳骞堥梺璇茬箳閸嬫稒鏅舵禒瀣ラ柟鐑橆殕閻撳啰鎲稿鍫濈闁挎洖鍊搁崹鍌炴煕椤愶絾绀€闁藉啰鍠栭弻娑樷槈濡吋鎲奸梺绋款儐閸旀牠濡甸崟顖氱睄闁稿本绋戝▓灞剧箾?        闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?        - ConcreteKnowledgeResult闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熸潏楣冩闁稿顑夐弻娑㈠焺閸愵亖濮囬梺绋款儏鐎氫即寮诲鍫闂佸憡鎸婚悷鈺侊耿?Vision AI 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁嶉崟顒佹闂佹悶鍎洪崜娆戝瑜版帗鐓涢柛銉ｅ劚閻忣亪鏌涚€Ｑ勬珔閾绘牠鏌ㄥ┑鍡樺櫣闁哄棛鍋ら弻娑欐償閿涘嫅褔鏌＄仦鍓ф创鐎殿喗鎸虫俊鎼佸Ψ閵堝洨娉块梺璇叉捣閺佸憡鏅跺Δ鈧灋闁告劦鍠栫粻鏍喐閻楀牆绗氶柛濠傤煼閺屾盯濡烽姀鈩冪彇闂佹寧绋撻崰鎾舵閹惧鐟归柛銉戝嫮浜栭梻浣告啞閺屻劑鏌婇敐澶屽祦闊洦娲嶉崑鎾绘晲鎼粹剝鐏嶉梺绋款儐閻楃娀骞冨Δ鍛棃婵炴垶鐟﹂崰鎰渻閵堝繐鐦滈柛瀣工椤?""
        if graphic_region is not None:
            logger.debug("graphic_region input ignored; validation uses raw image: %s", Path(image_path).name)

        try:
            api_result = llm_gateway.vision_validate_image_sync(
                image_path=image_path,
                prompt="",
                system_prompt=self._concrete_knowledge_system_prompt,
                client=self._vision_client,
                timeout=60,
            )
            return self._build_result_from_vision_payload(api_result if isinstance(api_result, dict) else {})
            
        except Exception as e:
            logger.error(f"VisionAIClient validation failed: {e}")
            return ConcreteKnowledgeResult(
                has_concrete=True,
                has_formula=False,
                confidence=0.5,
                concrete_type="formula",
                reason=f"Vision AI 闂傚倸鍊峰ù鍥х暦閸偅鍙忛柟缁㈠櫘閺佸嫰鏌涘☉娆愮稇闁汇値鍠栭湁闁稿繐鍚嬬紞鎴︽煛鐎ｂ晝绐旈柡灞炬礋瀹曠厧鈹戦崶鑸殿棧缂傚倷绀佹晶搴ㄥ磻閵堝拋鍤曢柛顐ｆ礀闁卞洦銇勯幇鍨窔闁告垳绮欓幃? {e}",
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=True,  # fallback keep
                img_description="description unavailable"
            )
    
    def validate_batch(self, tasks: List[Dict]) -> List[ConcreteKnowledgeResult]:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊峰ù鍥敋瑜忛幑銏ゅ箛椤旇棄搴婇梺褰掑亰閸庨潧鈽夐姀鐘电潉闂佸壊鍋呯换鍕敊婢舵劖鈷戦梻鍫熺〒缁犵偤鏌涙繝鍐╃缂侇喖鐗婂鍕箛椤撶姴甯楅梻渚€娼чˇ顓㈠磿闁秴姹查梺顒€绉甸悡鐔肩叓閸ャ劍鈷掗柟鍐叉喘閺岀喖顢欓幐搴ｃ€愰梺瀹犳椤﹂潧顕ｉ崐鐕佹Щ濡炪値鍓欓ˇ顖炲煘閹达箑鐒洪柛鎰╁妼婵煡姊洪崨濠佺繁闁告挻鐩畷婊堝Ω閳哄倵鎷洪梺鍛婄☉閿曘儲寰勯崟顖涚厱閻庯綆浜滈顓㈡煙椤曞棛绡€鐎规洘甯掗…銊╁川椤栥倗搴婇梻鍌欒兌椤㈠﹪锝炴径鎰闁哄稁鍋呴崗婊堟煕椤愶絾绀冮柣鎾冲暣閺屻倖鎱ㄩ幇顑藉亾閺囩姵鏆滈柛顐ｆ礀閺嬩線鏌涢鐘插姕闁绘挾鍠愭穱濠囶敍濮樺彉铏庨梺钘夊閵堟悂骞冨Δ鈧～婵嬵敇閻樺啿娅氶梻浣告惈閺堫剛绮欓幋锕€鐓濋幖娣妼缁犳稒銇勯幒鎴濃偓宄邦浖閹炬枼鏀介柣姗嗗亝婵即鏌涘☉鍗炴灍闁绘繍浜铏规嫚閳ヨ櫕鐏嶆繝銏㈡嚀濡瑧绮嬮幒妤佹櫇闁稿本绋戦埀顒€顭烽弻宥嗘姜閻楀牜妯傛繝?
        # 2) 婵犵數濮烽弫鎼佸磿閹寸姴绶ら柦妯侯棦濞差亝鏅滈柣鎰靛墮鎼村﹪姊洪崨濠傚Е濞存粍鐗犲畷鎴﹀箻鐠囨彃鐎銈嗗姧缁叉椽骞忛懡銈囩＝濞达絼绮欓崫娲偨椤栥倗绡€闁绘侗鍠栬灒閻忓繑鐗曟禍楣冩煟閵忕姵鍟炴繛鍛矒閺屾盯骞嬮敐鍛呮挸菐閸パ嶈含闁诡喗鐟╅、鏃堝礋閵娿儰澹曢梺鍝勬储閸ㄥ湱绮荤憴鍕╀簻闁规壋鏅涢埀顒佺⊕閹便劑鎼归鐘辩盎闂佽宕樺▔娑㈠几閺冨牊鐓冪紓浣股戦ˉ鍫燁殽閻愬澧懣鎰亜閹哄棗浜鹃梺閫炲苯澧い銊ョ墢濡叉劕鈻庨幇顔剧槇濠殿喗锕╅崢楣冨储闁秵顥婃い鎰╁灪閹兼劖銇勯幋婵囧殗鐎规洘绻傞埢搴ㄥ箣閻樼绱查梻浣告惈閸熺娀宕戦幘缁樼厵闁告稑锕ら埢鏇燁殽閻愯宸ラ柍钘夘槸铻ｉ柟鐐▕閸欏嫭銇勯姀锛勫⒌闁圭锕ュ鍕緞婵犲孩顥?
        # 3) 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩顔瑰亾閸愵喖骞㈡俊鐐存礃濡炰粙鐛€ｎ喗鏅滈柣锝呰嫰鐢挻绻濋悽闈浶㈤柨鏇樺€楅埀顒佸嚬閸ｏ綁濡撮崨瀛樺€婚柤鎭掑劚娴狀垶姊洪幖鐐插姌闁告柨鐬煎濠勭磼濡晲绨诲銈嗗姉婵挳鍩€椤掍緡娈滈柛鈹惧亾濡炪倖甯婄欢锟犲疮韫囨稒鐓曢柣妯虹－婢х數鈧娲橀崝娆撶嵁閺嶃劍濯撮柣鐔碱暒閸戜粙姊绘担绋款棌闁稿鎳庣叅闁哄稁鍋嗙亸鐢碘偓骞垮劚椤︿即鎮″▎鎰╀簻闁哄秲鍔庨埥澶嬨亜閵夈儺鍎戠紒杈ㄥ浮閹晛鐣烽崶褍缁╅梻浣告惈閺堫剟鎯勯娑楃箚闁汇値鍨煎Σ缁樼箾鐎电鞋濞存粠鍓涘Σ鎰板箳濡ゅ﹥鏅梺鍛婁緱閸樼偓绂掗鐐粹拺闁荤喐婢樺Σ缁樸亜閹存繍妲瑰ǎ鍥э躬瀹曞ジ寮撮悙闈涒偓鐐烘⒑閸愬弶鎯堥柛濠冩倐閹啯銈ｉ崘鈹炬嫽婵炶揪绲介幉锟犲箚閸喆浜滈柟瀛樼箖椤ャ垻鈧娲栭悘姘跺箚閺冨牆惟闁靛／鍐ㄐ?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘茬畺婵炲樊浜滅痪褍鈹戦埄鍐ㄦ倕adPoolExecutor + validate闂?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堝煕閹寸姵鍠愰柣妤€鐗嗙粭姘箾閹冲嘲鎳愮壕鐣屸偓骞垮劚閹锋垿鐓鍌楀亾鐟欏嫭绀冨┑鐐诧躬楠炲啫顭ㄩ崘鐐缓闂佺硶鍓濋敃鈺呭疮瀹ュ鐓熼幖绮光偓鍐茶緟闂佺顑嗛幑鍥蓟濞戙垹绠涢柛蹇撴憸閸戠懓顪冮妶搴′簻缂佸鎳撻～蹇撁洪鍕祶濡炪倖鎸炬慨瀵哥箔閿熺姵鈷戦柛娑橈攻閳锋劖绻涢崣澶屽⒌闁诡喗鍎抽悾锟犲箥椤旇姤鐣烽梻浣告啞濞诧箓宕戞径搴澓闂傚倸鍊烽懗鍓佸垝椤栫偛绀夐柡鍥╁剱閸ゆ洟鏌涢锝嗙缁炬儳缍婇弻锝夊箣閿濆憛鎾绘煟閹惧崬鍔滅紒缁樼洴楠炲鎮滈崱娆忓Ъ闂佽瀛╅惌顕€宕￠幎钘夎摕闁绘梻鈷堥弫濠囨煠閹帒鍔氱痪鐐▕閹鈻撻崹顔界彯闂侀潻缍囩徊浠嬵敋閿濆鍋ㄩ柛娑橈工娴犳椽姊哄Ч鍥х仾妞ゆ梹鐗犲畷顖氼吋婢跺鎷绘繛杈剧到閹芥粎绮斿ú顏呯厸闁告稒婢橀惃铏圭磼椤旇偐澧涚紒妤冨枛閸┾偓妞ゆ帊妞掔换鍡涙煟閵忋埄鐒剧紒鐙呯秮閺岋絽螣閸濆嫭姣愬銈呯箲閹倸顫忓ú顏咁棃婵炴垼椴歌倴闂備胶顭堝锔界椤掑嫬绠悗锝庡枛閽冪喖鏌曟径娑橆洭闁告鏁婚弻锝夋偐椤旂厧顬堝銈忓瘜閸犳岸骞忕€ｎ喖钃熼柕澶堝劤閿?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悢婵嗘处閸嬪倹銇勯弽顐沪闁绘挻鐟﹂〃銉╂倷閼碱兛铏庨梺璇茬箺鐏忔瑩鍩€椤掑喚娼愭繛鍙夘焽閹广垽宕奸妷銉︽К闂佸憡娲﹂崹鎵矆閸愵喗鐓忓┑鐐茬仢閻忣亪鎮楅崹顐ゅ⒌婵﹥妞介幊锟犲Χ閸涱剚鍕冪紓鍌欑椤戝棝宕归崸妤€绠犻柣鏃傗拡閺佸秵绻濇繝鍌氼伌婵炶偐鍠栧铏规喆閸曨偆顦ㄩ梺绯曟櫆閻楃姴顕ｉ崘宸叆闁割偆鍠撻崢鍗炩攽閻愭潙鐏﹂柨鏇ㄥ亰瀵劎绱掑Ο闀愮盎闁挎粌顭峰畷鍫曞Ω瑜嶉獮鍫ユ⒒娓氣偓濞佳囨晬韫囨稑绀冪憸搴綖閺囥垺鈷掑〒姘ｅ亾闁逞屽墰閸嬫盯鎳熼鐐插偍闁圭虎鍠楅悡鏇㈡倵閿濆骸浜濈€规洖鐭傞弻?_default_result(True)闂?
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - 闂傚倸鍊风粈渚€骞栭位鍥敃閿曗偓閻ょ偓绻濇繝鍌滃缂佲偓婢跺鍙忔俊鐐额嚙娴滈箖姊洪棃娑欘棛缂佽埖宀搁悰顔锯偓锝庡枟閺呮粓鏌﹀Ο渚Х婵顨婂缁樻媴閸濆嫬浠橀梺纭呭Г缁捇濡撮崒娑氼浄閻庯綆浜為敍娑㈡⒑鐟欏嫬鍔ゆい鏇ㄥ弮閹兘鎮烽幍铏杸闂佺粯蓱瑜板啴顢旈锔界厽婵炴垼椴搁悵鐜紅ure.result() 闂傚倸鍊搁崐鐑芥嚄閸洏鈧焦绻濋崶褎妲梺鍝勭▉閸嬪棝鎯屽▎鎾寸厵閺夊牆澧介悾閬嶆煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏哥棯闂備胶顭堟鍝ョ矓瑜版帒钃熸繛鎴欏灩閸楁娊鏌曟繛鍨姍缂併劎鏅槐鎺楀箚瑜嶇紞鏍磼椤旂晫鎳囨鐐插暞缁傛帞鈧絽鐏氶弲顒€鈹戦悙鏉戠仸闁荤喆鍎茬粋鎺楀煛閸涱喒鎷?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - tasks: 闂傚倸鍊搁崐宄懊归崶褜娴栭柕濞炬櫆閸ゅ嫰鏌ょ粙璺ㄤ粵婵炲懐濮垫穱濠囧Χ閸屾矮澹曢梻浣风串缁蹭粙鎮樺杈╃當闁绘梻鍘ч悞鍨亜閹哄棗浜鹃梺浼欑到閸㈡煡锝炲┑瀣垫晞闁冲搫鍊归ˉ鍫⑩偓瑙勬礈閸犳牠宕洪悙鍝勭畾鐟滃本绔?闂傚倸鍊搁崐鎼佸磹閹间礁纾圭紒瀣嚦濞戞鏃堝焵椤掑啰浜辨繝寰锋澘鈧洟骞婃惔锝囦笉闁哄稁鍘肩粻瑙勭箾閿濆骸澧┑鈥茬矙閺岋繝宕担绋库拫闂佸搫鏈惄顖炪€侀弴銏″亹闁肩⒈鍏涚槐妯讳繆閻愵亜鈧倝宕戦幘鍓佷笉闁规崘顕ч拑鐔哥箾閹存瑥鐏╅幆鐔兼⒑闂堟侗妯堥柛鐘崇墵瀹曟繈鍩€椤掑倻纾介柛灞剧懄缁佹澘霉濠婂骸澧い顒€锕﹀濠囨偉瀵摵Dict]闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - ConcreteKnowledgeResult 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁嶉崟顒佹濠德板€曢幊宀勫焵椤掆偓閸燁垰顕ラ崟顖氱疀妞ゆ垟鏂傞崕鐢稿蓟濞戙垹绠涢梻鍫熺⊕閻忓秹姊洪崫鍕闁告挾鍠栧璇测槈閵忊晜鏅濋梺缁樕戣ぐ鍐╂叏瀹€鍕拺闂侇偆鍋涢懟顖涙櫠椤曗偓閹鈽夐幒鎾寸彋濡ょ姷鍋涘Λ婵嗩嚕椤曗偓瀹曞爼鍩￠崘鈺傜帆闂備胶顢婃竟鍫ュ箵椤忓棗绶ら柛褎顨呯粻鐔兼煕瑜庨〃鍡涙偂濞戞﹩鐔嗛悹铏瑰皑閸旂喎顭胯閻╊垶寮婚悢鐓庡窛濠电姴鍟埀顒佸姉閳?tasks 闂傚倸鍊峰ù鍥敋瑜旈弻濠囨晲閸滀胶鍔烽悷婊冪箳濡叉劙鎮欑€靛摜鐦堥梺鍛婃处閸橀箖藝椤栫偞鈷戦柛鎾村絻娴滄牠鏌涙惔銏㈠弨鐎殿喗鐓￠、妤呭焵椤掑嫧鈧妇鎹勯妸锕€纾梺鎯х箳閹虫捇銆傞悽鍛娾拺?""
        if not tasks:
            return []

        batch_results = self._validate_batch_with_vision_api(tasks)
        if batch_results is not None:
            return batch_results

        import concurrent.futures

        # 可通过环境变量调优并发，便于在不同机器上做压测寻优。
        raw_workers = str(os.getenv("PHASE2B_OCR_VALIDATE_WORKERS", "auto") or "auto").strip().lower()
        if raw_workers in {"", "auto"}:
            desired_workers = max(1, (os.cpu_count() or 2) - 1)
        else:
            try:
                desired_workers = int(raw_workers)
            except Exception:
                desired_workers = 1
        max_workers = max(1, min(desired_workers, len(tasks), 16))

        def _validate_task(task: Dict[str, Any]) -> ConcreteKnowledgeResult:
            image_path = str(task.get("image_path", "") or "")
            ocr_text = str(task.get("ocr_text", "") or "")
            skip_duplicate_check = bool(task.get("skip_duplicate_check", False))
            try:
                return self.validate(
                    image_path=image_path,
                    ocr_text=ocr_text,
                    skip_duplicate_check=skip_duplicate_check,
                )
            except TypeError:
                try:
                    return self.validate(image_path=image_path, ocr_text=ocr_text)
                except TypeError:
                    return self.validate(image_path=image_path)

        results: List[Optional[ConcreteKnowledgeResult]] = [None] * len(tasks)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {}
            for i, task in enumerate(tasks):
                future = executor.submit(_validate_task, task)
                future_to_idx[future] = i

            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    logger.error(f"Batch validation failed for item {idx}: {e}")
                    results[idx] = self._default_result(True)

        return [item if item is not None else self._default_result(True) for item in results]

    def validate_for_coreference(
        self,
        image_path: str,
        sentence_text: str,
        context_text: str = ""
    ) -> Dict[str, Any]:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊搁崐鐑芥嚄閸洍鈧箓宕奸姀鈥冲簥闂佽澹嗘晶妤呭磻鐎ｎ亖鏀介柣妯诲絻閺嗙偞绻涢崨顖氣枅闁哄矉缍侀弫鎰板川椤旇姤瀚抽梻浣侯焾椤戝棝鏁冮姀銈呯畺婵せ鍋撻柟顔界懇瀹曞綊顢曢敐鍥ф锭闂傚倷鑳剁涵鍫曞疾濠婂牄鈧啯绻濋崶褏鐣洪悗鐟板婢瑰寮告惔銊у彄闁搞儯鍔嶉幆鍕归悩灞傚仮婵﹥妞藉畷銊︾節閸曨剙娅欐繝鐢靛仜椤︽澘煤閻旇偐宓侀柟鐗堟緲缁犺櫕淇婇妶鍌氫壕闂佺粯甯楀浠嬪蓟濞戙垹绠涢柛蹇撴憸閻╁酣姊洪幇浣风敖濠⒀勵殕缁岃鲸绻濋崶顬囨煕閵夘喚浠㈤柕蹇嬪€栭悡鐔搞亜閹捐泛鍓辨俊鑼劋娣囧﹪宕ｆ径濠傤潓闂佸疇顫夐崹鍨暦閹偊妲剧紓浣瑰姉閸嬨倝骞冨Δ鍛祦闁割煈鍠栨慨搴♀攽閻愰鍤嬬紒鐘虫尭椤曪絿鈧湱濮烽悿鈧┑鐐村灦閻熝囧储閽樺鏀介柣妯款嚋瀹搞儵鏌熼崘鏌ュ弰闁靛棗鍊垮畷妤呮嚃閳哄啰妲囬梻渚€鈧偛鑻晶顖炴煙瀹勭増鍤囬柟鐓庣秺瀹曠兘顢橀妸褔妫烽梻鍌氬€峰ù鍥綖婢舵劕纾跨€规洖娲らˉ姘亜閹惧崬鐏╃紒鐘虫そ閺岋絽螣閼姐們鍋炲┑鈩冨絻閻楁捇寮婚悢鐓庣畾鐟滃繘鏁嶅澶嬬厱闁瑰瓨绻冪拹锛勭磼鏉堛劌绗х紒杈ㄥ笒铻ｉ柛婵嗗閹虫瑩姊绘担绛嬪殐闁哥姵顨婂畷鏇㈠箮閽樺鍘洪柟鍏肩暘閸婃鎮块埀顒勬⒑閸︻厼浜炬繝鈧崷顓燁潟婵炲棙鎸婚埛鎺懨归敐鍛暈闁诡垰鐗撻弻娑氣偓锝庝簼椤ャ垻鈧娲忛崹濂稿Φ閹版澘绠抽柟瀵稿Т婵?
        # 2) 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冮崱娑樜﹂柛鏇ㄥ灠閸愨偓闂侀潧顭堥崕鍗炐掗崼婵冩斀闁斥晛鍟徊缁樸亜椤撶姴鍘寸€殿喖顭烽弫鎰緞濞戞氨鈼ゆ俊鐐€栧濠氬磻閹炬枼鏀介柍鈺佸暙椤曟粎绱掔紒妯兼创鐎规洖銈搁幃銏☆槹鎼存繄绀夐梺璇叉唉椤煤濮椻偓閵嗗啯绻濋崶褏鐣洪梺绉嗗嫷娈㈤柡浣哥У缁绘繈妫冨☉娆忣槱缂備浇鍩栧姗€鍩為幋锔藉€婚柛銉㈡櫅閸╁苯鈹戦悙璺虹毢濠电偐鍋撻悗瑙勬礃缁矂鍩為幋锕€鐐婄憸婊堝吹閵堝鈷戦柛娑橈功閳藉鏌ｆ幊閸旀垵顕ｉ弻銉晢闁告洦鍓涢崢鍗烆渻閵堝棗濮х紒鑼舵硶缁寮介鐔封偓鍫曠叓閸ャ劍鐓ュ┑顔肩Ч閺岋紕浠﹂崜褜鐏辩紓浣哄У缁嬫垿锝炲┑瀣垫晢闁稿本鑹剧紓姘攽?img_description闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎鈥崇湴閸旀垿宕洪埀顒併亜閹烘垵鈧崵澹曟總绋跨骇闁割偅绋戞俊璺ㄧ磼閻樺磭澧辩紒杈ㄥ笒铻栭柛鎰╁妽閻庡姊虹€圭姵顥夋い锔诲灥閻忓啴姊洪崨濠傚Е濞存粍绻堝畷顖炴偋閸垻鐦?DeepSeek 婵犵數濮烽弫鎼佸磻濞戙垺鍋ら柕濞у啫鐏婇悗鍏夊亾闁告洦鍋勯悵妯侯渻閵堝棗绗掗悗姘卞厴瀵偊宕橀妸銏℃杸闂佺鏈喊宥夊疮閻愮儤鐓曢柕鍫濇嫅閺€濠氭煏閸パ冾伂缂佺姵鐩獮姗€骞栭鐕佹＇婵犵數濮甸鏍疮椤愶箑鐐婇柕濞у啫绠為梻鍌欑閹碱偆绮旈弻銉ョ閹兼番鍨哄▍鐘垫喐閺冨牆钃熼柣鏂挎憸閻熷綊鏌涢…鎴濇灈妞ゎ剙顦甸幃妤呭垂椤愶絿鍑￠柣搴㈠嚬閸撶喖銆佸Ο鑽ら檮缂佸娉曢ˇ銊╂⒑閹稿海绠撻柟鍐茬У缁?
        # 3) 濠电姷鏁告慨鐢割敊閺嶎厼绐楁慨妯挎硾缁犵娀鏌熼幑鎰滄繛宸簼閺呮繈鏌涚仦鐐殤闁告棑绠戦—鍐Χ閸℃鐟愰梺缁樺釜缁犳捇鏁愰悙鍙傛棃宕橀鍡床缂傚倸鍊烽悞锕傛晪闂佽绻愰顓㈠焵椤掑喚娼愭繛鍙夌墵閹儲绺界粙璺ㄧ暫濠电姴锕ら悧鍡涙倷婵犲洦鐓冮柛婵嗗閺嬨倖淇婇幓鎺戞Щ闁宠鍨块幃娆戔偓娑櫭棄宥囩磼閻愵剚绶茬紒澶婂閸掓帒鈻庨幘宕囩杸濡炪倖鏌ㄦ晶浠嬫偩閸洘鈷戠紒瀣濠€浼存煕濠靛棝鍙勯柛鈹惧亾濡炪倖甯婄粈浣该归閿亾鐟欏嫭澶勯柛瀣攻娣囧﹪鎮块锝喰╂俊鐐€ч懙褰掑疾閻樿钃?concrete_result 濠电姴鐥夐弶搴撳亾濡や焦鍙忛柣鎴ｆ绾惧鏌ｅΟ鑽ゃ偞闁哄鐗楃换娑㈠箣閻戝洤鍙曢梺鑲╁鐎笛囧Φ閸曨喚鐤€闁规崘娉涢。娲煟閻斿摜鎳曠紒鐘虫崌閻涱噣寮介妸锔剧Ф闂佸憡鎸嗛崟顐¤繕闂傚倷娴囧銊ф濮樿京涓嶉柡宥庡幖閽冪喖鏌ｉ弮鍌氬付缂佺姾顫夐妵鍕箻鐎靛摜鐣奸梺鐑╂櫅妤犳悂鍩為幋锔藉€烽柤纰卞墯閹插ジ鏌﹂崘顓㈠摵闁靛洤瀚版俊鎼佹晲閸涱厼顫撻梻浣风串缁插墽鎹㈤崼婵堟殾婵せ鍋撴い銏℃瀹曨亝鎷呴崷顓犘繝鐢靛Х閺佸憡鎱ㄦ导鏉戝瀭闁绘挸绨堕弸鏍ㄧ箾閹寸偞鐨戦柣顓熸崌閺屾盯顢曢敐鍡欘槬闂佸搫顑勭欢姘跺蓟閺囥垹閱囨繝鍨姈绗戦柡?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘偓浣肝旈崨顔煎壃闂佹眹鍨荤粋鎶巃te闂?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堟偂閻旇偐鍙撻柛銉ｅ妽鐏忕數绱掗悩闈涱暭濞ｅ洤锕、鏇㈠閵忋垹濮遍梻浣虹《閺備線宕戦幘鎰佹富闁靛牆妫楃粭鍌炴煠閸愯尙鍩ｉ柟顖氭湰閹棃锝為璺ㄧ泿婵＄偑鍊栭崝鎴﹀春閸曨垰纾块柟杈鹃檮閻撴瑩鏌熺紒妯虹闁圭晫濮烽埀顒€鐏氬妯尖偓姘煎枟缁傛帡鏁冮崒姘辩暰閻熸粌顦靛畷鎴﹀箻缂佹ê鈧兘鏌ｉ幋鐏活亝绂掗鐐╂斀闁绘劕寮堕ˉ鐐烘煟閻旀繂鍟伴弳鍡樼箾閹存瑥鐏柣鎾冲暟閹茬顭ㄩ崼婵堫槶濠殿喗顭堟ご绋跨暦閺屻儲鐓曠€光偓閳ь剟宕戝☉姘变笉妞ゆ牗顕㈣ぐ鎺戠闁稿繗鍋愮粙鍥⒑鐠囪尙绠烘繛鍛礈閹广垹鈹戦崶鈺冪槇闂佺鏈崙瑙勭妤ｅ啯鈷戦悹鍥ㄥ絻閻︺劑鏌涢敐蹇曠М鐎殿喖顭烽弫鎾绘偐閺屻儱鏁规俊鐐€栭崝鎴﹀垂瑜版帒鐓曞鑸靛姈閳锋垿鏌熼懖鈺佷粶闁逞屽墯閻楁粓宕氶幒妤€绠婚悹鍥皺閻ｆ椽姊虹紒妯哄闁诲繑鑹鹃悾閿嬪緞閹邦厾鍘卞┑鐘绘涧濡顢旈幘顔界厱闁靛牆妫涢幊鍕磼缂佹娲撮柡浣瑰姍瀹曘劑顢欐穱鍗炰汗闂傚倷绀侀幗婊勬叏閻㈠憡鍋嬮柣妯煎劋椤ャ倝姊绘担鍛婃儓閻炴凹鍋婂畷鏇㈡焼瀹ュ棗鈧爼鏌ц箛锝呬簽缂佲檧鍋撻梻鍌氬€搁悧濠囨嚀娴犲鐓涢柛娑卞幘椤斿﹤鈹戞幊閸婃挾绮堟担绯曟灁鐎光偓閸曨兘鎷洪梺鍦焾鐎涒晠藟閸℃稒鐓曢悗锝庡亜婵绱掗鑲╁鐎垫澘瀚伴獮鍥敇閻樻彃绠為梻鍌欒兌閹虫捇鎮洪妸褎宕查柛鏇ㄥ幘娑撳秴霉閻撳海鎽犻柣鎾跺枑閵囧嫰顢橀悢椋庝淮闂佽楠忕粻鎾诲蓟閿涘嫪娌柛濠勫枎椤忣厼顪冮妶鍡樺碍闁靛牏顭堥悾鐑筋敂閸涱喖顎撻梺?Vision 闂傚倸鍊峰ù鍥х暦閸偅鍙忛柟缁㈠櫘閺佸嫰鏌涘☉娆愮稇闁汇値鍠栭湁闁稿繐鍚嬬紞鎴︽煛鐎ｂ晝绐旈柡灞炬礋瀹曠厧鈹戦幇顓夛箓寮?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻堝鍫曞礆缁佸穲nce_text/context_text 婵犵數濮烽弫鎼佸磻濞戙埄鏁嬫い鎾跺枑閸欏繘鏌熺紒銏犳灈缂佺姷濞€閺岀喖鎮ч崼鐔哄嚒闂佸搫顑勭欢姘跺蓟閺囥垹閱囨繝闈涙祩濡倕顪冮妶鍌涙珕闁绘搫绻濋悰顕€寮介鐔蜂壕婵炴垶鐟悞鐣岀磼閻樼鑰块柡宀嬬秮閹垻绮欓崹顕呮綌缂傚倷鑳剁划顖炴儎椤栫偟宓佹慨妞诲亾闁圭厧缍婇、鏇㈠閳╁啰鏆繝鐢靛Х椤ｈ棄危閸涙潙纾婚柍褜鍓熼弻锟犲川椤栨矮鎴风紓渚囧枦椤曆囧煡婢跺娼ㄩ柛鈩兠惁婊堟⒒娴ｅ憡鍟為柟绋挎瀹曘劑顢欓崗鍏碱啀婵犵數濮烽弫鎼佸磻閻愬搫鍨傞柛顐ｆ礀缁犱即鏌熼梻瀵歌窗闁轰礁瀚伴弻娑㈠焺閸愵亖妲堥梺鍛婂灩婵炩偓闁哄本鐩獮鍥濞戞瑧浜堕梻浣告惈閹峰宕滈悢鐓庣畺婵せ鍋撻柟顔界懇濡啫鈽夊Δ鈧ˉ姘舵⒒娴ｅ憡璐￠柛蹇斏戠粋宥夊醇閺囩偠鎽曢梺璺ㄥ枔婵澹曢崗鑲╃瘈闁割煈鍋勬慨鍫㈢磽瀹ュ懐澧㈢紒杈ㄥ笧缁辨帡濮€閻樿尙鍝楅梻浣虹《閺呮粓鎮у鍛灊婵炲棙鍔曠欢鐐测攽閻樻煡顎楅柟顔藉灴濮婃椽宕ㄦ繝浣虹箒闂佸憡鎸荤粙鎺楀Φ濞嗘挸绠柤鎭掑劗閹锋椽姊洪崨濠勨槈闁挎洩绠撻獮濠囧焵椤掑嫭鈷戠紒瀣健閸欏嫬霉濠婂啰鍩ｆ鐐插暣瀹曨偊宕熼妸锔锯偓濠氭⒑鐟欏嫬鍔ょ痪缁㈠弮瀹曨偄螖閸涱喒鎷洪梺鐓庮潟閸婃洟寮搁幋鐐电瘈闁靛繆妲勯懓璺ㄢ偓娈垮枟閻擄繝鐛弽銊﹀闁革富鍘肩敮?
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - validate(...) 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏哥棯闂備礁鎼幏瀣礈濮樿泛绠為柕濞垮劗閺€浠嬫煕椤愵偄浜濇い銉ョ崻uld_include/confidence/img_description闂?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - image_path: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮煡鏌涘☉鍙樼凹闁诲骸顭峰娲濞戞氨鐤勯梺鎼炲妼閹碱偊鎮鹃柨瀣窞閻庯絻鍔嬬花濠氭⒑閸濆嫮袪闁告柨绉归幆鍐箣閻樼數锛滈梺褰掑亰閸犳牗绂掗柆宥嗙厸閻忕偠顕ф俊濂告煃閽樺妲搁摶锝夋煟閹惧磭宀搁柛娆欑節濮婃椽鎳￠妶鍛勃闂佸憡鍨电紞濠傜暦閹寸偟绡€闁稿本绮嶅▓楣冩⒑闂堚晛鐦滈柛妯绘倐瀹曟垿骞樼紒妯绘珳闂佸憡渚楅崣搴ㄥ汲閵忋垻纾藉ù锝囨嚀婵牓鏌嶉鍡╂r闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # - sentence_text: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮繈鏌嶈閸撶喖寮崘顔碱潊闁炽儲鍓氶崵銈夋⒑閸濆嫷妲归柛銊ョ秺钘濋柕濞炬櫆閳锋垿鏌涘☉姗堟缂佸爼浜堕弻娑樷枎韫囨稑寮伴悗瑙勬处閸ㄨ泛鐣烽崼鏇ㄦ晢濞达絽鎼铏節閻㈤潧浠﹂柛銊ョ埣閳ワ箓宕堕妸锔界彿闂侀潧绻堥崐鏍磻閻樿櫕鍙忔俊顖滃帶鐢泛霉濠婂嫬顥嬮柍褜鍓濋～澶娒洪弽褝鑰块梺顒€绉撮悞鍨亜閹烘埊鍔熼柛鎺撴緲椤儻顦叉繛鎾棑閸掓帡鏁愰崪浣圭稁濡炪伇鍛闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # - context_text: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮繈鏌嶈閸撶喖寮崘顔碱潊闁炽儲鍓氶崵銈夋⒑閸濆嫷妲归柛銊ョ秺钘濋柕濞炬櫆閳锋垿鏌涘☉姗堟缂佸爼浜堕弻娑樷枎韫囨稑寮伴悗瑙勬处閸ㄨ泛鐣烽崼鏇ㄦ晢濞达絽鎼铏節閻㈤潧浠﹂柛銊ョ埣閳ワ箓宕堕妸锔界彿闂侀潧绻堥崐鏍磻閻樿櫕鍙忔俊顖滃帶鐢泛霉濠婂嫬顥嬮柍褜鍓濋～澶娒洪弽褝鑰块梺顒€绉撮悞鍨亜閹烘埊鍔熼柛鎺撴緲椤儻顦叉繛鎾棑閸掓帡鏁愰崪浣圭稁濡炪伇鍛闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸婂潡鏌ㄩ弴鐐测偓鍝ョ不閺夊簱鏀介柣妯虹－椤ｆ煡鏌涙繝鍌滀粵闁靛洤瀚板浠嬵敃椤厾鎹曠紓鍌欒閸嬫挾鈧厜鍋撻柛鏇ㄥ墰閸樻悂鎮楅獮鍨姎濡ょ姴鎲＄粋宥夋倷鐎靛摜顔曟繝銏ｆ硾閻楀棝鎮橀弻銉︾厸鐎光偓鐎ｎ剛锛熼梺閫炲苯澧剧紓宥呮瀹曟垿宕卞☉娆忓壒婵犵數濮村ú锕傛偂閻斿吋鐓冩い鏍ㄧ〒閹冲啯绻涚仦鍌氣偓妤冨垝閸儱纾奸柣鎰ˉ閹峰姊洪崜鎻掍簼缁炬澘绉撮埢宥夊炊閳哄啰锛滈梺閫炲苯澧寸€规洜鍠栭、娑樷槈濮橆剙绠為梻鍌欑閹碱偆鎷犻悙鍝勭妞ゆ劧缍嗘禒鈺呮⒒?should_include/confidence/img_description/concrete_result闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?""
        base_result = self.validate(image_path=image_path, ocr_text="")
        return {
            "should_include": base_result.should_include,
            "confidence": float(base_result.confidence),
            "reason": base_result.reason,
            "img_description": base_result.img_description,
            "mode": "coreference_concrete",
            "concrete_result": base_result,
            "sentence_text": str(sentence_text or "").strip(),
            "context_text": str(context_text or "").strip(),
        }

    def _get_math_ocr(self):
        """Lazily initialize ThreadSafeMathOCR when enabled."""
        if not bool(getattr(self, "_math_ocr_enabled", False)):
            return None
        if self._math_ocr is not None:
            return self._math_ocr
        if bool(getattr(self, "_math_ocr_init_attempted", False)):
            return None

        init_lock = getattr(self, "_math_ocr_init_lock", None)
        if init_lock is None:
            self._math_ocr_init_lock = threading.Lock()
            init_lock = self._math_ocr_init_lock

        with init_lock:
            if self._math_ocr is not None:
                return self._math_ocr
            if bool(getattr(self, "_math_ocr_init_attempted", False)):
                return None
            self._math_ocr_init_attempted = True
            try:
                from services.python_grpc.src.content_pipeline.infra.runtime.ocr_utils import (
                    ThreadSafeMathOCR,
                )

                self._math_ocr = ThreadSafeMathOCR()
                logger.info("ThreadSafeMathOCR initialized lazily for formula detection")
            except Exception as exc:
                self._math_ocr_init_error = str(exc)
                logger.warning(
                    "ThreadSafeMathOCR lazy init skipped, fallback to text-only formula detection: %s",
                    exc,
                )
                self._math_ocr = None
            return self._math_ocr

    def _detect_math_formula(self, text: str = "", image: Optional[np.ndarray] = None) -> bool:
        """Detect whether OCR text/image likely contains formula-like content."""
        if image is not None:
            math_ocr = self._get_math_ocr()
        else:
            math_ocr = None
        if math_ocr is not None:
            try:
                math_results = math_ocr.recognize_math(image)
                if math_results:
                    min_score = float(getattr(self, "_math_ocr_min_score", 0.7) or 0.7)
                    for res in math_results:
                        if float(res.get("score", 0) or 0.0) >= min_score:
                            logger.debug(f"MathOCR detected: {str(res.get('text', ''))[:50]}")
                            return True
            except Exception as e:
                logger.debug(f"MathOCR detection failed: {e}")

        normalized_text = str(text or "")
        if not normalized_text:
            return False
        text_lower = normalized_text.lower()

        formula_tokens = [
            "=",
            "==",
            ">=",
            "<=",
            "!=",
            "->",
            "<-",
            "=>",
            "lim",
            "log",
            "ln",
            "sin",
            "cos",
            "tan",
            "sqrt",
            "sum",
            "int(",
            "d/dx",
            "dy/dx",
            "o(n)",
            "asl",
        ]
        token_hits = sum(1 for token in formula_tokens if token in text_lower)
        math_chars = set("=+-*/^()[]{}<>_|\\")
        math_char_count = sum(1 for c in normalized_text if c in math_chars)
        if token_hits >= 2 or math_char_count >= 3:
            return True

        if re.search(r"\d+\s*/\s*\d+", normalized_text):
            return True
        if re.search(r"\b[a-zA-Z]\s*=\s*[-+]?\d+(\.\d+)?\b", normalized_text):
            return True
        return False

    def _get_ocr_extractor(self):
        """Docstring omitted."""
        if self._ocr_extractor is not None:
            return self._ocr_extractor
        if self._ocr_extractor_init_error:
            return None
        try:
            from services.python_grpc.src.content_pipeline.infra.runtime.ocr_utils import OCRExtractor

            self._ocr_extractor = OCRExtractor()
            return self._ocr_extractor
        except Exception as exc:
            self._ocr_extractor_init_error = str(exc)
            logger.warning(f"OCR extractor unavailable for text-only description: {exc}")
            return None

    def _extract_text_page_description(self, image_path: str, ocr_text: str = "") -> str:
        """Docstring omitted."""
        text = str(ocr_text or "").strip()
        if not text:
            extractor = self._get_ocr_extractor()
            if extractor is not None:
                try:
                    text = str(
                        extractor.extract_text_from_image(
                            image_path=image_path,
                            preprocess=True,
                        )
                        or ""
                    ).strip()
                except Exception as exc:
                    logger.debug(f"Extract OCR text for text-only page failed: {exc}")
                    text = ""

        if not text:
            return ""

        compact_text = re.sub(r"\s+", " ", text).strip()
        max_chars = 1600
        if len(compact_text) > max_chars:
            compact_text = compact_text[:max_chars].rstrip() + " ..."
        return compact_text

    def _analyze_cv_features(self, image_path: str) -> Tuple[float, str, Optional[np.ndarray]]:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊峰ù鍥х暦閸偅鍙忛柡澶嬪殮濞差亜鐓涢柛婊€鐒﹂弲顏堟偡濠婂嫬鐏村┑锛勬暬楠炲洭寮剁捄銊モ偓鐐差渻閵堝棗鍧婇柛瀣崌閺岋綁骞囬濠呭惈闂佸搫鐬奸崰鏍€佸☉銏犲耿婵°倐鍋撻柡鍡樼懃椤法鎲撮崟顓炩吂闁剧粯鐗犻弻锝咁潨閳ь剙顭囪缁傛帡鏁冮崒娑氬幈闁硅偐璇濋弶澶稿垝婵＄偑鍊戦崹娲晝閵忕姷鏆︽い鎰剁畱鍞悷婊冪灱缁厽寰勬繛鐐杸闁圭儤濞婂畷鎰板即閻橆偄浜炬慨妯煎帶濞呭秹鏌熼鐐毈鐎规洘绮嶉幏鍛存惞閻у摜闂梻鍌欒兌椤牓寮甸鍕殌濞寸姴顑嗛崵鍐煃閸濆嫬鏆熼柣锝呯埣閹宕楁径濠佸闂備礁鎲￠崝褏绱撳顑兾旈崨顔规嫼闂傚倸鐗婄粙鎾剁不濮橆厺绻嗛柣娆愮懃濞层倗澹曟繝姘厵闁诡垎鍛喖婵犳鍨遍幐鎶藉蓟濞戙垹绠婚柡澶嬪灩缁侀攱绻濈喊妯哄⒉濠电偛锕ら～蹇撁洪鍕獩婵犵數濮寸€氼剟鍩呰ぐ鎺撯拺闁革富鍘搁幏锟犳煕鐎ｎ亷韬鐐插暣閸ㄩ箖寮妷锔锯偓濠氭⒑閸︻厼浜剧紒?
        # 2) 婵犵數濮烽弫鎼佸磻閻愬樊鐒芥繛鍡樻尭鐟欙箓鎮楅敐搴′簽闁绘繂鐖奸弻娑㈠焺閸愵亖濮囬梺绋款儌閺呮盯鍩為幋锔藉亹鐎规洖娴傞弳鈥愁渻閵堝啫鍔氶柣妤佹崌瀵鏁愭径濠勭杸濡炪倖甯掗崐褰掑箟婵傚憡鈷戦悹鍥ㄥ絻閻︺劑鏌涢敐蹇曠М鐎殿喖顭烽弫鎾绘偐閺屻儱鏁规俊鐐€栭崝鎴﹀垂瑜版帪缍栭柍鍝勬噺閻撶喖鐓崶銊﹀鞍缂佸倸顑夐弻娑㈠Ω閵婏妇銆愰梺浼欑到閸㈣尙鍙呭銈呯箰閹冲孩绂嶅鍫熷€甸柣鐔告緲椤忣亜顭块悷鐗堫棦闁诡喕鍗虫俊鐑藉Ψ閵忊剝鏉告俊鐐€栧ú鏍涘☉姘辩煓濠电姵纰嶉悡娑㈡倶閻愰鍤欏┑顔煎€块弻鐔风暦閸パ勭亪闂佽鍠撻崹浠嬪箖閳╁啯鍎熼柨婵嗗瀛濋梻鍌氬€风欢姘焽瑜旈幃褔宕卞☉妯肩枃闂佸湱澧楀妯盒ч弻銉︾厸濠㈣泛顑愰崕銉︾箾閹炬剚鐓奸柡灞炬礋瀹曠厧鈹戦崶褜妲遍梻浣告惈濡參宕滃杈ㄥ床婵炴垯鍨圭粈鍌炴煟閹惧啿鐦ㄦ俊顐㈡濮婅櫣绱掑Ο鍨棟婵犫拃鍌滅煓妤犵偛绻橀幃鈺冪磼濡儤顓绘俊鐐€栭崹鐓幬涢崟顒傤洸濡わ絽鍟悡鐔兼煙鏉堝墽绋绘い銉ヮ槹濞?
        # 3) 闂傚倸鍊风粈渚€骞栭位鍥敃閿曗偓閻ょ偓绻濇繝鍌滃闁藉啰鍠栭弻鏇熺箾閸喖澹勫┑鐐叉▕娴滄粓宕橀埀顒€顪冮妶鍡樺暗闁革絻鍎辫灋闁告劦鍠楅埛鎴犵磽娴ｅ顏呮叏閸ヮ剚鐓熼幒鎶藉礉閹达箑绠栨俊銈呮媼閺佸鏌嶈閸撶喎顕ｆ繝姘櫢闁绘ɑ鐓￠崬璺侯渻閵堝棗濮傞柛銊ョ秺閿濈偤鍩￠崨顔惧幗闂婎偄娲﹂幐鑽ょ矉鐎ｎ喗鐓曢柕濞垮妽绾箖鏌ｉ敐鍛Щ閻撱倖銇勮箛鎾村櫣濞存粍绮庣槐鎾存媴閸撴彃鍓甸梺绋挎唉瀹曠數鍒掗崼鈶╁亾閿濆骸鏋熼柍閿嬪灴閺屻倗鎲撮崟顒傚嚒闂佺粯鎸婚崝妤冩閹烘鏁婇柤娴嬫櫅椤も偓缂傚倷绀侀崐鍝ョ矓瑜版帞宓侀柟鐑橆殔缁犲鏌涢幘鑼妽闁搞倕鏈换婵嬫偨闂堟稐娌梺鎼炲妽閸庡ジ骞楅锔解拺缂佸灏呴崝鐔兼煛娴ｅ憡鎲告俊鍙夊姍楠炴鈧潧鎽滈幊婵嬫⒑閹肩偛鍔€闁告劦浜堕崬鐑樼節閻㈤潧啸闁轰焦鎮傚畷鎴濃槈濡繑妞介、姗€濮€閻樼數鏆┑鐐差嚟婵挳顢栭崨顔煎姅闂傚倷鐒︾€笛呮崲閸岀偛纾归柛娑橈功椤?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘茬畺闁惧繐鍘滈崑鎾诲捶椤撶姳绨穘CV Canny/闂傚倷娴囧畷鐢稿窗閹邦喖鍨濋煫鍥ㄧ☉閺勩儵鏌涢妷顔煎闁搞劌鍊圭换娑橆啅椤旇崵鐩庨梺缁樺笒閻忔岸濡甸崟顖氱闁挎繂妫涢妴濠傤渻閵堝繗顓洪梻鍕婵＄敻宕熼姘鳖唺闂佸搫鍊圭€笛囨倶閸惊鏃堟偐闂堟稐娌梺鑽ゅ枂閸庣敻銆佸Ο鑽ら檮缂佸娉曢ˇ鏉款渻閵堝棙灏靛┑顕呭弮瀵?+ NumPy 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢锝嗙缂佹劖顨婇弻锟犲炊閵夈儳鍔撮梺杞扮濞差參寮婚悢鍏尖拻閻庨潧澹婂Σ顕€寮?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顧€婵″洨绮绘ィ鍐╁€甸柣銏☆問閻掑墽鎮妸鈺傗拺缂佸娼￠妤呮煟鎺抽崝搴ｅ垝鐎ｎ亶鍚嬪璺好¤椤法鎹勬笟顖氬壋闂?CV 闂傚倸鍊搁崐鐑芥嚄閸撲礁鍨濇い鏍亼閳ь剙鍟村畷濂稿Ψ閵壯呮瀮闂備浇顫夊畷姗€宕洪弽顓ф晝闁兼亽鍎崇粻楣冩煙鐎电浠фい锝嗙叀閹顫濋鐐叉懙闂佸搫鏈惄顖氼嚕閹绢喖惟闁靛牆娲ㄥ▔鍧楁煟鎼淬埄鍟忛柛鐘冲哺瀹曟顫滈埀顒€顕ｇ拠娴嬫闁靛繒濮堥妸褎鍠愮€广儱妫涢々鎻捨旈敐鍛殲闁绘挻鐩幃妤呮晲鎼存繄鐩庨梺鍝勬閸犳挾妲愰幒鎾寸秶闁靛鍎茬拠鐐参旈悩闈涗沪妞ゎ厼娲崺鈧い鎺戝€归弳鈺冪棯椤撶偟鍩ｇ€规洘鍨块獮妯好虹紒妯绘珫闂備胶绮崝鏇烆嚕?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻冪换鍛矆?is None
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻傞～蹇涙偉閹攱_area > 0
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻傞锝団偓鍦text_ratio < 0.1 / 0.3 / 0.6闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熸潏楣冩闁稿顑夐弻鐔煎箲閹伴潧娈弶鈺傜箖缁绘繈濮€閿濆懐鍘梺鍛婃⒐閻楃姾妫㈤梺鍓插亖閸庢煡鎮¤箛娑氬彄闁搞儜灞藉壈闂佽绻楁ご鎼佸Φ閸曨垼鏁冩い鎺戝€婚弳銈夋⒑閸濆嫭婀伴柣鈺婂灡娣囧﹪骞栨担鑲濄劍銇勯弮鍌氬付濠㈢懓鐗撳缁樻媴鐟欏嫬浠╅梺鍛婃煥缁夊爼骞戦姀銈呴唶婵犻潧鐗婂▓鎯р攽椤旂瓔鐒炬繛澶嬬洴閻涱噣濮€鎺虫禍婊堟煙閹规劖纭鹃崯绋款渻?
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - 闂傚倸鍊峰ù鍥х暦閸偅鍙忕€规洖娲﹂浠嬫煏閸繃澶勬い顐ｆ礋閺岋繝宕堕妷銉т痪闂佺顑傞弲娑㈠煘閹达附鍋愰悗鍦Т椤ユ繄绱撴担鍝勑￠柛妤佸▕瀵鈽夐姀鐘栥劑鏌ㄥ┑鍡樺櫣妞ゎ剙顑夊娲箹閻愭彃顬堥梺闈涚墛閹倸顕ｆ繝姘╅柕澶堝灪椤秴鈹戦悙鍙夘棡闁挎艾鈹戦鐟颁壕on_text_ratio闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟犻崑鎾寸箾閻撳海绠伴柕鍡樺笒椤﹤鈻藉顤rea闂?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - image_path: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮煡鏌涘☉鍙樼凹闁诲骸顭峰娲濞戞氨鐤勯梺鎼炲妼閹碱偊鎮鹃柨瀣窞閻庯絻鍔嬬花濠氭⒑閸濆嫮袪闁告柨绉归幆鍐箣閻樼數锛滈梺褰掑亰閸犳牗绂掗柆宥嗙厸閻忕偠顕ф俊濂告煃閽樺妲搁摶锝夋煟閹惧磭宀搁柛娆欑節濮婃椽鎳￠妶鍛勃闂佸憡鍨电紞濠傜暦閹寸偟绡€闁稿本绮嶅▓楣冩⒑闂堚晛鐦滈柛妯绘倐瀹曟垿骞樼紒妯绘珳闂佸憡渚楅崣搴ㄥ汲閵忋垻纾藉ù锝囨嚀婵牓鏌嶉鍡╂r闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - (non_text_ratio, page_type, non_text_region)闂?
          # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸劍閸嬪鐓崶銊р槈缁炬儳顭烽弻锝夊箛椤掍焦鍎撻梺鍛婂灩婵炩偓闁哄本鐩獮鍥濞戞瑧浜梺璇插閼归箖藝娴兼潙桅闁告洦鍨扮粻鎶芥煕閳╁啨浠﹀瑙勬礋閺岋絾鎯旈妶搴㈢秷濠电偛寮堕敋妞ゎ厼鐏濊灒婵炲棛鍋撳В搴ㄦ⒒閸屾瑧顦﹂柟纰卞亰瀹曟劖绻濆顒傦紵闂佹眹鍨婚…鍫㈠婵犳碍鐓欓柟顖嗗苯娈堕梺鑽ゅ枎缂嶅﹪寮诲鍫闂佸憡鎸鹃崰鎰┍婵犲嫧鍋撳☉娅亞娆㈤悙鐑樼厵闂侇叏绠戝鐐繆椤愩垹鏆ｆ慨濠冩そ濡啫鈽夊杈╂澖闂備焦鎮堕崝宀勫Χ閹间焦鏅查柣鎰劋閺呮繈鏌涚仦鍓с€掗柛姗€浜跺娲濞戣京鍔搁梺绋垮濡炶棄鐣烽弴鐑嗗悑濠㈣泛顑囬崢鎾绘偡濠婂嫮鐭掔€规洘顨呴埥澶婎潩椤撗冧缓婵犳鍠楅敃鈺呭礈閿曞倹鍊块柤鎭掑劘娴滄粓鐓崶銊﹀暗濠⒀勬礃娣囧﹪顢曢敐蹇氣偓鍧楁煛鐏炲墽娲村┑锛勫厴閺佹劙宕ㄩ褏鈧挳姊绘担鍛婅础闁稿繑蓱缁傚秹宕奸弴鐐舵憰濠电偞鍨崹褰掑礃閳ь剟鎮峰鍐弰妤犵偛妫濆畷濂稿Ψ閿旀儳骞楅梻浣筋潐閸庢娊顢氶銏犵疇闁搞儮鏂侀崑鎾舵喆閸曨剛顦ラ梺缁樼墪閵堟悂鎮伴鈧畷鍗烆潩閸忓す鈺呮⒒娴ｅ憡鍟為柟鍛婃倐婵″爼骞栨担姝屾憰?""
        try:
            img = cv2.imread(image_path)
            if img is None:
                return 0.0, "unknown", None
            
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            height, width = gray.shape
            
            # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜遍梺绯曞墲椤ㄦ劙鎳撻崸妤€绾ч柛顐ｇ濞呭棙銇勯锝嗙闁靛洤瀚伴獮妯兼崉鐞涒€充壕闁挎繂鎳夊Σ鍫ユ煏韫囨洖啸闁告棑绠戦—鍐Χ閸℃娼戦梺绋款儐閹瑰洤螞?
            edges = cv2.Canny(gray, 50, 150)
            
            # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮繈鏌嶈閸撶喖寮崘顔碱潊闁炽儲鍓氶崵銈夋⒑閸濆嫷妲归柛銊ョ秺钘濋梺顒€绉甸埛鎴︽煕濞戞﹫鍔熺紒鐘虫崌閹顫濋悡搴㈢彎閻庤娲忛崹钘夌暦瑜版帩鏁冮柨娑樺閻ｇ偓淇婇悙顏勨偓鏍礉濡ゅ懎绐楅幖娣灮椤╂煡鏌熼鍡忓亾闁衡偓?(婵犵數濮烽弫鎼佸磻閻樿绠垫い蹇撴缁€濠囨煃瑜滈崜姘辨崲濞戞瑥绶為悗锝庡亞椤︿即鎮楀▓鍨珮闁稿锕ユ穱濠囧醇閺囩偤鍞跺┑鐐村灦椤ㄥ棝鎯堣箛娑欌拻濞撴埃鍋撴繛浣冲懏宕查柛鈩冾殢閻庡墎鎲搁弬璺ㄦ殾闁硅揪绠戠粻濠氭倵濞戞顏堫敁閹剧粯鈷戦柛娑橈攻鐏忔壆鎲搁弶鍨殲濞ｅ洤锕獮搴ㄦ嚍閵夈垺瀚奸梻浣藉吹閸犳劖绔熼崱妯碱浄闁靛繈鍨荤壕鐓庮熆鐠虹尨鍔熼柨娑樼Ф缁?
            # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簻鍥存繝銏ｆ硾椤戝懏绂掗柆宥嗗仭婵犲﹤瀚ˉ鍫⑩偓娈垮枛椤嘲鐣烽妸鈺婃晣鐟滃酣宕撻棃娑辨富闁靛牆妫楃粭鎺楁煕婵犲啯鍊愰柟顕嗙節楠炲鏁傜憴锝嗗闂備礁鎲＄换鍌溾偓姘煎墴椤㈡棃骞橀鐣屽幈闂侀潧顭堥崐鏇炵暤閸℃稒鐓欐い鏍ㄧ懅缁愭梹顨ラ悙宸剶闁轰礁鍊婚幉鎾晲閸℃妯侀梻鍌氬€烽懗鍓佹兜閸洖绀堟繝闈涱儐閸嬶繝鏌ㄩ弴鐐测偓褰掑磻閳哄懏鈷戞い鎺嗗亾缂佸鏁诲畷鎰板醇閺囩姴褰勯梺鎼炲劘閸斿秶浜搁悽鐢电＜闁绘鍎ら鐘电磼鏉堛劌娴€殿噮鍣ｅ畷鎺戭潩椤撶姳绨寸紓?
            kernel_text = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 3))
            text_regions = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel_text)
            
            # 闂傚倸鍊搁崐鐑芥倿閿曞倸绠栭柛顐ｆ礀绾惧潡鏌＄仦璇插姎缁炬儳娼￠弻鐔煎箚閺夊晝鎾绘煕閵堝拋鍎忛棁澶愭煕韫囨挸鎮戠紓宥嗗灩缁辨帡鍩€椤掆偓閻ｆ繈宕熼鍌氬箞婵犵數濞€濞佳兾涘Δ鍜佹晜妞ゆ劧闄勯悡鏇熶繆椤栨繃顏犲ù鐘崇☉鑿愰柛銉戝秷鍚銈冨灪濞茬喖骞冮姀銈嗘啣闁稿本鍑瑰Λ婊勭節?(婵犵數濮烽弫鎼佸磻閻樿绠垫い蹇撴缁€濠囨煃瑜滈崜姘辨崲濞戞瑥绶為悗锝庡亞椤︿即鎮楀▓鍨珮闁稿锕ユ穱濠囧醇閺囩偛鑰垮┑鈽嗗灥閸嬫劘鍊撮梻鍌氬€风粈渚€骞夐敓鐘茬闁硅揪绠戠粈澶娾攽閻樻彃鏆熸い鈺傜叀閺屻劌鈹戦崱鈺傂ч梺缁樻尵閸犳牠寮婚弴鐔虹闁割煈鍠栨慨搴☆渻閵堝懏绂嬮柛瀣濡叉劙骞掑Δ濠冩櫓闂佸憡绻傜€氼剟锝為崶顒佸€垫繛鍫濈仢閺嬬喖鏌熼鐓庘偓鎼佹偩閻戣姤鍋勭痪鎷岄哺閺呮繈姊洪棃娑氱濠殿喚鍏橀幃鐑芥嚑椤掑倻锛?
            kernel_graphic = cv2.getStructuringElement(cv2.MORPH_RECT, (20, 20))
            graphic_regions = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel_graphic)
            
            # 闂傚倸鍊峰ù鍥х暦閸偅鍙忕€规洖娲﹂浠嬫煏閸繃澶勬い顐ｆ礋閺岋繝宕堕妷銉т痪闂佺顑傞弲娑㈠煘閹达附鍋愮€规洖娴傞弳鈥愁渻閵堝啫鍔氶柣妤佹崌瀵鏁愭径濠勭杸濡炪倖甯掗崐褰掑箟婵傚憡鈷戦悹鍥ㄥ絻閻︺劑鏌涢敐蹇曠М鐎殿喖顭烽弫鎾绘偐閺屻儱鏁规俊鐐€栭崝鎴﹀垂瑜版帪缍栭柍鍝勬噺閻撶喖鐓崶銊﹀鞍缂佸倸顑夐弻娑㈠Ω閵婏妇銆愰梺浼欑到閸㈣尙鍙呭銈呯箰閹冲孩绂嶅鍫熷€甸柣鐔告緲椤忣亜顭块悷鐗堫棦闁诡喕鍗虫俊鐑藉Ψ閵忊剝鏉?
            text_area = np.sum(text_regions > 0)
            graphic_area = np.sum(graphic_regions > 0)
            total_area = height * width
            
            # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸劍閸嬪鐓崶銊р槈缁炬儳顭烽弻锝夊箛椤掍焦鍎撻梺鍛婂灩婵炩偓闁哄本鐩獮鍥濞戞瑧浜梺璇插閼归箖藝娴兼潙桅闁告洦鍨扮粻鎶芥煕閳╁啨浠﹀瑙勬礋閺岋絾鎯旈妶搴㈢秷濠电偛寮堕敋妞ゎ厼鐏濊灒婵炲棛鍋撳В?= (闂傚倸鍊搁崐鐑芥倿閿曞倸绠栭柛顐ｆ礀绾惧潡鏌＄仦璇插姎缁炬儳娼￠弻鐔煎箚閺夊晝鎾绘煕閵堝拋鍎忛棁澶愭煕韫囨挸鎮戠紓宥嗗灩缁辨帡鍩€椤掆偓閻ｆ繈宕熼鍌氬箞婵犵數濞€濞佳兾涘Δ鍜佹晜妞ゆ劧闄勯悡?- 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧壕鍦磼鐎ｎ偓绱╂繛宸簼閺呮繈鏌嶈閸撶喖寮崘顔碱潊闁炽儲鍓氶崵銈夋⒑閸濆嫷妲归柛銊ョ秺钘濋梺顒€绉甸埛鎴︽煕濞戞﹫鍔熺紒鐘虫崌閹顫濋悡搴㈢彎閻? / 闂傚倸鍊搁崐宄懊归崶顒夋晪闁哄稁鍘肩粈鍫熺箾閸℃ɑ灏ㄩ柍褜鍓ㄧ粻鎴︽偩閿熺姵鐒介柨鏃€鍎虫慨娲⒒娴ｇ瓔娼愭い鏃€鐗犲畷鏉款潩閼搁潧鍓?
            non_text_area = max(0, graphic_area - text_area * 0.5)
            non_text_ratio = non_text_area / total_area if total_area > 0 else 0.0
            
            # 婵犵數濮烽。顔炬閺囥垹纾婚柟杈剧畱绾捐淇婇妶鍛櫣闁哄绶氶幃褰掑炊瑜庨埢鏇㈡煟閹哄秶鐭欓柡宀€鍠庨埢鎾诲垂椤旂晫浜鹃梺璇茬箰缁绘帡寮繝姘畺鐟滅増甯掗悙濠冦亜韫囨挸顏慨锝冨灲濮婃椽鏌呴悙鑼跺濠⒀傚嵆閺屾稖绠涢弬鍡╀邯閹儳鐣￠柇锔藉兊闁荤姾妗ㄧ紞宥堫樄闁哄本鐩崺鍕礃閸撗冨Ш闂?
            edge_density = np.sum(edges > 0) / total_area
            
            if non_text_ratio < 0.1:
                page_type = "text_only"
            elif non_text_ratio < 0.3:
                page_type = "text_with_diagram"
            elif non_text_ratio < 0.6:
                page_type = "mixed"
            else:
                page_type = "graphic_heavy"
            
            # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块弶鍫氭櫅閸ㄦ繃銇勯弽顐粶缂佲偓婢舵劖鐓涢柛銈呯埣椤ｏ箑效濡ゅ懏鈷戦柟鑲╁仜閸旀挳鏌涢幘鏉戝摵閽樼喖鏌涢鐘插姕闁绘挾鍠栭弻鐔兼焽閿曗偓婢т即鏌熼悾灞解枅闁哄矉绲鹃幆鏃堫敊閸忚偐褰庨梻浣告惈閻鎹㈠┑鍡欐殾濠靛倸澹婇弫鍐煏韫囨洖顎屾繛鐓庮煼濮婅櫣鎷犻懠顒傤唶濠电偛鐡ㄥ畝绋跨暦閸︻厽宕夐柧蹇氼潐濞?
            non_text_mask = cv2.subtract(graphic_regions, text_regions)
            non_text_region = cv2.bitwise_and(img, img, mask=non_text_mask)
            
            return non_text_ratio, page_type, non_text_region
            
        except Exception as e:
            logger.error(f"CV analysis failed: {e}")
            return 0.0, "unknown", None
    
    def _vision_validate(self, image: np.ndarray) -> ConcreteKnowledgeResult:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊峰ù鍥敋瑜忛幑銏ゅ箛椤旇棄搴婇梺褰掑亰閸庨潧鈽夊Ο婊勬閸┾偓妞ゆ帒瀚粻鎺撶節濞堝灝鏋熼柕鍥ㄧ洴瀹曟垿骞橀崹娑樹壕閻熸瑥瀚粈鍐煙闁稖鍏岀紒顔款嚙閳藉濮€閻樻彃绁梻浣虹帛椤洭宕戦妸锔绢浄闁靛繈鍊栭埛鎺懨归敐鍫綈闁靛洨鍠栭弻娑㈡偐閾忣偆顦ュ銈庝簻閸熸挳鐛幒妤€绫嶉柍褜鍓涢埀?base64 濠电姴鐥夐弶搴撳亾濡や焦鍙忛柣鎴ｆ绾惧鏌ｉ幇顒佹儓缁炬儳鐏濋埞鎴﹀磼濮橆厼鏆堥梺绋块缁夊綊寮婚敐澶婃闁割煈鍠楅崐顖炴⒑?ERNIE Vision API闂?
        # 2) 婵犵數濮烽弫鎼佸磻濞戙埄鏁嬫い鎾跺枑閸欏繘鎮楀☉娆欎緵婵炲牅绮欓弻鐔兼⒒鐎靛壊妲紓浣哄Т缂嶅﹤顫忓ú顏勫瀭妞ゆ洖鎳庨崜閬嶆⒑缁嬫寧鎹ｉ柛鐘崇墵瀵鏁撻悩鑼紲濠电偞鍨堕…鍥囬妸鈺傗拺闁告稑顭笟娑㈡煕閹捐泛鏋涚€殿喖顭烽幃銏ゆ惞閸︻叏绱叉繝纰樻閸ㄤ即骞栭锔肩稏鐎光偓閸曨剙浠?JSON 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸婂潡鏌ㄩ弴鐐测偓鍝ョ不閺夊簱鏀介柣妯虹－椤ｆ煡鏌涙繝鍛【妞ゎ厼娼￠幊婊堟濞戞﹩娼曢柣搴ゎ潐閹搁娆㈠璺鸿摕闁绘梻鈷堥弫宥夋煟閹邦剦鍤熼柣婵囧姍濮婃椽宕烽鐐插濡炪們鍔岄幊妯虹暦濞嗘挻鍋愮紓浣诡焽閸樼敻姊绘笟鍥у伎缂佺姵鍨堕弲鑸靛鐎涙鍘甸梺浼欑到閼活垶寮搁幋鐘电＜缂備焦顭囧ú瀵糕偓瑙勬礀閵堝憡鎱ㄩ埀顒勬煟濡搫鑸归悗鍨叀濮?
        # 3) 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佺粯鍨堕弸鑽ょ礊閺嵮岀唵閻犺櫣灏ㄩ崝鐔兼煛閸℃劕鈧洟婀侀梺鎸庣箓閻楀﹪顢旈悩鐢垫／闁告挆鍐у闂侀潧娲ょ€氭澘顕ｉ鈧畷鍓佹崉閻戞ɑ姣岀紓鍌氬€峰ù鍥敋瑜嶈灋婵犻潧顑囧畵渚€鏌″鍐ㄥ缂傚秴娲弻娑㈠箻濡も偓閹虫劙鎮伴妷褏纾介柛灞剧懅閸斿秵銇勯妸銉﹀殗闁诡垰鐭傚畷鎺戭潩鏉堛劍顔曟繝寰锋澘鈧洟骞婅箛娑樼；妞ゅ繐鎳屾禍婊堟煛瀹ュ啫濡跨紒鐘崇墬缁绘繈濮€閿涘嫬鈷屽┑顔硷攻濡炶棄鐣烽锕€绀嬫い鎾愁槶閸ㄨ櫣鎹㈠☉妯忓湱鈧綆鍋呴悵姘渻閵堝啫鐏柣鐔叉櫅閻ｇ兘宕奸弴銊︽櫌婵炶揪缍侀弲鑼磽閹剧粯鈷掑ù锝堟鐢稒鎱ㄦ繝鍌ょ吋闁炽儻绠撳畷濂稿Ψ閿曗偓閳ь剙鐖奸弻娑㈠Ψ閵忊剝鐝栫紒鐐礃濡嫰婀侀梺鎸庣箓閻楁粌顭囬幇鐗堝€堕煫鍥ч瀹撳棝鏌″畝瀣К缂佺姵鐩獮娆撳礃閳圭偓鐎版繝?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁敓鐘偓渚€寮介鐐茬彉濡炪們鍎抽惁?闂傚倸鍊峰ù鍥х暦閸偅鍙忛柟缁㈠櫘閺佸嫰鏌涘☉娆愮稇闁汇値鍠栭湁闁稿繐鍚嬬紞鎴︽煛?+ JSON 闂傚倸鍊峰ù鍥х暦閻㈢绐楅柟鎵閸嬶繝寮堕崼姘珔缂佽翰鍊曡灃闁挎繂鎳庨弳鐐烘煕?+ 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸缁愭鎱ㄥΟ鎸庣【缂佹劖顨婇弻锟犲炊閳轰焦鐏侀梺宕囨嚀缁夋挳鍩為幋锔藉亹闁圭粯甯楀▓鏌ユ⒑缁嬫寧鎹ｉ柛鐘崇墵瀵濡搁埡浣稿祮闂佺粯鍔栫粊鎾磻閹惧箍浜归柟铏瑰仧缁嬪繘鎮楅崗澶婁壕闂佸憡娲﹂崜娑㈠储鏉堛劎绡€闁汇垽娼у瓭闁诲孩鍑归崜娑㈠极椤曗偓瀹曞ジ寮撮悢鍝勫箞闂備線娼ч…鍫ュ磹濡ゅ懏鍎楁繛鍡樺灍閸嬫挸鈻撻崹顔界彯闂佸憡鎸鹃崰鏍Υ娴ｈ倽鐔烘偘閳╁啯鏆婇梻浣筋嚃閸ㄥ酣宕掑鍏碱棥?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堝煕閹达附鐓曢柨鏃囶嚙楠炴﹢鎮介娑氭创闁哄本鐩顕€鍩€椤掑倹鏆滈柣鎰ゴ閺?Vision AI 闂傚倸鍊搁崐鐑芥倿閿曞倹鍎戠憸鐗堝笒缁€澶屸偓鍏夊亾闁逞屽墴閸┾偓妞ゆ帊绀侀崵顒勬煕閻樺磭澧遍柛娆忔嚇濮婅櫣绮旈崱妤€顏存繛鍫熸⒒缁辨帡鎮╅崘娴嬫灆闂佸搫鏈ú妯侯嚗閸曨垰閱囨繝闈涙椤捇姊绘担绛嬪殭缂佺粯鍨块幃锟犳晸閻樿尙鐣哄┑掳鍊曢崯顖炲窗閸℃稒鐓曢柡鍥ュ妺缁ㄧ兘鎮楀☉鎺撴珚婵﹤鎼叅閻犲洦褰冪粻娲⒑缁嬪灝顒㈠┑鐐诧躬閵嗕礁顫濈捄浣曘劑鏌嶆潪鎷屽厡妞わ缚鍗冲娲川婵犲啫鐭梺鐓庡暱閻栬壈妫熼梺鍐叉惈閹冲繘鍩涢幋锔界厱婵犻潧妫楅鈺呮煛娴ｇ懓鈻曢柡灞剧☉閳藉宕￠悙鎻掝劀闂備礁鎼Λ顓㈠矗閸愵煈娼栭柧蹇撴贡閻瑩鎮归幁鎺戝妞ゆ柨锕娲川婵犲海鍔堕梺鎼炲劥閸╂牠寮查埡鍛拺閻熸瑥瀚ˉ瀣熆瑜庨〃濠傤嚕閺屻儺鏁嗛柍褜鍓熼垾锔炬崉閵婏箑纾梺鎯х箳閹虫捇銆傞悽鍛娾拺?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗?```json' in content
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗?```' in content
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻堝畷鑼姳婵傜棗oncrete_knowledge == "闂?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悢婵嗘搐缁犵儤銇勮椤ㄥセidence >= 0.5
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - API 闂傚倸鍊峰ù鍥敋瑜忛埀顒佺▓閺呮繄鍒掑▎鎾崇婵＄偛鐨烽崑鎾诲礃椤旂厧鑰垮┑鐐村灱妞存悂寮查埡鍛€甸柛蹇擃槸娴滈箖姊洪崨濠冨闁告挻鑹鹃埢宥夊冀瑜夐弨鑺ャ亜閺冨倹娅曠紒鐘崇墪铻栨俊銈咃攻閻濈ces[0].message.content闂?
        # - 闂傚倸鍊峰ù鍥х暦閻㈢绐楅柟鎵閸嬶繝寮堕崼姘珔缂佽翰鍊曡灃闁挎繂鎳庨弳鐐烘煕婵犲嫭鏆柡宀嬬秮閺佹劙宕ㄩ褎校闂備胶顭堥鍡涘箰閹间礁鐓″璺号堥弸搴繆椤栨繂鍚归柡鍡檮娣囧﹪鎮欓鍕ㄥ亾閺嶎厼钃熼柕濞炬櫆閸庢鏌涘畝鈧崑娑㈠几娴ｈ　鍋撻獮鍨姎闁瑰嘲顑呴…鍥ㄥ鐎涙鍘撻梺缁樺灦閿氶柣婵囶浂concrete_knowledge闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟悧鍡欑不閹烘鐓熼柣妯活問閻撶idence闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟悧鍡欑不閹烘鐓熼柣妯活問閻撶rete_type闂傚倸鍊搁崐椋庢濮橆剦鐒界憸宥堢亱闂佸搫鍟崐鍦偓姘煼閺岋綁鎮╅崹顐㈢毆son闂?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - image: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鎻掔€梺绋跨箰閸氬宕ｈ箛娑欑厪闁割偅绻嶅Σ鍛婃叏鐟欏嫮鍙€闁哄矉缍佸顕€宕掑顑跨帛缂傚倷璁查崑鎾绘煕瀹€鈧崑鐐烘偂韫囨搩鐔嗛悹楦挎婢ф洟鏌涢弮鈧喊宥嗙┍婵犲浂鏁冮柨婵嗙箺閳ь剙娼￠弻锛勪沪鐠囨彃顬堥梺瀹犳椤︻垵鐏掗梺缁樻尭缁ㄥ爼宕ｉ敓鐘斥拺闁煎鍊曟禒锕傛煕閹存繄绉虹€规洘鍨剁换婵嬪磼濠婂嫭顔曢梻渚€娼荤€靛矂宕㈤幖浣哥；闁瑰墽绮弲鏌ユ煕濞戝崬寮鹃柡鍡愬灮缁辨挻鎷呴悷鏉款潔濡炪們鍊曢崺?ndarray闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - ConcreteKnowledgeResult闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熸潏楣冩闁稿顑夐弻娑㈠焺閸愵亖濮囬梺绋款儏鐎氫即寮诲鍫闂佸憡鎸婚悷鈺侊耿?Vision API 闂傚倸鍊搁崐鐑芥倿閿曞倹鍎戠憸鐗堝笒缁€澶屸偓鍏夊亾闁逞屽墴閸┾偓妞ゆ帊绀侀崵顒勬煕閹捐泛鏋庨柣锝囧厴閺佹劙宕堕妸銉э紡闂備線娼ч…顓犵不閹达富鏁嬮柨鐔哄У閳锋垿鏌ｉ幇顖涱棄闁告梹宀搁弻娑㈡偄闁垮浠撮梺璇″枟閿曘垹顕ｆ繝姘ㄩ柨鏇楀亾闁逞屽墰閺佸骞冨畡鎵虫瀻闊洦鎼╂导鈧梻浣告憸閸犳洜浜稿▎鎴烆潟闁圭儤鎸荤紞鍥煏婵炲灝鍔氶柟钘夌仛缁?""
        try:
            # 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鎲搁悧鍫濈瑲闁稿顑嗙换婵囩節閸屾粌顤€闂佺顑呴ˇ鎵崲濠靛洨绡€闁稿本绮岄。娲⒑缂佹ê濮囬柨鏇ㄤ邯瀵鈽夐姀鐘殿啋濠德板€愰崑鎾绘煕閻樺啿鍝虹€殿喗濞婇、妤佹媴閾忚鍟?base64
            _, buffer = cv2.imencode('.png', image)
            image_base64 = base64.b64encode(buffer).decode("utf-8")
            
            # ERNIE Vision 濠电姷鏁告慨鐑藉极閹间礁纾婚柣鎰惈閸ㄥ倿鏌ｉ姀鐘冲暈闁稿顑呴埞鎴︽偐閹绘帗娈銈嗘礋娴滃爼寮诲☉妯锋婵炲棙鍔楃粙鍥⒑閸涘﹤濮€闁稿鎹囧缁樻媴閾忕懓绗￠梺鐟版憸椤牓婀佹俊鐐差儏鐎涒晠宕楀鍫熺厓鐟滄粓宕滈悢濂夋綎婵炲樊浜滅粻浼村箹鏉堝墽宀涘┑顔兼搐閳规垿顢欑憴鎺撶矒瀹曟繈骞嬮敃鈧拑鐔兼煕椤愮喐鍣伴柛瀣崌濡啫鈽夊▎鎰瀾缂備胶鍋撻崕鎶藉Χ閹间礁钃熼柨婵嗩槸缁秹鏌涚仦璇测偓妤呭礉閸濄儳纾藉ù锝囨嚀缁茬粯绻濋埀顒佹綇閳哄偆娼熼梺鍦劋椤ㄥ懘鎮為崹顐犱簻闁瑰搫绉瑰鐑芥煕鐎ｎ亷韬柟顔肩秺楠炰線骞掗幋婵愮€撮柣?+ 闂傚倸鍊搁崐鐑芥倿閿曗偓椤啴宕归鍛姺闂佺鍕垫當缂佲偓婢跺备鍋撻獮鍨姎妞わ富鍨跺浼村Ψ閿斿墽顔曢梺鐟邦嚟閸嬬喖骞婇崨顔轰簻闁冲搫鍟崢鎾煛鐏炲墽鈽夐柍钘夘樀瀹曪繝鎮欏顔介獎闂傚倷鑳剁划顖炲礉閺囥垺鏅濇い蹇撶墕閽冪喐绻涢幋娆忕仼閸烆垶姊洪棃娑辨Ф闁稿寒鍣ｉ獮鎰板礃閳哄啰鐦堥梺鍐茬殱閸嬫捇鏌涢弴銊ュ箹闁冲嘲鐭傚鐑樻姜閹殿噮妲紓浣割槹閹告娊骞冮幆褉鏀介悗锝庝簽椤︽澘顪冮妶鍛婵☆偅绋撶划鍫ュ幢濞戞瑢鎷绘繛杈剧导鐠€锕傛倿閸撗呯＜缂備焦锚婵绱掗鑺ヮ棃闁诡喚鏅崰濠偽熸潪鏉款棜闂備礁澹婇悡鍫ュ磻閸℃稏鈧倿鎳犻鍌滐紲闂佺鏈粙鎴澝归灏栨斀闂勫洭宕洪弽褜鍤楅柛鏇ㄥ墰缁♀偓闂佸憡鍔曡ぐ鐐哄箣闁垮绡€闁汇垽娼цⅷ闂佹悶鍔庨崢褔鍩㈤弬搴撴婵浜悡瀣倵鐟欏嫭绀€婵炶绠撻敐鐐哄即閵忥紕鍘卞銈嗗姧缁插墽绮堥崘顔界厽?
            messages = [
                {
                    "role": "system",
                    "content": self._concrete_knowledge_system_prompt,
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{image_base64}"
                            }
                        }
                    ]
                },
            ]
            
            payload = {
                "model": self._vision_model,
                "messages": messages,
                "temperature": self._temperature,
            }
            
            # 闂傚倸鍊峰ù鍥х暦閸偅鍙忛柟缁㈠櫘閺佸嫰鏌涘☉娆愮稇闁汇値鍠栭湁闁稿繐鍚嬬紞鎴︽煛?ERNIE Vision API
            with httpx.Client(timeout=60.0) as client:
                response = client.post(
                    self._base_url,
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {self._bearer_token}",
                        "Content-Type": "application/json"
                    }
                )
                response.raise_for_status()
                data = response.json()
            
            content = data["choices"][0]["message"]["content"].strip()
            
            # 闂傚倸鍊峰ù鍥х暦閻㈢绐楅柟鎵閸嬶繝寮堕崼姘珔缂佽翰鍊曡灃闁挎繂鎳庨弳鐐烘煕?JSON
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            
            result = json.loads(content)
            
            confidence = float(result.get("confidence", 1.0))
            img_description = str(
                result.get("img_description")
                or result.get("img_desription")
                or result.get("description")
                or result.get("caption")
                or result.get("reason", "")
            ).strip() or "description unavailable"
            
            return ConcreteKnowledgeResult(
                has_concrete=True,
                has_formula=False,
                confidence=confidence,
                concrete_type=str(result.get("concrete_type", "visual_description") or "visual_description"),
                reason=str(result.get("reason", "Vision AI description") or "Vision AI description"),
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=True,
                img_description=img_description,
            )
            
        except Exception as e:
            logger.error(f"ERNIE Vision validation failed: {e}")
            # Fallback: 婵犵數濮甸鏍窗濡ゅ啯鏆滄俊銈呭暟閻瑩鏌熼悜妯镐粶闁逞屽墾缁犳挸鐣锋總绋课ㄦい鏃囧Г濞呭秶绱撻崒姘偓鐑芥倿閿曞倵鈧箓宕堕鈧悡妯尖偓骞垮劚濡稓寮ч埀顒勬⒒閸屾氨澧涚紒瀣尰閺呰泛鈽夊▎鎴狀啎?
            return ConcreteKnowledgeResult(
                has_concrete=True,
                has_formula=False,
                confidence=0.5,
                concrete_type="formula",
                reason="detected formula, include screenshot",
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=True,
                img_description="description unavailable"
            )
    
    def _cv_only_validate(self, non_text_ratio: float, cv_page_type: str) -> ConcreteKnowledgeResult:
        """Docstring omitted."""
        # 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉甸崑锟犳煙閹増顥夋鐐灲閺屽秹宕崟顐熷亾瑜版帒绾ч柟闂寸劍閳锋帡鏌涚仦鍓ф噮閻犳劒鍗抽弻娑氣偓锝庝簼閸ｅ綊宕￠柆宥嗙厵闁绘垶蓱閳锋劖銇勯幇顖毿撻柕鍥у楠炲洭宕奸弴鐕佲偓宥夋⒑?
        # 1) 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佺粯鍨堕弸鑽ょ礊閺嵮岀唵閻犺櫣灏ㄩ崝鐔兼煛?non_text_ratio 缂傚倸鍊搁崐鎼佸磹妞嬪孩顐介柨鐔哄Т缁愭淇婇妶鍛櫡闁逞屽墮閸熸潙鐣烽妸鈺佺骇闁瑰瓨绻勯埀顒夊弮閺岀喖宕楅崗鐓庡壒濠电偛鍚嬬€笛呯矉瀹ュ拋鐓ラ柛顐ゅ枔閸樹粙姊洪棃娑氱濠殿喚鏁诲畷顖炴倷缂堢姷绠氬┑锛勫仧閸樠勪繆鐠恒劎纾奸弶鍫涘妼濞搭喗銇勯姀鈩冪濠碉紕鍏橀、娆撳礂绾板崬鏁抽梻鍌氬€峰ù鍥敋瑜嶉湁闁绘垼妫勭粈鍌涗繆椤栨繍鍞虹紒?
        # 2) 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉埀顒婄畵瀹曞ジ濡风€ｎ亝鍠樻俊顐㈠暙闇夌紒娑樻贡缁夘喚鈧娲栭悥濂搞€佸Δ鍛劦妞ゆ帒鍊搁ˉ姘辨喐閻楀牆绗氶柣鎾跺У缁绘盯寮堕幋顓炲壉濠电偛鎳庡ú顓㈠蓟閵堝洨鐭欓悹鎭掑妼閻撶喖姊洪崫鍕拱闁烩晩鍨堕悰顔锯偓锝庡枟閺呮煡鏌涢埄鍐ㄥ闁稿鎹囧畷姗€顢旈崱娆欑床缂傚倸鍊烽悞锕傛晪闂佸憡绻冨浠嬪蓟?闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼唶闁荤姴娲ゅ顒€鈽夐姀鐘殿槰闂侀潧顭堥崹娲倵濞差亝鐓欓柤娴嬫櫈钘熷┑鈩冨絻閹虫ê鐣烽悽绋块唶婵犮埄浜濆Λ鍐极閸屾粎椹抽悗锝庝簻婵″ジ姊绘担鍛婃喐闁稿鍋ら獮鎰板箮閽樺鎽?
        # 闂傚倸鍊峰ù鍥敋瑜庨〃銉х矙閸柭も偓鍧楁⒑椤掆偓缁夊澹曟繝姘厽闁哄啫娲ゆ禍鍦偓瑙勬尫缁舵岸寮诲☉銏犖ㄦい鏃傚帶椤晛鈹戦埥鍡椾簼闁挎洏鍨藉濠氭晲婢跺浜滅紓浣割儐椤戞瑥螞閸℃瑧纾肩紓浣靛灩楠炴劙鏌涚€ｎ偅宕屾慨濠勭帛閹峰懘鎮烽柇锕€娈濈紓鍌欑椤戝棛鏁幒妤嬬稏婵犻潧顑呮儫闂佸啿鎼崐鍛婄閻撳簶鏀介柣妯肩帛濞懷勩亜閹存繃鍣芥繛鎴犳暬閸┾偓妞ゆ帒瀚埛鎴︽煕濠靛棗顏柛灞诲姂閺屾盯濡搁敂濮愪虎闂佹寧绻勯崑娑㈡偩濠靛绀嬫い鎺戝€搁獮鍫濃攽閻樺灚鏆╁┑顔芥尦楠炲﹥鎯旈妸瀣槸椤撳ジ宕遍幇顏嗙泿婵＄偑鍊栭崝鎴﹀春閸曨倠鐔煎焵椤掑嫭鍊甸悷娆忓缁€鍐煠瑜版帞鐣洪柛鈹垮灪閹棃鍩ラ崱妤佸€┑鐐舵彧缂嶄線寮查崣澶岀彾?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú宥夊焵椤掑﹦鐣电€规洖銈告慨鈧柕蹇嬪灩椤︹晠姊绘担铏瑰笡闁告棑绠撳畷婊冣槈椤兘鍋撻崨顕呮Ч閹煎瓨锚娴滈箖鏌ｉ悢鍛婄凡妞ゅ浚浜滈…鍧楁偡闁箑鍓堕悗瑙勬礃閸ㄥ潡鐛鈧幊婊堟濞戞ê绠叉繝纰夌磿閸嬫盯顢栭崨顒煎綊鎮滈懞銉ヤ粧闂侀潧顦弲婊堝煕閹达附鈷掗柛顐ｇ濞呭牏绱掗埀顒傗偓锝庡枟閻撴洟鏌￠崒婵囩《鐎涙繈鏌?Vision AI 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢敃鈧悿顕€鏌熼幆鐗堫棄闁哄嫨鍎甸弻鈥愁吋鎼粹€冲箥婵炲瓨绮庨幊鎾绘箒闂佹寧绻傞幊蹇曟嫻閿熺姵鐓熼柣鏃€妞垮鎰磼缂佹娲存鐐差儔閹ê煤鐠佸磭鏁栭梺鐟板槻椤戝顕ｆ繝姘ㄩ柨鏇楀亾濞寸媭鍘奸埞鎴︽倷閸欏妫￠梺鑽ゅ暀閸涱垳鐓嬪銈嗘磵閸嬫捇鏌＄仦鍓ф创闁轰焦鍔欏畷鍗炍熼崫鍕暘闂傚倷娴囬鏍垂娴兼潙鐤柡澶嬪灩閺嗭箓鏌ｉ弮鍌楁嫛闁轰礁鍟撮弻銊モ攽閸℃銈╁┑鈥虫▕閸撶喎顫忔繝姘＜婵炲棙鍨肩粣妤呮⒑缁嬫鍎岄柛瀣崌濮婅櫣绮欏▎鎯у壉闂佸湱顭堟晶钘壩ｉ幇鏉跨闁瑰瓨姊归～宥呪攽閻愬弶顥為柛銊ф暬閸╂盯宕奸妷锔规嫼闂佸憡绻傜€氼參藟閻愮儤鐓曢柡鍐ｅ亾闁荤噦闄勭粚?
        # 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佸搫娲ㄦ慨鎾夊顓滀簻闁规儳宕悘顏堟煕鐎ｃ劌鈧繈寮婚弴鐔风窞闁糕剝蓱閻濇洟姊虹紒妯虹瑨妞ゎ厾鍏樺濠氭晸閻樿尙顦板銈嗗姂閸婃顢欐繝鍥ㄧ厽闁靛繆鏅涢悘鐘充繆椤愶絿绠炵€?
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻傞锝団偓鍦text_ratio >= 0.4
        # - 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曚綅閸ヮ剦鏁冮柨鏇楀亾闁绘帒鐏氶妵鍕箳閸℃ぞ澹曢柣搴㈩問閸犳牠鎮ユ總鎼炩偓渚€寮撮悢渚祫闁诲函缍嗛崑鍡涘矗閸℃せ鏀介柣鎰綑閻忥箓鏌ｉ悤浣哥仸鐎规洘绻傞锝団偓鍦text_ratio >= 0.2
        # 婵犵數濮烽弫鎼佸磻閻愬搫绠伴柤濮愬€曢弸鍫⑩偓骞垮劚濞诧箑鐣烽弻銉︾厱闁规壋鏅涙俊鍧楁煛閸℃劕鈧洟濡撮幒鎴僵闁挎繂鎳嶆竟鏇㈡煟鎼淬埄鍟忛柛鐘崇墵閹儵宕楅梻瀵哥畾闂佺粯鍨兼慨銈夊疾濠婂牊鐓曢柟鐐殔閸熶即宕㈤幘缁樷拻闁稿本鐟ч崝宥夋倵缁楁稑鎳愰惌娆撴煙鏉堥箖妾柛瀣儔閺岀喖骞嶉纰辨毉闂佺楠搁敃銈夆€﹂崸妤佸殝闂傚牊绋戦～宀€绱撴担鍝勑㈢€殿喖鐖兼俊鐢稿礋椤栨艾宓嗗┑掳鍊愰崑鎾趁瑰鍫㈢暫婵﹨娅ｅ☉鐢稿川椤曞懏顥夐梻渚€娼ч悧濠囧箖閸屾凹鍤曟い鎰╁焺閸氬鏌涢埄鍐炬畼缂佹劗鍋涢埞鎴︽倷閺夋垹浠稿┑顔角滈崝鎴﹀箖?
        # - CV 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉埀顒婄畵瀹曠厧顭垮┑鍥ㄣ仢闁轰礁鍟村畷鎺戔槈濡懓鍤遍梻鍌欑劍鐎笛呮崲閸岀倛鍥ㄥ閺夋垹鏌у銈嗗笂閻掞箓宕ｈ箛鏃傜瘈闂傚牊绋撴晶娑㈡煥濞戞瑦鍋text_ratio闂?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煟椤撶噥娈滈柡灞剧洴閸╁嫰宕橀浣诡潔闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - non_text_ratio: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鎻掔€梺绋跨箰閸氬宕ｈ箛娑欑厪闁割偅绻嶅Σ鍛婃叏鐟欏嫮鍙€闁哄矉缍佸顕€宕掑顑跨帛缂傚倷璁查崑鎾绘煕瀹€鈧崑鐐烘偂韫囨搩鐔嗛悹楦挎婢ф洟鏌涢弮鈧喊宥嗙┍婵犲浂鏁冮柨婵嗙箺閳ь剙娼￠弻锛勪沪鐠囨彃顬堥梺瀹犳椤︻垵鐏掗梺缁樻尭缁ㄥ爼宕ｉ敓鐘斥拺闁煎鍊曟禒锕傛煕閹存繄绉虹€规洘鍨剁换婵嬪磼濠婂嫭顔曢梻渚€娼荤€靛矂宕㈤幖浣哥；闁瑰墽绮弲鏌ユ煕濞戝崬寮鹃柡鍡愬灮缁辨挻鎷呴悷鏉款潔濡炪倧绲块崡鎲僡t闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # - cv_page_type: 闂傚倸鍊搁崐椋庣矆娓氣偓楠炲鏁撻悩鎻掔€梺绋跨箰閸氬宕ｈ箛娑欑厪闁割偅绻嶅Σ鍛婃叏鐟欏嫮鍙€闁哄矉缍佸顕€宕掑顑跨帛缂傚倷璁查崑鎾绘煕瀹€鈧崑鐐烘偂韫囨搩鐔嗛悹楦挎婢ф洟鏌涢弮鈧喊宥嗙┍婵犲浂鏁冮柨婵嗙箺閳ь剙娼￠弻锛勪沪鐠囨彃顬堥梺瀹犳椤︻垵鐏掗梺缁樻尭缁ㄥ爼宕ｉ敓鐘斥拺闁煎鍊曟禒锕傛煕閹存繄绉虹€规洘鍨剁换婵嬪磼濠婂嫭顔曢梻渚€娼荤€靛矂宕㈤幖浣哥；闁瑰墽绮弲鏌ユ煕濞戝崬寮鹃柡鍡愬灮缁辨挻鎷呴悷鏉款潔闂佹剚浜為ˉ婕撮梻鍌氬€搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熷▎陇顕уú顓€佸鈧慨鈧柣姗€娼ф慨?
        # 闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜辨繝鐢靛Т濞层倗绮绘导瀛樼厵闂傚倸顕ˇ锕傛煕濮樻剚娼愰柕鍥у楠炴﹢宕￠悙鍏告偅闂備焦妞块崢浠嬨€冩繝鍥ц摕婵炴垶鐭▽顏堟煕閹炬せ鍋撳┑顔兼搐閳规垿鎮欓懠顒€顣洪梺缁樼墪閵堢顕?
        # - ConcreteKnowledgeResult闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噽閻瑩鏌熸潏楣冩闁稿顑夐弻娑㈠焺閸愵亖濮囬梺绋款儏鐎氫即寮诲鍫闂佸憡鎸婚悷鈺侊耿娴ｇ硶鏀介柣妯款嚋瀹搞儵鏌涢悢鍛婂唉鐎规洩缍佸畷鐔碱敆閸屾粠鍟庣紓浣哄亾濠㈡銇愭径鎰劷妞ゆ牗澹曢崑鎾斥枔閸喗鐝梺鍛婃尵閸犲酣鎮鹃悜钘夐唶闁哄洨鍊ｉ埡鍛叆闁哄啫娲﹂ˉ澶嬨亜韫囧骸宓嗘慨濠勭帛閹峰懘宕ㄦ繝鍌涙畼婵＄偑鍊戦崝宀勬偋婵犲洤绠為柕濞炬櫆閸嬨劑鏌涘☉姗堝伐闁告挶鍔岄—鍐Χ閸℃锛曢梺绋款儐閹瑰洭寮婚悢纰辨晬婵﹩鍓氬▓濠氭倵鐟欏嫭绀堥柛鐘崇墵閵嗕線寮撮姀鐙€娼婂銈庡亽閸樺墽绮堥崟顖涒拻濞达絿鐡旈崵娆愭叏濮楀牏鐣甸柨婵堝仦瀵板嫰骞囬鐐扮盎婵＄偑鍊栫敮鎺楀磹瑜版帪缍栭柡鍥╁枑閸欏繑淇婇娑橆嚋缁绢厾鍋撳?""
        # 闂傚倸鍊搁崐鐑芥嚄閸撲焦鍏滈柛顐ｆ礀閻ら箖鏌ｉ幇顒佲枙婵炲瓨鐗犻弻鏇熺箾瑜嶅Λ妤€鐨┑锛勫亼閸婃牜鏁幒鏂哄亾濮樼厧寮挊鐔兼煕椤愮姴鍔滈柣鎾跺枛閺岀喖鏌囬敃鈧晶浼存煙閻ｅ苯鈻堥柡宀嬬稻閹棃顢欓崗鑲╁綆闂備礁鎼惌澶屾崲濠靛棛鏆﹀┑鍌氬閺佸啴鏌曡箛鏇烆€屾繛鐓庮煼濮婅櫣鎷犻懠顒傤唶濠电偛鐡ㄥ畝绋跨暦閸︻厽宕夐柧蹇氼潐濞堟儳鈹戦悙鍙夆枙濞存粍绻堝畷鎴﹀礋椤栨稓鍘卞┑鐘绘涧鐎氼剟宕濆鍫濈柈缂佸绨遍弨浠嬫煟閹邦剛鎽犻悘蹇庡嵆閺屻倛銇愰幒鏃傛毇濡炪們鍨洪悧鐘茬暦閵娾晛绾ч柟瀛樼箘閳ь剦鍘界换娑氣偓鐢殿焾瀛濆銈嗗灥閹冲氦鐏?
        if non_text_ratio >= 0.4:
            return ConcreteKnowledgeResult(
                has_concrete=True,
                has_formula=False,
                confidence=0.6,
                concrete_type="formula",
                reason="detected formula, include screenshot",
                is_mixed=True,
                non_text_ratio=non_text_ratio,
                should_include=True
            )
        elif non_text_ratio >= 0.2:
            return ConcreteKnowledgeResult(
                has_concrete=False,
                has_formula=False,
                confidence=0.5,
                concrete_type="formula",
                reason="detected formula, include screenshot",
                is_mixed=True,
                non_text_ratio=non_text_ratio,
                should_include=True  # 濠电姷鏁告慨鐑姐€傞挊澹╋綁宕ㄩ弶鎴濈€梺浼欑到閺堫剟锝為弴銏＄厸闁搞儮鏅涙牎闂佺绻戠划鎾诲蓟閵娾晛鍗抽柣鎰ゴ閸嬫捇宕妷褍鏆楅悗骞垮劚椤︿即鎮″☉銏＄厱妞ゆ劗濮撮悘顏堟煛閸″繑娅囩紒杈ㄥ浮閹晜娼忛埡鍐幆闂?
            )
        else:
            return ConcreteKnowledgeResult(
                has_concrete=False,
                has_formula=False,
                confidence=0.7,
                concrete_type="formula",
                reason="detected formula, include screenshot",
                is_mixed=False,
                non_text_ratio=non_text_ratio,
                should_include=False
            )
    
    def _default_result(self, should_include: bool) -> ConcreteKnowledgeResult:
        """Build a safe fallback result."""
        return ConcreteKnowledgeResult(
            has_concrete=False,
            has_formula=False,
            confidence=0.0,
            concrete_type="formula",
            reason="detected formula, include screenshot",
            is_mixed=False,
            non_text_ratio=0.0,
            should_include=should_include,
        )

# ==============================================================================
# Test Entry
# ==============================================================================



