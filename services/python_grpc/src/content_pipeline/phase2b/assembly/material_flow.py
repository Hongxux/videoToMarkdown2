"""
Phase 2B 缂備浇浜慨闈涱焽濡も偓闇夊ù锝堝Г缁侇喖鈽夐幘铏攭妞ゃ儱锕弻濠傤吋閸狅絼绶氬畷?(Material Flow Assembly)

闂佸搫鐗滈崜姘冲暞闂佺鍕垫當闁汇倕瀚伴幃铏紣娴ｅ搫鏅?RichTextPipeline 婵炴垶鎼╅崢浠嬫偋鐎圭姷鐤€闁告劦浜濋悾杈╃磼鏉堚晛校婵炲吋顨嗗鍕礋椤撶喎鈧偤姊洪锝嗩潡缂侀鍋婇弫宥呯暆閸愵亶妲归柣鐘冲姧缁蹭粙鎯冮姀銏″珰妞ゆ挻绻勯悿鍛存煕濡や焦绀€闁?(SemanticUnit)
闁哄鍎愰崜娆戔偓闈涘级缁嬪宕崟顐㈢樊婵炶揪绲鹃幐鎯р枔閹寸姭鍋撻棃娑滃闁绘稓鍠愰幏鍛村箻閸撲緡妲洪梺鍝勵槹閸斞呮濞嗘挸绠ｆい蹇撳缁傚牓鏌曢崱鏇熺グ妞ゎ偄顑嗛敍鎰板箣閻樺樊鏆┑鐐茬墢椤ｎ喚妲愬璺何?
婵炴垶鎸诲Σ鎺椼€呴敃鍌氱闁绘棁顕ч崢瀵哥磽娴ｇ顏ф繛鍡愬灲閺?1. generate_materials: 闂佺儵鏅涢悺銊ф暜閹绢喖绠ョ憸鎴︺€侀幋鐘愁潟闁绘娅曠紞蹇涙煟閵忋垹鏋戦柛銊﹁壘闇夊ù锝堛€€閺屻倝鏌ㄥ☉妯煎ⅵ闁逞屽墮閸婂寮妶鍡欘洸閹肩补鍓濋煬顒傜磼?闂佸憡顨夊▍鏇烆渻閸屾稑绶為柛鏇ㄥ幗閸婄偞淇婇妞诲亾瀹曞洨顢呴梺鎸庣☉椤︻參鍩€?2. collect_material_requests: 闁荤喐鐟ョ€氼剟宕瑰┑鍫燁潟闁绘娅曠紞蹇涙⒒閸ワ絽浜惧┑顔炬嚀閸嬪﹦妲愬▎鎾寸劵闁稿本绮嶉弳蹇撁瑰鍐€楅柣掳鍔庣划璇参旈埀顒勫垂鎼达絿鐭?闂佸綊娼х紞濠囧闯閻戞ê绶為柛鏇ㄥ幗閸婄偞淇婇妞诲亾瀹曞洨顢呴梺鎸庣☉椤︻參鍩€?3. apply_external_materials: 闁诲繐绻愬Λ妤吽囨繝姘劸闁靛ě鍕闂佺懓鐡ㄩ崝妤€鈻撻幋鐘愁潟闁绘娅曠紞蹇涙偣瑜嶇€氼厾鑺遍鈧畷鐘恒亹閹烘垵璧嬮梺闈涙缁舵岸鎮板▎鎴炲珰濞达絾鐡曠€氭瑩鎮锋担鍛婂櫣闁挎繄鍋ゅ畷鍫曟倷妫版繂娈ф繛鎴炴煥椤︻垰鐣烽悢鐓庣闁告劧鑵归崑?
闂佸搫绉堕…鍫㈢紦妤ｅ啯鍋嬮悷娆忓閸嬫捁顧傜紒?- 闂佸憡鏌ｉ崝瀣礊閺冨牆绠涢柣鏃囨閸欌偓: 闂佺硶鏅炲銊ц姳?Action Segments 闂佸搫鎳樼紓姘跺礂濡吋鍟戝ù锝囶焾椤ュ懘鎮峰▎蹇旑棦妞わ絽鐖煎畷顏嗕沪閹呭姷闂佹悶鍎伴崟姗€鍩€?- 闂佹椿鍘归崕鎾儊閹达箑绀嗛柛鈩冪懆椤箓鎮楅悽闈涘付闁? 闂佸搫绉烽～澶婄暤?Knowledge Type (婵犵鈧啿鈧粙顢欐径灞惧枂闁挎繂鎳愰埀?vs 闁哄鏅涘ú銊╁煝婵傜鍨? 闂佸憡鏌ｉ崝宥夊焵椤戞寧绁伴柣銊у枛瀵偆鈧潧鎽滈ˇ閬嶆煛婢跺濮夐柣掳鍎甸幃楣冨Ψ閿濆倸浜?- 闂佺厧顨庢禍顏堝焵椤掆偓閸婂摜鑺遍弻銉ョ闁告侗鍨抽幑? 闁荤姳绶ょ槐鏇㈡偩婵犳艾瀚夐柍褜鍓氶幏鍛暦閸ャ劎鏆犻梺鍝勫暙閻栫厧螞鐠恒劎鐜绘俊銈傚亾鐟?(Adaptive Envelope) 婵炲濮伴崕鎶藉灳濡崵鈹嶆繝闈涚墢椤﹂亶鏌℃径瀣闁伙綆鍓熷顐も偓娑櫱氶崑鎾诡槹闁?- 婵＄偟绻濋悞锕偹夊Δ鍛畱? 闂佸憡鍔曢幊鎰版偪閸℃ê绶炴慨姗€浜堕悰鎾绘煕韫囨梻鐭婄紒銊ョ－缁敻寮介锝嗩吅闂佹寧绋戦悧蹇氬綂闁诲氦顫夐懝楣冩儗?-> 闂佸憡顨嗗ú鏍储閹捐鏋佺紓鍫㈠Х缁?-> 闂備焦瀵ч悷銊╊敋閵堝绠掗柕蹇曞濡插鏌ㄥ☉姗嗘Ф闁?"""

from __future__ import annotations

import atexit
import concurrent.futures
import json
import logging
import os
import hashlib
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import cv2

from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import SemanticUnit

from services.python_grpc.src.content_pipeline.phase2b.assembly.request_models import (
    ClipRequest,
    MaterialRequests,
    ScreenshotRequest,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.pipeline_material_request_utils import (
    create_clip_request,
    create_screenshot_request,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_document import MaterialSet

logger = logging.getLogger(__name__)

_PHASE2B_STRUCTURE_POOL_LOCK = threading.Lock()
_PHASE2B_STRUCTURE_POOL: Optional[concurrent.futures.ProcessPoolExecutor] = None
_PHASE2B_STRUCTURE_POOL_WORKERS = 0
_PHASE2B_WORKER_VALIDATOR = None


def _is_truthy(value: Any) -> bool:
    """将多种布尔表示归一化为 bool。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_parallel_workers(raw_value: str, task_count: int, hard_cap: int = 8) -> int:
    """解析并发 worker 数，支持 auto 或显式整数。"""
    if task_count <= 0:
        return 1
    token = str(raw_value or "auto").strip().lower()
    if token in {"", "auto"}:
        desired = max(1, (os.cpu_count() or 2) - 1)
    else:
        try:
            desired = int(token)
        except Exception:
            desired = 1
    return max(1, min(desired, hard_cap, task_count))


def _should_enable_structure_process_parallel(task_count: int, validator: Any) -> bool:
    """判定是否启用结构预处理多进程并发。"""
    if task_count <= 1:
        return False

    mode = str(os.getenv("PHASE2B_STRUCTURE_PREPROCESS_MODE", "auto") or "auto").strip().lower()
    if mode in {"off", "disabled", "false", "0", "serial"}:
        return False
    if mode == "process":
        return True

    # auto 模式下避免测试环境误触发多进程，减少不确定性。
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False

    # 仅对真实 ConcreteKnowledgeValidator 启用进程并发，避免 fake/mocked validator 破坏测试语义。
    class_name = str(type(validator).__name__ or "").strip()
    module_name = str(getattr(type(validator), "__module__", "") or "").strip()
    if class_name != "ConcreteKnowledgeValidator":
        return False
    if "concrete_knowledge_validator" not in module_name:
        return False
    return True


def _shutdown_phase2b_structure_pool() -> None:
    """进程退出时释放 Phase2B 结构预处理进程池。"""
    global _PHASE2B_STRUCTURE_POOL
    global _PHASE2B_STRUCTURE_POOL_WORKERS

    with _PHASE2B_STRUCTURE_POOL_LOCK:
        if _PHASE2B_STRUCTURE_POOL is not None:
            try:
                _PHASE2B_STRUCTURE_POOL.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            _PHASE2B_STRUCTURE_POOL = None
            _PHASE2B_STRUCTURE_POOL_WORKERS = 0


atexit.register(_shutdown_phase2b_structure_pool)


def _get_phase2b_structure_pool(worker_count: int) -> concurrent.futures.ProcessPoolExecutor:
    """按 worker 数复用单例进程池，避免每个语义单元重复创建进程。"""
    global _PHASE2B_STRUCTURE_POOL
    global _PHASE2B_STRUCTURE_POOL_WORKERS

    with _PHASE2B_STRUCTURE_POOL_LOCK:
        if (
            _PHASE2B_STRUCTURE_POOL is None
            or _PHASE2B_STRUCTURE_POOL_WORKERS != int(worker_count)
        ):
            if _PHASE2B_STRUCTURE_POOL is not None:
                try:
                    _PHASE2B_STRUCTURE_POOL.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
            _PHASE2B_STRUCTURE_POOL = concurrent.futures.ProcessPoolExecutor(
                max_workers=max(1, int(worker_count))
            )
            _PHASE2B_STRUCTURE_POOL_WORKERS = max(1, int(worker_count))
    return _PHASE2B_STRUCTURE_POOL


def _phase2b_structure_worker_run(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    子进程执行结构预处理。
    说明：
    1) 使用进程内单例 validator，避免同一进程重复初始化 PP-Structure。
    2) 只返回结构化结果与错误摘要，主进程决定如何回退。
    """
    global _PHASE2B_WORKER_VALIDATOR

    index = int(task.get("index", -1) or -1)
    image_path = str(task.get("image_path", "") or "").strip()
    source_id = str(task.get("source_id", "") or "").strip()
    timestamp_sec = task.get("timestamp_sec")
    output_dir = str(task.get("output_dir", "") or "").strip()

    if _PHASE2B_WORKER_VALIDATOR is None:
        from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import (
            ConcreteKnowledgeValidator,
        )

        _PHASE2B_WORKER_VALIDATOR = ConcreteKnowledgeValidator(
            output_dir=output_dir if output_dir else None
        )

    try:
        items = _PHASE2B_WORKER_VALIDATOR.extract_structured_screenshots(
            image_path=image_path,
            source_id=source_id,
            timestamp_sec=float(timestamp_sec) if timestamp_sec is not None else None,
        )
        return {"index": index, "items": items, "error": ""}
    except Exception as exc:  # pragma: no cover
        return {"index": index, "items": None, "error": str(exc)}

async def generate_materials(pipeline, unit: SemanticUnit):
    """
    [闂佸湱鐟抽崱鈺傛杸闂備緡鍋呭Σ鎺旀閻?婵炴垶鎹囩紓姘额敋閵忥紕鈻曞璺猴工缁€瀣煕韫囨挸鏆熸繛鎻掓健楠炴帡濡疯閺呮悂鏌熺€涙ê濮夌紒鈧畝鍕骇闁规壆澧楅懖鐘绘煕濮橆剛澧戦柍?
    闂佸搫绉堕…鍫㈢紦閹灐瑙勬媴鐞涒剝鐓犻梺?    1. 婵炴垶鎸搁敃锝囩箔閸涙潙妫橀柛銉戝啰鈧鏌＄€ｎ偄濮х紒鍙樺嵆楠炴捇骞囬鈧徊鍧楁偣閸ヨ泛骞掔紒鐘靛枛瀹曪繝寮撮悙鎻掔瑲婵炴垶鎼╅崢鎯р枔閹达箑绀夐柕濞垮€楃粙濠囨煟濡も偓濞诧箓顢?(Action Segments) 闂佸憡绮岄惉鑹板綂闁诲氦顫夐懝楣冩儗?(Stable Islands)闂?    2. 闂佸憡鏌ｉ崝瀣礊閺冨牊鎳氱€广儱鎳忛崐銈夋煥濞戞瑨澹樻い鏇ㄥ櫍閹秶鈧潧澹婇弳銏ゆ煟閵娿儱顏╁┑顔规櫆閹峰懎顭ㄩ崨顓ф毈濠电偛鐗呯徊鑽ゆ崲濡吋鍋橀悘鐐靛亾椤ρ呯磼瀹€鍕窗闁诲孩妞藉畷?(Merge)闂?    3. 闂佹椿鍘归崕鎾儊閹达箑绀嗛柛鈩冪懆椤箓鏌ㄥ☉娆掑妞ゆ洦鍣ｅ畷婵嬪Ω閵堝洨鈻曢柡澶嗘櫆缁嬫牠銆侀幋鐘靛暗閻犲洩灏欓埀顒傚厴瀹曟岸濡堕崨顖涙瘞 (婵?闁荤姳娴囩亸顏囶暰闂?闁哄鏅涘ú銊╁煝婵傜绠?闂佹寧绋戦懟顖炲窗閸涱垪鍋撶憴鍕殤缂佲偓瀹€鍕骇闁归偊鍓濋崺宀勬煙椤戝潡妾烽柍?    4. 缂備浇浜慨闈涱焽濡ゅ懏鍋ㄩ柣鏃傤焾閻忓洭鏌?       - 闁荤姳娴囩亸顏囶暰闂?(Explanation): 婵炲濮撮幊鎰板极閹捐绠ｉ柟閭﹀墮瑜扮娀姊哄▎鎯ф灈闁硅埖宀搁獮瀣敂閸曨剛顩?(婵?闁?缂備礁顑呴崯鍧楁偩閹呮殕?闂佹寧绋戞總鏃傜箔婢舵劖鍋ㄩ柣鏃傤焾閻忓洭鎮峰▎蹇旑棦妞わ絽鐖兼俊?       - 闁哄鏅涘ú銊╁煝婵傜鍨?(Process): 闂佹眹鍨婚崰鎰板垂濮樿鲸鍠嗛柛鈩冧緱閺嗐儵鏌ｅΔ鈧ú锕傤敊?(Clip) 闂佸憡鐟ラ敃顏堝储閵堝棛闄勬俊銈呭暞閻ｉ亶鏌涜箛鏂库枙闁轰緡鍘鹃弫顔款槼闁糕晛鎳樺畷鑸电┍閹典礁浜?    5. 闂佺绻戠划宀€鑺遍幎钘夊珘闁告繂瀚悡鎴︽煥濞戞瑤浜㈤悗姘ュ灲瀵噣鎮╅幓鎹霉閿濆棛鐭屽褎鐗曢埢鎾跺枈婢跺瞼顦梺鍛婂笚閻熴儱煤閺嶎厽鐒婚柍褜鍓熷畷姘舵偋閸繄鍊掗梺鍛婄懄閻楁粏褰婇柣搴ゎ潐閼归箖鎯屽┑瀣妞ゅ繐瀚粋鍫ユ煙鐎涙澧柛娆忕箻瀹曪繝寮撮悙鎻掔瑲闂佺绻戠划宀€鑺遍幎钘夌妞ゅ繐瀚粋鍫ユ煏?
    闁哄鐗婇幐鎼佸矗閸℃稑鐭楅柛灞剧⊕濞堝爼鏌?    - pipeline: 闁哄鏅滈崝姗€銆侀幋锕€绫嶉柡鍫㈡暩閻熸劕鈽夐幘鎰佸剱闁哄鍟撮弫宥囦沪閹呭€掓繛鎾寸缁嬫挾绮堝畝鍕骇闁归偊鍘肩徊褰掓煕濞嗘劗澧悽顖ｅ亞閳ь剚绋掗崝姗€宕楀鈧畷?(闂佽鎯屾禍婊兠?闂佸憡甯掑ú銊︽櫠?闂?    - unit: 閻庡灚婢橀幊搴ㄋ囬埡鍛仩闁糕剝鐟﹂悾?SemanticUnit 闁诲海鏁搁、濠囨寘閸曨垰违?
    闁哄鐗婇幐鎼佸吹椤撱垺鏅?    - 闂佸搫鍟版慨鍓ф崲閹达箑鐐婇柣鎰ゴ閸嬫挾绮悰鈥充壕闁稿本绮嶇痪顖炴煙閹帒鍔欓柟渚垮姂瀵?unit.materials 闁诲繒鍋熼崑鐐哄焵椤戭剙绉剁粈澶娾攽婢舵ê浜滈柛?screenshot_paths/clip_paths 缂備焦绋戦ˇ顓㈠焵?    """
    materials = MaterialSet(modality=unit.modality)
    
    screenshot_paths = []
    screenshot_labels = []
    clip_paths = []
    
    # 闂佸吋鍎抽崲鑼躲亹閸モ晝鐭欓柛鎰皺閺嗘壆鈧厜鍋撴繛鎴炆戠€氭煡鏌涢弬璇插缂傚秵妫冨畷锟犲即閻愭彃绗氭繛锝呮礌閸撴繃瀵?    stable_islands = getattr(unit, 'stable_islands', [])
    action_segments = getattr(unit, 'action_segments', [])
    
    if action_segments:
        # ==== 闂佸搫鐗嗛ˇ顖涙叏閳哄倹濯存繝濠傚暙缁€瀣煕? 闁荤喐鐟ョ€氼剟宕归娑氣枖闁?+ 闁荤喐鐟ョ€氼剟宕归娑氼洸?====
        # 闁荤喐鐟ョ€氼剟宕归娑氣枖闁? 婵炴垶鎸哥粔浣冦亹娓氣偓瀹曪綁寮介澶婃婵炴垶鏌ㄩˇ顖氱暦閻旂厧绀傞柛鎰典邯閻?stable 闂備緡鍠撻崝宀勫垂?(闁荤姴鎼悿鍥╂崲?
        
        # 濡絽鍟€?闂佸憡鑹炬鎼佀囬埡鍛仩? 婵犮垼鍩栭懝鐐叏閳哄倹濯存繝濠傛閳ь剚妞藉畷?(闂佸憡鑹炬總鏃傜博鐎靛憡瀚氭い鎾寸箘閻ゅ懘鏌涘Δ浣圭闁告ɑ鎹囧畷銉т沪閻撳海妲ｆ俊顐ゅ閿曨偆妲愬┑瀣哗闁诡垎灞芥櫖闂佸憡鑹鹃悧鍡涙嚐閻斿吋鈷掗柟缁㈠枟椤?
        if len(action_segments) >= 2:
            merged_actions = pipeline._merge_action_segments(action_segments, gap_threshold_sec=5.0)
            if len(merged_actions) < len(action_segments):
                logger.info(
                    f"{unit.unit_id}: Post-merge (gap<5.0s) {len(action_segments)} 闂?{len(merged_actions)} actions"
                )
            action_segments = merged_actions
            unit.action_segments = merged_actions
        
        # 濡絽鍟悾?婵炴潙鍚嬮敋閻庡灚鐓￠弫宥咁潩椤愩倕鏋犻梺绋跨箰閻楀棝藝閺屻儲鍋ㄩ柕濞炬櫓閺嗘洟鏌涢幒鎴烆棤閻?婵炴垶鎸搁敃銉╂偉?knowledge_type闂佹寧绋戦惉鐓庮啅鏉堛劌绶為弶鍫涘妽椤ρ囨煙闂堟稓啸闁汇劎鍠栭幃?LLM
        for a in action_segments:
            if isinstance(a.get("classification"), dict) and a.get("classification", {}).get("knowledge_type"):
                continue
            kt = str(a.get("knowledge_type", "") or "").strip()
            if kt:
                a["classification"] = {
                    "knowledge_type": kt,
                    "confidence": float(a.get("confidence", 0.5) or 0.5),
                    "key_evidence": a.get("key_evidence", ""),
                    "reasoning": a.get("reasoning", ""),
                }

        if all(
            isinstance(a.get("classification"), dict) and a.get("classification", {}).get("knowledge_type")
            for a in action_segments
        ):
            batch_classifications = [a.get("classification", {}) for a in action_segments]
        else:
            batch_classifications = await pipeline._knowledge_classifier.classify_batch(
                semantic_unit_title=getattr(unit, 'knowledge_topic', 'unknown_title'),
                semantic_unit_text=getattr(unit, 'full_text', getattr(unit, 'text', '')),
                action_segments=action_segments
            )
            for action, classification in zip(action_segments, batch_classifications):
                action["classification"] = classification
        
        for i, (action, classification) in enumerate(zip(action_segments, batch_classifications)):
            action_start = action.get("start_sec", unit.start_sec)
            action_end = action.get("end_sec", unit.end_sec)
            # 闂佸吋鍎抽崲鑼躲亹閸モ晜瀚氶柕澶堝劚琚熸繛杈剧稻缁苯鐣烽悢鐓庣闁告劑鍔岄弫鍫曟⒑椤斿搫濡挎繛鍫熷灩缁瑩宕橀懠顒佹瘞閻庡厜鍋?            action_internal_islands = action.get("internal_stable_islands", [])
            
            # 濡絽鍟€?Sentence闂佹寧绋掗惌顔剧箔瀹€鍕闁靛鍊楃粙濠囨⒑閹绘帞孝鐟滅増妫冮幆鍐礋椤栨矮绨奸梺鍛婄懀閸庤崵妲愬▎鎾崇濠㈣泛锕﹂幗鐔割殽閻愬瓨绀冩俊鐐插€垮鑽も偓娑櫭悡鍫ユ倵鐟欏嫮鐓紓宥呮嚇閺?            sentence_start = pipeline._align_to_sentence_start(action_start)
            sentence_end = pipeline._align_to_sentence_end(action_end)
            
            # Classification already done in batch
            knowledge_type = classification.get("knowledge_type", "process")
            confidence = classification.get("confidence", 0.5)
            action_brief = pipeline._build_action_brief(action, classification, i + 1)
            asset_base = f"{unit.unit_id}/{pipeline._build_request_base_name(unit, f'action_{i+1:02d}_{action_brief}')}"

            
            logger.info(f"{unit.unit_id} action_{i+1}: {knowledge_type} (conf={confidence:.0%}) - {classification.get('key_evidence', '')[:30]}")

            # 濡絽鍟悾?Adaptive Action Envelope: 闁荤姴娴傞崢铏圭不閻旂厧纭€闁哄洨鍋涚敮妤呮煟椤撗冨箺婵＄偛鍊垮畷锝夘敍濞戞瑦顔掑┑鐐茬墢椤ｎ喚妲愭潏銊р枖?clip 缂傚倷鐒﹂幐璇差焽椤愶絿鈻旂€广儱鐗婄涵鍫曟偤?unit.end_sec
            envelope_start, envelope_end = pipeline._compute_action_envelope(
                unit=unit,
                action_start=action_start,
                action_end=action_end,
                sentence_start=sentence_start,
                sentence_end=sentence_end,
                knowledge_type=knowledge_type
            )
            logger.warning(
                f"{unit.unit_id} action_{i+1}: envelope [{envelope_start:.2f}s-{envelope_end:.2f}s] "
                f"(knowledge_type={knowledge_type})"
            )
            
            if knowledge_type == "讲解型":
                # 濡絽鍟€?闂傚倸瀚粔鑸殿殽? 闁荤姳娴囩亸顏囶暰闂佹悶鍔岄鍛般亹瑜旈獮瀣敂閸曨偆鎮兼俊顐稻閻楁洟鎮㈤鍌涙殰?+ 缂備礁顑呴崯鍧楁偩閹呮殕婵炴垶鐟ラ悞濠氭煕閵夛附瀚曠紒杈ㄧ箖缁嬪顓奸崱妤冨€掗梺鍛婄懄閻楃偤锝炵€ｎ偓绱?                logger.info("  闂?Downgrade to screenshots only (闁荤姳娴囩亸顏囶暰闂?")
                
                # 婵☆偓绲鹃悧鏇㈠箮濮樿泛绠ｆい蹇撳缁? 闂佸搫琚崕鍙夌珶濡偐鐜绘俊銈傚亾鐟滅増绋掔粙?[闂佸憡鐗曢幊鎰垝閸撲焦灏庡瀣娴? 闂佸憡鏌ｉ崝瀣礊閺冨倹灏庡瀣娴犵棇
                head_window_end = min(max(envelope_start + 0.5, action_start), envelope_end)
                head_ss = await pipeline._select_screenshot(
                    start_sec=envelope_start,
                    end_sec=head_window_end,
                    name=f"{asset_base}_head"
                )
                if head_ss:
                    screenshot_paths.append(head_ss)
                    screenshot_labels.append(f"动作{i+1}首帧")
                
                # 缂備礁顑呴崯鍧楁偩閹呮殕婵炴垶鐟ラ悞濠氭煕?
                for j, island in enumerate(action_internal_islands):
                    island_start = island.get("start", action_start)
                    island_end = island.get("end", action_end)
                    
                    island_ss = await pipeline._select_screenshot(
                        start_sec=island_start,
                        end_sec=island_end,
                        name=f"{asset_base}_island_{j+1:02d}"
                    )
                    if island_ss:
                        screenshot_paths.append(island_ss)
                        screenshot_labels.append(f"动作{i+1}稳定帧{j+1}")
                
                # 闂佸搫鐗滈崑鍕箮濮樿泛绠ｆい蹇撳缁? 闂佸搫琚崕鍙夌珶濡偐鐜绘俊銈傚亾鐟滅増绋掔粙?[闂佸憡鏌ｉ崝瀣礊閺冨倻纾奸柛顐犲灩娴? 闂佸憡鐗曢幊鎰垝閸撲胶纾奸柛顐犲灩娴犵棇
                tail_window_start = max(min(envelope_end - 0.5, action_end), envelope_start)
                tail_ss = await pipeline._select_screenshot(
                    start_sec=tail_window_start,
                    end_sec=envelope_end,
                    name=f"{asset_base}_tail"
                )
                if tail_ss:
                    screenshot_paths.append(tail_ss)
                    screenshot_labels.append(f"动作{i+1}末帧")
            
            else:
                # 闂傚倸鐗忛崑鐘活敊婢跺本鍠嗛柨婵嗘噽閳? 闂佸湱绮崝鏇°亹閸モ晜鍠嗛柛鈩冧緱閺?+ 婵☆偓绲鹃悧鏇㈡偄椤掑倹鏆?+ 缂備礁顑呴崯鍧楁偩閹呮殕婵炴垶鐟ラ悞濠氭煕?                
                # 1. 闂佸湱绮崝鏇°亹閸モ晜鍠嗛柛鈩冧緱閺嗐儵鏌ｅΔ鈧ú锕傤敊?(婵炶揪缍€濞夋洟寮妶澶嬪殜妞ゅ繐鐗冮崑鎾诲磼濮樺崬鐣ㄩ梺鍛婃煟閸斿绱為弮鍫濈闁告侗鍨抽幑鏇㈡煛閸愩劎鍩ｆ俊顐㈡健閹崇娀宕橀妸锔惧嚱)
                clip_path = await pipeline._extract_action_clip(
                    start_sec=envelope_start,
                    end_sec=envelope_end,
                    name=f"{asset_base}"
                )
                if clip_path:
                    clip_paths.append(clip_path)
                
                # 2. 闂佸湱绮崝鏇°亹閸ヮ煉绱旈柡宓嫬顫撻梺瑙勬儗娴滄粌霉? 闂佸搫琚崕鍙夌珶濡偐鐜绘俊銈傚亾鐟滅増绋掔粙?[闂佸憡鐗曢幊鎰垝閸撲焦灏庡瀣娴? 闂佸憡鏌ｉ崝瀣礊閺冨倹灏庡瀣娴犵棇
                head_window_end = min(max(envelope_start + 0.5, action_start), envelope_end)
                head_ss = await pipeline._select_screenshot(
                    start_sec=envelope_start,
                    end_sec=head_window_end,
                    name=f"{asset_base}_head"
                )
                if head_ss:
                    screenshot_paths.append(head_ss)
                    screenshot_labels.append(f"动作{i+1}首帧")
                
                # 3. 缂備礁顑呴崯鍧楁偩閹呮殕婵炴垶鐟ラ悞濠氭煕?
                for j, island in enumerate(action_internal_islands):
                    island_start = island.get("start", action_start)
                    island_end = island.get("end", action_end)
                    
                    island_ss = await pipeline._select_screenshot(
                        start_sec=island_start,
                        end_sec=island_end,
                        name=f"{asset_base}_island_{j+1:02d}"
                    )
                    if island_ss:
                        screenshot_paths.append(island_ss)
                        screenshot_labels.append(f"动作{i+1}稳定帧{j+1}")
                
                # 4. 闂佸湱绮崝鏇°亹閸ヮ剙瀚夋い鎰╁灪婵酣鏌熼幖顓濈盎婵? 闂佸搫琚崕鍙夌珶濡偐鐜绘俊銈傚亾鐟滅増绋掔粙?[闂佸憡鏌ｉ崝瀣礊閺冨倻纾奸柛顐犲灩娴? 闂佸憡鐗曢幊鎰垝閸撲胶纾奸柛顐犲灩娴犵棇
                tail_window_start = max(min(envelope_end - 0.5, action_end), envelope_start)
                tail_ss = await pipeline._select_screenshot(
                    start_sec=tail_window_start,
                    end_sec=envelope_end,
                    name=f"{asset_base}_tail"
                )
                if tail_ss:
                    screenshot_paths.append(tail_ss)
                    screenshot_labels.append(f"动作{i+1}末帧")
    
    elif stable_islands:
        # ==== 闂佸搫鍟版慨瀛樻叏閳哄倹濯存繝濠傚暙缁€瀣煕韫囨挸鏋︾紒杈ㄧ箖缁傛帡宕ㄩ鍏兼⒒閳ь剝顫夐懝楣冩儗? 闂佸湱绮崝鏇°亹閸ャ劎鈻旀い鎾卞灪閿涚喖鎮?====
        for i, island in enumerate(stable_islands):
            island_start = island.get("start", unit.start_sec)
            island_end = island.get("end", unit.end_sec)
            
            ss_path = await pipeline._select_screenshot(
                start_sec=island_start,
                end_sec=island_end,
                name=f"{unit.unit_id}/{pipeline._build_request_base_name(unit, f'stable_{i+1:02d}')}"
            )
            if ss_path:
                screenshot_paths.append(ss_path)
                screenshot_labels.append(f"稳定帧{i+1}")
    
    else:
        # ==== 闂佹悶鍎抽崑銈夊焵椤戣棄浜? 闂佸搫鍟版慨鎶藉箲閵忊剝濯撮柡鍥╁櫏濮婄偓绻涢弶鎴剳缂侇喓鍔戝?====
        fallback_ss = await pipeline._select_screenshot(
            start_sec=unit.start_sec,
            end_sec=unit.end_sec,
            name=f"{unit.unit_id}/{pipeline._build_request_base_name(unit, 'fallback')}"
        )
        if fallback_ss:
            screenshot_paths.append(fallback_ss)
            screenshot_labels.append("截图")
    
    # ==== 缂傚倷绀佺€氼垶藟婵犲嫭顫曢柣妯挎珪缂嶅繘姊婚崱妤侇棞闁?====
    materials.screenshot_paths = screenshot_paths
    materials.screenshot_labels = screenshot_labels
    materials.screenshot_items = [
        {
            "img_id": f"{unit.unit_id}_img_{idx + 1:02d}",
            "img_path": path,
            "img_description": screenshot_labels[idx] if idx < len(screenshot_labels) else f"image_{idx + 1}",
            "img_desription": screenshot_labels[idx] if idx < len(screenshot_labels) else f"image_{idx + 1}",
            "label": screenshot_labels[idx] if idx < len(screenshot_labels) else "",
            "source_id": Path(path).stem,
        }
        for idx, path in enumerate(screenshot_paths)
    ]
    materials.clip_paths = clip_paths
    materials.clip_path = clip_paths[0] if clip_paths else ""
    
    # 濡絽鍟€?V7.4: 闂佸湱绮崝鏇°亹閸ヮ剙绀夐柕濞垮€楃粙濠囨煕濡や焦绀€闁告ɑ鎹囧畷姘跺幢濞嗘帩娼剁紓鍌欑劍閹稿鎮?    action_classifications = []
    for action in action_segments:
        if "classification" in action:
            action_classifications.append({
                "time_range": [action.get("start_sec", 0), action.get("end_sec", 0)],
                **action["classification"]
            })
    materials.action_classifications = action_classifications
    
    unit.materials = materials
    
    logger.debug(f"{unit.unit_id}: {len(action_segments)} actions, {len(stable_islands)} islands 闂?"
                 f"{len(clip_paths)} clips + {len(screenshot_paths)} screenshots")


async def collect_material_requests(pipeline, unit: SemanticUnit) -> MaterialRequests:
    """
    [闁荤喐鐟ョ€氼剟宕瑰┑瀣劵闁哄嫬绻掔敮鍝?闂佸憡甯掑Λ娆撴倵閻ｅ本瀚氭い鎾寸箘閻ゅ懘鏌涘Δ浣圭闁告ɑ鎹囬悰顕€宕滄担瑙勬闂佺懓鐡ㄩ崝妤冪矆瀹€鍕骇闁归偊鍨扮粻顖炴煕濞嗘劗澧虫い鏇ㄥ墮鏁堥柛宀嬪缁€鍕槈閹惧磭校濠⒀呮櫕閹壆浠﹂悾灞炬緰闂傚倸瀚幊蹇氥亹娓氣偓瀹曪綁寮介悽鐢殿槴闂?
    闂佸搫鐗滈崜姘跺蓟閻斿皝鏋栭柡鍥╁У閺嗗繐霉濠婂啫顒㈤柣搴灠椤?"缂備浇浜慨闈涱焽濡ゅ懏顥嗛柍褜鍓欐晥闁稿本绋撻鎼佹煕? (MaterialRequests)闂佹寧绋戦懟顖炴儍閵忋垺鍠嗛柛鏇ㄥ亜閻忓﹤鈽夐幘鍐差劉濠⒀呮櫕閹澘鐣濋崘鍐╁浮閹虫捇鏁傜悰鈥充壕?
    闂佸搫绉堕…鍫㈢紦閹灐瑙勬媴鐞涒剝鐓犻梺?    1. 闂佸憡鏌ｉ崝瀣礊閺冨倹鍠嗛柛鏇ㄥ亜閻忓﹪鏌ㄥ☉娆掑闁?generate_materials闂佹寧绋戦惌浣烘崲濡吋鍋橀悘鐐舵琚熸繛杈剧稻缁繘鎮楀ú顏勮Е闁割偅绮庨悷銏ゆ煕閹烘垶顥犻悶姘煎亰婵?    2. 闂傚倸娲犻崑鎾存叏閻熸澘鈧綊鎮楅姘煎殘闁绘艾顕粣?       - 闁荤姳绶ょ槐鏇㈡偩閼姐倕鍨濋柛鎾椻偓閳ь剚锕㈤幆鍐礋椤掍緡妲梻鍌氬€瑰娆戔偓鍨焽缁?(start_sec, end_sec)闂?       - 婵炴垶鎸鹃崕銈夋儊閳╁啰鈻旀い蹇撳琚熸繛杈剧稻缁酣寮幘璇茬闁归偊鍓氶弳婊冣槈閹绢垰浜鹃梺?Request ID (婵?action_01_open_file)闂?       - 闂佸搫绉烽～澶婄暤娓氣偓閹矂濡烽妸褎顫氱紓渚囧亯椤曆囨倵閻戣姤鍋ㄩ柣鏃傤焾閻?ScreenshotRequest (head/tail/stable) 闂?ClipRequest闂?    3. 婵炴潙鍚嬮敋閻庝絻灏欑划鐢稿冀椤愶絾顓洪梺?       - 闂佸搫鎳樼紓姘跺礂濮椻偓楠炲秹鍩℃担鍏告捣闂佺懓鍚嬬划搴ㄥ磼閵娧呯幓婵°倐鍋撶憸?(Search Window) 婵炲濮伴崕鑼躲亹娴ｈ娈楁俊顖濐嚙閻掑鏌涢妷锕€鍔ら柟鏂ュ墲缁嬪顢橀悩鑼跺У闂?       - 闁诲海鏁搁、濠囶敊婢跺本鍠嗛柨婵嗘噽閳ь剛鍏樺畷姗€宕ㄩ褍鏅ｉ梺鐓庮殠娴滄粍鎱ㄩ埡鍐╁濞达絿顭堢敮鎶芥偡濞嗗繑顥㈡い锝呯埣楠炴捇骞囬鈧徊鍧楁偣閸ヮ剚鏁遍柣顏冨嵆婵?
    闁哄鐗婇幐鎼佸矗閸℃稑鐭楅柛灞剧⊕濞堝爼鏌?    - pipeline: 闁哄鏅滈崝姗€銆侀幋锕€绫嶉柡鍫㈡暩閻熸劕鈽夐幘鎰佸剱闁哄鍟存俊?    - unit: 閻庡灚婢橀幊搴ㄋ囬埡鍛仩闁糕剝鐟﹂悾?SemanticUnit 闁诲海鏁搁、濠囨寘閸曨垰违?
    闁哄鐗婇幐鎼佸吹椤撱垺鏅?    - MaterialRequests: 闂佸憡鐗曢幊搴ㄥ箚閸儱绠ラ柍褜鍓熷鍨緞鎼搭喗缍堥梺鍛婂笚鐢帡濡舵禒瀣剭闁告洦鍘鹃ˇ閬嶆煛婢跺濮屾い鏇ㄥ墮鏁堥柛灞惧嚬閸ょ娀鎮归悙鈺傤潐缂佽鲸鐟╅獮瀣敂閸曨剛顩柣鐘叉喘閺€閬嶆儑娴兼潙违濞达絿鐡斿鎺懳涢悧鍫濈仴闁搞劌绻橀幃褔宕堕宥呮濠殿喚鎳撻崐鎼佸垂椤忓棙鍋橀柕濞垮劚閹搞倝鏌涢弬璇插缂傚秵妫冨畷姘跺幢濞嗘帩娼堕梺绋跨箰閸燁垶寮抽悢鐓庣妞ゆ洖妫涚粈鍡涙煏?    """
    screenshot_requests: List[ScreenshotRequest] = []
    clip_requests: List[ClipRequest] = []
    action_classifications: List[Dict[str, Any]] = []
    
    # 闂佸吋鍎抽崲鑼躲亹閸モ晝鐭欓柛鎰皺閺嗘壆鈧厜鍋撴繛鎴炆戠€氭煡鏌涢弬璇插缂傚秵妫冨畷锟犲即閻愭彃绗氭繛锝呮礌閸撴繃瀵?    stable_islands = getattr(unit, 'stable_islands', [])
    action_segments = getattr(unit, 'action_segments', [])
    
    # 濡絽鍟€?闂佸憡鑹炬鎼佀囬埡鍛仩? 婵犮垼鍩栭懝鐐叏閳哄倹濯存繝濠傛閳ь剚妞藉畷?(婵炴垶鎸哥粚鐚nerate_materials婵烇絽娲︾换鍐偓鍨⒐缁嬪鍩€椤掑嫭鍤?
    if len(action_segments) >= 2:
        action_segments = pipeline._merge_action_segments(action_segments, gap_threshold_sec=5.0)
        unit.action_segments = action_segments
    
    if action_segments:
        # 濡絽鍟悾?婵炴潙鍚嬮敋閻庡灚鐓￠弫宥咁潩椤愩倕鏋犻梺绋跨箰閻楀棝藝閺屻儲鍋ㄩ柕濞炬櫓閺嗘洟鏌涢幒鎴烆棤閻?婵炴垶鎸搁敃銉╂偉?knowledge_type闂佹寧绋戦惉鐓庮啅鏉堛劌绶為弶鍫涘妽椤ρ囨煙闂堟稓啸闁汇劎鍠栭幃?LLM
        for a in action_segments:
            if isinstance(a.get("classification"), dict) and a.get("classification", {}).get("knowledge_type"):
                continue
            kt = str(a.get("knowledge_type", "") or "").strip()
            if kt:
                a["classification"] = {
                    "knowledge_type": kt,
                    "confidence": float(a.get("confidence", 0.5) or 0.5),
                    "key_evidence": a.get("key_evidence", ""),
                    "reasoning": a.get("reasoning", ""),
                }

        if all(
            isinstance(a.get("classification"), dict) and a.get("classification", {}).get("knowledge_type")
            for a in action_segments
        ):
            batch_classifications = [a.get("classification", {}) for a in action_segments]
        else:
            batch_classifications = await pipeline._knowledge_classifier.classify_batch(
                semantic_unit_title=getattr(unit, 'knowledge_topic', 'unknown_title'),
                semantic_unit_text=getattr(unit, 'full_text', getattr(unit, 'text', '')),
                action_segments=action_segments
            )
            for action, classification in zip(action_segments, batch_classifications):
                action["classification"] = classification

        # ==== 闂佸搫鐗嗛ˇ顖涙叏閳哄倹濯存繝濠傚暙缁€瀣煕?====
        for i, (action, classification) in enumerate(zip(action_segments, batch_classifications)):
            action_start = action.get("start_sec", unit.start_sec)
            action_end = action.get("end_sec", unit.end_sec)
            action_internal_islands = action.get("internal_stable_islands", [])
            
            # 濡絽鍟€?Sentence闂佹寧绋掗惌顔剧箔瀹€鍕闁靛鍊楃粙濠囨⒑閹绘帞孝鐟滅増妫冮幆鍐礋椤栨矮绨奸梺鍛婄懀閸庤崵妲愬▎鎾崇濠㈣泛锕﹂幗鐔割殽閻愬瓨绀冩俊鐐插€垮鑽も偓娑櫭悡鍫ユ倵鐟欏嫮鐓紓宥呮嚇閺?            sentence_start = pipeline._align_to_sentence_start(action_start)
            sentence_end = pipeline._align_to_sentence_end(action_end)
            
            # Classification already done in batch
            knowledge_type = classification.get("knowledge_type", "process")
            confidence = classification.get("confidence", 0.5)
            action_brief = pipeline._build_action_brief(action, classification, i + 1)
            request_base = pipeline._build_unit_relative_request_id(
                unit,
                f"action_{i+1:02d}_{action_brief}",
            )

            # 记录动作分类结果，供后续文档装配与审计复用。
            action_classifications.append({
                "time_range": [action_start, action_end],
                **classification,
            })
            
            logger.info(f"{unit.unit_id} action_{i+1}: {knowledge_type} (conf={confidence:.0%})")

            # 濡絽鍟悾?Adaptive Action Envelope: 闁荤姴娴傞崢铏圭不閻旂厧纭€闁哄洨鍋涚敮妤呮煟椤撗冨箺婵＄偛鍊垮畷锝夘敍濞戞瑦顔掑┑鐐茬墢椤ｎ喚妲愭潏銊р枖?clip 缂傚倷鐒﹂幐璇差焽椤愶絿鈻旂€广儱鐗婄涵鍫曟偤?unit.end_sec
            envelope_start, envelope_end = pipeline._compute_action_envelope(
                unit=unit,
                action_start=action_start,
                action_end=action_end,
                sentence_start=sentence_start,
                sentence_end=sentence_end,
                knowledge_type=knowledge_type
            )
            logger.warning(
                f"{unit.unit_id} action_{i+1}: envelope [{envelope_start:.2f}s-{envelope_end:.2f}s] "
                f"(knowledge_type={knowledge_type})"
            )
            
            if knowledge_type == "讲解型":
                # 闁荤姳娴囩亸顏囶暰闂? 闂佸憡鐟禍顏勩€掗崜浣瑰暫濞达絿鎳撻悞濠氭煕閵夛附瀚曠紒杈ㄧ箖缁嬪顓兼径瀣╃帛闁荤喐娲戝ù鍥綖鐎ｎ偓绱?                # 婵☆偓绲鹃悧鏇㈠箮濮樿泛绠ｆい蹇撳缁? 闂佺懓鍚嬬划搴ㄥ磼閵娧呯幓婵°倐鍋撶憸鐗堢洴楠炲秹鍩℃担鍏告捣婵?闂佸憡鐗曢幊鎰垝閸撲焦灏庡瀣娴?闁?.0s
                head_search_start, head_search_end = pipeline._clamp_time_range(envelope_start - 1.0, envelope_start + 1.0)
                fallback_head_ts = envelope_start
                head_ts = await pipeline._select_screenshot_timestamp(head_search_start, head_search_end, fallback_head_ts)
                
                screenshot_requests.append(
                    create_screenshot_request(
                        screenshot_request_type=ScreenshotRequest,
                        screenshot_id=f"{request_base}_head",
                        timestamp_sec=head_ts,
                        label="head",
                        semantic_unit_id=unit.unit_id,
                    )
                )
                
                # 缂備礁顑呴崯鍧楁偩閹呮殕婵炴垶鐟ラ悞濠氭煕?
                for j, island in enumerate(action_internal_islands):
                    island_start = island.get("start", action_start)
                    island_end = island.get("end", action_end)
                    island_mid_fallback = (island_start + island_end) / 2
                    island_start, island_end = pipeline._clamp_time_range(island_start, island_end)
                    island_mid = await pipeline._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

                    screenshot_requests.append(
                        create_screenshot_request(
                            screenshot_request_type=ScreenshotRequest,
                            screenshot_id=f"{request_base}_island_{j+1:02d}",
                            timestamp_sec=island_mid,
                            label="stable",
                            semantic_unit_id=unit.unit_id,
                        )
                    )
                
                # 闂佸搫鐗滈崑鍕箮濮樿泛绠ｆい蹇撳缁? 闂佺懓鍚嬬划搴ㄥ磼閵娧呯幓婵°倐鍋撶憸鐗堢洴楠炲秹鍩℃担鍏告捣婵?闂佸憡鐗曢幊鎰垝閸撲胶纾奸柛顐犲灩娴?闁?.0s
                tail_search_start, tail_search_end = pipeline._clamp_time_range(envelope_end - 1.0, envelope_end + 1.0)
                tail_search_end = min(tail_search_end, float(getattr(unit, "end_sec", tail_search_end)))
                tail_search_start, tail_search_end = pipeline._clamp_time_range(tail_search_start, tail_search_end)
                fallback_tail_ts = envelope_end
                tail_ts = await pipeline._select_screenshot_timestamp(tail_search_start, tail_search_end, fallback_tail_ts)
                
                screenshot_requests.append(
                    create_screenshot_request(
                        screenshot_request_type=ScreenshotRequest,
                        screenshot_id=f"{request_base}_tail",
                        timestamp_sec=tail_ts,
                        label="tail",
                        semantic_unit_id=unit.unit_id,
                    )
                )
            
            else:
                # 非讲解型动作：保留 clip，并追加头尾截图请求。
                clip_requests.append(
                    create_clip_request(
                        clip_request_type=ClipRequest,
                        clip_id=request_base,
                        start_sec=envelope_start,
                        end_sec=envelope_end,
                        knowledge_type=knowledge_type,
                        semantic_unit_id=unit.unit_id,
                    )
                )
                
                # 婵☆偓绲鹃悧鏇㈠箮濮樿泛绠ｆい蹇撳缁? 闂佺懓鍚嬬划搴ㄥ磼閵娧呯幓婵°倐鍋撶憸鐗堢洴楠炲秹鍩℃担鍏告捣婵?闂佸憡鐗曢幊鎰垝閸撲焦灏庡瀣娴?闁?.0s
                head_search_start, head_search_end = pipeline._clamp_time_range(envelope_start - 1.0, envelope_start + 1.0)
                fallback_head_ts = envelope_start
                head_ts = await pipeline._select_screenshot_timestamp(head_search_start, head_search_end, fallback_head_ts)
                
                screenshot_requests.append(
                    create_screenshot_request(
                        screenshot_request_type=ScreenshotRequest,
                        screenshot_id=f"{request_base}_head",
                        timestamp_sec=head_ts,
                        label="head",
                        semantic_unit_id=unit.unit_id,
                    )
                )
                
                # 缂備礁顑呴崯鍧楁偩閹呮殕婵炴垶鐟ラ悞濠氭煕?
                for j, island in enumerate(action_internal_islands):
                    island_start = island.get("start", action_start)
                    island_end = island.get("end", action_end)
                    island_mid_fallback = (island_start + island_end) / 2
                    island_start, island_end = pipeline._clamp_time_range(island_start, island_end)
                    island_mid = await pipeline._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

                    screenshot_requests.append(
                        create_screenshot_request(
                            screenshot_request_type=ScreenshotRequest,
                            screenshot_id=f"{request_base}_island_{j+1:02d}",
                            timestamp_sec=island_mid,
                            label="stable",
                            semantic_unit_id=unit.unit_id,
                        )
                    )
                
                # 闂佸搫鐗滈崑鍕箮濮樿泛绠ｆい蹇撳缁? 闂佺懓鍚嬬划搴ㄥ磼閵娧呯幓婵°倐鍋撶憸鐗堢洴楠炲秹鍩℃担鍏告捣婵?闂佸憡鐗曢幊鎰垝閸撲胶纾奸柛顐犲灩娴?闁?.0s
                tail_search_start, tail_search_end = pipeline._clamp_time_range(envelope_end - 1.0, envelope_end + 1.0)
                tail_search_end = min(tail_search_end, float(getattr(unit, "end_sec", tail_search_end)))
                tail_search_start, tail_search_end = pipeline._clamp_time_range(tail_search_start, tail_search_end)
                fallback_tail_ts = envelope_end
                tail_ts = await pipeline._select_screenshot_timestamp(tail_search_start, tail_search_end, fallback_tail_ts)
                
                screenshot_requests.append(
                    create_screenshot_request(
                        screenshot_request_type=ScreenshotRequest,
                        screenshot_id=f"{request_base}_tail",
                        timestamp_sec=tail_ts,
                        label="tail",
                        semantic_unit_id=unit.unit_id,
                    )
                )
    
    elif stable_islands:
        # ==== 闂佸搫鍟版慨瀛樻叏閳哄倹濯存繝濠傚暙缁€瀣煕韫囨挸鏋︾紒杈ㄧ箖缁傛帡宕ㄩ鍏兼⒒閳ь剝顫夐懝楣冩儗? 闂佸湱绮崝鏇°亹閸ャ劎鈻旀い鎾卞灪閿涚喖鎮?====
        for i, island in enumerate(stable_islands):
            island_start = island.get("start", unit.start_sec)
            island_end = island.get("end", unit.end_sec)
            island_mid_fallback = (island_start + island_end) / 2
            island_mid = await pipeline._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

            screenshot_requests.append(
                create_screenshot_request(
                    screenshot_request_type=ScreenshotRequest,
                    screenshot_id=pipeline._build_unit_relative_request_id(unit, f"stable_{i+1:02d}"),
                    timestamp_sec=island_mid,
                    label="stable",
                    semantic_unit_id=unit.unit_id,
                )
            )
    
    else:
        # ==== 闂佹悶鍎抽崑銈夊焵椤戣棄浜? 闂佸搫鍟版慨鎶藉箲閵忊剝濯撮柡鍥╁櫏濮婄偓绻涢弶鎴剳缂侇喓鍔戝?====
        fallback_ts = (unit.start_sec + unit.end_sec) / 2
        best_ts = await pipeline._select_screenshot_timestamp(unit.start_sec, unit.end_sec, fallback_ts)
        
        screenshot_requests.append(
            create_screenshot_request(
                screenshot_request_type=ScreenshotRequest,
                screenshot_id=pipeline._build_unit_relative_request_id(unit, "fallback"),
                timestamp_sec=best_ts,
                label="fallback",
                semantic_unit_id=unit.unit_id,
            )
        )
    
    logger.debug(f"{unit.unit_id}: collected {len(screenshot_requests)} screenshot requests, "
                 f"{len(clip_requests)} clip requests")
    
    return MaterialRequests(
        screenshot_requests=screenshot_requests,
        clip_requests=clip_requests,
        action_classifications=action_classifications
    )


def apply_external_materials(
    self,
    unit: SemanticUnit,
    screenshots_dir: str,
    clips_dir: str,
    material_requests: MaterialRequests
):
    """
    [闁荤喍绀侀幊姗€宕㈤妶澶嬬劵闁哄嫬绻掔敮鍝?闁诲繐绻愬Λ妤吽囨繝姘劸闁靛鍎卞▍娆忣熆鐠虹儤鎼愰柕鍫滅矙閹啴宕熼鐙€妲洪梺鍝勵槹閸旀绮婇鈶╂敠闁归偊鍨煎Λ鍛存⒑閺夎法孝闁糕晛鐬奸幏鐘活敇濠靛牏鏋€闂佸憡顨嗗ú鏍储閹惧鈻旀い鎾村閸?
    闂佸搫鐗滈崜姘跺蓟閻斿皝鏋栭柡鍥╁枑绗?"婵犮垼鍩栭悧鐘诲磿鐎靛憡顫曢柣妯挎珪缂嶅繘骞栫€涙ɑ鈷掗柡? 闂佹眹鍔岀€氼參鎮х€圭姷鐤€闁告劑鍔庨弶浠嬫煟濠婂啫绨荤紒杈ㄧ箞閺屽懎顫濋鍌氱厬闁?RichTextPipeline 闂佸憡鏌ｉ崝宥夊焵椤戞寧绁伴柣銊у枛閹粙濡搁敐鍌氫壕?
    闂佸搫绉堕…鍫㈢紦閹灐瑙勬媴鐞涒剝鐓犻梺?    1. 闁荤姍鍐仾缂侇煈鍣ｅ畷锝夊箣閻樿尙鐤€闂佹寧绋掔喊宥夋偋閹绢喖绠?MaterialRequests 婵炴垶鎼╅崢鎯р枔?ID闂佹寧绋戦懟顖氾耿?screenshots_dir/clips_dir 婵炴垶鎼╅崣鍐焵椤掍礁绗掔紓宥咃躬瀵濡烽敃鈧ˉ婵嬫煛閸屾碍鐭楁繛鍡愬灲婵?    2. 濠碘槅鍨界槐鏇犳兜閿曞倸绀岀憸鐗堝笒鐢娊鏌ㄥ☉娆戭暡闁轰緡鍣ｉ獮鎰媴閸撳弶鈻肩紓浣割槸缁夌敻寮搁崘鈺冾浄閻犺櫣鍎ら崐鎶芥煛瀹ュ洤甯剁紒鎲嬬節瀹曟鎮℃惔顔兼櫍 (婵?flatten 闂佸憡绋栧Λ鍕箖閺囥垹违濞达絿顭堥惁鍫曟煕?婵炴垶鎸哥粔瀵糕偓鍨耿瀹?unit_id 缂?闂?    3. 闁荤姵鍔戦崝鎴﹀闯濞差亜绠崇憸宥夊春濡ゅ懏鏅?       - 闂佸憡宸婚弲婵嬪极?_concrete_validator 闁哄鏅滅粙鏍€侀幋锕€鐐婇柟顖嗗啫澹栭梺鍛婂姇閹冲酣顢欓幇顓ф鐎光偓閸愵亝顫?(闂佸憡锚椤兘宕?闂佸憡顭堥褍鈻?闂?       - 婵犮垼娉涚€氼噣骞冩繝鍕＜闁规儳顕埀顒夊灦瀹曠娀寮介妸銉у姷闂?(Structured Screenshots) 闂佹眹鍔岀€氼剟鎯堝鍜佸殨闁逞屽墴閺屽懘寮拌箛鏇炵闂?    4. 闂佺绻愰悿鍥ㄧ閸儱鍙婇柣妯垮皺濞堟悂鏌ㄥ☉娆掑闁汇劊鍨介獮宥夊箚瑜嶉悡鍌炴煟閵娿儱顏紒鈧畝鍕骇闁归偊浜為悷銏ゆ倵閻㈠灚鍤€缂併劍鐓￠幆?Step 闂?Action 闂佺绻愰悿鍥ㄧ閸儱违?    5. 闂佸搫鐗冮崑鎾剁磽娴ｅ摜澧ｆい銉ワ攻缁诲懘顢曢鍌滅崶闂佸搫娲ら悺銊╁蓟?unit.materials闂佹寧绋戦懟顖炴嚐閻旂厧鎹堕柕濞у啰绠掓繝銏″劶妞寸顪冮崒鐐茬鐟滄垿銆侀幋锕€绀傛繝濠傚暟娣囨椽鏌熷▓鍨簼鐎殿喖娼℃俊?
    闁哄鐗婇幐鎼佸矗閸℃稑鐭楅柛灞剧⊕濞堝爼鏌?    - self: 缂傚倷鐒﹂崹鐢告偩妤ｅ啯鍎?Pipeline 闁诲骸婀遍崑妯兼閵壯€鍋撻悽娈挎敯闁芥牕瀚版俊?    - unit: 闂佺儵鏅╅崰妤呮偉?SemanticUnit闂?    - screenshots_dir: 闂佽鎯屾禍婊兠瑰Ο鍏煎皫闁告洦鍓涢悥閬嶆煛瀹ュ懏璐℃繛鍙夊閵囨劙寮村Ο宄颁壕?    - clips_dir: 闁荤喐鐟ュΛ婵嬨€傞崼鏇熷亱闁搞儺鐓堥崬浠嬫偣瑜嶇€氼厾鑺遍鈧浠嬪捶椤撶喓鐛ラ悷婊呭濞插繘鍩€?    - material_requests: 闂佸憡顭囬崰搴綖閹扮増顥嗛柍褜鍓欐晥闁稿本绋撻鎼佹煕?(闂佸湱绮崝鎺旀?ID/Timestamp 缂備焦绋戦ˇ顖炲储閹捐鏋侀柣妤€鐗嗙粊锕€鈽夐幘绛规缂佹鎳樺?闂?    """
    materials = MaterialSet()
    screenshot_paths: List[str] = []
    screenshot_labels: List[str] = []
    screenshot_items: List[Dict[str, Any]] = []
    sentence_timestamps = self._build_sentence_timestamps()

    def _normalize_knowledge_type(raw_type: str) -> str:
        lowered = (raw_type or "").strip().lower()
        if any(key in lowered for key in ["process", "??", "??", "procedural"]):
            return "process"
        if any(key in lowered for key in ["concrete", "??", "??", "??", "??"]):
            return "concrete"
        if any(key in lowered for key in ["abstract", "??", "??", "??", "explanation"]):
            return "abstract"
        return lowered or "abstract"

    normalized_kt = _normalize_knowledge_type(str(getattr(unit, "knowledge_type", "") or ""))
    instructional_steps = getattr(unit, "instructional_steps", []) or []
    is_tutorial_stepwise_unit = normalized_kt == "process" and bool(instructional_steps)
    request_has_screenshot = bool(list(getattr(material_requests, "screenshot_requests", []) or []))
    # 闂佸憡顨呭ú銊︻殽閸モ晝椹抽柡宥庡亝濞堬綁鏌?    # 1) abstract/concrete 婵犳鍠栭鍥╁垝閹捐埖灏庨柣妤€鐗嗛悞濠氭煕閵夆晝鐣洪柣娑欑懅閹风姵绗熸繝鍕€?    # 2) process 婵炴垶鎸哥粔鎾疮閳ь剟鏌涘▎妯圭盎婵犫偓椤忓牊鈷旂€广儱娲悰鎾绘煕閹烘垶顥為柡渚囧櫍閹粙鎮㈤崨濠冪彙闂佹寧绋戦張顒佹櫠瀹ュ瀚?process闂佹寧绋戦悧鍡欌偓鍨耿楠炲繘顢楅崒婊冨綃 process + 闂佸搫瀚崕宕囨閿熺姴绠ｆい蹇撳缁傚牓鎮归崶顒佹暠闁活亙鍗抽弫宥嗗緞閹邦剙骞嬮柣鐘欏倸宓嗛柣娑欑懅閹风姵鎷呯喊妯轰壕?    is_process_degraded_branch = normalized_kt == "process" and not request_has_screenshot
    should_validate_screenshot = normalized_kt in {"abstract", "concrete", "process"}
    allow_clip = normalized_kt == "process"
    is_process_degraded_branch = (
        normalized_kt == "process"
        and not request_has_screenshot
        and not is_tutorial_stepwise_unit
    )
    should_validate_screenshot = (
        normalized_kt in {"abstract", "concrete", "process"}
        and not is_tutorial_stepwise_unit
    )
    allow_unit_dir_screenshot_fallback = not is_tutorial_stepwise_unit
    allow_unit_dir_clip_fallback = not is_tutorial_stepwise_unit

    def _resolve_path_key(path_item: str) -> str:
        try:
            return str(Path(path_item).resolve())
        except Exception:
            return os.path.abspath(path_item)

    def _deduplicate_paths(paths: List[str]) -> List[str]:
        ordered: List[str] = []
        seen: set[str] = set()
        for path_item in paths:
            key = _resolve_path_key(path_item)
            if key in seen:
                continue
            seen.add(key)
            ordered.append(path_item)
        return ordered

    def _dedupe_material_candidates_by_path(candidates: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        deduped: List[Tuple[Any, ...]] = []
        seen_paths: set[str] = set()
        for candidate in candidates:
            if not candidate:
                continue
            raw_path = str(candidate[0] or "").strip()
            if not raw_path:
                continue
            path_key = _resolve_path_key(raw_path)
            if path_key in seen_paths:
                continue
            seen_paths.add(path_key)
            deduped.append(candidate)
        return deduped

    def _compute_exact_image_signature(image_path: str) -> Optional[str]:
        if not image_path:
            return None
        try:
            image = cv2.imread(image_path, cv2.IMREAD_UNCHANGED)
            if image is None:
                return None
            hasher = hashlib.sha256()
            hasher.update(str(image.shape).encode("utf-8"))
            hasher.update(str(image.dtype).encode("utf-8"))
            hasher.update(image.tobytes())
            return hasher.hexdigest()
        except Exception:
            return None

    def _delete_source_file_under_assets(image_path: str, reason: str) -> bool:
        if not image_path:
            return False
        reason_text = str(reason or "screenshot").strip() or "screenshot"
        try:
            resolved = Path(image_path).resolve()
        except Exception:
            resolved = Path(os.path.abspath(image_path))
        try:
            assets_root = Path(self.assets_dir).resolve()
            resolved.relative_to(assets_root)
        except Exception:
            logger.warning(
                "%s: skip deleting %s outside assets: %s",
                unit.unit_id,
                reason_text,
                image_path,
            )
            return False
        try:
            if resolved.exists():
                os.remove(str(resolved))
                return True
        except Exception as delete_error:
            logger.warning(
                "%s: failed to delete %s: %s, err=%s",
                unit.unit_id,
                reason_text,
                image_path,
                delete_error,
            )
        return False

    def _dedupe_screenshot_candidates_exact_keep_earliest(
        candidates: List[Tuple[str, str, str, Optional[float]]]
    ) -> Tuple[List[Tuple[str, str, str, Optional[float]]], int]:
        if not candidates:
            return [], 0
        indexed = list(enumerate(candidates))

        def _sort_key(item: Tuple[int, Tuple[str, str, str, Optional[float]]]) -> Tuple[int, float, int]:
            idx, payload = item
            request_ts = payload[3]
            try:
                if request_ts is None:
                    return (0, float(idx), idx)
                return (1, float(request_ts), idx)
            except Exception:
                return (0, float(idx), idx)

        ordered = [payload for _, payload in sorted(indexed, key=_sort_key)]
        kept: List[Tuple[str, str, str, Optional[float]]] = []
        accepted_signatures: set[str] = set()
        duplicate_count = 0
        deleted_count = 0
        removed_count = 0
        global_seen = getattr(self, "_prestructure_seen_raw_signatures", None)
        if not isinstance(global_seen, dict):
            global_seen = {}
            self._prestructure_seen_raw_signatures = global_seen

        for raw_path, label, sid, request_ts in ordered:
            signature = _compute_exact_image_signature(raw_path)
            if not signature:
                kept.append((raw_path, label, sid, request_ts))
                continue
            if signature in accepted_signatures or signature in global_seen:
                duplicate_count += 1
                removed_count += 1
                if _delete_source_file_under_assets(raw_path, reason="duplicate screenshot"):
                    deleted_count += 1
                continue
            accepted_signatures.add(signature)
            global_seen[signature] = raw_path
            kept.append((raw_path, label, sid, request_ts))

        if duplicate_count > 0:
            logger.info(
                "%s: pre-structure exact dedupe removed %d duplicate screenshot(s), deleted=%d, kept=%d",
                unit.unit_id,
                duplicate_count,
                deleted_count,
                len(kept),
            )
        return kept, removed_count

    def _collect_candidates_by_id(base_dir: str, req_id: str, exts: List[str]) -> List[str]:
        candidates: List[str] = []
        raw_id = str(req_id or "").strip().replace("\\", "/")
        if not raw_id:
            return candidates

        raw_id = raw_id.strip("/")

        raw_path = Path(raw_id)
        base_name = raw_path.name
        stem_name = raw_path.stem if raw_path.suffix else base_name
        normalized_exts = [str(ext or "").strip() for ext in exts if str(ext or "").strip()]
        base_path = Path(base_dir)
        unit_id = str(unit.unit_id or "").strip()

        checks: List[Path] = [base_path / raw_id]
        if raw_path.suffix:
            checks.append(base_path / raw_path.parent / base_name)
        for ext in normalized_exts:
            checks.append(base_path / raw_path.parent / f"{stem_name}{ext}")

        # 闂佺绻掗崢褔顢欓幇鏉跨濞达絾鎮傞幐顒勬煕濞戞粍顥夐柟顔芥礋閺佸秴顫濋鐕傜礈闂備胶鍋撳畷姗€鎯屾ィ鍐ㄧ煑妞ゆ牗绻傞崢鎾倶?"SU001/xxx" 闂佽В鍋撻柣锝呮湰绾剧鈽?"SU001_xxx"
        if "/" in raw_id:
            flattened = raw_id.replace("/", "_")
            flat_path = Path(flattened)
            flat_name = flat_path.name
            flat_stem = flat_path.stem if flat_path.suffix else flat_name
            checks.append(base_path / flattened)
            if flat_path.suffix:
                checks.append(base_path / flat_name)
            for ext in normalized_exts:
                checks.append(base_path / f"{flat_stem}{ext}")

        # 闂佺绻掗崢褔顢?legacy 闁荤姴娲弨閬嶆儑娴煎瓨鏅慨姗嗗幗閿?unit 闂佸搫鍊稿ú锝呪枎閵忊€崇窞閻熸瑥瀚ˇ褔鏌ㄥ☉妯垮闁汇劌澧介幏鐘诲即閻愭潙鈧亶鏌涘顒傂＄紒?unit 闂佺儵鏅╅崰鏍礊瀹ュ绀冮柛娑卞幖閻栭亶姊?
        if "/" not in raw_id:
            checks.append(base_path / base_name)
            if raw_path.suffix:
                checks.append(base_path / base_name)
            for ext in normalized_exts:
                checks.append(base_path / f"{stem_name}{ext}")
                if unit_id:
                    checks.append(base_path / unit_id / f"{stem_name}{ext}")

            if unit_id:
                unit_dir = base_path / unit_id
                if unit_dir.exists():
                    for ext in normalized_exts:
                        for item in unit_dir.glob(f"*{stem_name}*{ext}"):
                            if item.is_file():
                                checks.append(item)

        seen_checks: set[str] = set()
        for check in checks:
            try:
                check_key = str(check.resolve())
            except Exception:
                check_key = os.path.abspath(str(check))
            if check_key in seen_checks:
                continue
            seen_checks.add(check_key)
            if check.exists():
                candidates.append(str(check))
        return _deduplicate_paths(candidates)

    def _collect_candidates_by_unit_dir(base_dir: str, exts: List[str]) -> List[str]:
        base_path = Path(base_dir)
        if not base_path.exists():
            return []

        normalized_exts = {ext.lower() for ext in exts}
        unit_id = str(unit.unit_id or "").strip()
        if not unit_id:
            return []

        candidates: List[str] = []

        unit_dir = base_path / unit_id
        if unit_dir.exists():
            for item in unit_dir.rglob("*"):
                if item.is_file() and item.suffix.lower() in normalized_exts:
                    candidates.append(str(item))

        for ext in normalized_exts:
            for item in base_path.glob(f"{unit_id}*{ext}"):
                if item.is_file():
                    candidates.append(str(item))

        candidates = _deduplicate_paths(candidates)
        candidates.sort(key=lambda current: Path(current).name.lower())
        return candidates

    def _build_id_aliases(raw_id: str, matched_path: str = "") -> List[str]:
        aliases: List[str] = []

        normalized = str(raw_id or "").strip().replace("\\", "/").strip("/")
        if normalized:
            aliases.append(normalized)
            aliases.append(Path(normalized).stem)
            if "/" in normalized:
                tail = normalized.split("/", 1)[1]
                aliases.append(tail)
                aliases.append(Path(tail).stem)

        if matched_path:
            stem = Path(matched_path).stem
            aliases.append(stem)
            unit_prefix = str(unit.unit_id or "").strip()
            if unit_prefix:
                aliases.append(f"{unit_prefix}/{stem}")

        ordered_aliases: List[str] = []
        seen_aliases: set[str] = set()
        for alias in aliases:
            key = str(alias or "").strip().replace("\\", "/").strip("/")
            if not key or key in seen_aliases:
                continue
            seen_aliases.add(key)
            ordered_aliases.append(key)
        return ordered_aliases

    def _append_unique_path(target: List[str], candidate_path: str):
        if not candidate_path:
            return
        normalized_key = _resolve_path_key(candidate_path)

        existing_keys: set[str] = set()
        for current in target:
            existing_keys.add(_resolve_path_key(current))

        if normalized_key not in existing_keys:
            target.append(candidate_path)

    assets_root = Path(self.assets_dir)

    def _normalize_existing_asset_path(
        source_path: str,
        kind: str,
        source_id: str,
        require_exists: bool = True,
    ) -> str:
        candidate = Path(source_path)
        if require_exists and not candidate.exists():
            return ""

        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = Path(os.path.abspath(str(candidate)))

        try:
            resolved.relative_to(assets_root.resolve())
        except Exception:
            logger.warning(
                "Skip %s outside assets in no-copy mode: unit=%s id=%s path=%s",
                kind,
                unit.unit_id,
                source_id,
                source_path,
            )
            return ""

        return str(resolved)

    screenshot_candidates: List[Tuple[str, str, str, Optional[float]]] = []
    matched_screenshot_by_id: Dict[str, List[str]] = {}
    request_meta_by_id: Dict[str, ScreenshotRequest] = {}
    unit_screenshot_requests: List[ScreenshotRequest] = []
    if material_requests.screenshot_requests:
        for req in material_requests.screenshot_requests:
            if req.semantic_unit_id != unit.unit_id:
                continue
            unit_screenshot_requests.append(req)
            req_id = str(req.screenshot_id or "").strip()
            if req_id:
                for alias_id in _build_id_aliases(req_id):
                    request_meta_by_id.setdefault(alias_id, req)
            req_paths = _collect_candidates_by_id(
                screenshots_dir,
                req.screenshot_id,
                [".png", ".jpg", ".jpeg"],
            )
            for path_item in req_paths:
                screenshot_candidates.append((path_item, req.label, req.screenshot_id, float(req.timestamp_sec)))
                for alias_id in _build_id_aliases(req_id, path_item):
                    request_meta_by_id.setdefault(alias_id, req)

    if not screenshot_candidates and allow_unit_dir_screenshot_fallback:
        fallback_screenshots = _collect_candidates_by_unit_dir(
            screenshots_dir,
            [".png", ".jpg", ".jpeg"],
        )
        if fallback_screenshots:
            logger.info(
                "%s: fallback matched %d screenshot(s) by unit folder scan",
                unit.unit_id,
                len(fallback_screenshots),
            )
            enable_ordered_request_fallback = (
                len(unit_screenshot_requests) > 0
                and len(unit_screenshot_requests) == len(fallback_screenshots)
            )
            for fallback_idx, path_item in enumerate(fallback_screenshots):
                screenshot_stem = Path(path_item).stem
                screenshot_id = f"{unit.unit_id}/{screenshot_stem}"
                candidate_label = screenshot_stem
                candidate_sid = screenshot_id
                candidate_ts: Optional[float] = None
                matched_req: Optional[ScreenshotRequest] = None
                if enable_ordered_request_fallback and fallback_idx < len(unit_screenshot_requests):
                    matched_req = unit_screenshot_requests[fallback_idx]
                    candidate_label = str(getattr(matched_req, "label", "") or screenshot_stem)
                    candidate_sid = str(getattr(matched_req, "screenshot_id", "") or screenshot_id)
                    try:
                        candidate_ts = float(getattr(matched_req, "timestamp_sec"))
                    except Exception:
                        candidate_ts = None
                screenshot_candidates.append((path_item, candidate_label, candidate_sid, candidate_ts))
                if matched_req is not None:
                    for alias_id in _build_id_aliases(candidate_sid, path_item):
                        request_meta_by_id.setdefault(alias_id, matched_req)
    elif not screenshot_candidates and is_tutorial_stepwise_unit:
        logger.info(
            "%s: tutorial_stepwise unit skip unit-folder screenshot fallback, keep request-id matching only",
            unit.unit_id,
        )

    def _resolve_request_meta_for_candidate(sid_text: str, raw_path: str) -> Optional[ScreenshotRequest]:
        for alias_id in _build_id_aliases(str(sid_text or ""), str(raw_path or "")):
            req_meta = request_meta_by_id.get(alias_id)
            if req_meta is not None:
                return req_meta
        if len(unit_screenshot_requests) == 1:
            # 兜底：当仅有一个请求且文件命名偏差导致未命中别名时，复用唯一请求元信息补齐时间戳。
            return unit_screenshot_requests[0]
        return None

    def _prefilter_screenshot_candidates(
        candidates: List[Tuple[str, str, str, Optional[float]]]
    ) -> Tuple[List[Dict[str, Any]], int]:
        # 候选预处理顺序：规范化 -> 路径去重 ->（可选）精确去重。
        normalized_candidates = list(candidates or [])
        normalized_candidates = _dedupe_material_candidates_by_path(normalized_candidates)
        pre_dedupe_removed_count = 0
        if should_validate_screenshot and normalized_candidates:
            normalized_candidates, pre_dedupe_removed_count = _dedupe_screenshot_candidates_exact_keep_earliest(
                normalized_candidates
            )

        expanded: List[Dict[str, Any]] = []
        dropped_count = 0
        expanded_items_by_index: List[Optional[List[Dict[str, Any]]]] = [
            None for _ in normalized_candidates
        ]

        if should_validate_screenshot and self._concrete_validator and normalized_candidates:
            extractor = getattr(self._concrete_validator, "extract_structured_screenshots", None)
            if callable(extractor):
                use_process_parallel = _should_enable_structure_process_parallel(
                    len(normalized_candidates),
                    self._concrete_validator,
                )

                if use_process_parallel:
                    worker_count = _resolve_parallel_workers(
                        raw_value=os.getenv("PHASE2B_STRUCTURE_PREPROCESS_WORKERS", "auto"),
                        task_count=len(normalized_candidates),
                        hard_cap=16,
                    )
                    t0 = time.perf_counter()
                    try:
                        pool = _get_phase2b_structure_pool(worker_count)
                        future_to_index: Dict[concurrent.futures.Future, int] = {}
                        for candidate_index, (
                            raw_path,
                            _label,
                            sid,
                            request_ts,
                        ) in enumerate(normalized_candidates):
                            task_payload = {
                                "index": candidate_index,
                                "image_path": raw_path,
                                "source_id": sid,
                                "timestamp_sec": request_ts,
                                "output_dir": str(getattr(self, "output_dir", "") or ""),
                            }
                            future = pool.submit(_phase2b_structure_worker_run, task_payload)
                            future_to_index[future] = candidate_index

                        for future in concurrent.futures.as_completed(future_to_index):
                            candidate_index = future_to_index[future]
                            raw_path, _label, sid, _request_ts = normalized_candidates[candidate_index]
                            try:
                                payload = future.result()
                                expanded_items_by_index[candidate_index] = payload.get("items")
                                error_text = str(payload.get("error", "") or "").strip()
                                if error_text:
                                    logger.warning(
                                        "%s: structure preprocess failed, fallback to raw screenshot: %s, err=%s",
                                        unit.unit_id,
                                        sid or raw_path,
                                        error_text,
                                    )
                            except Exception as extract_error:
                                logger.warning(
                                    "%s: structure preprocess failed, fallback to raw screenshot: %s, err=%s",
                                    unit.unit_id,
                                    sid or raw_path,
                                    extract_error,
                                )
                                expanded_items_by_index[candidate_index] = None

                        elapsed_ms = (time.perf_counter() - t0) * 1000.0
                        logger.info(
                            "%s: structure preprocess process-parallel done: tasks=%d workers=%d elapsed_ms=%.1f",
                            unit.unit_id,
                            len(normalized_candidates),
                            worker_count,
                            elapsed_ms,
                        )
                    except Exception as parallel_error:
                        logger.warning(
                            "%s: structure preprocess process-parallel unavailable, fallback to serial: err=%s",
                            unit.unit_id,
                            parallel_error,
                        )
                        expanded_items_by_index = [None for _ in normalized_candidates]

                # 对未命中并行结果的条目执行串行兜底，保证行为与旧逻辑一致。
                for candidate_index, (raw_path, _label, sid, request_ts) in enumerate(normalized_candidates):
                    if expanded_items_by_index[candidate_index] is not None:
                        continue
                    try:
                        expanded_items_by_index[candidate_index] = extractor(
                            image_path=raw_path,
                            source_id=sid,
                            timestamp_sec=float(request_ts) if request_ts is not None else None,
                        )
                    except Exception as extract_error:
                        logger.warning(
                            "%s: structure preprocess failed, fallback to raw screenshot: %s, err=%s",
                            unit.unit_id,
                            sid or raw_path,
                            extract_error,
                        )
                        expanded_items_by_index[candidate_index] = None

        for candidate_index, (raw_path, label, sid, request_ts) in enumerate(normalized_candidates):
            parent_key = _resolve_path_key(raw_path)
            expanded_items = expanded_items_by_index[candidate_index]

            if expanded_items is None:
                expanded.append(
                    {
                        "image_path": raw_path,
                        "label": label,
                        "sid": sid,
                        "request_ts": request_ts,
                        "parent_key": parent_key,
                        "parent_image_path": raw_path,
                        "group_type": "",
                        "is_structured_crop": False,
                        "skip_validator_duplicate_check": True,
                        "_order": candidate_index,
                    }
                )
                continue

            if not expanded_items:
                dropped_count += 1
                expanded.append(
                    {
                        "image_path": raw_path,
                        "label": label,
                        "sid": sid,
                        "request_ts": request_ts,
                        "parent_key": parent_key,
                        "parent_image_path": raw_path,
                        "group_type": "text_only",
                        "is_structured_crop": False,
                        "skip_validator_duplicate_check": True,
                        "_order": candidate_index,
                        "ppstructure_text_only": True,
                    }
                )
                logger.info(
                    "%s: structure preprocess marked screenshot as text-only (kept for OCR description): %s",
                    unit.unit_id,
                    sid or raw_path,
                )
                continue

            for derived_index, derived in enumerate(expanded_items, start=1):
                derived_path = str(derived.get("image_path", "") or "")
                if not derived_path:
                    continue
                group_type = str(derived.get("group_type", "") or "").strip()
                derived_ts = derived.get("timestamp_sec", request_ts)
                derived_label = label or sid or Path(raw_path).stem
                if group_type:
                    derived_label = f"{derived_label}:{group_type}_{derived_index:02d}"
                expanded.append(
                    {
                        "image_path": derived_path,
                        "label": derived_label,
                        "sid": sid,
                        "request_ts": float(derived_ts) if derived_ts is not None else None,
                        "parent_key": str(derived.get("parent_key", parent_key) or parent_key),
                        "parent_image_path": str(derived.get("parent_image_path", raw_path) or raw_path),
                        "group_type": group_type,
                        "is_structured_crop": bool(derived.get("is_structured_crop", True)),
                        "crop_index": int(derived.get("crop_index", derived_index) or derived_index),
                        "bbox_xyxy": derived.get("bbox_xyxy"),
                        "bbox_normalized_xyxy": derived.get("bbox_normalized_xyxy"),
                        "parent_image_size": derived.get("parent_image_size"),
                        "skip_validator_duplicate_check": True,
                        "_order": candidate_index,
                    }
                )

        if should_validate_screenshot and self._concrete_validator and expanded:
            deduper = getattr(self._concrete_validator, "dedupe_structured_candidates_keep_latest", None)
            if callable(deduper):
                try:
                    expanded = deduper(expanded)
                except Exception as dedupe_error:
                    logger.warning(
                        "%s: structure dedupe failed, keep pre-dedupe candidates, err=%s",
                        unit.unit_id,
                        dedupe_error,
                    )
        if pre_dedupe_removed_count > 0:
            # 预结构化阶段已经确认重复并移除；若本单元候选清空，应禁止 unit-scan fallback 回灌重复图。
            dropped_count += pre_dedupe_removed_count
        return expanded, dropped_count

    effective_screenshot_candidates, structure_preprocess_drop_count = _prefilter_screenshot_candidates(
        screenshot_candidates
    )

    def _candidate_sort_key(candidate: Dict[str, Any]) -> Tuple[int, float, int, str]:
        try:
            order_idx = int(candidate.get("_order", 0) or 0)
        except Exception:
            order_idx = 0
        try:
            ts = candidate.get("request_ts")
            ts_value = float(ts) if ts is not None else -1.0
        except Exception:
            ts_value = -1.0
        try:
            crop_idx = int(candidate.get("crop_index", 0) or 0)
        except Exception:
            crop_idx = 0
        path_key = str(candidate.get("image_path", "") or "")
        return (order_idx, ts_value, crop_idx, path_key)

    if should_validate_screenshot and self._concrete_validator and effective_screenshot_candidates:
        grouped_validator = getattr(self._concrete_validator, "validate_structured_group", None)
        if callable(grouped_validator):
            grouped_candidates: Dict[str, List[Dict[str, Any]]] = {}
            for candidate in effective_screenshot_candidates:
                if not bool(candidate.get("is_structured_crop", False)):
                    continue
                raw_path = str(candidate.get("image_path", "") or "").strip()
                if not raw_path or not os.path.exists(raw_path):
                    continue
                parent_key = str(candidate.get("parent_key", "") or "").strip()
                if not parent_key:
                    continue
                grouped_candidates.setdefault(parent_key, []).append(candidate)

            for parent_key, group_items in grouped_candidates.items():
                if len(group_items) <= 1:
                    continue

                ordered_group_items = sorted(group_items, key=_candidate_sort_key)
                unresolved_count = 0
                for item in ordered_group_items:
                    item_path = str(item.get("image_path", "") or "").strip()
                    if not item_path:
                        unresolved_count += 1
                        continue
                    try:
                        item_key = str(Path(item_path).resolve())
                    except Exception:
                        item_key = os.path.abspath(item_path)
                    if item_key in self._prevalidated_concrete_results:
                        unresolved_count += 1
                if unresolved_count == len(ordered_group_items):
                    continue

                parent_image_path = str(
                    ordered_group_items[0].get("parent_image_path", "") or ordered_group_items[0].get("image_path", "")
                ).strip()
                if not parent_image_path:
                    parent_image_path = str(ordered_group_items[0].get("image_path", "") or "").strip()

                group_ocr_text = ""
                for item in ordered_group_items:
                    sid_key = str(item.get("sid", "") or "").strip()
                    item_path = str(item.get("image_path", "") or "").strip()
                    req_meta = _resolve_request_meta_for_candidate(sid_key, item_path)
                    if req_meta is None:
                        continue
                    maybe_text = str(getattr(req_meta, "ocr_text", "") or "").strip()
                    if maybe_text:
                        group_ocr_text = maybe_text
                        break

                try:
                    grouped_results = grouped_validator(
                        parent_image_path=parent_image_path,
                        items=ordered_group_items,
                        ocr_text=group_ocr_text,
                    )
                    if len(grouped_results) != len(ordered_group_items):
                        logger.warning(
                            "%s: grouped structured validation size mismatch, parent=%s expected=%d got=%d",
                            unit.unit_id,
                            parent_key,
                            len(ordered_group_items),
                            len(grouped_results),
                        )
                        continue

                    for item, result_obj in zip(ordered_group_items, grouped_results):
                        item_path = str(item.get("image_path", "") or "").strip()
                        if not item_path:
                            continue
                        try:
                            item_key = str(Path(item_path).resolve())
                        except Exception:
                            item_key = os.path.abspath(item_path)
                        self._prevalidated_concrete_results[item_key] = result_obj
                    logger.info(
                        "%s: grouped structured validation succeeded, parent=%s crops=%d",
                        unit.unit_id,
                        parent_key,
                        len(ordered_group_items),
                    )
                except Exception as grouped_error:
                    logger.warning(
                        "%s: grouped structured validation failed, fallback to per-crop validate, parent=%s crops=%d err=%s",
                        unit.unit_id,
                        parent_key,
                        len(ordered_group_items),
                        grouped_error,
                    )

    if structure_preprocess_drop_count > 0:
        logger.info(
            "%s: structure preprocess dropped %d original screenshot candidate(s)",
            unit.unit_id,
            structure_preprocess_drop_count,
        )

    skip_unit_scan_fallback = (
        is_tutorial_stepwise_unit
        or (
            should_validate_screenshot
            and structure_preprocess_drop_count > 0
            and not effective_screenshot_candidates
        )
    )

    def _read_image_bytes(image_path: str) -> Optional[bytes]:
        if not image_path:
            return None
        try:
            with open(image_path, "rb") as image_file:
                return image_file.read()
        except Exception:
            return None

    def _call_validator_validate(
        image_path: str,
        ocr_text_hint: str,
        skip_duplicate_check: bool,
    ) -> Any:
        try:
            return self._concrete_validator.validate(
                image_path,
                ocr_text=ocr_text_hint,
                skip_duplicate_check=skip_duplicate_check,
            )
        except TypeError:
            # 兼容旧版本 validator 不支持 skip_duplicate_check 形参。
            try:
                return self._concrete_validator.validate(image_path, ocr_text=ocr_text_hint)
            except TypeError:
                return self._concrete_validator.validate(image_path)

    original_image_bytes_by_key: Dict[str, Optional[bytes]] = {}
    if should_validate_screenshot and self._concrete_validator and effective_screenshot_candidates:
        batch_validator = getattr(self._concrete_validator, "validate_batch", None)
        pending_batch_tasks: List[Dict[str, Any]] = []
        pending_batch_meta: List[Tuple[str, str, str]] = []

        for candidate in effective_screenshot_candidates:
            raw_path = str(candidate.get("image_path", "") or "").strip()
            sid = str(candidate.get("sid", "") or "").strip()
            if not raw_path or not os.path.exists(raw_path):
                continue

            pre_key = _resolve_path_key(raw_path)
            if pre_key in self._prevalidated_concrete_results:
                continue

            req_meta = _resolve_request_meta_for_candidate(sid, raw_path)
            ocr_text_hint = ""
            if req_meta is not None:
                ocr_text_hint = str(getattr(req_meta, "ocr_text", "") or "").strip()
            skip_validator_duplicate_check = bool(
                candidate.get("skip_validator_duplicate_check", False)
            )
            original_image_bytes_by_key.setdefault(pre_key, _read_image_bytes(raw_path))
            pending_batch_tasks.append(
                {
                    "image_path": raw_path,
                    "ocr_text": ocr_text_hint,
                    "skip_duplicate_check": skip_validator_duplicate_check,
                }
            )
            pending_batch_meta.append((pre_key, raw_path, sid))

        unresolved_task_indexes = set(range(len(pending_batch_tasks)))
        if pending_batch_tasks and callable(batch_validator):
            batch_t0 = time.perf_counter()
            try:
                batch_results = batch_validator(pending_batch_tasks)
                if (
                    isinstance(batch_results, list)
                    and len(batch_results) == len(pending_batch_tasks)
                ):
                    for task_index, result_obj in enumerate(batch_results):
                        pre_key, _raw_path, _sid = pending_batch_meta[task_index]
                        self._prevalidated_concrete_results[pre_key] = result_obj
                        unresolved_task_indexes.discard(task_index)
                    logger.info(
                        "%s: screenshot validator batch-preload done: tasks=%d elapsed_ms=%.1f",
                        unit.unit_id,
                        len(pending_batch_tasks),
                        (time.perf_counter() - batch_t0) * 1000.0,
                    )
                else:
                    logger.warning(
                        "%s: screenshot validator batch-preload returned invalid result size, fallback to single validate",
                        unit.unit_id,
                    )
            except Exception as batch_error:
                logger.warning(
                    "%s: screenshot validator batch-preload failed, fallback to single validate: err=%s",
                    unit.unit_id,
                    batch_error,
                )

        for task_index in sorted(unresolved_task_indexes):
            pre_key, raw_path, sid = pending_batch_meta[task_index]
            task = pending_batch_tasks[task_index]
            res = _call_validator_validate(
                image_path=raw_path,
                ocr_text_hint=str(task.get("ocr_text", "") or "").strip(),
                skip_duplicate_check=bool(task.get("skip_duplicate_check", False)),
            )
            self._prevalidated_concrete_results[pre_key] = res
            logger.debug(
                "%s: screenshot validator single-preload done: %s",
                unit.unit_id,
                sid or raw_path,
            )

    rejected_screenshot_count = 0
    for _idx, candidate in enumerate(effective_screenshot_candidates, start=1):
        raw_path = str(candidate.get("image_path", "") or "")
        label = str(candidate.get("label", "") or "")
        sid = str(candidate.get("sid", "") or "")
        group_type = str(candidate.get("group_type", "") or "").strip()
        is_ppstructure_text_only = bool(candidate.get("ppstructure_text_only", False))
        parent_key = str(candidate.get("parent_key", "") or "").strip()
        request_ts = candidate.get("request_ts")
        skip_validator_duplicate_check = bool(candidate.get("skip_validator_duplicate_check", False))
        is_valid = True
        img_description = ""
        candidate_file = ""
        if raw_path:
            try:
                candidate_file = Path(raw_path).name
            except Exception:
                candidate_file = raw_path

        req_meta = _resolve_request_meta_for_candidate(str(sid or "").strip(), raw_path)
        ocr_text_hint = ""
        if req_meta is not None:
            ocr_text_hint = str(getattr(req_meta, "ocr_text", "") or "").strip()
        if request_ts is None and req_meta is not None:
            try:
                request_ts = float(req_meta.timestamp_sec)
            except Exception:
                request_ts = None

        sentence_id = ""
        sentence_text = ""
        if request_ts is not None:
            sentence_id = self._map_timestamp_to_sentence_id(float(request_ts), sentence_timestamps)
            if sentence_id:
                sentence_text = self._get_sentence_text_by_id(sentence_id)

        if should_validate_screenshot and self._concrete_validator:
            pre_key = _resolve_path_key(raw_path)
            if pre_key in original_image_bytes_by_key:
                original_image_bytes = original_image_bytes_by_key.get(pre_key)
            else:
                original_image_bytes = _read_image_bytes(raw_path)
                original_image_bytes_by_key[pre_key] = original_image_bytes

            if pre_key in self._prevalidated_concrete_results:
                res = self._prevalidated_concrete_results[pre_key]
            else:
                res = _call_validator_validate(
                    image_path=raw_path,
                    ocr_text_hint=ocr_text_hint,
                    skip_duplicate_check=skip_validator_duplicate_check,
                )
            img_description = str(getattr(res, "img_description", "") or getattr(res, "reason", "")).strip()
            res_reason = str(getattr(res, "reason", "") or "").strip()
            res_reason_lower = res_reason.lower()
            res_concrete_type = str(getattr(res, "concrete_type", "") or "").strip().lower()
            is_person_prefilter_reject = bool(
                (not res.should_include)
                and (
                    res_concrete_type == "person_subject"
                    or "person_subject_prefilter" in res_reason_lower
                    or "person-subject prefilter" in res_reason_lower
                    or "预过滤" in res_reason
                )
            )
            if not res.should_include:
                if is_person_prefilter_reject and _delete_source_file_under_assets(
                    raw_path, reason="person-subject prefilter screenshot"
                ):
                    logger.info(
                        "%s: deleted person-prefilter screenshot: %s",
                        unit.unit_id,
                        sid or raw_path,
                    )
                # 闂傚倸鍟ぐ鍐焊娴犲绠戠憸搴㈢椤旇棄绶炵€广儱绻掔粣妤呮⒑椤斿搫濡奸柛銊ュ椤ㄣ儱鐣濋崘顏咁潔闂侀潻绲婚崝瀣娴兼潙绀嗛柣妯肩帛閻濈喖姊婚崼銏犱粶闁告瑨娉曢幐鎺楀焺閸愩劎鍔甸梺鎼炲劜閹锋繄妲愬┑瀣畳闁靛繒濯Σ濠氭煕濮橆剟顎楃憸鏉垮级缁楃喎鈽夊Ο璇测偓鐢电磽娓氬洤骞栭柛妯款潐閹棃寮撮悙鑼煃闂備焦婢樼粔铏箾閸ヮ剚鍋?
                if (
                    not is_person_prefilter_reject
                    and not is_ppstructure_text_only
                    and original_image_bytes is not None
                    and not os.path.exists(raw_path)
                ):
                    try:
                        Path(raw_path).parent.mkdir(parents=True, exist_ok=True)
                        with open(raw_path, "wb") as image_file:
                            image_file.write(original_image_bytes)
                        logger.warning(
                            "%s: validator deleted screenshot, restored for fallback: %s",
                            unit.unit_id,
                            sid or raw_path,
                        )
                    except Exception as restore_error:
                        logger.warning(
                            "%s: failed to restore validator-deleted screenshot: %s, err=%s",
                            unit.unit_id,
                            sid or raw_path,
                            restore_error,
                        )
                rejected_path = _normalize_existing_asset_path(raw_path, "img", sid or label)
                if not rejected_path and is_ppstructure_text_only:
                    # 纯文本页只保留 OCR 描述，允许在删除原图后继续记录元数据。
                    rejected_path = _normalize_existing_asset_path(
                        raw_path,
                        "img",
                        sid or label,
                        require_exists=False,
                    )
                if rejected_path or is_ppstructure_text_only:
                    resolved_desc = img_description or label or sid or "validator_rejected"
                    img_index = len(screenshot_items) + 1
                    img_id = f"{unit.unit_id}_img_{img_index:02d}"
                    mapping_status = "excluded_by_validator"
                    if request_ts is None:
                        mapping_status = "excluded_no_timestamp"
                    elif not sentence_id:
                        mapping_status = "excluded_unmapped"
                    screenshot_items.append({
                        "img_id": img_id,
                        "img_path": rejected_path,
                        "img_description": resolved_desc,
                        "img_desription": resolved_desc,
                        "should_include": False,
                        "label": label,
                        "source_id": sid,
                        "timestamp_sec": float(request_ts) if request_ts is not None else None,
                        "sentence_id": sentence_id,
                        "sentence_text": sentence_text,
                    })
                    self._record_image_match_audit(
                        unit_id=unit.unit_id,
                        img_id=img_id,
                        source_id=sid,
                        timestamp_sec=float(request_ts) if request_ts is not None else None,
                        sentence_id=sentence_id,
                        sentence_text=sentence_text,
                        img_description=resolved_desc,
                        mapping_status=mapping_status,
                    )
                    if is_ppstructure_text_only:
                        if _delete_source_file_under_assets(raw_path, reason="text-only screenshot"):
                            logger.info(
                                "%s: deleted text-only screenshot after keeping OCR description: %s",
                                unit.unit_id,
                                sid or raw_path,
                            )
                logger.info(
                    "%s: Removing negative screenshot: sid=%s file=%s group_type=%s parent_key=%s reason=%s",
                    unit.unit_id,
                    sid or "-",
                    candidate_file or raw_path or "-",
                    group_type or "-",
                    parent_key or "-",
                    str(getattr(res, "reason", "") or "").strip() or "-",
                )
                is_valid = False
                rejected_screenshot_count += 1

        if not is_valid:
            continue

        normalized_path = _normalize_existing_asset_path(raw_path, "img", sid or label)
        if not normalized_path:
            continue

        screenshot_paths.append(normalized_path)
        screenshot_labels.append(label or sid)

        for alias_id in _build_id_aliases(str(sid or ""), normalized_path):
            matched_screenshot_by_id.setdefault(alias_id, []).append(normalized_path)

        resolved_desc = img_description or label or sid
        img_index = len(screenshot_items) + 1
        img_id = f"{unit.unit_id}_img_{img_index:02d}"
        mapping_status = "mapped"
        if request_ts is None:
            mapping_status = "no_timestamp"
        elif not sentence_id:
            mapping_status = "unmapped"

        screenshot_items.append({
            "img_id": img_id,
            "img_path": normalized_path,
            "img_description": resolved_desc,
            "img_desription": resolved_desc,
            "should_include": True,
            "label": label,
            "source_id": sid,
            "timestamp_sec": float(request_ts) if request_ts is not None else None,
            "sentence_id": sentence_id,
            "sentence_text": sentence_text,
        })

        self._record_image_match_audit(
            unit_id=unit.unit_id,
            img_id=img_id,
            source_id=sid,
            timestamp_sec=float(request_ts) if request_ts is not None else None,
            sentence_id=sentence_id,
            sentence_text=sentence_text,
            img_description=resolved_desc,
            mapping_status=mapping_status,
        )

    if should_validate_screenshot:
        logger.info(
            f"{unit.unit_id}: screenshot validation kept={len(screenshot_paths)}, "
            f"rejected={rejected_screenshot_count}"
        )
    clip_paths: List[str] = []
    matched_clip_by_id: Dict[str, List[str]] = {}
    if allow_clip:
        clip_candidates: List[Tuple[str, str]] = []
        if material_requests.clip_requests:
            for req in material_requests.clip_requests:
                if req.semantic_unit_id != unit.unit_id:
                    continue
                for path_item in _collect_candidates_by_id(clips_dir, req.clip_id, [".mp4", ".webm", ".mkv"]):
                    clip_candidates.append((path_item, req.clip_id))

        if not clip_candidates and allow_unit_dir_clip_fallback:
            fallback_clips = _collect_candidates_by_unit_dir(clips_dir, [".mp4", ".webm", ".mkv"])
            if fallback_clips:
                logger.info(
                    "%s: fallback matched %d clip(s) by unit folder scan",
                    unit.unit_id,
                    len(fallback_clips),
                )
                for path_item in fallback_clips:
                    clip_stem = Path(path_item).stem
                    clip_id = f"{unit.unit_id}/{clip_stem}"
                    clip_candidates.append((path_item, clip_id))
        elif not clip_candidates and is_tutorial_stepwise_unit:
            logger.info(
                "%s: tutorial_stepwise unit skip unit-folder clip fallback, keep request-id matching only",
                unit.unit_id,
            )

        clip_candidates = [
            (str(candidate[0]), str(candidate[1]))
            for candidate in _dedupe_material_candidates_by_path(clip_candidates)
        ]
        for selected, selected_label in clip_candidates:
            normalized_clip_path = _normalize_existing_asset_path(selected, "clip", selected_label)
            if normalized_clip_path:
                clip_paths.append(normalized_clip_path)
                for alias_id in _build_id_aliases(str(selected_label or ""), normalized_clip_path):
                    matched_clip_by_id.setdefault(alias_id, []).append(normalized_clip_path)
    else:
        logger.info(f"Skip clip for non-process unit: {unit.unit_id} ({normalized_kt})")

    if instructional_steps:
        for step in instructional_steps:
            if not isinstance(step, dict):
                continue

            step_materials = step.get("materials", {})
            if not isinstance(step_materials, dict):
                step_materials = {}

            resolved_step_screenshots: List[str] = []
            raw_screenshot_ids = step_materials.get("screenshot_ids", []) or []
            for raw_screenshot_id in raw_screenshot_ids:
                screenshot_id = str(raw_screenshot_id or "").strip()
                if not screenshot_id:
                    continue

                hit_paths: List[str] = []
                for alias_id in _build_id_aliases(screenshot_id):
                    hit_paths.extend(matched_screenshot_by_id.get(alias_id, []))

                if not hit_paths:
                    fallback_paths = _collect_candidates_by_id(
                        screenshots_dir,
                        screenshot_id,
                        [".png", ".jpg", ".jpeg"],
                    )
                    for fallback_path in fallback_paths:
                        normalized_fallback_path = _normalize_existing_asset_path(
                            fallback_path,
                            "img",
                            screenshot_id,
                        )
                        if normalized_fallback_path:
                            hit_paths.append(normalized_fallback_path)

                for hit_path in _deduplicate_paths(hit_paths):
                    _append_unique_path(resolved_step_screenshots, hit_path)

            if resolved_step_screenshots:
                step_materials["screenshot_paths"] = resolved_step_screenshots

            resolved_step_clips: List[str] = []
            raw_clip_id = str(step_materials.get("clip_id", "") or "").strip()
            if raw_clip_id:
                clip_hits: List[str] = []
                for alias_id in _build_id_aliases(raw_clip_id):
                    clip_hits.extend(matched_clip_by_id.get(alias_id, []))

                if not clip_hits:
                    fallback_clip_paths = _collect_candidates_by_id(
                        clips_dir,
                        raw_clip_id,
                        [".mp4", ".webm", ".mkv"],
                    )
                    for fallback_clip_path in fallback_clip_paths:
                        normalized_fallback_clip_path = _normalize_existing_asset_path(
                            fallback_clip_path,
                            "clip",
                            raw_clip_id,
                        )
                        if normalized_fallback_clip_path:
                            clip_hits.append(normalized_fallback_clip_path)

                for hit_path in _deduplicate_paths(clip_hits):
                    _append_unique_path(resolved_step_clips, hit_path)

            if resolved_step_clips:
                step_materials["clip_paths"] = resolved_step_clips
                step_materials["clip_path"] = resolved_step_clips[0]

            step["materials"] = step_materials

    materials.screenshot_paths = screenshot_paths
    materials.screenshot_labels = screenshot_labels
    materials.screenshot_items = screenshot_items
    materials.clip_paths = clip_paths
    materials.clip_path = clip_paths[0] if clip_paths else ""
    materials.action_classifications = material_requests.action_classifications

    if not materials.screenshot_paths and not materials.screenshot_items and not skip_unit_scan_fallback:
        fallback_screenshots = _collect_candidates_by_unit_dir(
            screenshots_dir,
            [".png", ".jpg", ".jpeg"],
        )
        if fallback_screenshots:
            fallback_path = _normalize_existing_asset_path(
                fallback_screenshots[0],
                "img",
                f"{unit.unit_id}/fallback_unit_scan",
            )
            if fallback_path:
                materials.screenshot_paths = [fallback_path]
                materials.screenshot_labels = ["fallback_unit_scan"]
                materials.screenshot_items = [
                    {
                        "img_id": f"{unit.unit_id}_img_01",
                        "img_path": fallback_path,
                        "img_description": "fallback_unit_scan",
                        "img_desription": "fallback_unit_scan",
                        "should_include": True,
                        "label": "fallback_unit_scan",
                        "source_id": f"{unit.unit_id}/fallback_unit_scan",
                        "timestamp_sec": None,
                        "sentence_id": "",
                        "sentence_text": "",
                    }
                ]
                logger.warning(
                    "%s: materials empty after matching, fallback to unit scan screenshot: %s",
                    unit.unit_id,
                    fallback_path,
                )
    elif not materials.screenshot_paths and skip_unit_scan_fallback:
        if is_tutorial_stepwise_unit:
            logger.info(
                "%s: tutorial_stepwise unit skip unit-scan screenshot fallback (request-id mapping only)",
                unit.unit_id,
            )
        else:
            logger.info(
                "%s: skip unit-scan screenshot fallback because structure preprocess returned no target type",
                unit.unit_id,
            )

    unit.materials = materials

    logger.debug(
        f"{unit.unit_id}: applied {len(unit.materials.screenshot_paths)} external screenshots, "
        f"clips={len(unit.materials.clip_paths)}"
    )
    if not unit.materials.screenshot_paths and not clip_paths:
        logger.warning(f"{unit.unit_id}: no external materials matched in Phase2B")
