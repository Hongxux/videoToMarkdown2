# Python 模块职责与方法清单

- 更新日期：2026-02-09
- 范围：`services/python_grpc/src`

## 说明
- 职责描述：使用统一规则按目录层级描述模块责任。
- 核心方法与工具方法：基于语法树自动统计。
- 每个方法的步骤化注释以模块内文档字符串为准。
- 当前有 6 个模块因语法问题未能解析，已单独列出。

## `services/python_grpc/src/__init__.py`
- 职责与功能：负责根模块下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/common/__init__.py`
- 职责与功能：负责common下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/common/logging/__init__.py`
- 职责与功能：负责common/logging下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/common/logging/pipeline_logging.py`
- 职责与功能：负责common/logging下相关功能的实现、组装与协作。
- 核心方法：`ensure_degrade_level`、`is_degrade_message`、`AutoDegradeFilter.filter`、`ColorConsoleFormatter.format`、`configure_pipeline_logging`、`get_pipeline_logger`、`log_degrade`。
- 工具方法：无。

## `services/python_grpc/src/common/utils/__init__.py`
- 职责与功能：负责common/utils下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/common/utils/numbers.py`
- 职责与功能：负责common/utils下相关功能的实现、组装与协作。
- 核心方法：`safe_int`、`safe_float`、`to_float`。
- 工具方法：无。

## `services/python_grpc/src/common/utils/path.py`
- 职责与功能：负责common/utils下相关功能的实现、组装与协作。
- 核心方法：`safe_filename`、`sanitize_filename_component`。
- 工具方法：无。

## `services/python_grpc/src/common/utils/time.py`
- 职责与功能：负责common/utils下相关功能的实现、组装与协作。
- 核心方法：`format_hhmmss`。
- 工具方法：无。

## `services/python_grpc/src/common/utils/video.py`
- 职责与功能：负责common/utils下相关功能的实现、组装与协作。
- 核心方法：`probe_video_duration_ffprobe`、`get_video_duration`。
- 工具方法：无。

## `services/python_grpc/src/config_paths.py`
- 职责与功能：负责根模块下相关功能的实现、组装与协作。
- 核心方法：`load_yaml_dict`、`resolve_video_config_path`、`resolve_module2_config_file`。
- 工具方法：`_append_unique`、`_walk_up`、`_collect_search_roots`、`_resolve_explicit_file`。

## `services/python_grpc/src/content_pipeline/__init__.py`
- 职责与功能：负责content_pipeline下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/common/__init__.py`
- 职责与功能：负责content_pipeline/common下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/common/utils/__init__.py`
- 职责与功能：负责content_pipeline/common/utils下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/common/utils/id_utils.py`
- 职责与功能：负责content_pipeline/common/utils下相关功能的实现、组装与协作。
- 核心方法：`build_unit_relative_asset_id`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/common/utils/path_utils.py`
- 职责与功能：负责content_pipeline/common/utils下相关功能的实现、组装与协作。
- 核心方法：`find_repo_root`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/__init__.py`
- 职责与功能：负责content_pipeline/infra下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/llm/__init__.py`
- 职责与功能：负责content_pipeline/infra/llm下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/llm/deepseek_audit.py`
- 职责与功能：负责content_pipeline/infra/llm下相关功能的实现、组装与协作。
- 核心方法：`build_phase2b_audit_context`、`push_deepseek_audit_context`、`pop_deepseek_audit_context`、`get_deepseek_audit_context`、`append_deepseek_call_record`。
- 工具方法：`_parse_bool`、`_now_iso`、`_is_img_desc_augment_call`、`_apply_text_limit`、`_metadata_to_dict`、`_initialize_audit_file`。

## `services/python_grpc/src/content_pipeline/infra/llm/llm_client.py`
- 职责与功能：负责content_pipeline/infra/llm下相关功能的实现、组装与协作。
- 核心方法：`_AsyncLRUTTLCache.now`、`_AsyncLRUTTLCache.ttl_seconds`、`_AsyncLRUTTLCache.get`、`_AsyncLRUTTLCache.set`、`_AsyncLRUTTLCache.stats`、`_AsyncInFlightDeduper.run`、`AdaptiveConcurrencyLimiter.acquire`、`AdaptiveConcurrencyLimiter.release`、`AdaptiveConcurrencyLimiter.record_success`、`AdaptiveConcurrencyLimiter.record_failure`、`AdaptiveConcurrencyLimiter.set_external_cap`、`AdaptiveConcurrencyLimiter.effective_limit`、`AdaptiveConcurrencyLimiter.stats`、`AdaptiveConnectionPoolManager.get_client`、`AdaptiveConnectionPoolManager.get_client_sync`、`get_pool_manager`、`get_concurrency_limiter`、`LLMClient.complete_json`、`LLMClient.complete_text`、`create_llm_client`。
- 工具方法：`_env_bool`、`_env_int`、`AdaptiveConcurrencyLimiter._recompute_effective_limit_locked`、`LLMClient._ensure_openai_client`、`LLMClient._refresh_client_if_needed`、`LLMClient._estimate_tokens`、`LLMClient._make_cache_key`、`LLMClient._cacheable`、`LLMClient._entry_to_metadata`、`LLMClient._compute_resource_cap`、`LLMClient._compute_permits`、`LLMClient._apply_resource_cap`。

## `services/python_grpc/src/content_pipeline/infra/llm/llm_gateway.py`
- 职责与功能：负责content_pipeline/infra/llm下相关功能的实现、组装与协作。
- 核心方法：`get_deepseek_client`、`deepseek_complete_text`、`deepseek_complete_json`、`vision_validate_image`、`vision_validate_image_sync`、`vl_chat_completion`。
- 工具方法：`_env_bool`、`_env_int`、`_env_float`、`_hash_text`、`_build_deepseek_client_key`、`_extract_usage_from_response`、`_call_vl_api_once`。

## `services/python_grpc/src/content_pipeline/infra/llm/prompt_loader.py`
- 职责与功能：负责content_pipeline/infra/llm下相关功能的实现、组装与协作。
- 核心方法：`clear_prompt_loader_cache`、`get_prompt`、`render_prompt`、`get_prompt_path`。
- 工具方法：`_detect_repo_root`、`_safe_bool`、`_resolve_file_path`、`_load_prompt_config`、`_resolve_prompt_root`、`_read_text_cached`、`_load_prompt_text`。

## `services/python_grpc/src/content_pipeline/infra/llm/prompt_registry.py`
- 职责与功能：负责content_pipeline/infra/llm下相关功能的实现、组装与协作。
- 核心方法：`get_prompt_entry`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/llm/vision_ai_client.py`
- 职责与功能：负责content_pipeline/infra/llm下相关功能的实现、组装与协作。
- 核心方法：`PerceptualHasher.compute_dhash`、`PerceptualHasher.compute_similarity`、`PerceptualHasher.compute_from_file`、`HashCacheManager.check_duplicate`、`HashCacheManager.store_result`、`HashCacheManager.load_results`、`HashCacheManager.export_results`、`HashCacheManager.get_stats`、`VisionAIConcurrencyLimiter.acquire`、`VisionAIConcurrencyLimiter.release`、`VisionAIConcurrencyLimiter.record_success`、`VisionAIConcurrencyLimiter.record_failure`、`VisionAIRateLimiter.acquire`、`VisionAIBackgroundLoop.get_loop`、`VisionAIBackgroundLoop.submit`、`VisionAIClient.validate_image`、`VisionAIClient.get_stats`、`VisionAIClient.validate_image_sync`、`VisionAIClient.close`、`get_vision_ai_client`。
- 工具方法：`VisionAIRateLimiter._ensure_loop`、`VisionAIBackgroundLoop._run`、`VisionAIClient._get_client`、`VisionAIClient._safe_close_client`、`VisionAIClient._call_vision_api`。

## `services/python_grpc/src/content_pipeline/infra/runtime/__init__.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/runtime/cache_metrics.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`enabled`、`reset_on_task_enabled`、`set_context`、`reset`、`hit`、`miss`、`snapshot`。
- 工具方法：`_env_truthy`、`_ensure`。

## `services/python_grpc/src/content_pipeline/infra/runtime/config_loader.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`ConfigLoader.load_dictionaries`、`load_module2_config`、`get_config_loader`。
- 工具方法：`ConfigLoader._deep_merge`。

## `services/python_grpc/src/content_pipeline/infra/runtime/cv_runtime_config.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：`_resolve_cv_float_dtype`。

## `services/python_grpc/src/content_pipeline/infra/runtime/dynamic_decision_engine.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`GlobalAnalysisCache.clear`、`DynamicDecisionEngine.preprocess_frames_adaptive`、`DynamicDecisionEngine.compute_base_features`、`DynamicDecisionEngine.detect_action_windows`、`DynamicDecisionEngine.judge_is_dynamic`、`DynamicDecisionEngine.calculate_ssim_feature`、`DynamicDecisionEngine.calculate_edge_flux`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/runtime/fast_metrics.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`fast_mse`、`fast_ssim`、`fast_diff_ratio`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/runtime/ocr_utils.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`OCRExtractor.extract_text_from_image`、`OCRExtractor.extract_text_from_frame`、`OCRExtractor.extract_text_regions_from_frame`、`OCRExtractor.extract_text_regions`、`OCRExtractor.calculate_text_match_rate`、`ThreadSafeMathOCR.recognize_math`。
- 工具方法：`OCRExtractor._setup_tesseract_path`、`OCRExtractor._check_tesseract`、`OCRExtractor._preprocess_image`、`OCRExtractor._clean_ocr_text`、`ThreadSafeMathOCR._init_engines`。

## `services/python_grpc/src/content_pipeline/infra/runtime/resource_manager.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`ResourceManager.get_io_executor`、`ResourceManager.get_video_capture`、`ResourceManager.get_video_lock`、`ResourceManager.get_video_info`、`ResourceManager.extract_frames`、`ResourceManager.shutdown`、`get_resource_manager`、`get_io_executor`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/runtime/resource_utils.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`ResourceOrchestrator.get_system_status`、`ResourceOrchestrator.get_adaptive_concurrency`、`ResourceOrchestrator.get_adaptive_cache_size`、`AdaptiveSemaphore.current_limit`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/runtime/vl_ffmpeg_utils.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`export_clip_asset_with_ffmpeg`、`export_keyframe_with_ffmpeg`、`concat_segments_with_ffmpeg`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/runtime/vl_interval_utils.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`normalize_intervals`、`subtract_intervals`、`build_removed_intervals_from_stable`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/infra/runtime/vl_prefetch_utils.py`
- 职责与功能：负责content_pipeline/infra/runtime下相关功能的实现、组装与协作。
- 核心方法：`resolve_max_workers`、`build_screenshot_prefetch_chunks`、`build_task_params_from_ts_map`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2a/__init__.py`
- 职责与功能：负责content_pipeline/phase2a下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2a/materials/__init__.py`
- 职责与功能：负责content_pipeline/phase2a/materials下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2a/materials/errors.py`
- 职责与功能：负责content_pipeline/phase2a/materials下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2a/materials/models.py`
- 职责与功能：负责content_pipeline/phase2a/materials下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2a/materials/video_clip_extractor.py`
- 职责与功能：负责content_pipeline/phase2a/materials下相关功能的实现、组装与协作。
- 核心方法：`VideoClipExtractor.set_subtitles`、`VideoClipExtractor.extract_video_clip`、`VideoClipExtractor.extract_clip`、`VideoClipExtractor.validate_animation`、`VideoClipExtractor.extract_result_screenshot`。
- 工具方法：`VideoClipExtractor._detect_best_physical_anchors`、`VideoClipExtractor._expand_logic_chain`、`VideoClipExtractor._get_dynamic_padding`、`VideoClipExtractor._check_scene_switch`、`VideoClipExtractor._get_subtitles_near`、`VideoClipExtractor._has_transition_at_boundary`、`VideoClipExtractor._refine_boundaries_semantically`、`VideoClipExtractor._get_complete_semantic_baseline`、`VideoClipExtractor._recalibrate_physical_anchor`、`VideoClipExtractor._check_boundary_overlap`、`VideoClipExtractor._judge_sentence_completeness_no_punc`、`VideoClipExtractor._add_speech_flow_padding`、`VideoClipExtractor._search_semantic_boundary`、`VideoClipExtractor._expand_logic_chain_v2`、`VideoClipExtractor._get_dynamic_padding_v2`、`VideoClipExtractor._classify_semantic_unit`、`VideoClipExtractor._is_next_topic`、`VideoClipExtractor._get_semantic_extractor`、`VideoClipExtractor._get_video_duration`、`VideoClipExtractor._export_clip_with_ffmpeg`、`VideoClipExtractor._export_poster_at_timestamp`、`VideoClipExtractor._cognitive_value_check`、`VideoClipExtractor._generate_transition_text`。

## `services/python_grpc/src/content_pipeline/phase2a/materials/vl_material_generator.py`
- 职责与功能：负责content_pipeline/phase2a/materials下相关功能的实现、组装与协作。
- 核心方法：`VLMaterialGenerator.analyzer`、`VLMaterialGenerator.is_enabled`、`VLMaterialGenerator.preprocess_process_units_for_routing`、`VLMaterialGenerator.generate`。
- 工具方法：`VLMaterialGenerator._get_cached_visual_extractor`、`VLMaterialGenerator._get_cache_path`、`VLMaterialGenerator._save_vl_results`、`VLMaterialGenerator._load_vl_results`、`VLMaterialGenerator._should_merge_multistep_unit`、`VLMaterialGenerator._collect_segments_from_clip`、`VLMaterialGenerator._merge_multistep_clip_requests`、`VLMaterialGenerator._is_tutorial_process_unit`、`VLMaterialGenerator._build_tutorial_extra_prompt`、`VLMaterialGenerator._slugify_action_brief`、`VLMaterialGenerator._build_tutorial_unit_dir`、`VLMaterialGenerator._export_clip_asset_with_ffmpeg`、`VLMaterialGenerator._export_keyframe_with_ffmpeg`、`VLMaterialGenerator._save_tutorial_assets_for_unit`、`VLMaterialGenerator._normalize_intervals`、`VLMaterialGenerator._subtract_intervals`、`VLMaterialGenerator._build_pruning_context_prompt`、`VLMaterialGenerator._build_removed_intervals_from_stable`、`VLMaterialGenerator._get_subtitle_repo_for_output_dir`、`VLMaterialGenerator._load_subtitles_for_output_dir`、`VLMaterialGenerator._build_unit_relative_subtitles`、`VLMaterialGenerator._split_complete_sentences_by_pause`、`VLMaterialGenerator._pick_sentence_for_anchor`、`VLMaterialGenerator._get_complete_semantic_baseline_for_segment`、`VLMaterialGenerator._detect_segment_mse_jump_end`、`VLMaterialGenerator._refine_kept_segments_before_concat`、`VLMaterialGenerator._detect_stable_islands_for_unit`、`VLMaterialGenerator._concat_segments_with_ffmpeg`、`VLMaterialGenerator._map_pruned_relative_to_original`、`VLMaterialGenerator._map_pruned_interval_to_original_segments`、`VLMaterialGenerator._prepare_pruned_clip_for_vl`、`VLMaterialGenerator._split_video_by_semantic_units`、`VLMaterialGenerator._find_clip_for_unit`、`VLMaterialGenerator._optimize_screenshot_timestamps`、`VLMaterialGenerator._optimize_screenshots_parallel`、`VLMaterialGenerator._optimize_screenshots_batch_mode`、`VLMaterialGenerator._is_truthy_env`、`VLMaterialGenerator._resolve_max_workers`、`VLMaterialGenerator._build_screenshot_prefetch_chunks`、`VLMaterialGenerator._prefetch_union_frames_to_registry_sync`、`VLMaterialGenerator._build_task_params_from_ts_map`、`VLMaterialGenerator._maybe_warmup_pool`、`VLMaterialGenerator._apply_selection_result`、`VLMaterialGenerator._optimize_screenshots_streaming_pipeline`、`VLMaterialGenerator._should_fallback`。

## `services/python_grpc/src/content_pipeline/phase2a/materials/vl_video_analyzer.py`
- 职责与功能：负责content_pipeline/phase2a/materials下相关功能的实现、组装与协作。
- 核心方法：`VLVideoAnalyzer.close`、`VLVideoAnalyzer.convert_timestamps`、`VLVideoAnalyzer.analyze_clip`。
- 工具方法：`VLVideoAnalyzer._normalize_analysis_mode`、`VLVideoAnalyzer._get_builtin_output_constraints_tutorial`、`VLVideoAnalyzer._get_builtin_output_constraints_default`、`VLVideoAnalyzer._get_output_constraints`、`VLVideoAnalyzer._load_prompt_template`、`VLVideoAnalyzer._get_default_prompt`、`VLVideoAnalyzer._extract_token_usage`、`VLVideoAnalyzer._build_vl_cache_key`、`VLVideoAnalyzer._call_vl_api`、`VLVideoAnalyzer._get_tutorial_system_prompt`、`VLVideoAnalyzer._build_messages`、`VLVideoAnalyzer._try_get_dashscope_temp_url`、`VLVideoAnalyzer._encode_video_base64`、`VLVideoAnalyzer._extract_keyframes`、`VLVideoAnalyzer._encode_image_as_jpeg_data_uri`、`VLVideoAnalyzer._sanitize_action_brief`、`VLVideoAnalyzer._build_unit_relative_asset_id`、`VLVideoAnalyzer._normalize_timestamp_list`、`VLVideoAnalyzer._enforce_tutorial_step_constraints`、`VLVideoAnalyzer._parse_response_with_payload`、`VLVideoAnalyzer._parse_response`、`VLVideoAnalyzer._extract_json_candidate`、`VLVideoAnalyzer._extract_balanced_json`、`VLVideoAnalyzer._repair_key_evidence_field`。

## `services/python_grpc/src/content_pipeline/phase2a/segmentation/__init__.py`
- 职责与功能：负责content_pipeline/phase2a/segmentation下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2a/segmentation/concrete_knowledge_validator.py`
- 职责与功能：负责content_pipeline/phase2a/segmentation下相关功能的实现、组装与协作。
- 核心方法：`ConcreteKnowledgeValidator.enabled`、`ConcreteKnowledgeValidator.validate`、`ConcreteKnowledgeValidator.validate_batch`、`ConcreteKnowledgeValidator.validate_for_coreference`。
- 工具方法：`ConcreteKnowledgeValidator._load_vision_config`、`ConcreteKnowledgeValidator._build_cache_signature`、`ConcreteKnowledgeValidator._load_persistent_cache`、`ConcreteKnowledgeValidator._save_persistent_cache`、`ConcreteKnowledgeValidator._finalize_validation_result`、`ConcreteKnowledgeValidator._cache_result`、`ConcreteKnowledgeValidator._vision_validate_v3`、`ConcreteKnowledgeValidator._detect_math_formula`、`ConcreteKnowledgeValidator._analyze_cv_features`、`ConcreteKnowledgeValidator._extract_graphic_region`、`ConcreteKnowledgeValidator._vision_validate`、`ConcreteKnowledgeValidator._cv_only_validate`、`ConcreteKnowledgeValidator._default_result`。

## `services/python_grpc/src/content_pipeline/phase2a/segmentation/knowledge_classifier.py`
- 职责与功能：负责content_pipeline/phase2a/segmentation下相关功能的实现、组装与协作。
- 核心方法：`KnowledgeClassifier.enabled`、`KnowledgeClassifier.classify_batch`、`KnowledgeClassifier.classify_units_batch`。
- 工具方法：`KnowledgeClassifier._resolve_config_path`、`KnowledgeClassifier._parse_batch_content`、`KnowledgeClassifier._extract_first_balanced_json`、`KnowledgeClassifier._extract_top_level_objects`、`KnowledgeClassifier._loads_jsonish`、`KnowledgeClassifier._jsonish_to_python_literal`、`KnowledgeClassifier._normalize_jsonish_text`、`KnowledgeClassifier._replace_outside_strings`、`KnowledgeClassifier._escape_control_chars_in_strings`、`KnowledgeClassifier._remove_trailing_commas`、`KnowledgeClassifier._normalize_batch_index`、`KnowledgeClassifier._load_all_subtitles`、`KnowledgeClassifier._get_subtitles_in_range`。

## `services/python_grpc/src/content_pipeline/phase2a/segmentation/semantic_unit_segmenter.py`
- 职责与功能：负责content_pipeline/phase2a/segmentation下相关功能的实现、组装与协作。
- 核心方法：`SemanticUnitSegmenter.segment`。
- 工具方法：`SemanticUnitSegmenter._ensure_llm_client`、`SemanticUnitSegmenter._save_to_cache`、`SemanticUnitSegmenter._load_from_cache`、`SemanticUnitSegmenter._calculate_timestamps`、`SemanticUnitSegmenter._collect_sentence_ids`、`SemanticUnitSegmenter._parse_min_schema_unit`、`SemanticUnitSegmenter._normalize_paragraph_ids`、`SemanticUnitSegmenter._collect_text_by_paragraph_ids`、`SemanticUnitSegmenter._decode_knowledge_type`、`SemanticUnitSegmenter._build_topic_from_text`、`SemanticUnitSegmenter._resolve_conflicts`、`SemanticUnitSegmenter._call_llm_for_decision`、`SemanticUnitSegmenter._execute_decision`、`SemanticUnitSegmenter._execute_split`、`SemanticUnitSegmenter._execute_adjust`、`SemanticUnitSegmenter._collect_text_by_ids`。

## `services/python_grpc/src/content_pipeline/phase2a/vision/__init__.py`
- 职责与功能：负责content_pipeline/phase2a/vision下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2a/vision/cv_knowledge_validator.py`
- 职责与功能：负责content_pipeline/phase2a/vision下相关功能的实现、组装与协作。
- 核心方法：`CVKnowledgeValidator.close`、`CVKnowledgeValidator.detect_visual_states`、`CVKnowledgeValidator.classify_visual_knowledge_type`、`CVKnowledgeValidator.validate_batch`、`CVKnowledgeValidator.validate_single`、`CVKnowledgeValidator.generate_conflict_packages`、`CVKnowledgeValidator.to_dict`。
- 工具方法：`CVKnowledgeValidator._init_video`、`CVKnowledgeValidator._resize_frame`、`CVKnowledgeValidator._quick_redundancy_check`、`CVKnowledgeValidator._compute_layout_feature`、`CVKnowledgeValidator._detect_roi`、`CVKnowledgeValidator._calculate_ssim_roi`、`CVKnowledgeValidator._calculate_diff_ratio_roi`、`CVKnowledgeValidator._sample_frames`、`CVKnowledgeValidator._light_stable_check`、`CVKnowledgeValidator._should_trigger_edge_detection`、`CVKnowledgeValidator._calculate_mse`、`CVKnowledgeValidator._refine_action_boundaries`、`CVKnowledgeValidator._has_internal_stable_islands`、`CVKnowledgeValidator._is_presentation_dynamic`、`CVKnowledgeValidator._calculate_spatial_spread`、`CVKnowledgeValidator._is_monotonic_smooth`、`CVKnowledgeValidator._calculate_frame_content_iou`、`CVKnowledgeValidator._has_creation_features`、`CVKnowledgeValidator._detect_local_pixel_increment`、`CVKnowledgeValidator._detect_trace_pattern`、`CVKnowledgeValidator._detect_boundary_expansion`、`CVKnowledgeValidator._classify_continuous_type`、`CVKnowledgeValidator._extract_key_screenshot_times`、`CVKnowledgeValidator._merge_state_intervals`、`CVKnowledgeValidator._merge_action_units_stage1`、`CVKnowledgeValidator._merge_action_units_stage2`、`CVKnowledgeValidator._get_stable_islands_in_range`、`CVKnowledgeValidator._collect_all_stable_islands`、`CVKnowledgeValidator._check_type_match`。

## `services/python_grpc/src/content_pipeline/phase2a/vision/cv_models.py`
- 职责与功能：负责content_pipeline/phase2a/vision下相关功能的实现、组装与协作。
- 核心方法：`StableIsland.duration_ms`、`ActionUnit.duration_ms`、`ActionUnit.classify`、`ActionUnit.classify_modality`、`RedundancySegment.duration_ms`、`CVValidationResult.is_normal`、`ROICache.get`、`ROICache.put`、`ROICache.get_last_layout_feature`、`FrameFeatureCache.get`、`FrameFeatureCache.put`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2a/vision/screenshot_range_calculator.py`
- 职责与功能：负责content_pipeline/phase2a/vision下相关功能的实现、组装与协作。
- 核心方法：`ScreenshotRange.mid_sec`、`ScreenshotRangeCalculator.calculate_ranges`、`ScreenshotRangeCalculator.get_screenshot_timestamps`。
- 工具方法：`ScreenshotRangeCalculator._resolve_overlaps`、`ScreenshotRangeCalculator._adjust_to_non_overlapping`。

## `services/python_grpc/src/content_pipeline/phase2a/vision/screenshot_selector.py`
- 职责与功能：负责content_pipeline/phase2a/vision下相关功能的实现、组装与协作。
- 核心方法：`ScreenshotSelector.create_lightweight`、`ScreenshotSelector.select_from_shared_frames`、`ScreenshotSelector.select_screenshots_for_range_sync`、`ScreenshotSelector.detect_stable_islands_from_frames`、`ScreenshotSelector.select_best_frame_from_frames`、`ScreenshotSelector.select_screenshot`、`ScreenshotSelector.filter_valid_islands`、`ScreenshotSelector.deduplicate_islands`。
- 工具方法：`_analyze_frame_quality_worker`、`ScreenshotSelector._ensure_detector`、`ScreenshotSelector._finalize_island_sync`、`ScreenshotSelector._filter_valid_islands_sync`、`ScreenshotSelector._deduplicate_islands_simple`、`ScreenshotSelector._select_intra_island_winner_sync`、`ScreenshotSelector._read_frames_at_timestamps_sequential`、`ScreenshotSelector._finalize_island`、`ScreenshotSelector._select_intra_island_winner`、`ScreenshotSelector._identify_action_type_v6`、`ScreenshotSelector._get_adaptive_threshold`、`ScreenshotSelector._calculate_S4_no_occlusion_v6`、`ScreenshotSelector._fallback_select`、`ScreenshotSelector._calculate_final_scores`、`ScreenshotSelector._export_debug_trace_tiered`、`ScreenshotSelector._get_video_fps`、`ScreenshotSelector._handle_empty_frames_complex`、`ScreenshotSelector._create_empty_selection`、`ScreenshotSelector._save_screenshot`。

## `services/python_grpc/src/content_pipeline/phase2a/vision/semantic_feature_extractor.py`
- 职责与功能：负责content_pipeline/phase2a/vision下相关功能的实现、组装与协作。
- 核心方法：`SemanticFeatureExtractor.model`、`SemanticFeatureExtractor.classify_semantic_role`、`SemanticFeatureExtractor.get_embedding`、`SemanticFeatureExtractor.batch_get_embeddings`、`SemanticFeatureExtractor.calculate_context_similarity`、`SemanticFeatureExtractor.calculate_domain_consistency`、`SemanticFeatureExtractor.detect_pattern`、`SemanticFeatureExtractor.classify_knowledge_type`、`SemanticFeatureExtractor.extract_semantic_features`。
- 工具方法：`SemanticFeatureExtractor._update_cache`、`SemanticFeatureExtractor._calculate_semantic_confidence`。

## `services/python_grpc/src/content_pipeline/phase2a/vision/visual_element_detection_helpers.py`
- 职责与功能：负责content_pipeline/phase2a/vision下相关功能的实现、组装与协作。
- 核心方法：`VisualElementDetector.detect_rectangles`、`VisualElementDetector.detect_circles`、`VisualElementDetector.detect_lines`、`VisualElementDetector.detect_lines_p`、`VisualElementDetector.calculate_gravity_center`、`VisualElementDetector.detect_arrows`、`VisualElementDetector.detect_connectors`、`VisualElementDetector.detect_diamonds`、`VisualElementDetector.detect_clouds`、`VisualElementDetector.detect_math_formula`、`VisualElementDetector.detect_matrix_brackets`、`VisualElementDetector.detect_sqrt`、`VisualElementDetector.detect_tables`、`VisualElementDetector.analyze_frame`、`VisualElementDetector.detect_structure_roi`、`VisualElementDetector.judge_structure_dynamic`。
- 工具方法：`_fast_cosine_angle`、`_fast_point_dist`、`VisualElementDetector._classify_arrow_direction`。

## `services/python_grpc/src/content_pipeline/phase2a/vision/visual_feature_extractor.py`
- 职责与功能：负责content_pipeline/phase2a/vision下相关功能的实现、组装与协作。
- 核心方法：`get_visual_process_pool`、`shutdown_visual_process_pool`、`SharedFrameRegistry.register_frame`、`SharedFrameRegistry.get_frame`、`SharedFrameRegistry.cleanup`、`SharedFrameRegistry.get_shm_ref`、`get_shared_frame_registry`、`VisualFeatureExtractor.extract_frames_async`、`VisualFeatureExtractor.extract_frames_fast`、`VisualFeatureExtractor.extract_frames`、`VisualFeatureExtractor.calculate_mse_diff`、`VisualFeatureExtractor.calculate_ssim`、`VisualFeatureExtractor.calculate_content_increment`、`VisualFeatureExtractor.enhance_low_quality_frame`、`VisualFeatureExtractor.match_handwritten_feature`、`VisualFeatureExtractor.validate_visual_feature_semantic`、`VisualFeatureExtractor.calculate_clip_score`、`VisualFeatureExtractor.calculate_all_diffs`、`VisualFeatureExtractor.calculate_ssim_sequence`、`VisualFeatureExtractor.calculate_edge_flux_sequence`、`VisualFeatureExtractor.classify_static_dynamic`、`VisualFeatureExtractor.detect_visual_elements`、`VisualFeatureExtractor.get_cached_content`、`VisualFeatureExtractor.extract_visual_features`、`VisualFeatureExtractor.judge_visual_voice_timing`、`VisualFeatureExtractor.extract_action_start_time`、`VisualFeatureExtractor.extract_action_end_time`、`VisualFeatureExtractor.limit_forward_extension`。
- 工具方法：`_get_clip_model`、`VisualFeatureExtractor._get_ffmpeg_hwaccel_args`、`VisualFeatureExtractor._get_resolution_factor`、`VisualFeatureExtractor._probe_one_frame`、`VisualFeatureExtractor._calculate_visual_confidence`。

## `services/python_grpc/src/content_pipeline/phase2b/__init__.py`
- 职责与功能：负责content_pipeline/phase2b下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2b/assembly/__init__.py`
- 职责与功能：负责content_pipeline/phase2b/assembly下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2b/assembly/pipeline_asset_utils.py`
- 职责与功能：负责content_pipeline/phase2b/assembly下相关功能的实现、组装与协作。
- 核心方法：`slugify_text`、`build_unit_asset_prefix`、`build_action_brief`、`build_request_base_name`、`build_unit_relative_request_id`、`resolve_asset_output_path`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2b/assembly/pipeline_material_request_utils.py`
- 职责与功能：负责content_pipeline/phase2b/assembly下相关功能的实现、组装与协作。
- 核心方法：`create_screenshot_request`、`create_clip_request`、`clamp_clip_segments`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2b/assembly/pipeline_timeline_utils.py`
- 职责与功能：负责content_pipeline/phase2b/assembly下相关功能的实现、组装与协作。
- 核心方法：`align_to_sentence_start`、`align_to_sentence_end`、`clamp_time_range`、`merge_action_segments`、`compute_action_envelope`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2b/assembly/request_models.py`
- 职责与功能：负责content_pipeline/phase2b/assembly下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/phase2b/assembly/rich_text_document.py`
- 职责与功能：负责content_pipeline/phase2b/assembly下相关功能的实现、组装与协作。
- 核心方法：`RichTextSection.duration_str`、`RichTextDocument.add_section`、`RichTextDocument.to_dict`、`RichTextDocument.to_json`、`RichTextDocument.to_markdown`、`create_section_from_semantic_unit`。
- 工具方法：`RichTextDocument._render_section_markdown`、`RichTextDocument._relative_path`。

## `services/python_grpc/src/content_pipeline/phase2b/assembly/rich_text_pipeline.py`
- 职责与功能：负责content_pipeline/phase2b/assembly下相关功能的实现、组装与协作。
- 核心方法：`RichTextPipeline.set_visual_extractor`、`RichTextPipeline.set_clip_extractor`。
- 工具方法：`RichTextPipeline._resolve_config_path`、`RichTextPipeline._parse_bool`、`RichTextPipeline._load_image_match_audit_switch`、`RichTextPipeline._record_image_match_audit`、`RichTextPipeline._flush_image_match_audit`、`RichTextPipeline._resolve_intermediate_path`、`RichTextPipeline._slugify_text`、`RichTextPipeline._build_unit_asset_prefix`、`RichTextPipeline._build_action_brief`、`RichTextPipeline._build_request_base_name`、`RichTextPipeline._build_unit_relative_request_id`、`RichTextPipeline._resolve_asset_output_path`、`RichTextPipeline._align_to_sentence_start`、`RichTextPipeline._align_to_sentence_end`、`RichTextPipeline._clamp_time_range`、`RichTextPipeline._merge_action_segments`、`RichTextPipeline._compute_action_envelope`、`RichTextPipeline._load_paragraphs`、`RichTextPipeline._assemble_document`、`RichTextPipeline._save_semantic_units`、`RichTextPipeline._load_semantic_units`、`RichTextPipeline._merge_cv_results`、`RichTextPipeline._classify_and_filter_actions`、`RichTextPipeline._merge_actions_local`、`RichTextPipeline._merge_actions_local_stage2`、`RichTextPipeline._collect_all_stable_islands_local`、`RichTextPipeline._build_sentence_timestamps`、`RichTextPipeline._map_timestamp_to_sentence_id`、`RichTextPipeline._get_sentence_text_by_id`、`RichTextPipeline._apply_modality_classification`、`RichTextPipeline._save_modality_cache`、`RichTextPipeline._load_modality_cache`、`RichTextPipeline._generate_materials_parallel`、`RichTextPipeline._preclassify_action_segments_multi_unit`、`RichTextPipeline._generate_materials`、`RichTextPipeline._collect_material_requests`、`RichTextPipeline._apply_external_materials`、`RichTextPipeline._select_screenshot`、`RichTextPipeline._select_screenshot_timestamp`、`RichTextPipeline._extract_frame_ffmpeg_fallback`、`RichTextPipeline._extract_action_clip`、`RichTextPipeline._extract_action_clip_ffmpeg`、`RichTextPipeline._extract_video_clip`、`RichTextPipeline._extract_clip_ffmpeg_fallback`。

## `services/python_grpc/src/content_pipeline/shared/__init__.py`
- 职责与功能：负责content_pipeline/shared下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/shared/models/__init__.py`
- 职责与功能：负责content_pipeline/shared/models下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/shared/models/data_structures.py`
- 职责与功能：负责content_pipeline/shared/models下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/shared/subtitle/__init__.py`
- 职责与功能：负责content_pipeline/shared/subtitle下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/shared/subtitle/data_loader.py`
- 职责与功能：负责content_pipeline/shared/subtitle下相关功能的实现、组装与协作。
- 核心方法：`load_corrected_subtitles`、`load_merged_segments`、`sanitize_module_input`、`load_sentence_timestamps`、`create_module2_input`、`validate_input_consistency`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/shared/subtitle/subtitle_repository.py`
- 职责与功能：负责content_pipeline/shared/subtitle下相关功能的实现、组装与协作。
- 核心方法：`SubtitleRepository.from_output_dir`、`SubtitleRepository.resolve_intermediate_path`、`SubtitleRepository.set_paths`、`SubtitleRepository.clear_cache`、`SubtitleRepository.set_raw_subtitles`、`SubtitleRepository.load_step2_subtitles`、`SubtitleRepository.list_subtitles`、`SubtitleRepository.load_step6_paragraphs`、`SubtitleRepository.build_sentence_timestamps`、`SubtitleRepository.align_to_sentence_start`、`SubtitleRepository.align_to_sentence_end`、`SubtitleRepository.clamp_time_range`、`SubtitleRepository.extract_subtitles_in_range`、`SubtitleRepository.get_subtitles_in_range`、`SubtitleRepository.map_timestamp_to_sentence_id`、`SubtitleRepository.get_sentence_text`、`SubtitleRepository.build_relative_subtitles`。
- 工具方法：`SubtitleRepository._load_step6_paragraphs_fallback`、`SubtitleRepository._normalize_sentence_timestamps`、`SubtitleRepository._normalize_range`、`SubtitleRepository._expand_to_sentence_boundary`。

## `services/python_grpc/src/content_pipeline/shared/subtitle/subtitle_utils.py`
- 职责与功能：负责content_pipeline/shared/subtitle下相关功能的实现、组装与协作。
- 核心方法：`extract_subtitle_text_in_range`、`calculate_subtitle_similarity`、`jaccard_similarity`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/tests/__init__.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/tests/test_concrete_knowledge_validator_cleanup.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：`test_finalize_validation_result_deletes_non_concrete_screenshot`、`test_finalize_validation_result_keeps_formula_screenshot`、`test_finalize_validation_result_writes_cache_when_enabled`。
- 工具方法：`_build_result`。

## `services/python_grpc/src/content_pipeline/tests/test_deepseek_audit.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：`_FakeGatewayClient.complete_text`、`test_append_deepseek_call_record_writes_input_output_pair`、`test_append_deepseek_call_record_skips_non_img_desc_when_filter_enabled`、`test_deepseek_gateway_call_is_audited`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/tests/test_knowledge_classifier_config_path.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：`test_resolve_config_path_prefers_unified_config_dir`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/tests/test_knowledge_classifier_parse.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：`test_parse_strict_json_array`、`test_parse_code_fence_json`、`test_parse_trailing_text`、`test_parse_trailing_commas`、`test_parse_single_quotes_pythonish`、`test_parse_chinese_punctuation_outside_strings`、`test_parse_unescaped_newline_in_string`、`test_parse_truncated_array_salvage_objects`、`test_parse_items_wrapper`。
- 工具方法：`_load_knowledge_classifier_module`、`_parser`。

## `services/python_grpc/src/content_pipeline/tests/test_prompt_loader.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：`test_get_prompt_uses_package_default`、`test_get_prompt_uses_root_dir_override`、`test_get_prompt_uses_key_override`、`test_get_prompt_returns_fallback_when_file_missing`、`test_get_prompt_strict_mode_raises_when_missing`、`test_render_prompt_missing_variable_raises`。
- 工具方法：`_reset_loader_cache`。

## `services/python_grpc/src/content_pipeline/tests/test_subtitle_repository.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：`test_repository_discovers_intermediate_files_and_loads_step2_step6`、`test_map_timestamp_to_sentence_id_prefers_in_range_then_nearest`、`test_get_subtitles_in_range_with_boundary_expansion`、`test_sentence_text_supports_index_and_subtitle_id`、`test_set_raw_subtitles_supports_in_memory_range_queries`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/tests/test_vl_material_prefetch.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：`test_chunking_groups_requests_by_span`、`test_task_params_filter_ts_map`。
- 工具方法：无。

## `services/python_grpc/src/content_pipeline/tests/test_vl_pre_prune.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：`test_subtract_intervals_keeps_edges_for_stable_core_cut`、`test_map_pruned_relative_time_back_to_original_axis`、`test_build_pruning_context_prompt_contains_topic_and_text`、`test_token_saving_estimation_linear_seconds`、`test_removed_intervals_require_stable_longer_than_3s`、`test_map_pruned_interval_to_original_segments_cross_gap`、`test_find_clip_for_unit_avoids_substring_collision`、`test_split_complete_sentences_by_pause_threshold`、`test_refine_kept_segments_before_concat_applies_semantic_physical_and_buffers`。
- 工具方法：`_build_generator`。

## `services/python_grpc/src/content_pipeline/tests/test_vl_tutorial_flow.py`
- 职责与功能：负责content_pipeline/tests下相关功能的实现、组装与协作。
- 核心方法：`test_tutorial_schema_parse_and_normalize`、`test_analyze_clip_uses_unit_relative_ids_for_default_mode`、`test_analyze_clip_uses_unit_relative_ids_for_tutorial_mode`、`_FakeAnalyzer.analyze_clip`、`test_generate_tutorial_assets_per_unit_full_flow_before_phase2b`。
- 工具方法：`_build_analyzer_config`、`_build_generator_config`。

## `services/python_grpc/src/media_engine/knowledge_engine/__init__.py`
- 职责与功能：负责media_engine/knowledge_engine下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/media_engine/knowledge_engine/core/__init__.py`
- 职责与功能：负责media_engine/knowledge_engine/core下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/media_engine/knowledge_engine/core/alignment.py`
- 职责与功能：负责media_engine/knowledge_engine/core下相关功能的实现、组装与协作。
- 核心方法：`LightweightVAD.read_wav`、`LightweightVAD.detect_speech`、`DTWCalibrator.text_similarity`、`DTWCalibrator.compute_dtw_matrix`、`DTWCalibrator.find_optimal_path`、`DTWCalibrator.calibrate_timestamps`。
- 工具方法：无。

## `services/python_grpc/src/media_engine/knowledge_engine/core/logging_config.py`
- 职责与功能：负责media_engine/knowledge_engine/core下相关功能的实现、组装与协作。
- 核心方法：`JSONFormatter.format`、`ColoredFormatter.format`、`PipelineLogger.debug`、`PipelineLogger.info`、`PipelineLogger.warning`、`PipelineLogger.degrade`、`PipelineLogger.error`、`PipelineLogger.critical`、`PipelineLogger.stage_start`、`PipelineLogger.stage_progress`、`PipelineLogger.stage_complete`、`PipelineLogger.stage_error`、`get_logger`、`setup_logging`。
- 工具方法：`PipelineLogger._log`。

## `services/python_grpc/src/media_engine/knowledge_engine/core/model_downloader.py`
- 职责与功能：负责media_engine/knowledge_engine/core下相关功能的实现、组装与协作。
- 核心方法：`set_hf_env`、`download_whisper_model`。
- 工具方法：`_verify_file_integrity`。

## `services/python_grpc/src/media_engine/knowledge_engine/core/parallel_transcription.py`
- 职责与功能：负责media_engine/knowledge_engine/core下相关功能的实现、组装与协作。
- 核心方法：`get_video_duration`、`split_video_segments`、`build_parallel_plan`、`transcribe_segment`、`transcribe_parallel`、`format_subtitles`、`format_hhmmss`。
- 工具方法：无。

## `services/python_grpc/src/media_engine/knowledge_engine/core/processing.py`
- 职责与功能：负责media_engine/knowledge_engine/core下相关功能的实现、组装与协作。
- 核心方法：`BaseProcessor.emit_progress`。
- 工具方法：无。

## `services/python_grpc/src/media_engine/knowledge_engine/core/transcription.py`
- 职责与功能：负责media_engine/knowledge_engine/core下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：`Transcriber._load_model`。

## `services/python_grpc/src/media_engine/knowledge_engine/core/utils.py`
- 职责与功能：负责media_engine/knowledge_engine/core下相关功能的实现、组装与协作。
- 核心方法：`get_config_value`、`format_duration`。
- 工具方法：无。

## `services/python_grpc/src/media_engine/knowledge_engine/core/video.py`
- 职责与功能：负责media_engine/knowledge_engine/core下相关功能的实现、组装与协作。
- 核心方法：`VideoProcessor.download`、`VideoProcessor.detect_playlist`、`VideoProcessor.get_playlist_info`、`VideoProcessor.parse_episode_range`、`VideoProcessor.download_playlist`。
- 工具方法：`VideoProcessor._progress_hook`。

## `services/python_grpc/src/server/__init__.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/server/dependency_check.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：`run_dependency_check`、`run_dependency_preflight`。
- 工具方法：`_prepare_preflight_paths`。

## `services/python_grpc/src/server/dependency_preflight.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/server/entrypoint.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：`main`。
- 工具方法：无。

## `services/python_grpc/src/server/import_path_setup.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：`setup_import_paths`、`prepare_runtime_paths`。
- 工具方法：无。

## `services/python_grpc/src/server/main.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/server/path_bootstrap.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/server/runtime_bootstrap.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/server/runtime_env.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：`configure_opencv_env`、`reconfigure_stdio_errors`、`safe_print`、`is_truthy_env`、`prepend_sys_path`、`log_boot_step`、`boot`。
- 工具方法：无。

## `services/python_grpc/src/server/runtime_support.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/server/server_entry.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/server/service.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：`serve`。
- 工具方法：`_impl_module`、`__getattr__`、`__dir__`。

## `services/python_grpc/src/server/startup_flags.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：`build_startup_arg_parser`、`parse_startup_flags`。
- 工具方法：无。

## `services/python_grpc/src/server/startup_runner.py`
- 职责与功能：负责server下相关功能的实现、组装与协作。
- 核心方法：`serve`、`configure_logging`、`run_server`。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/__init__.py`
- 职责与功能：负责transcript_pipeline下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/checkpoint.py`
- 职责与功能：负责transcript_pipeline下相关功能的实现、组装与协作。
- 核心方法：`SQLiteCheckpointer.start_run`、`SQLiteCheckpointer.get_run_info`、`SQLiteCheckpointer.update_run_status`、`SQLiteCheckpointer.save_checkpoint`、`SQLiteCheckpointer.load_checkpoint`、`SQLiteCheckpointer.get_last_completed_step`、`SQLiteCheckpointer.list_checkpoints`、`SQLiteCheckpointer.cleanup_old_runs`、`SQLiteCheckpointer.delete_run`、`generate_thread_id`。
- 工具方法：`SQLiteCheckpointer._init_db`、`SQLiteCheckpointer._get_conn`。

## `services/python_grpc/src/transcript_pipeline/graph.py`
- 职责与功能：负责transcript_pipeline下相关功能的实现、组装与协作。
- 核心方法：`StepOutputConfig.should_output`、`StepOutputConfig.save_step_output`、`create_checkpointed_node`、`create_pipeline_graph`、`run_pipeline`、`run_pipeline_sync`、`get_graph_mermaid`。
- 工具方法：`StepOutputConfig._extract_step_output`、`_execute_pipeline`。

## `services/python_grpc/src/transcript_pipeline/llm/__init__.py`
- 职责与功能：负责transcript_pipeline/llm下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/llm/client.py`
- 职责与功能：负责transcript_pipeline/llm下相关功能的实现、组装与协作。
- 核心方法：`LLMClient.last_prompt`、`LLMClient.last_response`、`LLMClient.last_token_count`、`LLMClient.complete`、`LLMClient.complete_json`、`LLMClient.complete_with_retry`、`load_config`、`create_llm_client`、`create_vision_client`。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/llm/deepseek.py`
- 职责与功能：负责transcript_pipeline/llm下相关功能的实现、组装与协作。
- 核心方法：`DeepSeekClient.close`、`DeepSeekClient.complete`、`DeepSeekClient.complete_json`、`DeepSeekClient.complete_batch`。
- 工具方法：`DeepSeekClient._get_client`。

## `services/python_grpc/src/transcript_pipeline/llm/vision.py`
- 职责与功能：负责transcript_pipeline/llm下相关功能的实现、组装与协作。
- 核心方法：`ERNIEVisionClient.close`、`ERNIEVisionClient.complete`、`ERNIEVisionClient.complete_with_image`、`ERNIEVisionClient.complete_with_images`、`ERNIEVisionClient.complete_json`、`ERNIEVisionClient.validate_frame`。
- 工具方法：`ERNIEVisionClient._get_client`、`ERNIEVisionClient._encode_image`、`ERNIEVisionClient._repair_json`。

## `services/python_grpc/src/transcript_pipeline/monitoring/__init__.py`
- 职责与功能：负责transcript_pipeline/monitoring下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/monitoring/logger.py`
- 职责与功能：负责transcript_pipeline/monitoring下相关功能的实现、组装与协作。
- 核心方法：`JSONFormatter.format`、`DetailedFormatter.format`、`StepLogger.start`、`StepLogger.end`、`StepLogger.log_input`、`StepLogger.log_output`、`StepLogger.log_llm_call`、`StepLogger.log_tool_call`、`StepLogger.log_progress`、`StepLogger.log_batch_summary`、`StepLogger.log_substep`、`StepLogger.log_degrade`、`StepLogger.log_warning`、`StepLogger.log_error`、`StepLogger.debug`、`StepLogger.info`、`setup_logging`、`get_logger`。
- 工具方法：`StepLogger._setup_logger`、`StepLogger._create_summary`。

## `services/python_grpc/src/transcript_pipeline/monitoring/metrics.py`
- 职责与功能：负责transcript_pipeline/monitoring下相关功能的实现、组装与协作。
- 核心方法：`TokenUsage.total_tokens`、`TokenUsage.add`、`MetricsCollector.record_step_execution`、`MetricsCollector.record_llm_usage`、`MetricsCollector.get_summary`、`MetricsCollector.save`、`MetricsCollector.print_summary`。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/monitoring/tracer.py`
- 职责与功能：负责transcript_pipeline/monitoring下相关功能的实现、组装与协作。
- 核心方法：`TraceEvent.to_dict`、`PipelineTracer.trace_step_start`、`PipelineTracer.trace_step_end`、`PipelineTracer.trace_step_error`、`PipelineTracer.trace_llm_call`、`PipelineTracer.trace_tool_call`、`PipelineTracer.checkpoint`、`PipelineTracer.get_timeline`、`PipelineTracer.get_metrics_summary`、`PipelineTracer.export_json`、`PipelineTracer.export_mermaid`、`PipelineTracer.save`。
- 工具方法：`PipelineTracer._generate_event_id`、`PipelineTracer._summarize`。

## `services/python_grpc/src/transcript_pipeline/nodes/__init__.py`
- 职责与功能：负责transcript_pipeline/nodes下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/nodes/phase1_preparation.py`
- 职责与功能：负责transcript_pipeline/nodes下相关功能的实现、组装与协作。
- 核心方法：`step1_node`。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/nodes/phase2_preprocessing.py`
- 职责与功能：负责transcript_pipeline/nodes下相关功能的实现、组装与协作。
- 核心方法：`step2_node`、`step3_node`、`step4_node`、`step5_node`、`step6_node`。
- 工具方法：`_summarize_error`、`_deduplicate_paragraphs`。

## `services/python_grpc/src/transcript_pipeline/state.py`
- 职责与功能：负责transcript_pipeline下相关功能的实现、组装与协作。
- 核心方法：`merge_lists`、`merge_dicts`、`create_initial_state`。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/tools/__init__.py`
- 职责与功能：负责transcript_pipeline/tools下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/tools/debug_visualizer.py`
- 职责与功能：负责transcript_pipeline/tools下相关功能的实现、组装与协作。
- 核心方法：`DebugVisualizer.draw_peak_strip`、`DebugVisualizer.draw_verification_overlay`。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/tools/file_validator.py`
- 职责与功能：负责transcript_pipeline/tools下相关功能的实现、组装与协作。
- 核心方法：`validate_video`、`validate_subtitle`、`read_subtitle_sample`、`extract_video_title`。
- 工具方法：`_parse_srt`、`_parse_vtt`、`_parse_txt`、`_hms_to_sec`、`_parse_json`、`_time_to_sec`、`_time_to_sec_vtt`。

## `services/python_grpc/src/transcript_pipeline/tools/frame_analyzer.py`
- 职责与功能：负责transcript_pipeline/tools下相关功能的实现、组装与协作。
- 核心方法：`FrameBoundaryAnalyzer.analyze_boundary`、`FrameBoundaryAnalyzer.cleanup`。
- 工具方法：`FrameBoundaryAnalyzer._extract_frames`、`FrameBoundaryAnalyzer._load_frames`、`FrameBoundaryAnalyzer._detect_start_candidates`、`FrameBoundaryAnalyzer._detect_end_candidates`、`FrameBoundaryAnalyzer._compare_frames`、`FrameBoundaryAnalyzer._simple_ssim`。

## `services/python_grpc/src/transcript_pipeline/tools/opencv_capture.py`
- 职责与功能：负责transcript_pipeline/tools下相关功能的实现、组装与协作。
- 核心方法：`FrameCapture.open`、`FrameCapture.close`、`FrameCapture.duration`、`FrameCapture.fps`、`FrameCapture.capture_frame`、`FrameCapture.capture_best_frame`、`FrameCapture.capture_multiple`、`FrameCapture.validate_frame`、`FrameCapture.extract_text_region`、`SemanticPeakDetector.calculate_frame_metrics`、`SemanticPeakDetector.detect_peak`、`calculate_capture_times`。
- 工具方法：`FrameCapture._seek_to_time`、`FrameCapture._apply_enhancement`、`FrameCapture._calculate_quality`。

## `services/python_grpc/src/transcript_pipeline/tools/storage.py`
- 职责与功能：负责transcript_pipeline/tools下相关功能的实现、组装与协作。
- 核心方法：`LocalStorage.save_subtitle_timestamps`、`LocalStorage.load_subtitle_timestamps`、`LocalStorage.get_subtitle_timestamp`、`LocalStorage.find_subtitle_by_text`、`LocalStorage.save_sentence_timestamps`、`LocalStorage.load_sentence_timestamps`、`LocalStorage.get_sentence_timestamp`、`LocalStorage.get_sentence_time_range`、`LocalStorage.save_segment_timestamps`、`LocalStorage.load_segment_timestamps`、`LocalStorage.get_segment_timestamp`、`LocalStorage.save_kp_timestamps`、`LocalStorage.load_kp_timestamps`、`LocalStorage.get_kp_timestamp`、`LocalStorage.get_temp_frame_path`、`LocalStorage.list_temp_frames`、`LocalStorage.cleanup_temp_frames`、`LocalStorage.save_checkpoint`、`LocalStorage.load_latest_checkpoint`、`LocalStorage.clear_all`、`LocalStorage.get_storage_info`。
- 工具方法：无。

## `services/python_grpc/src/transcript_pipeline/tools/video_utils.py`
- 职责与功能：负责transcript_pipeline/tools下相关功能的实现、组装与协作。
- 核心方法：`cut_video_segment`、`validate_video_file`、`get_video_duration`。
- 工具方法：无。

## `services/python_grpc/src/vision_validation/worker.py`
- 职责与功能：负责vision_validation下相关功能的实现、组装与协作。
- 核心方法：`init_cv_worker`、`get_frame_from_shm`、`run_cv_validation_task`、`cleanup_worker_resources`、`run_screenshot_selection_task`、`run_select_screenshots_for_range_task`、`warmup_worker`、`run_coarse_fine_screenshot_task`。
- 工具方法：`_is_truthy_env`、`_check_memory_usage`。

## `services/python_grpc/src/worker/__init__.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/worker/entrypoint.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：`build_arg_parser`、`main`。
- 工具方法：无。

## `services/python_grpc/src/worker/manager.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/worker/orchestrator.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：`resolve_worker_count`、`WorkerOrchestrator.start`。
- 工具方法：`WorkerOrchestrator._spawn_workers`、`WorkerOrchestrator._join_workers`、`WorkerOrchestrator._register_signals`、`WorkerOrchestrator._handle_signal`、`WorkerOrchestrator._terminate_workers`。

## `services/python_grpc/src/worker/process_worker_orchestrator.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/worker/process_worker_runner.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/worker/runtime.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：`run_worker_process`。
- 工具方法：无。

## `services/python_grpc/src/worker/worker_entry.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/worker/worker_process_orchestrator.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## `services/python_grpc/src/worker/worker_process_runtime.py`
- 职责与功能：负责worker下相关功能的实现、组装与协作。
- 核心方法：无（或仅含常量、数据结构定义）。
- 工具方法：无。

## 语法异常模块（需先修复文件再解析）
- `services/python_grpc/src/content_pipeline/phase2b/assembly/material_flow.py`：SyntaxError: '(' was never closed (<unknown>, line 526)
- `services/python_grpc/src/content_pipeline/tests/test_coreference_resolver.py`：SyntaxError: unterminated string literal (detected at line 23) (<unknown>, line 23)
- `services/python_grpc/src/content_pipeline/tests/test_data_loader.py`：IndentationError: unexpected indent (<unknown>, line 79)
- `services/python_grpc/src/content_pipeline/tests/test_markdown_enhancer_rich_text.py`：SyntaxError: unterminated string literal (detected at line 29) (<unknown>, line 29)
- `services/python_grpc/src/content_pipeline/tests/test_rich_text_pipeline_asset_naming.py`：SyntaxError: unterminated string literal (detected at line 600) (<unknown>, line 600)
- `services/python_grpc/src/content_pipeline/tests/test_vl_analyzer.py`：SyntaxError: unterminated string literal (detected at line 73) (<unknown>, line 73)
