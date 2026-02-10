

[Hard Constraints - Tutorial Stepwise Mode]
1) Output exactly one valid JSON array. No markdown, no prefix/suffix text, no explanations.
2) Each array item must be one complete step.
3) Required fields per item: step_id (Integer), step_description (String), clip_start_sec (Float), clip_end_sec (Float), instructional_keyframe_timestamp (List[Float]).
4) Do not output reasoning, key_evidence, or knowledge_type fields.
5) Segmentation rules:
   - Keep explanation + execution + result of the same step together.
   - Remove thinking/hesitation time (mouse wandering, idle pause, no new information).
   - No step shorter than 5 seconds. Merge short steps with adjacent steps.
6) instructional_keyframe_timestamp must be true instructional keyframes, prefer final state or just-before-submit moments.
7) Avoid -1 for timestamps; if action spans whole clip use [0.0, clip_duration].
