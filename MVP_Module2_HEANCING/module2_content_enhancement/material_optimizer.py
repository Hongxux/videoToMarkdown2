"""
Global Material Optimizer - Post-processing layer for Module 2
Handles redundancy elimination and selection of 'Information Supersets'.
"""

import logging
import cv2
import numpy as np
import asyncio
from typing import List, Dict, Optional, Set, Tuple
from pathlib import Path
from .data_structures import Enhancement, EnhancementType

logger = logging.getLogger(__name__)

class GlobalMaterialOptimizer:
    """
    全局材料优化器 (基于第一性原理的视觉容器锚定版)
    
    核心思想: 
    1. 视觉属于 PPT 页 (Container)
    2. 同一 Container 内的多次截图仅保留信息量最大的一帧 (Superset)
    3. 语义与时间作为校准，防止模板重合导致的误杀
    """
    
    def __init__(self, config: Dict = None, semantic_extractor=None):
        self.config = config or {}
        # 视觉聚类阈值
        self.DHASH_THRESHOLD = self.config.get("optimizer", {}).get("dhash_threshold", 2)
        self.HIST_SIM_THRESHOLD = self.config.get("optimizer", {}).get("hist_sim_threshold", 0.98)
        # 校准阈值
        self.TIME_GAP_THRESHOLD = self.config.get("optimizer", {}).get("intra_page_time_gap", 30.0)
        self.SEMANTIC_THRESHOLD = self.config.get("optimizer", {}).get("semantic_sim_threshold", 0.5)
        
        self.semantic_extractor = semantic_extractor
        
    async def optimize_enhancements(self, enhancements: List[Enhancement]) -> List[Enhancement]:
        """
        全量优化增强结果: 视觉场景聚类 -> 时间语义校准 -> 信息超集判定
        """
        if not enhancements: return []
        
        # 1. 提取所有带媒体的截图项
        screenshot_items = [e for e in enhancements if e.enhancement_type == EnhancementType.SCREENSHOT and e.media_paths]
        if not screenshot_items:
            logger.info("No screenshots found for optimization. Skipping.")
            return enhancements
        
        logger.info(f"Starting refinement for {len(screenshot_items)} materials...")
        
        # 按照时间排序
        screenshot_items.sort(key=lambda x: x.timestamp_start)
        
        # 2. 【第一层: 视觉锚定】 执行聚类 (Grouping by Physical Slide)
        clusters = self._cluster_by_visual_invariants(screenshot_items)
        logger.info(f"Visual Anchoring complete: Identified {len(clusters)} unique visual containers.")
        
        # 3. 【第二层: 时间语义校准】 拆分跨章节的误聚类
        calibrated_clusters = await self._calibrate_clusters(clusters)
        logger.info(f"Calibration complete: Refined into {len(calibrated_clusters)} semantic chapters.")
        
        redundant_ids: Set[str] = set()
        
        # 4. 【第三层: 信息超集判定】 在组内挑选胜者
        for idx, cluster in enumerate(calibrated_clusters):
            if len(cluster) <= 1: continue
            
            winner = self._pick_cluster_winner(cluster)
            
            cluster_redundants = []
            for e in cluster:
                if e.enhancement_id != winner.enhancement_id:
                    redundant_ids.add(e.enhancement_id)
                    cluster_redundants.append(e.enhancement_id)
            
            if cluster_redundants:
                logger.info(f"Cluster {idx} Pruning: Kept {winner.enhancement_id}, Hid {len(cluster_redundants)} subsets.")
        
        # 5. 构建结果: 仅置空冗余媒体，保留逻辑结构 (记录去重原因)
        for e in enhancements:
            if e.enhancement_id in redundant_ids:
                # 寻找该组的胜者 ID (用于诊断)
                winner_id = "unknown"
                for cluster in calibrated_clusters:
                    ids = [item.enhancement_id for item in cluster]
                    if e.enhancement_id in ids:
                        # 找到包含该项的簇，第一个通常是逻辑上的胜者(在本代码逻辑中winner标记在e.enhancement_id之外处理)
                        # 为了准确，我们在pick_cluster_winner时已经确定了winner
                        pass
                
                e.media_paths = []
                e.material_error = "Deduplicated: Optimized away as a visual subset (Redundancy Elimination)"
                
        logger.info(f"Final Optimization Result: {len(redundant_ids)} materials pruned across {len(calibrated_clusters)} groups.")
        return enhancements

    def _cluster_by_visual_invariants(self, items: List[Enhancement]) -> List[List[Enhancement]]:
        """底层原理: 基于视觉不变量(布局/背景)进行初步聚合"""
        clusters = []
        feature_cache = {} # path -> (hash, hist)
        
        for item in items:
            added = False
            path = item.media_paths[0]
            if path not in feature_cache:
                feature_cache[path] = self._get_image_features(path)
            item_hash, item_hist = feature_cache[path]
            
            for cluster in clusters:
                rep = cluster[0] # 取组内首帧作为容器基准
                rep_path = rep.media_paths[0]
                rep_hash, rep_hist = feature_cache[rep_path]
                
                # 双重校验: dHash (抗局部内容/文字干扰) + Histogram (校验背景/结构)
                hash_dist = self._hamming_distance(item_hash, rep_hash)
                hist_sim = cv2.compareHist(item_hist, rep_hist, cv2.HISTCMP_CORREL)
                
                if hash_dist <= self.DHASH_THRESHOLD and hist_sim >= self.HIST_SIM_THRESHOLD:
                    cluster.append(item)
                    added = True
                    break
            
            if not added:
                clusters.append([item])
        return clusters

    async def _calibrate_clusters(self, clusters: List[List[Enhancement]]) -> List[List[Enhancement]]:
        """校准逻辑: 使用时间连续性与语义一致性拆解误聚类"""
        new_clusters = []
        for cluster in clusters:
            if len(cluster) <= 1:
                new_clusters.append(cluster)
                continue
                
            sub_clusters = [[cluster[0]]]
            for i in range(1, len(cluster)):
                prev = sub_clusters[-1][-1]
                curr = cluster[i]
                
                # 时间校准 (Gap > 30s 极大概率不是同一页渐进展示)
                time_gap = curr.timestamp_start - prev.timestamp_end
                
                # 语义校准 (ASR 覆盖内容差异过大)
                semantic_sim = 1.0
                if self.semantic_extractor:
                    semantic_sim = await self.semantic_extractor.calculate_context_similarity(
                        curr.fault_text, prev.fault_text
                    )
                
                if time_gap > self.TIME_GAP_THRESHOLD and semantic_sim < self.SEMANTIC_THRESHOLD:
                    # 场景没变但时间过久且语义偏离 -> 判定为模板重合的下一节
                    sub_clusters.append([curr])
                else:
                    sub_clusters[-1].append(curr)
            new_clusters.extend(sub_clusters)
        return new_clusters

    def _pick_cluster_winner(self, cluster: List[Enhancement]) -> Enhancement:
        """核心指标: 挑选架构元素(矩形/箭头)最全、信息密度最高的一帧"""
        from .visual_element_detection_helpers import VisualElementDetector
        detector = VisualElementDetector()
        
        best_score = -1.0
        winner = cluster[0]
        
        for e in cluster:
            try:
                if not e.media_paths: continue
                path = e.media_paths[0]
                img = cv2.imread(path)
                if img is None:
                    logger.warning(f"Optimzer cannot read: {path}. Skipping scoring.")
                    continue
                    
                gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                edges = cv2.Canny(gray, 50, 150)
                
                # 指标1: 矩形框 (知识块容器)
                rects = detector.detect_rectangles(edges)
                # 指标2: 箭头 (逻辑连接符)
                arrows_data = detector.detect_arrows(edges, gray)
                arrows = arrows_data.get("total", 0) if isinstance(arrows_data, dict) else 0
                
                # 指标3: 边缘密度 (图像内容丰富度)
                density = np.sum(edges > 0) / edges.size
                
                # 打分公式: 矩形10分, 箭头5分, 密度100分 (加权归一化)
                score = (rects * 10) + (arrows * 5) + (density * 100)
                
                # 时间微弱偏置 (PPT 渐进通常后出的更完备)
                score += (e.timestamp_start / 2000.0)
                
                if score > best_score:
                    best_score = score
                    winner = e
            except Exception as ex:
                logger.warning(f"Error scoring frame {e.enhancement_id}: {ex}")
                continue
                
        return winner

    def _get_image_features(self, path: str) -> Tuple[np.ndarray, np.ndarray]:
        """提取 dHash 和 Histogram 特征 (带内部缓存)"""
        try:
            img = cv2.imread(path)
            if img is None:
                logger.warning(f"Optimizer cannot read: {path}. Returning zero features.")
                return np.zeros(64, dtype=bool), np.zeros(512, dtype=np.float32)

            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # 1. 计算 dHash (差异哈希)
            resized = cv2.resize(gray, (9, 8))
            diff = resized[:, 1:] > resized[:, :-1]
            dhash = diff.flatten()
            
            # 2. 计算简化的颜色直方图
            hist = cv2.calcHist([img], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
            cv2.normalize(hist, hist)
            
            return dhash, hist.flatten()
        except Exception as e:
            logger.warning(f"Feature extraction failed for {path}: {e}")
            return np.zeros(64, dtype=bool), np.zeros(512, dtype=np.float32)

    @staticmethod
    def _hamming_distance(h1: np.ndarray, h2: np.ndarray) -> int:
        return np.count_nonzero(h1 != h2)
