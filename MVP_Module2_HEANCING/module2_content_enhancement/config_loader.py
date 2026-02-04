"""
模块说明：Module2 内容增强中的 config_loader 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    类说明：封装 ConfigLoader 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def __init__(self, config_dir: str = None):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：config_dir is None
        依据来源（证据链）：
        - 输入参数：config_dir。
        输入参数：
        - config_dir: 目录路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if config_dir is None:
            # 默认配置目录: module2_content_enhancement/../config
            module_dir = Path(__file__).parent
            config_dir = module_dir.parent / "config"
        
        self.config_dir = Path(config_dir)
        self._cache = {}
    
    def load_dictionaries(self) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 校验输入路径与参数。
        2) 读取并解析为结构化对象。
        实现方式：通过内部方法调用/状态更新、YAML 解析、文件系统读写实现。
        核心价值：将外部数据转为内部结构，统一输入口径。
        决策逻辑：
        - 条件：not dict_path.exists()
        依据来源（证据链）：
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        dict_path = self.config_dir / "dictionaries.yaml"
        if not dict_path.exists():
            return {}
            
        with open(dict_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：key in result and isinstance(result[key], dict) and isinstance(value, dict)
        依据来源（证据链）：
        输入参数：
        - base: 函数入参（类型：Dict）。
        - override: 函数入参（类型：Dict）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result


def load_module2_config(config_dir: str = None) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 校验输入路径与参数。
    2) 读取并解析为结构化对象。
    实现方式：通过YAML 解析、文件系统读写实现。
    核心价值：将外部数据转为内部结构，统一输入口径。
    决策逻辑：
    - 条件：not config_path.exists()
    - 条件：dict_config
    依据来源（证据链）：
    输入参数：
    - config_dir: 目录路径（类型：str）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    loader = get_config_loader(config_dir)
    
    # 加载module2_config.yaml
    config_path = loader.config_dir / "module2_config.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"module2_config.yaml not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"Loaded module2 config from {config_path}")
    
    # 🚀 Merge dictionaries.yaml (Hardcode Optimization)
    dict_config = loader.load_dictionaries()
    if dict_config:
        config = loader._deep_merge(config, dict_config)
        logger.info("Merged external dictionaries into config")
    
    return config


# 全局配置加载器实例
_global_loader = None


def get_config_loader(config_dir: str = None) -> ConfigLoader:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：_global_loader is None
    依据来源（证据链）：
    输入参数：
    - config_dir: 目录路径（类型：str）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    global _global_loader
    
    if _global_loader is None:
        _global_loader = ConfigLoader(config_dir)
    
    return _global_loader
