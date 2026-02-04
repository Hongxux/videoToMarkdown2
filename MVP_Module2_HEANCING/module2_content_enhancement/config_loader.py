"""
Configuration Loader

Load configuration from YAML files for module2 components.
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    配置加载器
    
    从YAML文件加载配置,支持默认配置和用户自定义配置合并
    """
    
    def __init__(self, config_dir: str = None):
        """
        Args:
            config_dir: 配置文件目录,默认为模块根目录的config子目录
        """
        if config_dir is None:
            # 默认配置目录: module2_content_enhancement/../config
            module_dir = Path(__file__).parent
            config_dir = module_dir.parent / "config"
        
        self.config_dir = Path(config_dir)
        self._cache = {}
    
    def load_dictionaries(self) -> Dict[str, Any]:
        """
        加载集中管理的词典配置
        """
        dict_path = self.config_dir / "dictionaries.yaml"
        if not dict_path.exists():
            return {}
            
        with open(dict_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        深度合并两个字典
        
        Args:
            base: 基础配置
            override: 覆盖配置
        
        Returns:
            合并后的配置
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result


def load_module2_config(config_dir: str = None) -> Dict[str, Any]:
    """
    加载Module 2完整配置
    
    包含所有模块的可调参数
    
    Args:
        config_dir: 配置目录
    
    Returns:
        配置字典
    """
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
    获取全局配置加载器实例
    
    Args:
        config_dir: 配置目录 (可选)
    
    Returns:
        ConfigLoader实例
    """
    global _global_loader
    
    if _global_loader is None:
        _global_loader = ConfigLoader(config_dir)
    
    return _global_loader
