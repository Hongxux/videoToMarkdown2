
import yaml
import json
import logging
import sys
from pathlib import Path
from nltk.corpus import wordnet
import nltk

# Define paths
PROJECT_ROOT = Path("d:/videoToMarkdownTest2/MVP_Module2_HEANCING")
CONFIG_PATH = PROJECT_ROOT / "config" / "dictionaries.yaml"
OUTPUT_PATH = PROJECT_ROOT / "config" / "dictionaries_expanded.yaml"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DictExpander")

def ensure_resources():
    try:
        nltk.data.find('corpora/wordnet')
    except LookupError:
        logger.info("Downloading NLTK wordnet...")
        nltk.download('wordnet')
        nltk.download('omw-1.4')

class UniversalMapper:
    def __init__(self):
        # A rich mapping of common Chinese concepts to English WordNet query terms
        self.cn_to_en = {
            # Logic / Abstract
            "概念": "concept", "定义": "definition", "原因": "cause", "原理": "principle", 
            "逻辑": "logic", "关系": "relation", "本质": "essence", "含义": "meaning",
            "背景": "background", "属性": "attribute", "规则": "rule", "理论": "theory",
            
            # Spatial / Structure
            "架构": "architecture", "结构": "structure", "组成": "composition", "对比": "contrast",
            "分布": "distribution", "布局": "layout", "模块": "module", "组件": "component",
            "模型": "model", "公式": "formula", "位置": "position", "形状": "shape",
            
            # Process / Action
            "操作": "operate", "演示": "demonstrate", "执行": "execute", "步骤": "step",
            "变化": "change", "过程": "process", "循环": "loop", "推导": "derive",
            "交互": "interaction", "运行": "run", "演变": "evolve", "计算": "calculate",
            
            # General Verbs
            "查看": "view", "检查": "check", "分析": "analyze", "辨识": "identify",
            "推演": "deduce", "生成": "generate", "连接": "connect", "包含": "contain",
            "属于": "belong", "导致": "cause"
        }
        
        # Reverse mapping (English -> Common Chinese Synonyms)
        self.en_to_cn = {
            "concept": "概念,观念,思想",
            "definition": "定义,释义,解释",
            "cause": "原因,起因,缘故,导致,引起",
            "principle": "原理,原则,法则",
            "logic": "逻辑,理路",
            "relation": "关系,关联,联系",
            "essence": "本质,实质,精髓",
            "meaning": "含义,意义,意思",
            "background": "背景,底色",
            "attribute": "属性,特性,特征",
            "rule": "规则,法则,条例",
            "theory": "理论,学说",
            
            "architecture": "架构,体系结构",
            "structure": "结构,构造",
            "composition": "组成,构成,成分",
            "contrast": "对比,对照,比对",
            "distribution": "分布,分配",
            "layout": "布局,排版,规划",
            "module": "模块,模组",
            "component": "组件,部件,零件",
            "model": "模型,模式",
            "formula": "公式,算式",
            
            "operate": "操作,运作,运转,作业",
            "demonstrate": "演示,展示,示范",
            "execute": "执行,实施,实行",
            "step": "步骤,阶段",
            "change": "变化,改变,变更,变动",
            "process": "过程,进程,工序,处理",
            "loop": "循环,回路,迭代",
            "derive": "推导,导出,衍生",
            "interaction": "交互,互动",
            "run": "运行,跑,执行",
            "evolve": "演变,进化,发展",
            "calculate": "计算,演算,核算,运算",
            
            "view": "查看,观看,视察,浏览",
            "check": "检查,核对,校验,验证",
            "analyze": "分析,解析,剖析",
            "identify": "识别,辨识,鉴定,确认",
            "generate": "生成,产生,创造",
            "connect": "连接,连结,关联",
            "contain": "包含,包括,容纳",
            "belong": "属于,归属",
        }

    def get_synonyms(self, word):
        synonyms = set()
        
        # 1. Direct Lookup via WordNet bridge
        if word in self.cn_to_en:
            en_term = self.cn_to_en[word]
            if en_term in self.en_to_cn:
                synonyms.update(self.en_to_cn[en_term].split(","))
                
            # Expand via WordNet
            try:
                for syn in wordnet.synsets(en_term):
                    for lemma in syn.lemmas():
                        en_syn = lemma.name().lower()
                        # Reverse lookup the English synonym
                        if en_syn in self.en_to_cn:
                             synonyms.update(self.en_to_cn[en_syn].split(","))
            except:
                pass

        return list(synonyms)

def expand_list(word_list, mapper, top_k=5):
    """
    Expand a list of words using the mapper.
    """
    expanded_set = set(word_list)
    initial_count = len(expanded_set)
    
    for word in word_list:
        syns = mapper.get_synonyms(word)
        for s in syns:
            if s != word:
                expanded_set.add(s)
            
    final_list = sorted(list(expanded_set))
    if len(final_list) > initial_count:
        logger.info(f"Expanded: {initial_count} -> {len(final_list)} ({word}...)")
    return final_list

def recursive_expand(data, mapper):
    if isinstance(data, dict):
        new_dict = {}
        for k, v in data.items():
            new_dict[k] = recursive_expand(v, mapper)
        return new_dict
    elif isinstance(data, list):
        if data and isinstance(data[0], str):
            # Only expand keyword lists, avoid expanding patterns if they are sentences
            # Heuristic: if items are short (<5 chars), likely keywords
            # Logic: We expand ALL as requested, but "sentences" won't match mapper anyway.
            return expand_list(data, mapper)
        else:
            return data
    else:
        return data

def main():
    if not CONFIG_PATH.exists():
        logger.error(f"Config file not found: {CONFIG_PATH}")
        return

    # Load original yaml
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        original_dict = yaml.safe_load(f)

    ensure_resources()
    mapper = UniversalMapper()
    
    logger.info("Starting dictionary expansion using Native Mapper + WordNet...")
    expanded_dict = recursive_expand(original_dict, mapper)
    
    # Save
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        yaml.dump(expanded_dict, f, allow_unicode=True, sort_keys=False)
        
    logger.info(f"Expansion complete. Saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
