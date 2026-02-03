import jieba
import pandas as pd
from textrank4zh import TextRank4Keyword
from nltk.corpus import wordnet
import nltk
import re
import json
from opencc import OpenCC

# ---------------------- 1. 通用配置项（用户仅需修改这里，适配任意领域） ----------------------
# 配置说明：
# - DOMAIN_NAME：领域名称（仅用于标识，如"医疗"|"金融"|"电商"）
# - STATIC_CORE_WORDS：该领域"静态需求"核心词（如医疗：["查询", "查看", "识别"]；金融：["查看", "核对", "统计"]）
# - DYNAMIC_CORE_WORDS：该领域"动态需求"核心词（如医疗：["操作", "执行", "处理"]；金融：["交易", "计算", "转账"]）
# - GENERAL_CORPUS：该领域通用语料（可从公开语料库/行业文档中复制，越多越精准）
# - STATIC_FEATURES：静态需求特征词（通用维度：["结构", "信息", "属性", "内容", "详情"]）
# - DYNAMIC_FEATURES：动态需求特征词（通用维度：["步骤", "过程", "操作", "流程", "执行"]）

# 【数学教育领域配置】(Matched to current project context)
DOMAIN_NAME = "数学教育"
STATIC_CORE_WORDS = ["查看", "检查", "辨识", "分析"]  # 静态需求核心词
DYNAMIC_CORE_WORDS = ["推导", "演算", "变形", "计算"]  # 动态需求核心词
GENERAL_CORPUS = [
    "查看数学公式的结构",
    "辨识分子的上下标",
    "分析矩阵的行列式",
    "推导求根公式的步骤",
    "演算积分的变换过程",
    "计算方程的结果",
    "查看分式组成",
    "检查符号位置",
    "分析空间布局",
    "推上演变过程"
]
# 通用静态/动态特征词（适配任意领域，无需修改）
STATIC_FEATURES = ["结构", "信息", "属性", "内容", "详情", "组成", "维度", "字段", "布局", "位置"]
DYNAMIC_FEATURES = ["步骤", "过程", "操作", "流程", "执行", "变换", "流转", "汇总", "演变", "推演"]

# ---------------------- 2. 通用工具初始化 ----------------------
def ensure_resources():
    print("Checking dependencies...")
    try:
        nltk.data.find('corpora/wordnet')
    except LookupError:
        print("Downloading NLTK wordnet...")
        nltk.download('wordnet')
        nltk.download('omw-1.4')

# 初始化TextRank关键词提取工具
tr4w = TextRank4Keyword(stop_words_file=None)

# 初始化简繁转换（通用文本兼容）
cc = OpenCC('s2t')

# ---------------------- 3. 通用同义词提取（无场景绑定） ----------------------
def get_universal_synonyms(word):
    """
    通用同义词提取（适配任意领域，无需场景映射）
    :param word: 核心词
    :return: 通用同义词列表
    """
    # 通用中文核心词-英文映射（覆盖90%通用动词）
    universal_cn_en = {
        # 通用静态需求词
        "查询": "query", "查看": "view", "识别": "identify", "分析": "analyze",
        "核对": "check", "统计": "count", "浏览": "browse", "确认": "confirm",
        "检查": "inspect", "辨识": "recognize",
        # 通用动态需求词
        "操作": "operate", "执行": "execute", "计算": "calculate", "处理": "process",
        "交易": "trade", "转账": "transfer", "治疗": "treat", "给药": "administer",
        "推导": "derive", "演算": "calculate", "变形": "transform"
    }
    
    if word not in universal_cn_en:
        return []
    
    synonyms = set()
    try:
        # 调用WordNet获取通用同义词
        for syn in wordnet.synsets(universal_cn_en[word], pos=wordnet.VERB):
            for lemma in syn.lemmas():
                # 通用英文-中文同义词映射（无场景绑定）
                # 这里为了简化展示，扩充了映射表
                universal_en_cn = {
                    "query": "查询、检索、查找、查阅",
                    "view": "查看、浏览、审阅、检视,观察",
                    "identify": "识别、辨识、确认、鉴别",
                    "analyze": "分析、剖析、解析、研判",
                    "check": "核对、核查、校验、验证,检查",
                    "count": "统计、计数、核算、汇总",
                    "inspect": "检查,审查,视察",
                    "recognize": "辨识,认出,识别",
                    
                    "operate": "操作、操控、运行、执行",
                    "execute": "执行、实施、落实、推行",
                    "calculate": "计算、演算、核算、换算",
                    "process": "处理、处置、加工、整理",
                    "derive": "推导,导出",
                    "transform": "变形,变换,转化"
                }
                
                # WordNet 也是英文，我们需要映射回中文
                # 这里做一个简单的反查逻辑
                en_word = lemma.name().lower()
                
                # 尝试直接查找
                if en_word in universal_en_cn:
                     synonyms.update(universal_en_cn[en_word].replace(",", "、").split("、"))
                
                # 尝试模糊查找 (e.g. view matches view)
                for k, v in universal_en_cn.items():
                    if k == en_word:
                        synonyms.update(v.replace(",", "、").split("、"))
                        
    except Exception as e:
        print(f"WordNet lookup failed: {e}")
        
    return list(synonyms)

# ---------------------- 4. 通用术语提取（自适应任意领域语料） ----------------------
def extract_universal_terms(corpus, static_features, dynamic_features, top_k=20):
    """
    通用术语提取（从任意领域语料中自动提取上下文词）
    :param corpus: 通用语料
    :param static_features: 静态特征词（通用维度）
    :param dynamic_features: 动态特征词（通用维度）
    :param top_k: 提取高频关键词数量
    :return: 静态上下文词、动态上下文词
    """
    # 合并语料并预处理（通用文本清洗）
    full_text = " ".join([cc.convert(text) for text in corpus])  # 简繁统一
    full_text = re.sub(r"[^\u4e00-\u9fa5\s]", " ", full_text)    # 仅保留中文和空格
    full_text = re.sub(r"\s+", " ", full_text)                   # 合并多余空格
    
    # TextRank提取通用关键词
    print("Running TextRank on corpus...")
    tr4w.analyze(text=full_text, lower=True, window=2)
    keywords = tr4w.get_keywords(top_k, word_min_len=2)
    keyword_list = [kw["word"] for kw in keywords]
    
    # 自动分类上下文词（基于通用特征，无场景绑定）
    static_context = [kw for kw in keyword_list if kw in static_features]
    dynamic_context = [kw for kw in keyword_list if kw in dynamic_features]
    
    return static_context, dynamic_context

# ---------------------- 5. 通用词典清洗（适配任意领域） ----------------------
def universal_dict_clean(word_list, stop_words=None):
    """
    通用词典清洗：去重、过滤停用词、过滤无效词
    :param word_list: 待清洗词列表
    :param stop_words: 通用停用词（可自定义）
    :return: 清洗后的词列表
    """
    # 通用停用词（无场景绑定）
    default_stop_words = {"的", "和", "与", "为", "之", "及", "其", "于", "也", "了", "是"}
    stop_words = stop_words or default_stop_words
    
    # 去重
    unique_words = list(set(word_list))
    # 过滤停用词和无效词
    clean_words = [
        word for word in unique_words 
        if len(word) >= 1 and word not in stop_words and not word.isdigit()
    ]
    return clean_words

def main():
    ensure_resources()
    
    print(f"Generating dictionary for domain: {DOMAIN_NAME}")
    
    # 3. 提取静态/动态核心词的通用同义词
    static_synonyms = []
    for core_word in STATIC_CORE_WORDS:
        static_synonyms.extend(get_universal_synonyms(core_word))

    dynamic_synonyms = []
    for core_word in DYNAMIC_CORE_WORDS:
        dynamic_synonyms.extend(get_universal_synonyms(core_word))

    # 4. 提取通用上下文关联词
    static_context, dynamic_context = extract_universal_terms(
        GENERAL_CORPUS, STATIC_FEATURES, DYNAMIC_FEATURES
    )

    # 6. 清洗各层词典
    static_synonyms_clean = universal_dict_clean(static_synonyms)
    dynamic_synonyms_clean = universal_dict_clean(dynamic_synonyms)
    static_context_clean = universal_dict_clean(static_context)
    dynamic_context_clean = universal_dict_clean(dynamic_context)

    # 7. 生成通用分层词典（适配任意领域）
    final_universal_dict = {
        "domain": DOMAIN_NAME,
        "static_dict": {
            "core": {word: 2.0 for word in STATIC_CORE_WORDS},    # 核心词权重（通用标准）
            "synonym": {word: 1.5 for word in static_synonyms_clean},  # 同义词权重
            "context": {word: 1.0 for word in static_context_clean}    # 上下文词权重
        },
        "dynamic_dict": {
            "core": {word: 2.0 for word in DYNAMIC_CORE_WORDS},
            "synonym": {word: 1.5 for word in dynamic_synonyms_clean},
            "context": {word: 1.0 for word in dynamic_context_clean}
        }
    }

    # 8. 通用可视化输出
    def print_universal_dict(dict_data):
        """通用词典打印（适配任意领域）"""
        print("="*60)
        print(f"✅ 【{dict_data['domain']}】通用分层词典生成完成（可直接使用）")
        print("="*60)
        for dict_type, layers in dict_data.items():
            if dict_type == "domain":
                continue
            print(f"\n【{dict_type}】")
            for layer, words in layers.items():
                weight = list(words.values())[0] if words else 0
                word_list = list(words.keys()) if words else []
                print(f"  {layer}（权重{weight}）：{word_list}")

    # 打印最终词典
    print_universal_dict(final_universal_dict)

    # 9. 通用导出（JSON格式，适配任意领域复用）
    output_filename = f"{DOMAIN_NAME}_dict.json".replace(" ", "_").replace(":", "")
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(final_universal_dict, f, ensure_ascii=False, indent=4)
    print(f"\n📁 通用词典已保存为：{output_filename}")

if __name__ == "__main__":
    main()
