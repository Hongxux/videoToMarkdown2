"""Phase2A 素材生成异常定义。"""


class VLMaterialGeneratorError(Exception):
    """VL 素材生成错误。"""


class VLAnalysisError(VLMaterialGeneratorError):
    """VL 分析错误。"""


class JSONParseError(VLMaterialGeneratorError):
    """JSON 解析错误。"""

