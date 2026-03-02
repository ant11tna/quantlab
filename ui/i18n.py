"""i18n 翻译层

使用方式:
    from i18n import t
    
    # 简单翻译
    text = t("runs.title")  # -> "📊 回测运行记录"
    
    # 带参数格式化
    text = t("detail.caption", run_id="abc123", started="2024-01-01")
    # -> "运行ID：`abc123` | 启动时间：2024-01-01"
"""

from i18n_zh import ZH


def t(key: str, **kwargs) -> str:
    """翻译函数
    
    Args:
        key: 词典键名，如 "runs.title"
        **kwargs: 格式化参数
        
    Returns:
        翻译后的字符串，若 key 不存在则返回 key 本身
    """
    s = ZH.get(key, key)
    
    if kwargs:
        try:
            return s.format(**kwargs)
        except (KeyError, ValueError):
            # 格式化失败时返回原始字符串
            pass
    
    return s
