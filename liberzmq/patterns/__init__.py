#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消息模式模块

提供ZeroMQ的各种消息模式实现，包括：
- 发布-订阅模式 (Publish-Subscribe)
- 请求-响应模式 (Request-Reply)
- 推送-拉取模式 (Push-Pull)
"""

from .pubsub import Publisher, Subscriber
from .reqrep import Server, Client
from .pushpull import Pusher, Puller
from .dealerrouter import Dealer, Router

__all__ = [
    # 发布-订阅模式
    'Publisher',
    'Subscriber',
    
    # 请求-响应模式
    'Server',
    'Client',
    
    # 推送-拉取模式
    'Pusher',
    'Puller',
    
    # Dealer-Router模式
    'Dealer',
    'Router',
]

# 版本信息
__version__ = '1.0.0'
__author__ = 'LiberZMQ Team'
__description__ = 'ZeroMQ message patterns implementation'

# 模式类型映射
PATTERN_TYPES = {
    'publisher': Publisher,
    'subscriber': Subscriber,
    'server': Server,
    'client': Client,
    'pusher': Pusher,
    'puller': Puller,
    'dealer': Dealer,
    'router': Router,
}

# 模式别名
PATTERN_ALIASES = {
    'pub': 'publisher',
    'sub': 'subscriber',
    'req': 'client',
    'rep': 'server',
    'push': 'pusher',
    'pull': 'puller',
    'deal': 'dealer',
    'route': 'router',
}


def get_pattern_class(pattern_name: str):
    """根据模式名称获取对应的类
    
    Args:
        pattern_name: 模式名称
        
    Returns:
        对应的模式类
        
    Raises:
        ValueError: 未知的模式名称
    """
    # 标准化模式名称
    pattern_name = pattern_name.lower().strip()
    
    # 检查别名
    if pattern_name in PATTERN_ALIASES:
        pattern_name = PATTERN_ALIASES[pattern_name]
    
    # 获取类
    if pattern_name in PATTERN_TYPES:
        return PATTERN_TYPES[pattern_name]
    
    available_patterns = list(PATTERN_TYPES.keys()) + list(PATTERN_ALIASES.keys())
    raise ValueError(f"未知的模式名称: {pattern_name}，可用模式: {available_patterns}")


def list_available_patterns():
    """列出所有可用的模式
    
    Returns:
        Dict: 可用模式的信息
    """
    return {
        'patterns': list(PATTERN_TYPES.keys()),
        'aliases': PATTERN_ALIASES,
        'classes': {name: cls.__name__ for name, cls in PATTERN_TYPES.items()}
    }