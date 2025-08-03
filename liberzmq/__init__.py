#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LiberZMQ - Python ZeroMQ库高级封装

这是一个基于pyzmq的高级封装库，提供易于使用的消息传递解决方案。
支持多种消息模式（Pub/Sub, Req/Rep, Push/Pull）和数据格式（Binary, JSON, Protobuf）。

Author: prompt-optimizer
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "prompt-optimizer"
__email__ = "developer@example.com"
__description__ = "Python ZeroMQ库高级封装，提供易于使用、高性能和可扩展的消息传递解决方案"

# 导入核心类和函数
from .patterns.pubsub import Publisher, Subscriber
from .patterns.reqrep import Server, Client
from .patterns.pushpull import Pusher, Puller

# 导入配置和日志
from .core.config import Config
from .logging.logger import get_logger, configure_logging

# 导入异常类
from .exceptions.base import (
    LiberZMQError,
    ConnectionError,
    SerializationError,
    ValidationError,
    TimeoutError
)

# 导入序列化器
from .serializers.json_serializer import JSONSerializer
from .serializers.protobuf_serializer import ProtobufSerializer
from .serializers.binary_serializer import BinarySerializer

# 公开的API
__all__ = [
    # 版本信息
    "__version__",
    "__author__",
    "__email__",
    "__description__",
    
    # 消息模式类
    "Publisher",
    "Subscriber", 
    "Server",
    "Client",
    "Pusher",
    "Puller",
    
    # 配置和日志
    "Config",
    "get_logger",
    "configure_logging",
    
    # 异常类
    "LiberZMQError",
    "ConnectionError",
    "SerializationError",
    "ValidationError",
    "TimeoutError",
    
    # 序列化器
    "JSONSerializer",
    "ProtobufSerializer",
    "BinarySerializer",
]

# 默认配置初始化
from .core.config import Config
_default_config = Config()

# 配置默认日志
configure_logging()

# 获取库的日志记录器
logger = get_logger(__name__)
logger.info(f"LiberZMQ v{__version__} 初始化完成")