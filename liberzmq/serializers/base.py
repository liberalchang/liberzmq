#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
序列化器基类模块

定义所有序列化器的基础接口和通用功能。
"""

from abc import ABC, abstractmethod
from typing import Any, Union, Dict, Optional
from ..exceptions.base import SerializationError
from ..logging.logger import get_logger

logger = get_logger(__name__)


class BaseSerializer(ABC):
    """序列化器基类
    
    定义数据序列化和反序列化的通用接口。
    """
    
    def __init__(self, **kwargs):
        """
        初始化序列化器
        
        Args:
            **kwargs: 序列化器特定的配置参数
        """
        self.config = kwargs
        logger.debug(f"初始化序列化器: {self.__class__.__name__}")
    
    @property
    @abstractmethod
    def format_name(self) -> str:
        """序列化格式名称"""
        pass
    
    @property
    @abstractmethod
    def content_type(self) -> str:
        """内容类型"""
        pass
    
    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        """序列化数据
        
        Args:
            data: 要序列化的数据
            
        Returns:
            bytes: 序列化后的字节数据
            
        Raises:
            SerializationError: 序列化失败时抛出
        """
        pass
    
    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """反序列化数据
        
        Args:
            data: 要反序列化的字节数据
            
        Returns:
            Any: 反序列化后的数据
            
        Raises:
            SerializationError: 反序列化失败时抛出
        """
        pass
    
    def validate_data(self, data: Any) -> bool:
        """验证数据是否可以被序列化
        
        Args:
            data: 要验证的数据
            
        Returns:
            bool: 数据是否有效
        """
        try:
            # 尝试序列化来验证数据
            self.serialize(data)
            return True
        except Exception as e:
            logger.warning(f"数据验证失败: {e}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """获取序列化器元数据
        
        Returns:
            Dict[str, Any]: 序列化器元数据
        """
        return {
            "format_name": self.format_name,
            "content_type": self.content_type,
            "config": self.config
        }
    
    def __str__(self) -> str:
        """返回序列化器的字符串表示"""
        return f"{self.__class__.__name__}(format={self.format_name})"
    
    def __repr__(self) -> str:
        """返回序列化器的详细表示"""
        return f"{self.__class__.__name__}(format='{self.format_name}', config={self.config})"


class MessageEnvelope:
    """消息信封类
    
    用于包装消息，提供元数据支持。
    """
    
    def __init__(self, 
                 data: Any, 
                 message_type: Optional[str] = None,
                 timestamp: Optional[float] = None,
                 sender: Optional[str] = None,
                 metadata: Optional[Dict[str, Any]] = None):
        """
        初始化消息信封
        
        Args:
            data: 消息数据
            message_type: 消息类型
            timestamp: 时间戳
            sender: 发送者
            metadata: 额外的元数据
        """
        import time
        
        self.data = data
        self.message_type = message_type
        self.timestamp = timestamp or time.time()
        self.sender = sender
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "data": self.data,
            "message_type": self.message_type,
            "timestamp": self.timestamp,
            "sender": self.sender,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MessageEnvelope":
        """从字典创建消息信封"""
        return cls(
            data=data.get("data"),
            message_type=data.get("message_type"),
            timestamp=data.get("timestamp"),
            sender=data.get("sender"),
            metadata=data.get("metadata", {})
        )
    
    def __str__(self) -> str:
        return f"MessageEnvelope(type={self.message_type}, sender={self.sender}, timestamp={self.timestamp})"
    
    def __repr__(self) -> str:
        return f"MessageEnvelope(data={self.data}, message_type='{self.message_type}', timestamp={self.timestamp}, sender='{self.sender}', metadata={self.metadata})"


class SerializationContext:
    """序列化上下文
    
    提供序列化过程中的上下文信息。
    """
    
    def __init__(self, 
                 serializer: BaseSerializer,
                 use_envelope: bool = False,
                 validate: bool = True):
        """
        初始化序列化上下文
        
        Args:
            serializer: 序列化器
            use_envelope: 是否使用消息信封
            validate: 是否验证数据
        """
        self.serializer = serializer
        self.use_envelope = use_envelope
        self.validate = validate
    
    def serialize(self, data: Any, **envelope_kwargs) -> bytes:
        """序列化数据
        
        Args:
            data: 要序列化的数据
            **envelope_kwargs: 消息信封参数
            
        Returns:
            bytes: 序列化后的数据
        """
        if self.validate and not self.serializer.validate_data(data):
            raise SerializationError(f"数据验证失败: {type(data)}")
        
        if self.use_envelope:
            envelope = MessageEnvelope(data, **envelope_kwargs)
            return self.serializer.serialize(envelope.to_dict())
        else:
            return self.serializer.serialize(data)
    
    def deserialize(self, data: bytes) -> Any:
        """反序列化数据
        
        Args:
            data: 要反序列化的数据
            
        Returns:
            Any: 反序列化后的数据
        """
        result = self.serializer.deserialize(data)
        
        if self.use_envelope:
            if isinstance(result, dict) and "data" in result:
                envelope = MessageEnvelope.from_dict(result)
                return envelope
            else:
                logger.warning("期望消息信封格式，但收到的数据不符合格式")
                return result
        else:
            return result