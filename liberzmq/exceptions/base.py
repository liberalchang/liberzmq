#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
异常基类模块

定义LiberZMQ库中使用的所有异常类。
"""

from typing import Optional, Any


class LiberZMQError(Exception):
    """LiberZMQ库的基础异常类
    
    所有LiberZMQ相关的异常都应该继承自这个类。
    """
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Any] = None):
        """
        初始化异常
        
        Args:
            message: 错误消息
            error_code: 错误代码
            details: 错误详情
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details
    
    def __str__(self) -> str:
        """返回异常的字符串表示"""
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message
    
    def __repr__(self) -> str:
        """返回异常的详细表示"""
        return f"{self.__class__.__name__}(message='{self.message}', error_code='{self.error_code}', details={self.details})"


class ConnectionError(LiberZMQError):
    """连接相关异常
    
    当ZeroMQ连接建立、断开或通信过程中发生错误时抛出。
    """
    
    def __init__(self, message: str, address: Optional[str] = None, **kwargs):
        """
        初始化连接异常
        
        Args:
            message: 错误消息
            address: 连接地址
            **kwargs: 其他参数
        """
        super().__init__(message, error_code="CONNECTION_ERROR")
        self.address = address
    
    def __str__(self) -> str:
        if self.address:
            return f"连接错误 [{self.address}]: {self.message}"
        return f"连接错误: {self.message}"


class SerializationError(LiberZMQError):
    """序列化相关异常
    
    当数据序列化或反序列化过程中发生错误时抛出。
    """
    
    def __init__(self, message: str, data_type: Optional[str] = None, serializer: Optional[str] = None, **kwargs):
        """
        初始化序列化异常
        
        Args:
            message: 错误消息
            data_type: 数据类型
            serializer: 序列化器名称
            **kwargs: 其他参数
        """
        super().__init__(message, error_code="SERIALIZATION_ERROR")
        self.data_type = data_type
        self.serializer = serializer
    
    def __str__(self) -> str:
        if self.data_type:
            return f"序列化错误 [{self.data_type}]: {self.message}"
        return f"序列化错误: {self.message}"


class ValidationError(LiberZMQError):
    """数据验证相关异常
    
    当数据验证失败时抛出。
    """
    
    def __init__(self, message: str, field: Optional[str] = None, value: Optional[Any] = None, **kwargs):
        """
        初始化验证异常
        
        Args:
            message: 错误消息
            field: 验证失败的字段
            value: 验证失败的值
            **kwargs: 其他参数
        """
        super().__init__(message, error_code="VALIDATION_ERROR")
        self.field = field
        self.value = value
    
    def __str__(self) -> str:
        if self.field:
            return f"验证错误 [{self.field}]: {self.message}"
        return f"验证错误: {self.message}"


class TimeoutError(LiberZMQError):
    """超时相关异常
    
    当操作超时时抛出。
    """
    
    def __init__(self, message: str, timeout: Optional[int] = None, operation: Optional[str] = None, **kwargs):
        """
        初始化超时异常
        
        Args:
            message: 错误消息
            timeout: 超时时间（毫秒）
            operation: 超时的操作
            **kwargs: 其他参数
        """
        super().__init__(message, error_code="TIMEOUT_ERROR")
        self.timeout = timeout
        self.operation = operation
    
    def __str__(self) -> str:
        parts = ["超时错误"]
        if self.operation:
            parts.append(f"[{self.operation}]")
        if self.timeout:
            parts.append(f"(超时时间: {self.timeout}ms)")
        parts.append(f": {self.message}")
        return "".join(parts)


class ConfigurationError(LiberZMQError):
    """配置相关异常
    
    当配置错误或缺失时抛出。
    """
    
    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        """
        初始化配置异常
        
        Args:
            message: 错误消息
            config_key: 配置键
            **kwargs: 其他参数
        """
        super().__init__(message, error_code="CONFIGURATION_ERROR")
        self.config_key = config_key
    
    def __str__(self) -> str:
        if self.config_key:
            return f"配置错误 [{self.config_key}]: {self.message}"
        return f"配置错误: {self.message}"


class MessageError(LiberZMQError):
    """消息相关异常
    
    当消息处理过程中发生错误时抛出。
    """
    
    def __init__(self, message: str, message_type: Optional[str] = None, topic: Optional[str] = None, operation: Optional[str] = None, **kwargs):
        """
        初始化消息异常
        
        Args:
            message: 错误消息
            message_type: 消息类型
            topic: 主题
            operation: 操作类型
            **kwargs: 其他参数
        """
        super().__init__(message, error_code="MESSAGE_ERROR")
        self.message_type = message_type
        self.topic = topic
        self.operation = operation
    
    def __str__(self) -> str:
        parts = ["消息错误"]
        if self.topic:
            parts.append(f"[主题: {self.topic}]")
        if self.message_type:
            parts.append(f"[类型: {self.message_type}]")
        parts.append(f": {self.message}")
        return "".join(parts)


# 为了向后兼容，提供一些常用的异常别名
ZMQConnectionError = ConnectionError
ZMQSerializationError = SerializationError
ZMQValidationError = ValidationError
ZMQTimeoutError = TimeoutError
ZMQConfigurationError = ConfigurationError
ZMQMessageError = MessageError