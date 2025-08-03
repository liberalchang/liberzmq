#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
序列化器工厂模块

提供序列化器的创建、注册和管理功能。
"""

from typing import Dict, Type, Any, Optional, Union
from .base import BaseSerializer
from .json_serializer import JSONSerializer
from .binary_serializer import BinarySerializer
from .protobuf_serializer import ProtobufSerializer
from ..exceptions.base import ConfigurationError
from ..logging.logger import get_logger

logger = get_logger(__name__)


class SerializerFactory:
    """序列化器工厂类
    
    负责创建、注册和管理各种序列化器。
    """
    
    def __init__(self):
        """初始化序列化器工厂"""
        self._serializers: Dict[str, Type[BaseSerializer]] = {}
        self._instances: Dict[str, BaseSerializer] = {}
        self._default_configs: Dict[str, Dict[str, Any]] = {}
        
        # 注册内置序列化器
        self._register_builtin_serializers()
        
        logger.debug("序列化器工厂初始化完成")
    
    def _register_builtin_serializers(self) -> None:
        """注册内置序列化器"""
        # 注册JSON序列化器
        self.register("json", JSONSerializer, {
            "ensure_ascii": False,
            "indent": None,
            "use_custom_encoder": True
        })
        
        # 注册二进制序列化器
        self.register("binary", BinarySerializer, {
            "use_compression": False,
            "compression_level": 6
        })
        
        # 注册Protobuf序列化器
        self.register("protobuf", ProtobufSerializer, {
            "validate_message": True,
            "use_json_fallback": True
        })
        
        # 添加别名
        self._serializers["pb"] = ProtobufSerializer
        self._serializers["pickle"] = BinarySerializer
        
        logger.debug("内置序列化器注册完成")
    
    def register(self, 
                 name: str, 
                 serializer_class: Type[BaseSerializer], 
                 default_config: Optional[Dict[str, Any]] = None) -> None:
        """注册序列化器
        
        Args:
            name: 序列化器名称
            serializer_class: 序列化器类
            default_config: 默认配置
        """
        if not issubclass(serializer_class, BaseSerializer):
            raise ValueError(f"序列化器类必须继承自BaseSerializer: {serializer_class}")
        
        self._serializers[name.lower()] = serializer_class
        
        if default_config:
            self._default_configs[name.lower()] = default_config.copy()
        
        logger.info(f"注册序列化器: {name} -> {serializer_class.__name__}")
    
    def unregister(self, name: str) -> None:
        """注销序列化器
        
        Args:
            name: 序列化器名称
        """
        name = name.lower()
        
        if name in self._serializers:
            del self._serializers[name]
            logger.info(f"注销序列化器: {name}")
        
        if name in self._default_configs:
            del self._default_configs[name]
        
        if name in self._instances:
            del self._instances[name]
    
    def create(self, 
               name: str, 
               config: Optional[Dict[str, Any]] = None,
               cache: bool = True) -> BaseSerializer:
        """创建序列化器实例
        
        Args:
            name: 序列化器名称
            config: 配置参数
            cache: 是否缓存实例
            
        Returns:
            BaseSerializer: 序列化器实例
            
        Raises:
            ConfigurationError: 序列化器不存在或创建失败
        """
        name = name.lower()
        
        if name not in self._serializers:
            available = list(self._serializers.keys())
            raise ConfigurationError(
                f"未知的序列化器: {name}，可用的序列化器: {available}",
                config_key="serializer"
            )
        
        # 如果启用缓存且已有实例，直接返回
        if cache and name in self._instances:
            logger.debug(f"返回缓存的序列化器实例: {name}")
            return self._instances[name]
        
        # 合并配置
        final_config = {}
        if name in self._default_configs:
            final_config.update(self._default_configs[name])
        if config:
            final_config.update(config)
        
        try:
            serializer_class = self._serializers[name]
            instance = serializer_class(**final_config)
            
            if cache:
                self._instances[name] = instance
            
            logger.debug(f"创建序列化器实例: {name} ({serializer_class.__name__})")
            return instance
            
        except Exception as e:
            error_msg = f"创建序列化器失败: {name}, 错误: {str(e)}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg, config_key="serializer", details=str(e))
    
    def get_instance(self, name: str) -> Optional[BaseSerializer]:
        """获取缓存的序列化器实例
        
        Args:
            name: 序列化器名称
            
        Returns:
            Optional[BaseSerializer]: 序列化器实例，如果不存在则返回None
        """
        return self._instances.get(name.lower())
    
    def list_serializers(self) -> Dict[str, Type[BaseSerializer]]:
        """列出所有注册的序列化器
        
        Returns:
            Dict[str, Type[BaseSerializer]]: 序列化器名称到类的映射
        """
        return self._serializers.copy()
    
    def get_default_config(self, name: str) -> Optional[Dict[str, Any]]:
        """获取序列化器的默认配置
        
        Args:
            name: 序列化器名称
            
        Returns:
            Optional[Dict[str, Any]]: 默认配置，如果不存在则返回None
        """
        return self._default_configs.get(name.lower())
    
    def set_default_config(self, name: str, config: Dict[str, Any]) -> None:
        """设置序列化器的默认配置
        
        Args:
            name: 序列化器名称
            config: 默认配置
        """
        name = name.lower()
        
        if name not in self._serializers:
            raise ConfigurationError(f"序列化器不存在: {name}")
        
        self._default_configs[name] = config.copy()
        
        # 如果有缓存的实例，清除它
        if name in self._instances:
            del self._instances[name]
        
        logger.info(f"设置序列化器默认配置: {name}")
    
    def clear_cache(self, name: Optional[str] = None) -> None:
        """清除缓存的序列化器实例
        
        Args:
            name: 序列化器名称，如果为None则清除所有缓存
        """
        if name:
            name = name.lower()
            if name in self._instances:
                del self._instances[name]
                logger.debug(f"清除序列化器缓存: {name}")
        else:
            self._instances.clear()
            logger.debug("清除所有序列化器缓存")
    
    def get_info(self) -> Dict[str, Any]:
        """获取工厂信息
        
        Returns:
            Dict[str, Any]: 工厂信息
        """
        return {
            "registered_serializers": list(self._serializers.keys()),
            "cached_instances": list(self._instances.keys()),
            "default_configs": {k: bool(v) for k, v in self._default_configs.items()}
        }
    
    def validate_serializer(self, name: str, test_data: Any = None) -> bool:
        """验证序列化器是否可用
        
        Args:
            name: 序列化器名称
            test_data: 测试数据
            
        Returns:
            bool: 序列化器是否可用
        """
        try:
            serializer = self.create(name, cache=False)
            
            if test_data is not None:
                # 测试序列化和反序列化
                serialized = serializer.serialize(test_data)
                deserialized = serializer.deserialize(serialized)
                logger.debug(f"序列化器验证成功: {name}")
                return True
            else:
                # 只测试创建
                logger.debug(f"序列化器创建成功: {name}")
                return True
                
        except Exception as e:
            logger.warning(f"序列化器验证失败: {name}, 错误: {e}")
            return False


# 全局序列化器工厂实例
_global_factory: Optional[SerializerFactory] = None


def get_factory() -> SerializerFactory:
    """获取全局序列化器工厂实例
    
    Returns:
        SerializerFactory: 序列化器工厂实例
    """
    global _global_factory
    if _global_factory is None:
        _global_factory = SerializerFactory()
    return _global_factory


def get_serializer(name: str, 
                   config: Optional[Dict[str, Any]] = None,
                   cache: bool = True) -> BaseSerializer:
    """获取序列化器实例（便捷函数）
    
    Args:
        name: 序列化器名称
        config: 配置参数
        cache: 是否缓存实例
        
    Returns:
        BaseSerializer: 序列化器实例
    """
    factory = get_factory()
    return factory.create(name, config, cache)


def register_serializer(name: str, 
                       serializer_class: Type[BaseSerializer],
                       default_config: Optional[Dict[str, Any]] = None) -> None:
    """注册序列化器（便捷函数）
    
    Args:
        name: 序列化器名称
        serializer_class: 序列化器类
        default_config: 默认配置
    """
    factory = get_factory()
    factory.register(name, serializer_class, default_config)


def list_available_serializers() -> Dict[str, Type[BaseSerializer]]:
    """列出可用的序列化器（便捷函数）
    
    Returns:
        Dict[str, Type[BaseSerializer]]: 可用的序列化器
    """
    factory = get_factory()
    return factory.list_serializers()


def validate_all_serializers(test_data: Any = None) -> Dict[str, bool]:
    """验证所有序列化器（便捷函数）
    
    Args:
        test_data: 测试数据
        
    Returns:
        Dict[str, bool]: 验证结果
    """
    factory = get_factory()
    serializers = factory.list_serializers()
    
    if test_data is None:
        test_data = {"test": "data", "number": 42, "list": [1, 2, 3]}
    
    results = {}
    for name in serializers:
        results[name] = factory.validate_serializer(name, test_data)
    
    return results