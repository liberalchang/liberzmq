#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Protobuf序列化器模块

实现基于Protocol Buffers的数据序列化和反序列化。
"""

from typing import Any, Dict, Type, Optional, Union
from .base import BaseSerializer
from ..exceptions.base import SerializationError
from ..logging.logger import get_logger

try:
    import google.protobuf.message as pb_message
    from google.protobuf.json_format import MessageToDict, ParseDict
    from google.protobuf.message import DecodeError
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False
    pb_message = None
    MessageToDict = None
    ParseDict = None
    DecodeError = None

logger = get_logger(__name__)


class ProtobufSerializer(BaseSerializer):
    """Protobuf序列化器
    
    使用Protocol Buffers进行数据序列化和反序列化。
    支持protobuf消息对象和字典数据的相互转换。
    """
    
    def __init__(self, 
                 message_class: Optional[Type] = None,
                 validate_message: bool = True,
                 use_json_fallback: bool = True,
                 preserve_proto_field_name: bool = False,
                 including_default_value_fields: bool = False,
                 **kwargs):
        """
        初始化Protobuf序列化器
        
        Args:
            message_class: Protobuf消息类
            validate_message: 是否验证消息
            use_json_fallback: 当数据不是protobuf消息时是否使用JSON回退
            preserve_proto_field_name: 是否保留proto字段名
            including_default_value_fields: 是否包含默认值字段
            **kwargs: 其他配置参数
        """
        if not PROTOBUF_AVAILABLE:
            raise ImportError("protobuf库未安装，请运行: pip install protobuf")
        
        super().__init__(**kwargs)
        self.message_class = message_class
        self.validate_message = validate_message
        self.use_json_fallback = use_json_fallback
        self.preserve_proto_field_name = preserve_proto_field_name
        self.including_default_value_fields = including_default_value_fields
        
        # JSON序列化选项
        self.json_options = {
            "preserve_proto_field_name": self.preserve_proto_field_name,
            "including_default_value_fields": self.including_default_value_fields
        }
        
        # 如果启用JSON回退，导入JSON序列化器
        if self.use_json_fallback:
            from .json_serializer import JSONSerializer
            self.json_serializer = JSONSerializer()
        else:
            self.json_serializer = None
        
        logger.debug(f"Protobuf序列化器初始化完成，消息类: {message_class}, JSON回退: {use_json_fallback}")
    
    @property
    def format_name(self) -> str:
        """序列化格式名称"""
        return "protobuf"
    
    @property
    def content_type(self) -> str:
        """内容类型"""
        return "application/x-protobuf"
    
    def serialize(self, data: Any) -> bytes:
        """序列化数据为Protobuf格式
        
        Args:
            data: 要序列化的数据（protobuf消息对象或字典）
            
        Returns:
            bytes: Protobuf格式的字节数据
            
        Raises:
            SerializationError: 序列化失败时抛出
        """
        try:
            # 如果数据已经是protobuf消息对象
            if isinstance(data, pb_message.Message):
                if self.validate_message:
                    # 验证消息
                    try:
                        data.SerializeToString()  # 测试序列化
                    except Exception as e:
                        raise SerializationError(f"Protobuf消息验证失败: {str(e)}")
                
                result = data.SerializeToString()
                logger.debug(f"Protobuf序列化成功（消息对象），数据大小: {len(result)} 字节")
                return result
            
            # 如果有指定的消息类，尝试从字典创建消息
            elif self.message_class and isinstance(data, dict):
                try:
                    message = self.message_class()
                    ParseDict(data, message, **self.json_options)
                    
                    if self.validate_message:
                        message.SerializeToString()  # 测试序列化
                    
                    result = message.SerializeToString()
                    logger.debug(f"Protobuf序列化成功（字典转消息），数据大小: {len(result)} 字节")
                    return result
                    
                except Exception as e:
                    if self.use_json_fallback:
                        logger.warning(f"Protobuf序列化失败，使用JSON回退: {str(e)}")
                        return self._serialize_with_json_fallback(data)
                    else:
                        raise SerializationError(f"从字典创建Protobuf消息失败: {str(e)}")
            
            # 如果启用JSON回退，使用JSON序列化
            elif self.use_json_fallback:
                logger.debug("数据不是Protobuf消息，使用JSON回退序列化")
                return self._serialize_with_json_fallback(data)
            
            else:
                raise SerializationError(f"不支持的数据类型: {type(data)}，期望protobuf消息对象或字典")
                
        except SerializationError:
            # 重新抛出SerializationError
            raise
        except Exception as e:
            error_msg = f"Protobuf序列化时发生未知错误: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="protobuf")
    
    def deserialize(self, data: bytes) -> Any:
        """从Protobuf格式反序列化数据
        
        Args:
            data: Protobuf格式的字节数据
            
        Returns:
            Any: 反序列化后的数据
            
        Raises:
            SerializationError: 反序列化失败时抛出
        """
        try:
            # 如果有指定的消息类，尝试反序列化为消息对象
            if self.message_class:
                try:
                    message = self.message_class()
                    message.ParseFromString(data)
                    
                    logger.debug(f"Protobuf反序列化成功（消息对象），类型: {type(message)}")
                    return message
                    
                except DecodeError as e:
                    if self.use_json_fallback:
                        logger.warning(f"Protobuf反序列化失败，尝试JSON回退: {str(e)}")
                        return self._deserialize_with_json_fallback(data)
                    else:
                        raise SerializationError(f"Protobuf反序列化失败: {str(e)}")
            
            # 如果没有指定消息类但启用JSON回退
            elif self.use_json_fallback:
                logger.debug("未指定消息类，尝试JSON回退反序列化")
                return self._deserialize_with_json_fallback(data)
            
            else:
                raise SerializationError("未指定消息类且未启用JSON回退，无法反序列化")
                
        except SerializationError:
            # 重新抛出SerializationError
            raise
        except Exception as e:
            error_msg = f"Protobuf反序列化时发生未知错误: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="protobuf")
    
    def _serialize_with_json_fallback(self, data: Any) -> bytes:
        """使用JSON回退序列化"""
        if not self.json_serializer:
            raise SerializationError("JSON回退序列化器未初始化")
        
        # 添加标记表示这是JSON回退数据
        json_data = self.json_serializer.serialize(data)
        # 在前面添加一个字节标记（0xFF表示JSON回退）
        return b'\xff' + json_data
    
    def _deserialize_with_json_fallback(self, data: bytes) -> Any:
        """使用JSON回退反序列化"""
        if not self.json_serializer:
            raise SerializationError("JSON回退序列化器未初始化")
        
        # 检查是否有JSON回退标记
        if data.startswith(b'\xff'):
            json_data = data[1:]  # 移除标记字节
            return self.json_serializer.deserialize(json_data)
        else:
            # 直接尝试JSON反序列化
            return self.json_serializer.deserialize(data)
    
    def validate_data(self, data: Any) -> bool:
        """验证数据是否可以被Protobuf序列化
        
        Args:
            data: 要验证的数据
            
        Returns:
            bool: 数据是否有效
        """
        try:
            self.serialize(data)
            return True
        except Exception as e:
            logger.debug(f"Protobuf数据验证失败: {e}")
            return False
    
    def message_to_dict(self, message: pb_message.Message) -> Dict[str, Any]:
        """将Protobuf消息转换为字典
        
        Args:
            message: Protobuf消息对象
            
        Returns:
            Dict[str, Any]: 字典表示
        """
        if not isinstance(message, pb_message.Message):
            raise ValueError("输入必须是Protobuf消息对象")
        
        try:
            return MessageToDict(message, **self.json_options)
        except Exception as e:
            logger.error(f"Protobuf消息转字典失败: {e}")
            raise SerializationError(f"消息转字典失败: {str(e)}")
    
    def dict_to_message(self, data: Dict[str, Any]) -> pb_message.Message:
        """将字典转换为Protobuf消息
        
        Args:
            data: 字典数据
            
        Returns:
            pb_message.Message: Protobuf消息对象
        """
        if not self.message_class:
            raise ValueError("未指定消息类")
        
        if not isinstance(data, dict):
            raise ValueError("输入必须是字典")
        
        try:
            message = self.message_class()
            ParseDict(data, message, **self.json_options)
            return message
        except Exception as e:
            logger.error(f"字典转Protobuf消息失败: {e}")
            raise SerializationError(f"字典转消息失败: {str(e)}")
    
    def set_message_class(self, message_class: Type) -> None:
        """设置消息类
        
        Args:
            message_class: Protobuf消息类
        """
        if not issubclass(message_class, pb_message.Message):
            raise ValueError("消息类必须继承自protobuf Message")
        
        self.message_class = message_class
        logger.info(f"设置消息类: {message_class.__name__}")
    
    def get_metadata(self) -> Dict[str, Any]:
        """获取序列化器元数据"""
        metadata = super().get_metadata()
        metadata.update({
            "message_class": self.message_class.__name__ if self.message_class else None,
            "validate_message": self.validate_message,
            "use_json_fallback": self.use_json_fallback,
            "preserve_proto_field_name": self.preserve_proto_field_name,
            "including_default_value_fields": self.including_default_value_fields,
            "protobuf_available": PROTOBUF_AVAILABLE
        })
        return metadata
    
    def set_json_options(self, **kwargs) -> None:
        """设置JSON转换选项
        
        Args:
            **kwargs: JSON转换选项
        """
        valid_options = ["preserve_proto_field_name", "including_default_value_fields"]
        
        for key, value in kwargs.items():
            if key in valid_options:
                setattr(self, key, value)
                self.json_options[key] = value
                logger.debug(f"更新JSON选项: {key} = {value}")
            else:
                logger.warning(f"未知的JSON选项: {key}")
    
    def get_message_info(self) -> Optional[Dict[str, Any]]:
        """获取消息类信息
        
        Returns:
            Optional[Dict[str, Any]]: 消息类信息，如果未设置消息类则返回None
        """
        if not self.message_class:
            return None
        
        try:
            message = self.message_class()
            descriptor = message.DESCRIPTOR
            
            fields_info = []
            for field in descriptor.fields:
                field_info = {
                    "name": field.name,
                    "number": field.number,
                    "type": field.type,
                    "label": field.label,
                    "required": field.label == field.LABEL_REQUIRED
                }
                fields_info.append(field_info)
            
            return {
                "name": descriptor.name,
                "full_name": descriptor.full_name,
                "fields_count": len(descriptor.fields),
                "fields": fields_info
            }
        except Exception as e:
            logger.error(f"获取消息信息失败: {e}")
            return None