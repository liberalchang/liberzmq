#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
JSON序列化器模块

实现基于JSON格式的数据序列化和反序列化。
"""

import json
from typing import Any, Dict, Optional
from datetime import datetime, date
from decimal import Decimal
from .base import BaseSerializer
from ..exceptions.base import SerializationError
from ..logging.logger import get_logger

logger = get_logger(__name__)


class JSONEncoder(json.JSONEncoder):
    """自定义JSON编码器
    
    支持更多的Python数据类型。
    """
    
    def default(self, obj: Any) -> Any:
        """处理默认JSON编码器不支持的类型"""
        if isinstance(obj, datetime):
            return {
                "__type__": "datetime",
                "value": obj.isoformat()
            }
        elif isinstance(obj, date):
            return {
                "__type__": "date",
                "value": obj.isoformat()
            }
        elif isinstance(obj, Decimal):
            return {
                "__type__": "decimal",
                "value": str(obj)
            }
        elif isinstance(obj, set):
            return {
                "__type__": "set",
                "value": list(obj)
            }
        elif isinstance(obj, bytes):
            return {
                "__type__": "bytes",
                "value": obj.hex()
            }
        elif isinstance(obj, tuple):
            return {
                "__type__": "tuple",
                "value": list(obj)
            }
        elif hasattr(obj, '__dict__'):
            # 处理自定义对象
            return {
                "__type__": "object",
                "class": obj.__class__.__name__,
                "value": obj.__dict__
            }
        
        return super().default(obj)


def json_object_hook(obj: Dict[str, Any]) -> Any:
    """JSON对象钩子函数
    
    用于反序列化特殊类型的对象。
    """
    if "__type__" in obj:
        obj_type = obj["__type__"]
        value = obj["value"]
        
        if obj_type == "datetime":
            return datetime.fromisoformat(value)
        elif obj_type == "date":
            return date.fromisoformat(value)
        elif obj_type == "decimal":
            return Decimal(value)
        elif obj_type == "set":
            return set(value)
        elif obj_type == "bytes":
            return bytes.fromhex(value)
        elif obj_type == "tuple":
            return tuple(value)
        elif obj_type == "object":
            # 对于自定义对象，返回字典形式
            # 实际应用中可能需要更复杂的对象重建逻辑
            return value
    
    return obj


def _preprocess_data(data: Any) -> Any:
    """预处理数据，转换特殊类型"""
    if isinstance(data, tuple):
        return {
            "__type__": "tuple",
            "value": [_preprocess_data(item) for item in data]
        }
    elif isinstance(data, set):
        return {
            "__type__": "set",
            "value": [_preprocess_data(item) for item in data]
        }
    elif isinstance(data, bytes):
        return {
            "__type__": "bytes",
            "value": data.hex()
        }
    elif isinstance(data, datetime):
        return {
            "__type__": "datetime",
            "value": data.isoformat()
        }
    elif isinstance(data, date):
        return {
            "__type__": "date",
            "value": data.isoformat()
        }
    elif isinstance(data, Decimal):
        return {
            "__type__": "decimal",
            "value": str(data)
        }
    elif isinstance(data, dict):
        return {key: _preprocess_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_preprocess_data(item) for item in data]
    else:
        return data


class JSONSerializer(BaseSerializer):
    """JSON序列化器
    
    使用JSON格式进行数据序列化和反序列化。
    """
    
    def __init__(self, 
                 ensure_ascii: bool = False,
                 indent: Optional[int] = None,
                 separators: Optional[tuple] = None,
                 sort_keys: bool = False,
                 use_custom_encoder: bool = True,
                 **kwargs):
        """
        初始化JSON序列化器
        
        Args:
            ensure_ascii: 是否确保ASCII编码
            indent: JSON缩进
            separators: JSON分隔符
            sort_keys: 是否排序键
            use_custom_encoder: 是否使用自定义编码器
            **kwargs: 其他配置参数
        """
        super().__init__(**kwargs)
        self.ensure_ascii = ensure_ascii
        self.indent = indent
        self.separators = separators
        self.sort_keys = sort_keys
        self.use_custom_encoder = use_custom_encoder
        
        # JSON编码参数
        self.json_kwargs = {
            "ensure_ascii": self.ensure_ascii,
            "indent": self.indent,
            "separators": self.separators,
            "sort_keys": self.sort_keys
        }
        
        if self.use_custom_encoder:
            self.json_kwargs["cls"] = JSONEncoder
        
        logger.debug(f"JSON序列化器初始化完成: {self.json_kwargs}")
    
    @property
    def format_name(self) -> str:
        """序列化格式名称"""
        return "json"
    
    @property
    def content_type(self) -> str:
        """内容类型"""
        return "application/json"
    
    def serialize(self, data: Any) -> bytes:
        """序列化数据为JSON格式
        
        Args:
            data: 要序列化的数据
            
        Returns:
            bytes: JSON格式的字节数据
            
        Raises:
            SerializationError: 序列化失败时抛出
        """
        try:
            # 预处理数据，转换特殊类型
            if self.use_custom_encoder:
                processed_data = _preprocess_data(data)
            else:
                processed_data = data
                
            # 创建不包含cls的json_kwargs
            json_kwargs = {
                "ensure_ascii": self.ensure_ascii,
                "indent": self.indent,
                "separators": self.separators,
                "sort_keys": self.sort_keys
            }
            
            json_str = json.dumps(processed_data, **json_kwargs)
            result = json_str.encode('utf-8')
            
            logger.debug(f"JSON序列化成功，数据大小: {len(result)} 字节")
            return result
            
        except (TypeError, ValueError, UnicodeEncodeError) as e:
            error_msg = f"JSON序列化失败: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="json")
        except Exception as e:
            error_msg = f"JSON序列化时发生未知错误: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="json")
    
    def deserialize(self, data: bytes) -> Any:
        """从JSON格式反序列化数据
        
        Args:
            data: JSON格式的字节数据
            
        Returns:
            Any: 反序列化后的数据
            
        Raises:
            SerializationError: 反序列化失败时抛出
        """
        try:
            json_str = data.decode('utf-8')
            
            if self.use_custom_encoder:
                result = json.loads(json_str, object_hook=json_object_hook)
            else:
                result = json.loads(json_str)
            
            logger.debug(f"JSON反序列化成功，数据类型: {type(result)}")
            return result
            
        except UnicodeDecodeError as e:
            error_msg = f"JSON反序列化失败，UTF-8解码错误: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="json")
        except json.JSONDecodeError as e:
            error_msg = f"JSON反序列化失败，JSON格式错误: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="json")
        except Exception as e:
            error_msg = f"JSON反序列化时发生未知错误: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="json")
    
    def validate_data(self, data: Any) -> bool:
        """验证数据是否可以被JSON序列化
        
        Args:
            data: 要验证的数据
            
        Returns:
            bool: 数据是否有效
        """
        try:
            json.dumps(data, **self.json_kwargs)
            return True
        except (TypeError, ValueError) as e:
            logger.debug(f"JSON数据验证失败: {e}")
            return False
        except Exception as e:
            logger.warning(f"JSON数据验证时发生未知错误: {e}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """获取序列化器元数据"""
        metadata = super().get_metadata()
        metadata.update({
            "encoding": "utf-8",
            "ensure_ascii": self.ensure_ascii,
            "indent": self.indent,
            "sort_keys": self.sort_keys,
            "use_custom_encoder": self.use_custom_encoder
        })
        return metadata
    
    def set_options(self, **kwargs) -> None:
        """设置JSON序列化选项
        
        Args:
            **kwargs: JSON序列化选项
        """
        for key, value in kwargs.items():
            if key in ["ensure_ascii", "indent", "separators", "sort_keys", "use_custom_encoder"]:
                setattr(self, key, value)
                self.json_kwargs[key] = value
                logger.debug(f"更新JSON选项: {key} = {value}")
            else:
                logger.warning(f"未知的JSON选项: {key}")
        
        # 重新设置自定义编码器
        if self.use_custom_encoder:
            self.json_kwargs["cls"] = JSONEncoder
        elif "cls" in self.json_kwargs:
            del self.json_kwargs["cls"]
    
    def pretty_print(self, data: Any) -> str:
        """美化打印JSON数据
        
        Args:
            data: 要打印的数据
            
        Returns:
            str: 美化后的JSON字符串
        """
        try:
            return json.dumps(data, indent=2, ensure_ascii=False, cls=JSONEncoder if self.use_custom_encoder else None)
        except Exception as e:
            logger.error(f"美化打印JSON失败: {e}")
            return str(data)
    
    def minify(self, data: Any) -> str:
        """压缩JSON数据
        
        Args:
            data: 要压缩的数据
            
        Returns:
            str: 压缩后的JSON字符串
        """
        try:
            return json.dumps(data, separators=(',', ':'), ensure_ascii=self.ensure_ascii, cls=JSONEncoder if self.use_custom_encoder else None)
        except Exception as e:
            logger.error(f"压缩JSON失败: {e}")
            return str(data)