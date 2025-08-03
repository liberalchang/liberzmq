#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置管理模块

提供全局配置管理功能，支持从文件、环境变量等多种方式加载配置。
"""

import os
import yaml
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from ..logging.logger import get_logger

logger = get_logger(__name__)


class ZMQConfig(BaseModel):
    """ZeroMQ相关配置"""
    io_threads: int = Field(default=1, description="IO线程数")
    max_sockets: int = Field(default=1024, description="最大socket数量")
    socket_timeout: int = Field(default=5000, description="socket超时时间(毫秒)")
    linger: int = Field(default=1000, description="socket关闭时的等待时间(毫秒)")
    send_timeout: int = Field(default=5000, description="发送超时时间(毫秒)")
    high_water_mark: int = Field(default=1000, description="高水位标记")
    

class LoggingConfig(BaseModel):
    """日志配置"""
    level: str = Field(default="INFO", description="日志级别")
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="日志格式"
    )
    file_path: Optional[str] = Field(default=None, description="日志文件路径")
    max_file_size: int = Field(default=10*1024*1024, description="日志文件最大大小(字节)")
    backup_count: int = Field(default=5, description="日志文件备份数量")
    console_output: bool = Field(default=True, description="是否输出到控制台")


class SerializationConfig(BaseModel):
    """序列化配置"""
    default: str = Field(default="json", description="默认序列化格式")
    json_ensure_ascii: bool = Field(default=False, description="JSON是否确保ASCII")
    json_indent: Optional[int] = Field(default=None, description="JSON缩进")
    protobuf_validate: bool = Field(default=True, description="是否验证Protobuf消息")


class Config(BaseModel):
    """主配置类"""
    
    zmq: ZMQConfig = Field(default_factory=ZMQConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    serialization: SerializationConfig = Field(default_factory=SerializationConfig)
    
    # 其他配置
    debug: bool = Field(default=False, description="是否开启调试模式")
    validate_data: bool = Field(default=True, description="是否验证数据")
    
    # 动态配置存储
    dynamic_config: dict = Field(default_factory=dict, exclude=True)
    
    class Config:
        extra = "allow"  # 允许额外字段
    
    @classmethod
    def from_file(cls, config_path: str) -> "Config":
        """从配置文件加载配置
        
        Args:
            config_path: 配置文件路径，支持YAML和JSON格式
            
        Returns:
            Config: 配置实例
            
        Raises:
            FileNotFoundError: 配置文件不存在
            ValueError: 配置文件格式错误
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                if config_path.endswith(('.yml', '.yaml')):
                    config_data = yaml.safe_load(f)
                elif config_path.endswith('.json'):
                    import json
                    config_data = json.load(f)
                else:
                    raise ValueError(f"不支持的配置文件格式: {config_path}")
                    
            logger.info(f"从文件加载配置: {config_path}")
            return cls(**config_data)
            
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise ValueError(f"配置文件格式错误: {e}")
    
    @classmethod
    def from_env(cls, prefix: str = "LIBERZMQ_") -> "Config":
        """从环境变量加载配置
        
        Args:
            prefix: 环境变量前缀
            
        Returns:
            Config: 配置实例
        """
        config_data = {}
        
        # 扫描环境变量
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # 移除前缀并转换为小写
                config_key = key[len(prefix):].lower()
                
                # 处理嵌套配置
                if '__' in config_key:
                    parts = config_key.split('__')
                    current = config_data
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    current[parts[-1]] = cls._convert_env_value(value)
                else:
                    config_data[config_key] = cls._convert_env_value(value)
        
        logger.info(f"从环境变量加载配置，前缀: {prefix}")
        return cls(**config_data)
    
    @staticmethod
    def _convert_env_value(value: str) -> Any:
        """转换环境变量值为合适的类型"""
        # 尝试转换为布尔值
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        
        # 尝试转换为整数
        try:
            return int(value)
        except ValueError:
            pass
        
        # 尝试转换为浮点数
        try:
            return float(value)
        except ValueError:
            pass
        
        # 返回字符串
        return value
    
    def to_file(self, config_path: str) -> None:
        """将配置保存到文件
        
        Args:
            config_path: 配置文件路径
        """
        config_data = self.model_dump()
        
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                if config_path.endswith(('.yml', '.yaml')):
                    yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
                elif config_path.endswith('.json'):
                    import json
                    json.dump(config_data, f, indent=2, ensure_ascii=False)
                else:
                    raise ValueError(f"不支持的配置文件格式: {config_path}")
                    
            logger.info(f"配置已保存到文件: {config_path}")
            
        except Exception as e:
            logger.error(f"保存配置文件失败: {e}")
            raise
    
    def set(self, key: str, value: Any) -> None:
        """设置配置值
        
        Args:
            key: 配置键，支持点号分隔的嵌套访问，如 'zmq.linger'
            value: 配置值
        """
        parts = key.split('.')
        obj = self
        
        # 导航到目标对象
        for part in parts[:-1]:
            if hasattr(obj, part):
                obj = getattr(obj, part)
            else:
                # 如果路径不存在，存储到动态配置中
                if not hasattr(self, 'dynamic_config'):
                    self.dynamic_config = {}
                self.dynamic_config[key] = value
                logger.debug(f"设置动态配置项: {key} = {value}")
                return
        
        # 设置最终值
        final_key = parts[-1]
        if hasattr(obj, final_key):
            setattr(obj, final_key, value)
            logger.debug(f"设置配置项: {key} = {value}")
        else:
            # 存储到动态配置中
            if not hasattr(self, 'dynamic_config'):
                self.dynamic_config = {}
            self.dynamic_config[key] = value
            logger.debug(f"设置动态配置项: {key} = {value}")
    
    def update(self, updates: dict) -> None:
        """批量更新配置
        
        Args:
            updates: 要更新的配置项字典
        """
        for key, value in updates.items():
            self.set(key, value)
    
    def get(self, key: str, default=None):
        """使用点号分隔的键获取配置值
        
        Args:
            key: 配置键，支持点号分隔的嵌套访问，如 'zmq.linger'
            default: 默认值
            
        Returns:
            配置值或默认值
        """
        # 首先检查动态配置中是否有完整的键
        if hasattr(self, 'dynamic_config') and key in self.dynamic_config:
            return self.dynamic_config[key]
        
        try:
            parts = key.split('.')
            value = self
            
            for part in parts:
                if hasattr(value, part):
                    value = getattr(value, part)
                else:
                    return default
            
            return value
        except Exception:
            return default
    
    def load_from_dict(self, config_dict: dict) -> None:
        """从字典加载配置
        
        Args:
            config_dict: 配置字典
        """
        def update_nested(obj, data, prefix=""):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                
                if hasattr(obj, key) and isinstance(value, dict):
                    # 递归更新嵌套对象
                    nested_obj = getattr(obj, key)
                    update_nested(nested_obj, value, full_key)
                elif hasattr(obj, key):
                    # 直接设置属性
                    setattr(obj, key, value)
                    logger.debug(f"设置配置项: {full_key} = {value}")
                else:
                    # 存储到动态配置中
                    if not hasattr(obj, 'dynamic_config'):
                        obj.dynamic_config = {}
                    obj.dynamic_config[full_key] = value
                    logger.debug(f"动态配置项: {full_key} = {value}")
        
        update_nested(self, config_dict)
        logger.info("从字典加载配置完成")
    
    def load_from_file(self, config_path: str) -> None:
        """从配置文件加载配置到当前实例
        
        Args:
            config_path: 配置文件路径，支持YAML和JSON格式
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                if config_path.endswith(('.yml', '.yaml')):
                    config_data = yaml.safe_load(f)
                elif config_path.endswith('.json'):
                    import json
                    config_data = json.load(f)
                else:
                    raise ValueError(f"不支持的配置文件格式: {config_path}")
                    
            logger.info(f"从文件加载配置: {config_path}")
            self.load_from_dict(config_data)
            
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise ValueError(f"配置文件格式错误: {e}")
    
    def load_from_env(self, prefix: str = "LIBERZMQ_") -> None:
        """从环境变量加载配置到当前实例
        
        Args:
            prefix: 环境变量前缀
        """
        config_data = {}
        
        # 扫描环境变量
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # 移除前缀并转换为小写
                config_key = key[len(prefix):].lower()
                
                # 处理嵌套配置 (支持下划线分隔)
                if '_' in config_key:
                    parts = config_key.split('_')
                    current = config_data
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    current[parts[-1]] = self._convert_env_value(value)
                else:
                    config_data[config_key] = self._convert_env_value(value)
        
        logger.info(f"从环境变量加载配置，前缀: {prefix}")
        self.load_from_dict(config_data)
    
    def to_dict(self) -> dict:
        """将配置导出为字典
        
        Returns:
            dict: 配置字典
        """
        result = self.model_dump()
        # 合并动态配置
        if hasattr(self, 'dynamic_config') and self.dynamic_config:
            for key, value in self.dynamic_config.items():
                # 处理点号分隔的键
                parts = key.split('.')
                current = result
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = value
        return result


# 全局配置实例
_global_config: Optional[Config] = None


def get_config() -> Config:
    """获取全局配置实例"""
    global _global_config
    if _global_config is None:
        _global_config = Config()
    return _global_config


def set_config(config: Config) -> None:
    """设置全局配置实例"""
    global _global_config
    _global_config = config
    logger.info("全局配置已更新")


def load_config_from_file(config_path: str) -> None:
    """从文件加载全局配置"""
    config = Config.from_file(config_path)
    set_config(config)


def load_config_from_env(prefix: str = "LIBERZMQ_") -> None:
    """从环境变量加载全局配置"""
    config = Config.from_env(prefix)
    set_config(config)


def load_config(source) -> 'ConfigWrapper':
    """通用配置加载函数
    
    Args:
        source: 配置源，可以是字典或文件路径
        
    Returns:
        ConfigWrapper: 配置包装器对象
    """
    if isinstance(source, dict):
        # 从字典加载
        config = Config(**source)
    elif isinstance(source, str):
        # 从文件加载
        config = Config.from_file(source)
    else:
        raise ValueError(f"不支持的配置源类型: {type(source)}")
    
    return ConfigWrapper(config)


class ConfigWrapper:
    """配置包装器，提供点号访问支持"""
    
    def __init__(self, config: Config):
        self._config = config
    
    def get(self, key: str, default=None):
        """使用点号分隔的键获取配置值
        
        Args:
            key: 配置键，支持点号分隔的嵌套访问，如 'zmq.linger'
            default: 默认值
            
        Returns:
            配置值或默认值
        """
        try:
            parts = key.split('.')
            value = self._config
            
            for part in parts:
                if hasattr(value, part):
                    value = getattr(value, part)
                else:
                    return default
            
            return value
        except Exception:
            return default
    
    def __getattr__(self, name):
        """代理属性访问到内部配置对象"""
        # 首先尝试从标准属性获取
        if hasattr(self._config, name):
            return getattr(self._config, name)
        # 然后尝试从动态配置获取
        elif hasattr(self._config, 'dynamic_config') and name in self._config.dynamic_config:
            return self._config.dynamic_config[name]
        else:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")