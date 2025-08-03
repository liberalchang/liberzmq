#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
二进制序列化器模块

实现基于pickle的二进制数据序列化和反序列化。
"""

import pickle
import struct
from typing import Any, Dict, Optional
from .base import BaseSerializer
from ..exceptions.base import SerializationError
from ..logging.logger import get_logger

logger = get_logger(__name__)


class BinarySerializer(BaseSerializer):
    """二进制序列化器
    
    使用pickle进行二进制数据序列化和反序列化。
    支持几乎所有Python对象类型。
    """
    
    def __init__(self, 
                 protocol: int = pickle.HIGHEST_PROTOCOL,
                 fix_imports: bool = True,
                 buffer_callback=None,
                 use_compression: bool = False,
                 compression_level: int = 6,
                 **kwargs):
        """
        初始化二进制序列化器
        
        Args:
            protocol: pickle协议版本
            fix_imports: 是否修复导入问题
            buffer_callback: 缓冲区回调函数
            use_compression: 是否使用压缩
            compression_level: 压缩级别 (1-9)
            **kwargs: 其他配置参数
        """
        super().__init__(**kwargs)
        self.protocol = protocol
        self.fix_imports = fix_imports
        self.buffer_callback = buffer_callback
        self.use_compression = use_compression
        self.compression_level = compression_level
        
        # 如果使用压缩，导入压缩模块
        if self.use_compression:
            try:
                import zlib
                self.compressor = zlib
                logger.debug(f"启用zlib压缩，压缩级别: {compression_level}")
            except ImportError:
                logger.warning("zlib模块不可用，禁用压缩")
                self.use_compression = False
                self.compressor = None
        else:
            self.compressor = None
        
        logger.debug(f"二进制序列化器初始化完成，协议版本: {protocol}，压缩: {use_compression}")
    
    @property
    def format_name(self) -> str:
        """序列化格式名称"""
        return "binary"
    
    @property
    def content_type(self) -> str:
        """内容类型"""
        return "application/octet-stream"
    
    def serialize(self, data: Any) -> bytes:
        """序列化数据为二进制格式
        
        Args:
            data: 要序列化的数据
            
        Returns:
            bytes: 二进制格式的字节数据
            
        Raises:
            SerializationError: 序列化失败时抛出
        """
        try:
            # 使用pickle序列化
            pickle_kwargs = {
                "protocol": self.protocol,
                "fix_imports": self.fix_imports
            }
            
            if self.buffer_callback is not None:
                pickle_kwargs["buffer_callback"] = self.buffer_callback
            
            pickled_data = pickle.dumps(data, **pickle_kwargs)
            
            # 如果启用压缩，压缩数据
            if self.use_compression and self.compressor:
                compressed_data = self.compressor.compress(pickled_data, self.compression_level)
                
                # 添加压缩标志和原始大小信息
                header = struct.pack("!BI", 1, len(pickled_data))  # 1表示压缩，原始大小
                result = header + compressed_data
                
                logger.debug(f"二进制序列化成功（压缩），原始大小: {len(pickled_data)} 字节，压缩后: {len(compressed_data)} 字节")
            else:
                # 添加未压缩标志
                header = struct.pack("!BI", 0, len(pickled_data))  # 0表示未压缩
                result = header + pickled_data
                
                logger.debug(f"二进制序列化成功（未压缩），数据大小: {len(pickled_data)} 字节")
            
            return result
            
        except (pickle.PickleError, struct.error) as e:
            error_msg = f"二进制序列化失败: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="binary")
        except Exception as e:
            error_msg = f"二进制序列化时发生未知错误: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="binary")
    
    def deserialize(self, data: bytes) -> Any:
        """从二进制格式反序列化数据
        
        Args:
            data: 二进制格式的字节数据
            
        Returns:
            Any: 反序列化后的数据
            
        Raises:
            SerializationError: 反序列化失败时抛出
        """
        try:
            # 检查数据长度
            if len(data) < 5:  # 至少需要5字节的头部
                raise SerializationError("二进制数据太短，无法解析头部")
            
            # 解析头部
            is_compressed, original_size = struct.unpack("!BI", data[:5])
            payload = data[5:]
            
            # 如果数据被压缩，先解压缩
            if is_compressed:
                if not self.use_compression or not self.compressor:
                    raise SerializationError("数据被压缩但序列化器未启用压缩支持")
                
                try:
                    pickled_data = self.compressor.decompress(payload)
                    
                    # 验证解压缩后的大小
                    if len(pickled_data) != original_size:
                        logger.warning(f"解压缩后大小不匹配，期望: {original_size}，实际: {len(pickled_data)}")
                    
                    logger.debug(f"二进制数据解压缩成功，压缩大小: {len(payload)} 字节，原始大小: {len(pickled_data)} 字节")
                    
                except Exception as e:
                    raise SerializationError(f"数据解压缩失败: {str(e)}")
            else:
                pickled_data = payload
                logger.debug(f"二进制数据未压缩，大小: {len(pickled_data)} 字节")
            
            # 使用pickle反序列化
            pickle_kwargs = {
                "fix_imports": self.fix_imports
            }
            
            result = pickle.loads(pickled_data, **pickle_kwargs)
            
            logger.debug(f"二进制反序列化成功，数据类型: {type(result)}")
            return result
            
        except (pickle.PickleError, struct.error) as e:
            error_msg = f"二进制反序列化失败: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="binary")
        except SerializationError:
            # 重新抛出SerializationError
            raise
        except Exception as e:
            error_msg = f"二进制反序列化时发生未知错误: {str(e)}"
            logger.error(error_msg)
            raise SerializationError(error_msg, serializer="binary")
    
    def validate_data(self, data: Any) -> bool:
        """验证数据是否可以被pickle序列化
        
        Args:
            data: 要验证的数据
            
        Returns:
            bool: 数据是否有效
        """
        try:
            pickle.dumps(data, protocol=self.protocol, fix_imports=self.fix_imports)
            return True
        except (pickle.PickleError, TypeError) as e:
            logger.debug(f"二进制数据验证失败: {e}")
            return False
        except Exception as e:
            logger.warning(f"二进制数据验证时发生未知错误: {e}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """获取序列化器元数据"""
        metadata = super().get_metadata()
        metadata.update({
            "protocol": self.protocol,
            "fix_imports": self.fix_imports,
            "use_compression": self.use_compression,
            "compression_level": self.compression_level if self.use_compression else None,
            "compressor": self.compressor.__name__ if self.compressor else None
        })
        return metadata
    
    def set_compression(self, enabled: bool, level: int = 6) -> None:
        """设置压缩选项
        
        Args:
            enabled: 是否启用压缩
            level: 压缩级别 (1-9)
        """
        if enabled:
            try:
                import zlib
                self.compressor = zlib
                self.use_compression = True
                self.compression_level = max(1, min(9, level))
                logger.info(f"启用压缩，级别: {self.compression_level}")
            except ImportError:
                logger.error("无法启用压缩，zlib模块不可用")
                self.use_compression = False
                self.compressor = None
        else:
            self.use_compression = False
            self.compressor = None
            logger.info("禁用压缩")
    
    def get_compression_ratio(self, data: Any) -> Optional[float]:
        """获取数据的压缩比
        
        Args:
            data: 要测试的数据
            
        Returns:
            Optional[float]: 压缩比（压缩后大小/原始大小），如果不支持压缩则返回None
        """
        if not self.use_compression or not self.compressor:
            return None
        
        try:
            pickled_data = pickle.dumps(data, protocol=self.protocol, fix_imports=self.fix_imports)
            compressed_data = self.compressor.compress(pickled_data, self.compression_level)
            
            ratio = len(compressed_data) / len(pickled_data)
            logger.debug(f"压缩比: {ratio:.3f} (原始: {len(pickled_data)} 字节，压缩: {len(compressed_data)} 字节)")
            
            return ratio
        except Exception as e:
            logger.error(f"计算压缩比失败: {e}")
            return None
    
    def estimate_size(self, data: Any) -> int:
        """估算序列化后的数据大小
        
        Args:
            data: 要估算的数据
            
        Returns:
            int: 估算的字节大小
        """
        try:
            pickled_data = pickle.dumps(data, protocol=self.protocol, fix_imports=self.fix_imports)
            
            if self.use_compression and self.compressor:
                compressed_data = self.compressor.compress(pickled_data, self.compression_level)
                return len(compressed_data) + 5  # 加上头部大小
            else:
                return len(pickled_data) + 5  # 加上头部大小
        except Exception as e:
            logger.error(f"估算数据大小失败: {e}")
            return 0