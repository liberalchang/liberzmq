#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
基础类模块

定义所有消息模式的基类和通用接口。
"""

import zmq
import threading
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, Union
from ..logging.logger import get_logger
from ..exceptions.base import LiberZMQError, ConnectionError
from .config import get_config

logger = get_logger(__name__)


class BaseZMQNode(ABC):
    """ZeroMQ节点基类
    
    所有ZeroMQ消息模式的基类，提供通用的连接管理和配置功能。
    """
    
    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 5555,
                 socket_type: Optional[int] = None,
                 bind: bool = False,
                 **kwargs):
        """
        初始化ZeroMQ节点
        
        Args:
            host: 主机地址
            port: 端口号
            socket_type: ZeroMQ socket类型
            bind: 是否绑定端口（True为服务端，False为客户端）
            **kwargs: 其他配置参数
        """
        self.host = host
        self.port = port
        self.socket_type = socket_type
        self.bind = bind
        self.address = f"tcp://{host}:{port}"
        
        # 获取全局配置
        self.config = get_config()
        
        # ZeroMQ相关对象
        self.context: Optional[zmq.Context] = None
        self.socket: Optional[zmq.Socket] = None
        
        # 状态管理
        self._connected = False
        self._lock = threading.RLock()
        
        # 应用自定义配置
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        logger.debug(f"初始化ZMQ节点: {self.__class__.__name__}, 地址: {self.address}")
    
    def connect(self) -> None:
        """建立连接"""
        with self._lock:
            if self._connected:
                logger.warning(f"节点已连接: {self.address}")
                return
            
            try:
                # 创建ZeroMQ上下文
                self.context = zmq.Context()
                self.context.setsockopt(zmq.IO_THREADS, self.config.zmq.io_threads)
                self.context.setsockopt(zmq.MAX_SOCKETS, self.config.zmq.max_sockets)
                
                # 创建socket
                if self.socket_type is None:
                    raise ValueError("必须指定socket_type")
                
                self.socket = self.context.socket(self.socket_type)
                
                # 设置socket选项
                self.socket.setsockopt(zmq.LINGER, self.config.zmq.linger)
                self.socket.setsockopt(zmq.RCVTIMEO, self.config.zmq.socket_timeout)
                self.socket.setsockopt(zmq.SNDTIMEO, self.config.zmq.socket_timeout)
                self.socket.setsockopt(zmq.SNDHWM, self.config.zmq.high_water_mark)
                self.socket.setsockopt(zmq.RCVHWM, self.config.zmq.high_water_mark)
                
                # 绑定或连接
                if self.bind:
                    self.socket.bind(self.address)
                    logger.info(f"绑定地址: {self.address}")
                else:
                    self.socket.connect(self.address)
                    logger.info(f"连接地址: {self.address}")
                
                self._connected = True
                self._on_connected()
                
            except Exception as e:
                logger.error(f"连接失败: {e}")
                self.disconnect()
                raise ConnectionError(f"连接失败: {e}")
    
    def disconnect(self) -> None:
        """断开连接"""
        with self._lock:
            if not self._connected:
                return
            
            try:
                self._on_disconnecting()
                
                if self.socket:
                    self.socket.close()
                    self.socket = None
                
                if self.context:
                    self.context.term()
                    self.context = None
                
                self._connected = False
                logger.info(f"断开连接: {self.address}")
                
            except Exception as e:
                logger.error(f"断开连接时发生错误: {e}")
    
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self._connected
    
    def _ensure_connected(self) -> None:
        """确保已连接"""
        if not self._connected:
            self.connect()
    
    def _on_connected(self) -> None:
        """连接建立后的回调"""
        pass
    
    def _on_disconnecting(self) -> None:
        """断开连接前的回调"""
        pass
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
    
    def __del__(self):
        """析构函数"""
        try:
            self.disconnect()
        except:
            pass


class BasePublisher(BaseZMQNode):
    """发布者基类"""
    
    def __init__(self, **kwargs):
        super().__init__(socket_type=zmq.PUB, bind=True, **kwargs)
    
    @abstractmethod
    def publish(self, topic: str, data: Any, **kwargs) -> None:
        """发布消息
        
        Args:
            topic: 主题
            data: 数据
            **kwargs: 其他参数
        """
        pass


class BaseSubscriber(BaseZMQNode):
    """订阅者基类"""
    
    def __init__(self, **kwargs):
        super().__init__(socket_type=zmq.SUB, bind=False, **kwargs)
    
    @abstractmethod
    def subscribe(self, topic: str) -> None:
        """订阅主题
        
        Args:
            topic: 主题
        """
        pass
    
    @abstractmethod
    def unsubscribe(self, topic: str) -> None:
        """取消订阅主题
        
        Args:
            topic: 主题
        """
        pass
    
    @abstractmethod
    def receive(self, timeout: Optional[int] = None) -> Any:
        """接收消息
        
        Args:
            timeout: 超时时间（毫秒）
            
        Returns:
            接收到的消息
        """
        pass


class BaseServer(BaseZMQNode):
    """服务器基类"""
    
    def __init__(self, **kwargs):
        super().__init__(socket_type=zmq.REP, bind=True, **kwargs)
    
    @abstractmethod
    def start(self) -> None:
        """启动服务器"""
        pass
    
    @abstractmethod
    def stop(self) -> None:
        """停止服务器"""
        pass
    
    @abstractmethod
    def handler(self, func):
        """注册请求处理函数
        
        Args:
            func: 处理函数
        """
        pass


class BaseClient(BaseZMQNode):
    """客户端基类"""
    
    def __init__(self, **kwargs):
        super().__init__(socket_type=zmq.REQ, bind=False, **kwargs)
    
    @abstractmethod
    def request(self, data: Any, timeout: Optional[int] = None) -> Any:
        """发送请求
        
        Args:
            data: 请求数据
            timeout: 超时时间（毫秒）
            
        Returns:
            响应数据
        """
        pass


class BasePusher(BaseZMQNode):
    """推送者基类"""
    
    def __init__(self, **kwargs):
        super().__init__(socket_type=zmq.PUSH, bind=True, **kwargs)
    
    @abstractmethod
    def push(self, data: Any, **kwargs) -> None:
        """推送数据
        
        Args:
            data: 数据
            **kwargs: 其他参数
        """
        pass


class BasePuller(BaseZMQNode):
    """拉取者基类"""
    
    def __init__(self, **kwargs):
        super().__init__(socket_type=zmq.PULL, bind=False, **kwargs)
    
    @abstractmethod
    def pull(self, timeout: Optional[int] = None) -> Any:
        """拉取数据
        
        Args:
            timeout: 超时时间（毫秒）
            
        Returns:
            拉取到的数据
        """
        pass