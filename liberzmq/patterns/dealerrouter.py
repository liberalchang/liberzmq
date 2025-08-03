#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Dealer-Router模式实现

提供Dealer（经销商）和Router（路由器）类，实现ZeroMQ的Dealer-Router消息模式。
这种模式适用于异步请求-响应、负载均衡和高级路由场景。
"""

import time
import threading
import socket
import uuid
from typing import Any, Optional, List, Callable, Dict, Union, Tuple
from typeguard import typechecked
import zmq

from ..core.base import BaseZMQNode
from ..serializers.base import BaseSerializer
from ..serializers.factory import get_serializer
from ..exceptions.base import ConnectionError, MessageError, TimeoutError
from ..logging.logger import get_logger

logger = get_logger(__name__)


class Dealer(BaseZMQNode):
    """Dealer类
    
    用于异步发送请求和接收响应。支持多个并发请求。
    """
    
    @typechecked
    def __init__(self, 
                 address: str = "tcp://localhost:5558",
                 serializer: Union[str, BaseSerializer] = "json",
                 bind: bool = False,
                 identity: Optional[str] = None,
                 **kwargs):
        """初始化Dealer
        
        Args:
            address: 连接或绑定的地址
            serializer: 序列化器名称或实例
            bind: 是否绑定地址（True）或连接地址（False）
            identity: Dealer身份标识（可选）
            **kwargs: 其他配置参数
        """
        # 解析地址
        if address.startswith('tcp://'):
            parts = address.replace('tcp://', '').split(':')
            host = parts[0] if parts[0] != '*' else 'localhost'
            port = int(parts[1]) if len(parts) > 1 else 5558
        else:
            host = 'localhost'
            port = 5558
        
        super().__init__(host=host, port=port, socket_type=zmq.DEALER, bind=bind, **kwargs)
        
        # 获取序列化器
        if isinstance(serializer, str):
            self.serializer = get_serializer(serializer)
        else:
            self.serializer = serializer
        
        self.identity = identity or f"dealer-{uuid.uuid4().hex[:8]}"
        self._pending_requests = {}  # 存储待处理的请求
        self._request_counter = 0
        self._request_lock = threading.Lock()
        
        logger.info(f"初始化Dealer，地址: {self.address}, 身份: {self.identity}")
    
    def connect(self) -> None:
        """连接到地址 - 重写以在连接前设置身份标识"""
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
                
                # 在设置其他选项前先设置身份标识
                self.socket.setsockopt(zmq.IDENTITY, self.identity.encode('utf-8'))
                logger.debug(f"Dealer设置身份标识: {self.identity}")
                
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
    
    @typechecked
    def send_request(self, 
                    data: Any, 
                    timeout: Optional[int] = None,
                    callback: Optional[Callable] = None) -> str:
        """发送异步请求
        
        Args:
            data: 要发送的数据
            timeout: 超时时间（毫秒）
            callback: 响应回调函数
            
        Returns:
            str: 请求ID
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息发送错误
        """
        if not self.is_connected():
            raise ConnectionError("Dealer未连接")
        
        try:
            with self._request_lock:
                self._request_counter += 1
                request_id = f"{self.identity}-{self._request_counter}"
            
            # 序列化数据
            serialized_data = self.serializer.serialize(data)
            
            # 发送消息（包含请求ID）
            self.socket.send_multipart([
                request_id.encode('utf-8'),
                serialized_data
            ])
            
            # 记录待处理请求
            with self._request_lock:
                self._pending_requests[request_id] = {
                    'timestamp': time.time(),
                    'timeout': timeout,
                    'callback': callback,
                    'data': data
                }
            
            logger.debug(f"Dealer发送请求: {request_id}")
            return request_id
            
        except Exception as e:
            logger.error(f"发送请求失败: {e}")
            raise MessageError(f"发送请求失败: {e}")
    
    @typechecked
    def receive_response(self, timeout: Optional[int] = None) -> Optional[Tuple[str, Any]]:
        """接收响应
        
        Args:
            timeout: 超时时间（毫秒）
            
        Returns:
            Tuple[str, Any]: (请求ID, 响应数据) 或 None
            
        Raises:
            TimeoutError: 接收超时
            MessageError: 消息接收错误
        """
        if not self.is_connected():
            raise ConnectionError("Dealer未连接")
        
        try:
            # 设置接收超时
            if timeout is not None:
                self.socket.setsockopt(zmq.RCVTIMEO, timeout)
            
            # 接收多部分消息
            parts = self.socket.recv_multipart(zmq.NOBLOCK if timeout == 0 else 0)
            
            if len(parts) < 2:
                raise MessageError("接收到的消息格式不正确")
            
            request_id = parts[0].decode('utf-8')
            response_data = self.serializer.deserialize(parts[1])
            
            # 处理响应
            with self._request_lock:
                if request_id in self._pending_requests:
                    request_info = self._pending_requests.pop(request_id)
                    
                    # 调用回调函数
                    if request_info.get('callback'):
                        try:
                            request_info['callback'](response_data)
                        except Exception as e:
                            logger.error(f"回调函数执行失败: {e}")
            
            logger.debug(f"Dealer接收响应: {request_id}")
            return request_id, response_data
            
        except zmq.Again:
            if timeout == 0:
                return None
            raise TimeoutError("接收响应超时")
        except Exception as e:
            logger.error(f"接收响应失败: {e}")
            raise MessageError(f"接收响应失败: {e}")
    
    def get_pending_requests(self) -> Dict[str, Dict]:
        """获取待处理请求列表
        
        Returns:
            Dict: 待处理请求信息
        """
        with self._request_lock:
            return self._pending_requests.copy()
    
    def cleanup_expired_requests(self) -> int:
        """清理过期请求
        
        Returns:
            int: 清理的请求数量
        """
        current_time = time.time()
        expired_count = 0
        
        with self._request_lock:
            expired_requests = []
            
            for request_id, request_info in self._pending_requests.items():
                timeout = request_info.get('timeout')
                if timeout and (current_time - request_info['timestamp']) > (timeout / 1000):
                    expired_requests.append(request_id)
            
            for request_id in expired_requests:
                self._pending_requests.pop(request_id, None)
                expired_count += 1
                logger.warning(f"请求超时被清理: {request_id}")
        
        return expired_count


class Router(BaseZMQNode):
    """Router类
    
    用于接收来自多个Dealer的请求并路由响应。支持负载均衡和请求路由。
    """
    
    @typechecked
    def __init__(self, 
                 address: str = "tcp://*:5558",
                 serializer: Union[str, BaseSerializer] = "json",
                 bind: bool = True,
                 **kwargs):
        """初始化Router
        
        Args:
            address: 绑定或连接的地址
            serializer: 序列化器名称或实例
            bind: 是否绑定地址（True）或连接地址（False）
            **kwargs: 其他配置参数
        """
        # 解析地址
        if address.startswith('tcp://'):
            parts = address.replace('tcp://', '').split(':')
            host = parts[0] if parts[0] != '*' else '0.0.0.0'
            port = int(parts[1]) if len(parts) > 1 else 5558
        else:
            host = '0.0.0.0'
            port = 5558
        
        super().__init__(host=host, port=port, socket_type=zmq.ROUTER, bind=bind, **kwargs)
        
        # 获取序列化器
        if isinstance(serializer, str):
            self.serializer = get_serializer(serializer)
        else:
            self.serializer = serializer
        
        self._client_stats = {}  # 客户端统计信息
        self._stats_lock = threading.Lock()
        
        logger.info(f"初始化Router，地址: {self.address}")
    

    
    @typechecked
    def receive_request(self, timeout: Optional[int] = None) -> Optional[Tuple[str, str, Any]]:
        """接收请求
        
        Args:
            timeout: 超时时间（毫秒）
            
        Returns:
            Tuple[str, str, Any]: (客户端ID, 请求ID, 请求数据) 或 None
            
        Raises:
            TimeoutError: 接收超时
            MessageError: 消息接收错误
        """
        if not self.is_connected():
            raise ConnectionError("Router未连接")
        
        try:
            # 设置接收超时
            if timeout is not None:
                self.socket.setsockopt(zmq.RCVTIMEO, timeout)
            
            # 接收多部分消息
            parts = self.socket.recv_multipart(zmq.NOBLOCK if timeout == 0 else 0)
            
            if len(parts) < 3:
                raise MessageError("接收到的消息格式不正确")
            
            # Router自动添加客户端身份作为第一部分
            client_id = parts[0].decode('utf-8')
            request_id = parts[1].decode('utf-8')
            request_data = self.serializer.deserialize(parts[2])
            
            # 更新客户端统计
            with self._stats_lock:
                if client_id not in self._client_stats:
                    self._client_stats[client_id] = {
                        'request_count': 0,
                        'last_request': None,
                        'first_seen': time.time()
                    }
                
                self._client_stats[client_id]['request_count'] += 1
                self._client_stats[client_id]['last_request'] = time.time()
            
            logger.debug(f"Router接收请求: {client_id} -> {request_id}")
            return client_id, request_id, request_data
            
        except zmq.Again:
            if timeout == 0:
                return None
            raise TimeoutError("接收请求超时")
        except Exception as e:
            logger.error(f"接收请求失败: {e}")
            raise MessageError(f"接收请求失败: {e}")
    
    @typechecked
    def send_response(self, 
                     client_id: str, 
                     request_id: str, 
                     data: Any) -> None:
        """发送响应
        
        Args:
            client_id: 客户端ID
            request_id: 请求ID
            data: 响应数据
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息发送错误
        """
        if not self.is_connected():
            raise ConnectionError("Router未连接")
        
        try:
            # 序列化数据
            serialized_data = self.serializer.serialize(data)
            
            # 发送响应
            client_id_bytes = client_id.encode('utf-8')
            
            self.socket.send_multipart([
                client_id_bytes,
                request_id.encode('utf-8'),
                serialized_data
            ])
            
            logger.debug(f"Router发送响应: {client_id} <- {request_id}")
            
        except Exception as e:
            logger.error(f"发送响应失败: {e}")
            raise MessageError(f"发送响应失败: {e}")
    
    def get_client_stats(self) -> Dict[str, Dict]:
        """获取客户端统计信息
        
        Returns:
            Dict: 客户端统计信息
        """
        with self._stats_lock:
            return self._client_stats.copy()
    
    def get_active_clients(self, timeout_seconds: int = 300) -> List[str]:
        """获取活跃客户端列表
        
        Args:
            timeout_seconds: 超时时间（秒）
            
        Returns:
            List[str]: 活跃客户端ID列表
        """
        current_time = time.time()
        active_clients = []
        
        with self._stats_lock:
            for client_id, stats in self._client_stats.items():
                if stats['last_request'] and (current_time - stats['last_request']) < timeout_seconds:
                    active_clients.append(client_id)
        
        return active_clients
    
    def cleanup_inactive_clients(self, timeout_seconds: int = 3600) -> int:
        """清理不活跃客户端
        
        Args:
            timeout_seconds: 超时时间（秒）
            
        Returns:
            int: 清理的客户端数量
        """
        current_time = time.time()
        inactive_clients = []
        
        with self._stats_lock:
            for client_id, stats in self._client_stats.items():
                if stats['last_request'] and (current_time - stats['last_request']) > timeout_seconds:
                    inactive_clients.append(client_id)
            
            for client_id in inactive_clients:
                self._client_stats.pop(client_id, None)
                logger.info(f"清理不活跃客户端: {client_id}")
        
        return len(inactive_clients)


# 便利函数
def create_dealer_router_pair(dealer_address: str = "tcp://localhost:5558",
                             router_address: str = "tcp://*:5558",
                             serializer: str = "json") -> Tuple[Dealer, Router]:
    """创建Dealer-Router对
    
    Args:
        dealer_address: Dealer连接地址
        router_address: Router绑定地址
        serializer: 序列化器类型
        
    Returns:
        Tuple[Dealer, Router]: Dealer和Router实例
    """
    dealer = Dealer(address=dealer_address, serializer=serializer, bind=False)
    router = Router(address=router_address, serializer=serializer, bind=True)
    
    return dealer, router