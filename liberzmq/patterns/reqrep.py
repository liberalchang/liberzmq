#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
请求-响应模式实现

提供Server（服务器）和Client（客户端）类，实现ZeroMQ的请求-响应消息模式。
"""

import time
import threading
import uuid
from typing import Any, Optional, Callable, Dict, Union
from typeguard import typechecked
import zmq

from ..core.base import BaseServer, BaseClient
from ..serializers.base import BaseSerializer
from ..serializers.factory import get_serializer
from ..exceptions.base import ConnectionError, MessageError, TimeoutError
from ..logging.logger import get_logger

logger = get_logger(__name__)


class Server(BaseServer):
    """服务器类
    
    用于处理客户端请求并发送响应。支持多种数据格式和自定义请求处理器。
    """
    
    @typechecked
    def __init__(self, 
                 address: str = "tcp://*:5556",
                 serializer: Union[str, BaseSerializer] = "json",
                 bind: bool = True,
                 **kwargs):
        """初始化服务器
        
        Args:
            address: 绑定或连接的地址
            serializer: 序列化器名称或实例
            bind: 是否绑定地址（True）或连接地址（False）
            **kwargs: 其他配置参数
        """
        # BaseServer已经设置了bind=True，不需要再传递
        super().__init__(address=address, serializer=serializer, **kwargs)
        self._address = self.address  # 设置_address属性供pattern类使用
        self._bind = bind
        self._socket: Optional[zmq.Socket] = None  # 初始化_socket属性
        self._context = zmq.Context()
        self._config = kwargs  # 保存配置
        self._request_handlers: Dict[str, Callable] = {}
        self._default_handler: Optional[Callable] = None
        self._processed_count = 0
        self._running = False
        self._server_thread: Optional[threading.Thread] = None
        
        # 设置序列化器属性
        if isinstance(serializer, str):
            self._serializer = get_serializer(serializer)
        else:
            self._serializer = serializer
        
        logger.info(f"Server初始化: {address}, 绑定模式: {bind}")
    
    def _create_socket(self) -> zmq.Socket:
        """创建ZeroMQ套接字"""
        socket = self._context.socket(zmq.REP)
        
        # 设置套接字选项
        socket.setsockopt(zmq.LINGER, self._config.get('linger', 1000))
        socket.setsockopt(zmq.RCVHWM, self._config.get('recv_hwm', 1000))
        socket.setsockopt(zmq.SNDHWM, self._config.get('send_hwm', 1000))
        
        # 设置超时
        recv_timeout = self._config.get('recv_timeout', 5000)
        send_timeout = self._config.get('send_timeout', 5000)
        
        if recv_timeout > 0:
            socket.setsockopt(zmq.RCVTIMEO, recv_timeout)
        if send_timeout > 0:
            socket.setsockopt(zmq.SNDTIMEO, send_timeout)
        
        return socket
    
    def connect(self) -> None:
        """连接到ZeroMQ"""
        if self._socket is not None:
            logger.warning("Server已经连接")
            return
        
        try:
            self._socket = self._create_socket()
            
            if self._bind:
                self._socket.bind(self._address)
                logger.info(f"Server绑定到: {self._address}")
            else:
                self._socket.connect(self._address)
                logger.info(f"Server连接到: {self._address}")
            
            self._connected = True
            
        except Exception as e:
            error_msg = f"Server连接失败: {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg, address=self._address)
    
    @typechecked
    def register_handler(self, 
                        request_type: str, 
                        handler: Callable[[Any], Any]) -> None:
        """注册请求处理器
        
        Args:
            request_type: 请求类型
            handler: 处理函数，接收请求数据，返回响应数据
        """
        self._request_handlers[request_type] = handler
        logger.info(f"注册请求处理器: {request_type}")
    
    @typechecked
    def set_default_handler(self, handler: Callable[[str, Any], Any]) -> None:
        """设置默认请求处理器
        
        Args:
            handler: 默认处理函数，接收 (request_type, request_data)，返回响应数据
        """
        self._default_handler = handler
        logger.info("设置默认请求处理器")
    
    def receive_request(self, timeout: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """接收请求
        
        Args:
            timeout: 超时时间（毫秒），None表示使用默认超时
            
        Returns:
            Optional[Dict]: 请求信息，包含 'type', 'data', 'metadata' 等字段
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息接收错误
        """
        if not self._connected or self._socket is None:
            raise ConnectionError("Server未连接", address=self._address)
        
        try:
            # 设置临时超时
            if timeout is not None:
                old_timeout = self._socket.getsockopt(zmq.RCVTIMEO)
                self._socket.setsockopt(zmq.RCVTIMEO, timeout)
            
            try:
                # 接收请求数据
                request_data = self._socket.recv()
                
                # 反序列化请求
                request = self._serializer.deserialize(request_data)
                
                logger.debug(f"接收请求: {len(request_data)} 字节")
                
                # 确保请求格式正确
                if not isinstance(request, dict):
                    request = {'type': 'unknown', 'data': request}
                
                if 'type' not in request:
                    request['type'] = 'unknown'
                
                return request
                
            finally:
                # 恢复原始超时设置
                if timeout is not None:
                    self._socket.setsockopt(zmq.RCVTIMEO, old_timeout)
            
        except zmq.Again:
            # 超时
            return None
        except Exception as e:
            error_msg = f"接收请求失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="receive_request")
    
    @typechecked
    def send_response(self, response: Any, **kwargs) -> None:
        """发送响应
        
        Args:
            response: 响应数据
            **kwargs: 额外的响应属性
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息发送错误
        """
        if not self._connected or self._socket is None:
            raise ConnectionError("Server未连接", address=self._address)
        
        try:
            # 序列化响应
            response_data = self._serializer.serialize(response, **kwargs)
            
            # 发送响应
            self._socket.send(response_data)
            
            logger.debug(f"发送响应: {len(response_data)} 字节")
            
        except zmq.Again:
            error_msg = "发送响应超时"
            logger.error(error_msg)
            raise TimeoutError(error_msg, operation="send_response")
        except Exception as e:
            error_msg = f"发送响应失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="send_response")
    
    def _process_request(self, request: Dict[str, Any]) -> Any:
        """处理请求
        
        Args:
            request: 请求信息
            
        Returns:
            Any: 响应数据
        """
        request_type = request.get('type', 'unknown')
        request_data = request.get('data')
        
        try:
            # 查找特定处理器
            if request_type in self._request_handlers:
                handler = self._request_handlers[request_type]
                response = handler(request_data)
            elif self._default_handler:
                response = self._default_handler(request_type, request_data)
            else:
                # 默认响应
                response = {
                    'status': 'error',
                    'message': f'未知的请求类型: {request_type}',
                    'request_type': request_type
                }
            
            return response
            
        except Exception as e:
            logger.error(f"处理请求失败: {e}")
            return {
                'status': 'error',
                'message': f'处理请求时发生错误: {str(e)}',
                'request_type': request_type
            }
    
    def _server_loop(self) -> None:
        """服务器主循环"""
        logger.info("开始服务器主循环")
        
        while self._running:
            try:
                # 接收请求
                request = self.receive_request(timeout=1000)
                if request:
                    # 处理请求
                    response = self._process_request(request)
                    
                    # 发送响应
                    self.send_response(response)
                    
                    self._processed_count += 1
                    
            except Exception as e:
                if self._running:
                    logger.error(f"服务器循环错误: {e}")
                    # 发送错误响应
                    try:
                        error_response = {
                            'status': 'error',
                            'message': f'服务器内部错误: {str(e)}'
                        }
                        self.send_response(error_response)
                    except:
                        pass
                    time.sleep(0.1)
        
        logger.info("服务器主循环结束")
    
    def start_server(self) -> None:
        """启动服务器"""
        if self._running:
            logger.warning("服务器已经在运行")
            return
        
        if not self._connected:
            raise ConnectionError("Server未连接", address=self._address)
        
        self._running = True
        self._server_thread = threading.Thread(target=self._server_loop, daemon=True)
        self._server_thread.start()
        
        logger.info("启动服务器")
    
    def stop_server(self) -> None:
        """停止服务器"""
        if not self._running:
            logger.warning("服务器未在运行")
            return
        
        self._running = False
        
        if self._server_thread and self._server_thread.is_alive():
            self._server_thread.join(timeout=2.0)
        
        logger.info("停止服务器")
    
    def start(self) -> None:
        """启动服务器（BaseServer抽象方法实现）"""
        self.start_server()
    
    def stop(self) -> None:
        """停止服务器（BaseServer抽象方法实现）"""
        self.stop_server()
    
    def handler(self, func):
        """注册请求处理函数（BaseServer抽象方法实现）
        
        Args:
            func: 处理函数
        """
        # 这个方法用于装饰器模式，暂时简单实现
        if callable(func):
            self.set_default_handler(func)
        return func
    
    def disconnect(self) -> None:
        """断开连接"""
        # 先停止服务器
        self.stop_server()
        
        # 先设置_socket为None，避免引用已关闭的socket对象
        self._socket = None
        
        # 调用父类断开连接
        super().disconnect()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取服务器统计信息
        
        Returns:
            Dict: 统计信息
        """
        return {
            'processed_count': self._processed_count,
            'registered_handlers': list(self._request_handlers.keys()),
            'has_default_handler': self._default_handler is not None,
            'connected': self._connected,
            'running': self._running,
            'address': self._address,
            'bind_mode': self._bind
        }


class Client(BaseClient):
    """客户端类
    
    用于向服务器发送请求并接收响应。支持同步和异步请求。
    """
    
    @typechecked
    def __init__(self, 
                 address: str = "tcp://localhost:5556",
                 serializer: Union[str, BaseSerializer] = "json",
                 bind: bool = False,
                 **kwargs):
        """初始化客户端
        
        Args:
            address: 连接或绑定的地址
            serializer: 序列化器名称或实例
            bind: 是否绑定地址（True）或连接地址（False）
            **kwargs: 其他配置参数
        """
        # BaseClient已经设置了bind=False，不需要再传递
        super().__init__(address=address, serializer=serializer, **kwargs)
        self._address = self.address  # 设置_address属性供pattern类使用
        self._bind = bind
        self._socket: Optional[zmq.Socket] = None  # 初始化_socket属性
        self._context = zmq.Context()
        self._config = kwargs  # 保存配置
        self._request_count = 0
        self._response_count = 0
        
        # 设置序列化器属性
        if isinstance(serializer, str):
            self._serializer = get_serializer(serializer)
        else:
            self._serializer = serializer
        
        logger.info(f"Client初始化: {address}, 绑定模式: {bind}")
    
    def _create_socket(self) -> zmq.Socket:
        """创建ZeroMQ套接字"""
        socket = self._context.socket(zmq.REQ)
        
        # 设置套接字选项
        socket.setsockopt(zmq.LINGER, self._config.get('linger', 1000))
        socket.setsockopt(zmq.RCVHWM, self._config.get('recv_hwm', 1000))
        socket.setsockopt(zmq.SNDHWM, self._config.get('send_hwm', 1000))
        
        # 设置超时
        recv_timeout = self._config.get('recv_timeout', 10000)
        send_timeout = self._config.get('send_timeout', 5000)
        
        if recv_timeout > 0:
            socket.setsockopt(zmq.RCVTIMEO, recv_timeout)
        if send_timeout > 0:
            socket.setsockopt(zmq.SNDTIMEO, send_timeout)
        
        return socket
    
    def connect(self) -> None:
        """连接到ZeroMQ"""
        if self._socket is not None:
            logger.warning("Client已经连接")
            return
        
        try:
            self._socket = self._create_socket()
            
            if self._bind:
                self._socket.bind(self._address)
                logger.info(f"Client绑定到: {self._address}")
            else:
                self._socket.connect(self._address)
                logger.info(f"Client连接到: {self._address}")
            
            self._connected = True
            
        except Exception as e:
            error_msg = f"Client连接失败: {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg, address=self._address)
    
    @typechecked
    def send_request(self, 
                    request_type: str, 
                    data: Any, 
                    timeout: Optional[int] = None,
                    **kwargs) -> Any:
        """发送请求并等待响应
        
        Args:
            request_type: 请求类型
            data: 请求数据
            timeout: 超时时间（毫秒），None表示使用默认超时
            **kwargs: 额外的请求属性
            
        Returns:
            Any: 响应数据
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息发送/接收错误
            TimeoutError: 超时错误
        """
        if not self._connected or self._socket is None:
            raise ConnectionError("Client未连接", address=self._address)
        
        try:
            # 构造请求
            request = {
                'type': request_type,
                'data': data,
                'timestamp': time.time(),
                'request_id': str(uuid.uuid4()),
                **kwargs
            }
            
            # 序列化请求
            request_data = self._serializer.serialize(request)
            
            # 设置临时超时
            old_recv_timeout = None
            old_send_timeout = None
            
            if timeout is not None:
                old_recv_timeout = self._socket.getsockopt(zmq.RCVTIMEO)
                old_send_timeout = self._socket.getsockopt(zmq.SNDTIMEO)
                self._socket.setsockopt(zmq.RCVTIMEO, timeout)
                self._socket.setsockopt(zmq.SNDTIMEO, timeout)
            
            try:
                # 发送请求
                self._socket.send(request_data)
                self._request_count += 1
                
                logger.debug(f"发送请求 '{request_type}': {len(request_data)} 字节")
                
                # 接收响应
                response_data = self._socket.recv()
                self._response_count += 1
                
                # 反序列化响应
                response = self._serializer.deserialize(response_data)
                
                logger.debug(f"接收响应: {len(response_data)} 字节")
                
                return response
                
            finally:
                # 恢复原始超时设置
                if timeout is not None and old_recv_timeout is not None:
                    self._socket.setsockopt(zmq.RCVTIMEO, old_recv_timeout)
                if timeout is not None and old_send_timeout is not None:
                    self._socket.setsockopt(zmq.SNDTIMEO, old_send_timeout)
            
        except zmq.Again:
            error_msg = f"请求超时: {request_type}"
            logger.error(error_msg)
            raise TimeoutError(error_msg, operation="send_request")
        except Exception as e:
            error_msg = f"发送请求失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="send_request")
    
    @typechecked
    def ping(self, timeout: Optional[int] = None) -> Dict[str, Any]:
        """发送ping请求测试连接
        
        Args:
            timeout: 超时时间（毫秒）
            
        Returns:
            Dict: ping响应信息
        """
        ping_data = {
            'timestamp': time.time(),
            'client_id': id(self)
        }
        
        response = self.send_request('ping', ping_data, timeout=timeout)
        return response
    
    @typechecked
    def call(self, 
             method: str, 
             *args, 
             timeout: Optional[int] = None,
             **kwargs) -> Any:
        """调用远程方法
        
        Args:
            method: 方法名称
            *args: 位置参数
            timeout: 超时时间（毫秒）
            **kwargs: 关键字参数
            
        Returns:
            Any: 方法返回值
        """
        call_data = {
            'method': method,
            'args': args,
            'kwargs': kwargs
        }
        
        response = self.send_request('call', call_data, timeout=timeout)
        
        # 检查响应状态
        if isinstance(response, dict) and response.get('status') == 'error':
            raise MessageError(f"远程方法调用失败: {response.get('message', '未知错误')}")
        
        return response
    
    def request(self, data: Any, timeout: Optional[int] = None) -> Any:
        """发送请求（BaseClient抽象方法实现）
        
        Args:
            data: 请求数据
            timeout: 超时时间（毫秒）
            
        Returns:
            响应数据
        """
        # 如果data是字典且包含type字段，直接使用
        if isinstance(data, dict) and 'type' in data:
            request_type = data['type']
            request_data = data.get('data', data)
        else:
            # 否则使用默认类型
            request_type = 'request'
            request_data = data
        
        return self.send_request(request_type, request_data, timeout=timeout)
    
    def disconnect(self) -> None:
        """断开连接"""
        # 先设置_socket为None，避免引用已关闭的socket对象
        self._socket = None
        
        # 调用父类断开连接
        super().disconnect()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取客户端统计信息
        
        Returns:
            Dict: 统计信息
        """
        return {
            'request_count': self._request_count,
            'response_count': self._response_count,
            'connected': self._connected,
            'address': self._address,
            'bind_mode': self._bind
        }