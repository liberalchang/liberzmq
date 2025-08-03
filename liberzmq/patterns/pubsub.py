#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
发布-订阅模式实现

提供Publisher（发布者）和Subscriber（订阅者）类，实现ZeroMQ的发布-订阅消息模式。
"""

import time
import threading
from typing import Any, Optional, List, Callable, Dict, Union
from typeguard import typechecked
import zmq

from ..core.base import BasePublisher, BaseSubscriber
from ..serializers.base import BaseSerializer
from ..serializers.factory import get_serializer
from ..exceptions.base import ConnectionError, MessageError, TimeoutError
from ..logging.logger import get_logger

logger = get_logger(__name__)


class Publisher(BasePublisher):
    """发布者类
    
    用于发布消息到指定主题。支持多种数据格式和序列化方式。
    """
    
    @typechecked
    def __init__(self, 
                 address: str = "tcp://*:5555",
                 serializer: Union[str, BaseSerializer] = "json",
                 bind: bool = True,
                 **kwargs):
        """初始化发布者
        
        Args:
            address: 绑定或连接的地址
            serializer: 序列化器名称或实例
            bind: 是否绑定地址（True）或连接地址（False）
            **kwargs: 其他配置参数
        """
        # 解析地址
        if address.startswith('tcp://'):
            if '://*:' in address:
                # 绑定地址格式: tcp://*:port
                port = int(address.split(':')[-1])
                host = '*'
            elif '://localhost:' in address:
                # 连接地址格式: tcp://localhost:port
                port = int(address.split(':')[-1])
                host = 'localhost'
            else:
                # 其他格式，尝试解析
                parts = address.replace('tcp://', '').split(':')
                host = parts[0] if parts[0] else 'localhost'
                port = int(parts[1]) if len(parts) > 1 else 5555
        else:
            host = 'localhost'
            port = 5555
        
        # 调用BasePublisher初始化，传递解析后的host和port
        # 注意：BasePublisher已经设置了bind=True，所以我们需要在kwargs中处理bind参数
        if not bind:
            kwargs['bind'] = bind
        super().__init__(host=host, port=port, **kwargs)
        
        # 设置序列化器
        if isinstance(serializer, str):
            self._serializer_name = serializer
            self._serializer = get_serializer(serializer)
        else:
            self._serializer_name = getattr(serializer, 'name', 'custom')
            self._serializer = serializer
        
        self._address = address  # 保存原始地址
        self._bind = bind
        self._socket: Optional[zmq.Socket] = None  # 初始化_socket属性
        self._published_count = 0
        self._topics = set()
        
        logger.info(f"Publisher初始化: {address}, 绑定模式: {bind}")
    

    
    def connect(self) -> None:
        """连接到ZeroMQ"""
        if self._connected:
            logger.warning("Publisher已经连接")
            return
        
        try:
            # 调用父类的connect方法，它会处理所有的连接逻辑
            super().connect()
            
            # 设置_socket属性以匹配测试期望
            self._socket = self.socket
            
            # 设置发布者特定的socket选项
            if self.socket:
                # 设置发送高水位标记
                send_hwm = getattr(self, '_config', {}).get('send_hwm', 1000)
                self.socket.setsockopt(zmq.SNDHWM, send_hwm)
                
                # 设置发送超时
                send_timeout = getattr(self, '_config', {}).get('send_timeout', 5000)
                if send_timeout > 0:
                    self.socket.setsockopt(zmq.SNDTIMEO, send_timeout)
            
            # 等待套接字准备就绪
            time.sleep(0.1)
            
        except Exception as e:
            error_msg = f"Publisher连接失败: {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg, address=self._address)
    
    def disconnect(self) -> None:
        """断开连接"""
        try:
            # 调用父类的disconnect方法
            super().disconnect()
        except Exception as e:
            logger.error(f"Publisher断开连接失败: {e}")
        
        # 确保_socket属性为None以匹配测试期望
        self._socket = None
    
    @typechecked
    def publish(self, 
                topic: str, 
                message: Any, 
                **kwargs) -> None:
        """发布消息
        
        Args:
            topic: 消息主题
            message: 消息内容
            **kwargs: 额外的消息属性
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息发送错误
        """
        if not self._connected or self.socket is None:
            raise ConnectionError("Publisher未连接", address=self._address)
        
        try:
            # 序列化消息
            serialized_data = self._serializer.serialize(message, **kwargs)
            
            # 发送多帧消息：主题 + 数据
            self.socket.send_multipart([
                topic.encode('utf-8'),
                serialized_data
            ])
            
            self._published_count += 1
            self._topics.add(topic)
            
            logger.debug(f"发布消息到主题 '{topic}': {len(serialized_data)} 字节")
            
        except zmq.Again:
            error_msg = f"发布消息超时: 主题 '{topic}'"
            logger.error(error_msg)
            raise TimeoutError(error_msg, operation="publish")
        except Exception as e:
            error_msg = f"发布消息失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="publish")
    
    @typechecked
    def publish_multipart(self, 
                         topic: str, 
                         messages: List[Any], 
                         **kwargs) -> None:
        """发布多部分消息
        
        Args:
            topic: 消息主题
            messages: 消息列表
            **kwargs: 额外的消息属性
        """
        if not self._connected or self.socket is None:
            raise ConnectionError("Publisher未连接", address=self._address)
        
        try:
            # 序列化所有消息
            serialized_parts = [topic.encode('utf-8')]
            for msg in messages:
                serialized_data = self._serializer.serialize(msg, **kwargs)
                serialized_parts.append(serialized_data)
            
            # 发送多帧消息
            self.socket.send_multipart(serialized_parts)
            
            self._published_count += 1
            self._topics.add(topic)
            
            total_size = sum(len(part) for part in serialized_parts)
            logger.debug(f"发布多部分消息到主题 '{topic}': {len(messages)} 部分, {total_size} 字节")
            
        except zmq.Again:
            error_msg = f"发布多部分消息超时: 主题 '{topic}'"
            logger.error(error_msg)
            raise TimeoutError(error_msg, operation="publish_multipart")
        except Exception as e:
            error_msg = f"发布多部分消息失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="publish_multipart")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取发布者统计信息
        
        Returns:
            Dict: 统计信息
        """
        return {
            'published_count': self._published_count,
            'topics_count': len(self._topics),
            'topics': list(self._topics),
            'connected': self._connected,
            'address': self._address,
            'bind_mode': self._bind
        }


class Subscriber(BaseSubscriber):
    """订阅者类
    
    用于订阅指定主题的消息。支持多种数据格式和回调处理。
    """
    
    @typechecked
    def __init__(self, 
                 address: str = "tcp://localhost:5555",
                 topics: Optional[List[str]] = None,
                 serializer: Union[str, BaseSerializer] = "json",
                 bind: bool = False,
                 **kwargs):
        """初始化订阅者
        
        Args:
            address: 连接或绑定的地址
            topics: 订阅的主题列表
            serializer: 序列化器名称或实例
            bind: 是否绑定地址（True）或连接地址（False）
            **kwargs: 其他配置参数
        """
        # 解析地址
        if address.startswith('tcp://'):
            if '://*:' in address:
                # 绑定地址格式: tcp://*:port
                port = int(address.split(':')[-1])
                host = '*'
            elif '://localhost:' in address:
                # 连接地址格式: tcp://localhost:port
                port = int(address.split(':')[-1])
                host = 'localhost'
            else:
                # 其他格式，尝试解析
                parts = address.replace('tcp://', '').split(':')
                host = parts[0] if parts[0] else 'localhost'
                port = int(parts[1]) if len(parts) > 1 else 5555
        else:
            host = 'localhost'
            port = 5555
        
        # 调用BaseSubscriber初始化，传递解析后的host和port
        # 注意：BaseSubscriber已经设置了bind=False，所以我们需要在kwargs中处理bind参数
        if bind:
            kwargs['bind'] = bind
        super().__init__(host=host, port=port, **kwargs)
        
        # 设置序列化器
        if isinstance(serializer, str):
            self._serializer_name = serializer
            self._serializer = get_serializer(serializer)
        else:
            self._serializer_name = getattr(serializer, 'name', 'custom')
            self._serializer = serializer
        
        self._address = address  # 保存原始地址
        self._bind = bind
        self._socket: Optional[zmq.Socket] = None  # 初始化_socket属性
        self._topics = set(topics or [])
        self._callbacks: Dict[str, List[Callable]] = {}
        self._received_count = 0
        self._running = False
        self._receive_thread: Optional[threading.Thread] = None
        
        logger.info(f"Subscriber初始化: {address}, 绑定模式: {bind}, 主题: {topics}")
    

    
    def connect(self) -> None:
        """连接到ZMQ套接字"""
        if self._connected:
            logger.warning("Subscriber已经连接")
            return
        
        try:
            # 调用父类的connect方法，它会处理所有的连接逻辑
            super().connect()
            
            # 设置_socket属性以匹配测试期望
            self._socket = self.socket
            
            # 设置订阅者特定的socket选项
            if self.socket:
                # 设置接收高水位标记
                recv_hwm = getattr(self, '_config', {}).get('recv_hwm', 1000)
                self.socket.setsockopt(zmq.RCVHWM, recv_hwm)
                
                # 设置接收超时
                recv_timeout = getattr(self, '_config', {}).get('recv_timeout', 1000)
                if recv_timeout > 0:
                    self.socket.setsockopt(zmq.RCVTIMEO, recv_timeout)
            
                # 订阅主题
                for topic in self._topics:
                    self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
                    logger.debug(f"订阅主题: {topic}")
                
                # 如果没有指定主题，订阅所有消息
                if not self._topics:
                    self.socket.setsockopt(zmq.SUBSCRIBE, b"")
                    logger.debug("订阅所有主题")
            
            # 等待套接字准备就绪
            time.sleep(0.1)
            
        except Exception as e:
            error_msg = f"Subscriber连接失败: {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg, address=self._address)
    
    def disconnect(self) -> None:
        """断开连接"""
        try:
            # 先设置_socket为None，避免引用已关闭的socket对象
            self._socket = None
            
            # 调用父类的disconnect方法
            super().disconnect()
        except Exception as e:
            logger.error(f"Subscriber断开连接失败: {e}")
    
    @typechecked
    def subscribe(self, topic: str, callback: Optional[Callable] = None) -> None:
        """订阅新主题
        
        Args:
            topic: 要订阅的主题
            callback: 可选的回调函数，用于处理该主题的消息
        """
        if topic in self._topics:
            logger.warning(f"主题已订阅: {topic}")
            if callback is not None:
                self.add_callback(topic, callback)
            return
        
        self._topics.add(topic)
        
        if self.socket is not None:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
            logger.info(f"订阅新主题: {topic}")
        
        # 如果提供了回调函数，注册它
        if callback is not None:
            self.add_callback(topic, callback)
    
    @typechecked
    def unsubscribe(self, topic: str) -> None:
        """取消订阅主题
        
        Args:
            topic: 要取消订阅的主题
        """
        if topic not in self._topics:
            logger.warning(f"主题未订阅: {topic}")
            return
        
        self._topics.discard(topic)
        
        if self.socket is not None:
            self.socket.setsockopt_string(zmq.UNSUBSCRIBE, topic)
            logger.info(f"取消订阅主题: {topic}")
    
    @typechecked
    def add_callback(self, topic: str, callback: Callable[[str, Any], None]) -> None:
        """添加消息回调函数
        
        Args:
            topic: 主题名称（支持通配符 '*' 表示所有主题）
            callback: 回调函数，接收 (topic, message) 参数
        """
        if topic not in self._callbacks:
            self._callbacks[topic] = []
        
        self._callbacks[topic].append(callback)
        logger.debug(f"添加回调函数到主题: {topic}")
    
    @typechecked
    def remove_callback(self, topic: str, callback: Callable[[str, Any], None]) -> None:
        """移除消息回调函数
        
        Args:
            topic: 主题名称
            callback: 要移除的回调函数
        """
        if topic in self._callbacks and callback in self._callbacks[topic]:
            self._callbacks[topic].remove(callback)
            if not self._callbacks[topic]:
                del self._callbacks[topic]
            logger.debug(f"移除回调函数从主题: {topic}")
    
    def receive_message(self, timeout: Optional[int] = None) -> Optional[tuple]:
        """接收单条消息
        
        Args:
            timeout: 超时时间（毫秒），None表示使用默认超时
            
        Returns:
            Optional[tuple]: (topic, message) 或 None（超时）
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息接收错误
        """
        if not self._connected or self.socket is None:
            raise ConnectionError("Subscriber未连接", address=self._address)
        
        try:
            # 设置临时超时
            if timeout is not None:
                old_timeout = self.socket.getsockopt(zmq.RCVTIMEO)
                self.socket.setsockopt(zmq.RCVTIMEO, timeout)
            
            try:
                # 接收多帧消息
                frames = self.socket.recv_multipart()
                
                if len(frames) < 2:
                    raise MessageError("消息格式错误：帧数不足")
                
                # 解析主题和消息
                topic = frames[0].decode('utf-8')
                message_data = frames[1]
                
                # 反序列化消息
                message = self._serializer.deserialize(message_data)
                
                self._received_count += 1
                logger.debug(f"接收消息从主题 '{topic}': {len(message_data)} 字节")
                
                return topic, message
                
            finally:
                # 恢复原始超时设置
                if timeout is not None:
                    self.socket.setsockopt(zmq.RCVTIMEO, old_timeout)
            
        except zmq.Again:
            # 超时
            return None
        except Exception as e:
            error_msg = f"接收消息失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="receive")
    
    def _handle_message(self, topic: str, message: Any) -> None:
        """处理接收到的消息
        
        Args:
            topic: 消息主题
            message: 消息内容
        """
        # 调用特定主题的回调
        if topic in self._callbacks:
            for callback in self._callbacks[topic]:
                try:
                    callback(topic, message)
                except Exception as e:
                    logger.error(f"回调函数执行失败: {e}")
        
        # 调用通配符回调
        if '*' in self._callbacks:
            for callback in self._callbacks['*']:
                try:
                    callback(topic, message)
                except Exception as e:
                    logger.error(f"通配符回调函数执行失败: {e}")
    
    def _receive_loop(self) -> None:
        """消息接收循环"""
        logger.info("开始消息接收循环")
        
        while self._running:
            try:
                result = self.receive_message(timeout=1000)
                if result:
                    topic, message = result
                    self._handle_message(topic, message)
            except Exception as e:
                if self._running:
                    logger.error(f"消息接收循环错误: {e}")
                    time.sleep(0.1)
        
        logger.info("消息接收循环结束")
    
    def start_receiving(self) -> None:
        """开始异步接收消息"""
        if self._running:
            logger.warning("消息接收已经在运行")
            return
        
        if not self._connected:
            raise ConnectionError("Subscriber未连接", address=self._address)
        
        self._running = True
        self._receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._receive_thread.start()
        
        logger.info("开始异步接收消息")
    
    def stop_receiving(self) -> None:
        """停止异步接收消息"""
        if not self._running:
            logger.warning("消息接收未在运行")
            return
        
        self._running = False
        
        if self._receive_thread and self._receive_thread.is_alive():
            self._receive_thread.join(timeout=2.0)
        
        logger.info("停止异步接收消息")
    
    def disconnect(self) -> None:
        """断开连接"""
        # 先停止接收
        self.stop_receiving()
        
        # 先设置_socket为None，避免引用已关闭的socket对象
        self._socket = None
        
        # 调用父类断开连接
        super().disconnect()
    
    def receive(self, timeout: Optional[int] = None) -> Any:
        """接收消息
        
        Args:
            timeout: 超时时间（毫秒）
            
        Returns:
            接收到的消息
        """
        return self.receive_message(timeout)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取订阅者统计信息
        
        Returns:
            Dict: 统计信息
        """
        return {
            'received_count': self._received_count,
            'subscribed_topics': list(self._topics),
            'callback_topics': list(self._callbacks.keys()),
            'connected': self._connected,
            'receiving': self._running,
            'address': self._address,
            'bind_mode': self._bind
        }