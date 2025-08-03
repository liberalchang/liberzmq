#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
推送-拉取模式实现

提供Pusher（推送者）和Puller（拉取者）类，实现ZeroMQ的推送-拉取消息模式。
这种模式适用于分布式任务处理和负载均衡场景。
"""

import time
import threading
import socket
from typing import Any, Optional, List, Callable, Dict, Union
from typeguard import typechecked
import zmq

from ..core.base import BasePusher, BasePuller
from ..serializers.base import BaseSerializer
from ..serializers.factory import get_serializer
from ..exceptions.base import ConnectionError, MessageError, TimeoutError
from ..logging.logger import get_logger

logger = get_logger(__name__)


class Pusher(BasePusher):
    """推送者类
    
    用于推送任务到工作者队列。支持负载均衡和任务分发。
    """
    
    @typechecked
    def __init__(self, 
                 address: str = "tcp://*:5557",
                 serializer: Union[str, BaseSerializer] = "json",
                 bind: bool = True,
                 include_host_info: bool = True,
                 custom_host_info: Optional[Dict[str, Any]] = None,
                 default_task_type: str = "default",
                 **kwargs):
        """初始化推送者
        
        Args:
            address: 绑定或连接的地址
            serializer: 序列化器名称或实例
            bind: 是否绑定地址（True）或连接地址（False）
            include_host_info: 是否包含主机信息（默认True）
            custom_host_info: 自定义主机信息字典
            default_task_type: 默认任务类型（默认"default"）
            **kwargs: 其他配置参数
        """
        super().__init__(address=address, serializer=serializer, **kwargs)
        self._address = self.address  # 设置_address属性供pattern类使用
        self._bind = bind
        self._socket: Optional[zmq.Socket] = None  # 初始化_socket属性
        self._context = zmq.Context()
        self._config = kwargs  # 保存配置
        self._pushed_count = 0
        self._sent_count = 0  # 添加_sent_count属性
        self._task_types = set()
        
        # 包装配置
        self._include_host_info = include_host_info
        self._custom_host_info = custom_host_info or {}
        self._default_task_type = default_task_type
        
        # 设置序列化器
        if isinstance(serializer, str):
            self._serializer_name = serializer
            self._serializer = get_serializer(serializer)
        else:
            self._serializer_name = getattr(serializer, 'name', 'custom')
            self._serializer = serializer
        
        logger.info(f"Pusher初始化: {address}, 绑定模式: {bind}")
    
    def push(self, data: Any, **kwargs) -> None:
        """推送数据（实现抽象方法）
        
        Args:
            data: 要推送的数据
            **kwargs: 其他参数
        """
        self.push_simple(data)
    
    def _create_socket(self) -> zmq.Socket:
        """创建ZeroMQ套接字"""
        socket = self._context.socket(zmq.PUSH)
        
        # 设置套接字选项
        socket.setsockopt(zmq.LINGER, self._config.get('linger', 1000))
        socket.setsockopt(zmq.SNDHWM, self._config.get('send_hwm', 1000))
        
        # 设置发送超时
        send_timeout = self._config.get('send_timeout', 5000)
        if send_timeout > 0:
            socket.setsockopt(zmq.SNDTIMEO, send_timeout)
        
        return socket
    
    def connect(self) -> None:
        """连接到ZeroMQ"""
        if self._socket is not None:
            logger.warning("Pusher已经连接")
            return
        
        try:
            self._socket = self._create_socket()
            
            if self._bind:
                self._socket.bind(self._address)
                logger.info(f"Pusher绑定到: {self._address}")
            else:
                self._socket.connect(self._address)
                logger.info(f"Pusher连接到: {self._address}")
            
            self._connected = True
            
            # 等待套接字准备就绪
            time.sleep(0.1)
            
        except Exception as e:
            error_msg = f"Pusher连接失败: {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg, address=self._address)
    
    @typechecked
    def push_task(self, 
                  task_type_or_data: Any, 
                  task_data: Any = None, 
                  priority: int = 0,
                  **kwargs) -> None:
        """推送任务
        
        Args:
            task_type_or_data: 任务类型（如果提供task_data）或任务数据（如果task_data为None）
            task_data: 任务数据（可选）
            priority: 任务优先级（数值越大优先级越高）
            **kwargs: 额外的任务属性
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息发送错误
        """
        if not self._connected or self._socket is None:
            raise ConnectionError("Pusher未连接", address=self._address)
        
        try:
            # 处理参数：如果task_data为None，则task_type_or_data就是任务数据
            if task_data is None:
                # 单参数模式：直接发送数据
                actual_task_data = task_type_or_data
                task_type = self._default_task_type
            else:
                # 双参数模式：第一个参数是任务类型，第二个是数据
                task_type = str(task_type_or_data)
                actual_task_data = task_data
            
            # 构造任务消息
            task = {
                'type': task_type,
                'data': actual_task_data,
                'priority': priority,
                'timestamp': time.time(),
                'task_id': f"{task_type}_{int(time.time() * 1000000)}_{self._pushed_count}",
                **kwargs
            }
            
            # 添加主机信息
            if self._include_host_info:
                # 基本主机信息
                host_info = {
                    'hostname': socket.gethostname(),
                    'pusher_address': self._address,
                    'bind_mode': self._bind
                }
                
                # 合并自定义主机信息
                if self._custom_host_info:
                    host_info.update(self._custom_host_info)
                
                task['host_info'] = host_info
            
            # 序列化任务
            task_data_serialized = self._serializer.serialize(task)
            
            # 发送任务
            self._socket.send(task_data_serialized)
            
            self._pushed_count += 1
            self._sent_count += 1  # 更新sent_count
            self._task_types.add(task_type)
            
            logger.debug(f"推送任务 '{task_type}': {len(task_data_serialized)} 字节")
            
        except zmq.Again:
            error_msg = f"推送任务超时: {task_type}"
            logger.error(error_msg)
            raise TimeoutError(error_msg, operation="push_task")
        except Exception as e:
            error_msg = f"推送任务失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="push_task")
    
    @typechecked
    def push_batch(self, tasks: List[Dict[str, Any]]) -> None:
        """批量推送任务
        
        Args:
            tasks: 任务列表，每个任务包含 'type', 'data' 等字段
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息发送错误
        """
        if not self._connected or self._socket is None:
            raise ConnectionError("Pusher未连接", address=self._address)
        
        for i, task in enumerate(tasks):
            try:
                task_type = task.get('type', 'unknown')
                task_data = task.get('data')
                priority = task.get('priority', 0)
                
                # 移除已处理的字段，其余作为额外属性
                extra_attrs = {k: v for k, v in task.items() 
                             if k not in ['type', 'data', 'priority']}
                
                self.push_task(task_type, task_data, priority, **extra_attrs)
                
            except Exception as e:
                logger.error(f"批量推送第 {i+1} 个任务失败: {e}")
                raise
        
        logger.info(f"批量推送完成: {len(tasks)} 个任务")
    
    @typechecked
    def push_simple(self, data: Any) -> None:
        """推送简单数据（不包装为任务格式）
        
        Args:
            data: 要推送的数据
        """
        if not self._connected or self._socket is None:
            raise ConnectionError("Pusher未连接", address=self._address)
        
        try:
            # 直接序列化数据
            serialized_data = self._serializer.serialize(data)
            
            # 发送数据
            self._socket.send(serialized_data)
            
            self._pushed_count += 1
            
            logger.debug(f"推送简单数据: {len(serialized_data)} 字节")
            
        except zmq.Again:
            error_msg = "推送简单数据超时"
            logger.error(error_msg)
            raise TimeoutError(error_msg, operation="push_simple")
        except Exception as e:
            error_msg = f"推送简单数据失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="push_simple")
    
    def disconnect(self) -> None:
        """断开连接"""
        # 先设置_socket为None，避免引用已关闭的socket对象
        self._socket = None
        
        # 调用父类断开连接
        super().disconnect()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取推送者统计信息
        
        Returns:
            Dict: 统计信息
        """
        return {
            'pushed_count': self._pushed_count,
            'sent_count': self._sent_count,  # 添加sent_count字段
            'task_types_count': len(self._task_types),
            'task_types': list(self._task_types),
            'connected': self._connected,
            'address': self._address,
            'bind_mode': self._bind
        }


class Puller(BasePuller):
    """拉取者类
    
    用于从任务队列中拉取任务进行处理。支持多种任务处理器和异步处理。
    """
    
    @typechecked
    def __init__(self, 
                 address: str = "tcp://localhost:5557",
                 serializer: Union[str, BaseSerializer] = "json",
                 bind: bool = False,
                 **kwargs):
        """初始化拉取者
        
        Args:
            address: 连接或绑定的地址
            serializer: 序列化器名称或实例
            bind: 是否绑定地址（True）或连接地址（False）
            **kwargs: 其他配置参数
        """
        super().__init__(address=address, serializer=serializer, **kwargs)
        self._address = self.address  # 设置_address属性供pattern类使用
        self._bind = bind
        self._socket: Optional[zmq.Socket] = None  # 初始化_socket属性
        self._context = zmq.Context()
        self._config = kwargs  # 保存配置
        self._task_handlers: Dict[str, Callable] = {}
        self._default_handler: Optional[Callable] = None
        self._pulled_count = 0
        self._received_count = 0  # 添加_received_count属性
        self._processed_count = 0
        self._failed_count = 0
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # 设置序列化器
        if isinstance(serializer, str):
            self._serializer_name = serializer
            self._serializer = get_serializer(serializer)
        else:
            self._serializer_name = getattr(serializer, 'name', 'custom')
            self._serializer = serializer
        
        logger.info(f"Puller初始化: {address}, 绑定模式: {bind}")
    
    def pull(self, timeout: Optional[int] = None) -> Any:
        """拉取数据（实现抽象方法）
        
        Args:
            timeout: 超时时间（毫秒）
            
        Returns:
            拉取到的数据
        """
        return self.pull_simple(timeout)
    
    def _create_socket(self) -> zmq.Socket:
        """创建ZeroMQ套接字"""
        socket = self._context.socket(zmq.PULL)
        
        # 设置套接字选项
        socket.setsockopt(zmq.LINGER, self._config.get('linger', 1000))
        socket.setsockopt(zmq.RCVHWM, self._config.get('recv_hwm', 1000))
        
        # 设置接收超时
        recv_timeout = self._config.get('recv_timeout', 1000)
        if recv_timeout > 0:
            socket.setsockopt(zmq.RCVTIMEO, recv_timeout)
        
        return socket
    
    def connect(self) -> None:
        """连接到ZeroMQ"""
        if self._socket is not None:
            logger.warning("Puller已经连接")
            return
        
        try:
            self._socket = self._create_socket()
            
            if self._bind:
                self._socket.bind(self._address)
                logger.info(f"Puller绑定到: {self._address}")
            else:
                self._socket.connect(self._address)
                logger.info(f"Puller连接到: {self._address}")
            
            self._connected = True
            
            # 等待套接字准备就绪
            time.sleep(0.1)
            
        except Exception as e:
            error_msg = f"Puller连接失败: {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg, address=self._address)
    
    @typechecked
    def register_handler(self, 
                        task_type: str, 
                        handler: Callable[[Any], Any]) -> None:
        """注册任务处理器
        
        Args:
            task_type: 任务类型
            handler: 处理函数，接收任务数据，返回处理结果
        """
        self._task_handlers[task_type] = handler
        logger.info(f"注册任务处理器: {task_type}")
    
    @typechecked
    def register_task_handler(self, 
                             task_type: str, 
                             handler: Callable[[Any], Any]) -> None:
        """注册任务处理器（别名方法，兼容测试代码和文档）
        
        Args:
            task_type: 任务类型
            handler: 处理函数，接收任务数据，返回处理结果
        """
        self.register_handler(task_type, handler)
    
    @typechecked
    def set_default_handler(self, handler: Callable[[str, Any], Any]) -> None:
        """设置默认任务处理器
        
        Args:
            handler: 默认处理函数，接收 (task_type, task_data)，返回处理结果
        """
        self._default_handler = handler
        logger.info("设置默认任务处理器")
    
    @typechecked
    def set_default_task_handler(self, handler: Callable[[Any], Any]) -> None:
        """设置默认任务处理器（别名方法，兼容测试代码）
        
        Args:
            handler: 默认处理函数，接收任务数据，返回处理结果
        """
        # 包装处理器以匹配原始接口
        def wrapped_handler(task_type: str, task_data: Any) -> Any:
            return handler(task_data)
        
        self._default_handler = wrapped_handler
        self._default_task_handler = handler  # 保存原始处理器供测试使用
        logger.info("设置默认任务处理器（兼容模式）")
    
    def pull_task(self, timeout: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """拉取任务
        
        Args:
            timeout: 超时时间（毫秒），None表示使用默认超时
            
        Returns:
            Optional[Dict]: 任务信息，如果超时则返回None
            
        Raises:
            ConnectionError: 连接错误
            MessageError: 消息接收错误
        """
        if not self._connected or self._socket is None:
            raise ConnectionError("Puller未连接", address=self._address)
        
        try:
            # 设置临时超时
            if timeout is not None:
                old_timeout = self._socket.getsockopt(zmq.RCVTIMEO)
                self._socket.setsockopt(zmq.RCVTIMEO, timeout)
            
            try:
                # 接收任务数据
                task_data = self._socket.recv()
                
                # 反序列化任务
                task = self._serializer.deserialize(task_data)
                
                self._pulled_count += 1
                self._received_count += 1  # 更新received_count
                
                logger.debug(f"拉取任务: {len(task_data)} 字节")
                
                # 确保任务格式正确
                if not isinstance(task, dict):
                    task = {'type': 'unknown', 'data': task}
                
                if 'type' not in task:
                    task['type'] = 'unknown'
                
                return task
                
            finally:
                # 恢复原始超时设置
                if timeout is not None:
                    self._socket.setsockopt(zmq.RCVTIMEO, old_timeout)
            
        except zmq.Again:
            # 超时
            return None
        except Exception as e:
            error_msg = f"拉取任务失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="pull_task")
    
    def pull_simple(self, timeout: Optional[int] = None) -> Optional[Any]:
        """拉取简单数据（不解析为任务格式）
        
        Args:
            timeout: 超时时间（毫秒），None表示使用默认超时
            
        Returns:
            Optional[Any]: 数据，如果超时则返回None
        """
        if not self._connected or self._socket is None:
            raise ConnectionError("Puller未连接", address=self._address)
        
        try:
            # 设置临时超时
            if timeout is not None:
                old_timeout = self._socket.getsockopt(zmq.RCVTIMEO)
                self._socket.setsockopt(zmq.RCVTIMEO, timeout)
            
            try:
                # 接收数据
                data = self._socket.recv()
                
                # 反序列化数据
                result = self._serializer.deserialize(data)
                
                self._pulled_count += 1
                
                logger.debug(f"拉取简单数据: {len(data)} 字节")
                
                return result
                
            finally:
                # 恢复原始超时设置
                if timeout is not None:
                    self._socket.setsockopt(zmq.RCVTIMEO, old_timeout)
            
        except zmq.Again:
            # 超时
            return None
        except Exception as e:
            error_msg = f"拉取简单数据失败: {e}"
            logger.error(error_msg)
            raise MessageError(error_msg, operation="pull_simple")
    
    def _process_task(self, task: Dict[str, Any]) -> Any:
        """处理任务
        
        Args:
            task: 任务信息
            
        Returns:
            Any: 处理结果
        """
        task_type = task.get('type', 'unknown')
        task_data = task.get('data')
        
        try:
            # 查找特定处理器
            if task_type in self._task_handlers:
                handler = self._task_handlers[task_type]
                result = handler(task_data)
            elif self._default_handler:
                result = self._default_handler(task_type, task_data)
            else:
                # 默认处理：返回任务信息
                result = {
                    'status': 'unhandled',
                    'message': f'未找到处理器: {task_type}',
                    'task_type': task_type,
                    'task_data': task_data
                }
            
            self._processed_count += 1
            return result
            
        except Exception as e:
            self._failed_count += 1
            logger.error(f"处理任务失败: {e}")
            return {
                'status': 'error',
                'message': f'处理任务时发生错误: {str(e)}',
                'task_type': task_type,
                'error': str(e)
            }
    
    def _worker_loop(self) -> None:
        """工作者主循环"""
        logger.info("开始工作者主循环")
        
        while self._running:
            try:
                # 拉取任务
                task = self.pull_task(timeout=1000)
                if task:
                    # 处理任务
                    result = self._process_task(task)
                    
                    logger.debug(f"处理任务完成: {task.get('type', 'unknown')}")
                    
            except Exception as e:
                if self._running:
                    logger.error(f"工作者循环错误: {e}")
                    time.sleep(0.1)
        
        logger.info("工作者主循环结束")
    
    def start_worker(self) -> None:
        """启动工作者"""
        if self._running:
            logger.warning("工作者已经在运行")
            return
        
        if not self._connected:
            raise ConnectionError("Puller未连接", address=self._address)
        
        self._running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        
        logger.info("启动工作者")
    
    def stop_worker(self) -> None:
        """停止工作者"""
        if not self._running:
            logger.warning("工作者未在运行")
            return
        
        self._running = False
        
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)
        
        logger.info("停止工作者")
    
    def disconnect(self) -> None:
        """断开连接"""
        # 先停止工作者
        self.stop_worker()
        
        # 先设置_socket为None，避免引用已关闭的socket对象
        self._socket = None
        
        # 调用父类断开连接
        super().disconnect()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取拉取者统计信息
        
        Returns:
            Dict: 统计信息
        """
        return {
            'pulled_count': self._pulled_count,
            'received_count': self._received_count,  # 添加received_count字段
            'processed_count': self._processed_count,
            'failed_count': self._failed_count,
            'success_rate': self._processed_count / max(self._pulled_count, 1),
            'registered_handlers': list(self._task_handlers.keys()),
            'has_default_handler': self._default_handler is not None,
            'connected': self._connected,
            'running': self._running,
            'address': self._address,
            'bind_mode': self._bind
        }