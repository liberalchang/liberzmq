#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
请求-响应模式测试

测试Server和Client类的功能，包括请求处理、响应发送、远程方法调用等。
"""

import unittest
import time
import threading
from unittest.mock import MagicMock, patch

from liberzmq.patterns.reqrep import Server, Client
from liberzmq.exceptions.base import ConnectionError, MessageError, TimeoutError

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from casetest import get_test_address, get_test_data, get_test_serializers


class TestServer(unittest.TestCase):
    """服务器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.address = get_test_address('reqrep', 'server')
        self.server = Server(address=self.address, serializer='json')
    
    def tearDown(self):
        """测试后清理"""
        if self.server:
            # 先停止服务器线程，再断开连接
            if hasattr(self.server, '_running') and self.server._running:
                self.server.stop_server()
            self.server.disconnect()
    
    def test_server_initialization(self):
        """测试服务器初始化"""
        self.assertEqual(self.server._address, self.address)
        self.assertTrue(self.server._bind)
        self.assertFalse(self.server._connected)
        self.assertEqual(self.server._processed_count, 0)
        self.assertFalse(self.server._running)
    
    def test_server_connect_disconnect(self):
        """测试服务器连接和断开"""
        # 测试连接
        self.server.connect()
        self.assertTrue(self.server._connected)
        self.assertIsNotNone(self.server._socket)
        
        # 测试断开连接
        self.server.disconnect()
        self.assertFalse(self.server._connected)
        self.assertIsNone(self.server._socket)
    
    def test_server_handler_registration(self):
        """测试请求处理器注册"""
        # 注册处理器
        def echo_handler(data):
            return {'echo': data}
        
        def add_handler(data):
            return data.get('a', 0) + data.get('b', 0)
        
        self.server.register_handler('echo', echo_handler)
        self.server.register_handler('add', add_handler)
        
        self.assertIn('echo', self.server._request_handlers)
        self.assertIn('add', self.server._request_handlers)
        self.assertEqual(self.server._request_handlers['echo'], echo_handler)
        self.assertEqual(self.server._request_handlers['add'], add_handler)
    
    def test_server_default_handler(self):
        """测试默认处理器"""
        def default_handler(request_type, data):
            return {'type': request_type, 'data': data, 'handled_by': 'default'}
        
        self.server.set_default_handler(default_handler)
        self.assertEqual(self.server._default_handler, default_handler)
    
    def test_server_receive_timeout(self):
        """测试接收请求超时"""
        self.server.connect()
        
        # 测试超时接收（没有请求）
        result = self.server.receive_request(timeout=100)
        self.assertIsNone(result)
    
    def test_server_without_connection(self):
        """测试未连接时的操作"""
        # 测试接收请求
        with self.assertRaises(ConnectionError):
            self.server.receive_request()
        
        # 测试发送响应
        with self.assertRaises(ConnectionError):
            self.server.send_response({'status': 'ok'})
    
    def test_server_stats(self):
        """测试服务器统计信息"""
        # 注册一些处理器
        self.server.register_handler('test1', lambda x: x)
        self.server.register_handler('test2', lambda x: x)
        self.server.set_default_handler(lambda t, d: d)
        
        self.server.connect()
        
        stats = self.server.get_stats()
        
        self.assertEqual(stats['processed_count'], 0)
        self.assertEqual(len(stats['registered_handlers']), 2)
        self.assertIn('test1', stats['registered_handlers'])
        self.assertIn('test2', stats['registered_handlers'])
        self.assertTrue(stats['has_default_handler'])
        self.assertTrue(stats['connected'])
        self.assertFalse(stats['running'])
        self.assertTrue(stats['bind_mode'])
        self.assertEqual(stats['address'], self.address)
    
    def test_server_start_stop(self):
        """测试服务器启动和停止"""
        self.server.connect()
        
        # 启动服务器
        self.server.start_server()
        self.assertTrue(self.server._running)
        self.assertIsNotNone(self.server._server_thread)
        
        # 停止服务器
        self.server.stop_server()
        self.assertFalse(self.server._running)


class TestClient(unittest.TestCase):
    """客户端测试"""
    
    def setUp(self):
        """测试前准备"""
        self.address = get_test_address('reqrep', 'client')
        self.client = Client(address=self.address, serializer='json')
    
    def tearDown(self):
        """测试后清理"""
        if self.client:
            self.client.disconnect()
    
    def test_client_initialization(self):
        """测试客户端初始化"""
        self.assertEqual(self.client._address, self.address)
        self.assertFalse(self.client._bind)
        self.assertFalse(self.client._connected)
        self.assertEqual(self.client._request_count, 0)
        self.assertEqual(self.client._response_count, 0)
    
    def test_client_connect_disconnect(self):
        """测试客户端连接和断开"""
        # 测试连接
        self.client.connect()
        self.assertTrue(self.client._connected)
        self.assertIsNotNone(self.client._socket)
        
        # 测试断开连接
        self.client.disconnect()
        self.assertFalse(self.client._connected)
        self.assertIsNone(self.client._socket)
    
    def test_client_without_connection(self):
        """测试未连接时发送请求"""
        with self.assertRaises(ConnectionError):
            self.client.send_request('test', {'data': 'test'})
    
    def test_client_stats(self):
        """测试客户端统计信息"""
        self.client.connect()
        
        stats = self.client.get_stats()
        
        self.assertEqual(stats['request_count'], 0)
        self.assertEqual(stats['response_count'], 0)
        self.assertTrue(stats['connected'])
        self.assertFalse(stats['bind_mode'])
        self.assertEqual(stats['address'], self.address)


class TestReqRepIntegration(unittest.TestCase):
    """请求-响应集成测试"""
    
    def setUp(self):
        """测试前准备"""
        self.server_address = get_test_address('reqrep', 'server')
        self.client_address = get_test_address('reqrep', 'client')
        
        self.server = Server(address=self.server_address, serializer='json')
        self.client = Client(address=self.client_address, serializer='json')
    
    def tearDown(self):
        """测试后清理"""
        if self.server:
            # 先停止服务器线程，再断开连接
            if hasattr(self.server, '_running') and self.server._running:
                self.server.stop_server()
            self.server.disconnect()
        if self.client:
            self.client.disconnect()
    
    def test_basic_request_response(self):
        """测试基本请求-响应"""
        # 注册处理器
        def echo_handler(data):
            return {'echo': data, 'status': 'success'}
        
        self.server.register_handler('echo', echo_handler)
        
        # 连接服务器和客户端
        self.server.connect()
        self.client.connect()
        
        # 启动服务器
        self.server.start_server()
        
        # 等待服务器启动
        time.sleep(0.2)
        
        try:
            # 发送请求
            test_data = get_test_data('simple')
            response = self.client.send_request('echo', test_data, timeout=2000)
            
            # 验证响应
            self.assertIsInstance(response, dict)
            self.assertEqual(response['echo'], test_data)
            self.assertEqual(response['status'], 'success')
            
            # 验证统计信息
            self.assertEqual(self.client._request_count, 1)
            self.assertEqual(self.client._response_count, 1)
            
        finally:
            self.server.stop_server()
    
    def test_multiple_request_types(self):
        """测试多种请求类型"""
        # 注册多个处理器
        def add_handler(data):
            return {'result': data.get('a', 0) + data.get('b', 0)}
        
        def multiply_handler(data):
            return {'result': data.get('a', 1) * data.get('b', 1)}
        
        def info_handler(data):
            return {
                'server': 'liberzmq_test_server',
                'version': '1.0.0',
                'request_data': data
            }
        
        self.server.register_handler('add', add_handler)
        self.server.register_handler('multiply', multiply_handler)
        self.server.register_handler('info', info_handler)
        
        # 连接和启动
        self.server.connect()
        self.client.connect()
        self.server.start_server()
        
        time.sleep(0.2)
        
        try:
            # 测试加法
            add_response = self.client.send_request('add', {'a': 5, 'b': 3}, timeout=2000)
            self.assertEqual(add_response['result'], 8)
            
            # 测试乘法
            multiply_response = self.client.send_request('multiply', {'a': 4, 'b': 6}, timeout=2000)
            self.assertEqual(multiply_response['result'], 24)
            
            # 测试信息
            info_response = self.client.send_request('info', {'client': 'test'}, timeout=2000)
            self.assertEqual(info_response['server'], 'liberzmq_test_server')
            self.assertEqual(info_response['request_data']['client'], 'test')
            
            # 验证请求计数
            self.assertEqual(self.client._request_count, 3)
            self.assertEqual(self.client._response_count, 3)
            
        finally:
            self.server.stop_server()
    
    def test_default_handler(self):
        """测试默认处理器"""
        # 设置默认处理器
        def default_handler(request_type, data):
            return {
                'status': 'handled_by_default',
                'request_type': request_type,
                'data': data
            }
        
        self.server.set_default_handler(default_handler)
        
        # 连接和启动
        self.server.connect()
        self.client.connect()
        self.server.start_server()
        
        time.sleep(0.2)
        
        try:
            # 发送未注册的请求类型
            response = self.client.send_request('unknown_type', {'test': 'data'}, timeout=2000)
            
            self.assertEqual(response['status'], 'handled_by_default')
            self.assertEqual(response['request_type'], 'unknown_type')
            self.assertEqual(response['data']['test'], 'data')
            
        finally:
            self.server.stop_server()
    
    def test_no_handler_error(self):
        """测试没有处理器的错误响应"""
        # 不注册任何处理器
        self.server.connect()
        self.client.connect()
        self.server.start_server()
        
        time.sleep(0.2)
        
        try:
            # 发送请求
            response = self.client.send_request('unknown', {'test': 'data'}, timeout=2000)
            
            # 应该收到错误响应
            self.assertIsInstance(response, dict)
            self.assertEqual(response['status'], 'error')
            self.assertIn('未知的请求类型', response['message'])
            
        finally:
            self.server.stop_server()
    
    def test_handler_exception(self):
        """测试处理器异常"""
        # 注册会抛出异常的处理器
        def error_handler(data):
            raise ValueError("测试异常")
        
        self.server.register_handler('error', error_handler)
        
        # 连接和启动
        self.server.connect()
        self.client.connect()
        self.server.start_server()
        
        time.sleep(0.2)
        
        try:
            # 发送请求
            response = self.client.send_request('error', {'test': 'data'}, timeout=2000)
            
            # 应该收到错误响应
            self.assertIsInstance(response, dict)
            self.assertEqual(response['status'], 'error')
            self.assertIn('处理请求时发生错误', response['message'])
            
        finally:
            self.server.stop_server()
    
    def test_ping_functionality(self):
        """测试ping功能"""
        # 注册ping处理器
        def ping_handler(data):
            return {
                'status': 'pong',
                'timestamp': data.get('timestamp'),
                'server_time': time.time()
            }
        
        self.server.register_handler('ping', ping_handler)
        
        # 连接和启动
        self.server.connect()
        self.client.connect()
        self.server.start_server()
        
        time.sleep(0.2)
        
        try:
            # 发送ping
            response = self.client.ping(timeout=2000)
            
            self.assertIsInstance(response, dict)
            self.assertEqual(response['status'], 'pong')
            self.assertIn('timestamp', response)
            self.assertIn('server_time', response)
            
        finally:
            self.server.stop_server()
    
    def test_remote_method_call(self):
        """测试远程方法调用"""
        # 注册call处理器
        def call_handler(data):
            method = data.get('method')
            args = data.get('args', [])
            kwargs = data.get('kwargs', {})
            
            # 模拟一些方法
            if method == 'add':
                return sum(args)
            elif method == 'concat':
                return ''.join(str(arg) for arg in args)
            elif method == 'get_info':
                return {'method': method, 'args': args, 'kwargs': kwargs}
            else:
                raise ValueError(f"未知方法: {method}")
        
        self.server.register_handler('call', call_handler)
        
        # 连接和启动
        self.server.connect()
        self.client.connect()
        self.server.start_server()
        
        time.sleep(0.2)
        
        try:
            # 测试add方法
            result = self.client.call('add', 1, 2, 3, 4, timeout=2000)
            self.assertEqual(result, 10)
            
            # 测试concat方法
            result = self.client.call('concat', 'hello', ' ', 'world', timeout=2000)
            self.assertEqual(result, 'hello world')
            
            # 测试get_info方法
            result = self.client.call('get_info', 'arg1', key='value', timeout=2000)
            self.assertEqual(result['method'], 'get_info')
            self.assertEqual(result['args'], ('arg1',))
            self.assertEqual(result['kwargs'], {'key': 'value'})
            
        finally:
            self.server.stop_server()
    
    def test_request_timeout(self):
        """测试请求超时"""
        # 注册慢处理器
        def slow_handler(data):
            time.sleep(2)  # 睡眠2秒
            return {'status': 'completed'}
        
        self.server.register_handler('slow', slow_handler)
        
        # 连接和启动
        self.server.connect()
        self.client.connect()
        self.server.start_server()
        
        time.sleep(0.2)
        
        try:
            # 发送请求，设置较短的超时时间
            with self.assertRaises(TimeoutError):
                self.client.send_request('slow', {'test': 'data'}, timeout=500)
            
        finally:
            self.server.stop_server()
    
    def test_different_serializers(self):
        """测试不同序列化器的兼容性"""
        for serializer_name in get_test_serializers():
            with self.subTest(serializer=serializer_name):
                # 创建使用相同序列化器的服务器和客户端
                server = Server(
                    address=get_test_address('reqrep', 'server'),
                    serializer=serializer_name
                )
                client = Client(
                    address=get_test_address('reqrep', 'client'),
                    serializer=serializer_name
                )
                
                # 注册处理器
                def echo_handler(data):
                    return {'echo': data}
                
                server.register_handler('echo', echo_handler)
                
                try:
                    server.connect()
                    client.connect()
                    server.start_server()
                    
                    time.sleep(0.2)
                    
                    # 发送请求
                    test_data = get_test_data('simple')
                    response = client.send_request('echo', test_data, timeout=2000)
                    
                    self.assertEqual(response['echo'], test_data)
                    
                finally:
                    server.stop_server()
                    server.disconnect()
                    client.disconnect()


if __name__ == '__main__':
    unittest.main()