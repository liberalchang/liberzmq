#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Dealer-Router模式测试

测试Dealer和Router类的功能，包括异步请求-响应、负载均衡等。
"""

import unittest
import time
import threading
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from liberzmq.patterns.dealerrouter import Dealer, Router, create_dealer_router_pair
from liberzmq.exceptions.base import ConnectionError, MessageError, TimeoutError


class TestDealerRouter(unittest.TestCase):
    """Dealer-Router模式测试类"""
    
    def setUp(self):
        """测试前准备"""
        self.dealer_address = "tcp://localhost:5558"
        self.router_address = "tcp://*:5558"
        self.dealer = None
        self.router = None
    
    def tearDown(self):
        """测试后清理"""
        if self.dealer:
            self.dealer.disconnect()
        if self.router:
            self.router.disconnect()
        time.sleep(0.1)  # 等待端口释放
    
    def test_dealer_initialization(self):
        """测试Dealer初始化"""
        dealer = Dealer(
            address="tcp://localhost:5559",
            serializer="json",
            bind=False,
            identity="test-dealer"
        )
        
        self.assertEqual(dealer.host, "localhost")
        self.assertEqual(dealer.port, 5559)
        self.assertEqual(dealer.identity, "test-dealer")
        self.assertFalse(dealer.is_connected())
        
        dealer.disconnect()
    
    def test_router_initialization(self):
        """测试Router初始化"""
        router = Router(
            address="tcp://*:5559",
            serializer="json",
            bind=True
        )
        
        self.assertEqual(router.host, "0.0.0.0")
        self.assertEqual(router.port, 5559)
        self.assertFalse(router.is_connected())
        
        router.disconnect()
    
    def test_dealer_router_connection(self):
        """测试Dealer-Router连接"""
        # 创建Router并绑定
        self.router = Router(address=self.router_address)
        self.router.connect()
        self.assertTrue(self.router.is_connected())
        
        # 创建Dealer并连接
        self.dealer = Dealer(address=self.dealer_address)
        self.dealer.connect()
        self.assertTrue(self.dealer.is_connected())
        
        time.sleep(0.1)  # 等待连接建立
    
    def test_simple_request_response(self):
        """测试简单的请求-响应"""
        # 设置Router
        self.router = Router(address=self.router_address)
        self.router.connect()
        
        # 设置Dealer
        self.dealer = Dealer(address=self.dealer_address)
        self.dealer.connect()
        
        time.sleep(0.1)  # 等待连接建立
        
        # 发送请求
        request_data = {"message": "Hello Router", "value": 42}
        request_id = self.dealer.send_request(request_data)
        
        self.assertIsInstance(request_id, str)
        self.assertTrue(request_id.startswith(self.dealer.identity))
        
        # 接收请求
        client_id, recv_request_id, recv_data = self.router.receive_request(timeout=1000)
        
        self.assertEqual(client_id, self.dealer.identity)
        self.assertEqual(recv_request_id, request_id)
        self.assertEqual(recv_data, request_data)
        
        # 发送响应
        response_data = {"status": "success", "echo": recv_data}
        self.router.send_response(client_id, recv_request_id, response_data)
        
        # 接收响应
        response_request_id, response = self.dealer.receive_response(timeout=1000)
        
        self.assertEqual(response_request_id, request_id)
        self.assertEqual(response, response_data)
    
    def test_multiple_requests(self):
        """测试多个并发请求"""
        # 设置Router
        self.router = Router(address=self.router_address)
        self.router.connect()
        
        # 设置Dealer
        self.dealer = Dealer(address=self.dealer_address)
        self.dealer.connect()
        
        time.sleep(0.1)
        
        # 发送多个请求
        request_ids = []
        for i in range(3):
            request_data = {"request_number": i, "message": f"Request {i}"}
            request_id = self.dealer.send_request(request_data)
            request_ids.append(request_id)
        
        # 处理所有请求
        responses = {}
        for i in range(3):
            client_id, request_id, request_data = self.router.receive_request(timeout=1000)
            response_data = {"response_to": request_data["request_number"], "status": "processed"}
            self.router.send_response(client_id, request_id, response_data)
            responses[request_id] = response_data
        
        # 接收所有响应
        received_responses = {}
        for i in range(3):
            request_id, response = self.dealer.receive_response(timeout=1000)
            received_responses[request_id] = response
        
        # 验证响应
        self.assertEqual(len(received_responses), 3)
        for request_id in request_ids:
            self.assertIn(request_id, received_responses)
    
    def test_callback_functionality(self):
        """测试回调功能"""
        # 设置Router
        self.router = Router(address=self.router_address)
        self.router.connect()
        
        # 设置Dealer
        self.dealer = Dealer(address=self.dealer_address)
        self.dealer.connect()
        
        time.sleep(0.1)
        
        # 回调函数
        callback_results = []
        
        def response_callback(response_data):
            callback_results.append(response_data)
        
        # 发送带回调的请求
        request_data = {"message": "Test callback"}
        request_id = self.dealer.send_request(request_data, callback=response_callback)
        
        # 处理请求
        client_id, recv_request_id, recv_data = self.router.receive_request(timeout=1000)
        response_data = {"callback_test": True, "original": recv_data}
        self.router.send_response(client_id, recv_request_id, response_data)
        
        # 接收响应（触发回调）
        self.dealer.receive_response(timeout=1000)
        
        # 验证回调被调用
        self.assertEqual(len(callback_results), 1)
        self.assertEqual(callback_results[0], response_data)
    
    def test_pending_requests_management(self):
        """测试待处理请求管理"""
        self.dealer = Dealer(address=self.dealer_address)
        self.dealer.connect()
        
        # 发送请求但不处理响应
        request_data = {"test": "pending"}
        request_id = self.dealer.send_request(request_data, timeout=1000)
        
        # 检查待处理请求
        pending = self.dealer.get_pending_requests()
        self.assertEqual(len(pending), 1)
        self.assertIn(request_id, pending)
        self.assertEqual(pending[request_id]['data'], request_data)
        
        # 清理过期请求（等待超时）
        time.sleep(1.1)
        expired_count = self.dealer.cleanup_expired_requests()
        self.assertEqual(expired_count, 1)
        
        # 验证请求已被清理
        pending_after = self.dealer.get_pending_requests()
        self.assertEqual(len(pending_after), 0)
    
    def test_client_statistics(self):
        """测试客户端统计功能"""
        # 设置Router
        self.router = Router(address=self.router_address)
        self.router.connect()
        
        # 设置Dealer
        self.dealer = Dealer(address=self.dealer_address, identity="stats-test-dealer")
        self.dealer.connect()
        
        time.sleep(0.1)
        
        # 发送几个请求
        for i in range(3):
            request_data = {"test": f"stats_{i}"}
            self.dealer.send_request(request_data)
            
            # Router接收请求
            client_id, request_id, recv_data = self.router.receive_request(timeout=1000)
            
            # 发送响应
            self.router.send_response(client_id, request_id, {"response": i})
            
            # Dealer接收响应
            self.dealer.receive_response(timeout=1000)
        
        # 检查客户端统计
        stats = self.router.get_client_stats()
        self.assertIn("stats-test-dealer", stats)
        
        client_stats = stats["stats-test-dealer"]
        self.assertEqual(client_stats['request_count'], 3)
        self.assertIsNotNone(client_stats['last_request'])
        self.assertIsNotNone(client_stats['first_seen'])
        
        # 检查活跃客户端
        active_clients = self.router.get_active_clients(timeout_seconds=10)
        self.assertIn("stats-test-dealer", active_clients)
    
    def test_timeout_handling(self):
        """测试超时处理"""
        # 只创建Dealer，不创建Router
        self.dealer = Dealer(address=self.dealer_address)
        self.dealer.connect()
        
        # 尝试接收响应（应该超时）
        with self.assertRaises(TimeoutError):
            self.dealer.receive_response(timeout=100)
        
        # 测试非阻塞接收
        result = self.dealer.receive_response(timeout=0)
        self.assertIsNone(result)
    
    def test_create_dealer_router_pair(self):
        """测试便利函数"""
        dealer, router = create_dealer_router_pair(
            dealer_address="tcp://localhost:5560",
            router_address="tcp://*:5560",
            serializer="json"
        )
        
        self.assertIsInstance(dealer, Dealer)
        self.assertIsInstance(router, Router)
        self.assertEqual(dealer.port, 5560)
        self.assertEqual(router.port, 5560)
        
        # 测试连接
        router.connect()
        dealer.connect()
        
        self.assertTrue(router.is_connected())
        self.assertTrue(dealer.is_connected())
        
        # 清理
        dealer.disconnect()
        router.disconnect()
    
    def test_error_handling(self):
        """测试错误处理"""
        dealer = Dealer(address="tcp://localhost:5561")
        
        # 测试未连接时发送请求
        with self.assertRaises(ConnectionError):
            dealer.send_request({"test": "data"})
        
        # 测试未连接时接收响应
        with self.assertRaises(ConnectionError):
            dealer.receive_response()
        
        dealer.disconnect()


class TestDealerRouterIntegration(unittest.TestCase):
    """Dealer-Router集成测试"""
    
    def test_multiple_dealers_single_router(self):
        """测试多个Dealer连接到单个Router"""
        # 创建Router
        router = Router(address="tcp://*:5562")
        router.connect()
        
        # 创建多个Dealer
        dealers = []
        for i in range(3):
            dealer = Dealer(
                address="tcp://localhost:5562",
                identity=f"dealer-{i}"
            )
            dealer.connect()
            dealers.append(dealer)
        
        time.sleep(0.1)
        
        try:
            # 每个Dealer发送请求
            request_ids = []
            for i, dealer in enumerate(dealers):
                request_data = {"dealer_id": i, "message": f"From dealer {i}"}
                request_id = dealer.send_request(request_data)
                request_ids.append((dealer, request_id))
            
            # Router处理所有请求
            processed_requests = []
            for i in range(3):
                client_id, request_id, request_data = router.receive_request(timeout=1000)
                processed_requests.append((client_id, request_id, request_data))
                
                response_data = {"processed_by_router": True, "original": request_data}
                router.send_response(client_id, request_id, response_data)
            
            # 每个Dealer接收响应
            responses = []
            for dealer, expected_request_id in request_ids:
                request_id, response = dealer.receive_response(timeout=1000)
                self.assertEqual(request_id, expected_request_id)
                responses.append(response)
            
            # 验证所有请求都被处理
            self.assertEqual(len(processed_requests), 3)
            self.assertEqual(len(responses), 3)
            
            # 验证客户端统计
            stats = router.get_client_stats()
            self.assertEqual(len(stats), 3)
            for i in range(3):
                self.assertIn(f"dealer-{i}", stats)
                self.assertEqual(stats[f"dealer-{i}"]['request_count'], 1)
        
        finally:
            # 清理
            for dealer in dealers:
                dealer.disconnect()
            router.disconnect()
    
    def test_load_balancing_simulation(self):
        """测试负载均衡模拟"""
        # 创建Router
        router = Router(address="tcp://*:5563")
        router.connect()
        
        # 创建Dealer
        dealer = Dealer(address="tcp://localhost:5563")
        dealer.connect()
        
        time.sleep(0.1)
        
        try:
            # 模拟高并发请求
            num_requests = 10
            request_ids = []
            
            # 快速发送多个请求
            for i in range(num_requests):
                request_data = {"task_id": i, "data": f"task_{i}"}
                request_id = dealer.send_request(request_data)
                request_ids.append(request_id)
            
            # Router批量处理请求
            processed_count = 0
            for i in range(num_requests):
                client_id, request_id, request_data = router.receive_request(timeout=1000)
                
                # 模拟处理时间
                time.sleep(0.01)
                
                response_data = {
                    "task_completed": True,
                    "task_id": request_data["task_id"],
                    "processed_at": time.time()
                }
                router.send_response(client_id, request_id, response_data)
                processed_count += 1
            
            # 接收所有响应
            received_count = 0
            for i in range(num_requests):
                request_id, response = dealer.receive_response(timeout=1000)
                self.assertIn("task_completed", response)
                self.assertTrue(response["task_completed"])
                received_count += 1
            
            self.assertEqual(processed_count, num_requests)
            self.assertEqual(received_count, num_requests)
        
        finally:
            dealer.disconnect()
            router.disconnect()


if __name__ == '__main__':
    unittest.main()