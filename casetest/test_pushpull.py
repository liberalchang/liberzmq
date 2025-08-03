#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
推拉模式测试

测试Pusher和Puller类的功能，包括任务分发、负载均衡、批量处理等。
"""

import unittest
import time
import threading
from unittest.mock import MagicMock, patch

from liberzmq.patterns.pushpull import Pusher, Puller
from liberzmq.exceptions.base import ConnectionError, MessageError, TimeoutError

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from casetest import get_test_address, get_test_data, get_test_serializers


class TestPusher(unittest.TestCase):
    """推送器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.address = get_test_address('pushpull', 'pusher')
        self.pusher = Pusher(address=self.address, serializer='json')
    
    def tearDown(self):
        """测试后清理"""
        if self.pusher:
            self.pusher.disconnect()
    
    def test_pusher_initialization(self):
        """测试推送器初始化"""
        self.assertEqual(self.pusher._address, self.address)
        self.assertTrue(self.pusher._bind)
        self.assertFalse(self.pusher._connected)
        self.assertEqual(self.pusher._sent_count, 0)
    
    def test_pusher_connect_disconnect(self):
        """测试推送器连接和断开"""
        # 测试连接
        self.pusher.connect()
        self.assertTrue(self.pusher._connected)
        self.assertIsNotNone(self.pusher._socket)
        
        # 测试断开连接
        self.pusher.disconnect()
        self.assertFalse(self.pusher._connected)
        self.assertIsNone(self.pusher._socket)
    
    def test_pusher_without_connection(self):
        """测试未连接时推送任务"""
        with self.assertRaises(ConnectionError):
            self.pusher.push_task({'task': 'test'})
    
    def test_pusher_stats(self):
        """测试推送器统计信息"""
        self.pusher.connect()
        
        stats = self.pusher.get_stats()
        
        self.assertEqual(stats['sent_count'], 0)
        self.assertTrue(stats['connected'])
        self.assertTrue(stats['bind_mode'])
        self.assertEqual(stats['address'], self.address)
    
    def test_pusher_task_validation(self):
        """测试任务验证"""
        self.pusher.connect()
        
        # 测试有效任务
        valid_tasks = [
            {'type': 'process', 'data': 'test'},
            {'id': 1, 'action': 'compute'},
            'simple_string_task',
            123,
            [1, 2, 3]
        ]
        
        for task in valid_tasks:
            with self.subTest(task=task):
                # 应该不抛出异常
                self.pusher.push_task(task)
                
        self.assertEqual(self.pusher._sent_count, len(valid_tasks))


class TestPuller(unittest.TestCase):
    """拉取器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.address = get_test_address('pushpull', 'puller')
        self.puller = Puller(address=self.address, serializer='json')
    
    def tearDown(self):
        """测试后清理"""
        if self.puller:
            self.puller.disconnect()
    
    def test_puller_initialization(self):
        """测试拉取器初始化"""
        self.assertEqual(self.puller._address, self.address)
        self.assertFalse(self.puller._bind)
        self.assertFalse(self.puller._connected)
        self.assertEqual(self.puller._received_count, 0)
        self.assertEqual(self.puller._processed_count, 0)
        self.assertFalse(self.puller._running)
    
    def test_puller_connect_disconnect(self):
        """测试拉取器连接和断开"""
        # 测试连接
        self.puller.connect()
        self.assertTrue(self.puller._connected)
        self.assertIsNotNone(self.puller._socket)
        
        # 测试断开连接
        self.puller.disconnect()
        self.assertFalse(self.puller._connected)
        self.assertIsNone(self.puller._socket)
    
    def test_puller_without_connection(self):
        """测试未连接时拉取任务"""
        with self.assertRaises(ConnectionError):
            self.puller.pull_task()
    
    def test_puller_timeout(self):
        """测试拉取超时"""
        self.puller.connect()
        
        # 测试超时拉取（没有任务）
        result = self.puller.pull_task(timeout=100)
        self.assertIsNone(result)
    
    def test_puller_task_handler(self):
        """测试任务处理器"""
        # 注册任务处理器
        def process_handler(task):
            return {'processed': task, 'status': 'completed'}
        
        def compute_handler(task):
            if isinstance(task, dict) and 'numbers' in task:
                return sum(task['numbers'])
            return None
        
        self.puller.register_task_handler('process', process_handler)
        self.puller.register_task_handler('compute', compute_handler)
        
        self.assertIn('process', self.puller._task_handlers)
        self.assertIn('compute', self.puller._task_handlers)
        self.assertEqual(self.puller._task_handlers['process'], process_handler)
        self.assertEqual(self.puller._task_handlers['compute'], compute_handler)
    
    def test_puller_default_handler(self):
        """测试默认任务处理器"""
        def default_handler(task):
            return {'default_processed': task}
        
        self.puller.set_default_task_handler(default_handler)
        self.assertEqual(self.puller._default_task_handler, default_handler)
    
    def test_puller_stats(self):
        """测试拉取器统计信息"""
        # 注册一些处理器
        self.puller.register_task_handler('test1', lambda x: x)
        self.puller.register_task_handler('test2', lambda x: x)
        self.puller.set_default_task_handler(lambda x: x)
        
        self.puller.connect()
        
        stats = self.puller.get_stats()
        
        self.assertEqual(stats['received_count'], 0)
        self.assertEqual(stats['processed_count'], 0)
        self.assertEqual(len(stats['registered_handlers']), 2)
        self.assertIn('test1', stats['registered_handlers'])
        self.assertIn('test2', stats['registered_handlers'])
        self.assertTrue(stats['has_default_handler'])
        self.assertTrue(stats['connected'])
        self.assertFalse(stats['running'])
        self.assertFalse(stats['bind_mode'])
        self.assertEqual(stats['address'], self.address)
    
    def test_puller_start_stop(self):
        """测试拉取器启动和停止"""
        self.puller.connect()
        
        # 启动拉取器
        self.puller.start_worker()
        self.assertTrue(self.puller._running)
        self.assertIsNotNone(self.puller._worker_thread)
        
        # 停止拉取器
        self.puller.stop_worker()
        self.assertFalse(self.puller._running)


class TestPushPullIntegration(unittest.TestCase):
    """推拉集成测试"""
    
    def setUp(self):
        """测试前准备"""
        self.pusher_address = get_test_address('pushpull', 'pusher')
        self.puller_address = get_test_address('pushpull', 'puller')
        
        self.pusher = Pusher(address=self.pusher_address, serializer='json')
        self.puller = Puller(address=self.puller_address, serializer='json')
    
    def tearDown(self):
        """测试后清理"""
        if self.pusher:
            self.pusher.disconnect()
        if self.puller:
            self.puller.disconnect()
    
    def test_basic_push_pull(self):
        """测试基本推拉功能"""
        # 连接推送器和拉取器
        self.pusher.connect()
        self.puller.connect()
        
        # 等待连接建立
        time.sleep(0.2)
        
        # 推送任务
        test_task = get_test_data('simple')
        self.pusher.push_task(test_task)
        
        # 拉取任务
        received_task = self.puller.pull_task(timeout=2000)
        
        # 验证任务
        self.assertEqual(received_task, test_task)
        self.assertEqual(self.pusher._sent_count, 1)
        self.assertEqual(self.puller._received_count, 1)
    
    def test_multiple_tasks(self):
        """测试多个任务"""
        self.pusher.connect()
        self.puller.connect()
        
        time.sleep(0.2)
        
        # 推送多个任务
        tasks = [
            {'id': 1, 'type': 'process', 'data': 'task1'},
            {'id': 2, 'type': 'compute', 'numbers': [1, 2, 3]},
            {'id': 3, 'type': 'analyze', 'text': 'hello world'},
            'simple_task',
            42
        ]
        
        for task in tasks:
            self.pusher.push_task(task)
        
        # 拉取所有任务
        received_tasks = []
        for _ in range(len(tasks)):
            task = self.puller.pull_task(timeout=2000)
            self.assertIsNotNone(task)
            received_tasks.append(task)
        
        # 验证任务
        self.assertEqual(len(received_tasks), len(tasks))
        self.assertEqual(set(str(t) for t in received_tasks), set(str(t) for t in tasks))
        self.assertEqual(self.pusher._sent_count, len(tasks))
        self.assertEqual(self.puller._received_count, len(tasks))
    
    def test_task_processing_with_handlers(self):
        """测试带处理器的任务处理"""
        # 注册任务处理器
        processed_tasks = []
        
        def process_handler(task):
            result = {'processed': task, 'handler': 'process'}
            processed_tasks.append(result)
            return result
        
        def compute_handler(task):
            if isinstance(task, dict) and 'numbers' in task:
                result = {'sum': sum(task['numbers']), 'handler': 'compute'}
                processed_tasks.append(result)
                return result
            return None
        
        def default_handler(task):
            result = {'default_processed': task, 'handler': 'default'}
            processed_tasks.append(result)
            return result
        
        self.puller.register_task_handler('process', process_handler)
        self.puller.register_task_handler('compute', compute_handler)
        self.puller.set_default_task_handler(default_handler)
        
        # 连接
        self.pusher.connect()
        self.puller.connect()
        
        time.sleep(0.2)
        
        # 推送不同类型的任务
        tasks = [
            {'type': 'process', 'data': 'test_data'},
            {'type': 'compute', 'numbers': [1, 2, 3, 4, 5]},
            {'type': 'unknown', 'data': 'should_use_default'},
            'string_task'
        ]
        
        for task in tasks:
            self.pusher.push_task(task)
        
        # 处理任务
        for _ in range(len(tasks)):
            task = self.puller.pull_task(timeout=2000)
            self.assertIsNotNone(task)
            result = self.puller.process_task(task)
            self.assertIsNotNone(result)
        
        # 验证处理结果
        self.assertEqual(len(processed_tasks), len(tasks))
        
        # 验证特定处理器的结果
        process_results = [t for t in processed_tasks if t.get('handler') == 'process']
        compute_results = [t for t in processed_tasks if t.get('handler') == 'compute']
        default_results = [t for t in processed_tasks if t.get('handler') == 'default']
        
        self.assertEqual(len(process_results), 1)
        self.assertEqual(len(compute_results), 1)
        self.assertEqual(len(default_results), 2)  # unknown type + string task
        
        # 验证计算结果
        self.assertEqual(compute_results[0]['sum'], 15)
    
    def test_worker_mode(self):
        """测试工作器模式"""
        # 设置任务处理器
        processed_tasks = []
        
        def task_handler(task):
            processed_tasks.append(task)
            return f"processed_{task}"
        
        self.puller.set_default_task_handler(task_handler)
        
        # 连接
        self.pusher.connect()
        self.puller.connect()
        
        # 启动工作器
        self.puller.start_worker()
        
        time.sleep(0.2)
        
        try:
            # 推送任务
            tasks = ['task1', 'task2', 'task3', 'task4', 'task5']
            for task in tasks:
                self.pusher.push_task(task)
            
            # 等待处理完成
            time.sleep(1)
            
            # 验证处理结果
            self.assertEqual(len(processed_tasks), len(tasks))
            self.assertEqual(set(processed_tasks), set(tasks))
            self.assertEqual(self.puller._processed_count, len(tasks))
            
        finally:
            self.puller.stop_worker()
    
    def test_load_balancing(self):
        """测试负载均衡"""
        # 创建多个拉取器
        pullers = []
        processed_by_puller = {}
        
        for i in range(3):
            puller = Puller(
                address=get_test_address('pushpull', f'puller_{i}'),
                serializer='json'
            )
            
            # 设置处理器来跟踪哪个拉取器处理了任务
            def make_handler(puller_id):
                def handler(task):
                    if puller_id not in processed_by_puller:
                        processed_by_puller[puller_id] = []
                    processed_by_puller[puller_id].append(task)
                    return f"processed_by_{puller_id}"
                return handler
            
            puller.set_default_task_handler(make_handler(i))
            puller.connect()
            puller.start_worker()
            pullers.append(puller)
        
        # 连接推送器
        self.pusher.connect()
        
        time.sleep(0.5)
        
        try:
            # 推送多个任务
            tasks = [f'task_{i}' for i in range(15)]
            for task in tasks:
                self.pusher.push_task(task)
                time.sleep(0.1)  # 稍微延迟以确保负载均衡
            
            # 等待处理完成
            time.sleep(2)
            
            # 验证负载均衡
            total_processed = sum(len(tasks) for tasks in processed_by_puller.values())
            self.assertEqual(total_processed, len(tasks))
            
            # 每个拉取器都应该处理一些任务
            for puller_id, puller_tasks in processed_by_puller.items():
                self.assertGreater(len(puller_tasks), 0, f"拉取器 {puller_id} 没有处理任何任务")
            
        finally:
            for puller in pullers:
                puller.stop_worker()
                puller.disconnect()
    
    def test_batch_processing(self):
        """测试批量处理"""
        # 连接
        self.pusher.connect()
        self.puller.connect()
        
        time.sleep(0.2)
        
        # 批量推送任务
        batch_size = 10
        tasks = [{'id': i, 'data': f'task_{i}'} for i in range(batch_size)]
        
        self.pusher.push_batch(tasks)
        
        # 批量拉取任务
        received_tasks = self.puller.pull_batch(batch_size, timeout=2000)
        
        # 验证批量操作
        self.assertEqual(len(received_tasks), batch_size)
        self.assertEqual(self.pusher._sent_count, batch_size)
        self.assertEqual(self.puller._received_count, batch_size)
        
        # 验证任务内容
        received_ids = {task['id'] for task in received_tasks}
        expected_ids = {task['id'] for task in tasks}
        self.assertEqual(received_ids, expected_ids)
    
    def test_task_priority(self):
        """测试任务优先级"""
        # 连接
        self.pusher.connect()
        self.puller.connect()
        
        time.sleep(0.2)
        
        # 推送不同优先级的任务
        high_priority_task = {'priority': 'high', 'data': 'urgent_task'}
        normal_tasks = [{'priority': 'normal', 'data': f'task_{i}'} for i in range(3)]
        
        # 先推送普通任务
        for task in normal_tasks:
            self.pusher.push_task(task)
        
        # 再推送高优先级任务
        self.pusher.push_task(high_priority_task, priority='high')
        
        # 拉取任务
        received_tasks = []
        for _ in range(len(normal_tasks) + 1):
            task = self.puller.pull_task(timeout=2000)
            self.assertIsNotNone(task)
            received_tasks.append(task)
        
        # 验证任务接收
        self.assertEqual(len(received_tasks), len(normal_tasks) + 1)
    
    def test_different_serializers(self):
        """测试不同序列化器的兼容性"""
        for serializer_name in get_test_serializers():
            with self.subTest(serializer=serializer_name):
                # 创建使用相同序列化器的推送器和拉取器
                pusher = Pusher(
                    address=get_test_address('pushpull', 'pusher'),
                    serializer=serializer_name
                )
                puller = Puller(
                    address=get_test_address('pushpull', 'puller'),
                    serializer=serializer_name
                )
                
                try:
                    pusher.connect()
                    puller.connect()
                    
                    time.sleep(0.2)
                    
                    # 推送和拉取任务
                    test_task = get_test_data('simple')
                    pusher.push_task(test_task)
                    
                    received_task = puller.pull_task(timeout=2000)
                    
                    self.assertEqual(received_task, test_task)
                    
                finally:
                    pusher.disconnect()
                    puller.disconnect()
    
    def test_error_handling(self):
        """测试错误处理"""
        # 注册会抛出异常的处理器
        def error_handler(task):
            if task.get('should_error'):
                raise ValueError("测试异常")
            return {'processed': task}
        
        self.puller.set_default_task_handler(error_handler)
        
        # 连接
        self.pusher.connect()
        self.puller.connect()
        
        time.sleep(0.2)
        
        # 推送正常任务和错误任务
        normal_task = {'data': 'normal', 'should_error': False}
        error_task = {'data': 'error', 'should_error': True}
        
        self.pusher.push_task(normal_task)
        self.pusher.push_task(error_task)
        
        # 处理任务
        # 正常任务
        task1 = self.puller.pull_task(timeout=2000)
        result1 = self.puller.process_task(task1)
        self.assertIsNotNone(result1)
        self.assertEqual(result1['processed'], normal_task)
        
        # 错误任务
        task2 = self.puller.pull_task(timeout=2000)
        result2 = self.puller.process_task(task2)
        self.assertIsNone(result2)  # 处理失败应该返回None


if __name__ == '__main__':
    unittest.main()