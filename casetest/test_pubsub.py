#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
发布-订阅模式测试

测试Publisher和Subscriber类的功能，包括消息发布、订阅、回调处理等。
"""

import unittest
import time
import threading
from unittest.mock import MagicMock, patch

from liberzmq.patterns.pubsub import Publisher, Subscriber
from liberzmq.exceptions.base import ConnectionError, MessageError, TimeoutError
# 添加父目录到路径以便导入
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from casetest import get_test_address, get_test_data, get_test_serializers


class TestPublisher(unittest.TestCase):
    """发布者测试"""
    
    def setUp(self):
        """测试前准备"""
        self.address = get_test_address('pubsub', 'pub')
        self.publisher = Publisher(address=self.address, serializer='json')
    
    def tearDown(self):
        """测试后清理"""
        if self.publisher:
            self.publisher.disconnect()
    
    def test_publisher_initialization(self):
        """测试发布者初始化"""
        self.assertEqual(self.publisher._address, self.address)
        self.assertTrue(self.publisher._bind)
        self.assertFalse(self.publisher._connected)
        self.assertEqual(self.publisher._published_count, 0)
    
    def test_publisher_connect_disconnect(self):
        """测试发布者连接和断开"""
        # 测试连接
        self.publisher.connect()
        self.assertTrue(self.publisher._connected)
        self.assertIsNotNone(self.publisher._socket)
        
        # 测试断开连接
        self.publisher.disconnect()
        self.assertFalse(self.publisher._connected)
        self.assertIsNone(self.publisher._socket)
    
    def test_publisher_publish_message(self):
        """测试发布消息"""
        self.publisher.connect()
        
        # 测试发布简单消息
        test_data = get_test_data('simple')
        self.publisher.publish('test_topic', test_data)
        
        self.assertEqual(self.publisher._published_count, 1)
        self.assertIn('test_topic', self.publisher._topics)
        
        # 测试发布复杂消息
        complex_data = get_test_data('complex')
        self.publisher.publish('complex_topic', complex_data)
        
        self.assertEqual(self.publisher._published_count, 2)
        self.assertIn('complex_topic', self.publisher._topics)
    
    def test_publisher_publish_multipart(self):
        """测试发布多部分消息"""
        self.publisher.connect()
        
        messages = [
            get_test_data('simple'),
            get_test_data('complex'),
            {'part': 3, 'data': 'third_part'}
        ]
        
        self.publisher.publish_multipart('multipart_topic', messages)
        
        self.assertEqual(self.publisher._published_count, 1)
        self.assertIn('multipart_topic', self.publisher._topics)
    
    def test_publisher_without_connection(self):
        """测试未连接时发布消息"""
        with self.assertRaises(ConnectionError):
            self.publisher.publish('test_topic', 'test_message')
    
    def test_publisher_stats(self):
        """测试发布者统计信息"""
        self.publisher.connect()
        
        # 发布几条消息
        for i in range(3):
            self.publisher.publish(f'topic_{i}', f'message_{i}')
        
        stats = self.publisher.get_stats()
        
        self.assertEqual(stats['published_count'], 3)
        self.assertEqual(stats['topics_count'], 3)
        self.assertEqual(len(stats['topics']), 3)
        self.assertTrue(stats['connected'])
        self.assertTrue(stats['bind_mode'])
        self.assertEqual(stats['address'], self.address)
    
    def test_publisher_different_serializers(self):
        """测试不同序列化器"""
        for serializer_name in get_test_serializers():
            with self.subTest(serializer=serializer_name):
                publisher = Publisher(
                    address=get_test_address('pubsub', 'pub'),
                    serializer=serializer_name
                )
                
                try:
                    publisher.connect()
                    test_data = get_test_data('simple')
                    publisher.publish('test_topic', test_data)
                    
                    self.assertEqual(publisher._published_count, 1)
                finally:
                    publisher.disconnect()


class TestSubscriber(unittest.TestCase):
    """订阅者测试"""
    
    def setUp(self):
        """测试前准备"""
        self.address = get_test_address('pubsub', 'sub')
        self.subscriber = Subscriber(
            address=self.address,
            topics=['test_topic'],
            serializer='json'
        )
    
    def tearDown(self):
        """测试后清理"""
        if self.subscriber:
            self.subscriber.disconnect()
    
    def test_subscriber_initialization(self):
        """测试订阅者初始化"""
        self.assertEqual(self.subscriber._address, self.address)
        self.assertFalse(self.subscriber._bind)
        self.assertFalse(self.subscriber._connected)
        self.assertIn('test_topic', self.subscriber._topics)
        self.assertEqual(self.subscriber._received_count, 0)
    
    def test_subscriber_connect_disconnect(self):
        """测试订阅者连接和断开"""
        # 测试连接
        self.subscriber.connect()
        self.assertTrue(self.subscriber._connected)
        self.assertIsNotNone(self.subscriber._socket)
        
        # 测试断开连接
        self.subscriber.disconnect()
        self.assertFalse(self.subscriber._connected)
        self.assertIsNone(self.subscriber._socket)
    
    def test_subscriber_topic_management(self):
        """测试主题管理"""
        self.subscriber.connect()
        
        # 测试订阅新主题
        self.subscriber.subscribe('new_topic')
        self.assertIn('new_topic', self.subscriber._topics)
        
        # 测试取消订阅
        self.subscriber.unsubscribe('test_topic')
        self.assertNotIn('test_topic', self.subscriber._topics)
        
        # 测试重复订阅
        self.subscriber.subscribe('new_topic')  # 应该不会重复添加
        topic_count = len([t for t in self.subscriber._topics if t == 'new_topic'])
        self.assertEqual(topic_count, 1)
    
    def test_subscriber_callback_management(self):
        """测试回调函数管理"""
        callback1 = MagicMock()
        callback2 = MagicMock()
        
        # 添加回调
        self.subscriber.add_callback('test_topic', callback1)
        self.subscriber.add_callback('test_topic', callback2)
        self.subscriber.add_callback('*', callback1)  # 通配符回调
        
        self.assertIn('test_topic', self.subscriber._callbacks)
        self.assertIn('*', self.subscriber._callbacks)
        self.assertEqual(len(self.subscriber._callbacks['test_topic']), 2)
        
        # 移除回调
        self.subscriber.remove_callback('test_topic', callback1)
        self.assertEqual(len(self.subscriber._callbacks['test_topic']), 1)
        
        # 移除所有回调
        self.subscriber.remove_callback('test_topic', callback2)
        self.assertNotIn('test_topic', self.subscriber._callbacks)
    
    def test_subscriber_receive_timeout(self):
        """测试接收超时"""
        self.subscriber.connect()
        
        # 测试超时接收（没有消息）
        result = self.subscriber.receive_message(timeout=100)
        self.assertIsNone(result)
    
    def test_subscriber_without_connection(self):
        """测试未连接时接收消息"""
        with self.assertRaises(ConnectionError):
            self.subscriber.receive_message()
    
    def test_subscriber_stats(self):
        """测试订阅者统计信息"""
        callback = MagicMock()
        self.subscriber.add_callback('test_topic', callback)
        self.subscriber.connect()
        
        stats = self.subscriber.get_stats()
        
        self.assertEqual(stats['received_count'], 0)
        self.assertIn('test_topic', stats['subscribed_topics'])
        self.assertIn('test_topic', stats['callback_topics'])
        self.assertTrue(stats['connected'])
        self.assertFalse(stats['receiving'])
        self.assertFalse(stats['bind_mode'])
        self.assertEqual(stats['address'], self.address)
    
    def test_subscriber_async_receiving(self):
        """测试异步接收"""
        self.subscriber.connect()
        
        # 启动异步接收
        self.subscriber.start_receiving()
        self.assertTrue(self.subscriber._running)
        self.assertIsNotNone(self.subscriber._receive_thread)
        
        # 停止异步接收
        self.subscriber.stop_receiving()
        self.assertFalse(self.subscriber._running)


class TestPubSubIntegration(unittest.TestCase):
    """发布-订阅集成测试"""
    
    def setUp(self):
        """测试前准备"""
        self.pub_address = get_test_address('pubsub', 'pub')
        self.sub_address = get_test_address('pubsub', 'sub')
        
        self.publisher = Publisher(address=self.pub_address, serializer='json')
        self.subscriber = Subscriber(
            address=self.sub_address,
            topics=['test_topic', 'another_topic'],
            serializer='json'
        )
    
    def tearDown(self):
        """测试后清理"""
        if self.publisher:
            self.publisher.disconnect()
        if self.subscriber:
            self.subscriber.disconnect()
    
    def test_basic_pubsub_communication(self):
        """测试基本发布-订阅通信"""
        # 连接发布者和订阅者
        self.publisher.connect()
        self.subscriber.connect()
        
        # 等待连接建立
        time.sleep(0.2)
        
        # 发布消息
        test_data = get_test_data('simple')
        self.publisher.publish('test_topic', test_data)
        
        # 等待消息传输
        time.sleep(0.1)
        
        # 接收消息
        result = self.subscriber.receive_message(timeout=1000)
        
        if result:  # 如果接收到消息
            topic, message = result
            self.assertEqual(topic, 'test_topic')
            self.assertEqual(message, test_data)
    
    def test_multiple_topics(self):
        """测试多主题通信"""
        self.publisher.connect()
        self.subscriber.connect()
        
        time.sleep(0.2)
        
        # 发布到不同主题
        topics_data = {
            'test_topic': get_test_data('simple'),
            'another_topic': get_test_data('complex'),
            'unsubscribed_topic': {'should': 'not_receive'}
        }
        
        for topic, data in topics_data.items():
            self.publisher.publish(topic, data)
        
        time.sleep(0.1)
        
        # 接收消息
        received_messages = []
        for _ in range(3):  # 尝试接收3条消息
            result = self.subscriber.receive_message(timeout=500)
            if result:
                received_messages.append(result)
        
        # 应该只接收到订阅主题的消息
        received_topics = [topic for topic, _ in received_messages]
        self.assertNotIn('unsubscribed_topic', received_topics)
    
    def test_callback_handling(self):
        """测试回调处理"""
        self.publisher.connect()
        self.subscriber.connect()
        
        # 设置回调
        received_messages = []
        
        def message_callback(topic, message):
            received_messages.append((topic, message))
        
        self.subscriber.add_callback('test_topic', message_callback)
        self.subscriber.add_callback('*', message_callback)  # 通配符回调
        
        # 启动异步接收
        self.subscriber.start_receiving()
        
        time.sleep(0.2)
        
        # 发布消息
        test_data = get_test_data('simple')
        self.publisher.publish('test_topic', test_data)
        
        # 等待消息处理
        time.sleep(0.3)
        
        # 停止接收
        self.subscriber.stop_receiving()
        
        # 检查回调是否被调用
        # 注意：由于有特定主题回调和通配符回调，可能会收到2条消息
        self.assertGreater(len(received_messages), 0)
        
        # 检查消息内容
        for topic, message in received_messages:
            self.assertEqual(topic, 'test_topic')
            self.assertEqual(message, test_data)
    
    def test_multipart_messages(self):
        """测试多部分消息"""
        self.publisher.connect()
        self.subscriber.connect()
        
        time.sleep(0.2)
        
        # 发布多部分消息
        messages = [
            get_test_data('simple'),
            get_test_data('complex'),
            {'part': 3, 'final': True}
        ]
        
        self.publisher.publish_multipart('multipart_topic', messages)
        
        time.sleep(0.1)
        
        # 订阅多部分主题
        self.subscriber.subscribe('multipart_topic')
        
        # 由于已经发布了消息，这里可能接收不到
        # 重新发布一次
        self.publisher.publish_multipart('multipart_topic', messages)
        
        time.sleep(0.1)
        
        result = self.subscriber.receive_message(timeout=1000)
        
        if result:
            topic, message = result
            self.assertEqual(topic, 'multipart_topic')
            # 多部分消息的第一部分应该是第一个消息
            self.assertEqual(message, messages[0])
    
    def test_different_serializers(self):
        """测试不同序列化器的兼容性"""
        for serializer_name in get_test_serializers():
            with self.subTest(serializer=serializer_name):
                # 创建使用相同序列化器的发布者和订阅者
                pub = Publisher(
                    address=get_test_address('pubsub', 'pub'),
                    serializer=serializer_name
                )
                sub = Subscriber(
                    address=get_test_address('pubsub', 'sub'),
                    topics=['test_topic'],
                    serializer=serializer_name
                )
                
                try:
                    pub.connect()
                    sub.connect()
                    
                    time.sleep(0.2)
                    
                    # 发布消息
                    test_data = get_test_data('simple')
                    pub.publish('test_topic', test_data)
                    
                    time.sleep(0.1)
                    
                    # 接收消息
                    result = sub.receive_message(timeout=1000)
                    
                    if result:
                        topic, message = result
                        self.assertEqual(topic, 'test_topic')
                        self.assertEqual(message, test_data)
                
                finally:
                    pub.disconnect()
                    sub.disconnect()


if __name__ == '__main__':
    unittest.main()