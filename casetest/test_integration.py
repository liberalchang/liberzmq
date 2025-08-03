#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
集成测试

测试不同消息模式之间的交互和整体系统功能，包括混合模式使用、
性能测试、错误恢复等。
"""

import unittest
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

from liberzmq.patterns.pubsub import Publisher, Subscriber
from liberzmq.patterns.reqrep import Server, Client
from liberzmq.patterns.pushpull import Pusher, Puller
from liberzmq.exceptions.base import ConnectionError, MessageError, TimeoutError
from liberzmq.core.config import Config
from liberzmq.serializers import get_serializer

# 添加父目录到路径以便导入
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from casetest import get_test_address, get_test_data, get_test_serializers


class TestMixedPatterns(unittest.TestCase):
    """混合模式测试"""
    
    def setUp(self):
        """测试前准备"""
        self.components = []
    
    def tearDown(self):
        """测试后清理"""
        for component in self.components:
            if hasattr(component, 'disconnect'):
                component.disconnect()
            if hasattr(component, 'stop_server'):
                component.stop_server()
            if hasattr(component, 'stop_worker'):
                component.stop_worker()
    
    def test_pubsub_with_reqrep(self):
        """测试发布-订阅与请求-响应的组合使用"""
        # 创建组件
        publisher = Publisher(address=get_test_address('pubsub', 'publisher'), serializer='json')
        subscriber = Subscriber(address=get_test_address('pubsub', 'subscriber'), serializer='json')
        server = Server(address=get_test_address('reqrep', 'server'), serializer='json')
        client = Client(address=get_test_address('reqrep', 'client'), serializer='json')
        
        self.components.extend([publisher, subscriber, server, client])
        
        # 设置订阅者回调
        received_messages = []
        def message_callback(topic, message):
            received_messages.append((topic, message))
        
        subscriber.subscribe('notifications', message_callback)
        
        # 设置服务器处理器
        def process_request(data):
            # 处理请求并发布通知
            result = {'processed': data, 'timestamp': time.time()}
            publisher.publish('notifications', {
                'type': 'request_processed',
                'result': result
            })
            return result
        
        server.register_handler('process', process_request)
        
        # 连接所有组件
        publisher.connect()
        subscriber.connect()
        server.connect()
        client.connect()
        
        # 启动服务器
        server.start_server()
        
        time.sleep(0.5)
        
        # 发送请求
        request_data = {'task': 'test_task', 'id': 123}
        response = client.send_request('process', request_data, timeout=3000)
        
        # 验证响应
        self.assertIsNotNone(response)
        self.assertEqual(response['processed'], request_data)
        
        # 等待通知
        time.sleep(0.5)
        
        # 验证通知
        self.assertEqual(len(received_messages), 1)
        topic, message = received_messages[0]
        self.assertEqual(topic, 'notifications')
        self.assertEqual(message['type'], 'request_processed')
        self.assertEqual(message['result']['processed'], request_data)
    
    def test_pushpull_with_pubsub(self):
        """测试推拉与发布-订阅的组合使用"""
        # 创建组件
        pusher = Pusher(address=get_test_address('pushpull', 'pusher'), serializer='json')
        puller = Puller(address=get_test_address('pushpull', 'puller'), serializer='json')
        publisher = Publisher(address=get_test_address('pubsub', 'publisher'), serializer='json')
        subscriber = Subscriber(address=get_test_address('pubsub', 'subscriber'), serializer='json')
        
        self.components.extend([pusher, puller, publisher, subscriber])
        
        # 设置订阅者回调
        task_results = []
        def result_callback(topic, message):
            task_results.append(message)
        
        subscriber.subscribe('task_results', result_callback)
        
        # 设置拉取器任务处理器
        def task_processor(task):
            # 处理任务
            result = {
                'task_id': task.get('id'),
                'result': f"processed_{task.get('data')}",
                'timestamp': time.time()
            }
            
            # 发布结果
            publisher.publish('task_results', result)
            return result
        
        puller.set_default_task_handler(task_processor)
        
        # 连接所有组件
        pusher.connect()
        puller.connect()
        publisher.connect()
        subscriber.connect()
        
        # 启动工作器
        puller.start_worker()
        
        time.sleep(0.5)
        
        # 推送任务
        tasks = [
            {'id': 1, 'data': 'task1'},
            {'id': 2, 'data': 'task2'},
            {'id': 3, 'data': 'task3'}
        ]
        
        for task in tasks:
            pusher.push_task(task)
        
        # 等待处理完成
        time.sleep(2)
        
        # 验证结果
        self.assertEqual(len(task_results), len(tasks))
        
        # 验证每个任务的结果
        result_ids = {result['task_id'] for result in task_results}
        expected_ids = {task['id'] for task in tasks}
        self.assertEqual(result_ids, expected_ids)
    
    def test_all_patterns_workflow(self):
        """测试所有模式的工作流"""
        # 创建所有组件
        publisher = Publisher(address=get_test_address('pubsub', 'publisher'), serializer='json')
        subscriber = Subscriber(address=get_test_address('pubsub', 'subscriber'), serializer='json')
        server = Server(address=get_test_address('reqrep', 'server'), serializer='json')
        client = Client(address=get_test_address('reqrep', 'client'), serializer='json')
        pusher = Pusher(address=get_test_address('pushpull', 'pusher'), serializer='json')
        puller = Puller(address=get_test_address('pushpull', 'puller'), serializer='json')
        
        self.components.extend([publisher, subscriber, server, client, pusher, puller])
        
        # 工作流状态跟踪
        workflow_state = {
            'commands_received': [],
            'tasks_created': [],
            'tasks_completed': [],
            'notifications_sent': []
        }
        
        # 设置订阅者（监听通知）
        def notification_callback(topic, message):
            workflow_state['notifications_sent'].append(message)
        
        subscriber.subscribe('workflow', notification_callback)
        
        # 设置服务器（接收命令，创建任务）
        def command_handler(data):
            command = data.get('command')
            workflow_state['commands_received'].append(command)
            
            if command == 'create_tasks':
                # 创建任务
                task_count = data.get('count', 1)
                for i in range(task_count):
                    task = {
                        'id': f"task_{i}",
                        'data': f"data_{i}",
                        'created_by': 'command_handler'
                    }
                    pusher.push_task(task)
                    workflow_state['tasks_created'].append(task)
                
                # 发布通知
                publisher.publish('workflow', {
                    'type': 'tasks_created',
                    'count': task_count
                })
                
                return {'status': 'tasks_created', 'count': task_count}
            
            return {'status': 'unknown_command'}
        
        server.register_handler('workflow', command_handler)
        
        # 设置拉取器（处理任务）
        def task_handler(task):
            # 模拟任务处理
            time.sleep(0.1)
            
            result = {
                'task_id': task['id'],
                'processed_data': f"processed_{task['data']}",
                'status': 'completed'
            }
            
            workflow_state['tasks_completed'].append(result)
            
            # 发布完成通知
            publisher.publish('workflow', {
                'type': 'task_completed',
                'task_id': task['id']
            })
            
            return result
        
        puller.set_default_task_handler(task_handler)
        
        # 连接所有组件
        for component in self.components:
            component.connect()
        
        # 启动服务
        server.start_server()
        puller.start_worker()
        
        time.sleep(0.5)
        
        # 执行工作流
        response = client.send_request('workflow', {
            'command': 'create_tasks',
            'count': 3
        }, timeout=5000)
        
        # 验证命令响应
        self.assertEqual(response['status'], 'tasks_created')
        self.assertEqual(response['count'], 3)
        
        # 等待任务处理完成
        time.sleep(3)
        
        # 验证工作流状态
        self.assertEqual(len(workflow_state['commands_received']), 1)
        self.assertEqual(len(workflow_state['tasks_created']), 3)
        self.assertEqual(len(workflow_state['tasks_completed']), 3)
        self.assertGreaterEqual(len(workflow_state['notifications_sent']), 4)  # 1 tasks_created + 3 task_completed


class TestPerformance(unittest.TestCase):
    """性能测试"""
    
    def setUp(self):
        """测试前准备"""
        self.components = []
    
    def tearDown(self):
        """测试后清理"""
        for component in self.components:
            if hasattr(component, 'disconnect'):
                component.disconnect()
            if hasattr(component, 'stop_server'):
                component.stop_server()
            if hasattr(component, 'stop_worker'):
                component.stop_worker()
    
    def test_pubsub_throughput(self):
        """测试发布-订阅吞吐量"""
        publisher = Publisher(address=get_test_address('pubsub', 'publisher'), serializer='json')
        subscriber = Subscriber(address=get_test_address('pubsub', 'subscriber'), serializer='json')
        
        self.components.extend([publisher, subscriber])
        
        # 消息计数
        received_count = {'value': 0}
        
        def message_callback(topic, message):
            received_count['value'] += 1
        
        subscriber.subscribe('performance', message_callback)
        
        # 连接
        publisher.connect()
        subscriber.connect()
        
        time.sleep(0.5)
        
        # 性能测试
        message_count = 1000
        start_time = time.time()
        
        for i in range(message_count):
            publisher.publish('performance', {'id': i, 'data': f'message_{i}'})
        
        # 等待接收完成
        timeout = 10
        while received_count['value'] < message_count and timeout > 0:
            time.sleep(0.1)
            timeout -= 0.1
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 验证性能
        self.assertEqual(received_count['value'], message_count)
        throughput = message_count / duration
        
        print(f"发布-订阅吞吐量: {throughput:.2f} 消息/秒")
        self.assertGreater(throughput, 100)  # 至少100消息/秒
    
    def test_reqrep_latency(self):
        """测试请求-响应延迟"""
        server = Server(address=get_test_address('reqrep', 'server'), serializer='json')
        client = Client(address=get_test_address('reqrep', 'client'), serializer='json')
        
        self.components.extend([server, client])
        
        # 简单回显处理器
        def echo_handler(data):
            return {'echo': data, 'timestamp': time.time()}
        
        server.register_handler('echo', echo_handler)
        
        # 连接
        server.connect()
        client.connect()
        server.start_server()
        
        time.sleep(0.5)
        
        # 延迟测试
        request_count = 100
        latencies = []
        
        for i in range(request_count):
            start_time = time.time()
            response = client.send_request('echo', {'id': i}, timeout=5000)
            end_time = time.time()
            
            self.assertIsNotNone(response)
            latency = (end_time - start_time) * 1000  # 转换为毫秒
            latencies.append(latency)
        
        # 计算统计信息
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        
        print(f"请求-响应延迟 - 平均: {avg_latency:.2f}ms, 最小: {min_latency:.2f}ms, 最大: {max_latency:.2f}ms")
        
        # 验证延迟
        self.assertLess(avg_latency, 100)  # 平均延迟小于100ms
        self.assertLess(max_latency, 500)  # 最大延迟小于500ms
    
    def test_pushpull_load_balancing(self):
        """测试推拉负载均衡性能"""
        pusher = Pusher(address=get_test_address('pushpull', 'pusher'), serializer='json')
        
        # 创建多个工作器
        worker_count = 3
        workers = []
        processed_by_worker = {}
        
        for i in range(worker_count):
            puller = Puller(
                address=get_test_address('pushpull', f'puller_{i}'),
                serializer='json'
            )
            
            def make_handler(worker_id):
                def handler(task):
                    if worker_id not in processed_by_worker:
                        processed_by_worker[worker_id] = 0
                    processed_by_worker[worker_id] += 1
                    time.sleep(0.01)  # 模拟处理时间
                    return f"processed_by_{worker_id}"
                return handler
            
            puller.set_default_task_handler(make_handler(i))
            puller.connect()
            puller.start_worker()
            workers.append(puller)
        
        self.components.extend([pusher] + workers)
        
        # 连接推送器
        pusher.connect()
        
        time.sleep(0.5)
        
        # 性能测试
        task_count = 300
        start_time = time.time()
        
        for i in range(task_count):
            pusher.push_task({'id': i, 'data': f'task_{i}'})
        
        # 等待处理完成
        while sum(processed_by_worker.values()) < task_count:
            time.sleep(0.1)
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = task_count / duration
        
        print(f"推拉吞吐量: {throughput:.2f} 任务/秒")
        print(f"负载分布: {processed_by_worker}")
        
        # 验证负载均衡
        self.assertEqual(sum(processed_by_worker.values()), task_count)
        
        # 检查负载分布的均匀性
        avg_load = task_count / worker_count
        for worker_id, load in processed_by_worker.items():
            # 每个工作器的负载应该在平均值的50%范围内
            self.assertGreater(load, avg_load * 0.5)
            self.assertLess(load, avg_load * 1.5)


class TestErrorRecovery(unittest.TestCase):
    """错误恢复测试"""
    
    def setUp(self):
        """测试前准备"""
        self.components = []
    
    def tearDown(self):
        """测试后清理"""
        for component in self.components:
            if hasattr(component, 'disconnect'):
                component.disconnect()
            if hasattr(component, 'stop_server'):
                component.stop_server()
            if hasattr(component, 'stop_worker'):
                component.stop_worker()
    
    def test_connection_recovery(self):
        """测试连接恢复"""
        publisher = Publisher(address=get_test_address('pubsub', 'publisher'), serializer='json')
        subscriber = Subscriber(address=get_test_address('pubsub', 'subscriber'), serializer='json')
        
        self.components.extend([publisher, subscriber])
        
        received_messages = []
        def message_callback(topic, message):
            received_messages.append(message)
        
        subscriber.subscribe('test', message_callback)
        
        # 初始连接
        publisher.connect()
        subscriber.connect()
        
        time.sleep(0.2)
        
        # 发送消息
        publisher.publish('test', {'phase': 'before_disconnect'})
        time.sleep(0.2)
        
        # 断开连接
        publisher.disconnect()
        subscriber.disconnect()
        
        # 重新连接
        publisher.connect()
        subscriber.connect()
        
        time.sleep(0.2)
        
        # 发送消息
        publisher.publish('test', {'phase': 'after_reconnect'})
        time.sleep(0.2)
        
        # 验证消息接收
        self.assertGreaterEqual(len(received_messages), 1)
        
        # 检查重连后的消息
        reconnect_messages = [msg for msg in received_messages if msg.get('phase') == 'after_reconnect']
        self.assertGreater(len(reconnect_messages), 0)
    
    def test_server_restart(self):
        """测试服务器重启"""
        server = Server(address=get_test_address('reqrep', 'server'), serializer='json')
        client = Client(address=get_test_address('reqrep', 'client'), serializer='json')
        
        self.components.extend([server, client])
        
        def echo_handler(data):
            return {'echo': data}
        
        server.register_handler('echo', echo_handler)
        
        # 初始启动
        server.connect()
        client.connect()
        server.start_server()
        
        time.sleep(0.5)
        
        # 发送请求
        response1 = client.send_request('echo', {'test': 'before_restart'}, timeout=2000)
        self.assertIsNotNone(response1)
        
        # 重启服务器
        server.stop_server()
        server.disconnect()
        
        time.sleep(0.2)
        
        server.connect()
        server.start_server()
        
        time.sleep(0.5)
        
        # 发送请求
        response2 = client.send_request('echo', {'test': 'after_restart'}, timeout=2000)
        self.assertIsNotNone(response2)
        self.assertEqual(response2['echo']['test'], 'after_restart')
    
    def test_worker_failure_recovery(self):
        """测试工作器故障恢复"""
        pusher = Pusher(address=get_test_address('pushpull', 'pusher'), serializer='json')
        puller = Puller(address=get_test_address('pushpull', 'puller'), serializer='json')
        
        self.components.extend([pusher, puller])
        
        processed_tasks = []
        failure_count = {'value': 0}
        
        def unreliable_handler(task):
            # 模拟随机失败
            if random.random() < 0.3:  # 30% 失败率
                failure_count['value'] += 1
                raise Exception("模拟工作器故障")
            
            processed_tasks.append(task)
            return {'processed': task}
        
        puller.set_default_task_handler(unreliable_handler)
        
        # 连接
        pusher.connect()
        puller.connect()
        puller.start_worker()
        
        time.sleep(0.5)
        
        # 推送任务
        task_count = 50
        for i in range(task_count):
            pusher.push_task({'id': i, 'data': f'task_{i}'})
        
        # 等待处理完成（包括重试）
        time.sleep(5)
        
        print(f"处理成功: {len(processed_tasks)}, 失败: {failure_count['value']}")
        
        # 验证至少有一些任务被处理
        self.assertGreater(len(processed_tasks), 0)
        # 验证确实有一些失败
        self.assertGreater(failure_count['value'], 0)


class TestConfigurationIntegration(unittest.TestCase):
    """配置集成测试"""
    
    def test_global_configuration(self):
        """测试全局配置"""
        # 设置全局配置
        config = Config()
        config.set('zmq.socket_timeout', 5000)
        config.set('serialization.default', 'json')
        config.set('logging.level', 'INFO')
        
        # 创建组件（应该使用全局配置）
        publisher = Publisher(address=get_test_address('pubsub', 'publisher'))
        subscriber = Subscriber(address=get_test_address('pubsub', 'subscriber'))
        
        try:
            # 验证配置被应用
            self.assertEqual(publisher._serializer_name, 'json')
            self.assertEqual(subscriber._serializer_name, 'json')
            
            # 测试功能
            received_messages = []
            def callback(topic, message):
                received_messages.append(message)
            
            subscriber.subscribe('test', callback)
            
            publisher.connect()
            subscriber.connect()
            
            time.sleep(0.2)
            
            publisher.publish('test', {'config_test': True})
            time.sleep(0.2)
            
            self.assertEqual(len(received_messages), 1)
            self.assertTrue(received_messages[0]['config_test'])
            
        finally:
            publisher.disconnect()
            subscriber.disconnect()
    
    def test_serializer_compatibility(self):
        """测试序列化器兼容性"""
        for serializer_name in get_test_serializers():
            with self.subTest(serializer=serializer_name):
                # 测试序列化器创建
                serializer = get_serializer(serializer_name)
                self.assertIsNotNone(serializer)
                
                # 测试序列化和反序列化
                test_data = get_test_data('complex')
                
                try:
                    serialized = serializer.serialize(test_data)
                    deserialized = serializer.deserialize(serialized)
                    
                    # 验证数据一致性
                    if isinstance(test_data, dict):
                        self.assertEqual(deserialized, test_data)
                    else:
                        # 对于某些序列化器，可能会有类型转换
                        self.assertIsNotNone(deserialized)
                        
                except Exception as e:
                    self.fail(f"序列化器 {serializer_name} 失败: {e}")


class TestConcurrency(unittest.TestCase):
    """并发测试"""
    
    def setUp(self):
        """测试前准备"""
        self.components = []
    
    def tearDown(self):
        """测试后清理"""
        for component in self.components:
            if hasattr(component, 'disconnect'):
                component.disconnect()
            if hasattr(component, 'stop_server'):
                component.stop_server()
            if hasattr(component, 'stop_worker'):
                component.stop_worker()
    
    def test_concurrent_publishers(self):
        """测试并发发布者"""
        # 创建多个发布者
        publishers = []
        for i in range(3):
            publisher = Publisher(
                address=get_test_address('pubsub', f'publisher_{i}'),
                serializer='json'
            )
            publisher.connect()
            publishers.append(publisher)
        
        # 创建订阅者
        subscriber = Subscriber(
            address=get_test_address('pubsub', 'subscriber'),
            serializer='json'
        )
        subscriber.connect()
        
        self.components.extend(publishers + [subscriber])
        
        # 收集消息
        received_messages = []
        lock = threading.Lock()
        
        def message_callback(topic, message):
            with lock:
                received_messages.append((topic, message))
        
        subscriber.subscribe('concurrent', message_callback)
        
        time.sleep(0.5)
        
        # 并发发布
        def publish_messages(publisher, publisher_id):
            for i in range(10):
                publisher.publish('concurrent', {
                    'publisher_id': publisher_id,
                    'message_id': i,
                    'data': f'message_{publisher_id}_{i}'
                })
                time.sleep(0.01)
        
        threads = []
        for i, publisher in enumerate(publishers):
            thread = threading.Thread(target=publish_messages, args=(publisher, i))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 等待消息接收
        time.sleep(1)
        
        # 验证消息接收
        expected_count = len(publishers) * 10
        self.assertEqual(len(received_messages), expected_count)
        
        # 验证每个发布者的消息都被接收
        publisher_counts = {}
        for topic, message in received_messages:
            pub_id = message['publisher_id']
            publisher_counts[pub_id] = publisher_counts.get(pub_id, 0) + 1
        
        for i in range(len(publishers)):
            self.assertEqual(publisher_counts[i], 10)
    
    def test_concurrent_clients(self):
        """测试并发客户端"""
        server = Server(address=get_test_address('reqrep', 'server'), serializer='json')
        
        # 创建多个客户端
        clients = []
        for i in range(5):
            client = Client(
                address=get_test_address('reqrep', f'client_{i}'),
                serializer='json'
            )
            client.connect()
            clients.append(client)
        
        self.components.extend([server] + clients)
        
        # 设置服务器处理器
        request_count = {'value': 0}
        lock = threading.Lock()
        
        def concurrent_handler(data):
            with lock:
                request_count['value'] += 1
            
            # 模拟处理时间
            time.sleep(0.1)
            
            return {
                'client_id': data.get('client_id'),
                'request_id': data.get('request_id'),
                'processed_at': time.time()
            }
        
        server.register_handler('concurrent', concurrent_handler)
        
        # 启动服务器
        server.connect()
        server.start_server()
        
        time.sleep(0.5)
        
        # 并发请求
        def send_requests(client, client_id):
            responses = []
            for i in range(5):
                response = client.send_request('concurrent', {
                    'client_id': client_id,
                    'request_id': i
                }, timeout=5000)
                responses.append(response)
            return responses
        
        # 使用线程池执行并发请求
        with ThreadPoolExecutor(max_workers=len(clients)) as executor:
            futures = []
            for i, client in enumerate(clients):
                future = executor.submit(send_requests, client, i)
                futures.append(future)
            
            # 收集所有响应
            all_responses = []
            for future in as_completed(futures):
                responses = future.result()
                all_responses.extend(responses)
        
        # 验证响应
        expected_count = len(clients) * 5
        self.assertEqual(len(all_responses), expected_count)
        self.assertEqual(request_count['value'], expected_count)
        
        # 验证每个客户端的响应
        client_responses = {}
        for response in all_responses:
            client_id = response['client_id']
            if client_id not in client_responses:
                client_responses[client_id] = []
            client_responses[client_id].append(response)
        
        for i in range(len(clients)):
            self.assertEqual(len(client_responses[i]), 5)


if __name__ == '__main__':
    unittest.main()