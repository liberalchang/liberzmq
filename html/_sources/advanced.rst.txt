高级用法
========

本指南介绍LiberZMQ的高级功能和复杂使用场景，帮助您构建更强大和灵活的消息传递系统。

自定义序列化器
--------------

创建自定义序列化器
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from liberzmq.serializers.base import BaseSerializer
   import pickle
   import gzip
   
   class CompressedPickleSerializer(BaseSerializer):
       """压缩的Pickle序列化器"""
       
       def serialize(self, data) -> bytes:
           """序列化数据并压缩"""
           pickled_data = pickle.dumps(data)
           compressed_data = gzip.compress(pickled_data)
           return compressed_data
       
       def deserialize(self, data: bytes):
           """解压缩并反序列化数据"""
           decompressed_data = gzip.decompress(data)
           return pickle.loads(decompressed_data)
   
   # 注册自定义序列化器
   from liberzmq.serializers.factory import SerializerFactory
   SerializerFactory.register('compressed_pickle', CompressedPickleSerializer)
   
   # 使用自定义序列化器
   publisher = Publisher(
       address="tcp://*:5555", 
       serializer='compressed_pickle'
   )

数据校验集成
------------

使用Pydantic进行数据校验
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pydantic import BaseModel, ValidationError
   from liberzmq.patterns import Publisher, Subscriber
   from liberzmq.exceptions import ValidationError as LiberZMQValidationError
   
   # 定义数据模型
   class UserMessage(BaseModel):
       user_id: int
       username: str
       email: str
       age: int
       
       class Config:
           min_anystr_length = 1
           validate_assignment = True
   
   class ValidatedPublisher(Publisher):
       """带数据校验的发布者"""
       
       def __init__(self, *args, **kwargs):
           super().__init__(*args, **kwargs)
           self.message_models = {}
       
       def register_message_model(self, topic: str, model_class):
           """注册主题的数据模型"""
           self.message_models[topic] = model_class
       
       def publish(self, topic: str, message):
           """发布消息前进行数据校验"""
           if topic in self.message_models:
               try:
                   # 验证数据
                   model_class = self.message_models[topic]
                   validated_message = model_class(**message)
                   # 发布验证后的数据
                   super().publish(topic, validated_message.dict())
               except ValidationError as e:
                   raise LiberZMQValidationError(f"数据校验失败: {e}")
           else:
               super().publish(topic, message)
   
   # 使用带校验的发布者
   publisher = ValidatedPublisher(address="tcp://*:5555")
   publisher.connect()
   publisher.register_message_model('user', UserMessage)
   
   # 发布有效数据
   publisher.publish('user', {
       'user_id': 1,
       'username': 'john_doe',
       'email': 'john@example.com',
       'age': 25
   })
   
   # 发布无效数据会抛出异常
   try:
       publisher.publish('user', {
           'user_id': 'invalid',  # 应该是整数
           'username': '',        # 不能为空
           'email': 'invalid',    # 无效邮箱
           'age': -1             # 年龄不能为负
       })
   except LiberZMQValidationError as e:
       print(f"数据校验失败: {e}")

使用Cerberus进行数据校验
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from cerberus import Validator
   from liberzmq.patterns import Server
   
   class ValidatedServer(Server):
       """带数据校验的服务器"""
       
       def __init__(self, *args, **kwargs):
           super().__init__(*args, **kwargs)
           self.validators = {}
       
       def register_validator(self, action: str, schema: dict):
           """注册动作的数据校验模式"""
           self.validators[action] = Validator(schema)
       
       def register_handler(self, action: str, handler, schema: dict = None):
           """注册带校验的处理器"""
           if schema:
               self.register_validator(action, schema)
           
           def validated_handler(data):
               if action in self.validators:
                   validator = self.validators[action]
                   if not validator.validate(data):
                       return {'error': f'数据校验失败: {validator.errors}'}
               return handler(data)
           
           super().register_handler(action, validated_handler)
   
   # 使用带校验的服务器
   server = ValidatedServer(address="tcp://*:5556")
   server.connect()
   
   # 定义校验模式
   calculate_schema = {
       'a': {'type': 'number', 'required': True},
       'b': {'type': 'number', 'required': True},
       'operation': {'type': 'string', 'allowed': ['add', 'subtract', 'multiply', 'divide']}
   }
   
   def calculate_handler(data):
       a, b = data['a'], data['b']
       operation = data.get('operation', 'add')
       
       if operation == 'add':
           return {'result': a + b}
       elif operation == 'subtract':
           return {'result': a - b}
       elif operation == 'multiply':
           return {'result': a * b}
       elif operation == 'divide':
           if b == 0:
               return {'error': '除数不能为零'}
           return {'result': a / b}
   
   server.register_handler('calculate', calculate_handler, calculate_schema)
   server.start_server()

中间件系统
----------

消息中间件
~~~~~~~~~~

.. code-block:: python

   from liberzmq.patterns import Publisher
   import time
   import json
   
   class MiddlewarePublisher(Publisher):
       """支持中间件的发布者"""
       
       def __init__(self, *args, **kwargs):
           super().__init__(*args, **kwargs)
           self.middlewares = []
       
       def add_middleware(self, middleware):
           """添加中间件"""
           self.middlewares.append(middleware)
       
       def publish(self, topic: str, message):
           """通过中间件处理后发布消息"""
           # 应用中间件
           for middleware in self.middlewares:
               topic, message = middleware.process_outgoing(topic, message)
           
           super().publish(topic, message)
   
   class TimestampMiddleware:
       """添加时间戳的中间件"""
       
       def process_outgoing(self, topic, message):
           if isinstance(message, dict):
               message['timestamp'] = time.time()
           return topic, message
   
   class LoggingMiddleware:
       """日志记录中间件"""
       
       def __init__(self, logger):
           self.logger = logger
       
       def process_outgoing(self, topic, message):
           self.logger.info(f"发送消息到主题 {topic}: {json.dumps(message, ensure_ascii=False)}")
           return topic, message
   
   class CompressionMiddleware:
       """消息压缩中间件"""
       
       def process_outgoing(self, topic, message):
           if isinstance(message, dict) and len(str(message)) > 1000:
               # 对大消息进行压缩标记
               message['_compressed'] = True
           return topic, message
   
   # 使用中间件
   from liberzmq.logging import get_logger
   
   publisher = MiddlewarePublisher(address="tcp://*:5555")
   publisher.connect()
   
   # 添加中间件
   publisher.add_middleware(TimestampMiddleware())
   publisher.add_middleware(LoggingMiddleware(get_logger(__name__)))
   publisher.add_middleware(CompressionMiddleware())
   
   # 发布消息
   publisher.publish('news', {
       'title': '重要新闻',
       'content': '这是一条重要新闻内容'
   })

连接池管理
----------

高级连接池
~~~~~~~~~~

.. code-block:: python

   import threading
   import queue
   import time
   from contextlib import contextmanager
   from liberzmq.patterns import Client
   
   class ConnectionPool:
       """线程安全的连接池"""
       
       def __init__(self, address, min_size=5, max_size=20, timeout=30):
           self.address = address
           self.min_size = min_size
           self.max_size = max_size
           self.timeout = timeout
           
           self._pool = queue.Queue(maxsize=max_size)
           self._created_connections = 0
           self._lock = threading.Lock()
           
           # 预创建最小连接数
           for _ in range(min_size):
               self._create_connection()
       
       def _create_connection(self):
           """创建新连接"""
           client = Client(address=self.address)
           client.connect()
           self._pool.put(client)
           self._created_connections += 1
           return client
       
       def get_connection(self):
           """获取连接"""
           try:
               # 尝试从池中获取连接
               client = self._pool.get(timeout=1)
               return client
           except queue.Empty:
               # 池中没有可用连接，创建新连接
               with self._lock:
                   if self._created_connections < self.max_size:
                       return self._create_connection()
                   else:
                       # 达到最大连接数，等待
                       return self._pool.get(timeout=self.timeout)
       
       def return_connection(self, client):
           """归还连接"""
           try:
               self._pool.put_nowait(client)
           except queue.Full:
               # 池已满，关闭连接
               client.disconnect()
               with self._lock:
                   self._created_connections -= 1
       
       @contextmanager
       def connection(self):
           """上下文管理器"""
           client = self.get_connection()
           try:
               yield client
           finally:
               self.return_connection(client)
       
       def close_all(self):
           """关闭所有连接"""
           while not self._pool.empty():
               try:
                   client = self._pool.get_nowait()
                   client.disconnect()
               except queue.Empty:
                   break
           self._created_connections = 0
   
   # 使用连接池
   pool = ConnectionPool("tcp://localhost:5556")
   
   # 方式1：手动管理
   client = pool.get_connection()
   try:
       response = client.send_request('process', {'data': 'test'})
       print(response)
   finally:
       pool.return_connection(client)
   
   # 方式2：使用上下文管理器
   with pool.connection() as client:
       response = client.send_request('process', {'data': 'test'})
       print(response)
   
   # 清理
   pool.close_all()

负载均衡和故障转移
------------------

客户端负载均衡
~~~~~~~~~~~~~~

.. code-block:: python

   import random
   import time
   from liberzmq.patterns import Client
   from liberzmq.exceptions import ConnectionError, TimeoutError
   
   class LoadBalancedClient:
       """负载均衡客户端"""
       
       def __init__(self, addresses, strategy='round_robin'):
           self.addresses = addresses
           self.strategy = strategy
           self.clients = {}
           self.current_index = 0
           self.failed_addresses = set()
           
           # 初始化客户端
           for address in addresses:
               try:
                   client = Client(address=address)
                   client.connect()
                   self.clients[address] = client
               except ConnectionError:
                   self.failed_addresses.add(address)
       
       def _get_next_address(self):
           """根据策略获取下一个地址"""
           available_addresses = [addr for addr in self.addresses 
                                if addr not in self.failed_addresses]
           
           if not available_addresses:
               raise ConnectionError("没有可用的服务器")
           
           if self.strategy == 'round_robin':
               address = available_addresses[self.current_index % len(available_addresses)]
               self.current_index += 1
               return address
           elif self.strategy == 'random':
               return random.choice(available_addresses)
           else:
               return available_addresses[0]
       
       def send_request(self, action, data, timeout=5000, retry_count=3):
           """发送请求，支持重试和故障转移"""
           last_exception = None
           
           for attempt in range(retry_count):
               try:
                   address = self._get_next_address()
                   client = self.clients[address]
                   return client.send_request(action, data, timeout=timeout)
               
               except (ConnectionError, TimeoutError) as e:
                   last_exception = e
                   # 标记地址为失败
                   if address in self.clients:
                       self.failed_addresses.add(address)
                   
                   # 等待后重试
                   if attempt < retry_count - 1:
                       time.sleep(0.1 * (2 ** attempt))  # 指数退避
           
           raise last_exception
       
       def health_check(self):
           """健康检查，恢复可用的服务器"""
           for address in list(self.failed_addresses):
               try:
                   if address not in self.clients:
                       client = Client(address=address)
                       client.connect()
                       self.clients[address] = client
                   
                   # 发送健康检查请求
                   self.clients[address].send_request('ping', {}, timeout=1000)
                   self.failed_addresses.remove(address)
               except (ConnectionError, TimeoutError):
                   pass
       
       def disconnect_all(self):
           """断开所有连接"""
           for client in self.clients.values():
               client.disconnect()
   
   # 使用负载均衡客户端
   addresses = [
       "tcp://server1:5556",
       "tcp://server2:5556",
       "tcp://server3:5556"
   ]
   
   client = LoadBalancedClient(addresses, strategy='round_robin')
   
   # 发送请求
   try:
       response = client.send_request('process', {'data': 'test'})
       print(response)
   except ConnectionError as e:
       print(f"所有服务器都不可用: {e}")
   
   # 定期健康检查
   import threading
   
   def periodic_health_check():
       while True:
           client.health_check()
           time.sleep(30)  # 每30秒检查一次
   
   health_thread = threading.Thread(target=periodic_health_check, daemon=True)
   health_thread.start()

消息重试和死信队列
------------------

重试机制
~~~~~~~~

.. code-block:: python

   import time
   import json
   from liberzmq.patterns import Puller, Publisher
   from liberzmq.exceptions import TimeoutError
   
   class RetryablePuller(Puller):
       """支持重试的任务拉取器"""
       
       def __init__(self, *args, **kwargs):
           super().__init__(*args, **kwargs)
           self.max_retries = kwargs.get('max_retries', 3)
           self.retry_delay = kwargs.get('retry_delay', 1.0)
           self.dead_letter_publisher = None
       
       def set_dead_letter_queue(self, publisher):
           """设置死信队列"""
           self.dead_letter_publisher = publisher
       
       def register_task_handler(self, task_type, handler):
           """注册带重试的任务处理器"""
           def retry_wrapper(task):
               retry_count = task.get('_retry_count', 0)
               
               try:
                   # 执行原始处理器
                   result = handler(task)
                   return result
               
               except Exception as e:
                   retry_count += 1
                   
                   if retry_count <= self.max_retries:
                       # 重试
                       task['_retry_count'] = retry_count
                       task['_last_error'] = str(e)
                       task['_retry_at'] = time.time() + (self.retry_delay * retry_count)
                       
                       # 延迟后重新推送任务
                       time.sleep(self.retry_delay * retry_count)
                       self._retry_task(task)
                       
                       return {'status': 'retrying', 'retry_count': retry_count}
                   else:
                       # 超过最大重试次数，发送到死信队列
                       if self.dead_letter_publisher:
                           dead_letter_task = {
                               'original_task': task,
                               'final_error': str(e),
                               'retry_count': retry_count,
                               'failed_at': time.time()
                           }
                           self.dead_letter_publisher.publish('dead_letter', dead_letter_task)
                       
                       return {'status': 'failed', 'error': str(e), 'retry_count': retry_count}
           
           super().register_task_handler(task_type, retry_wrapper)
       
       def _retry_task(self, task):
           """重新推送任务"""
           # 这里需要一个推送器来重新发送任务
           # 实际实现中可能需要更复杂的重试队列机制
           pass
   
   # 使用重试机制
   puller = RetryablePuller(
       address="tcp://localhost:5557",
       max_retries=3,
       retry_delay=1.0
   )
   puller.connect()
   
   # 设置死信队列
   dead_letter_publisher = Publisher(address="tcp://*:5559")
   dead_letter_publisher.connect()
   puller.set_dead_letter_queue(dead_letter_publisher)
   
   # 注册可能失败的任务处理器
   def unreliable_handler(task):
       if random.random() < 0.7:  # 70%的失败率
           raise Exception("随机失败")
       return {'status': 'success', 'processed': task}
   
   puller.register_task_handler('unreliable_task', unreliable_handler)
   puller.start_worker()

分布式锁
--------

基于ZeroMQ的分布式锁
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import time
   import threading
   import uuid
   from liberzmq.patterns import Client, Server
   
   class DistributedLock:
       """分布式锁实现"""
       
       def __init__(self, lock_server_address, lock_name, timeout=30):
           self.client = Client(address=lock_server_address)
           self.client.connect()
           self.lock_name = lock_name
           self.timeout = timeout
           self.lock_id = None
           self.acquired = False
       
       def acquire(self, blocking=True, timeout=None):
           """获取锁"""
           self.lock_id = str(uuid.uuid4())
           start_time = time.time()
           
           while True:
               try:
                   response = self.client.send_request('acquire_lock', {
                       'lock_name': self.lock_name,
                       'lock_id': self.lock_id,
                       'timeout': self.timeout
                   })
                   
                   if response.get('acquired'):
                       self.acquired = True
                       return True
                   
                   if not blocking:
                       return False
                   
                   if timeout and (time.time() - start_time) > timeout:
                       return False
                   
                   time.sleep(0.1)  # 短暂等待后重试
               
               except Exception:
                   if not blocking:
                       return False
                   time.sleep(0.1)
       
       def release(self):
           """释放锁"""
           if self.acquired and self.lock_id:
               try:
                   self.client.send_request('release_lock', {
                       'lock_name': self.lock_name,
                       'lock_id': self.lock_id
                   })
                   self.acquired = False
               except Exception:
                   pass
       
       def __enter__(self):
           self.acquire()
           return self
       
       def __exit__(self, exc_type, exc_val, exc_tb):
           self.release()
   
   class LockServer:
       """锁服务器"""
       
       def __init__(self, address):
           self.server = Server(address=address)
           self.locks = {}  # {lock_name: {'lock_id': str, 'expires_at': float}}
           self.lock = threading.Lock()
       
       def start(self):
           self.server.connect()
           self.server.register_handler('acquire_lock', self._acquire_lock)
           self.server.register_handler('release_lock', self._release_lock)
           
           # 启动清理过期锁的线程
           cleanup_thread = threading.Thread(target=self._cleanup_expired_locks, daemon=True)
           cleanup_thread.start()
           
           self.server.start_server()
       
       def _acquire_lock(self, data):
           lock_name = data['lock_name']
           lock_id = data['lock_id']
           timeout = data.get('timeout', 30)
           
           with self.lock:
               current_time = time.time()
               
               # 检查锁是否存在且未过期
               if lock_name in self.locks:
                   existing_lock = self.locks[lock_name]
                   if existing_lock['expires_at'] > current_time:
                       return {'acquired': False, 'reason': 'lock_held'}
                   else:
                       # 锁已过期，删除
                       del self.locks[lock_name]
               
               # 获取锁
               self.locks[lock_name] = {
                   'lock_id': lock_id,
                   'expires_at': current_time + timeout
               }
               
               return {'acquired': True, 'lock_id': lock_id}
       
       def _release_lock(self, data):
           lock_name = data['lock_name']
           lock_id = data['lock_id']
           
           with self.lock:
               if lock_name in self.locks:
                   existing_lock = self.locks[lock_name]
                   if existing_lock['lock_id'] == lock_id:
                       del self.locks[lock_name]
                       return {'released': True}
               
               return {'released': False, 'reason': 'lock_not_found_or_mismatch'}
       
       def _cleanup_expired_locks(self):
           """清理过期的锁"""
           while True:
               time.sleep(5)  # 每5秒检查一次
               current_time = time.time()
               
               with self.lock:
                   expired_locks = []
                   for lock_name, lock_info in self.locks.items():
                       if lock_info['expires_at'] <= current_time:
                           expired_locks.append(lock_name)
                   
                   for lock_name in expired_locks:
                       del self.locks[lock_name]
   
   # 使用分布式锁
   # 首先启动锁服务器（在单独的进程中）
   # lock_server = LockServer("tcp://*:5560")
   # lock_server.start()
   
   # 在客户端使用锁
   with DistributedLock("tcp://localhost:5560", "resource_1") as lock:
       print("获得锁，执行关键代码")
       time.sleep(2)
       print("释放锁")

总结
----

本指南介绍了LiberZMQ的高级功能，包括：

1. **自定义序列化器**: 创建适合特定需求的序列化方案
2. **数据校验**: 集成Pydantic和Cerberus进行数据验证
3. **中间件系统**: 实现消息处理的横切关注点
4. **连接池管理**: 优化连接资源的使用
5. **负载均衡**: 实现客户端负载均衡和故障转移
6. **重试机制**: 处理临时故障和死信队列
7. **分布式锁**: 实现分布式环境下的资源同步

这些高级功能可以帮助您构建更加健壮、高性能和可扩展的分布式系统。