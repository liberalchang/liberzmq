性能指南
========

本指南提供了使用LiberZMQ时的性能优化技巧和最佳实践，帮助您构建高性能的消息传递系统。

性能基准
--------

LiberZMQ基于ZeroMQ构建，继承了其优秀的性能特性：

* **高吞吐量**: 单机可达百万级消息/秒
* **低延迟**: 微秒级消息传递延迟
* **零拷贝**: 减少内存拷贝开销
* **无锁队列**: 避免锁竞争，提高并发性能

序列化器性能对比
----------------

不同序列化器的性能特点：

.. list-table:: 序列化器性能对比
   :header-rows: 1
   :widths: 20 20 20 20 20

   * - 序列化器
     - 序列化速度
     - 反序列化速度
     - 数据大小
     - 适用场景
   * - Binary
     - 最快
     - 最快
     - 最小
     - 简单数据类型
   * - MessagePack
     - 快
     - 快
     - 小
     - 通用场景
   * - JSON
     - 中等
     - 中等
     - 中等
     - 调试和开发
   * - ProtoBuf
     - 快
     - 快
     - 小
     - 跨语言通信

选择建议
~~~~~~~~

.. code-block:: python

   # 高性能场景，使用二进制序列化器
   publisher = Publisher(address="tcp://*:5555", serializer='binary')
   
   # 通用场景，使用MessagePack
   publisher = Publisher(address="tcp://*:5555", serializer='msgpack')
   
   # 调试阶段，使用JSON
   publisher = Publisher(address="tcp://*:5555", serializer='json')
   
   # 跨语言通信，使用ProtoBuf
   publisher = Publisher(address="tcp://*:5555", serializer='protobuf')

消息模式性能特点
----------------

发布-订阅模式
~~~~~~~~~~~~~

**优势**:
- 一对多广播，高效的消息分发
- 订阅者可以动态加入和离开
- 支持主题过滤，减少不必要的消息传输

**性能优化**:

.. code-block:: python

   # 使用具体的主题，避免通配符
   subscriber.subscribe("news.sports", handler)  # 好
   subscriber.subscribe("news.*", handler)       # 避免
   
   # 批量发布消息
   messages = [("news", msg1), ("weather", msg2), ("sports", msg3)]
   publisher.publish_batch(messages)

请求-响应模式
~~~~~~~~~~~~~

**优势**:
- 同步通信，简单直观
- 自动的请求-响应匹配
- 支持超时控制

**性能优化**:

.. code-block:: python

   # 复用客户端连接
   client = Client(address="tcp://localhost:5556")
   client.connect()
   
   for i in range(1000):
       response = client.send_request('process', {'data': i})
   
   client.disconnect()  # 批量处理完后再断开
   
   # 设置合理的超时时间
   response = client.send_request('process', data, timeout=1000)

推拉模式
~~~~~~~~

**优势**:
- 自动负载均衡
- 高吞吐量的任务分发
- 支持多个工作器并行处理

**性能优化**:

.. code-block:: python

   # 批量推送任务
   tasks = [task1, task2, task3, ...]
   pusher.push_batch(tasks)
   
   # 多个工作器并行处理
   import multiprocessing
   
   def worker_process():
       puller = Puller(address="tcp://localhost:5557")
       puller.connect()
       puller.start_worker()
   
   # 启动多个工作器进程
   processes = []
   for i in range(multiprocessing.cpu_count()):
       p = multiprocessing.Process(target=worker_process)
       p.start()
       processes.append(p)

Dealer/Router模式
~~~~~~~~~~~~~~~~~

**优势**:
- 异步通信，高并发性能
- 支持多对多通信
- 灵活的消息路由

**性能优化**:

.. code-block:: python

   # 异步发送请求，提高并发性
   dealer = Dealer(address="tcp://localhost:5558")
   dealer.connect()
   
   # 并发发送多个请求
   callbacks = []
   for i in range(100):
       def callback(response, request_id=i):
           print(f"请求 {request_id} 完成: {response}")
       
       dealer.send_request_async({'data': i}, callback)
       callbacks.append(callback)
   
   # 等待所有请求完成
   dealer.wait_for_responses()

配置优化
--------

ZeroMQ配置优化
~~~~~~~~~~~~~~

.. code-block:: python

   from liberzmq.core import get_config
   
   # 获取配置对象
   config = get_config()
   
   # 优化I/O线程数（通常设置为CPU核心数）
   config.zmq.io_threads = 4
   
   # 增加高水位标记，提高缓冲能力
   config.zmq.high_water_mark = 10000
   
   # 调整socket超时时间
   config.zmq.socket_timeout = 5000
   
   # 设置合适的linger时间
   config.zmq.linger = 1000

连接优化
~~~~~~~~

.. code-block:: python

   # 使用连接池管理多个连接
   class ConnectionPool:
       def __init__(self, address, pool_size=10):
           self.address = address
           self.pool = []
           for _ in range(pool_size):
               client = Client(address=address)
               client.connect()
               self.pool.append(client)
       
       def get_client(self):
           return self.pool.pop() if self.pool else None
       
       def return_client(self, client):
           self.pool.append(client)
   
   # 使用连接池
   pool = ConnectionPool("tcp://localhost:5556")
   client = pool.get_client()
   response = client.send_request('process', data)
   pool.return_client(client)

内存优化
--------

消息大小控制
~~~~~~~~~~~~

.. code-block:: python

   # 避免发送过大的消息
   MAX_MESSAGE_SIZE = 1024 * 1024  # 1MB
   
   def send_large_data(data):
       if len(data) > MAX_MESSAGE_SIZE:
           # 分块发送
           chunks = [data[i:i+MAX_MESSAGE_SIZE] 
                    for i in range(0, len(data), MAX_MESSAGE_SIZE)]
           for i, chunk in enumerate(chunks):
               publisher.publish("data_chunk", {
                   "chunk_id": i,
                   "total_chunks": len(chunks),
                   "data": chunk
               })
       else:
           publisher.publish("data", data)

对象复用
~~~~~~~~

.. code-block:: python

   # 复用消息对象
   class MessagePool:
       def __init__(self):
           self.pool = []
       
       def get_message(self):
           return self.pool.pop() if self.pool else {}
       
       def return_message(self, msg):
           msg.clear()
           self.pool.append(msg)
   
   # 使用对象池
   msg_pool = MessagePool()
   
   for i in range(1000):
       msg = msg_pool.get_message()
       msg['id'] = i
       msg['data'] = f'message_{i}'
       publisher.publish("test", msg)
       msg_pool.return_message(msg)

监控和调试
----------

性能监控
~~~~~~~~

.. code-block:: python

   from liberzmq.logging import get_logger
   import time
   
   logger = get_logger(__name__)
   
   # 监控消息处理性能
   def timed_handler(func):
       def wrapper(*args, **kwargs):
           start_time = time.time()
           result = func(*args, **kwargs)
           end_time = time.time()
           logger.info(f"处理时间: {(end_time - start_time) * 1000:.2f}ms")
           return result
       return wrapper
   
   @timed_handler
   def message_handler(topic, message):
       # 处理消息
       pass

性能分析
~~~~~~~~

.. code-block:: python

   import cProfile
   import pstats
   
   # 性能分析
   def profile_performance():
       profiler = cProfile.Profile()
       profiler.enable()
       
       # 执行性能测试代码
       for i in range(10000):
           publisher.publish("test", {"id": i})
       
       profiler.disable()
       
       # 分析结果
       stats = pstats.Stats(profiler)
       stats.sort_stats('cumulative')
       stats.print_stats(10)

最佳实践总结
------------

1. **选择合适的序列化器**: 根据性能需求和数据特点选择
2. **复用连接**: 避免频繁的连接建立和断开
3. **批量处理**: 尽可能批量发送和处理消息
4. **合理配置**: 根据硬件和网络环境调整ZeroMQ参数
5. **监控性能**: 持续监控和优化系统性能
6. **内存管理**: 控制消息大小，复用对象
7. **异步处理**: 在适当的场景使用异步模式
8. **负载均衡**: 使用多个工作器并行处理任务

故障排查
--------

常见性能问题
~~~~~~~~~~~~

**消息积压**
- 检查高水位标记设置
- 增加工作器数量
- 优化消息处理逻辑

**连接超时**
- 检查网络连接
- 调整超时时间设置
- 检查防火墙配置

**内存泄漏**
- 检查对象生命周期
- 使用内存分析工具
- 及时释放资源

**CPU使用率高**
- 检查序列化器选择
- 优化消息处理算法
- 调整I/O线程数