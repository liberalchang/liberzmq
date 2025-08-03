快速开始
========

本指南将帮助您快速上手LiberZMQ的基本功能。

基本概念
--------

LiberZMQ提供四种主要的消息模式：

* **发布-订阅 (Pub/Sub)**: 一对多的消息广播
* **请求-响应 (Req/Rep)**: 同步的请求-响应通信
* **推拉 (Push/Pull)**: 负载均衡的任务分发
* **Dealer/Router**: 异步的多对多通信，支持负载均衡和路由

发布-订阅模式
------------

发布-订阅模式适用于一对多的消息广播场景。

发布者示例
~~~~~~~~~~

.. code-block:: python

   from liberzmq.patterns import Publisher
   
   # 创建发布者
   publisher = Publisher(address="tcp://*:5555")
   publisher.connect()
   
   # 发布消息
   publisher.publish("news", {
       "title": "重要新闻",
       "content": "这是一条重要新闻"
   })
   
   publisher.publish("weather", {
       "city": "北京",
       "temperature": 25
   })
   
   publisher.disconnect()

订阅者示例
~~~~~~~~~~

.. code-block:: python

   from liberzmq.patterns import Subscriber
   
   # 创建订阅者
   subscriber = Subscriber(address="tcp://localhost:5555")
   subscriber.connect()
   
   # 定义消息处理函数
   def handle_news(topic, message):
       print(f"收到新闻: {message['title']}")
   
   def handle_weather(topic, message):
       print(f"{message['city']}的温度: {message['temperature']}°C")
   
   # 订阅主题
   subscriber.subscribe("news", handle_news)
   subscriber.subscribe("weather", handle_weather)
   
   # 开始监听
   subscriber.start_listening()

请求-响应模式
------------

请求-响应模式适用于同步的客户端-服务器通信。

服务器示例
~~~~~~~~~~

.. code-block:: python

   from liberzmq.patterns import Server
   
   # 创建服务器
   server = Server(address="tcp://*:5556")
   server.connect()
   
   # 注册请求处理器
   def calculate_handler(data):
       a = data.get('a', 0)
       b = data.get('b', 0)
       operation = data.get('operation', 'add')
       
       if operation == 'add':
           result = a + b
       elif operation == 'multiply':
           result = a * b
       else:
           return {'error': '不支持的操作'}
       
       return {'result': result}
   
   server.register_handler('calculate', calculate_handler)
   
   # 启动服务器
   server.start_server()

客户端示例
~~~~~~~~~~

.. code-block:: python

   from liberzmq.patterns import Client
   
   # 创建客户端
   client = Client(address="tcp://localhost:5556")
   client.connect()
   
   # 发送请求
   response = client.send_request('calculate', {
       'a': 10,
       'b': 5,
       'operation': 'add'
   })
   
   print(f"计算结果: {response['result']}")  # 输出: 计算结果: 15
   
   # 使用远程方法调用
   result = client.call('calculate', a=20, b=3, operation='multiply')
   print(f"乘法结果: {result['result']}")  # 输出: 乘法结果: 60
   
   client.disconnect()

推拉模式
--------

推拉模式适用于负载均衡的任务分发场景。

任务分发器示例
~~~~~~~~~~~~~~

.. code-block:: python

   from liberzmq.patterns import Pusher
   
   # 创建推送器
   pusher = Pusher(address="tcp://*:5557")
   pusher.connect()
   
   # 推送任务
   tasks = [
       {'id': 1, 'type': 'process_image', 'filename': 'image1.jpg'},
       {'id': 2, 'type': 'process_image', 'filename': 'image2.jpg'},
       {'id': 3, 'type': 'send_email', 'to': 'user@example.com'}
   ]
   
   for task in tasks:
       pusher.push_task(task)
   
   pusher.disconnect()

工作器示例
~~~~~~~~~~

.. code-block:: python

   from liberzmq.patterns import Puller
   
   # 创建拉取器
   puller = Puller(address="tcp://localhost:5557")
   puller.connect()
   
   # 注册任务处理器
   def process_image_handler(task):
       filename = task['filename']
       print(f"处理图片: {filename}")
       return {'status': 'completed', 'processed_file': f"processed_{filename}"}
   
   def send_email_handler(task):
       to = task['to']
       print(f"发送邮件到: {to}")
       return {'status': 'sent', 'to': to}
   
   puller.register_task_handler('process_image', process_image_handler)
   puller.register_task_handler('send_email', send_email_handler)
   
   # 启动工作器
   puller.start_worker()

Dealer/Router模式
-----------------

Dealer/Router模式适用于异步的多对多通信场景，支持负载均衡和消息路由。

Router服务器示例
~~~~~~~~~~~~~~~~

.. code-block:: python

   from liberzmq.patterns import Router
   
   # 创建Router服务器
   router = Router(address="tcp://*:5558")
   router.connect()
   
   # 处理请求
   while True:
       try:
           # 接收请求
           request = router.receive_request(timeout=1000)
           print(f"收到来自 {request.client_id} 的请求: {request.data}")
           
           # 处理请求并发送响应
           response_data = {"status": "success", "echo": request.data}
           router.send_response(request.client_id, request.request_id, response_data)
           
       except TimeoutError:
           continue
       except KeyboardInterrupt:
           break
   
   router.disconnect()

Dealer客户端示例
~~~~~~~~~~~~~~~~

.. code-block:: python

   from liberzmq.patterns import Dealer
   
   # 创建Dealer客户端
   dealer = Dealer(address="tcp://localhost:5558")
   dealer.connect()
   
   # 发送请求并等待响应
   request_data = {"message": "Hello from Dealer"}
   response = dealer.send_request(request_data, timeout=5000)
   print(f"收到响应: {response}")
   
   # 使用回调方式发送异步请求
   def response_callback(response_data):
       print(f"异步响应: {response_data}")
   
   dealer.send_request_async(request_data, response_callback)
   
   # 等待一段时间让异步请求完成
   import time
   time.sleep(1)
   
   dealer.disconnect()

配置和序列化
------------

使用不同的序列化器
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # 使用JSON序列化器（默认）
   publisher = Publisher(address="tcp://*:5555", serializer='json')
   
   # 使用二进制序列化器
   publisher = Publisher(address="tcp://*:5555", serializer='binary')
   
   # 使用Protocol Buffers序列化器
   publisher = Publisher(address="tcp://*:5555", serializer='protobuf')

配置日志
~~~~~~~~

.. code-block:: python

   from liberzmq.logging import configure_logging
   
   # 配置日志
   configure_logging(
       level='INFO',
       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
       filename='liberzmq.log'
   )

异常处理
~~~~~~~~

.. code-block:: python

   from liberzmq.exceptions import ConnectionError, TimeoutError
   from liberzmq.patterns import Client
   
   client = Client(address="tcp://localhost:5556")
   
   try:
       client.connect()
       response = client.send_request('test', {'data': 'test'}, timeout=1000)
   except ConnectionError as e:
       print(f"连接错误: {e}")
   except TimeoutError as e:
       print(f"请求超时: {e}")
   finally:
       client.disconnect()

下一步
------

现在您已经了解了LiberZMQ的基本用法，可以继续阅读：

* :doc:`api/core` - 查看核心模块API文档
* :doc:`api/patterns` - 查看消息模式API文档
* :doc:`api/serializers` - 查看序列化器API文档
* :doc:`api/logging` - 查看日志模块API文档
* :doc:`api/exceptions` - 查看异常处理API文档