LiberZMQ 文档
==============

LiberZMQ是一个基于ZeroMQ的高级Python消息传递库，提供简单易用的API来实现各种消息模式。

特性
----

* **多种消息模式**: 支持发布-订阅、请求-响应、推拉、Dealer/Router等四种核心模式
* **灵活的序列化**: 支持JSON、Protocol Buffers、二进制、MessagePack等序列化格式
* **配置管理**: 支持文件和环境变量配置
* **日志系统**: 内置可配置的日志功能
* **异常处理**: 完善的异常处理机制和重试机制
* **数据校验**: 支持Cerberus、Pydantic等数据校验工具
* **高性能**: 基于ZeroMQ的高性能异步消息传递
* **易于使用**: 简洁的API设计，降低学习成本
* **可扩展性**: 支持自定义序列化器和扩展接口
* **监控支持**: 内置性能指标和监控机制

快速开始
--------

安装
~~~~

.. code-block:: bash

   pip install pyzmq
   # 可选：安装protobuf支持
   pip install protobuf

基本使用
~~~~~~~~

发布-订阅模式:

.. code-block:: python

   from liberzmq.patterns import Publisher, Subscriber
   
   # 发布者
   publisher = Publisher(address="tcp://*:5555")
   publisher.connect()
   publisher.publish("news", {"title": "重要新闻"})
   
   # 订阅者
   subscriber = Subscriber(address="tcp://localhost:5555")
   subscriber.connect()
   subscriber.subscribe("news", lambda topic, msg: print(msg))
   subscriber.start_listening()

Dealer/Router模式:

.. code-block:: python

   from liberzmq.patterns import Dealer, Router
   
   # Router服务器
   router = Router(address="tcp://*:5558")
   router.connect()
   
   # Dealer客户端
   dealer = Dealer(address="tcp://localhost:5558")
   dealer.connect()
   
   # 发送请求并接收响应
   response = dealer.send_request({"action": "process", "data": "test"})
   print(f"响应: {response}")

目录
----

.. toctree::
   :maxdepth: 2
   :caption: 用户指南:

   installation
   quickstart
   architecture
   performance
   advanced

.. toctree::
   :maxdepth: 2
   :caption: API参考:

   api/core
   api/patterns
   api/serializers
   api/logging
   api/exceptions

索引和表格
============

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`