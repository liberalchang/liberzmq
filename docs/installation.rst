安装指南
========

系统要求
--------

LiberZMQ需要以下环境：

* Python 3.8 或更高版本
* ZeroMQ库（通过pyzmq安装）

基础安装
--------

使用pip安装LiberZMQ：

.. code-block:: bash

   pip install liberzmq

这将自动安装所有必需的依赖项，包括：

* pyzmq >= 25.0.0
* pydantic >= 2.0.0
* typeguard >= 4.0.0
* pyyaml >= 6.0

可选依赖
--------

Protocol Buffers支持
~~~~~~~~~~~~~~~~~~~

如果需要使用Protocol Buffers序列化器，请安装protobuf：

.. code-block:: bash

   pip install protobuf>=4.21.0

开发依赖
~~~~~~~~

如果需要进行开发或运行测试，请安装开发依赖：

.. code-block:: bash

   pip install liberzmq[dev]

这将安装额外的开发工具：

* pytest >= 7.0.0
* pytest-cov >= 4.0.0
* sphinx >= 5.0.0
* sphinx-rtd-theme >= 1.0.0

从源码安装
----------

如果需要从源码安装最新版本：

.. code-block:: bash

   git clone https://github.com/liberalchang/liberzmq.git
   cd liberzmq
   pip install -e .

验证安装
--------

安装完成后，可以通过以下方式验证：

.. code-block:: python

   import liberzmq
   print(liberzmq.__version__)

   # 测试基本功能
   from liberzmq.patterns import Publisher
   publisher = Publisher(address="tcp://*:5555")
   print("LiberZMQ安装成功！")

故障排除
--------

常见问题
~~~~~~~~

**ZeroMQ安装失败**

如果在安装pyzmq时遇到问题，请确保系统已安装ZeroMQ库：

* Ubuntu/Debian: ``sudo apt-get install libzmq3-dev``
* CentOS/RHEL: ``sudo yum install zeromq-devel``
* macOS: ``brew install zmq``
* Windows: 通常pip会自动处理

**Python版本不兼容**

确保使用Python 3.8或更高版本：

.. code-block:: bash

   python --version

**权限问题**

如果遇到权限问题，可以使用用户安装：

.. code-block:: bash

   pip install --user liberzmq