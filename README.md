# LiberZMQ

一个基于ZeroMQ的高级Python消息传递库，提供简单易用的API来实现各种消息模式。
pyzmq项目地址: [pyzmq](https://github.com/zeromq/pyzmq)

## 特性

- **多种消息模式**: 支持发布-订阅、请求-响应、推拉等模式
- **灵活的序列化**: 支持JSON、Protocol Buffers、二进制等序列化格式
- **配置管理**: 支持文件和环境变量配置
- **日志系统**: 内置可配置的日志功能
- **异常处理**: 完善的异常处理机制
- **高性能**: 基于ZeroMQ的高性能消息传递
- **易于使用**: 简洁的API设计

## 打包
```bash
whl和tar.gz打包
python setup.py bdist_wheel sdist

```

## 安装

```bash
pip install pyzmq
# 可选：安装protobuf支持
pip install protobuf

pip install dist\liberzmq-1.0.0-py3-none-any.whl --force-reinstall 
```

## 快速开始

### 发布-订阅模式

**发布者**:
```python
from liberzmq.patterns import Publisher

# 创建发布者
publisher = Publisher(address="tcp://*:5555")
publisher.connect()

# 发布消息
publisher.publish("news", {"title": "重要新闻", "content": "这是一条重要新闻"})
publisher.publish("weather", {"city": "北京", "temperature": 25})

publisher.disconnect()
```

**订阅者**:
```python
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

# 开始监听（阻塞）
subscriber.start_listening()
```

### 请求-响应模式

**服务器**:
```python
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
```

**客户端**:
```python
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
```

### 推拉模式

**任务分发器（Pusher）**:
```python
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

# 批量推送
pusher.push_batch(tasks)

pusher.disconnect()
```

**工作器（Puller）**:
```python
from liberzmq.patterns import Puller

# 创建拉取器
puller = Puller(address="tcp://localhost:5557")
puller.connect()

# 注册任务处理器
def process_image_handler(task):
    filename = task['filename']
    print(f"处理图片: {filename}")
    # 模拟图片处理
    return {'status': 'completed', 'processed_file': f"processed_{filename}"}

def send_email_handler(task):
    to = task['to']
    print(f"发送邮件到: {to}")
    # 模拟邮件发送
    return {'status': 'sent', 'to': to}

puller.register_task_handler('process_image', process_image_handler)
puller.register_task_handler('send_email', send_email_handler)

# 启动工作器（后台运行）
puller.start_worker()

# 或者手动处理任务
while True:
    task = puller.pull_task(timeout=5000)
    if task:
        result = puller.process_task(task)
        print(f"任务处理结果: {result}")
    else:
        print("没有新任务")
        break
```

## 配置管理

### 使用配置文件

创建 `config.json`:
```json
{
    "zmq": {
        "socket_timeout": 5000,
        "linger_time": 1000
    },
    "serialization": {
        "default": "json",
        "compression": true
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    }
}
```

加载配置:
```python
from liberzmq.core.config import load_config

# 从文件加载配置
config = load_config('config.json')

# 从环境变量加载配置
config = load_config()  # 自动检测环境变量

# 使用配置创建组件
publisher = Publisher(
    address="tcp://*:5555",
    serializer=config.get('serialization.default', 'json')
)
```

### 环境变量配置

```bash
export LIBERZMQ_ZMQ_SOCKET_TIMEOUT=5000
export LIBERZMQ_SERIALIZATION_DEFAULT=json
export LIBERZMQ_LOGGING_LEVEL=DEBUG
```

## 序列化器

### JSON序列化器（默认）
```python
from liberzmq.patterns import Publisher

publisher = Publisher(address="tcp://*:5555", serializer='json')
```

### 二进制序列化器
```python
publisher = Publisher(address="tcp://*:5555", serializer='binary')
# 支持压缩
publisher = Publisher(address="tcp://*:5555", serializer='binary', compression=True)
```

### Protocol Buffers序列化器
```python
publisher = Publisher(address="tcp://*:5555", serializer='protobuf')
```

### 自定义序列化器
```python
from liberzmq.serializers import BaseSerializer, register_serializer

class CustomSerializer(BaseSerializer):
    def serialize(self, data):
        # 自定义序列化逻辑
        return custom_encode(data)
    
    def deserialize(self, data):
        # 自定义反序列化逻辑
        return custom_decode(data)

# 注册自定义序列化器
register_serializer('custom', CustomSerializer)

# 使用自定义序列化器
publisher = Publisher(address="tcp://*:5555", serializer='custom')
```

## 日志系统

```python
from liberzmq.logging import get_logger, configure_logging

# 配置日志
configure_logging(
    level='INFO',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='liberzmq.log'
)

# 获取日志器
logger = get_logger('my_app')
logger.info('应用启动')
logger.error('发生错误')
```

## 异常处理

```python
from liberzmq.exceptions import ConnectionError, TimeoutError, MessageError
from liberzmq.patterns import Client

client = Client(address="tcp://localhost:5556")

try:
    client.connect()
    response = client.send_request('test', {'data': 'test'}, timeout=1000)
except ConnectionError as e:
    print(f"连接错误: {e}")
except TimeoutError as e:
    print(f"请求超时: {e}")
except MessageError as e:
    print(f"消息错误: {e}")
finally:
    client.disconnect()
```

## 测试

运行所有测试:
```bash
python -m casetest.run_tests
```

运行特定模块测试:
```bash
python -m casetest.run_tests --module core
python -m casetest.run_tests --module pubsub
python -m casetest.run_tests --module reqrep
python -m casetest.run_tests --module pushpull
python -m casetest.run_tests --module integration
```

运行快速测试:
```bash
python -m casetest.run_tests --quick
```

生成测试报告:
```bash
python -m casetest.run_tests --report
```
## 项目结构

```
liberzmq/
├── liberzmq/                 # 主要源代码目录
│   ├── __init__.py          # 包初始化文件
│   ├── core/                # 核心模块
│   │   ├── __init__.py      # 核心模块导出
│   │   ├── config.py        # 配置管理
│   │   └── exceptions.py    # 异常定义
│   ├── patterns/            # 消息模式实现
│   │   ├── __init__.py      # 模式导出
│   │   ├── pubsub.py        # 发布-订阅模式
│   │   ├── reqrep.py        # 请求-响应模式
│   │   └── pushpull.py      # 推拉模式
│   ├── serializers/         # 数据序列化模块
│   │   ├── __init__.py      # 序列化器导出
│   │   ├── base.py          # 基础序列化器
│   │   ├── json_serializer.py    # JSON序列化器
│   │   ├── binary_serializer.py  # 二进制序列化器
│   │   ├── protobuf_serializer.py # Protobuf序列化器
│   │   └── factory.py       # 序列化器工厂
│   └── logging/             # 日志模块
│       ├── __init__.py      # 日志导出
│       └── logger.py        # 日志实现
├── casetest/                # 测试代码目录
│   ├── __init__.py          # 测试模块初始化
│   ├── test_core.py         # 核心功能测试
│   ├── test_pubsub.py       # 发布-订阅测试
│   ├── test_reqrep.py       # 请求-响应测试
│   ├── test_pushpull.py     # 推拉模式测试
│   ├── test_integration.py  # 集成测试
│   └── run_tests.py         # 测试运行器
├── setup.py                 # 安装脚本
├── requirements.txt         # 依赖文件
└── README.md               # 项目说明
```
## 模式特性
模式 |	通信方向 | 身份识别 | 适用场景
--- | --- | --- | ---
PUSH/PULL	|单向（多对一） | 无 | 数据采集、任务分发
Pub/Sub | 一对多 | 无 | 实时消息广播、事件驱动系统
REQ/REP	|双向（一对一） | 隐式 | 简单请求 - 响应（严格顺序）
DEALER/ROUTER	|双向（多对多） | 显式 | 复杂分布式系统、动态连接管理

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 更新日志

### v1.0.0
- 初始版本发布
- 支持发布-订阅、请求-响应、推拉消息模式
- 支持JSON、二进制、Protocol Buffers序列化格式
- 集成配置管理、日志和异常处理系统
- 提供完整的测试套件