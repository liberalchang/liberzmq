# PushPull 模式包装配置指南

本指南详细说明了如何在 liberzmq 的 PushPull 模式中配置任务包装信息。

## 概述

在 PushPull 模式中，`Pusher` 类会自动将原始数据包装成任务消息，添加元数据字段以支持任务分发、优先级处理和负载均衡等功能。

## 任务包装结构

包装后的任务消息具有以下结构：

```json
{
    "type": "任务类型",
    "data": "原始数据",
    "priority": 优先级数值,
    "timestamp": 时间戳,
    "task_id": "唯一任务ID",
    "host_info": {
        "hostname": "主机名",
        "pusher_address": "推送器地址",
        "bind_mode": "绑定模式",
        "自定义字段": "自定义值"
    },
    "额外字段": "额外值"
}
```

## Pusher 配置参数

### 基本参数

- `address`: 绑定或连接的地址（默认: "tcp://*:5557"）
- `serializer`: 序列化器（默认: "json"）
- `bind`: 是否绑定地址（默认: True）

### 包装配置参数

- `include_host_info`: 是否包含主机信息（默认: True）
- `custom_host_info`: 自定义主机信息字典（默认: None）
- `default_task_type`: 默认任务类型（默认: "default"）

## 使用示例

### 1. 基本使用

```python
from liberzmq.patterns.pushpull import Pusher

# 创建推送器
pusher = Pusher(
    address="tcp://localhost:5557",
    bind=False
)

pusher.start()

# 发送数据（使用默认任务类型）
data = {"message": "Hello World"}
pusher.push_task(data)

# 发送数据并指定任务类型
pusher.push_task("urgent", data)

pusher.stop()
```

### 2. 自定义主机信息

```python
# 自定义主机信息
custom_host_info = {
    "department": "IT",
    "location": "Beijing",
    "version": "1.0.0"
}

pusher = Pusher(
    address="tcp://localhost:5557",
    bind=False,
    custom_host_info=custom_host_info,
    default_task_type="custom_task"
)
```

### 3. 禁用主机信息

```python
# 禁用主机信息以减少包装开销
pusher = Pusher(
    address="tcp://localhost:5557",
    bind=False,
    include_host_info=False
)
```

### 4. 设置优先级和额外字段

```python
# 发送带优先级和额外字段的任务
pusher.push_task(
    "high_priority",  # 任务类型
    data,             # 数据
    priority=10,      # 优先级
    source="client1", # 额外字段
    category="urgent" # 额外字段
)
```

## push_task 方法详解

### 方法签名

```python
def push_task(self, 
              task_type_or_data: Any, 
              task_data: Any = None, 
              priority: int = 0, 
              **kwargs) -> None
```

### 调用模式

#### 单参数模式

```python
# 直接发送数据，使用默认任务类型
pusher.push_task(data)
```

#### 双参数模式

```python
# 指定任务类型和数据
pusher.push_task("task_type", data)
```

### 参数说明

- `task_type_or_data`: 任务类型（双参数模式）或数据（单参数模式）
- `task_data`: 任务数据（仅双参数模式）
- `priority`: 任务优先级（默认: 0）
- `**kwargs`: 额外字段，会添加到任务消息中

## 任务ID生成规则

任务ID格式：`{任务类型}_{微秒时间戳}_{推送计数}`

示例：`urgent_1703123456789012_5`

## 最佳实践

### 1. 任务类型命名

- 使用有意义的任务类型名称
- 避免使用特殊字符
- 建议使用下划线分隔单词

```python
# 好的命名
pusher.push_task("data_processing", data)
pusher.push_task("file_upload", data)
pusher.push_task("email_notification", data)

# 避免的命名
pusher.push_task("task1", data)
pusher.push_task("temp-task", data)
```

### 2. 主机信息配置

- 在分布式环境中启用主机信息
- 添加有用的自定义字段（如版本、部门、位置）
- 在高性能场景中考虑禁用主机信息

```python
# 分布式环境推荐配置
custom_host_info = {
    "service_name": "data_processor",
    "version": "2.1.0",
    "environment": "production",
    "region": "us-east-1"
}
```

### 3. 优先级使用

- 使用数值表示优先级（数值越大优先级越高）
- 建立优先级标准

```python
# 优先级标准示例
PRIORITY_LOW = 1
PRIORITY_NORMAL = 5
PRIORITY_HIGH = 10
PRIORITY_CRITICAL = 20

pusher.push_task("urgent_task", data, priority=PRIORITY_CRITICAL)
```

### 4. 错误处理

```python
try:
    pusher.push_task("task_type", data)
except Exception as e:
    logger.error(f"发送任务失败: {e}")
    # 处理错误逻辑
```

## 性能考虑

1. **主机信息开销**: 如果不需要主机信息，设置 `include_host_info=False`
2. **序列化开销**: 选择合适的序列化器（json、pickle、msgpack）
3. **额外字段**: 避免添加过多或过大的额外字段
4. **任务类型**: 使用简短但有意义的任务类型名称

## 故障排除

### 常见问题

1. **任务丢失**: 检查网络连接和地址配置
2. **序列化错误**: 确保数据可序列化
3. **性能问题**: 检查主机信息和额外字段配置

### 调试技巧

```python
# 启用详细日志
from liberzmq.logging.logger import configure_logging
configure_logging(level="DEBUG")

# 检查任务结构
logger.debug(f"发送任务: {task}")
```