#!/usr/bin/env python
# -*- coding: utf-8 -*-

from liberzmq.patterns.pubsub import Subscriber
from casetest import get_test_address

# 创建订阅者实例
subscriber = Subscriber(
    address=get_test_address('pubsub', 'sub'),
    topics=['test_topic'],
    serializer='json'
)

print(f"初始状态:")
print(f"  _connected: {subscriber._connected}")
print(f"  _socket: {subscriber._socket}")
print(f"  socket: {getattr(subscriber, 'socket', 'Not exists')}")

# 连接
print(f"\n连接中...")
subscriber.connect()

print(f"连接后:")
print(f"  _connected: {subscriber._connected}")
print(f"  _socket: {subscriber._socket}")
print(f"  socket: {subscriber.socket}")
print(f"  _socket is socket: {subscriber._socket is subscriber.socket}")

# 断开连接
print(f"\n断开连接中...")
subscriber.disconnect()

print(f"断开连接后:")
print(f"  _connected: {subscriber._connected}")
print(f"  _socket: {subscriber._socket}")
print(f"  socket: {getattr(subscriber, 'socket', 'Not exists')}")
print(f"  _socket is None: {subscriber._socket is None}")