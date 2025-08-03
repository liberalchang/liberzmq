#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调试Dealer-Router消息格式
"""

import zmq
import time
import threading
from liberzmq.serializers import get_serializer

def debug_dealer_router():
    """调试Dealer-Router消息格式"""
    context = zmq.Context()
    
    # 创建Router socket
    router = context.socket(zmq.ROUTER)
    router.bind("tcp://*:5559")
    
    # 创建Dealer socket
    dealer = context.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, b"test-dealer")
    dealer.connect("tcp://localhost:5559")
    
    time.sleep(0.2)  # 等待连接建立
    
    # 发送消息
    serializer = get_serializer("json")
    test_data = {"message": "Hello Router", "value": 42}
    serialized_data = serializer.serialize(test_data)
    
    print(f"发送数据: {test_data}")
    print(f"序列化后长度: {len(serialized_data)} 字节")
    print(f"序列化数据前10字节: {serialized_data[:10]}")
    
    # Dealer发送消息
    print("\n发送消息...")
    dealer.send_multipart([
        b"request-123",
        serialized_data
    ])
    print("消息已发送")
    
    time.sleep(0.1)  # 等待消息传输
    
    # Router接收消息
    print("\n尝试接收消息...")
    try:
        parts = router.recv_multipart(zmq.NOBLOCK)
        print(f"\n接收到 {len(parts)} 个部分:")
        
        for i, part in enumerate(parts):
            print(f"部分 {i}: 长度={len(part)}, 类型={type(part)}")
            print(f"  前20字节: {part[:20]}")
            try:
                decoded = part.decode('utf-8')
                print(f"  UTF-8解码: {decoded}")
            except UnicodeDecodeError as e:
                print(f"  UTF-8解码失败: {e}")
            print()
        
        # 尝试按预期格式解析
        if len(parts) >= 3:
            try:
                client_id = parts[0].decode('utf-8')
                request_id = parts[1].decode('utf-8')
                request_data = serializer.deserialize(parts[2])
                
                print(f"解析成功:")
                print(f"  客户端ID: {client_id}")
                print(f"  请求ID: {request_id}")
                print(f"  请求数据: {request_data}")
                
            except Exception as e:
                print(f"解析失败: {e}")
        
    except zmq.Again:
        print("没有接收到消息")
    
    # 清理
    dealer.close()
    router.close()
    context.term()

if __name__ == "__main__":
    debug_dealer_router()