#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试模块

包含liberzmq库的全面单元测试，覆盖所有核心功能和消息模式。
"""

__version__ = '1.0.0'
__author__ = 'LiberZMQ Team'
__description__ = 'Comprehensive test suite for liberzmq library'

# 测试配置
TEST_CONFIG = {
    'default_timeout': 5000,  # 默认超时时间（毫秒）
    'test_ports': {
        'pubsub': 15555,
        'reqrep': 15556,
        'pushpull': 15557,
    },
    'test_addresses': {
        'pubsub_pub': 'tcp://*:15555',
        'pubsub_sub': 'tcp://localhost:15555',
        'reqrep_server': 'tcp://*:15556',
        'reqrep_client': 'tcp://localhost:15556',
        'pushpull_push': 'tcp://*:15557',
        'pushpull_pull': 'tcp://localhost:15557',
    },
    'serializers': ['json', 'binary', 'protobuf'],
    'test_data': {
        'simple': {'message': 'hello', 'number': 42},
        'complex': {
            'list': [1, 2, 3],
            'dict': {'nested': {'value': 'test'}},
            'tuple': (1, 'two', 3.0),
            'boolean': True,
            'null': None
        },
        'binary': b'\x00\x01\x02\x03\xff\xfe\xfd',
        'unicode': '测试中文字符 🚀 emoji',
    }
}

# 测试工具函数
def get_test_address(pattern: str, role: str = None) -> str:
    """获取测试地址
    
    Args:
        pattern: 消息模式 ('pubsub', 'reqrep', 'pushpull')
        role: 角色 ('pub', 'sub', 'server', 'client', 'push', 'pull')
        
    Returns:
        str: 测试地址
    """
    if role:
        key = f"{pattern}_{role}"
    else:
        key = pattern
    
    return TEST_CONFIG['test_addresses'].get(key, f"tcp://localhost:{TEST_CONFIG['test_ports'].get(pattern, 15555)}")


def get_test_data(data_type: str = 'simple'):
    """获取测试数据
    
    Args:
        data_type: 数据类型 ('simple', 'complex', 'binary', 'unicode')
        
    Returns:
        测试数据
    """
    return TEST_CONFIG['test_data'].get(data_type, TEST_CONFIG['test_data']['simple'])


def get_test_serializers() -> list:
    """获取测试序列化器列表
    
    Returns:
        list: 序列化器名称列表
    """
    return TEST_CONFIG['serializers'].copy()