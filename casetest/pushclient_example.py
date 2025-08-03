#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推送客户端使用示例
演示如何设置包装信息和主机信息
"""

import time
from liberzmq.patterns.pushpull import Pusher
from liberzmq.logging.logger import get_logger, configure_logging

# 配置日志
configure_logging(level="INFO")
logger = get_logger("pushclient_example")

def example_basic_usage():
    """基本使用示例"""
    logger.info("=== 基本使用示例 ===")
    
    # 创建推送器（使用默认配置）
    pusher = Pusher(
        address="tcp://localhost:5557",
        bind=False  # 连接模式
    )
    
    try:
        pusher.start()
        
        # 发送数据（使用默认任务类型"default"）
        data = {
            "message": "Hello World",
            "value": 123
        }
        pusher.push_task(data)
        logger.info("发送数据（默认类型）")
        
        # 发送数据并指定任务类型
        pusher.push_task("urgent", data)
        logger.info("发送数据（指定类型：urgent）")
        
        time.sleep(1)
        
    finally:
        pusher.stop()

def example_custom_host_info():
    """自定义主机信息示例"""
    logger.info("=== 自定义主机信息示例 ===")
    
    # 创建推送器（自定义主机信息）
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
    
    try:
        pusher.start()
        
        data = {
            "message": "Custom host info example",
            "timestamp": time.time()
        }
        pusher.push_task(data)
        logger.info("发送数据（自定义主机信息）")
        
        time.sleep(1)
        
    finally:
        pusher.stop()

def example_disable_host_info():
    """禁用主机信息示例"""
    logger.info("=== 禁用主机信息示例 ===")
    
    # 创建推送器（禁用主机信息）
    pusher = Pusher(
        address="tcp://localhost:5557",
        bind=False,
        include_host_info=False,  # 禁用主机信息
        default_task_type="minimal"
    )
    
    try:
        pusher.start()
        
        data = {
            "message": "No host info example",
            "data_only": True
        }
        pusher.push_task(data)
        logger.info("发送数据（无主机信息）")
        
        time.sleep(1)
        
    finally:
        pusher.stop()

def example_with_priority_and_kwargs():
    """优先级和额外参数示例"""
    logger.info("=== 优先级和额外参数示例 ===")
    
    pusher = Pusher(
        address="tcp://localhost:5557",
        bind=False,
        default_task_type="priority_task"
    )
    
    try:
        pusher.start()
        
        data = {
            "message": "High priority task",
            "urgent": True
        }
        
        # 发送高优先级任务，并添加额外字段
        pusher.push_task(
            data,
            priority=10,  # 高优先级
            source="example_client",  # 额外字段
            category="test"  # 额外字段
        )
        logger.info("发送数据（高优先级 + 额外字段）")
        
        time.sleep(1)
        
    finally:
        pusher.stop()

if __name__ == "__main__":
    logger.info("开始推送客户端示例")
    
    try:
        # 运行各种示例
        example_basic_usage()
        time.sleep(2)
        
        example_custom_host_info()
        time.sleep(2)
        
        example_disable_host_info()
        time.sleep(2)
        
        example_with_priority_and_kwargs()
        
    except KeyboardInterrupt:
        logger.info("用户中断")
    except Exception as e:
        logger.error(f"示例运行出错: {e}")
    
    logger.info("推送客户端示例结束")