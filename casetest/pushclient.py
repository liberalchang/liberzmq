#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
推送客户端 - 数据发送客户端

该客户端连接到中心服务器，定期或按需发送数据。
多台主机可同时运行此脚本，实现分布式数据收集。
使用 liberzmq 的 pushpull 模式中的 Pusher 角色。
"""

import time
import json
import random
import threading
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional
import socket
import psutil
import uuid

# 导入 liberzmq 模块
from liberzmq.patterns.pushpull import Pusher
from liberzmq.exceptions.base import ConnectionError, TimeoutError
from liberzmq.logging.logger import get_logger, configure_logging


class DataGenerator:
    """数据生成器 - 生成各种类型的模拟数据"""
    
    def __init__(self, client_id: str):
        """初始化数据生成器
        
        Args:
            client_id: 客户端唯一标识
        """
        self.client_id = client_id
        self.hostname = socket.gethostname()
        self.start_time = datetime.now()
        
        # 初始化日志器
        self.logger = get_logger('pushclient.generator')
        
    def generate_sensor_data(self) -> Dict[str, Any]:
        """生成传感器数据"""
        return {
            'type': 'sensor',
            'source': self.client_id,
            'hostname': self.hostname,
            'timestamp': datetime.now().isoformat(),
            'payload': {
                'sensor_id': f"sensor_{random.randint(1, 10)}",
                'value': round(random.uniform(20, 120), 2),  # 模拟温度或其他数值
                'unit': random.choice(['°C', '%', 'Pa', 'V']),
                'location': random.choice(['room_a', 'room_b', 'outdoor', 'basement'])
            }
        }
    
    def generate_log_data(self) -> Dict[str, Any]:
        """生成日志数据"""
        levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        messages = [
            '系统启动完成',
            '用户登录成功',
            '数据库连接建立',
            '文件上传完成',
            '内存使用率较高',
            '网络连接超时',
            '磁盘空间不足',
            '服务异常重启'
        ]
        
        level = random.choice(levels)
        return {
            'type': 'log',
            'source': self.client_id,
            'hostname': self.hostname,
            'timestamp': datetime.now().isoformat(),
            'payload': {
                'level': level,
                'message': random.choice(messages),
                'module': random.choice(['auth', 'database', 'network', 'storage']),
                'thread_id': random.randint(1000, 9999)
            }
        }
    
    def generate_system_info(self) -> Dict[str, Any]:
        """生成系统信息数据"""
        try:
            # 获取真实的系统信息
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'type': 'system_info',
                'source': self.client_id,
                'hostname': self.hostname,
                'timestamp': datetime.now().isoformat(),
                'payload': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory.percent,
                    'memory_used_gb': round(memory.used / (1024**3), 2),
                    'memory_total_gb': round(memory.total / (1024**3), 2),
                    'disk_percent': round((disk.used / disk.total) * 100, 2),
                    'disk_free_gb': round(disk.free / (1024**3), 2),
                    'uptime_seconds': (datetime.now() - self.start_time).total_seconds()
                }
            }
        except Exception as e:
            # 如果获取系统信息失败，返回模拟数据
            return {
                'type': 'system_info',
                'source': self.client_id,
                'hostname': self.hostname,
                'timestamp': datetime.now().isoformat(),
                'payload': {
                    'cpu_percent': round(random.uniform(10, 80), 1),
                    'memory_percent': round(random.uniform(30, 90), 1),
                    'memory_used_gb': round(random.uniform(2, 8), 2),
                    'memory_total_gb': 16.0,
                    'disk_percent': round(random.uniform(20, 70), 1),
                    'disk_free_gb': round(random.uniform(50, 200), 2),
                    'uptime_seconds': (datetime.now() - self.start_time).total_seconds(),
                    'error': str(e)
                }
            }
    
    def generate_heartbeat(self) -> Dict[str, Any]:
        """生成心跳数据"""
        return {
            'type': 'heartbeat',
            'source': self.client_id,
            'hostname': self.hostname,
            'timestamp': datetime.now().isoformat(),
            'payload': {
                'status': 'alive',
                'uptime_seconds': (datetime.now() - self.start_time).total_seconds(),
                'version': '1.0.0'
            }
        }
    
    def generate_custom_data(self, data_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """生成自定义数据
        
        Args:
            data_type: 数据类型
            payload: 自定义载荷
        """
        return {
            'type': data_type,
            'source': self.client_id,
            'hostname': self.hostname,
            'timestamp': datetime.now().isoformat(),
            'payload': payload
        }


class PushClient:
    """推送客户端 - 主客户端类"""
    
    def __init__(self, 
                 server_address: str = "tcp://localhost:5557",
                 client_id: Optional[str] = None,
                 serializer: str = "json",
                 include_host_info: bool = True,
                 custom_host_info: Optional[Dict[str, Any]] = None,
                 default_task_type: str = "data_task"):
        """初始化推送客户端
        
        Args:
            server_address: 服务器地址
            client_id: 客户端ID（如果为None则自动生成）
            serializer: 序列化器类型
            include_host_info: 是否包含主机信息
            custom_host_info: 自定义主机信息
            default_task_type: 默认任务类型
        """
        self.server_address = server_address
        self.client_id = client_id or f"client_{socket.gethostname()}_{uuid.uuid4().hex[:8]}"
        
        # 准备自定义主机信息
        if custom_host_info is None:
            custom_host_info = {
                "client_id": self.client_id,
                "client_type": "push_client",
                "version": "1.0.0"
            }
        
        self.pusher = Pusher(
            address=server_address, 
            serializer=serializer, 
            bind=False,
            include_host_info=include_host_info,
            custom_host_info=custom_host_info,
            default_task_type=default_task_type
        )
        self.generator = DataGenerator(self.client_id)
        
        self.running = False
        self.send_threads = []
        self.stats = {
            'sent_count': 0,
            'error_count': 0,
            'start_time': None
        }
        
        # 初始化日志器
        self.logger = get_logger('pushclient.client')
    
    def connect(self) -> bool:
        """连接到服务器
        
        Returns:
            bool: 连接是否成功
        """
        try:
            self.logger.info(f"连接到服务器: {self.server_address}")
            self.pusher.connect()
            self.logger.info(f"客户端 {self.client_id} 连接成功")
            return True
        except Exception as e:
            self.logger.error(f"连接失败: {e}")
            return False
    
    def send_data(self, data: Dict[str, Any], task_type: Optional[str] = None, priority: int = 0, **kwargs) -> bool:
        """发送数据到服务器
        
        Args:
            data: 要发送的数据
            task_type: 任务类型（如果为None则使用默认类型）
            priority: 任务优先级
            **kwargs: 额外的任务字段
            
        Returns:
            bool: 发送是否成功
        """
        try:
            if task_type:
                # 指定任务类型
                self.pusher.push_task(task_type, data, priority=priority, **kwargs)
            else:
                # 使用默认任务类型
                self.pusher.push_task(data, priority=priority, **kwargs)
            self.stats['sent_count'] += 1
            return True
        except Exception as e:
            self.logger.error(f"发送数据失败: {e}")
            self.stats['error_count'] += 1
            return False
    
    def start_sensor_data_sender(self, interval: float = 5.0) -> None:
        """启动传感器数据发送线程
        
        Args:
            interval: 发送间隔（秒）
        """
        def sender():
            while self.running:
                try:
                    data = self.generator.generate_sensor_data()
                    # 使用传感器任务类型，普通优先级
                    if self.send_data(data, task_type="sensor_data", priority=5, source="sensor_thread"):
                        self.logger.debug(f"发送传感器数据: {data['payload']['sensor_id']} = {data['payload']['value']}{data['payload']['unit']}")
                    time.sleep(interval)
                except Exception as e:
                    self.logger.error(f"传感器数据发送线程错误: {e}")
                    time.sleep(1)
        
        thread = threading.Thread(target=sender, daemon=True)
        thread.start()
        self.send_threads.append(thread)
    
    def start_log_data_sender(self, interval: float = 10.0) -> None:
        """启动日志数据发送线程
        
        Args:
            interval: 发送间隔（秒）
        """
        def sender():
            while self.running:
                try:
                    data = self.generator.generate_log_data()
                    # 根据日志级别设置不同优先级
                    level = data['payload']['level']
                    priority = 1 if level == 'DEBUG' else 3 if level == 'INFO' else 7 if level == 'WARNING' else 10
                    
                    if self.send_data(data, task_type="log_data", priority=priority, source="log_thread", log_level=level):
                        message = data['payload']['message']
                        self.logger.debug(f"发送日志: [{level}] {message}")
                    time.sleep(interval + random.uniform(0, 5))  # 添加随机延迟
                except Exception as e:
                    self.logger.error(f"日志数据发送线程错误: {e}")
                    time.sleep(1)
        
        thread = threading.Thread(target=sender, daemon=True)
        thread.start()
        self.send_threads.append(thread)
    
    def start_system_info_sender(self, interval: float = 30.0) -> None:
        """启动系统信息发送线程
        
        Args:
            interval: 发送间隔（秒）
        """
        def sender():
            while self.running:
                try:
                    data = self.generator.generate_system_info()
                    # 系统信息使用中等优先级
                    if self.send_data(data, task_type="system_info", priority=6, source="system_thread"):
                        cpu = data['payload']['cpu_percent']
                        memory = data['payload']['memory_percent']
                        self.logger.debug(f"发送系统信息: CPU {cpu}%, 内存 {memory}%")
                    time.sleep(interval)
                except Exception as e:
                    self.logger.error(f"系统信息发送线程错误: {e}")
                    time.sleep(1)
        
        thread = threading.Thread(target=sender, daemon=True)
        thread.start()
        self.send_threads.append(thread)
    
    def start_heartbeat_sender(self, interval: float = 60.0) -> None:
        """启动心跳发送线程
        
        Args:
            interval: 发送间隔（秒）
        """
        def sender():
            while self.running:
                try:
                    data = self.generator.generate_heartbeat()
                    # 心跳使用高优先级，确保连接状态监控
                    if self.send_data(data, task_type="heartbeat", priority=8, source="heartbeat_thread", critical=True):
                        self.logger.debug(f"发送心跳: {self.client_id} 在线")
                    time.sleep(interval)
                except Exception as e:
                    self.logger.error(f"心跳发送线程错误: {e}")
                    time.sleep(1)
        
        thread = threading.Thread(target=sender, daemon=True)
        thread.start()
        self.send_threads.append(thread)
    
    def send_custom_data(self, data_type: str, payload: Dict[str, Any]) -> bool:
        """发送自定义数据
        
        Args:
            data_type: 数据类型
            payload: 数据载荷
            
        Returns:
            bool: 发送是否成功
        """
        data = self.generator.generate_custom_data(data_type, payload)
        return self.send_data(data)
    
    def start_stats_reporter(self, interval: float = 60.0) -> None:
        """启动统计报告线程
        
        Args:
            interval: 报告间隔（秒）
        """
        def reporter():
            while self.running:
                try:
                    time.sleep(interval)
                    if self.stats['start_time']:
                        uptime = time.time() - self.stats['start_time']
                        rate = self.stats['sent_count'] / max(uptime, 1)
                        
                        self.logger.info("=" * 40)
                        self.logger.info(f"客户端统计 [{self.client_id}]")
                        self.logger.info(f"运行时间: {uptime:.1f} 秒")
                        self.logger.info(f"发送成功: {self.stats['sent_count']} 条")
                        self.logger.info(f"发送失败: {self.stats['error_count']} 条")
                        self.logger.info(f"发送速率: {rate:.2f} 条/秒")
                        self.logger.info("=" * 40)
                        
                except Exception as e:
                    self.logger.error(f"统计报告错误: {e}")
        
        thread = threading.Thread(target=reporter, daemon=True)
        thread.start()
        self.send_threads.append(thread)
    
    def start(self, 
              sensor_interval: float = 5.0,
              log_interval: float = 10.0,
              system_interval: float = 30.0,
              heartbeat_interval: float = 60.0) -> None:
        """启动客户端
        
        Args:
            sensor_interval: 传感器数据发送间隔
            log_interval: 日志数据发送间隔
            system_interval: 系统信息发送间隔
            heartbeat_interval: 心跳发送间隔
        """
        if not self.connect():
            return
        
        self.running = True
        self.stats['start_time'] = time.time()
        
        self.logger.info(f"启动客户端 {self.client_id}")
        self.logger.info("启动数据发送线程...")
        
        # 启动各种数据发送线程
        self.start_sensor_data_sender(sensor_interval)
        self.start_log_data_sender(log_interval)
        self.start_system_info_sender(system_interval)
        self.start_heartbeat_sender(heartbeat_interval)
        self.start_stats_reporter()
        
        self.logger.info("所有发送线程已启动")
        self.logger.info("按 Ctrl+C 停止客户端")
        
        try:
            # 主线程保持运行
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("收到停止信号...")
        finally:
            self.stop()
    
    def stop(self) -> None:
        """停止客户端"""
        self.logger.info("正在停止客户端...")
        self.running = False
        
        try:
            # 等待所有线程结束
            for thread in self.send_threads:
                if thread.is_alive():
                    thread.join(timeout=2)
            
            # 断开连接
            self.pusher.disconnect()
            self.logger.info("客户端连接已断开")
            
            # 输出最终统计
            if self.stats['start_time']:
                uptime = time.time() - self.stats['start_time']
                self.logger.info("最终统计:")
                self.logger.info(f"运行时间: {uptime:.1f} 秒")
                self.logger.info(f"发送成功: {self.stats['sent_count']} 条")
                self.logger.info(f"发送失败: {self.stats['error_count']} 条")
            
            self.logger.info("客户端已停止")
            
        except Exception as e:
            self.logger.error(f"停止客户端时出错: {e}")


def main():
    """主函数"""
    # 配置日志
    configure_logging(level='INFO')
    logger = get_logger('pushclient.main')
    
    parser = argparse.ArgumentParser(description='推送客户端 - 向中心服务器发送数据')
    parser.add_argument('--server', '-s', default='tcp://localhost:5557',
                       help='服务器地址 (默认: tcp://localhost:5557)')
    parser.add_argument('--client-id', '-c', default=None,
                       help='客户端ID (默认: 自动生成)')
    parser.add_argument('--sensor-interval', type=float, default=5.0,
                       help='传感器数据发送间隔/秒 (默认: 5.0)')
    parser.add_argument('--log-interval', type=float, default=10.0,
                       help='日志数据发送间隔/秒 (默认: 10.0)')
    parser.add_argument('--system-interval', type=float, default=30.0,
                       help='系统信息发送间隔/秒 (默认: 30.0)')
    parser.add_argument('--heartbeat-interval', type=float, default=60.0,
                       help='心跳发送间隔/秒 (默认: 60.0)')
    
    # 包装配置参数
    parser.add_argument('--no-host-info', action='store_true',
                       help='禁用主机信息包装')
    parser.add_argument('--task-type', default='data_task',
                       help='默认任务类型 (默认: data_task)')
    parser.add_argument('--department', default=None,
                       help='部门信息 (用于自定义主机信息)')
    parser.add_argument('--location', default=None,
                       help='位置信息 (用于自定义主机信息)')
    
    args = parser.parse_args()
    
    # 准备自定义主机信息
    custom_host_info = {
        "client_id": args.client_id or f"client_{socket.gethostname()}_{uuid.uuid4().hex[:8]}",
        "client_type": "push_client",
        "version": "1.0.0"
    }
    
    # 添加可选的自定义信息
    if args.department:
        custom_host_info["department"] = args.department
    if args.location:
        custom_host_info["location"] = args.location
    
    logger.info(f"包装配置: 主机信息={'禁用' if args.no_host_info else '启用'}, 默认任务类型={args.task_type}")
    if not args.no_host_info:
        logger.info(f"自定义主机信息: {custom_host_info}")
    
    # 创建并启动客户端
    client = PushClient(
        server_address=args.server,
        client_id=args.client_id,
        serializer="json",
        include_host_info=not args.no_host_info,
        custom_host_info=custom_host_info,
        default_task_type=args.task_type
    )
    
    try:
        client.start(
            sensor_interval=args.sensor_interval,
            log_interval=args.log_interval,
            system_interval=args.system_interval,
            heartbeat_interval=args.heartbeat_interval
        )
    except KeyboardInterrupt:
        logger.info("用户中断")
    except Exception as e:
        logger.error(f"客户端运行错误: {e}")
    finally:
        client.stop()


if __name__ == "__main__":
    main()