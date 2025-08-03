#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
拉取服务器 - 中心数据收集服务器

该服务器绑定端口，持续接收来自多台主机的数据，并进行处理（存储、分析等）。
使用 liberzmq 的 pushpull 模式中的 Puller 角色。
"""

import time
import json
import threading
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path

# 导入 liberzmq 模块
from liberzmq.patterns.pushpull import Puller
from liberzmq.exceptions.base import ConnectionError, TimeoutError
from liberzmq.logging.logger import get_logger, configure_logging


class DataProcessor:
    """数据处理器 - 负责存储和分析接收到的数据"""
    
    def __init__(self, data_dir: str = "./data"):
        """初始化数据处理器
        
        Args:
            data_dir: 数据存储目录
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # 初始化日志器
        self.logger = get_logger('pullserver.processor')
        
        # 统计信息
        self.total_received = 0
        self.data_by_source = {}
        self.start_time = datetime.now()
        
        # 数据缓存（用于批量写入）
        self.data_buffer = []
        self.buffer_size = 100
        self.last_flush_time = time.time()
        
    def process_data(self, data: Dict[str, Any]) -> None:
        """处理接收到的数据
        
        Args:
            data: 接收到的包装任务数据字典
        """
        try:
            # 更新统计信息
            self.total_received += 1
            
            # 提取包装信息
            task_type = data.get('type', 'unknown')
            task_id = data.get('task_id', 'unknown')
            priority = data.get('priority', 0)
            timestamp = data.get('timestamp', 0)
            host_info = data.get('host_info', {})
            
            # 提取原始数据
            original_data = data.get('data', {})
            source = original_data.get('source', 'unknown')
            
            # 如果没有原始数据，可能是旧格式，直接使用整个data
            if not original_data:
                original_data = data
                source = data.get('source', 'unknown')
            
            # 更新来源统计
            if source not in self.data_by_source:
                self.data_by_source[source] = {
                    'count': 0,
                    'last_received': None,
                    'data_types': set(),
                    'task_types': set(),
                    'host_info': {}
                }
            
            self.data_by_source[source]['count'] += 1
            self.data_by_source[source]['last_received'] = datetime.now().isoformat()
            
            # 记录数据类型和任务类型
            original_data_type = original_data.get('type', 'unknown')
            self.data_by_source[source]['data_types'].add(original_data_type)
            self.data_by_source[source]['task_types'].add(task_type)
            
            # 更新主机信息
            if host_info:
                self.data_by_source[source]['host_info'] = host_info
            
            # 添加服务端处理信息
            data['processed_at'] = datetime.now().isoformat()
            data['server_id'] = 'pullserver_001'
            data['processing_priority'] = priority
            
            # 添加到缓存
            self.data_buffer.append(data)
            
            # 检查是否需要刷新缓存
            current_time = time.time()
            if (len(self.data_buffer) >= self.buffer_size or 
                current_time - self.last_flush_time > 10):  # 10秒强制刷新
                self._flush_buffer()
            
            # 实时分析（示例）
            self._analyze_data(data)
            
            # 记录详细日志
            hostname = host_info.get('hostname', 'unknown') if host_info else 'unknown'
            self.logger.info(f"处理任务: {task_type}[{task_id}] 来源: {source}@{hostname} 优先级: {priority} 数据类型: {original_data_type}")
            
        except Exception as e:
            self.logger.error(f"数据处理错误: {e}")
            self.logger.debug(f"错误数据: {data}")
    
    def _flush_buffer(self) -> None:
        """刷新数据缓存到文件"""
        if not self.data_buffer:
            return
        
        try:
            # 按日期创建文件
            today = datetime.now().strftime('%Y%m%d')
            data_file = self.data_dir / f"data_{today}.json"
            
            # 写入数据（JSONL格式）
            with open(data_file, 'a', encoding='utf-8') as f:
                for data in self.data_buffer:
                    f.write(json.dumps(data, ensure_ascii=False) + '\n')
            
            self.logger.info(f"已保存 {len(self.data_buffer)} 条数据到 {data_file}")
            
            # 清空缓存
            self.data_buffer.clear()
            self.last_flush_time = time.time()
            
        except Exception as e:
            self.logger.error(f"数据保存错误: {e}")
    
    def _analyze_data(self, data: Dict[str, Any]) -> None:
        """实时数据分析（示例）"""
        try:
            # 提取任务信息
            task_type = data.get('type', 'unknown')
            priority = data.get('priority', 0)
            host_info = data.get('host_info', {})
            
            # 提取原始数据
            original_data = data.get('data', {})
            if not original_data:
                original_data = data  # 兼容旧格式
            
            # 简单的数据分析示例
            data_type = original_data.get('type')
            source = original_data.get('source', 'unknown')
            
            # 根据任务类型进行分析
            if task_type == 'sensor_data' or data_type == 'sensor':
                # 传感器数据分析
                payload = original_data.get('payload', {})
                value = payload.get('value', 0)
                sensor_id = payload.get('sensor_id', 'unknown')
                
                if value > 100:  # 阈值检测
                    hostname = host_info.get('hostname', 'unknown') if host_info else 'unknown'
                    self.logger.warning(f"传感器数值异常: {sensor_id}={value} (来源: {source}@{hostname}, 优先级: {priority})")
            
            elif task_type == 'heartbeat' or data_type == 'heartbeat':
                # 心跳数据分析
                hostname = host_info.get('hostname', 'unknown') if host_info else 'unknown'
                self.logger.debug(f"心跳: {source}@{hostname} 在线 (优先级: {priority})")
            
            elif task_type == 'log_data' or data_type == 'log':
                # 日志数据分析
                payload = original_data.get('payload', {})
                log_level = payload.get('level', 'INFO')
                message = payload.get('message', '')
                
                if log_level in ['ERROR', 'CRITICAL']:
                    hostname = host_info.get('hostname', 'unknown') if host_info else 'unknown'
                    self.logger.warning(f"收到错误日志: [{log_level}] {message} (来源: {source}@{hostname})")
            
            elif task_type == 'system_info' or data_type == 'system':
                # 系统信息分析
                payload = original_data.get('payload', {})
                cpu_percent = payload.get('cpu_percent', 0)
                memory_percent = payload.get('memory_percent', 0)
                
                if cpu_percent > 80 or memory_percent > 80:
                    hostname = host_info.get('hostname', 'unknown') if host_info else 'unknown'
                    self.logger.warning(f"系统资源告警: CPU {cpu_percent}%, 内存 {memory_percent}% (来源: {source}@{hostname})")
            
            # 优先级分析
            if priority >= 8:
                self.logger.info(f"高优先级任务: {task_type} (优先级: {priority})")
            elif priority >= 5:
                self.logger.debug(f"中等优先级任务: {task_type} (优先级: {priority})")
            else:
                self.logger.debug(f"低优先级任务: {task_type} (优先级: {priority})")
        
        except Exception as e:
            self.logger.error(f"数据分析错误: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        uptime = datetime.now() - self.start_time
        
        # 转换 set 为 list 以便 JSON 序列化
        sources_info = {}
        for source, info in self.data_by_source.items():
            sources_info[source] = {
                'count': info['count'],
                'last_received': info['last_received'],
                'data_types': list(info['data_types']),
                'task_types': list(info.get('task_types', set())),
                'host_info': info.get('host_info', {})
            }
        
        return {
            'total_received': self.total_received,
            'uptime_seconds': uptime.total_seconds(),
            'sources_count': len(self.data_by_source),
            'sources_info': sources_info,
            'buffer_size': len(self.data_buffer),
            'data_directory': str(self.data_dir)
        }
    
    def shutdown(self) -> None:
        """关闭处理器，保存剩余数据"""
        self.logger.info("正在关闭数据处理器...")
        self._flush_buffer()
        self.logger.info("数据处理器已关闭")


class PullServer:
    """拉取服务器 - 主服务器类"""
    
    def __init__(self, address: str = "tcp://*:5557", serializer: str = "json"):
        """初始化拉取服务器
        
        Args:
            address: 绑定地址
            serializer: 序列化器类型
        """
        self.address = address
        self.puller = Puller(address=address, serializer=serializer, bind=True)
        self.processor = DataProcessor()
        self.running = False
        self.stats_thread = None
        
        # 初始化日志器
        self.logger = get_logger('pullserver.server')
        
    def start(self) -> None:
        """启动服务器"""
        try:
            self.logger.info(f"启动拉取服务器，绑定地址: {self.address}")
            
            # 连接 ZeroMQ
            self.puller.connect()
            self.logger.info("ZeroMQ 连接成功")
            
            # 启动统计线程
            self.running = True
            self.stats_thread = threading.Thread(target=self._stats_loop, daemon=True)
            self.stats_thread.start()
            
            self.logger.info("服务器启动成功，等待数据...")
            self.logger.info("按 Ctrl+C 停止服务器")
            
            # 主循环 - 接收和处理数据
            while self.running:
                try:
                    # 接收数据（1秒超时）
                    data = self.puller.pull_task(timeout=1000)
                    
                    if data:
                        # 处理数据
                        self.processor.process_data(data)
                    
                except TimeoutError:
                    # 超时是正常的，继续循环
                    continue
                except KeyboardInterrupt:
                    self.logger.info("收到停止信号...")
                    break
                except Exception as e:
                    self.logger.error(f"接收数据错误: {e}")
                    time.sleep(1)
        
        except Exception as e:
            self.logger.error(f"服务器启动失败: {e}")
        finally:
            self.stop()
    
    def _stats_loop(self) -> None:
        """统计信息循环"""
        while self.running:
            try:
                time.sleep(30)  # 每30秒输出一次统计
                stats = self.processor.get_stats()
                
                self.logger.info("=" * 50)
                self.logger.info(f"服务器统计 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
                self.logger.info(f"总接收数据: {stats['total_received']} 条")
                self.logger.info(f"运行时间: {stats['uptime_seconds']:.1f} 秒")
                self.logger.info(f"数据源数量: {stats['sources_count']}")
                self.logger.info(f"缓存数据: {stats['buffer_size']} 条")
                
                if stats['sources_info']:
                    self.logger.info("数据源详情:")
                    for source, info in stats['sources_info'].items():
                        host_info = info.get('host_info', {})
                        hostname = host_info.get('hostname', 'unknown') if host_info else 'unknown'
                        client_type = host_info.get('client_type', 'unknown') if host_info else 'unknown'
                        
                        self.logger.info(f"  {source}@{hostname} ({client_type}):")
                        self.logger.info(f"    数据量: {info['count']} 条")
                        self.logger.info(f"    数据类型: {info['data_types']}")
                        self.logger.info(f"    任务类型: {info.get('task_types', [])}")
                        
                        if host_info:
                            self.logger.info(f"    主机信息: {host_info}")
                        
                        self.logger.info(f"    最后接收: {info['last_received']}")
                        self.logger.info("")
                
                self.logger.info("=" * 50)
                
            except Exception as e:
                self.logger.error(f"统计错误: {e}")
    
    def stop(self) -> None:
        """停止服务器"""
        self.logger.info("正在停止服务器...")
        self.running = False
        
        try:
            # 关闭数据处理器
            self.processor.shutdown()
            
            # 断开 ZeroMQ 连接
            self.puller.disconnect()
            self.logger.info("ZeroMQ 连接已断开")
            
            # 等待统计线程结束
            if self.stats_thread and self.stats_thread.is_alive():
                self.stats_thread.join(timeout=2)
            
            self.logger.info("服务器已停止")
            
        except Exception as e:
            self.logger.error(f"停止服务器时出错: {e}")


def main():
    """主函数"""
    # 配置日志
    configure_logging(level='INFO')
    logger = get_logger('pullserver.main')
    
    # 配置参数
    SERVER_ADDRESS = "tcp://*:5557"  # 绑定所有网络接口的5557端口
    SERIALIZER = "json"  # 使用JSON序列化
    
    # 创建并启动服务器
    server = PullServer(address=SERVER_ADDRESS, serializer=SERIALIZER)
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("用户中断")
    except Exception as e:
        logger.error(f"服务器运行错误: {e}")
    finally:
        server.stop()


if __name__ == "__main__":
    main()