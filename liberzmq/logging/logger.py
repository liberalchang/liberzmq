#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日志记录器模块

提供可配置的日志记录功能，支持控制台和文件输出。
"""

import logging
import logging.handlers
import os
import sys
from typing import Optional, Dict, Any
from pathlib import Path

# 全局日志记录器字典
_loggers: Dict[str, logging.Logger] = {}
_configured = False


def get_logger(name: str = "liberzmq") -> logging.Logger:
    """获取日志记录器
    
    Args:
        name: 日志记录器名称
        
    Returns:
        logging.Logger: 日志记录器实例
    """
    # 确保名称以liberzmq开头
    if name != "liberzmq" and not name.startswith("liberzmq."):
        full_name = f"liberzmq.{name}"
    else:
        full_name = name
        
    if full_name not in _loggers:
        logger = logging.getLogger(full_name)
        _loggers[full_name] = logger
        
        # 如果还没有配置过日志，使用默认配置
        if not _configured:
            configure_logging()
        
        # 确保新logger使用正确的级别（无论是否已配置）
        root_logger = logging.getLogger()
        if root_logger.level != logging.NOTSET:
            logger.setLevel(root_logger.level)
    
    return _loggers[full_name]


def configure_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    console_output: bool = True,
    file_path: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> None:
    """配置日志系统
    
    Args:
        level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: 日志格式字符串
        console_output: 是否输出到控制台
        file_path: 日志文件路径
        max_file_size: 日志文件最大大小（字节）
        backup_count: 日志文件备份数量
    """
    global _configured
    
    # 默认日志格式
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # 创建格式化器
    formatter = logging.Formatter(format_string)
    
    # 设置根日志记录器级别
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # 设置所有已存在的logger级别
    for logger in _loggers.values():
        logger.setLevel(getattr(logging, level.upper()))
    
    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 添加控制台处理器
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(getattr(logging, level.upper()))
        root_logger.addHandler(console_handler)
    
    # 添加文件处理器
    if file_path:
        add_file_handler(
            file_path=file_path,
            level=level,
            format_string=format_string,
            max_file_size=max_file_size,
            backup_count=backup_count
        )
    
    _configured = True
    
    # 记录配置完成
    logger = get_logger("liberzmq.logging")
    logger.info(f"日志系统配置完成 - 级别: {level}, 控制台输出: {console_output}, 文件输出: {file_path}")


def set_log_level(level: str, logger_name: Optional[str] = None) -> None:
    """设置日志级别
    
    Args:
        level: 日志级别
        logger_name: 日志记录器名称，None表示根记录器
    """
    if logger_name:
        logger = get_logger(logger_name)
    else:
        logger = logging.getLogger()
    
    logger.setLevel(getattr(logging, level.upper()))
    
    # 同时设置所有处理器的级别
    for handler in logger.handlers:
        handler.setLevel(getattr(logging, level.upper()))
    
    get_logger("liberzmq.logging").info(f"设置日志级别: {level} (记录器: {logger_name or 'root'})")


def add_file_handler(
    file_path: str,
    level: str = "INFO",
    format_string: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    logger_name: Optional[str] = None
) -> logging.Handler:
    """添加文件处理器
    
    Args:
        file_path: 日志文件路径
        level: 日志级别
        format_string: 日志格式字符串
        max_file_size: 日志文件最大大小（字节）
        backup_count: 日志文件备份数量
        logger_name: 日志记录器名称，None表示根记录器
        
    Returns:
        logging.Handler: 创建的文件处理器
    """
    # 确保日志目录存在
    log_dir = Path(file_path).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建轮转文件处理器
    file_handler = logging.handlers.RotatingFileHandler(
        filename=file_path,
        maxBytes=max_file_size,
        backupCount=backup_count,
        encoding='utf-8'
    )
    
    # 设置格式
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    formatter = logging.Formatter(format_string)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, level.upper()))
    
    # 添加到指定的日志记录器
    if logger_name:
        logger = get_logger(logger_name)
    else:
        logger = logging.getLogger()
    
    logger.addHandler(file_handler)
    
    get_logger("liberzmq.logging").info(f"添加文件处理器: {file_path} (级别: {level})")
    
    return file_handler


def add_console_handler(
    level: str = "INFO",
    format_string: Optional[str] = None,
    stream=None,
    logger_name: Optional[str] = None
) -> logging.Handler:
    """添加控制台处理器
    
    Args:
        level: 日志级别
        format_string: 日志格式字符串
        stream: 输出流，默认为sys.stdout
        logger_name: 日志记录器名称，None表示根记录器
        
    Returns:
        logging.Handler: 创建的控制台处理器
    """
    if stream is None:
        stream = sys.stdout
    
    console_handler = logging.StreamHandler(stream)
    
    # 设置格式
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    formatter = logging.Formatter(format_string)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, level.upper()))
    
    # 添加到指定的日志记录器
    if logger_name:
        logger = get_logger(logger_name)
    else:
        logger = logging.getLogger()
    
    logger.addHandler(console_handler)
    
    get_logger("liberzmq.logging").info(f"添加控制台处理器 (级别: {level})")
    
    return console_handler


def remove_handler(handler: logging.Handler, logger_name: Optional[str] = None) -> None:
    """移除处理器
    
    Args:
        handler: 要移除的处理器
        logger_name: 日志记录器名称，None表示根记录器
    """
    if logger_name:
        logger = get_logger(logger_name)
    else:
        logger = logging.getLogger()
    
    logger.removeHandler(handler)
    handler.close()
    
    get_logger("liberzmq.logging").info(f"移除处理器: {handler.__class__.__name__}")


def configure_from_dict(config: Dict[str, Any]) -> None:
    """从字典配置日志系统
    
    Args:
        config: 配置字典
    """
    level = config.get("level", "INFO")
    format_string = config.get("format")
    console_output = config.get("console_output", True)
    file_path = config.get("file_path")
    max_file_size = config.get("max_file_size", 10 * 1024 * 1024)
    backup_count = config.get("backup_count", 5)
    
    configure_logging(
        level=level,
        format_string=format_string,
        console_output=console_output,
        file_path=file_path,
        max_file_size=max_file_size,
        backup_count=backup_count
    )


def get_log_config() -> Dict[str, Any]:
    """获取当前日志配置
    
    Returns:
        Dict[str, Any]: 当前日志配置
    """
    root_logger = logging.getLogger()
    
    config = {
        "level": logging.getLevelName(root_logger.level),
        "handlers": []
    }
    
    for handler in root_logger.handlers:
        handler_info = {
            "type": handler.__class__.__name__,
            "level": logging.getLevelName(handler.level),
            "format": handler.formatter._fmt if handler.formatter else None
        }
        
        if isinstance(handler, logging.FileHandler):
            handler_info["file_path"] = handler.baseFilename
        
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            handler_info["max_file_size"] = handler.maxBytes
            handler_info["backup_count"] = handler.backupCount
        
        config["handlers"].append(handler_info)
    
    return config


# 创建一个默认的日志记录器
default_logger = get_logger("liberzmq")