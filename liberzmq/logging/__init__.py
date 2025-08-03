#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日志模块

提供可配置的日志记录功能，支持多种输出目标和格式。
"""

from .logger import (
    get_logger,
    configure_logging,
    set_log_level,
    add_file_handler,
    add_console_handler,
    remove_handler
)

__all__ = [
    "get_logger",
    "configure_logging",
    "set_log_level",
    "add_file_handler",
    "add_console_handler",
    "remove_handler"
]