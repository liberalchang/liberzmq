#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LiberZMQ - Python ZeroMQ库高级封装
设置脚本，用于打包和发布
"""

from setuptools import setup, find_packages
import os

# 读取README文件
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# 读取requirements文件
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="liberzmq",
    version="1.0.0",
    author="liber",
    author_email="liberalcxl@gmail.com",
    description="Python ZeroMQ库高级封装，提供易于使用、高性能和可扩展的消息传递解决方案",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/liberalchang/liberzmq",
    packages=find_packages(exclude=["casetest", "casetest.*", "docs", "docs.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Networking",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)