# -*- coding: utf-8 -*-
"""
LiberZMQ文档配置文件
Sphinx文档生成器配置
"""

import os
import sys

# 添加项目路径到Python路径
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('../liberzmq'))

# 确保能找到liberzmq包
try:
    import liberzmq
    print(f"Successfully imported liberzmq from {liberzmq.__file__}")
except ImportError as e:
    print(f"Failed to import liberzmq: {e}")
    print(f"Python path: {sys.path}")

# -- 项目信息 -----------------------------------------------------

project = 'LiberZMQ'
copyright = '2024, liber'
author = 'liber'

# 版本信息
release = '1.0.0'
version = '1.0.0'

# -- 通用配置 ---------------------------------------------------

# 需要的扩展
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx.ext.mathjax',
    'sphinx.ext.ifconfig',
    'sphinx.ext.githubpages',
]

# 添加任何包含模板的路径，相对于此目录
templates_path = ['_templates']

# 源文件后缀
source_suffix = '.rst'

# 主文档（"master document"）
master_doc = 'index'

# 语言设置
language = 'zh_CN'

# 排除的模式
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Pygments语法高亮样式
pygments_style = 'sphinx'

# -- HTML输出选项 ------------------------------------------------

# HTML主题
html_theme = 'alabaster'

# 主题选项
html_theme_options = {
    'github_user': 'liberalchang',
    'github_repo': 'liberzmq',
    'github_banner': True,
    'github_button': True,
    'show_powered_by': False,
    'sidebar_width': '280px',
    'page_width': '1400px',
    'fixed_sidebar': True,
    'show_related': True,
    'description': 'LiberZMQ - 高性能ZeroMQ消息传递库',
    'logo': None,
    'logo_name': True,
    'body_text_align': 'left',
}

# 添加任何包含自定义静态文件的路径（如样式表），相对于此目录
html_static_path = ['_static']

# 自定义侧边栏模板
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'relations.html',
        'searchbox.html',
        'donate.html',
    ]
}

# -- autodoc配置 ------------------------------------------------

# autodoc选项
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

# -- Napoleon配置 -----------------------------------------------

# Napoleon设置
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True

# -- Intersphinx配置 --------------------------------------------

# Intersphinx映射
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'zmq': ('https://pyzmq.readthedocs.io/en/latest/', None),
}

# -- Todo扩展配置 -----------------------------------------------

# 显示todo项目
todo_include_todos = True