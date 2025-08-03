#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LiberZMQ构建脚本
自动化文档构建和打包过程
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path


def run_command(cmd, cwd=None):
    """运行命令并返回结果"""
    print(f"执行命令: {cmd}")
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            cwd=cwd, 
            capture_output=True, 
            text=True, 
            encoding='utf-8'
        )
        if result.returncode != 0:
            print(f"命令执行失败: {result.stderr}")
            return False
        else:
            print(f"命令执行成功: {result.stdout}")
            return True
    except Exception as e:
        print(f"命令执行异常: {e}")
        return False


def clean_build():
    """清理构建文件"""
    print("清理构建文件...")
    
    # 清理Python构建文件
    dirs_to_clean = [
        'build',
        'dist',
        'liberzmq.egg-info',
        'docs/_build'
    ]
    
    for dir_name in dirs_to_clean:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
            print(f"已删除: {dir_name}")
    
    # 清理__pycache__目录
    for root, dirs, files in os.walk('.'):
        for dir_name in dirs[:]:
            if dir_name == '__pycache__':
                full_path = os.path.join(root, dir_name)
                shutil.rmtree(full_path)
                print(f"已删除: {full_path}")
                dirs.remove(dir_name)
    
    print("构建文件清理完成")


def install_dependencies():
    """安装依赖"""
    print("安装依赖...")
    
    # 安装基础依赖
    if not run_command("pip install -r requirements.txt"):
        return False
    
    # 安装开发依赖
    dev_deps = [
        "sphinx>=5.0.0",
        "sphinx-rtd-theme>=1.0.0",
        "pytest>=7.0.0",
        "pytest-cov>=4.0.0",
        "wheel",
        "twine"
    ]
    
    for dep in dev_deps:
        if not run_command(f"pip install {dep}"):
            print(f"警告: 安装 {dep} 失败")
    
    print("依赖安装完成")
    return True


def run_tests():
    """运行测试"""
    print("运行测试...")
    
    # 运行快速测试
    if not run_command("python -m casetest.run_tests --quick"):
        print("警告: 测试失败，但继续构建")
        return False
    
    print("测试完成")
    return True


def build_docs():
    """构建文档"""
    print("构建Sphinx文档...")
    
    docs_dir = Path("docs")
    if not docs_dir.exists():
        print("错误: docs目录不存在")
        return False
    
    # 构建HTML文档
    if not run_command("sphinx-build -b html . _build/html", cwd="docs"):
        print("错误: Sphinx文档构建失败")
        return False
    
    print("文档构建完成")
    return True


def build_package():
    """构建Python包"""
    print("构建Python包...")
    
    # 构建源码分发包
    if not run_command("python setup.py sdist"):
        print("错误: 源码包构建失败")
        return False
    
    # 构建wheel包
    if not run_command("python setup.py bdist_wheel"):
        print("错误: wheel包构建失败")
        return False
    
    print("Python包构建完成")
    return True


def check_package():
    """检查包的完整性"""
    print("检查包完整性...")
    
    # 检查分发包
    if not run_command("twine check dist/*"):
        print("警告: 包检查失败")
        return False
    
    print("包检查完成")
    return True


def main():
    """主函数"""
    print("LiberZMQ构建脚本")
    print("=" * 50)
    
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='LiberZMQ构建脚本')
    parser.add_argument('--clean', action='store_true', help='清理构建文件')
    parser.add_argument('--deps', action='store_true', help='安装依赖')
    parser.add_argument('--test', action='store_true', help='运行测试')
    parser.add_argument('--docs', action='store_true', help='构建文档')
    parser.add_argument('--package', action='store_true', help='构建包')
    parser.add_argument('--check', action='store_true', help='检查包')
    parser.add_argument('--all', action='store_true', help='执行所有步骤')
    
    args = parser.parse_args()
    
    # 如果没有指定参数，默认执行所有步骤
    if not any([args.clean, args.deps, args.test, args.docs, args.package, args.check]):
        args.all = True
    
    success = True
    
    try:
        if args.all or args.clean:
            clean_build()
        
        if args.all or args.deps:
            if not install_dependencies():
                success = False
        
        if args.all or args.test:
            if not run_tests():
                print("警告: 测试失败，但继续构建")
        
        if args.all or args.docs:
            if not build_docs():
                success = False
        
        if args.all or args.package:
            if not build_package():
                success = False
        
        if args.all or args.check:
            if not check_package():
                success = False
        
        if success:
            print("\n" + "=" * 50)
            print("构建完成！")
            print("\n生成的文件:")
            
            # 显示生成的文件
            if os.path.exists('dist'):
                for file in os.listdir('dist'):
                    print(f"  - dist/{file}")
            
            if os.path.exists('docs/_build/html'):
                print(f"  - docs/_build/html/index.html (文档)")
            
            print("\n使用说明:")
            print("  安装包: pip install dist/liberzmq-1.0.0-py3-none-any.whl")
            print("  查看文档: 打开 docs/_build/html/index.html")
            print("  发布包: twine upload dist/*")
        else:
            print("\n构建过程中出现错误！")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n构建被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n构建过程中出现异常: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()