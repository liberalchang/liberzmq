#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试运行器

运行所有测试模块并生成测试报告。
"""

import unittest
import sys
import os
import time
from io import StringIO

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入测试模块
from casetest import test_core
from casetest import test_pubsub
from casetest import test_reqrep
from casetest import test_pushpull
from casetest import test_integration


class ColoredTextTestResult(unittest.TextTestResult):
    """带颜色的测试结果"""
    
    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self.success_count = 0
        self.verbosity = verbosity  # 显式保存verbosity属性
    
    def addSuccess(self, test):
        super().addSuccess(test)
        self.success_count += 1
        if self.verbosity > 1:
            self.stream.write("\033[92m✓\033[0m ")
            self.stream.write(self.getDescription(test))
            self.stream.writeln()
    
    def addError(self, test, err):
        super().addError(test, err)
        if self.verbosity > 1:
            self.stream.write("\033[91m✗\033[0m ")
            self.stream.write(self.getDescription(test))
            self.stream.writeln(" (ERROR)")
    
    def addFailure(self, test, err):
        super().addFailure(test, err)
        if self.verbosity > 1:
            self.stream.write("\033[91m✗\033[0m ")
            self.stream.write(self.getDescription(test))
            self.stream.writeln(" (FAIL)")
    
    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        if self.verbosity > 1:
            self.stream.write("\033[93m-\033[0m ")
            self.stream.write(self.getDescription(test))
            self.stream.writeln(f" (SKIP: {reason})")


class ColoredTextTestRunner(unittest.TextTestRunner):
    """带颜色的测试运行器"""
    
    def _makeResult(self):
        return ColoredTextTestResult(self.stream, self.descriptions, self.verbosity)


def create_test_suite():
    """创建测试套件"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # 添加测试模块
    test_modules = [
        test_core,
        test_pubsub,
        test_reqrep,
        test_pushpull,
        test_integration
    ]
    
    for module in test_modules:
        suite.addTests(loader.loadTestsFromModule(module))
    
    return suite


def run_specific_tests(test_pattern=None):
    """运行特定的测试"""
    if test_pattern:
        loader = unittest.TestLoader()
        suite = loader.discover('casetest', pattern=test_pattern)
    else:
        suite = create_test_suite()
    
    runner = ColoredTextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        buffer=True
    )
    
    print("\n" + "="*70)
    print("LiberZMQ 测试套件")
    print("="*70)
    
    start_time = time.time()
    result = runner.run(suite)
    end_time = time.time()
    
    # 打印测试总结
    print("\n" + "="*70)
    print("测试总结")
    print("="*70)
    
    total_tests = result.testsRun
    success_count = getattr(result, 'success_count', total_tests - len(result.failures) - len(result.errors))
    failure_count = len(result.failures)
    error_count = len(result.errors)
    skip_count = len(result.skipped)
    
    print(f"总测试数: {total_tests}")
    print(f"\033[92m成功: {success_count}\033[0m")
    if failure_count > 0:
        print(f"\033[91m失败: {failure_count}\033[0m")
    if error_count > 0:
        print(f"\033[91m错误: {error_count}\033[0m")
    if skip_count > 0:
        print(f"\033[93m跳过: {skip_count}\033[0m")
    
    duration = end_time - start_time
    print(f"\n执行时间: {duration:.2f} 秒")
    
    success_rate = (success_count / total_tests) * 100 if total_tests > 0 else 0
    print(f"成功率: {success_rate:.1f}%")
    
    # 打印失败和错误的详细信息
    if result.failures:
        print("\n" + "="*70)
        print("失败详情")
        print("="*70)
        for test, traceback in result.failures:
            print(f"\n\033[91mFAIL: {test}\033[0m")
            print("-" * 50)
            print(traceback)
    
    if result.errors:
        print("\n" + "="*70)
        print("错误详情")
        print("="*70)
        for test, traceback in result.errors:
            print(f"\n\033[91mERROR: {test}\033[0m")
            print("-" * 50)
            print(traceback)
    
    return result.wasSuccessful()


def run_module_tests(module_name):
    """运行特定模块的测试"""
    module_map = {
        'core': test_core,
        'pubsub': test_pubsub,
        'reqrep': test_reqrep,
        'pushpull': test_pushpull,
        'integration': test_integration
    }
    
    if module_name not in module_map:
        print(f"未知的测试模块: {module_name}")
        print(f"可用模块: {', '.join(module_map.keys())}")
        return False
    
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(module_map[module_name])
    
    runner = ColoredTextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        buffer=True
    )
    
    print(f"\n运行 {module_name} 模块测试...")
    print("="*50)
    
    start_time = time.time()
    result = runner.run(suite)
    end_time = time.time()
    
    duration = end_time - start_time
    print(f"\n{module_name} 模块测试完成，耗时: {duration:.2f} 秒")
    
    return result.wasSuccessful()


def run_quick_tests():
    """运行快速测试（跳过性能和集成测试）"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # 只添加核心功能测试
    test_modules = [
        test_core,
        test_pubsub,
        test_reqrep,
        test_pushpull
    ]
    
    for module in test_modules:
        module_suite = loader.loadTestsFromModule(module)
        # 过滤掉性能测试
        filtered_suite = unittest.TestSuite()
        for test_group in module_suite:
            for test in test_group:
                test_name = test._testMethodName
                if not any(keyword in test_name.lower() for keyword in ['performance', 'load', 'stress']):
                    filtered_suite.addTest(test)
        suite.addTest(filtered_suite)
    
    runner = ColoredTextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        buffer=True
    )
    
    print("\n运行快速测试...")
    print("="*50)
    
    start_time = time.time()
    result = runner.run(suite)
    end_time = time.time()
    
    duration = end_time - start_time
    print(f"\n快速测试完成，耗时: {duration:.2f} 秒")
    
    return result.wasSuccessful()


def generate_test_report():
    """生成测试报告"""
    # 捕获测试输出
    output = StringIO()
    
    # 重定向标准输出
    original_stdout = sys.stdout
    sys.stdout = output
    
    try:
        success = run_specific_tests()
    finally:
        sys.stdout = original_stdout
    
    # 获取测试输出
    test_output = output.getvalue()
    
    # 生成报告文件
    report_file = os.path.join(os.path.dirname(__file__), 'test_report.txt')
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("LiberZMQ 测试报告\n")
        f.write("=" * 50 + "\n")
        f.write(f"生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")
        f.write(test_output)
    
    print(f"测试报告已生成: {report_file}")
    return success


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='LiberZMQ 测试运行器')
    parser.add_argument('--module', '-m', help='运行特定模块的测试')
    parser.add_argument('--pattern', '-p', help='测试文件模式')
    parser.add_argument('--quick', '-q', action='store_true', help='运行快速测试')
    parser.add_argument('--report', '-r', action='store_true', help='生成测试报告')
    parser.add_argument('--list', '-l', action='store_true', help='列出所有测试')
    
    args = parser.parse_args()
    
    if args.list:
        # 列出所有测试
        suite = create_test_suite()
        print("可用测试:")
        for test_group in suite:
            for test in test_group:
                print(f"  {test}")
        return
    
    if args.report:
        success = generate_test_report()
    elif args.module:
        success = run_module_tests(args.module)
    elif args.quick:
        success = run_quick_tests()
    elif args.pattern:
        success = run_specific_tests(args.pattern)
    else:
        success = run_specific_tests()
    
    # 返回适当的退出码
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()