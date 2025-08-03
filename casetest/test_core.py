#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
核心功能测试

测试liberzmq库的核心功能，包括配置管理、序列化器和异常处理。
"""

import unittest
import tempfile
import os
import json
import yaml
import logging
from unittest.mock import patch, MagicMock

from liberzmq.core.config import Config, load_config
from liberzmq.serializers.factory import (
    SerializerFactory, get_serializer, register_serializer,
    list_available_serializers, validate_all_serializers
)
from liberzmq.serializers.json_serializer import JSONSerializer
from liberzmq.serializers.binary_serializer import BinarySerializer
from liberzmq.serializers.protobuf_serializer import ProtobufSerializer
from liberzmq.exceptions.base import (
    LiberZMQError, ConnectionError, SerializationError,
    ValidationError, TimeoutError, ConfigurationError, MessageError
)
from liberzmq.logging.logger import get_logger, configure_logging

# 添加父目录到路径以便导入
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from casetest import get_test_address, get_test_data, get_test_serializers


class TestConfig(unittest.TestCase):
    """配置管理测试"""
    
    def setUp(self):
        """测试前准备"""
        self.config = Config()
    
    def test_default_config(self):
        """测试默认配置"""
        # 检查默认值
        self.assertEqual(self.config.get('zmq.linger'), 1000)
        self.assertEqual(self.config.get('logging.level'), 'INFO')
        self.assertEqual(self.config.get('serialization.default'), 'json')
    
    def test_set_get_config(self):
        """测试设置和获取配置"""
        # 设置配置
        self.config.set('test.key', 'test_value')
        self.assertEqual(self.config.get('test.key'), 'test_value')
        
        # 测试嵌套键
        self.config.set('nested.deep.key', 42)
        self.assertEqual(self.config.get('nested.deep.key'), 42)
        
        # 测试默认值
        self.assertEqual(self.config.get('nonexistent.key', 'default'), 'default')
    
    def test_update_config(self):
        """测试批量更新配置"""
        updates = {
            'zmq.send_timeout': 3000,
            'logging.level': 'DEBUG',
            'custom.setting': True
        }
        
        self.config.update(updates)
        
        self.assertEqual(self.config.get('zmq.send_timeout'), 3000)
        self.assertEqual(self.config.get('logging.level'), 'DEBUG')
        self.assertEqual(self.config.get('custom.setting'), True)
    
    def test_load_from_dict(self):
        """测试从字典加载配置"""
        config_dict = {
            'zmq': {'linger': 2000, 'send_timeout': 4000},
            'logging': {'level': 'WARNING'},
            'serialization': {'default': 'binary'}
        }
        
        self.config.load_from_dict(config_dict)
        
        self.assertEqual(self.config.get('zmq.linger'), 2000)
        self.assertEqual(self.config.get('zmq.send_timeout'), 4000)
        self.assertEqual(self.config.get('logging.level'), 'WARNING')
        self.assertEqual(self.config.get('serialization.default'), 'binary')
    
    def test_load_from_file(self):
        """测试从文件加载配置"""
        # 创建临时配置文件
        config_data = {
            'zmq': {'linger': 1500},
            'logging': {'level': 'ERROR'}
        }
        
        # 测试JSON文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            json_file = f.name
        
        try:
            self.config.load_from_file(json_file)
            self.assertEqual(self.config.get('zmq.linger'), 1500)
            self.assertEqual(self.config.get('logging.level'), 'ERROR')
        finally:
            os.unlink(json_file)
        
        # 测试YAML文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            yaml_file = f.name
        
        try:
            self.config.load_from_file(yaml_file)
            self.assertEqual(self.config.get('zmq.linger'), 1500)
            self.assertEqual(self.config.get('logging.level'), 'ERROR')
        finally:
            os.unlink(yaml_file)
    
    @patch.dict(os.environ, {'LIBERZMQ_ZMQ_LINGER': '3000', 'LIBERZMQ_LOGGING_LEVEL': 'DEBUG'})
    def test_load_from_env(self):
        """测试从环境变量加载配置"""
        self.config.load_from_env()
        
        self.assertEqual(self.config.get('zmq.linger'), 3000)
        self.assertEqual(self.config.get('logging.level'), 'DEBUG')
    
    def test_to_dict(self):
        """测试导出为字典"""
        self.config.set('test.key1', 'value1')
        self.config.set('test.key2', 42)
        
        config_dict = self.config.to_dict()
        
        self.assertIsInstance(config_dict, dict)
        self.assertEqual(config_dict['test']['key1'], 'value1')
        self.assertEqual(config_dict['test']['key2'], 42)
    
    def test_load_config_function(self):
        """测试load_config函数"""
        # 测试从字典加载
        config_dict = {'zmq': {'linger': 2500}}
        config = load_config(config_dict)
        self.assertEqual(config.get('zmq.linger'), 2500)
        
        # 测试从文件加载
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_dict, f)
            json_file = f.name
        
        try:
            config = load_config(json_file)
            self.assertEqual(config.get('zmq.linger'), 2500)
        finally:
            os.unlink(json_file)


class TestSerializers(unittest.TestCase):
    """序列化器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.factory = SerializerFactory()
        self.test_data = get_test_data('complex')
    
    def test_json_serializer(self):
        """测试JSON序列化器"""
        serializer = JSONSerializer()
        
        # 测试序列化和反序列化
        serialized = serializer.serialize(self.test_data)
        self.assertIsInstance(serialized, bytes)
        
        deserialized = serializer.deserialize(serialized)
        self.assertEqual(deserialized, self.test_data)
        
        # 测试特殊数据类型
        import datetime
        special_data = {
            'datetime': datetime.datetime.now(),
            'set': {1, 2, 3},
            'bytes': b'test'
        }
        
        serialized = serializer.serialize(special_data)
        deserialized = serializer.deserialize(serialized)
        
        # 检查类型保持（新的正确行为）
        self.assertIsInstance(deserialized['datetime'], datetime.datetime)
        self.assertIsInstance(deserialized['set'], set)
        self.assertIsInstance(deserialized['bytes'], bytes)
        
        # 检查值的正确性
        self.assertEqual(deserialized['set'], special_data['set'])
        self.assertEqual(deserialized['bytes'], special_data['bytes'])
    
    def test_binary_serializer(self):
        """测试二进制序列化器"""
        # 测试不压缩
        serializer = BinarySerializer(use_compression=False)
        
        serialized = serializer.serialize(self.test_data)
        self.assertIsInstance(serialized, bytes)
        
        deserialized = serializer.deserialize(serialized)
        self.assertEqual(deserialized, self.test_data)
        
        # 测试压缩
        serializer_compressed = BinarySerializer(use_compression=True)
        
        serialized_compressed = serializer_compressed.serialize(self.test_data)
        self.assertIsInstance(serialized_compressed, bytes)
        
        deserialized_compressed = serializer_compressed.deserialize(serialized_compressed)
        self.assertEqual(deserialized_compressed, self.test_data)
        
        # 压缩版本应该更小（对于大数据）
        large_data = {'data': 'x' * 1000}
        normal_size = len(serializer.serialize(large_data))
        compressed_size = len(serializer_compressed.serialize(large_data))
        self.assertLess(compressed_size, normal_size)
    
    def test_protobuf_serializer(self):
        """测试Protobuf序列化器"""
        serializer = ProtobufSerializer()
        
        # 测试字典数据（使用JSON回退）
        serialized = serializer.serialize(self.test_data)
        self.assertIsInstance(serialized, bytes)
        
        deserialized = serializer.deserialize(serialized)
        self.assertEqual(deserialized, self.test_data)
    
    def test_serializer_factory(self):
        """测试序列化器工厂"""
        # 测试创建内置序列化器
        json_serializer = self.factory.create('json')
        self.assertIsInstance(json_serializer, JSONSerializer)
        
        binary_serializer = self.factory.create('binary')
        self.assertIsInstance(binary_serializer, BinarySerializer)
        
        protobuf_serializer = self.factory.create('protobuf')
        self.assertIsInstance(protobuf_serializer, ProtobufSerializer)
        
        # 测试别名
        pb_serializer = self.factory.create('pb')
        self.assertIsInstance(pb_serializer, ProtobufSerializer)
        
        pickle_serializer = self.factory.create('pickle')
        self.assertIsInstance(pickle_serializer, BinarySerializer)
    
    def test_factory_registration(self):
        """测试工厂注册功能"""
        # 创建自定义序列化器
        class CustomSerializer(JSONSerializer):
            def serialize(self, data, **kwargs):
                return super().serialize({'custom': data}, **kwargs)
            
            def deserialize(self, data, **kwargs):
                result = super().deserialize(data, **kwargs)
                return result.get('custom')
        
        # 注册自定义序列化器
        self.factory.register('custom', CustomSerializer)
        
        # 测试创建
        custom_serializer = self.factory.create('custom')
        self.assertIsInstance(custom_serializer, CustomSerializer)
        
        # 测试功能
        test_data = {'test': 'data'}
        serialized = custom_serializer.serialize(test_data)
        deserialized = custom_serializer.deserialize(serialized)
        self.assertEqual(deserialized, test_data)
    
    def test_factory_caching(self):
        """测试工厂缓存功能"""
        # 测试缓存
        serializer1 = self.factory.create('json', cache=True)
        serializer2 = self.factory.create('json', cache=True)
        self.assertIs(serializer1, serializer2)
        
        # 测试不缓存
        serializer3 = self.factory.create('json', cache=False)
        serializer4 = self.factory.create('json', cache=False)
        self.assertIsNot(serializer3, serializer4)
    
    def test_factory_validation(self):
        """测试工厂验证功能"""
        # 测试所有序列化器
        results = validate_all_serializers()
        
        for serializer_name, is_valid in results.items():
            self.assertTrue(is_valid, f"序列化器 {serializer_name} 验证失败")
    
    def test_convenience_functions(self):
        """测试便捷函数"""
        # 测试get_serializer
        serializer = get_serializer('json')
        self.assertIsInstance(serializer, JSONSerializer)
        
        # 测试list_available_serializers
        available = list_available_serializers()
        self.assertIn('json', available)
        self.assertIn('binary', available)
        self.assertIn('protobuf', available)
    
    def test_serializer_errors(self):
        """测试序列化器错误处理"""
        # 测试未知序列化器
        with self.assertRaises(ConfigurationError):
            self.factory.create('unknown_serializer')
        
        # 测试序列化错误
        serializer = JSONSerializer()
        
        # 创建不可序列化的对象
        class UnserializableClass:
            pass
        
        unserializable_data = UnserializableClass()
        
        with self.assertRaises(SerializationError):
            serializer.serialize(unserializable_data)


class TestExceptions(unittest.TestCase):
    """异常处理测试"""
    
    def test_base_exception(self):
        """测试基础异常"""
        error = LiberZMQError("测试错误")
        self.assertEqual(str(error), "测试错误")
        self.assertIsNone(error.details)
        
        # 测试带详细信息的异常
        error_with_details = LiberZMQError("测试错误", details="详细信息")
        self.assertEqual(error_with_details.details, "详细信息")
    
    def test_connection_error(self):
        """测试连接错误"""
        error = ConnectionError("连接失败", address="tcp://localhost:5555")
        self.assertEqual(error.address, "tcp://localhost:5555")
        self.assertIn("连接失败", str(error))
    
    def test_serialization_error(self):
        """测试序列化错误"""
        error = SerializationError("序列化失败", serializer="json")
        self.assertEqual(error.serializer, "json")
        self.assertIn("序列化失败", str(error))
    
    def test_validation_error(self):
        """测试验证错误"""
        error = ValidationError("验证失败", field="test_field", value="invalid_value")
        self.assertEqual(error.field, "test_field")
        self.assertEqual(error.value, "invalid_value")
        self.assertIn("验证失败", str(error))
    
    def test_timeout_error(self):
        """测试超时错误"""
        error = TimeoutError("操作超时", operation="send", timeout=5000)
        self.assertEqual(error.operation, "send")
        self.assertEqual(error.timeout, 5000)
        self.assertIn("操作超时", str(error))
    
    def test_configuration_error(self):
        """测试配置错误"""
        error = ConfigurationError("配置错误", config_key="test.key")
        self.assertEqual(error.config_key, "test.key")
        self.assertIn("配置错误", str(error))
    
    def test_message_error(self):
        """测试消息错误"""
        error = MessageError("消息错误", operation="receive")
        self.assertEqual(error.operation, "receive")
        self.assertIn("消息错误", str(error))


class TestLogging(unittest.TestCase):
    """日志功能测试"""
    
    def test_get_logger(self):
        """测试获取日志器"""
        logger = get_logger('test_module')
        self.assertEqual(logger.name, 'liberzmq.test_module')
    
    def test_configure_logging(self):
        """测试配置日志"""
        # 测试基本配置
        configure_logging(level='DEBUG')
        logger = get_logger('test')
        self.assertEqual(logger.level, 10)  # DEBUG level
        
        # 测试文件日志
        import tempfile
        import time
        
        # 创建临时文件路径
        temp_dir = tempfile.gettempdir()
        log_file = os.path.join(temp_dir, f'test_log_{int(time.time())}.log')
        
        try:
            configure_logging(level='INFO', file_path=log_file)
            logger = get_logger('test_file')
            logger.info('测试日志消息')
            
            # 等待一下确保日志写入
            time.sleep(0.1)
            
            # 检查文件是否存在且有内容
            self.assertTrue(os.path.exists(log_file))
            with open(log_file, 'r', encoding='utf-8') as f:
                content = f.read()
                self.assertIn('测试日志消息', content)
        finally:
            # 清理日志处理器
            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                if hasattr(handler, 'baseFilename') and handler.baseFilename == log_file:
                    handler.close()
                    root_logger.removeHandler(handler)
            
            # 删除临时文件
            if os.path.exists(log_file):
                try:
                    os.unlink(log_file)
                except PermissionError:
                    pass  # 忽略权限错误


if __name__ == '__main__':
    unittest.main()