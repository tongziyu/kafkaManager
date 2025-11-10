import os
import json
from typing import Dict, Any, List
from pathlib import Path


class Settings:
    """应用程序配置管理类"""

    def __init__(self):
        # 应用基本信息
        self.APP_NAME = "Kafka管理工具"
        self.APP_VERSION = "1.0.0"
        self.APP_AUTHOR = "Your Name"

        # 配置文件路径
        self.config_dir = Path.home() / ".kafka_manager"
        self.config_file = self.config_dir / "config.json"

        # 确保配置目录存在
        self.config_dir.mkdir(exist_ok=True)

        # 默认配置
        self.default_settings = {
            "ui": {
                "window_width": 1000,
                "window_height": 700,
                "window_x": 100,
                "window_y": 100,
                "theme": "default",  # default, dark, light
                "font_size": 10,
                "language": "zh_CN",  # zh_CN, en_US
                "auto_connect": False,
                "refresh_interval": 30,  # 自动刷新间隔（秒）
            },
            "kafka": {
                "bootstrap_servers": ["123.56.111.122:9092"],
                "client_id": "kafka_manager",
                "security_protocol": "SASL_PLAINTEXT",  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": "cametapp2",
                "sasl_plain_password": "ITjihgN0EG2P9tAPHuci",
                "ssl_cafile": "",
                "ssl_certfile": "",
                "ssl_keyfile": "",
                "request_timeout_ms": 15000,
                "api_version_auto_timeout_ms": 15000,
                "consumer_timeout_ms": 1000,
                # 若Kafka版本已知，可填写如"2.6.0"，否则保持空由客户端自动探测
                "api_version": "",
                "max_poll_records": 100,
            },
            "connections": [
                {
                    "name": "本地开发环境",
                    "bootstrap_servers": "localhost:9092",
                    "security_protocol": "PLAINTEXT"
                },
                {
                    "name": "测试环境",
                    "bootstrap_servers": "kafka-test:9092",
                    "security_protocol": "PLAINTEXT"
                }
            ],
            "consumer": {
                "auto_offset_reset": "latest",  # earliest, latest
                "enable_auto_commit": False,
                "group_id": "kafka_manager_ui",
                "value_deserializer": "bytes",
                "key_deserializer": "bytes",
            },
            "producer": {
                "acks": "all",
                "retries": 3,
                "batch_size": 16384,
                "linger_ms": 0,
                "buffer_memory": 33554432,
            },
            "topics": {
                "default_partitions": 1,
                "default_replication_factor": 1,
                "default_configs": {
                    "retention.ms": "604800000",  # 7天
                    "cleanup.policy": "delete"
                }
            },
            "logging": {
                "level": "INFO",  # DEBUG, INFO, WARNING, ERROR
                "max_files": 10,
                "max_file_size": 10485760,  # 10MB
                "log_to_file": True,
            },
            "features": {
                "auto_refresh": True,
                "confirm_deletions": True,
                "show_internal_topics": False,
                "color_scheme": "system",  # system, dark, light
                "message_display_format": "json"  # json, text, hex
            }
        }

        # 当前配置
        self.current_settings = self.default_settings.copy()

        # 加载保存的配置
        self.load_settings()

    def load_settings(self) -> bool:
        """从文件加载配置"""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    saved_settings = json.load(f)
                    self._deep_update(self.current_settings, saved_settings)
                print(f"配置已从 {self.config_file} 加载")
                return True
            else:
                print("未找到配置文件，使用默认配置")
                return False
        except Exception as e:
            print(f"加载配置失败: {e}，使用默认配置")
            return False

    def save_settings(self) -> bool:
        """保存配置到文件"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.current_settings, f, indent=4, ensure_ascii=False)
            print(f"配置已保存到 {self.config_file}")
            return True
        except Exception as e:
            print(f"保存配置失败: {e}")
            return False

    def _deep_update(self, original: Dict, update: Dict) -> Dict:
        """深度更新字典"""
        for key, value in update.items():
            if isinstance(value, dict) and key in original and isinstance(original[key], dict):
                self._deep_update(original[key], value)
            else:
                original[key] = value
        return original

    def get(self, key_path: str, default=None) -> Any:
        """通过路径获取配置值，例如 'ui.window_width'"""
        keys = key_path.split('.')
        value = self.current_settings

        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

    def set(self, key_path: str, value: Any) -> bool:
        """通过路径设置配置值"""
        keys = key_path.split('.')
        settings_ref = self.current_settings

        try:
            for key in keys[:-1]:
                if key not in settings_ref:
                    settings_ref[key] = {}
                settings_ref = settings_ref[key]

            settings_ref[keys[-1]] = value
            return True
        except Exception as e:
            print(f"设置配置失败: {e}")
            return False

    def add_connection(self, name: str, bootstrap_servers: str, **kwargs):
        """添加快捷连接"""
        connection = {
            "name": name,
            "bootstrap_servers": bootstrap_servers,
            **kwargs
        }

        connections = self.get("connections", [])
        connections.append(connection)
        self.set("connections", connections)

    def remove_connection(self, name: str) -> bool:
        """移除快捷连接"""
        connections = self.get("connections", [])
        updated_connections = [conn for conn in connections if conn["name"] != name]

        if len(updated_connections) != len(connections):
            self.set("connections", updated_connections)
            return True
        return False

    def get_connection_names(self) -> List[str]:
        """获取所有连接名称"""
        connections = self.get("connections", [])
        return [conn["name"] for conn in connections]

    def get_connection_by_name(self, name: str) -> Dict:
        """根据名称获取连接配置"""
        connections = self.get("connections", [])
        for conn in connections:
            if conn["name"] == name:
                return conn
        return {}

    def reset_to_defaults(self):
        """重置为默认配置"""
        self.current_settings = self.default_settings.copy()
        self.save_settings()

    @property
    def kafka_config(self) -> Dict:
        """获取Kafka配置"""
        return self.get("kafka", {})

    @property
    def ui_config(self) -> Dict:
        """获取UI配置"""
        return self.get("ui", {})

    @property
    def consumer_config(self) -> Dict:
        """获取消费者配置"""
        return self.get("consumer", {})

    @property
    def producer_config(self) -> Dict:
        """获取生产者配置"""
        return self.get("producer", {})


# 全局配置实例
settings = Settings()