# -*- coding: utf-8 -*-
import logging
import json
import ast
import base64
from datetime import datetime
from PyQt5.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QPushButton, QTextEdit, QListWidget, QLabel,
                             QLineEdit, QTabWidget, QStatusBar, QMessageBox,
                             QComboBox, QSplitter, QDialog, QFormLayout,
                             QGroupBox, QCheckBox, QListWidgetItem, QStyle,
                             QDialogButtonBox, QSpinBox)
from PyQt5.QtGui import QTextCursor
from PyQt5.QtCore import pyqtSignal, QThread, Qt
from core.kafka_client import KafkaManager
from config.settings import settings
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable


class ConnectionConfigDialog(QDialog):
    """连接配置对话框"""
    def __init__(self, parent=None, config=None):
        super().__init__(parent)
        self.config = config or {}
        self.setWindowTitle("连接配置")
        self.setMinimumWidth(500)
        self.init_ui()
        self.load_config()
    
    def get_font(self, size=12, weight=400):
        """获取字体"""
        from PyQt5.QtGui import QFont
        font = QFont()
        font.setFamily("Microsoft YaHei UI, SimHei, Arial, sans-serif")
        font.setPointSize(size)
        font.setWeight(weight)
        return font
    
    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout()
        
        # 基本信息组
        basic_group = QGroupBox("基本信息")
        basic_layout = QFormLayout()
        
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("连接名称（如：生产环境、测试环境）")
        basic_layout.addRow("连接名称:", self.name_input)
        
        self.bootstrap_servers_input = QLineEdit()
        self.bootstrap_servers_input.setPlaceholderText("localhost:9092 或 host1:9092,host2:9092")
        basic_layout.addRow("Kafka地址:", self.bootstrap_servers_input)
        
        basic_group.setLayout(basic_layout)
        layout.addWidget(basic_group)
        
        # 安全配置组
        security_group = QGroupBox("安全配置")
        security_layout = QFormLayout()
        
        self.security_protocol_combo = QComboBox()
        self.security_protocol_combo.addItems(["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
        self.security_protocol_combo.currentTextChanged.connect(self.on_security_protocol_changed)
        security_layout.addRow("安全协议:", self.security_protocol_combo)
        
        self.sasl_mechanism_combo = QComboBox()
        self.sasl_mechanism_combo.addItems(["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
        security_layout.addRow("SASL机制:", self.sasl_mechanism_combo)
        
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("SASL用户名")
        security_layout.addRow("用户名:", self.username_input)
        
        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("SASL密码")
        self.password_input.setEchoMode(QLineEdit.Password)
        security_layout.addRow("密码:", self.password_input)
        
        security_group.setLayout(security_layout)
        layout.addWidget(security_group)
        
        # SSL配置组（可选）
        ssl_group = QGroupBox("SSL配置（可选）")
        ssl_layout = QFormLayout()
        
        self.ssl_cafile_input = QLineEdit()
        self.ssl_cafile_input.setPlaceholderText("CA证书文件路径")
        ssl_layout.addRow("CA证书:", self.ssl_cafile_input)
        
        self.ssl_certfile_input = QLineEdit()
        self.ssl_certfile_input.setPlaceholderText("客户端证书文件路径")
        ssl_layout.addRow("客户端证书:", self.ssl_certfile_input)
        
        self.ssl_keyfile_input = QLineEdit()
        self.ssl_keyfile_input.setPlaceholderText("客户端密钥文件路径")
        ssl_layout.addRow("客户端密钥:", self.ssl_keyfile_input)
        
        ssl_group.setLayout(ssl_layout)
        layout.addWidget(ssl_group)
        
        # 按钮
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        self.save_btn = QPushButton("保存")
        self.save_btn.clicked.connect(self.accept)
        self.cancel_btn = QPushButton("取消")
        self.cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_btn)
        button_layout.addWidget(self.save_btn)
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
        
        # 初始状态
        self.on_security_protocol_changed()
    
    def on_security_protocol_changed(self):
        """当安全协议改变时，更新相关字段的可用性"""
        protocol = self.security_protocol_combo.currentText()
        is_sasl = protocol in ["SASL_PLAINTEXT", "SASL_SSL"]
        is_ssl = protocol in ["SSL", "SASL_SSL"]
        
        # SASL相关字段
        self.sasl_mechanism_combo.setEnabled(is_sasl)
        self.username_input.setEnabled(is_sasl)
        self.password_input.setEnabled(is_sasl)
        
        # SSL相关字段
        self.ssl_cafile_input.setEnabled(is_ssl)
        self.ssl_certfile_input.setEnabled(is_ssl)
        self.ssl_keyfile_input.setEnabled(is_ssl)
    
    def load_config(self):
        """加载配置"""
        if self.config:
            self.name_input.setText(self.config.get("name", ""))
            self.bootstrap_servers_input.setText(self.config.get("bootstrap_servers", ""))
            self.security_protocol_combo.setCurrentText(self.config.get("security_protocol", "PLAINTEXT"))
            self.sasl_mechanism_combo.setCurrentText(self.config.get("sasl_mechanism", "PLAIN"))
            self.username_input.setText(self.config.get("sasl_plain_username", ""))
            self.password_input.setText(self.config.get("sasl_plain_password", ""))
            self.ssl_cafile_input.setText(self.config.get("ssl_cafile", ""))
            self.ssl_certfile_input.setText(self.config.get("ssl_certfile", ""))
            self.ssl_keyfile_input.setText(self.config.get("ssl_keyfile", ""))
    
    def get_config(self):
        """获取配置"""
        config = {
            "name": self.name_input.text().strip(),
            "bootstrap_servers": self.bootstrap_servers_input.text().strip(),
            "security_protocol": self.security_protocol_combo.currentText(),
            "sasl_mechanism": self.sasl_mechanism_combo.currentText(),
            "sasl_plain_username": self.username_input.text().strip(),
            "sasl_plain_password": self.password_input.text().strip(),
            "ssl_cafile": self.ssl_cafile_input.text().strip(),
            "ssl_certfile": self.ssl_certfile_input.text().strip(),
            "ssl_keyfile": self.ssl_keyfile_input.text().strip(),
        }
        return config


class CreateTopicDialog(QDialog):
    """创建Topic的对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("创建Topic")
        self.setMinimumWidth(400)
        self.result_data = None
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()
        form_layout = QFormLayout()

        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("请输入Topic名称")
        form_layout.addRow("Topic名称:", self.name_input)

        self.partitions_input = QSpinBox()
        self.partitions_input.setRange(1, 1000)
        self.partitions_input.setValue(1)
        form_layout.addRow("分区数:", self.partitions_input)

        self.replication_input = QSpinBox()
        self.replication_input.setRange(1, 10)
        self.replication_input.setValue(1)
        form_layout.addRow("副本数:", self.replication_input)

        self.configs_input = QTextEdit()
        self.configs_input.setPlaceholderText("可选: 一行一个配置，例如\ncleanup.policy=compact")
        self.configs_input.setFixedHeight(100)
        form_layout.addRow("Topic配置:", self.configs_input)

        layout.addLayout(form_layout)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.on_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self.setLayout(layout)

    def parse_configs(self):
        configs_text = self.configs_input.toPlainText().strip()
        if not configs_text:
            return {}

        configs = {}
        for line in configs_text.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                raise ValueError(f"配置格式错误: '{line}'，请使用 key=value 格式")
            key, value = line.split('=', 1)
            configs[key.strip()] = value.strip()
        return configs

    def on_accept(self):
        name = self.name_input.text().strip()
        if not name:
            QMessageBox.warning(self, "输入无效", "Topic名称不能为空")
            return

        try:
            configs = self.parse_configs()
        except ValueError as e:
            QMessageBox.warning(self, "配置错误", str(e))
            return

        self.result_data = {
            'name': name,
            'partitions': self.partitions_input.value(),
            'replication_factor': self.replication_input.value(),
            'configs': configs
        }
        self.accept()

    def get_result(self):
        return self.result_data


class MessageConsumerThread(QThread):
    """消息消费线程"""
    message_received = pyqtSignal(dict)  # 发送接收到的消息
    error_occurred = pyqtSignal(str)  # 发送错误信息
    
    def __init__(self, kafka_manager, topic, auto_offset_reset='latest'):
        super().__init__()
        self.kafka_manager = kafka_manager
        self.topic = topic
        self.auto_offset_reset = auto_offset_reset
        self.consumer = None
        self.is_running = False
        
    def run(self):
        """运行消费线程"""
        try:
            self.consumer = self.kafka_manager.create_consumer(
                self.topic,
                auto_offset_reset=self.auto_offset_reset
            )
            self.is_running = True
            while self.is_running:
                try:
                    records = self.consumer.poll(timeout_ms=200)
                except Exception as poll_error:
                    if self.is_running:
                        self.error_occurred.emit(f"消费消息失败: {str(poll_error)}")
                    break

                if not records:
                    continue

                for _, messages in records.items():
                    if not self.is_running:
                        break
                    for message in messages:
                        if not self.is_running:
                            break
                        try:
                            msg_data = {
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                                'topic': message.topic,
                                'partition': message.partition,
                                'offset': message.offset,
                                'key': message.key.decode('utf-8') if message.key else None,
                                'value': message.value,
                                'headers': dict(message.headers) if message.headers else {}
                            }

                            parsed_value = self.parse_message_value(message.value)
                            msg_data.update(parsed_value)
                            self.message_received.emit(msg_data)

                        except Exception as handle_error:
                            self.error_occurred.emit(f"处理消息失败: {str(handle_error)}")

            self.is_running = False

        except Exception as e:
            self.error_occurred.emit(f"消费消息失败: {str(e)}")
        finally:
            if self.consumer:
                self.consumer.close()
    
    def stop(self):
        """停止消费"""
        self.is_running = False
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass

    def parse_message_value(self, raw_value):
        parsed = {
            'value_raw': raw_value,
            'value_bytes': raw_value if isinstance(raw_value, bytes) else None,
            'value_str': None,
            'value_json': None,
            'value_python': None,
            'value_type': None,
            'formatted_json': None
        }

        def to_pretty_json(obj):
            try:
                return json.dumps(obj, ensure_ascii=False, indent=2)
            except Exception:
                return str(obj)

        if isinstance(raw_value, bytes):
            try:
                text = raw_value.decode('utf-8')
                parsed['value_type'] = 'text'
            except UnicodeDecodeError:
                text = base64.b64encode(raw_value).decode('utf-8')
                parsed['value_type'] = 'binary(base64)'
                parsed['value_str'] = text
                parsed['formatted_json'] = text
                return parsed
        else:
            if isinstance(raw_value, (dict, list)):
                parsed['value_json'] = raw_value
                parsed['value_python'] = raw_value
                parsed['value_type'] = 'json'
                parsed['value_str'] = to_pretty_json(raw_value)
                parsed['formatted_json'] = parsed['value_str']
                return parsed
            elif isinstance(raw_value, (int, float, bool)):
                parsed['value_python'] = raw_value
                parsed['value_type'] = type(raw_value).__name__
                parsed['value_str'] = str(raw_value)
                parsed['formatted_json'] = parsed['value_str']
                return parsed
            text = str(raw_value)
            parsed['value_type'] = type(raw_value).__name__

        parsed['value_str'] = text

        # 尝试解析为JSON
        try:
            json_obj = json.loads(text)
            if isinstance(json_obj, (dict, list)):
                parsed['value_json'] = json_obj
                parsed['value_python'] = json_obj
                parsed['value_type'] = 'json'
                parsed['formatted_json'] = to_pretty_json(json_obj)
                return parsed
        except Exception:
            pass

        # 尝试解析为Python字面量
        try:
            literal_obj = ast.literal_eval(text)
            parsed['value_python'] = literal_obj
            parsed['value_type'] = type(literal_obj).__name__
            if isinstance(literal_obj, (dict, list)):
                parsed['value_json'] = literal_obj
                parsed['formatted_json'] = to_pretty_json(literal_obj)
            else:
                parsed['formatted_json'] = str(literal_obj)
            return parsed
        except Exception:
            pass

        parsed['formatted_json'] = text
        if not parsed['value_type']:
            parsed['value_type'] = 'text'
        return parsed


class TopicLoaderThread(QThread):
    topics_loaded = pyqtSignal(int, list, object)
    error_occurred = pyqtSignal(int, str)

    def __init__(self, kafka_manager, request_id, preselect_topic=None, parent=None):
        super().__init__(parent)
        self.kafka_manager = kafka_manager
        self.request_id = request_id
        self.preselect_topic = preselect_topic

    def run(self):
        try:
            topics = sorted(self.kafka_manager.list_topics())
            self.topics_loaded.emit(self.request_id, topics, self.preselect_topic)
        except Exception as e:
            self.error_occurred.emit(self.request_id, str(e))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings = settings
        self.kafka_manager = None
        self.connection_thread = None
        self.topic_loader_thread = None
        self.topic_loader_request_id = 0
        self.logger = logging.getLogger(__name__)
        self.consumer_thread = None  # 消息消费线程
        self.message_count = 0  # 消息计数
        self.current_connection_config = None  # 当前连接配置
        self.message_queue = []  # 消息队列，用于批量处理
        self.max_message_count = 1000  # 最大保留消息数
        self.init_ui()
        self.setup_connections()
        self.load_connection_configs()
        
        # 定时器用于批量更新UI
        from PyQt5.QtCore import QTimer
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.batch_update_messages)
        self.update_timer.setInterval(100)  # 每100ms更新一次

    def init_ui(self):
        """初始化用户界面"""
        self.setWindowTitle("Kafka管理工具")
        self.setGeometry(100, 100, 1200, 800)

        # 应用全局样式
        self.apply_styles()

        # 创建中心部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # 主布局使用水平排列，左侧为连接配置，右侧为功能区
        main_layout = QHBoxLayout()
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # 1. 左侧连接配置面板
        conn_widget = self.create_connection_widget()
        main_layout.addWidget(conn_widget)

        # 2. 右侧功能区域
        right_container = QWidget()
        right_layout = QVBoxLayout()
        right_layout.setSpacing(10)
        right_layout.setContentsMargins(0, 0, 0, 0)

        self.tab_widget = QTabWidget()
        self.tab_widget.setFont(self.get_font(size=12))
        right_layout.addWidget(self.tab_widget)

        right_container.setLayout(right_layout)
        main_layout.addWidget(right_container)

        # 设置布局伸缩比例，使右侧区域占据更多空间
        main_layout.setStretch(0, 0)
        main_layout.setStretch(1, 1)

        # 创建各个标签页
        self.setup_tabs()

        # 按钮图标
        self.apply_button_icons()

        # 状态栏
        self.setup_statusbar()

        central_widget.setLayout(main_layout)
    
    def get_font(self, size=12, weight=400):
        """获取字体"""
        from PyQt5.QtGui import QFont
        font = QFont()
        font.setFamily("Microsoft YaHei UI, SimHei, Arial, sans-serif")
        font.setPointSize(size)
        font.setWeight(weight)
        return font
    
    def apply_styles(self):
        """应用全局样式"""
        # 使用系统默认样式，不添加额外样式
        pass

    def save_window_geometry(self):
        """保存窗口位置和大小"""
        self.settings.set("ui.window_width", self.width())
        self.settings.set("ui.window_height", self.height())
        self.settings.set("ui.window_x", self.x())
        self.settings.set("ui.window_y", self.y())
        self.settings.save_settings()

    def load_connection_configs(self):
        """加载保存的连接配置"""
        connections = self.settings.get("connections", [])

        self.connection_list.blockSignals(True)
        self.connection_list.clear()

        for conn in connections:
            name = conn.get("name", "未命名连接")
            item = QListWidgetItem(name)
            item.setData(Qt.UserRole, conn)
            self.connection_list.addItem(item)

        self.connection_list.blockSignals(False)

        if self.current_connection_config:
            self.select_connection_by_name(self.current_connection_config.get("name"))
        elif connections:
            self.connection_list.setCurrentRow(0)
        else:
            self.on_connection_config_changed(None, None)
    
    def select_connection_by_name(self, name):
        """根据名称选择连接配置"""
        if not name:
            return

        for row in range(self.connection_list.count()):
            item = self.connection_list.item(row)
            if item.text() == name:
                self.connection_list.setCurrentRow(row)
                return

    def on_connection_config_changed(self, current, previous):
        """当连接配置改变时"""
        if not current:
            self.current_connection_config = None
            self.host_input.clear()
            self.edit_config_btn.setEnabled(False)
            self.delete_config_btn.setEnabled(False)
            return

        config = current.data(Qt.UserRole)
        if config:
            self.current_connection_config = config
            bootstrap_servers = config.get("bootstrap_servers", "")
            self.host_input.setText(bootstrap_servers)
            self.edit_config_btn.setEnabled(True)
            self.delete_config_btn.setEnabled(True)
    
    def new_connection_config(self):
        """新建连接配置"""
        dialog = ConnectionConfigDialog(self)
        if dialog.exec_() == QDialog.Accepted:
            config = dialog.get_config()
            
            # 验证配置
            if not config.get("name"):
                QMessageBox.warning(self, "配置无效", "连接名称不能为空")
                return
            if not config.get("bootstrap_servers"):
                QMessageBox.warning(self, "配置无效", "Kafka地址不能为空")
                return
            
            # 保存配置
            connections = self.settings.get("connections", [])
            connections.append(config)
            self.settings.set("connections", connections)
            self.settings.save_settings()
            
            # 刷新下拉框
            self.load_connection_configs()
            
            # 自动选择新配置
            self.select_connection_by_name(config.get("name"))
            
            QMessageBox.information(self, "成功", "连接配置已保存")
    
    def edit_connection_config(self):
        """编辑连接配置"""
        current_item = self.connection_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "未选择", "请先选择一个连接配置")
            return
        
        config = current_item.data(Qt.UserRole)
        if not config:
            return
        
        dialog = ConnectionConfigDialog(self, config)
        if dialog.exec_() == QDialog.Accepted:
            new_config = dialog.get_config()
            
            # 验证配置
            if not new_config.get("name"):
                QMessageBox.warning(self, "配置无效", "连接名称不能为空")
                return
            if not new_config.get("bootstrap_servers"):
                QMessageBox.warning(self, "配置无效", "Kafka地址不能为空")
                return
            
            # 更新配置
            connections = self.settings.get("connections", [])
            for i, conn in enumerate(connections):
                if conn.get("name") == config.get("name"):
                    connections[i] = new_config
                    break
            
            self.settings.set("connections", connections)
            self.settings.save_settings()
            
            # 刷新下拉框
            self.load_connection_configs()
            
            # 自动选择编辑后的配置
            self.select_connection_by_name(new_config.get("name"))
            
            QMessageBox.information(self, "成功", "连接配置已更新")
    
    def on_topic_selected(self, item):
        """当选中Topic时显示详情"""
        if not item:
            return

        if not self.kafka_manager:
            QMessageBox.warning(self, "未连接", "请先连接到Kafka服务器")
            return

        topic_name = item.text()
        try:
            self.statusBar().showMessage(f"Topic: {topic_name}")
            placeholder = [
                f"Topic: {topic_name}",
                "详细信息功能暂时禁用以提升性能",
                "如需查看请稍后开启或使用命令行工具"
            ]
            self.topic_detail.setPlainText("\n".join(placeholder))
            self.delete_topic_btn.setEnabled(True)
        except Exception as e:
            self.logger.error(f"显示Topic占位信息失败: {str(e)}")
            self.topic_detail.setPlainText(f"获取Topic详情失败: {str(e)}")
            self.statusBar().showMessage("获取Topic详情失败")
    
    def delete_topic(self):
        """删除选中的Topic"""
        topic_name = self.get_selected_topic()
        if not topic_name:
            QMessageBox.warning(self, "未选择", "请先选择一个要删除的Topic")
            return
        
        # 确认删除
        reply = QMessageBox.question(self, "确认删除", 
                                     f"确定要删除Topic '{topic_name}'吗？此操作不可撤销！",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            try:
                self.statusBar().showMessage(f"正在删除Topic: {topic_name}")
                self.kafka_manager.delete_topic(topic_name)
                QMessageBox.information(self, "成功", f"Topic '{topic_name}'已成功删除")
                # 刷新Topic列表
                self.refresh_topics()
                # 清空详情区域
                self.topic_detail.clear()
            except Exception as e:
                self.logger.error(f"删除Topic失败: {str(e)}")
                QMessageBox.critical(self, "错误", f"删除Topic失败: {str(e)}")
            finally:
                self.statusBar().showMessage("就绪")
    
    def delete_connection_config(self):
        """删除连接配置"""
        current_item = self.connection_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "未选择", "请先选择一个连接配置")
            return
        
        config = current_item.data(Qt.UserRole)
        if not config:
            QMessageBox.warning(self, "错误", "无法获取连接配置信息")
            return
        
        # 确认删除
        reply = QMessageBox.question(
            self, 
            "确认删除", 
            f"确定要删除连接配置 '{config.get('name')}' 吗？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            # 删除配置
            connections = self.settings.get("connections", [])
            connections = [conn for conn in connections if conn.get("name") != config.get("name")]
            self.settings.set("connections", connections)
            self.settings.save_settings()
            
            # 刷新下拉框
            self.load_connection_configs()
            
            QMessageBox.information(self, "成功", "连接配置已删除")


    def create_connection_widget(self):
        """创建连接配置区域"""
        conn_widget = QWidget()
        main_layout = QVBoxLayout()
        main_layout.setSpacing(12)
        main_layout.setContentsMargins(10, 10, 10, 10)
        conn_widget.setMinimumWidth(280)
        conn_widget.setMaximumWidth(360)

        # 顶部按钮栏
        button_bar = QHBoxLayout()

        self.new_config_btn = QPushButton("新建配置")
        self.new_config_btn.setMinimumWidth(90)
        button_bar.addWidget(self.new_config_btn)

        self.edit_config_btn = QPushButton("编辑配置")
        self.edit_config_btn.setMinimumWidth(90)
        self.edit_config_btn.setEnabled(False)
        button_bar.addWidget(self.edit_config_btn)

        self.delete_config_btn = QPushButton("删除配置")
        self.delete_config_btn.setMinimumWidth(90)
        self.delete_config_btn.setEnabled(False)
        button_bar.addWidget(self.delete_config_btn)

        button_bar.addStretch()
        main_layout.addLayout(button_bar)

        # 连接配置列表
        main_layout.addWidget(QLabel("连接列表:"))
        self.connection_list = QListWidget()
        self.connection_list.setSelectionMode(QListWidget.SingleSelection)
        self.connection_list.currentItemChanged.connect(self.on_connection_config_changed)
        main_layout.addWidget(self.connection_list, stretch=1)

        # 快速连接区域
        quick_container = QWidget()
        quick_layout = QVBoxLayout()
        quick_layout.setSpacing(6)
        quick_layout.setContentsMargins(0, 0, 0, 0)

        # 连接信息标题
        info_header = QHBoxLayout()
        info_icon_label = QLabel()
        info_icon = self.style().standardIcon(QStyle.SP_FileDialogInfoView)
        info_icon_label.setPixmap(info_icon.pixmap(16, 16))
        info_header.addWidget(info_icon_label)

        info_title = QLabel("连接信息")
        info_title.setStyleSheet("font-weight: bold;")
        info_header.addWidget(info_title)
        info_header.addStretch()
        quick_layout.addLayout(info_header)

        quick_layout.addWidget(QLabel("Kafka地址:"))

        self.host_input = QLineEdit()
        self.host_input.setPlaceholderText("直接输入地址连接（如：localhost:9092）")
        quick_layout.addWidget(self.host_input)

        control_layout = QHBoxLayout()
        self.connect_btn = QPushButton("连接")
        self.connect_btn.setMinimumWidth(80)
        control_layout.addWidget(self.connect_btn)

        self.disconnect_btn = QPushButton("断开")
        self.disconnect_btn.setMinimumWidth(80)
        self.disconnect_btn.setEnabled(False)
        control_layout.addWidget(self.disconnect_btn)

        control_layout.addStretch()
        quick_layout.addLayout(control_layout)

        self.conn_status = QLabel("未连接")
        self.conn_status.setStyleSheet("color: red; font-weight: bold;")
        quick_layout.addWidget(self.conn_status)

        quick_container.setLayout(quick_layout)
        main_layout.addWidget(quick_container)

        conn_widget.setLayout(main_layout)
        return conn_widget

    def setup_tabs(self):
        """设置各个功能标签页"""
        # Topic管理标签页
        self.topic_tab = self.create_topic_tab()
        self.tab_widget.addTab(self.topic_tab, "Topic管理")

        # 消费者组标签页
        self.consumer_tab = self.create_consumer_tab()
        self.tab_widget.addTab(self.consumer_tab, "消费者组")

        # 消息浏览标签页
        self.message_tab = self.create_message_tab()
        self.tab_widget.addTab(self.message_tab, "消息浏览")

    def create_topic_tab(self):
        """创建Topic管理标签页"""
        widget = QWidget()
        layout = QVBoxLayout()

        # 操作按钮区域
        btn_layout = QHBoxLayout()
        self.refresh_topics_btn = QPushButton("刷新Topic列表")
        self.create_topic_btn = QPushButton("创建Topic")
        self.delete_topic_btn = QPushButton("删除Topic")

        btn_layout.addWidget(self.refresh_topics_btn)
        btn_layout.addWidget(self.create_topic_btn)
        btn_layout.addWidget(self.delete_topic_btn)
        btn_layout.addStretch()

        # Topic列表
        layout.addLayout(btn_layout)
        layout.addWidget(QLabel("Topic列表:"))
        self.topic_list = QListWidget()
        layout.addWidget(self.topic_list)

        # Topic详情
        layout.addWidget(QLabel("Topic详情:"))
        self.topic_detail = QTextEdit()
        self.topic_detail.setMaximumHeight(150)
        layout.addWidget(self.topic_detail)

        widget.setLayout(layout)
        return widget

    def create_consumer_tab(self):
        """创建消费者组标签页"""
        widget = QWidget()
        layout = QVBoxLayout()
        layout.addWidget(QLabel("消费者组功能开发中..."))
        widget.setLayout(layout)
        return widget

    def create_message_tab(self):
        """创建消息浏览标签页"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # 控制区域
        control_layout = QHBoxLayout()
        control_layout.addWidget(QLabel("Topic:"))
        
        # Topic选择下拉框
        self.message_topic_combo = QComboBox()
        self.message_topic_combo.setEditable(True)
        self.message_topic_combo.setMinimumWidth(300)
        self.message_topic_combo.setPlaceholderText("选择或输入Topic名称")
        control_layout.addWidget(self.message_topic_combo)
        
        # 消费位置选择
        control_layout.addWidget(QLabel("消费位置:"))
        self.offset_combo = QComboBox()
        self.offset_combo.addItems(["最新消息 (latest)", "最早消息 (earliest)"])
        control_layout.addWidget(self.offset_combo)
        
        # 开始监控按钮
        self.start_monitor_btn = QPushButton("开始监控")
        self.start_monitor_btn.setMinimumWidth(100)
        control_layout.addWidget(self.start_monitor_btn)
        
        # 停止监控按钮
        self.stop_monitor_btn = QPushButton("停止监控")
        self.stop_monitor_btn.setMinimumWidth(100)
        self.stop_monitor_btn.setEnabled(False)
        control_layout.addWidget(self.stop_monitor_btn)
        
        # 清空消息按钮
        self.clear_messages_btn = QPushButton("清空消息")
        self.clear_messages_btn.setMinimumWidth(100)
        control_layout.addWidget(self.clear_messages_btn)
        
        control_layout.addStretch()
        
        # 消息计数标签
        self.message_count_label = QLabel("消息数: 0")
        control_layout.addWidget(self.message_count_label)
        
        layout.addLayout(control_layout)
        
        # 消息显示区域 - 使用分割器
        splitter = QSplitter(Qt.Vertical)
        
        # 消息列表（简化显示）
        list_widget = QWidget()
        list_layout = QVBoxLayout()
        list_layout.addWidget(QLabel("消息列表:"))
        self.message_list = QListWidget()
        self.message_list.setMaximumHeight(150)
        list_layout.addWidget(self.message_list)
        list_widget.setLayout(list_layout)
        splitter.addWidget(list_widget)
        
        # 消息详情（JSON格式化显示）
        detail_widget = QWidget()
        detail_layout = QVBoxLayout()
        detail_header = QHBoxLayout()
        detail_header.addWidget(QLabel("消息详情 (JSON格式):"))
        detail_header.addStretch()
        self.message_search_input = QLineEdit()
        self.message_search_input.setPlaceholderText("输入关键字搜索")
        self.message_search_input.returnPressed.connect(self.search_message_detail)
        detail_header.addWidget(self.message_search_input)
        self.message_search_btn = QPushButton("搜索")
        self.message_search_btn.clicked.connect(self.search_message_detail)
        detail_header.addWidget(self.message_search_btn)
        detail_layout.addLayout(detail_header)
        self.message_detail = QTextEdit()
        self.message_detail.setReadOnly(True)
        self.message_detail.setFontFamily("Courier")
        detail_layout.addWidget(self.message_detail)
        detail_widget.setLayout(detail_layout)
        splitter.addWidget(detail_widget)
        
        # 设置分割器比例
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 3)
        
        layout.addWidget(splitter)
        
        widget.setLayout(layout)
        return widget

    def setup_statusbar(self):
        """设置状态栏"""
        self.statusBar().showMessage("就绪")

    def setup_connections(self):
        """连接信号和槽"""
        self.connect_btn.clicked.connect(self.connect_to_kafka)
        self.disconnect_btn.clicked.connect(self.disconnect_from_kafka)
        self.refresh_topics_btn.clicked.connect(self.refresh_topics)
        self.create_topic_btn.clicked.connect(self.create_topic)
        self.delete_topic_btn.clicked.connect(self.delete_topic)
        
        # 连接配置相关
        self.edit_config_btn.clicked.connect(self.edit_connection_config)
        self.new_config_btn.clicked.connect(self.new_connection_config)
        self.delete_config_btn.clicked.connect(self.delete_connection_config)
        
        # 消息监控相关
        self.start_monitor_btn.clicked.connect(self.start_monitoring)
        self.stop_monitor_btn.clicked.connect(self.stop_monitoring)
        self.clear_messages_btn.clicked.connect(self.clear_messages)
        self.message_list.itemClicked.connect(self.on_message_selected)
        
        # Topic列表选择事件
        self.topic_list.itemClicked.connect(self.on_topic_selected)
        self.topic_list.currentItemChanged.connect(self.on_topic_item_changed)

        # 初始禁用Topic相关按钮
        self.refresh_topics_btn.setEnabled(False)
        self.create_topic_btn.setEnabled(False)
        self.delete_topic_btn.setEnabled(False)
        
        # 初始禁用消息监控按钮
        self.start_monitor_btn.setEnabled(False)

    def apply_button_icons(self):
        """为常用按钮设置图标"""
        style = self.style()

        # 连接管理
        self.new_config_btn.setIcon(style.standardIcon(QStyle.SP_FileDialogNewFolder))
        self.edit_config_btn.setIcon(style.standardIcon(QStyle.SP_FileDialogDetailedView))
        self.delete_config_btn.setIcon(style.standardIcon(QStyle.SP_TrashIcon))
        self.connect_btn.setIcon(style.standardIcon(QStyle.SP_DialogApplyButton))
        self.disconnect_btn.setIcon(style.standardIcon(QStyle.SP_DialogCancelButton))

        # Topic 管理
        self.refresh_topics_btn.setIcon(style.standardIcon(QStyle.SP_BrowserReload))
        self.create_topic_btn.setIcon(style.standardIcon(QStyle.SP_FileDialogNewFolder))
        self.delete_topic_btn.setIcon(style.standardIcon(QStyle.SP_TrashIcon))

        # 消息监控
        self.start_monitor_btn.setIcon(style.standardIcon(QStyle.SP_MediaPlay))
        self.stop_monitor_btn.setIcon(style.standardIcon(QStyle.SP_MediaStop))
        self.clear_messages_btn.setIcon(style.standardIcon(QStyle.SP_DialogResetButton))
        self.message_search_btn.setIcon(style.standardIcon(QStyle.SP_FileDialogContentsView))

    def connect_to_kafka(self):
        """连接到Kafka"""
        if self.connection_thread and self.connection_thread.isRunning():
            return

        class ConnectionWorker(QThread):
            success = pyqtSignal(object, object, dict, dict)
            failed = pyqtSignal(Exception)

            def __init__(self, parent):
                super().__init__(parent)
                self.window = parent

            def run(self):
                try:
                    bootstrap_servers, security_protocol, sasl_mechanism, sasl_plain_username, \
                        sasl_plain_password, ssl_cafile, ssl_certfile, ssl_keyfile, \
                        client_overrides, consumer_overrides = self.window.prepare_kafka_connection_params()

                    kafka_manager = KafkaManager(
                        bootstrap_servers=bootstrap_servers,
                        security_protocol=security_protocol,
                        sasl_mechanism=sasl_mechanism,
                        sasl_plain_username=sasl_plain_username,
                        sasl_plain_password=sasl_plain_password,
                        ssl_cafile=ssl_cafile,
                        ssl_certfile=ssl_certfile,
                        ssl_keyfile=ssl_keyfile,
                        client_config_overrides=client_overrides,
                        consumer_config_overrides=consumer_overrides
                    )

                    self.success.emit(kafka_manager, bootstrap_servers, client_overrides, consumer_overrides)

                except Exception as e:
                    self.failed.emit(e)

        self.connect_btn.setEnabled(False)
        self.statusBar().showMessage("正在连接Kafka...")

        worker = ConnectionWorker(self)

        def on_success(manager, bootstrap, client_overrides, consumer_overrides):
            self.kafka_manager = manager
            self.conn_status.setText("已连接")
            self.conn_status.setStyleSheet("color: green; font-weight: bold;")
            self.disconnect_btn.setEnabled(True)
            self.refresh_topics_btn.setEnabled(True)
            self.create_topic_btn.setEnabled(True)
            self.start_monitor_btn.setEnabled(True)
            self.statusBar().showMessage(f"已连接到 {bootstrap}")
            if self.settings.get("features.auto_refresh", True):
                self.refresh_topics()

        def on_failed(error):
            self.kafka_manager = None
            self.connect_btn.setEnabled(True)
            if isinstance(error, NoBrokersAvailable):
                QMessageBox.critical(self, "连接失败", "未发现可用的Kafka Broker，请检查地址或网络")
                self.statusBar().showMessage("连接失败：未发现可用Broker")
            else:
                QMessageBox.critical(self, "连接失败", f"无法连接到Kafka: {str(error)}")
                self.statusBar().showMessage("连接失败")

        worker.success.connect(on_success)
        worker.failed.connect(on_failed)
        worker.finished.connect(lambda: self.connect_btn.setEnabled(True))
        worker.start()
        self.connection_thread = worker

    def prepare_kafka_connection_params(self):
        if self.current_connection_config:
            config = self.current_connection_config
            bootstrap_servers = config.get("bootstrap_servers", "").strip()
            security_protocol = config.get("security_protocol")
            sasl_mechanism = config.get("sasl_mechanism")
            sasl_plain_username = config.get("sasl_plain_username", "")
            sasl_plain_password = config.get("sasl_plain_password", "")
        else:
            bootstrap_servers = self.host_input.text().strip()
            if not bootstrap_servers:
                raise ValueError("Kafka地址不能为空")

            kafka_config = self.settings.kafka_config
            security_protocol = kafka_config.get("security_protocol")
            sasl_mechanism = kafka_config.get("sasl_mechanism")
            sasl_plain_username = kafka_config.get("sasl_plain_username", "")
            sasl_plain_password = kafka_config.get("sasl_plain_password", "")

        if not bootstrap_servers:
            raise ValueError("Kafka地址不能为空")

        kafka_config = self.settings.kafka_config or {}
        client_overrides = {}
        consumer_overrides = {}

        def normalize_numeric(value):
            if value in (None, ""):
                return None
            if isinstance(value, str):
                try:
                    return int(value)
                except ValueError:
                    return value
            return value

        api_version_value = kafka_config.get("api_version")
        if isinstance(api_version_value, str) and api_version_value.strip():
            try:
                client_overrides["api_version"] = tuple(int(part) for part in api_version_value.strip().split('.'))
            except ValueError:
                self.logger.warning("api_version 配置格式不正确，应为例如 '2.6.0'")
        elif api_version_value is not None:
            client_overrides["api_version"] = api_version_value

        for key in [
            "request_timeout_ms",
            "api_version_auto_timeout_ms",
            "metadata_max_age_ms",
            "connections_max_idle_ms",
            "socket_timeout_ms"
        ]:
            value = normalize_numeric(kafka_config.get(key))
            if value is not None:
                client_overrides[key] = value

        client_id_value = kafka_config.get("client_id")
        if client_id_value:
            client_overrides.setdefault("client_id", client_id_value)

        for key in [
            "max_poll_records",
            "fetch_max_bytes",
            "fetch_min_bytes",
            "max_partition_fetch_bytes",
            "session_timeout_ms",
            "heartbeat_interval_ms",
            "consumer_timeout_ms"
        ]:
            value = normalize_numeric(kafka_config.get(key))
            if value is not None:
                consumer_overrides[key] = value

        consumer_overrides.setdefault("consumer_timeout_ms", 1000)
        session_timeout = normalize_numeric(consumer_overrides.get("session_timeout_ms"))
        if not session_timeout:
            session_timeout = 10000
        consumer_overrides["session_timeout_ms"] = session_timeout

        request_timeout = normalize_numeric(client_overrides.get("request_timeout_ms"))
        min_request_timeout = session_timeout + 5000

        if not request_timeout or request_timeout <= session_timeout:
            request_timeout = max(min_request_timeout, 15000)

        client_overrides["request_timeout_ms"] = request_timeout

        ssl_cafile = None
        ssl_certfile = None
        ssl_keyfile = None

        if self.current_connection_config:
            ssl_cafile = self.current_connection_config.get("ssl_cafile") or None
            ssl_certfile = self.current_connection_config.get("ssl_certfile") or None
            ssl_keyfile = self.current_connection_config.get("ssl_keyfile") or None

        return (bootstrap_servers, security_protocol, sasl_mechanism,
                sasl_plain_username, sasl_plain_password,
                ssl_cafile, ssl_certfile, ssl_keyfile,
                client_overrides, consumer_overrides)

    def disconnect_from_kafka(self):
        """断开Kafka连接"""
        if self.kafka_manager:
            self.kafka_manager.close()
            self.kafka_manager = None

        # 更新UI状态
        self.conn_status.setText("未连接")
        self.conn_status.setStyleSheet("color: red; font-weight: bold;")
        self.connect_btn.setEnabled(True)
        self.disconnect_btn.setEnabled(False)
        self.refresh_topics_btn.setEnabled(False)
        self.create_topic_btn.setEnabled(False)
        self.delete_topic_btn.setEnabled(False)
        self.start_monitor_btn.setEnabled(False)

        # 取消正在进行的Topic加载请求
        self.topic_loader_request_id += 1
        self.topic_loader_thread = None
        self.message_topic_combo.blockSignals(False)

        self.topic_list.clear()
        self.topic_detail.clear()
        
        # 停止消息监控
        if self.consumer_thread:
            self.stop_monitoring()
        
        self.statusBar().showMessage("已断开连接")

    def refresh_topics(self, preselect_topic=None):
        """刷新Topic列表"""
        if not self.kafka_manager:
            return
        target_selection = preselect_topic or self.get_selected_topic()
        self.load_topics_async(target_selection)

    def get_selected_topic(self):
        """获取当前选择的Topic"""
        current_item = self.topic_list.currentItem()
        if current_item:
            return current_item.text()
        return None

    def load_topics_async(self, preselect_topic=None):
        if not self.kafka_manager:
            return

        self.topic_loader_request_id += 1
        request_id = self.topic_loader_request_id

        self.refresh_topics_btn.setEnabled(False)
        self.statusBar().showMessage("正在加载Topic列表...")
        self.message_topic_combo.blockSignals(True)

        loader = TopicLoaderThread(self.kafka_manager, request_id, preselect_topic, self)
        loader.topics_loaded.connect(self.on_topics_loaded)
        loader.error_occurred.connect(self.on_topics_load_failed)
        loader.finished.connect(lambda rid=request_id: self.on_topics_load_finished(rid))
        self.topic_loader_thread = loader
        loader.start()

    def on_topics_loaded(self, request_id, topics, preselect_topic):
        if request_id != self.topic_loader_request_id:
            return
        self.update_topics_ui(topics, preselect_topic)
        self.statusBar().showMessage(f"已加载 {len(topics)} 个Topic")

    def on_topics_load_failed(self, request_id, error_msg):
        if request_id != self.topic_loader_request_id:
            return
        QMessageBox.warning(self, "刷新失败", f"无法获取Topic列表: {error_msg}")
        self.topic_detail.clear()
        self.delete_topic_btn.setEnabled(False)
        self.statusBar().showMessage("刷新Topic列表失败")

    def on_topics_load_finished(self, request_id):
        if request_id != self.topic_loader_request_id:
            return
        self.refresh_topics_btn.setEnabled(True)
        self.message_topic_combo.blockSignals(False)
        self.topic_loader_thread = None

    def update_topics_ui(self, topics, preselect_topic=None):
        self.topic_list.clear()
        self.message_topic_combo.clear()

        for topic in topics:
            self.topic_list.addItem(topic)
            self.message_topic_combo.addItem(topic)

        if preselect_topic and preselect_topic in topics:
            self.select_topic_in_list(preselect_topic)
        elif topics:
            self.topic_list.setCurrentRow(0)
        else:
            self.topic_detail.clear()
            self.delete_topic_btn.setEnabled(False)

        self.delete_topic_btn.setEnabled(len(topics) > 0)

    def select_topic_in_list(self, topic_name):
        """在Topic列表和下拉框中选中指定的Topic"""
        if not topic_name:
            return

        items = self.topic_list.findItems(topic_name, Qt.MatchExactly)
        if items:
            item = items[0]
            self.topic_list.setCurrentItem(item)
            self.topic_list.scrollToItem(item)

        index = self.message_topic_combo.findText(topic_name, Qt.MatchExactly)
        if index >= 0:
            self.message_topic_combo.setCurrentIndex(index)

    def on_topic_item_changed(self, current, previous):
        if current:
            self.on_topic_selected(current)
        else:
            self.topic_detail.clear()
            self.delete_topic_btn.setEnabled(False)

    def create_topic(self):
        """创建新的Topic"""
        if not self.kafka_manager:
            QMessageBox.warning(self, "未连接", "请先连接到Kafka服务器")
            return

        dialog = CreateTopicDialog(self)
        if dialog.exec_() != QDialog.Accepted:
            return

        result = dialog.get_result()
        if not result:
            return

        topic_name = result['name']
        try:
            self.kafka_manager.create_topic(
                name=topic_name,
                num_partitions=result['partitions'],
                replication_factor=result['replication_factor'],
                configs=result['configs'] or None
            )
        except TopicAlreadyExistsError:
            QMessageBox.information(self, "提示", f"Topic '{topic_name}' 已存在")
            self.refresh_topics(preselect_topic=topic_name)
        except Exception as e:
            QMessageBox.critical(self, "创建失败", f"创建Topic失败: {str(e)}")
        else:
            QMessageBox.information(self, "成功", f"Topic '{topic_name}' 创建成功")
            self.refresh_topics(preselect_topic=topic_name)

    def auto_connect_to_last(self):
        """自动连接到上次使用的Kafka服务器"""
        try:
            # 获取上次使用的连接配置
            connections = self.settings.get("connections", [])
            if connections:
                # 使用第一个连接配置
                last_connection = connections[0]
                bootstrap_servers = last_connection.get("bootstrap_servers", "localhost:9092")
                
                # 设置到输入框
                self.host_input.setText(bootstrap_servers)
                
                # 自动连接
                self.connect_to_kafka()
        except Exception as e:
            self.logger.error(f"自动连接失败: {str(e)}")

    def start_monitoring(self):
        """开始监控消息"""
        if not self.kafka_manager:
            QMessageBox.warning(self, "未连接", "请先连接到Kafka服务器")
            return
        
        # 获取选择的Topic
        topic = self.message_topic_combo.currentText().strip()
        if not topic:
            QMessageBox.warning(self, "Topic为空", "请选择或输入Topic名称")
            return
        
        # 如果已经在监控，先停止
        if self.consumer_thread and self.consumer_thread.isRunning():
            self.stop_monitoring()
        
        try:
            # 清空消息队列和列表
            self.message_queue.clear()
            self.message_list.clear()
            self.message_detail.clear()
            self.message_count = 0
            self.message_count_label.setText("消息数: 0")
            
            # 获取消费位置
            offset_text = self.offset_combo.currentText()
            auto_offset_reset = 'earliest' if 'earliest' in offset_text else 'latest'
            
            # 创建消费线程
            self.consumer_thread = MessageConsumerThread(
                self.kafka_manager,
                topic,
                auto_offset_reset=auto_offset_reset
            )
            
            # 连接信号
            self.consumer_thread.message_received.connect(self.on_message_received)
            self.consumer_thread.error_occurred.connect(self.on_consumer_error)
            
            # 启动线程
            self.consumer_thread.start()
            
            # 更新UI状态
            self.start_monitor_btn.setEnabled(False)
            self.stop_monitor_btn.setEnabled(True)
            self.message_topic_combo.setEnabled(False)
            self.offset_combo.setEnabled(False)
            
            self.statusBar().showMessage(f"开始监控Topic: {topic}")
            
        except Exception as e:
            QMessageBox.critical(self, "启动监控失败", f"无法启动消息监控: {str(e)}")
    
    def stop_monitoring(self):
        """停止监控消息"""
        # 停止定时器
        if self.update_timer.isActive():
            self.update_timer.stop()
        
        # 处理剩余的消息
        if self.message_queue:
            self.batch_update_messages()
        
        if self.consumer_thread:
            self.consumer_thread.stop()
            self.consumer_thread.wait(3000)  # 等待最多3秒
            self.consumer_thread = None
        
        # 更新UI状态
        self.start_monitor_btn.setEnabled(True)
        self.stop_monitor_btn.setEnabled(False)
        self.message_topic_combo.setEnabled(True)
        self.offset_combo.setEnabled(True)
        
        self.statusBar().showMessage("已停止监控")
    
    def on_message_received(self, msg_data):
        """处理接收到的消息 - 添加到队列，由定时器批量处理"""
        try:
            # 添加到消息队列
            self.message_queue.append(msg_data)
            
            # 如果队列过长，启动定时器批量处理
            if not self.update_timer.isActive():
                self.update_timer.start()
            
        except Exception as e:
            self.logger.error(f"处理消息失败: {str(e)}")
    
    def batch_update_messages(self):
        """批量更新消息列表，避免频繁更新UI导致卡顿"""
        if not self.message_queue:
            self.update_timer.stop()
            return
        
        try:
            # 批量处理消息（最多一次处理50条）
            batch_size = min(50, len(self.message_queue))
            messages_to_add = self.message_queue[:batch_size]
            self.message_queue = self.message_queue[batch_size:]
            
            # 禁用自动滚动，提高性能
            was_scrolled_to_bottom = False
            scrollbar = self.message_list.verticalScrollBar()
            if scrollbar.value() >= scrollbar.maximum() - 10:
                was_scrolled_to_bottom = True
            
            # 批量添加消息
            for msg_data in messages_to_add:
                # 更新消息计数
                self.message_count += 1
                
                # 添加到消息列表
                display_text = f"[{msg_data['timestamp']}] Partition:{msg_data['partition']} Offset:{msg_data['offset']}"
                if msg_data['key']:
                    display_text += f" Key:{msg_data['key']}"
                
                # 将消息数据存储到item中
                from PyQt5.QtWidgets import QListWidgetItem
                item = QListWidgetItem(display_text)
                item.setData(Qt.UserRole, msg_data)  # 存储完整消息数据
                self.message_list.addItem(item)
                
                # 限制消息数量，删除最旧的消息
                if self.message_list.count() > self.max_message_count:
                    self.message_list.takeItem(0)
            
            # 更新消息计数标签
            self.message_count_label.setText(f"消息数: {self.message_count}")
            
            # 如果之前在底部，自动滚动到底部
            if was_scrolled_to_bottom:
                self.message_list.scrollToBottom()
                # 显示最新消息的详情
                if self.message_list.count() > 0:
                    self.message_list.setCurrentRow(self.message_list.count() - 1)
                    last_item = self.message_list.item(self.message_list.count() - 1)
                    if last_item:
                        self.on_message_selected(last_item)
            
            # 如果队列还有消息，继续处理
            if not self.message_queue:
                self.update_timer.stop()
                
        except Exception as e:
            self.logger.error(f"批量更新消息失败: {str(e)}")
            self.update_timer.stop()
    
    def on_message_selected(self, item):
        """当选择消息时显示详情"""
        try:
            # 如果item是QListWidgetItem，直接使用；如果是index，获取item
            if isinstance(item, int):
                item = self.message_list.item(item)
            
            if not item:
                return
                
            msg_data = item.data(Qt.UserRole)
            if not msg_data:
                return
            
            # 构建详情显示内容
            detail_text = f"时间戳: {msg_data['timestamp']}\n"
            detail_text += f"Topic: {msg_data['topic']}\n"
            detail_text += f"分区: {msg_data['partition']}\n"
            detail_text += f"偏移量: {msg_data['offset']}\n"
            
            if msg_data['key']:
                detail_text += f"Key: {msg_data['key']}\n"
            
            if msg_data.get('headers'):
                detail_text += f"Headers: {msg_data['headers']}\n"
            
            if msg_data.get('value_type'):
                detail_text += f"值类型: {msg_data['value_type']}\n"

            detail_text += "\n" + "="*50 + "\n"
            detail_text += "消息内容:\n"
            detail_text += "="*50 + "\n\n"
            detail_text += msg_data.get('formatted_json', msg_data.get('value_str', ''))
            
            self.message_detail.setText(detail_text)
            
        except Exception as e:
            self.logger.error(f"显示消息详情失败: {str(e)}")
    
    def on_consumer_error(self, error_msg):
        """处理消费者错误"""
        QMessageBox.warning(self, "监控错误", error_msg)
        self.stop_monitoring()
    
    def clear_messages(self):
        """清空消息"""
        self.message_queue.clear()
        self.message_list.clear()
        self.message_detail.clear()
        self.message_count = 0
        self.message_count_label.setText("消息数: 0")
        self.reset_message_search_position()

    def search_message_detail(self):
        """在消息详情中搜索关键字"""
        keyword = self.message_search_input.text()
        if not keyword:
            self.reset_message_search_position()
            return

        document = self.message_detail.document()
        cursor = self.message_detail.textCursor()
        start_pos = cursor.selectionEnd() if cursor.hasSelection() else cursor.position()
        found_cursor = document.find(keyword, start_pos)

        if found_cursor.isNull():
            found_cursor = document.find(keyword, 0)

        if found_cursor.isNull():
            QMessageBox.information(self, "搜索结果", "未找到匹配的内容")
            return

        self.message_detail.setTextCursor(found_cursor)
        self.message_detail.ensureCursorVisible()

    def reset_message_search_position(self):
        """重置消息详情的光标位置"""
        cursor = self.message_detail.textCursor()
        cursor.clearSelection()
        cursor.movePosition(QTextCursor.Start)
        self.message_detail.setTextCursor(cursor)
        self.message_detail.ensureCursorVisible()