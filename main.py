# -*- coding: utf-8 -*-
import sys
import os
import logging
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt

from config.settings import settings
from ui.main_window import MainWindow

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def main():
    # 读取配置
    window_width = settings.get("ui.window_width")
    bootstrap_servers = settings.get("kafka.bootstrap_servers")

    # 应用配置到UI
    app = QApplication(sys.argv)
    window = MainWindow()
    window.resize(window_width, settings.get("ui.window_height"))

    # 自动连接（如果配置了）
    if settings.get("ui.auto_connect"):
        window.auto_connect_to_last()

    # 显示窗口
    window.show()

    # 运行应用
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()