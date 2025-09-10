import sys
import os
import time
import threading
import logging
from pathlib import Path
from PySide6.QtWidgets import QApplication, QMainWindow
from PySide6.QtGui import QIcon
from PySide6.QtCore import QUrl
from PySide6.QtWebEngineWidgets import QWebEngineView

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('desktop.log'), logging.StreamHandler()])

def resource_path(relative_path):
    base_path = getattr(sys, '_MEIPASS', os.path.abspath("."))
    full_path = os.path.join(base_path, relative_path)
    logging.debug(f"Accessing resource: {full_path}")
    if not os.path.exists(full_path):
        logging.error(f"Resource not found: {full_path}")
        raise FileNotFoundError(f"Resource not found: {full_path}")
    return full_path

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DataGenie Test")
        icon_file = "ibilogo.icns" if sys.platform == "darwin" else "ibilogo.ico"
        try:
            icon_path = resource_path(icon_file)
            self.setWindowIcon(QIcon(icon_path))
        except FileNotFoundError:
            logging.warning(f"Icon file {icon_file} not found, proceeding without icon")
        browser = QWebEngineView()
        browser.setUrl(QUrl("http://127.0.0.1:5000"))
        self.setCentralWidget(browser)
        self.showMaximized()

def start_flask_thread():
    logging.debug("Starting Flask in thread")
    from app import app
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False, threaded=True)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    flask_thread = threading.Thread(target=start_flask_thread, daemon=True)
    flask_thread.start()

    import requests
    timeout = 30
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get("http://127.0.0.1:5000", timeout=2)
            if response.status_code == 200:
                logging.debug("Flask server is ready")
                break
        except (requests.ConnectionError, requests.Timeout):
            time.sleep(0.5)
    else:
        logging.error("Flask server failed to start")
        sys.exit(1)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())