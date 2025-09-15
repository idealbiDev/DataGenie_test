import sys
import os
import time
import threading
import logging
import json
from pathlib import Path
from PySide6.QtWidgets import QApplication, QMainWindow, QSplashScreen, QWidget
from PySide6.QtCore import QUrl, QTimer, Qt
from PySide6.QtGui import QPainter, QLinearGradient, QColor, QFont, QPixmap
from PySide6.QtWebEngineWidgets import QWebEngineView
from cryptography.fernet import Fernet, InvalidToken

ENCRYPTION_KEY = b'H_2mTjFv5nWr7fGJQyGH72wOSuM9FyPQPoPv0rECptQ='
cipher = Fernet(ENCRYPTION_KEY)
VERSION = "1.0.0"
BUILD_NUMBER = os.environ.get('GITHUB_RUN_NUMBER', 'dev')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('desktop.log'), logging.StreamHandler()])

def resource_path(relative_path):
    """Get absolute path to resource"""
    exe_dir = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__)
    local_path = os.path.join(exe_dir, relative_path)
    if os.path.exists(local_path):
        return local_path
   
    base_path = getattr(sys, '_MEIPASS', os.path.abspath("."))
    full_path = os.path.join(base_path, relative_path)
    if os.path.exists(full_path):
        return full_path

    logging.error(f"Resource not found: {relative_path}")
    return None

def read_license():
    """Read license file without validation"""
    try:
        license_path = resource_path("license.dat")
        if license_path and os.path.exists(license_path):
            with open(license_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            license_data = json.loads(decrypted_data.decode())
            logging.info(f"License data: {license_data}")
            return True
        return False
    except Exception as e:
        logging.error(f"Error reading license: {str(e)}")
        return False

def read_engine_db():
    """Read engine_db file without validation"""
    try:
        license_path = resource_path("license.dat")
        if license_path:
            license_dir = os.path.dirname(license_path)
            engine_db_path = os.path.join(license_dir, "engine_db")
            
            if os.path.exists(engine_db_path):
                with open(engine_db_path, 'rb') as f:
                    encrypted_data = f.read()
                decrypted_data = cipher.decrypt(encrypted_data)
                engine_config = json.loads(decrypted_data.decode())
                logging.info(f"Engine DB config: {engine_config}")
                return True
        return False
    except Exception as e:
        logging.error(f"Error reading engine_db: {str(e)}")
        return False

class SplashWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.setFixedSize(400, 300)

    def paintEvent(self, event):
        painter = QPainter(self)
        gradient = QLinearGradient(0, 0, 0, self.height())
        gradient.setColorAt(0, QColor(0, 102, 204, 204))
        gradient.setColorAt(1, QColor(0, 102, 204, 51))
        painter.fillRect(self.rect(), gradient)

        painter.setFont(QFont("Arial", 24, QFont.Bold))
        data_text = "Data"
        genie_text = "Genie"
        data_width = painter.fontMetrics().horizontalAdvance(data_text)
        genie_width = painter.fontMetrics().horizontalAdvance(genie_text)
        total_width = data_width + genie_width
        x = (self.width() - total_width) // 2
        y = self.height() // 2

        painter.setPen(QColor("#C0C0C0"))
        painter.drawText(x, y, data_text)
        painter.setPen(QColor("#FFA500"))
        painter.drawText(x + data_width, y, genie_text)

        painter.setFont(QFont("Arial", 8))
        painter.setPen(QColor("#333333"))
        version_text = f"Version {VERSION} (Build {BUILD_NUMBER})"
        painter.drawText(self.width() - painter.fontMetrics().horizontalAdvance(version_text) - 10, 
                         self.height() - 10, version_text)

    def to_pixmap(self):
        pixmap = QPixmap(self.size())
        self.render(pixmap)
        return pixmap

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("IdealBI-DataGenie")
        browser = QWebEngineView()
        browser.setUrl(QUrl("http://127.0.0.1:5000/"))
        self.setCentralWidget(browser)
        self.showMaximized()

def start_flask_thread():
    """Start Flask server in a thread"""
    logging.info("Starting Flask in thread")
    try:
        from app import app
        app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        logging.error(f"Failed to start Flask: {str(e)}")
        try:
            from flask import Flask
            fallback_app = Flask(__name__)
            
            @fallback_app.route('/')
            def fallback_index():
                return "DataGenie is running"
                
            @fallback_app.route('/health')
            def fallback_health():
                return {'status': 'ok', 'message': 'Flask app is running'}
                
            fallback_app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False, threaded=True)
        except Exception as e2:
            logging.error(f"Failed to start fallback Flask: {str(e2)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Read both files without validation
    license_exists = read_license()
    engine_db_exists = read_engine_db()
    
    logging.info(f"License exists: {license_exists}")
    logging.info(f"Engine DB exists: {engine_db_exists}")

    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=start_flask_thread, daemon=True)
    flask_thread.start()

    # Wait for Flask to start
    import requests
    timeout = 30
    start_time = time.time()
    flask_started = False
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get("http://127.0.0.1:5000/health", timeout=1)
            if response.status_code == 200:
                logging.info("Flask server is ready")
                flask_started = True
                break
        except:
            time.sleep(0.5)
    
    if not flask_started:
        logging.error("Flask server failed to start")
        # Continue anyway - the browser might handle connection errors

    # Create and show splash screen
    splash_widget = SplashWidget()
    splash_pixmap = splash_widget.to_pixmap()
    splash = QSplashScreen(splash_pixmap, Qt.WindowType.WindowStaysOnTopHint)
    splash.show()

    # Process events to make sure the splash screen is displayed
    QApplication.processEvents()

    # Transition to main window after 2 seconds
    main_window = MainWindow()
    QTimer.singleShot(2000, lambda: [main_window.show(), splash.finish(main_window)])

    sys.exit(app.exec())