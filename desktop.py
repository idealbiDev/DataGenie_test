import sys
import os
import time
import threading
import logging
import json
from pathlib import Path
from PySide6.QtWidgets import QApplication, QMainWindow, QMessageBox, QSplashScreen, QWidget
from PySide6.QtCore import QUrl, QTimer, Qt
from PySide6.QtGui import QPainter, QLinearGradient, QColor, QFont
from PySide6.QtWebEngineWidgets import QWebEngineView
from cryptography.fernet import Fernet, InvalidToken

ENCRYPTION_KEY = b'your-encryption-key-here'  # Must match website
cipher = Fernet(ENCRYPTION_KEY)
VERSION = "1.0.0"  # Update as needed
BUILD_NUMBER = os.environ.get('GITHUB_RUN_NUMBER', 'dev')  # From GitHub Actions

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

def validate_license():
    try:
        license_path = resource_path("license.dat")
        with open(license_path, 'rb') as f:
            encrypted_data = f.read()
        decrypted_data = cipher.decrypt(encrypted_data)
        license_data = json.loads(decrypted_data.decode())
        logging.debug(f"License data: {license_data}")
        required_keys = ['company_id', 'licence_type', 'app_id']
        if not all(key in license_data for key in required_keys):
            raise ValueError("Invalid license: missing required fields")
        return True
    except (InvalidToken, ValueError, FileNotFoundError) as e:
        logging.error(f"License validation failed: {str(e)}")
        return False

class SplashWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.setFixedSize(400, 300)  # 20% of a typical 1920x1080 screen

    def paintEvent(self, event):
        painter = QPainter(self)
        # Fading blue gradient background
        gradient = QLinearGradient(0, 0, 0, self.height())
        gradient.setColorAt(0, QColor(0, 102, 204, 204))  # rgba(0, 102, 204, 0.8)
        gradient.setColorAt(1, QColor(0, 102, 204, 51))   # rgba(0, 102, 204, 0.2)
        painter.fillRect(self.rect(), gradient)

        # Draw "DataGenie"
        painter.setFont(QFont("Arial", 24, QFont.Bold))
        data_text = "Data"
        genie_text = "Genie"
        data_width = painter.fontMetrics().horizontalAdvance(data_text)
        genie_width = painter.fontMetrics().horizontalAdvance(genie_text)
        total_width = data_width + genie_width
        x = (self.width() - total_width) // 2
        y = self.height() // 2

        painter.setPen(QColor("#C0C0C0"))  # Silver-gray for "Data"
        painter.drawText(x, y, data_text)
        painter.setPen(QColor("#FFA500"))  # Orange for "Genie"
        painter.drawText(x + data_width, y, genie_text)

        # Draw version and build number
        painter.setFont(QFont("Arial", 8))
        painter.setPen(QColor("#333333"))
        version_text = f"Version {VERSION} (Build {BUILD_NUMBER})"
        painter.drawText(self.width() - painter.fontMetrics().horizontalAdvance(version_text) - 10, self.height() - 10, version_text)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("IdealBI-DataGenie")
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

    # Validate license
    if not validate_license():
        QMessageBox.critical(None, "License Error", "Invalid or missing license. Please contact support.")
        sys.exit(1)

    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=start_flask_thread, daemon=True)
    flask_thread.start()

    # Wait for Flask to start
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

    # Show splash screen
    splash_widget = SplashWidget()
    splash = QSplashScreen(splash_widget)
    splash.show()

    # Transition to main window after 3 seconds
    main_window = MainWindow()
    QTimer.singleShot(3000, lambda: [main_window.show(), splash.close()])

    sys.exit(app.exec())