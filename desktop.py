import sys
import os
import time
import threading
import logging
import json
from PySide6.QtWidgets import QApplication, QMainWindow, QSplashScreen, QWidget
from PySide6.QtCore import QUrl, QTimer, Qt
from PySide6.QtGui import QPainter, QLinearGradient, QColor, QFont, QPixmap
from PySide6.QtWebEngineWidgets import QWebEngineView
from cryptography.fernet import Fernet
import requests

# --- Configuration and Setup ---
ENCRYPTION_KEY = b'H_2mTjFv5nWr7fGJQyGH72wOSuM9FyPQPoPv0rECptQ='
cipher = Fernet(ENCRYPTION_KEY)
VERSION = "1.0.0"
BUILD_NUMBER = os.environ.get('GITHUB_RUN_NUMBER', 'dev')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('desktop.log'), logging.StreamHandler()])

# --- Helper Functions ---

def resource_path(relative_path):
    """ Get absolute path to a resource, works for development and for PyInstaller. """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def read_license_file():
    """Reads and decrypts the license file, logging its content."""
    try:
        license_path = resource_path("license.dat")
        if os.path.exists(license_path):
            with open(license_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            license_data = json.loads(decrypted_data.decode())
            logging.info(f"License file read successfully: {license_data}")
        else:
            logging.warning("license.dat not found.")
    except Exception as e:
        logging.error(f"Failed to read or decrypt license file: {e}")

def read_engine_db_file():
    """Reads and decrypts the engine_db file, logging its content."""
    try:
        engine_db_path = resource_path("engine_db")
        if os.path.exists(engine_db_path):
            with open(engine_db_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            engine_config = json.loads(decrypted_data.decode())
            logging.info(f"Engine DB file read successfully: {engine_config}")
        else:
            logging.warning("engine_db file not found.")
    except Exception as e:
        logging.error(f"Failed to read or decrypt engine_db file: {e}")


# --- GUI Classes (Defining the App's Appearance) ---

class SplashWidget(QWidget):
    """A custom widget for the splash screen content."""
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
        data_text, genie_text = "Data", "Genie"
        data_width = painter.fontMetrics().horizontalAdvance(data_text)
        total_width = data_width + painter.fontMetrics().horizontalAdvance(genie_text)
        x, y = (self.width() - total_width) // 2, self.height() // 2

        painter.setPen(QColor("#C0C0C0"))
        painter.drawText(x, y, data_text)
        painter.setPen(QColor("#FFA500"))
        painter.drawText(x + data_width, y, genie_text)

        painter.setFont(QFont("Arial", 8))
        painter.setPen(QColor("#333333"))
        version_text = f"Version {VERSION} (Build {BUILD_NUMBER})"
        painter.drawText(self.rect().adjusted(0, 0, -10, -10), Qt.AlignRight | Qt.AlignBottom, version_text)

    def to_pixmap(self):
        pixmap = QPixmap(self.size())
        self.render(pixmap)
        return pixmap

class MainWindow(QMainWindow):
    """The main application window that holds the web browser view."""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("IdealBI-DataGenie")
        browser = QWebEngineView()
        browser.setUrl(QUrl("http://127.0.0.1:5000/"))
        self.setCentralWidget(browser)
        self.showMaximized()

# --- Backend Server ---

def start_flask_thread():
    """Starts the backend Flask web server in a separate thread."""
    logging.info("Attempting to start Flask server...")
    try:
        from app import app
        # Run the Flask app; debug=False is recommended for this setup
        app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)
    except Exception as e:
        logging.critical(f"FATAL: Could not start Flask server. The application cannot run. Error: {e}")

# --- Main Application Execution ---

if __name__ == "__main__":
    app = QApplication(sys.argv)

    # 1. Read the configuration files at startup
    logging.info("Reading configuration files...")
    read_license_file()
    read_engine_db_file()

    # 2. Start the backend Flask server in a daemon thread
    flask_thread = threading.Thread(target=start_flask_thread, daemon=True)
    flask_thread.start()

    # 3. Wait for the server to be responsive before showing the UI
    logging.info("Waiting for backend server to start...")
    flask_started = False
    start_time = time.time()
    while time.time() - start_time < 20:  # 20-second timeout
        try:
            # Check if the root URL is accessible
            response = requests.get("http://127.0.0.1:5000/", timeout=1)
            if response.status_code == 200:
                logging.info("Backend server is ready.")
                flask_started = True
                break
        except requests.ConnectionError:
            time.sleep(0.5)  # Wait and retry if connection is refused

    if not flask_started:
        logging.error("Backend server did not start. The app may not function correctly.")

    # 4. Create and show the splash screen
    splash_widget = SplashWidget()
    splash = QSplashScreen(splash_widget.to_pixmap(), Qt.WindowType.WindowStaysOnTopHint)
    splash.show()
    app.processEvents()  # Ensure splash screen renders immediately

    # 5. After 2 seconds, show the main window and close the splash screen
    main_window = MainWindow()
    QTimer.singleShot(5000, lambda: [splash.finish(main_window), main_window.show()])

    sys.exit(app.exec())