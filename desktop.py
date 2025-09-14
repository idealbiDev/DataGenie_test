import sys
import os
import time
import threading
import logging
import json
from pathlib import Path

from flask import jsonify
from PySide6.QtWidgets import (QApplication, QMainWindow, QMessageBox, QSplashScreen, 
                               QWidget, QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
                               QLineEdit, QPushButton, QGroupBox, QFormLayout, QComboBox)
from PySide6.QtCore import QUrl, QTimer, Qt, QSettings
from PySide6.QtGui import QPainter, QLinearGradient, QColor, QFont, QPixmap, QIcon
from PySide6.QtWebEngineWidgets import QWebEngineView
from cryptography.fernet import Fernet, InvalidToken

ENCRYPTION_KEY = b'H_2mTjFv5nWr7fGJQyGH72wOSuM9FyPQPoPv0rECptQ='  # Must match website
cipher = Fernet(ENCRYPTION_KEY)
VERSION = "1.0.0"  # Update as needed
BUILD_NUMBER = os.environ.get('GITHUB_RUN_NUMBER', 'dev')  # From GitHub Actions

# Global variable to store engine_db status
ENGINE_DB_EXISTS = False
ENGINE_DB_CONFIG = {}

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('desktop.log'), logging.StreamHandler()])

def resource_path(relative_path):
    """
    Get absolute path to resource, works for dev and PyInstaller builds.
    Tries:
    1. Folder where the EXE/script is located
    2. PyInstaller temp folder (_MEIPASS)
    """
    
    exe_dir = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__)
    local_path = os.path.join(exe_dir, relative_path)
    if os.path.exists(local_path):
        logging.debug(f"Found resource in EXE folder: {local_path}")
        return local_path

   
    base_path = getattr(sys, '_MEIPASS', os.path.abspath("."))
    full_path = os.path.join(base_path, relative_path)
    logging.debug(f"Looking for resource in _MEIPASS: {full_path}")
    if os.path.exists(full_path):
        return full_path

    logging.error(f"Resource not found in either EXE folder or _MEIPASS: {relative_path}")
    raise FileNotFoundError(f"Resource not found: {relative_path}")


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

def check_engine_db():
    """
    Check if engine_db file exists in the same folder as license.dat
    Returns True if exists, False otherwise
    """
    try:
        license_dir = os.path.dirname(resource_path("license.dat"))
        engine_db_path = os.path.join(license_dir, "engine_db")
        
        # Check if file exists
        exists = os.path.exists(engine_db_path)
        logging.debug(f"Engine DB exists: {exists} at path: {engine_db_path}")
        
        # Update global variable
        global ENGINE_DB_EXISTS
        ENGINE_DB_EXISTS = exists
        
        # If exists, read and decrypt the content
        if exists:
            with open(engine_db_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            global ENGINE_DB_CONFIG
            ENGINE_DB_CONFIG = json.loads(decrypted_data.decode())
            logging.debug(f"Engine DB config loaded: {ENGINE_DB_CONFIG}")
        
        return exists
    except Exception as e:
        logging.error(f"Error checking engine_db: {str(e)}")
        return False

def save_engine_db_config(config):
    """
    Save engine DB configuration to encrypted file
    """
    try:
        license_dir = os.path.dirname(resource_path("license.dat"))
        engine_db_path = os.path.join(license_dir, "engine_db")
        
        # Encrypt the configuration
        json_data = json.dumps(config)
        encrypted_data = cipher.encrypt(json_data.encode())
        
        # Save to file
        with open(engine_db_path, 'wb') as f:
            f.write(encrypted_data)
        
        logging.debug(f"Engine DB config saved to: {engine_db_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving engine_db config: {str(e)}")
        return False

class EngineDbDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Database Connection Configuration")
        self.setModal(True)
        self.setFixedSize(500, 450)  # Increased height for the new dropdown
        
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout()
        
        # Add title
        title = QLabel("Database Connection Settings")
        title.setStyleSheet("font-size: 16px; font-weight: bold; margin: 10px;")
        layout.addWidget(title)
        
        # Add description
        description = QLabel("Please provide the connection details for your database engine. This information will be encrypted and stored securely.")
        description.setWordWrap(True)
        description.setStyleSheet("margin: 10px;")
        layout.addWidget(description)
        
        # Form group
        form_group = QGroupBox("Connection Details")
        form_layout = QFormLayout()
        
        # Database type dropdown
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["MySQL", "MS SQL Server", "Redshift"])
        form_layout.addRow("Database Type:", self.db_type_combo)
        
        self.host_input = QLineEdit()
        self.host_input.setPlaceholderText("e.g., localhost or 192.168.1.100")
        form_layout.addRow("Host:", self.host_input)
        
        self.port_input = QLineEdit()
        self.port_input.setPlaceholderText("e.g., 3306 for MySQL, 1433 for MS SQL, 5439 for Redshift")
        form_layout.addRow("Port:", self.port_input)
        
        self.database_input = QLineEdit()
        self.database_input.setPlaceholderText("Database name")
        form_layout.addRow("Database:", self.database_input)
        
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("Database username")
        form_layout.addRow("Username:", self.username_input)
        
        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("Database password")
        self.password_input.setEchoMode(QLineEdit.Password)
        form_layout.addRow("Password:", self.password_input)
        
        # Set default ports based on database type
        self.db_type_combo.currentTextChanged.connect(self.update_default_port)
        
        form_group.setLayout(form_layout)
        layout.addWidget(form_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_button)
        
        self.save_button = QPushButton("Save Configuration")
        self.save_button.clicked.connect(self.save_config)
        self.save_button.setDefault(True)
        self.save_button.setStyleSheet("background-color: #4CAF50; color: white;")
        button_layout.addWidget(self.save_button)
        
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
        
    def update_default_port(self, db_type):
        """Update the port field with default port for selected database type"""
        default_ports = {
            "MySQL": "3306",
            "MS SQL Server": "1433",
            "Redshift": "5439"
        }
        self.port_input.setText(default_ports.get(db_type, ""))
        
    def save_config(self):
        # Validate inputs
        if not all([self.host_input.text(), self.port_input.text(), 
                   self.database_input.text(), self.username_input.text()]):
            QMessageBox.warning(self, "Validation Error", 
                               "Please fill in all required fields.")
            return
        
        try:
            port = int(self.port_input.text())
        except ValueError:
            QMessageBox.warning(self, "Validation Error", 
                               "Port must be a valid number.")
            return
        
        # Create config object
        config = {
            'db_type': self.db_type_combo.currentText(),
            'host': self.host_input.text(),
            'port': port,
            'database': self.database_input.text(),
            'username': self.username_input.text(),
            'password': self.password_input.text(),
            'configured_at': time.time()
        }
        
        # Save config
        if save_engine_db_config(config):
            QMessageBox.information(self, "Success", 
                                   "Database configuration saved successfully. The application will now restart.")
            self.accept()
        else:
            QMessageBox.critical(self, "Error", 
                                "Failed to save database configuration. Please check the logs for details.")

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

    def to_pixmap(self):
        """Convert the widget to a QPixmap"""
        pixmap = QPixmap(self.size())
        self.render(pixmap)
        return pixmap

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("IdealBI-DataGenie")
        browser = QWebEngineView()
        # Load the main index page directly
        browser.setUrl(QUrl("http://127.0.0.1:5000/"))
        self.setCentralWidget(browser)
        self.showMaximized()

def restart_application():
    """Restart the application"""
    python = sys.executable
    os.execl(python, python, *sys.argv)

def start_flask_thread():
    logging.debug("Starting Flask in thread")
    try:
        # Import and start Flask app - don't add routes here
        from app import app
        
        # Start Flask without the reloader to avoid issues
        app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        logging.error(f"Failed to start Flask: {str(e)}")
        # Try to start with a simple Flask app if the main one fails
        try:
            from flask import Flask
            fallback_app = Flask(__name__)
            
            @fallback_app.route('/')
            def fallback_index():
                return "DataGenie is running"
                
            @fallback_app.route('/health')
            def fallback_health():
                return jsonify({'status': 'ok', 'message': 'Flask app is running'})
                
            fallback_app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False, threaded=True)
        except Exception as e2:
            logging.error(f"Failed to start fallback Flask: {str(e2)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Validate license
    if not validate_license():
        QMessageBox.critical(None, "License Error", "Invalid or missing license. Please contact support.")
        sys.exit(1)

    # Check if engine_db exists
    engine_db_exists = check_engine_db()
    logging.info(f"Engine DB check result: {engine_db_exists}")

    # If engine_db doesn't exist, show configuration dialog
    if not engine_db_exists:
        dialog = EngineDbDialog()
        if dialog.exec() == QDialog.Accepted:
            # Configuration saved, restart the application
            logging.info("Restarting application after saving engine_db config")
            QTimer.singleShot(1000, restart_application)
            sys.exit(0)
        else:
            # User cancelled, exit the application
            logging.info("User cancelled configuration, exiting application")
            sys.exit(0)

    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=start_flask_thread, daemon=True)
    flask_thread.start()

    # Wait for Flask to start with more attempts and shorter interval
    import requests
    timeout = 60  # Increased timeout to 60 seconds
    start_time = time.time()
    flask_started = False
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get("http://127.0.0.1:5000/health", timeout=1)
            if response.status_code == 200:
                logging.debug("Flask server is ready")
                flask_started = True
                break
        except (requests.ConnectionError, requests.Timeout):
            time.sleep(0.5)  # Shorter sleep interval
        except Exception as e:
            logging.debug(f"Waiting for Flask: {str(e)}")
            time.sleep(0.5)
    
    if not flask_started:
        logging.error("Flask server failed to start within timeout period")
        # Try one more time with health endpoint
        try:
            response = requests.get("http://127.0.0.1:5000/health", timeout=2)
            if response.status_code == 200:
                logging.debug("Flask health check passed")
                flask_started = True
        except:
            pass
        
        if not flask_started:
            QMessageBox.critical(None, "Server Error", 
                                "Failed to start the application server. Please check the logs for details.")
            sys.exit(1)

    # Create and show splash screen using a pixmap
    splash_widget = SplashWidget()
    splash_pixmap = splash_widget.to_pixmap()
    splash = QSplashScreen(splash_pixmap, Qt.WindowType.WindowStaysOnTopHint)
    splash.show()

    # Process events to make sure the splash screen is displayed
    QApplication.processEvents()

    # Transition to main window after 3 seconds
    main_window = MainWindow()
    QTimer.singleShot(3000, lambda: [main_window.show(), splash.finish(main_window)])

    sys.exit(app.exec())