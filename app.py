from flask import Flask, render_template, request, redirect, url_for, jsonify
from pathlib import Path
import json
import os
from cryptography.fernet import Fernet, InvalidToken

# App setup
BASE_DIR = Path(__file__).parent
app = Flask(__name__,
            template_folder=BASE_DIR / "templates",
            static_folder=BASE_DIR / "static")

# Use the same encryption key as the desktop app
ENCRYPTION_KEY = b'H_2mTjFv5nWr7fGJQyGH72wOSuM9FyPQPoPv0rECptQ='
cipher = Fernet(ENCRYPTION_KEY)

# Global variable to store database configuration
db_config = {}

def get_engine_db_config():
    """
    Get the engine_db configuration by reading the encrypted file directly.
    Returns the config dict if successful, None otherwise.
    """
    global db_config
    
    # Check if we already loaded the config
    if db_config:
        return db_config
    
    # Try to read the engine_db file directly
    try:
        engine_db_path = BASE_DIR / "engine_db"
        if engine_db_path.exists():
            with open(engine_db_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            db_config = json.loads(decrypted_data.decode())
            print("Database configuration loaded from file:", db_config)
            return db_config
        else:
            print("Engine DB file does not exist")
            return None
    except InvalidToken:
        print("Error: Invalid encryption key or corrupted engine_db file")
        return None
    except Exception as e:
        print(f"Error reading engine_db file: {e}")
        return None

def test_database_connection(config):
    """
    Test the database connection using the provided configuration.
    This is a placeholder - implement your actual database connection test here.
    """
    try:
        # Placeholder for actual database connection test
        db_type = config.get('db_type', 'MySQL')
        print(f"Testing {db_type} connection to {config.get('host')}:{config.get('port')}")
        
        # Simulate connection test
        # In a real implementation, you would use appropriate database connectors:
        # - MySQL: mysql-connector-python or pymysql
        # - MS SQL: pyodbc or pymssql
        # - Redshift: psycopg2 or sqlalchemy
        
        return True
    except Exception as e:
        print(f"Database connection test failed: {e}")
        return False

@app.route('/')
def index():
    config = get_engine_db_config()
    if config and test_database_connection(config):
        # Pass the database configuration to the template
        return render_template('index.html', db_config=config)
    else:
        # If no valid config, show configuration page
        return render_template('engineconfig.html')

@app.route('/api/check_engine_db')
def check_engine_db_status():
    """
    API endpoint for the desktop app to check engine_db status.
    """
    config = get_engine_db_config()
    if config:
        return jsonify({
            'engine_db_exists': True,
            'engine_db_config': config
        })
    else:
        return jsonify({
            'engine_db_exists': False,
            'engine_db_config': {}
        })

@app.route('/api/test_db_connection')
def test_db_connection_api():
    """
    API endpoint to test the database connection.
    """
    config = get_engine_db_config()
    if not config:
        return jsonify({'status': 'error', 'message': 'No database configuration found'})
    
    success = test_database_connection(config)
    if success:
        return jsonify({'status': 'success', 'message': 'Database connection successful'})
    else:
        return jsonify({'status': 'error', 'message': 'Database connection failed'})

@app.route('/create_engine_db', methods=['POST'])
def create_engine_db():
    """
    Handle form submission from engineconfig.html and create engine_db file.
    This endpoint is kept for backward compatibility but may not be used
    in the desktop app version.
    """
    try:
        # Capture form data
        db_type = request.form.get("db_type", "MySQL")
        host = request.form.get("host", "")
        port = request.form.get("port", "")
        database = request.form.get("database", "")
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        
        # Create config object
        config = {
            'db_type': db_type,
            'host': host,
            'port': int(port) if port else 3306,
            'database': database,
            'username': username,
            'password': password,
            'configured_at': os.path.getmtime(__file__)  # Use current time
        }
        
        # Encrypt and save the configuration
        json_data = json.dumps(config)
        encrypted_data = cipher.encrypt(json_data.encode())
        
        with open(BASE_DIR / "engine_db", 'wb') as f:
            f.write(encrypted_data)
        
        print(f"[INFO] engine_db created at: {BASE_DIR / 'engine_db'}")
        
        # Update global config
        global db_config
        db_config = config
        
        # Redirect back to index, which should now show index.html
        return redirect(url_for('index'))
    except Exception as e:
        print(f"[ERROR] Failed to create engine_db: {e}")
        return redirect(url_for('index'))

@app.route('/health')
def health_check():
    """
    Health check endpoint for the desktop app to verify Flask is running.
    """
    return jsonify({'status': 'ok', 'message': 'Flask app is running'})

# Pre-load the database configuration when the app starts
get_engine_db_config()

if __name__ == "__main__":
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)