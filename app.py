from flask import Flask, render_template, request, redirect, url_for, jsonify
from pathlib import Path
import json
import requests
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
    Get the engine_db configuration from the desktop app API.
    Returns the config dict if successful, None otherwise.
    """
    global db_config
    try:
        response = requests.get('http://127.0.0.1:5000/api/check_engine_db', timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('engine_db_exists'):
                db_config = data.get('engine_db_config', {})
                print("Database configuration loaded:", db_config)
                return db_config
        return None
    except requests.exceptions.RequestException as e:
        print("Error connecting to desktop app API:", e)
        return None
    except Exception as e:
        print("Error loading database configuration:", e)
        return None

@app.route('/')
def index():
    config = get_engine_db_config()
    if config:
        return render_template('index.html', db_config=config)
    else:
        return render_template('engineconfig.html')

@app.route('/api/check_engine_db')
def check_engine_db_status():
    """
    API endpoint for the desktop app to check engine_db status.
    """
    try:
        # Try to read the engine_db file directly as a fallback
        engine_db_path = BASE_DIR / "engine_db"
        if engine_db_path.exists():
            with open(engine_db_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            config = json.loads(decrypted_data.decode())
            return jsonify({
                'engine_db_exists': True,
                'engine_db_config': config
            })
        else:
            return jsonify({
                'engine_db_exists': False,
                'engine_db_config': {}
            })
    except Exception as e:
        print(f"Error reading engine_db file: {e}")
        return jsonify({
            'engine_db_exists': False,
            'engine_db_config': {}
        })

@app.route('/health')
def health_check():
    """
    Health check endpoint for the desktop app to verify Flask is running.
    """
    return jsonify({'status': 'ok', 'message': 'Flask app is running'})

if __name__ == "__main__":
    # Pre-load the database configuration when the app starts
    get_engine_db_config()
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)