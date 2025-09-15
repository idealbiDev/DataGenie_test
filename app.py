from flask import Flask, render_template, jsonify
from pathlib import Path
import json
from cryptography.fernet import Fernet, InvalidToken

# App setup
BASE_DIR = Path(__file__).parent
app = Flask(__name__,
            template_folder=BASE_DIR / "templates",
            static_folder=BASE_DIR / "static")

# Use the same encryption key
ENCRYPTION_KEY = b'H_2mTjFv5nWr7fGJQyGH72wOSuM9FyPQPoPv0rECptQ='
cipher = Fernet(ENCRYPTION_KEY)

def read_engine_db():
    """Read and decrypt engine_db file"""
    try:
        engine_db_path = BASE_DIR / "engine_db"
        if engine_db_path.exists():
            with open(engine_db_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode())
    except Exception as e:
        print(f"Error reading engine_db: {e}")
    return None

def read_license():
    """Read and decrypt license file"""
    try:
        license_path = BASE_DIR / "license.dat"
        if license_path.exists():
            with open(license_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode())
    except Exception as e:
        print(f"Error reading license: {e}")
    return None

@app.route('/')
def index():
    """Main page - always show index.html"""
    engine_config = read_engine_db()
    license_data = read_license()
    
    return render_template('index.html', 
                         db_config=engine_config, 
                         license_data=license_data)

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'message': 'Flask app is running'})

@app.route('/api/check_engine_db')
def check_engine_db_status():
    """API endpoint to check engine_db status"""
    config = read_engine_db()
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

if __name__ == "__main__":
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)