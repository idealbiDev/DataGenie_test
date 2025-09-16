from flask import Flask, render_template
from pathlib import Path
import json
from cryptography.fernet import Fernet
import os
import mysql.connector
from dotenv import load_dotenv

# --- Basic App Setup ---
BASE_DIR = Path(__file__).parent
app = Flask(__name__,
            template_folder=BASE_DIR / "templates",
            static_folder=BASE_DIR / "static")

# --- Encryption Key (Required to read the files) ---
ENCRYPTION_KEY = b'H_2mTjFv5nWr7fGJQyGH72wOSuM9FyPQPoPv0rECptQ='
cipher = Fernet(ENCRYPTION_KEY)


# --- Core Functions to Read Files ---

def read_engine_db():
    """Reads and decrypts the engine_db file."""
    try:
        engine_db_path = BASE_DIR / "engine_db"
        if engine_db_path.exists():
            with open(engine_db_path, 'rb') as f:
                encrypted_data = f.read()
            
            decrypted_data = cipher.decrypt(encrypted_data)
            db_config = json.loads(decrypted_data.decode())
            print("✅ Successfully read and decrypted engine_db.")
            return db_config
    except Exception as e:
        print(f"⚠️  Error reading engine_db file: {e}")
    
    # Return None if the file doesn't exist or an error occurs
    return None

def read_license():
    """Reads and decrypts the license file (used by the original index.html)."""
    try:
        license_path = BASE_DIR / "license.dat"
        if license_path.exists():
            with open(license_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode())
    except Exception as e:
        print(f"⚠️  Error reading license file: {e}")
    return None

def local_db_connection(db_config_local):
    """Establishes a connection to the local database using the provided config."""
    try:
        conn = mysql.connector.connect(**db_config_local)
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        db_version = cursor.fetchone()
        version_local = db_version[0]
       
        return version_local
    except Exception as e:
     
        return None
# --- Main Application Route ---

@app.route('/')
def index():
    """Main page that reads config files and shows index.html."""
    engine_config = read_engine_db()
    license_data = read_license()
    db_config_local ={
        'host':  'localhost',
        'user': 'datagenie',
        'password': 'P@ss1234',
        'database': 'datagenie'
        }
    localdb = local_db_connection(db_config_local)


    # Create a simple status object for the template
    status_info = {
        'connected': engine_config is not None,
        'message': 'Configuration loaded from engine_db.' if engine_config else 'engine_db file not found or is invalid.'
    }

    return render_template('index.html',
                           db_config=engine_config,localdb=localdb,
                           license_data=license_data,
                           connection_status=status_info)


# --- Run the Flask App ---

if __name__ == "__main__":
    # Start the Flask development server
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)