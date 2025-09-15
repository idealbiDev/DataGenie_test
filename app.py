from flask import Flask, render_template, jsonify, request
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

# Global variables
db_config = {}
connection_string = ""

def read_engine_db():
    """Read and decrypt engine_db file"""
    global db_config, connection_string
    try:
        engine_db_path = BASE_DIR / "engine_db"
        if engine_db_path.exists():
            with open(engine_db_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            db_config = json.loads(decrypted_data.decode())
            
            # Generate connection string based on db_type
            connection_string = generate_connection_string(db_config)
            
            return db_config
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

def generate_connection_string(config):
    """Generate appropriate connection string based on database type"""
    if not config:
        return ""
    
    db_type = config.get('db_type', '').lower()
    host = config.get('host', '')
    port = config.get('port', '')
    database = config.get('database', '')
    username = config.get('username', '')
    password = config.get('password', '')
    
    if db_type == 'mysql':
        return f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}"
    
    elif db_type == 'mssql':
        return f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    
    elif db_type == 'redshift':
        return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    
    else:
        return ""

def save_engine_db(config):
    """Encrypt and save engine_db configuration"""
    global db_config, connection_string
    try:
        engine_db_path = BASE_DIR / "engine_db"
        
        # Add timestamp
        config['configured_at'] = json.loads(json.dumps(config)).get('configured_at', '')
        
        # Encrypt the configuration
        json_data = json.dumps(config)
        encrypted_data = cipher.encrypt(json_data.encode())
        
        # Save to file
        with open(engine_db_path, 'wb') as f:
            f.write(encrypted_data)
        
        # Update global variables
        db_config = config
        connection_string = generate_connection_string(config)
        
        print(f"[INFO] engine_db saved at: {engine_db_path}")
        print(f"[INFO] Connection string: {connection_string}")
        return True
    except Exception as e:
        print(f"Error saving engine_db: {e}")
        return False

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
            'engine_db_config': config,
            'connection_string': connection_string
        })
    else:
        return jsonify({
            'engine_db_exists': False,
            'engine_db_config': {},
            'connection_string': ''
        })

@app.route('/api/save_engine_db', methods=['POST'])
def save_engine_db_api():
    """API endpoint to save engine_db configuration"""
    try:
        config = request.json
        if save_engine_db(config):
            return jsonify({
                'success': True, 
                'message': 'Configuration saved successfully',
                'connection_string': connection_string
            })
        else:
            return jsonify({'success': False, 'message': 'Failed to save configuration'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/get_connection_string')
def get_connection_string():
    """API endpoint to get the current connection string"""
    return jsonify({
        'connection_string': connection_string,
        'db_config': db_config
    })

# Context processor to make connection string available in all templates
@app.context_processor
def inject_connection_info():
    return {
        'connection_string': connection_string,
        'db_config': db_config,
        'has_db_connection': bool(connection_string)
    }



if __name__ == "__main__":
    # Initialize the app before running
    with app.app_context():
        read_engine_db()
    
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)