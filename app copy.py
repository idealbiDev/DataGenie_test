from flask import Flask, render_template, jsonify, request
from pathlib import Path
import json
import os
from cryptography.fernet import Fernet, InvalidToken
import sqlalchemy as db
from sqlalchemy.exc import SQLAlchemyError
import requests

# App setup
BASE_DIR = Path(__file__).parent
app = Flask(__name__,
            template_folder=BASE_DIR / "templates",
            static_folder=BASE_DIR / "static")

# Configuration
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')

# Database Configuration for Data Mapping Tool
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data_mapping.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# AI Service Configuration
app.config['OLLAMA_HOST'] = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
app.config['OLLAMA_MODEL'] = os.getenv('OLLAMA_MODEL', 'llama3')
app.config['COHERE_API_KEY'] = os.getenv('COHERE_API_KEY', '')
app.config['COHERE_MODEL'] = os.getenv('COHERE_MODEL', 'command')
app.config['AI_PREFERRED_PROVIDER'] = os.getenv('AI_PREFERRED_PROVIDER', 'ollama')

# Encryption
ENCRYPTION_KEY = b'H_2mTjFv5nWr7fGJQyGH72wOSuM9FyPQPoPv0rECptQ='
cipher = Fernet(ENCRYPTION_KEY)

# Global variables
db_config = {}
connection_string = ""
db_engine = None
connection_status = {
    'connected': False,
    'message': 'Not configured',
    'test_result': None
}

# Import and register blueprints
try:
    from data_mapping import data_mapping_bp
    from data_mapping.models import db as mapping_db
    
    # Initialize the mapping database
    mapping_db.init_app(app)
    
    # Register blueprints
    app.register_blueprint(data_mapping_bp)
    print("✅ Data Mapping blueprint registered successfully")
except ImportError as e:
    print(f"⚠️ Data Mapping blueprint not available: {e}")

try:
    from data_dictionary import data_dictionary_bp
    app.register_blueprint(data_dictionary_bp)
    print("✅ Data Dictionary blueprint registered successfully")
except ImportError as e:
    print(f"⚠️ Data Dictionary blueprint not available: {e}")

def read_engine_db():
    """Read and decrypt engine_db file"""
    global db_config, connection_string, db_engine, connection_status
    try:
        engine_db_path = BASE_DIR / "engine_db"
        if engine_db_path.exists():
            with open(engine_db_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            db_config = json.loads(decrypted_data.decode())
            
            # Generate connection string based on db_type
            connection_string = generate_connection_string(db_config)
            
            # Store in app config for blueprint access
            app.config['GLOBAL_CONNECTION_STRING'] = connection_string
            app.config['GLOBAL_DB_CONFIG'] = db_config
            
            # Test the connection
            test_database_connection()
            
            return db_config
    except Exception as e:
        print(f"Error reading engine_db: {e}")
        connection_status = {
            'connected': False,
            'message': f'Error reading config: {str(e)}',
            'test_result': None
        }
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
    
    elif db_type == 'postgresql':
        return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    
    else:
        return ""

def test_database_connection():
    """Test the database connection and execute a simple query"""
    global db_engine, connection_status, connection_string
    
    if not connection_string:
        connection_status = {
            'connected': False,
            'message': 'No connection string configured',
            'test_result': None
        }
        return False
    
    try:
        # Create engine and test connection
        db_engine = db.create_engine(connection_string)
        connection = db_engine.connect()
        
        # Execute a simple query based on database type
        db_type = db_config.get('db_type', '').lower()
        
        if db_type == 'mysql':
            result = connection.execute(db.text("SELECT version() as version, NOW() as current_time"))
        elif db_type == 'mssql':
            result = connection.execute(db.text("SELECT @@version as version, GETDATE() as current_time"))
        elif db_type in ['postgresql', 'redshift']:
            result = connection.execute(db.text("SELECT version() as version, CURRENT_DATE as current_time"))
        else:
            # Generic query for unknown databases
            result = connection.execute(db.text("SELECT 1 as test_result"))
        
        test_result = result.fetchone()
        connection.close()
        
        connection_status = {
            'connected': True,
            'message': 'Connection successful',
            'test_result': dict(test_result) if test_result else {'status': 'Connected but no test result'}
        }
        
        print(f"Database connection test successful: {connection_status}")
        return True
        
    except SQLAlchemyError as e:
        error_msg = str(e)
        print(f"Database connection failed: {error_msg}")
        connection_status = {
            'connected': False,
            'message': f'Connection failed: {error_msg}',
            'test_result': None
        }
        return False
    except Exception as e:
        error_msg = str(e)
        print(f"Unexpected error during connection test: {error_msg}")
        connection_status = {
            'connected': False,
            'message': f'Unexpected error: {error_msg}',
            'test_result': None
        }
        return False

def save_engine_db(config):
    """Encrypt and save engine_db configuration"""
    global db_config, connection_string, db_engine, connection_status
    try:
        engine_db_path = BASE_DIR / "engine_db"
        
        # Add timestamp if not present
        if 'configured_at' not in config:
            import time
            config['configured_at'] = time.time()
        
        # Encrypt the configuration
        json_data = json.dumps(config)
        encrypted_data = cipher.encrypt(json_data.encode())
        
        # Save to file
        with open(engine_db_path, 'wb') as f:
            f.write(encrypted_data)
        
        # Update global variables
        db_config = config
        connection_string = generate_connection_string(config)
        
        # Update app config for blueprints
        app.config['GLOBAL_CONNECTION_STRING'] = connection_string
        app.config['GLOBAL_DB_CONFIG'] = db_config
        
        # Test the new connection
        test_database_connection()
        
        print(f"[INFO] engine_db saved at: {engine_db_path}")
        print(f"[INFO] Connection string: {connection_string}")
        return True
    except Exception as e:
        print(f"Error saving engine_db: {e}")
        connection_status = {
            'connected': False,
            'message': f'Save error: {str(e)}',
            'test_result': None
        }
        return False

# Initialize the application
def init_app():
    """Initialize the application by reading the engine_db config"""
    global db_config, connection_string
    read_engine_db()
    print(f"Application initialized. Connection status: {connection_status}")

# Context processor to make connection info available in all templates
@app.context_processor
def inject_connection_info():
    return {
        'connection_string': connection_string,
        'db_config': db_config,
        'has_db_connection': connection_status['connected'],
        'connection_status': connection_status
    }

@app.route('/')
def index():
    """Main page - show index.html with connection status"""
    engine_config = read_engine_db()
    license_data = read_license()
    
    return render_template('index.html', 
                         db_config=engine_config, connection_string=connection_string,
                         license_data=license_data,
                         connection_status=connection_status)

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok', 
        'message': 'Flask app is running',
        'database_connected': connection_status['connected'],
        'blueprints_loaded': {
            'data_mapping': 'data_mapping' in [bp.name for bp in app.blueprints.values()],
            'data_dictionary': 'data_dictionary' in [bp.name for bp in app.blueprints.values()]
        }
    })

@app.route('/api/check_engine_db')
def check_engine_db_status():
    """API endpoint to check engine_db status"""
    config = read_engine_db()
    if config:
        return jsonify({
            'engine_db_exists': True,
            'engine_db_config': config,
            'connection_string': connection_string,
            'connection_status': connection_status
        })
    else:
        return jsonify({
            'engine_db_exists': False,
            'engine_db_config': {},
            'connection_string': '',
            'connection_status': connection_status
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
                'connection_string': connection_string,
                'connection_status': connection_status
            })
        else:
            return jsonify({
                'success': False, 
                'message': 'Failed to save configuration',
                'connection_status': connection_status
            })
    except Exception as e:
        return jsonify({
            'success': False, 
            'message': str(e),
            'connection_status': connection_status
        })

@app.route('/api/get_connection_string')
def get_connection_string():
    """API endpoint to get the current connection string"""
    return jsonify({
        'connection_string': connection_string,
        'db_config': db_config,
        'connection_status': connection_status
    })

@app.route('/api/test_connection')
def test_connection():
    """API endpoint to test the database connection"""
    test_database_connection()
    return jsonify(connection_status)

@app.route('/api/ai/health', methods=['GET'])
def ai_health_check():
    """Check status of AI services"""
    status = {
        'preferred_provider': app.config.get('AI_PREFERRED_PROVIDER', 'ollama'),
        'ollama_available': False,
        'cohere_available': bool(app.config.get('COHERE_API_KEY')),
        'services': []
    }
    
    # Check Ollama
    try:
        response = requests.get(f"{app.config['OLLAMA_HOST']}/api/tags", timeout=10)
        if response.status_code == 200:
            status['ollama_available'] = True
            status['services'].append({
                'name': 'Ollama',
                'status': 'available',
                'models': [model['name'] for model in response.json().get('models', [])]
            })
        else:
            status['services'].append({
                'name': 'Ollama',
                'status': 'unavailable',
                'error': f"HTTP {response.status_code}"
            })
    except Exception as e:
        status['services'].append({
            'name': 'Ollama',
            'status': 'unavailable',
            'error': str(e)
        })
    
    # Check Cohere
    if app.config.get('COHERE_API_KEY'):
        status['services'].append({
            'name': 'Cohere',
            'status': 'configured',
            'model': app.config.get('COHERE_MODEL', 'command')
        })
    else:
        status['services'].append({
            'name': 'Cohere',
            'status': 'not_configured',
            'error': 'API key not set'
        })
    
    return jsonify(status)

# Initialize the app when imported
init_app()

if __name__ == "__main__":
    # Create database tables for mapping tool
    with app.app_context():
        try:
            from data_mapping.models import db as mapping_db
            mapping_db.create_all()
            print("✅ Data Mapping database tables created")
        except Exception as e:
            print(f"⚠️ Failed to create data mapping tables: {e}")
    
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)