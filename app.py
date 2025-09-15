from flask import Flask, render_template, redirect, url_for, request, session, flash,jsonify
import pyodbc,pymysql
from utils_main import getAdvert, getSolution
from db import get_conn,get_db_connection
from home.client_routes import client_bp
#from sttm.sttm_routes import sttm_bp
#importing blueprint 
#from data_dictionary import data_dictionary_bp
#from data_monitor import monitor_bp
#from harmoniser import harmoniser_bp
#from relationship import relationship_bp
#from flask import Flask, render_template, request, redirect, url_for, jsonify
from pathlib import Path
import json
import os
from cryptography.fernet import Fernet, InvalidToken
from config import create_connection_string

# App setup
BASE_DIR = Path(__file__).parent


app = Flask(__name__,
            template_folder=BASE_DIR / "templates",
            static_folder=BASE_DIR / "static")
app.secret_key = "124578963"
#app.register_blueprint(data_dictionary_bp, url_prefix='/dictionary')
#app.register_blueprint(monitor_bp, url_prefix='/monitor')
app.register_blueprint(client_bp, url_prefix='/home')
#app.register_blueprint(sttm_bp, url_prefix='/sttm')
#app.register_blueprint(harmoniser_bp, url_prefix='/harmoniser')
#app.register_blueprint(relationship_bp, url_prefix='/relationship')
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

@app.route("/", methods=["GET", "POST"])
def login():
    ad_content = getAdvert()  # Assuming this function is defined elsewhere
    if request.method == "POST":
        username = request.form["username"].strip()
        password = request.form["password"].strip()

        conn = get_db_connection()
        if conn is None:
            flash("Database connection failed. Please try again later.", "danger")
            return redirect(url_for("login"))

        try:
            with conn.cursor() as cursor:
                query = """
                    SELECT user_id, username, password_hash, user_email, company_id, company_name, company_type,
                           billing_contact_email, billing_tier_id, tier_name, max_columns, cost_per_column,
                           base_price, products_opted
                    FROM app_users
                    WHERE username = %s
                """
                cursor.execute(query, (username,))
                user = cursor.fetchone()

                if user and user["password_hash"] == password:
                    server_conn=0
                    session["user_id"] = user["user_id"]
                    session["username"] = user["username"]
                    session["user_email"] = user["user_email"]
                    session["company_id"] = user["company_id"]
                    session["company_name"] = user["company_name"]
                    session["tier_name"] = user["tier_name"]
                    session["products_opted"] = user["products_opted"].split(', ') if user["products_opted"] else []
                    session["company_type"] = user["company_type"]
                    session["server_conn"] = server_conn
                    flash(f"Welcome, {user['username']} from {user['company_name']}!", "success")
                    return redirect(url_for("home.clienthome" if user["company_type"] == "Billing" else "dashboard"))
                else:
                    flash("Invalid username or password.", "danger")
                    return redirect(url_for("login"))

        except pymysql.Error as err:
            flash(f"Database error: {err}", "danger")
            return redirect(url_for("login"))
        finally:
            conn.close()

    return render_template("login.html", ad_content=ad_content)

@app.route('/register', methods=['GET', 'POST'])
def register():
    ad_content = getAdvert()
    return render_template('register.html', ad_content=ad_content)
    
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route('/index')
def index():
    config = get_engine_db_config()
    if config and test_database_connection(config):
        # Pass the database configuration to the template
        return render_template('index.html', db_config=config)
    else:
        # If no valid config, show configuration page
        return render_template('engineconfig.html')

@app.route('/testdb')
def testdb():
   config = get_engine_db_config()
   con_config= create_connection_string(config)
   return render_template('testdb.html', db_config=config,con_config=con_config)


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