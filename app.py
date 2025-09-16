from flask import Flask, render_template,session, request, redirect, url_for, flash
from pathlib import Path
import json
from cryptography.fernet import Fernet
import os
import mysql.connector
from dotenv import load_dotenv
import pyodbc,pymysql
from db import get_conn,get_db_connection
from utils_main import getAdvert, getSolution
# --- Basic App Setup ---
BASE_DIR = Path(__file__).parent
app = Flask(__name__,
            template_folder=BASE_DIR / "templates",
            static_folder=BASE_DIR / "static")
app.secret_key = "SECRET_KEY"
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

@app.route("/", methods=["GET", "POST"])
def login():
    ad_content = getAdvert()  # Assuming this function is defined elsewhere
    if request.method == "POST":
        username = request.form["username"].strip()
        password = request.form["password"].strip()

        dg_conn = get_db_connection()
        if dg_conn is None:
            flash("Database connection failed. Please try again later.", "danger")
            return redirect(url_for("login"))

        try:
            with dg_conn.cursor() as cursor:
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
                    return redirect(url_for("index" if user["company_type"] == "Billing" else "dashboard"))
                else:
                    flash("Invalid username or password.", "danger")
                    return redirect(url_for("login"))

        except pymysql.Error as err:
            flash(f"Database error: {err}", "danger")
            return redirect(url_for("login"))
        finally:
            dg_conn.close()

    return render_template("login.html", ad_content=ad_content)



@app.route('/index')
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

    return render_template('client_home_base.html',
                           db_config=engine_config,localdb=localdb,
                           license_data=license_data,
                           connection_status=status_info)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))
# --- Run the Flask App ---

if __name__ == "__main__":
    # Start the Flask development server
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)