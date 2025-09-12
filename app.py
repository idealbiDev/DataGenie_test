
from flask import Flask, render_template, request, redirect, url_for
from pathlib import Path

# App setup
BASE_DIR = Path(__file__).parent
app = Flask(__name__,
            template_folder=BASE_DIR / "templates",
            static_folder=BASE_DIR / "static")

ENGINE_DB_PATH = BASE_DIR / "engine_db"  # file can have no extension

def engine_db_exists():
    """Check if engine_db file exists in the app folder."""
    return ENGINE_DB_PATH.exists()

@app.route('/')
def index():
    if engine_db_exists():
        return render_template('index.html')
    else:
        return render_template('engineconfig.html')

@app.route('/create_engine_db', methods=['POST'])
def create_engine_db():
    """
    Handle form submission from page2.html and create engine_db file.
    """
    try:
        # You can capture form data if needed
        user_input = request.form.get("username", "")
        
        # Create the file (blank or write user input)
        with open(ENGINE_DB_PATH, 'w') as f:
            f.write(f"Created by: {user_input}\n")
        
        print(f"[INFO] engine_db created at: {ENGINE_DB_PATH}")
    except Exception as e:
        print(f"[ERROR] Failed to create engine_db: {e}")

    # Redirect back to index, which should now show page1.html
    return redirect(url_for('index'))

if __name__ == "__main__":
    app.run(debug=True)
