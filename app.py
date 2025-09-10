from flask import Flask, render_template
from pathlib import Path

app = Flask(__name__,
            template_folder=Path(__file__).parent / "templates",
            static_folder=Path(__file__).parent / "static")

@app.route('/')
def index():
    return render_template('index.html')
