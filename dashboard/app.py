from flask import Flask, send_from_directory
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, 'static')

app = Flask(__name__, static_folder=STATIC_DIR)

# Serve homepage
@app.route('/')
def index():
    return send_from_directory(STATIC_DIR, 'index.html')

# Serve any static asset (CSS, JS, images, etc.)
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory(STATIC_DIR, filename)

# Serve status.json
@app.route('/status.json')
def status():
    return send_from_directory(BASE_DIR, 'status.json')

# Serve FULLDATA, OUTPUT, PDF etc. if needed
@app.route('/<path:path>')
def all_files(path):
    # fallback: serve any file relative to dashboard folder
    root = BASE_DIR
    return send_from_directory(root, path)

if __name__ == '__main__':
    print("🚀 Dashboard running at: http://localhost:8000/")
    app.run(host='0.0.0.0', port=8000, debug=False)
