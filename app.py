import os
from flask import Flask, send_from_directory
from threading import Thread
from dotenv import load_dotenv

load_dotenv()

from mongodb_generic_mirroring import mirror


def create_app():
    frontend_enabled = os.getenv("ENABLE_FRONTEND", "false").lower() == "true"
    
    if frontend_enabled:
        static_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'frontend', 'build')
        app = Flask(__name__, static_folder=static_folder, static_url_path='')
    else:
        app = Flask(__name__)
    
    if frontend_enabled:
        try:
            from flask_cors import CORS
            CORS(app, resources={r"/api/*": {"origins": "*"}})
        except ImportError:
            print("Warning: flask-cors not installed. CORS support disabled.")
        
        from api import api_bp
        app.register_blueprint(api_bp)
    
    thread_name = Thread(target=mirror, daemon=True).start()
    
    @app.route("/status")
    def status_page():
        import threading
        threads = [t.name for t in threading.enumerate()]
        return {
            "status": "running",
            "message": "The MongoDB Fabric Mirroring Service is running...",
            "threads": threads,
            "frontend_enabled": frontend_enabled
        }
    
    if frontend_enabled:
        @app.route("/")
        def serve_frontend():
            return send_from_directory(app.static_folder, 'index.html')
        
        @app.route("/<path:path>")
        def serve_static(path):
            if path.startswith('api/'):
                return {"error": "Not found"}, 404
            file_path = os.path.join(app.static_folder, path)
            if os.path.exists(file_path):
                return send_from_directory(app.static_folder, path)
            return send_from_directory(app.static_folder, 'index.html')
    else:
        @app.route("/")
        def home_page():
            return "The MongoDB Fabric Mirroring Service is running. Frontend is disabled."
        
    return app


app = create_app()

if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    port = int(os.getenv("FLASK_PORT", "5000"))
    app.run(debug=debug_mode, port=port, host="0.0.0.0")
