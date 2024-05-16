from flask import Flask
from threading import Thread

from mongodb_generic_mirroring import mirror


def create_app():
    app = Flask(__name__)
    Thread(target=mirror).start()
    
    @app.route("/")
    def home_page():
        return "The MongoDB Fabric Mirroring Service is running..."
    
    return app

app = create_app()
if __name__ == "__main__":
    app.run()