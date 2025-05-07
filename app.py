from flask import Flask
from threading import Thread

from mongodb_generic_mirroring import mirror


def create_app():
    app = Flask(__name__)
    thread_name=Thread(target=mirror).start()
    
    @app.route("/")
    def home_page():
        import threading
        for thread in threading.enumerate(): 
            print(thread.name)
        return "The MongoDB Fabric Mirroring Service is running..."
        
    return app

app = create_app()
if __name__ == "__main__":
    app.run()