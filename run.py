# Run a test server.
import sys
from werkzeug.serving import WSGIRequestHandler
from annotationengine import create_app
import os

HOME = os.path.expanduser("~")
app = create_app()

if __name__ == "__main__":

    WSGIRequestHandler.protocol_version = "HTTP/1.1"

    app.run(host='0.0.0.0',
            port=4001,
            debug=True,
            threaded=True,
            ssl_context=(HOME + '/keys/server.crt',
                         HOME + '/keys/server.key'))
