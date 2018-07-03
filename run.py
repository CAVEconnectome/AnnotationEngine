# Run a test server.
from annotationengine import app
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=7777, debug=True)
