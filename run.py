# Run a test server.
import sys
from annotationengine import create_app, create_app_seg

if __name__ == "__main__":

    if len(sys.argv) == 3:
        if sys.argv[2] == "--seg":
            app = create_app_seg()
        else:
            raise Exception("Unknow parameter %s" % sys.argv[2])
    elif len(sys.argv) == 2:
        app = create_app()
    else:
        raise Exception("Wrong number of parameters")

    app.run(host='0.0.0.0', port=4001, debug=True)
