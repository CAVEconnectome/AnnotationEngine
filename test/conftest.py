import pytest
from annotationengine import create_app


@pytest.fixture
def app():
    app = create_app(
        {
            'TESTING': True,
            'CV_SEGMENTATION_PATH': "file://data/segmentation"
        }
    )
    yield app


@pytest.fixture
def client(app):
    return app.test_client()
