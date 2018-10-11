import pytest
from unittest.mock import patch


# This method will be used by the mock to replace requests.get
def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0].endswith("/dataset"):
        return MockResponse(['test_dataset'], 200)
    elif args[0].endswith("/dataset/test_dataset"):
        return MockResponse({"name": "test_dataset", "CV_SEGMENTATION_PATH": "/test_data"}, 200)

    return MockResponse(None, 404)
