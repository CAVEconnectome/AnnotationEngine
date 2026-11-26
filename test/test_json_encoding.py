import json
from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pytest
from flask import Flask
from marshmallow import missing

from annotationengine import AEEncoder, AEJSONProvider

# Values the app returns that the standard encoder rejects, and what they become.
EXTRA_TYPES = [
    (np.array([[1, 2], [3, 4]]), [[1, 2], [3, 4]]),
    (np.uint64(2**63 + 1), 2**63 + 1),
    (datetime(2024, 1, 2, 3, 4, 5), "2024-01-02T03:04:05"),
    (date(2024, 1, 2), "2024-01-02"),
    (missing, None),
]
IDS = ["ndarray", "uint64", "datetime", "date", "marshmallow missing"]


class TestAEEncoder:
    """flask-restx serialises API responses with this (RESTX_JSON)."""

    @pytest.mark.parametrize("value, expected", EXTRA_TYPES, ids=IDS)
    def test_encodes_extra_types(self, value, expected):
        assert json.loads(json.dumps({"v": value}, cls=AEEncoder)) == {"v": expected}

    def test_rejects_unknown_types(self):
        with pytest.raises(TypeError):
            json.dumps({"v": object()}, cls=AEEncoder)


class TestAEJSONProvider:
    """flask.jsonify serialises with this."""

    @pytest.fixture()
    def provider(self):
        return AEJSONProvider(Flask(__name__))

    @pytest.mark.parametrize("value, expected", EXTRA_TYPES, ids=IDS)
    def test_encodes_extra_types(self, provider, value, expected):
        assert json.loads(provider.dumps({"v": value})) == {"v": expected}

    def test_keeps_flask_default_types(self, provider):
        # Decimal is handled by Flask's own default, which ours falls back to.
        assert json.loads(provider.dumps({"v": Decimal("1.5")})) == {"v": "1.5"}

    def test_rejects_unknown_types(self, provider):
        with pytest.raises(TypeError):
            provider.dumps({"v": object()})

    def test_app_uses_the_provider(self, client):
        assert isinstance(client.application.json, AEJSONProvider)
