from annotationengine.anno_database import get_db
import logging


def test_create_db(app_config, mocker):
    with app_config.app_context():
        db = get_db("test_aligned_volume")
        logging.info(f"db.__dict__: {db.__dict__}")
        assert "_aligned_volume" in db.__dict__
        assert db.__dict__["_aligned_volume"] == "test_aligned_volume"
