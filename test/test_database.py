from annotationengine.database import get_db


def test_db(client, app):
    with app.app_context():
        db = get_db()
        tables = db.get_tables()
        assert(len(tables) > 0)
