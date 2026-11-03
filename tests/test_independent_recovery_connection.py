from types import SimpleNamespace
from unittest.mock import Mock

from workers import coordinated_recovery as recovery


def test_new_connection_preserves_identity_and_uses_unmasked_password(monkeypatch):
    connect = Mock(return_value=object())
    monkeypatch.setattr(recovery.psycopg2, "connect", connect)
    parameters = {
        "host": "db.internal",
        "dbname": "example",
        "user": "worker",
        "options": "-c search_path=isolated,public",
        "sslmode": "verify-full",
    }
    db = SimpleNamespace(
        dsn="password=xxx",
        info=SimpleNamespace(dsn_parameters=parameters, password="fixture-secret"),
    )
    assert recovery.independent_connection(db) is connect.return_value
    connect.assert_called_once_with(
        **parameters, connect_timeout=5, password="fixture-secret"
    )
    assert "password" not in parameters


def test_certificate_connection_does_not_invent_password(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(recovery.psycopg2, "connect", connect)
    db = SimpleNamespace(
        info=SimpleNamespace(dsn_parameters={"host": "db.internal"}, password=None)
    )
    recovery.independent_connection(db)
    connect.assert_called_once_with(host="db.internal", connect_timeout=5)
