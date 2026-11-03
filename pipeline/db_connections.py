"""Fresh same-target libpq connection; secrets remain in memory and out of DSN logs."""

import psycopg2


def independent_connection(db):
    parameters = dict(db.info.dsn_parameters)
    parameters["connect_timeout"] = 5
    if db.info.password:
        parameters["password"] = db.info.password
    return psycopg2.connect(**parameters)
