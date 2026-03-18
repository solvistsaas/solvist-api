import os
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import psycopg2


def _ensure_sslmode(database_url: str) -> str:
    parsed = urlparse(database_url)
    query = dict(parse_qsl(parsed.query))
    if query.get("sslmode") != "require":
        query["sslmode"] = "require"
        parsed = parsed._replace(query=urlencode(query))
    return urlunparse(parsed)


def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(_ensure_sslmode(database_url))
