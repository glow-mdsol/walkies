import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

from psycopg import connect as pg_connect
from psycopg.rows import dict_row


class SQLiteSession:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def execute(self, sql: str, params: tuple | list = ()):
        return self._conn.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Iterable[tuple | list]):
        return self._conn.executemany(sql, seq_of_params)

    def executescript(self, script: str):
        return self._conn.executescript(script)


class PostgresSession:
    def __init__(self, conn):
        self._conn = conn

    @staticmethod
    def _adapt_placeholders(sql: str) -> str:
        # Keep SQL text readable in callers by allowing sqlite-style placeholders.
        return sql.replace('?', '%s')

    def execute(self, sql: str, params: tuple | list = ()):
        return self._conn.execute(self._adapt_placeholders(sql), params)

    def executemany(self, sql: str, seq_of_params: Iterable[tuple | list]):
        return self._conn.executemany(self._adapt_placeholders(sql), seq_of_params)

    def executescript(self, script: str):
        for statement in script.split(';'):
            stmt = statement.strip()
            if stmt:
                self._conn.execute(stmt)


class SQLiteAdapter:
    kind = 'sqlite'

    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield SQLiteSession(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


class PostgresAdapter:
    kind = 'postgres'

    def __init__(self, dsn: str):
        self.dsn = dsn

    @contextmanager
    def connect(self):
        conn = pg_connect(self.dsn, row_factory=dict_row)
        try:
            yield PostgresSession(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def build_analytics_adapter(default_sqlite_path: Path):
    db_url = os.getenv('ANALYTICS_DB_URL', '').strip()
    if not db_url:
        return SQLiteAdapter(str(default_sqlite_path))

    if db_url.startswith('postgres://') or db_url.startswith('postgresql://'):
        return PostgresAdapter(db_url)

    if db_url.startswith('sqlite:///'):
        sqlite_path = db_url[len('sqlite:///'):]
        if os.name == 'nt' and re.match(r'^/[a-zA-Z]:', sqlite_path):
            sqlite_path = sqlite_path[1:]
        return SQLiteAdapter(sqlite_path)

    # Fallback: treat as a direct sqlite file path.
    return SQLiteAdapter(db_url)
