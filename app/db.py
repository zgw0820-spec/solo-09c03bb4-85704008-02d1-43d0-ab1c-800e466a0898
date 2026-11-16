from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


NODES_TABLE = """
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    cpu_capacity INTEGER NOT NULL CHECK (cpu_capacity > 0),
    status TEXT NOT NULL DEFAULT 'ready'
        CHECK (status IN ('ready', 'draining', 'drained')),
    position INTEGER NOT NULL DEFAULT 0
);
"""

SERVICES_TABLE = """
CREATE TABLE IF NOT EXISTS services (
    name TEXT PRIMARY KEY,
    min_ready INTEGER NOT NULL CHECK (min_ready >= 0),
    position INTEGER NOT NULL DEFAULT 0
);
"""

CONTAINERS_TABLE = """
CREATE TABLE IF NOT EXISTS containers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    service TEXT NOT NULL REFERENCES services(name),
    cpu_request INTEGER NOT NULL CHECK (cpu_request > 0),
    node_id TEXT NOT NULL REFERENCES nodes(id),
    status TEXT NOT NULL DEFAULT 'serving'
        CHECK (status IN ('serving')),
    position INTEGER NOT NULL DEFAULT 0
);
"""

DRAINS_TABLE = """
CREATE TABLE IF NOT EXISTS drains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL REFERENCES nodes(id),
    status TEXT NOT NULL
        CHECK (status IN ('active', 'incomplete', 'completed', 'cancelled')),
    created_at TEXT NOT NULL,
    finished_at TEXT
);
"""

# One row per attempted replica move. Retries append a new row; the latest
# row per (drain, container) is the container's current attempt. 'cancelled'
# rows are replicas whose reservation was released by an aborted drain.
MIGRATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    drain_id INTEGER NOT NULL REFERENCES drains(id) ON DELETE CASCADE,
    container_id TEXT NOT NULL REFERENCES containers(id),
    source_node_id TEXT NOT NULL REFERENCES nodes(id),
    target_node_id TEXT REFERENCES nodes(id),
    state TEXT NOT NULL
        CHECK (state IN ('starting', 'completed', 'failed', 'blocked', 'cancelled')),
    reserved_cpu INTEGER NOT NULL CHECK (reserved_cpu > 0),
    error_code TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_containers_node ON containers(node_id);
CREATE INDEX IF NOT EXISTS idx_migrations_drain ON migrations(drain_id);
CREATE INDEX IF NOT EXISTS idx_migrations_container ON migrations(container_id);
CREATE INDEX IF NOT EXISTS idx_migrations_target ON migrations(target_node_id);

-- A node may only have one drain that has not completed or been cancelled.
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_open_drain_per_node
    ON drains(node_id) WHERE status IN ('active', 'incomplete');

-- A logical container can only have one new replica starting at a time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_starting_migration_per_container
    ON migrations(container_id) WHERE state = 'starting';
"""

SCHEMA = "\n".join(
    [
        NODES_TABLE,
        SERVICES_TABLE,
        CONTAINERS_TABLE,
        DRAINS_TABLE,
        MIGRATIONS_TABLE,
        INDEXES,
    ]
)


class Database:
    """Small SQLite helper that opens short-lived, transaction-scoped connections."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def initialize(self) -> None:
        if self.path != ":memory:":
            parent = Path(self.path).expanduser().resolve().parent
            parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            if self.path != ":memory:":
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA synchronous = NORMAL")
            self._migrate(conn)
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """Rebuild drains/migrations when their CHECK constraints predate 'cancelled'.

        SQLite cannot alter a CHECK constraint in place, so existing databases
        get their two history tables rebuilt under one write lock. Rows are
        copied verbatim; indexes are recreated by ``initialize``'s schema pass
        (dropping a table also drops the indexes that moved with the rename).
        """
        definitions: dict[str, str] = {}
        for table in ("drains", "migrations"):
            row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table,),
            ).fetchone()
            if row is None:
                # Fresh (or partially initialised) database: the schema pass
                # below creates whatever is missing.
                return
            definitions[table] = row["sql"]
        if all("'cancelled'" in sql for sql in definitions.values()):
            return

        conn.execute("PRAGMA foreign_keys = OFF")
        try:
            conn.execute("BEGIN IMMEDIATE")
            # Rename children first so no table briefly references a name
            # that is about to be reused.
            conn.execute("ALTER TABLE migrations RENAME TO migrations_legacy")
            conn.execute("ALTER TABLE drains RENAME TO drains_legacy")
            conn.execute(DRAINS_TABLE.replace("IF NOT EXISTS ", ""))
            conn.execute(MIGRATIONS_TABLE.replace("IF NOT EXISTS ", ""))
            conn.execute("INSERT INTO drains SELECT * FROM drains_legacy")
            conn.execute("INSERT INTO migrations SELECT * FROM migrations_legacy")
            conn.execute("DROP TABLE migrations_legacy")
            conn.execute("DROP TABLE drains_legacy")
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.execute("PRAGMA foreign_keys = ON")

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        """Read several tables from one consistent snapshot."""
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        """Serialise mutating operations with an immediate write lock."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
