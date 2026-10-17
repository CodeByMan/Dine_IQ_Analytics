from __future__ import annotations

import re
import sqlite3
from pathlib import Path


class _ClosingConnection(sqlite3.Connection):
    """Connection context manager that commits/rolls back and always closes."""
    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


# One authority for fresh databases and the legacy migration. Keep the public
# DDL name for existing callers/tests without maintaining a second schema.
SCHEMA_PATH = Path(__file__).resolve().parents[3] / "database" / "schema.sql"
DDL = SCHEMA_PATH.read_text(encoding="utf-8")


def connect_database(path: str | Path) -> sqlite3.Connection:
    """Open a connection with foreign-key enforcement and row mappings."""
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, timeout=30, factory=_ClosingConnection)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _schema_objects(connection: sqlite3.Connection, kind: str) -> dict[str, str]:
    return {
        row["name"]: row["sql"]
        for row in connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE type=? AND sql IS NOT NULL "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name", (kind,)
        )
    }


def _table_definition(sql: str) -> str:
    # ALTER TABLE quotes the table name. Compare the body to avoid remigrating
    # solely because SQLite rewrites that name or whitespace.
    return re.sub(r"\s+", "", sql[sql.index("("):]).casefold()


def register_ordering_channels(connection: sqlite3.Connection, values) -> None:
    """Register observed channel identifiers before enforcing orders' FK.

    Main-dataset channel identifiers are their actual display labels. Existing
    channel names/descriptions win; no alternate orders or labels are invented.
    """
    identifiers = sorted({str(value) for value in values if value is not None})
    connection.executemany(
        "INSERT INTO ordering_channels(Channel_ID,Channel_Name) VALUES(?,?) "
        "ON CONFLICT(Channel_ID) DO NOTHING",
        [(identifier, identifier) for identifier in identifiers],
    )


def initialize_database(path: str | Path) -> None:
    """Match schema.sql, preserving existing rows or rolling back on failure."""
    expected = sqlite3.connect(":memory:")
    expected.row_factory = sqlite3.Row
    try:
        expected.executescript(DDL)
        tables = _schema_objects(expected, "table")
        indexes = _schema_objects(expected, "index")
    finally:
        expected.close()

    with connect_database(path) as connection:
        # SQLite cannot add CHECK/FK clauses through ADD COLUMN. Rebuild only
        # changed tables inside one transaction, with FK enforcement restored
        # after a successful integrity check. No existing row is discarded.
        connection.execute("PRAGMA foreign_keys = OFF")
        try:
            connection.execute("BEGIN IMMEDIATE")
            current_tables = _schema_objects(connection, "table")
            current_indexes = _schema_objects(connection, "index")
            triggers = _schema_objects(connection, "trigger")
            changed = {
                name for name, sql in tables.items()
                if name in current_tables
                and _table_definition(current_tables[name]) != _table_definition(sql)
            }
            for name, sql in tables.items():
                if name not in current_tables:
                    connection.execute(sql)

            # Preserve legacy orders' observed channels before adding their FK.
            if "orders" in current_tables and "orders" in changed:
                register_ordering_channels(
                    connection,
                    (row[0] for row in connection.execute(
                        "SELECT DISTINCT Channel_ID FROM orders WHERE Channel_ID IS NOT NULL"
                    )),
                )

            for name in sorted(changed):
                source_columns = [
                    row["name"] for row in connection.execute(f'PRAGMA table_info("{name}")')
                ]
                temporary_name = "__dineiq_migrate_" + name
                if connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE name=?", (temporary_name,)
                ).fetchone():
                    raise RuntimeError(f"Schema migration name already exists: {temporary_name}")
                # Reuse the canonical table body verbatim, including all checks.
                connection.execute(
                    f'CREATE TABLE "{temporary_name}" ' + tables[name][tables[name].index("("):]
                )
                target_columns = {
                    row["name"] for row in connection.execute(
                        f'PRAGMA table_info("{temporary_name}")'
                    )
                }
                if set(source_columns) - target_columns:
                    raise RuntimeError(
                        f"Schema migration would remove columns from {name}; "
                        "database retained without changes."
                    )
                columns = ",".join('"' + column.replace('"', '""') + '"' for column in source_columns)
                connection.execute(
                    f'INSERT INTO "{temporary_name}" ({columns}) '
                    f'SELECT {columns} FROM "{name}"'
                )
                connection.execute(f'DROP TABLE "{name}"')
                connection.execute(f'ALTER TABLE "{temporary_name}" RENAME TO "{name}"')

            # Existing extra indexes/triggers survive; canonical indexes are
            # recreated as specified, including changed same-name definitions.
            installed_indexes = _schema_objects(connection, "index")
            for name, sql in current_indexes.items():
                if name not in indexes and name not in installed_indexes:
                    connection.execute(sql)
            for name, sql in indexes.items():
                installed = installed_indexes.get(name)
                if installed is None or _table_definition(installed) != _table_definition(sql):
                    connection.execute(f'DROP INDEX IF EXISTS "{name}"')
                    connection.execute(sql)
            for name, sql in triggers.items():
                if not connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?", (name,)
                ).fetchone():
                    connection.execute(sql)

            if changed:
                violation = connection.execute("PRAGMA foreign_key_check").fetchone()
                if violation is not None:
                    raise sqlite3.IntegrityError(
                        f"Canonical schema migration failed foreign-key validation in "
                        f"{violation[0]}; database retained without changes."
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.execute("PRAGMA foreign_keys = ON")
