"""Password hashing and database-backed authentication using Python stdlib."""
from __future__ import annotations

import getpass
import hashlib
import hmac
import json
import secrets
import sqlite3

from dineiq.db.permissions import Principal, ROLES
from dineiq.db.schema import connect_database


_SCRYPT_N = 2**14


def hash_password(password: str, *, salt: bytes | None = None) -> str:
    if len(password) < 12 or len(password) > 1024:
        raise ValueError("Password must contain 12 to 1,024 characters.")
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=_SCRYPT_N, r=8, p=1)
    return f"scrypt${_SCRYPT_N}${salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, n_text, salt_hex, expected_hex = encoded.split("$")
        if scheme != "scrypt" or int(n_text) != _SCRYPT_N:
            return False
        actual = hashlib.scrypt(password.encode("utf-8"), salt=bytes.fromhex(salt_hex),
                                n=int(n_text), r=8, p=1).hex()
        return hmac.compare_digest(actual, expected_hex)
    except (ValueError, TypeError):
        return False


def user_count(database_path: str) -> int:
    with connect_database(database_path) as connection:
        return int(connection.execute("SELECT COUNT(*) FROM users").fetchone()[0])


def create_user(database_path: str, username: str, password: str, role: str,
                assigned_location_id: str | None = None, *, actor_role: str | None = None) -> int:
    username = username.strip()
    if not username or len(username) > 80:
        raise ValueError("Username must contain 1 to 80 characters.")
    if role not in ROLES:
        raise ValueError("Unknown user role.")
    with connect_database(database_path) as connection:
        if role == "administrator" and connection.execute("SELECT COUNT(*) FROM users").fetchone()[0] and actor_role != "administrator":
            raise PermissionError("Only an administrator may create another administrator.")
        if actor_role is not None and actor_role != "administrator":
            raise PermissionError("Only an administrator may create user accounts.")
        if role == "manager" and not assigned_location_id:
            raise ValueError("Restaurant managers must have an assigned location.")
        if assigned_location_id and not connection.execute(
            "SELECT 1 FROM locations WHERE Location_ID=?", (assigned_location_id,)
        ).fetchone():
            raise ValueError("Assigned location does not exist.")
        cursor = connection.execute(
            "INSERT INTO users(username,password_hash,role,assigned_location_id) VALUES(?,?,?,?)",
            (username, hash_password(password), role, assigned_location_id),
        )
        connection.execute(
            "INSERT INTO audit_events(actor_user_id,actor_name,action,entity_type,entity_id,outcome,details) VALUES(NULL,?, 'create_user', 'users', ?, 'success', ?)" ,
            (actor_role or "system", str(cursor.lastrowid),
            json.dumps({"username": username, "role": role, "assigned_location_id": assigned_location_id})),
        )
        return int(cursor.lastrowid)


def authenticate(database_path: str, username: str, password: str) -> Principal | None:
    with connect_database(database_path) as connection:
        row = connection.execute(
            "SELECT user_id,username,password_hash,role,assigned_location_id,is_active FROM users WHERE username=? COLLATE NOCASE",
            (username.strip(),),
        ).fetchone()
    if not row or not row["is_active"] or not verify_password(password, row["password_hash"]):
        with connect_database(database_path) as connection:
            connection.execute(
                "INSERT INTO audit_events(actor_user_id,actor_name,action,entity_type,entity_id,outcome,details) VALUES(NULL,?, 'login', 'authentication', NULL, 'failure', '{}')",
                (username.strip()[:80] or "unknown",),
            )
        return None
    principal = Principal(int(row["user_id"]), row["username"], row["role"], row["assigned_location_id"])
    with connect_database(database_path) as connection:
        connection.execute(
            "INSERT INTO audit_events(actor_user_id,actor_name,action,entity_type,entity_id,outcome,details) VALUES(?,?, 'login', 'authentication', ?, 'success', '{}')",
            (principal.user_id, principal.username, str(principal.user_id)),
        )
    return principal


def create_first_admin_interactive(database_path: str) -> int:
    """Safely create the first administrator without echoing its password."""
    with connect_database(database_path) as connection:
        if connection.execute("SELECT COUNT(*) FROM users").fetchone()[0]:
            raise RuntimeError("Users already exist; use an administrator account to add users.")
    username = input("Administrator username: ").strip()
    password = getpass.getpass("Administrator password (12+ characters): ")
    confirmation = getpass.getpass("Confirm password: ")
    if password != confirmation:
        raise ValueError("Passwords do not match.")
    return create_user(database_path, username, password, "administrator")
