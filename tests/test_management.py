"""management account security, role enforcement, and persistence checks."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from dineiq.db.auth import authenticate, create_user, hash_password, verify_password
from dineiq.db.permissions import Principal, allowed
from dineiq.db.repository import list_records, save_record
from dineiq.db.schema import connect_database, initialize_database


class ManagementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / "dineiq.sqlite3")
        initialize_database(self.db)
        initialize_database(self.db)  # initializer is idempotent
        self.admin = Principal(1, "admin", "administrator")
        self.analyst = Principal(2, "analyst", "analyst")
        self.manager = Principal(3, "manager", "manager", "L1")
        self.regional = Principal(4, "regional", "regional_manager")

    def tearDown(self):
        self.temp.cleanup()

    def test_passwords_are_salted_hashed_and_verified(self):
        first = hash_password("correct horse battery", salt=b"a" * 16)
        second = hash_password("correct horse battery", salt=b"b" * 16)
        self.assertNotEqual(first, second)
        self.assertTrue(verify_password("correct horse battery", first))
        self.assertFalse(verify_password("wrong password", first))
        with self.assertRaises(ValueError):
            hash_password("short")

    def test_user_login_and_role_creation(self):
        create_user(self.db, "owner", "a-secure-password", "administrator")
        create_user(self.db, "reviewer", "another-secure-password", "analyst", actor_role="administrator")
        self.assertEqual(authenticate(self.db, "OWNER", "a-secure-password").role, "administrator")
        self.assertIsNone(authenticate(self.db, "reviewer", "wrong-password"))
        with self.assertRaises(PermissionError):
            create_user(self.db, "bad", "another-secure-password", "administrator")

    def test_role_permission_matrix_is_enforced(self):
        self.assertTrue(allowed("administrator", "locations", "create"))
        self.assertFalse(allowed("regional_manager", "locations", "create"))
        self.assertTrue(allowed("analyst", "orders", "read"))
        self.assertFalse(allowed("analyst", "orders", "update"))
        self.assertFalse(allowed("manager", "menu_items", "update"))
        self.assertTrue(allowed("manager", "inventory", "update"))

    def test_crud_persists_and_manager_scope_is_enforced(self):
        save_record(self.db, "locations", {"Location_ID":"L1", "Restaurant_Name":"Central"}, self.admin)
        save_record(self.db, "locations", {"Location_ID":"L2", "Restaurant_Name":"West"}, self.admin)
        save_record(self.db, "menu_categories", {"Category_ID":"C1", "Category_Name":"Mains"}, self.admin)
        save_record(self.db, "menu_items", {"Item_ID":"I1", "Item_Name":"Bowl", "Category_ID":"C1", "Selling_Price":"12.5"}, self.admin)
        save_record(self.db, "orders", {"Order_ID":"O1", "Location_ID":"L1", "Order_Date":"2026-09-01", "Total_Amount":"12.5", "Order_Status":"Completed"}, self.admin)
        scoped = list_records(self.db, "orders", self.manager)
        self.assertEqual([row["Order_ID"] for row in scoped], ["O1"])
        with self.assertRaises(PermissionError):
            save_record(self.db, "orders", {"Order_ID":"O2", "Location_ID":"L2", "Order_Date":"2026-09-02", "Total_Amount":"1", "Order_Status":"Completed"}, self.manager)
        unassigned = Principal(5, "orphan", "manager")
        with self.assertRaises(PermissionError):
            list_records(self.db, "orders", unassigned)
        save_record(self.db, "orders", {"Total_Amount":"13.0"}, self.manager, record_id="O1")
        self.assertEqual(list_records(self.db, "orders", self.regional)[0]["Total_Amount"], 13.0)
        with self.assertRaises(PermissionError):
            save_record(self.db, "orders", {"Location_ID":"L2"}, self.manager, record_id="O1")

    def test_customer_schema_and_api_exclude_direct_identifiers(self):
        with sqlite3.connect(self.db) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(customers)")}
        self.assertNotIn("Customer_Name", columns)
        self.assertNotIn("Synthetic_Contact", columns)
        with self.assertRaises(ValueError):
            save_record(self.db, "customers", {"Customer_ID":"C", "Customer_Name":"private"}, self.admin)

    def test_connection_context_closes_database_handle(self):
        connection = connect_database(self.db)
        with connection as active:
            active.execute("SELECT 1")
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")

    def test_foreign_keys_and_database_constraints_are_active(self):
        with self.assertRaises(sqlite3.IntegrityError):
            save_record(self.db, "order_items", {"Order_Item_ID":"OI1", "Order_ID":"missing", "Item_ID":"missing", "Quantity":"1", "Unit_Price":"2", "Line_Total":"2"}, self.admin)


if __name__ == "__main__":
    unittest.main()
