from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from dineiq.db.auth import authenticate, create_user, hash_password, verify_password
from dineiq.db.permissions import Principal, allowed
from dineiq.db.repository import list_records, save_record
from dineiq.db.schema import DDL, connect_database, initialize_database


class ManagementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / "dineiq.sqlite3")
        initialize_database(self.db)
        initialize_database(self.db)  # initializer is idempotent
        # Audit actors must be persisted users now that the canonical schema
        # enforces audit_events.actor_user_id -> users.user_id.
        with connect_database(self.db) as connection:
            connection.executemany(
                "INSERT INTO users(user_id,username,password_hash,role) VALUES(?,?,?,?)",
                [(1, "admin", "fixture", "administrator"), (2, "analyst", "fixture", "analyst"),
                 (3, "manager", "fixture", "manager"), (4, "regional", "fixture", "regional_manager")],
            )
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
        create_user(self.db, "owner", "a-secure-password", "administrator", actor_role="administrator")
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

    def test_chain_wide_prices_validate_periods_and_manager_permissions(self):
        save_record(self.db, "menu_items", {"Item_ID": "I1", "Item_Name": "Bowl", "Selling_Price": 12}, self.admin)
        base = {"Price_History_ID": "P1", "Item_ID": "I1", "Effective_From": "2025-01-01",
                "Effective_To": "2025-06-30", "New_Price": 12}
        save_record(self.db, "pricing_history", base, self.admin)
        with self.assertRaisesRegex(ValueError, "overlap"):
            save_record(self.db, "pricing_history", {**base, "Price_History_ID": "P2", "New_Price": 13}, self.admin)
        save_record(self.db, "pricing_history", {**base, "Price_History_ID": "P2", "Effective_From": "2025-07-01",
                                                "Effective_To": None, "New_Price": 13}, self.admin)
        with self.assertRaisesRegex(ValueError, "chain-wide"):
            save_record(self.db, "pricing_history", {"Location_ID": "L1"}, self.admin, record_id="P2")
        with self.assertRaisesRegex(ValueError, "precede"):
            save_record(self.db, "pricing_history", {"Effective_To": "2024-12-31"}, self.admin, record_id="P1")
        with self.assertRaises(PermissionError):
            save_record(self.db, "pricing_history", {"New_Price": 15}, self.manager, record_id="P2")
        self.assertEqual(list_records(self.db, "pricing_history", self.admin)[1]["New_Price"], 13)

    def test_price_rejects_nonfinite_zero_and_excess_precision(self):
        save_record(self.db, "menu_items", {"Item_ID": "I1", "Item_Name": "Bowl", "Selling_Price": 12}, self.admin)
        for value in (0, -1, "NaN", "Infinity", "12.001"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                save_record(self.db, "pricing_history", {"Price_History_ID": "P1", "Item_ID": "I1",
                                                        "Effective_From": "2025-01-01", "New_Price": value}, self.admin)

    def test_connection_context_closes_database_handle(self):
        connection = connect_database(self.db)
        with connection as active:
            active.execute("SELECT 1")
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")

    def test_foreign_keys_and_database_constraints_are_active(self):
        with self.assertRaises(sqlite3.IntegrityError):
            save_record(self.db, "order_items", {"Order_Item_ID":"OI1", "Order_ID":"missing", "Item_ID":"missing", "Quantity":"1", "Unit_Price":"2", "Line_Total":"2"}, self.admin)

    def test_runtime_tables_and_indexes_match_canonical_sql(self):
        with sqlite3.connect(":memory:") as expected, connect_database(self.db) as actual:
            expected.executescript(DDL)
            def catalog(connection):
                return {(row[0], row[1]): "".join(row[2][row[2].index("("):].split()).lower()
                        for row in connection.execute(
                            "SELECT type,name,sql FROM sqlite_master WHERE type IN ('table','index') "
                            "AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'")}
            self.assertEqual(catalog(actual), catalog(expected))

    def test_observed_order_channels_remain_supported_with_foreign_key(self):
        save_record(self.db, "locations", {"Location_ID": "L1", "Restaurant_Name": "Central"}, self.admin)
        save_record(self.db, "orders", {"Order_ID": "O1", "Location_ID": "L1",
                    "Order_Date": "2025-01-01", "Total_Amount": 12, "Order_Status": "Completed",
                    "Channel_ID": "Website/App"}, self.admin)
        with connect_database(self.db) as connection:
            self.assertEqual(connection.execute(
                "SELECT Channel_Name FROM ordering_channels WHERE Channel_ID='Website/App'"
            ).fetchone()[0], "Website/App")
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE orders SET Channel_ID='orphan' WHERE Order_ID='O1'")

    def test_legacy_schema_migration_preserves_rows_and_enforces_new_checks(self):
        legacy_path = Path(self.temp.name) / "legacy.sqlite3"
        legacy = DDL.replace("Channel_ID TEXT REFERENCES ordering_channels(Channel_ID)", "Channel_ID TEXT")
        legacy = legacy.replace("Opening_Stock >= 0", "1 = 1")
        with sqlite3.connect(legacy_path) as connection:
            connection.executescript(legacy)
            connection.execute("INSERT INTO locations(Location_ID,Restaurant_Name) VALUES('L1','Central')")
            connection.execute("INSERT INTO menu_items(Item_ID,Item_Name,Selling_Price) VALUES('I1','Bowl',12)")
            connection.execute("INSERT INTO orders(Order_ID,Location_ID,Order_Date,Total_Amount,Order_Status,Channel_ID) "
                               "VALUES('O1','L1','2025-01-01',12,'Completed','Delivery')")
            connection.execute("INSERT INTO inventory(Inventory_ID,Item_ID,Location_ID,Inventory_Date,Opening_Stock) "
                               "VALUES('INV1','I1','L1','2025-01-01',5)")
        initialize_database(legacy_path)
        initialize_database(legacy_path)
        with connect_database(legacy_path) as connection:
            self.assertEqual(connection.execute("SELECT Channel_ID FROM orders").fetchone()[0], "Delivery")
            self.assertEqual(connection.execute("SELECT Opening_Stock FROM inventory").fetchone()[0], 5)
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE inventory SET Opening_Stock=-1")

    def test_legacy_constraint_violation_rolls_back_migration(self):
        legacy_path = Path(self.temp.name) / "invalid_legacy.sqlite3"
        legacy = DDL.replace("Opening_Stock >= 0", "1 = 1")
        with sqlite3.connect(legacy_path) as connection:
            connection.executescript(legacy)
            connection.execute("INSERT INTO locations(Location_ID,Restaurant_Name) VALUES('L1','Central')")
            connection.execute("INSERT INTO menu_items(Item_ID,Item_Name,Selling_Price) VALUES('I1','Bowl',12)")
            connection.execute("INSERT INTO inventory(Inventory_ID,Item_ID,Location_ID,Inventory_Date,Opening_Stock) "
                               "VALUES('INV1','I1','L1','2025-01-01',-5)")
        with self.assertRaises(sqlite3.IntegrityError):
            initialize_database(legacy_path)
        with sqlite3.connect(legacy_path) as connection:
            self.assertEqual(connection.execute("SELECT Opening_Stock FROM inventory").fetchone()[0], -5)
            self.assertIn("1 = 1", connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='inventory'").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
