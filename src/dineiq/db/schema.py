"""Idempotent SQLite schema for DineIQ operational records and accounts."""
from __future__ import annotations

import sqlite3
from pathlib import Path

class _ClosingConnection(sqlite3.Connection):
    """Connection context manager that commits/rolls back and always closes."""
    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


DDL = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS users (
 user_id INTEGER PRIMARY KEY, username TEXT NOT NULL UNIQUE COLLATE NOCASE,
 password_hash TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('administrator','regional_manager','manager','analyst')),
 assigned_location_id TEXT, is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS audit_events (
 event_id INTEGER PRIMARY KEY, occurred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 actor_user_id INTEGER, actor_name TEXT NOT NULL,
 action TEXT NOT NULL, entity_type TEXT NOT NULL, entity_id TEXT,
 outcome TEXT NOT NULL CHECK(outcome IN ('success','failure')), details TEXT
);
CREATE TABLE IF NOT EXISTS model_runs (
 run_id TEXT PRIMARY KEY, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 pipeline TEXT NOT NULL, model_name TEXT NOT NULL, model_version TEXT NOT NULL,
 artifact_path TEXT NOT NULL, metrics_json TEXT NOT NULL, record_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS stored_results (
 result_id INTEGER PRIMARY KEY, run_id TEXT REFERENCES model_runs(run_id),
 result_type TEXT NOT NULL, entity_id TEXT, model_version TEXT NOT NULL,
 value_json TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS processing_jobs (
 job_id TEXT PRIMARY KEY, started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 finished_at TEXT, job_name TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed')),
 message TEXT, details TEXT
);
CREATE TABLE IF NOT EXISTS locations (
 Location_ID TEXT PRIMARY KEY, Restaurant_Name TEXT NOT NULL, City TEXT, Region TEXT,
 Address TEXT, Phone TEXT, Opening_Date TEXT, Seating_Capacity INTEGER,
 Average_Rating REAL, Is_Active INTEGER NOT NULL DEFAULT 1 CHECK(Is_Active IN (0,1))
);
CREATE TABLE IF NOT EXISTS menu_categories (
 Category_ID TEXT PRIMARY KEY, Category_Name TEXT NOT NULL, Description TEXT
);
CREATE TABLE IF NOT EXISTS menu_items (
 Item_ID TEXT PRIMARY KEY, Item_Name TEXT NOT NULL,
 Category_ID TEXT REFERENCES menu_categories(Category_ID), Description TEXT,
 Selling_Price REAL NOT NULL CHECK(Selling_Price >= 0), Ingredient_Cost REAL CHECK(Ingredient_Cost >= 0),
 Is_Available INTEGER NOT NULL DEFAULT 1 CHECK(Is_Available IN (0,1)),
 Is_Active INTEGER NOT NULL DEFAULT 1 CHECK(Is_Active IN (0,1))
);
CREATE TABLE IF NOT EXISTS pricing_history (
 Price_History_ID TEXT PRIMARY KEY, Item_ID TEXT NOT NULL REFERENCES menu_items(Item_ID),
 Location_ID TEXT REFERENCES locations(Location_ID), Effective_From TEXT NOT NULL, Effective_To TEXT CHECK(Effective_To IS NULL OR Effective_To >= Effective_From),
 Old_Price REAL CHECK(Old_Price >= 0), New_Price REAL NOT NULL CHECK(New_Price >= 0), Reason TEXT
);
CREATE TABLE IF NOT EXISTS customers (
 Customer_ID TEXT PRIMARY KEY, Customer_Segment TEXT, Signup_Date TEXT,
 Total_Orders INTEGER CHECK(Total_Orders >= 0), Total_Spend REAL CHECK(Total_Spend >= 0),
 Average_Order_Value REAL CHECK(Average_Order_Value >= 0), Last_Order_Date TEXT,
 Recency_Days INTEGER CHECK(Recency_Days >= 0), Frequency_Score REAL, Monetary_Score REAL
);
CREATE TABLE IF NOT EXISTS orders (
 Order_ID TEXT PRIMARY KEY, Customer_ID TEXT REFERENCES customers(Customer_ID),
 Location_ID TEXT NOT NULL REFERENCES locations(Location_ID), Promotion_ID TEXT REFERENCES promotions(Promotion_ID),
 Order_Date TEXT NOT NULL, Order_Time TEXT, Total_Amount REAL NOT NULL CHECK(Total_Amount >= 0),
 Order_Status TEXT NOT NULL, Channel_ID TEXT, Payment_Method TEXT, Cancellation_Reason TEXT
);
CREATE TABLE IF NOT EXISTS order_items (
 Order_Item_ID TEXT PRIMARY KEY, Order_ID TEXT NOT NULL REFERENCES orders(Order_ID),
 Item_ID TEXT NOT NULL REFERENCES menu_items(Item_ID), Quantity REAL NOT NULL CHECK(Quantity > 0),
 Unit_Price REAL NOT NULL CHECK(Unit_Price >= 0), Discount_Amount REAL NOT NULL DEFAULT 0 CHECK(Discount_Amount >= 0),
 Line_Total REAL NOT NULL CHECK(Line_Total >= 0), Gross_Profit REAL
);
CREATE TABLE IF NOT EXISTS promotions (
 Promotion_ID TEXT PRIMARY KEY, Promotion_Name TEXT NOT NULL, Promotion_Type TEXT,
 Discount_Percentage REAL CHECK(Discount_Percentage BETWEEN 0 AND 100),
 Start_Date TEXT NOT NULL, End_Date TEXT NOT NULL CHECK(End_Date >= Start_Date), Is_Active INTEGER NOT NULL DEFAULT 1 CHECK(Is_Active IN (0,1)),
 Applicable_Item_IDs TEXT, Description TEXT
);
CREATE TABLE IF NOT EXISTS ratings (
 Rating_ID TEXT PRIMARY KEY, Customer_ID TEXT REFERENCES customers(Customer_ID),
 Order_ID TEXT REFERENCES orders(Order_ID), Item_ID TEXT REFERENCES menu_items(Item_ID),
 Location_ID TEXT REFERENCES locations(Location_ID), Rating REAL NOT NULL CHECK(Rating BETWEEN 1 AND 5),
 Review_Date TEXT NOT NULL, CHECK(Item_ID IS NOT NULL OR Location_ID IS NOT NULL)
);
CREATE TABLE IF NOT EXISTS inventory (
 Inventory_ID TEXT PRIMARY KEY, Item_ID TEXT NOT NULL REFERENCES menu_items(Item_ID),
 Location_ID TEXT NOT NULL REFERENCES locations(Location_ID), Inventory_Date TEXT NOT NULL,
 Opening_Stock REAL NOT NULL DEFAULT 0, Purchases REAL NOT NULL DEFAULT 0, Units_Sold REAL NOT NULL DEFAULT 0,
 Wastage_Quantity REAL NOT NULL DEFAULT 0, Closing_Stock REAL NOT NULL DEFAULT 0,
 Reorder_Level REAL NOT NULL DEFAULT 0, Adjustments REAL NOT NULL DEFAULT 0, Stockout_Flag INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS wastage (
 Wastage_ID TEXT PRIMARY KEY, Item_ID TEXT NOT NULL REFERENCES menu_items(Item_ID),
 Location_ID TEXT NOT NULL REFERENCES locations(Location_ID), Wastage_Date TEXT NOT NULL,
 Quantity_Wasted REAL NOT NULL CHECK(Quantity_Wasted >= 0), Unit_Cost REAL NOT NULL CHECK(Unit_Cost >= 0),
 Wastage_Cost REAL NOT NULL CHECK(Wastage_Cost >= 0), Reason TEXT, Preparation_Quantity REAL CHECK(Preparation_Quantity >= 0)
);
CREATE INDEX IF NOT EXISTS idx_orders_location_date ON orders(Location_ID, Order_Date);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(Order_ID);
CREATE INDEX IF NOT EXISTS idx_pricing_item_dates ON pricing_history(Item_ID, Effective_From);
CREATE INDEX IF NOT EXISTS idx_inventory_loc_date ON inventory(Location_ID, Inventory_Date);
CREATE INDEX IF NOT EXISTS idx_wastage_loc_date ON wastage(Location_ID, Wastage_Date);
CREATE INDEX IF NOT EXISTS idx_audit_occurred ON audit_events(occurred_at);
CREATE INDEX IF NOT EXISTS idx_jobs_started ON processing_jobs(started_at);
CREATE INDEX IF NOT EXISTS idx_results_run ON stored_results(run_id);
"""


def connect_database(path: str | Path) -> sqlite3.Connection:
    """Open a connection with foreign-key enforcement and row mappings."""
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, timeout=30, factory=_ClosingConnection)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database(path: str | Path) -> None:
    """Create all operational and account tables safely on first launch."""
    with connect_database(path) as connection:
        connection.executescript(DDL)
