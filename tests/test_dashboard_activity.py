import unittest

import pandas as pd

from dineiq.ui.activity import customer_cohort_heatmap, order_activity_heatmap


class DashboardActivityTests(unittest.TestCase):
    def setUp(self):
        self.orders = pd.DataFrame([
            ("c1", "2025-01-06", "09:20:00", "Completed"),
            ("c2", "2025-01-06", "09:55:00", "completed"),
            ("c1", "2025-02-03", "17:10:00", "completed"),
            ("c3", "2025-02-04", "11:00:00", "cancelled"),
        ], columns=["Customer_ID", "Order_Date", "Order_Time", "Order_Status"])

    def test_hourly_heatmap_counts_only_completed_orders_in_real_buckets(self):
        rows = order_activity_heatmap(self.orders)
        monday_morning = next(row for row in rows if row["day_name"] == "Monday" and row["hour_bucket"] == 8)
        tuesday_noon = next(row for row in rows if row["day_name"] == "Tuesday" and row["hour_bucket"] == 10)
        self.assertEqual(monday_morning["order_count"], 2)
        self.assertEqual(tuesday_noon["order_count"], 0)
        self.assertEqual(len(rows), 84)

    def test_cohorts_compute_retention_and_censor_future_periods(self):
        rows = customer_cohort_heatmap(self.orders, max_cohorts=7, periods=3)
        jan = [row for row in rows if row["cohort_month"] == "2025-01"]
        self.assertEqual(jan[0]["retention_pct"], 100.0)
        self.assertEqual(jan[1]["retention_pct"], 50.0)
        self.assertIsNone(jan[2]["retention_pct"])

    def test_missing_required_columns_yield_no_chart_data(self):
        self.assertEqual(order_activity_heatmap(pd.DataFrame({"Order_Date": []})), [])
        self.assertEqual(customer_cohort_heatmap(pd.DataFrame({"Customer_ID": []})), [])


if __name__ == "__main__":
    unittest.main()
