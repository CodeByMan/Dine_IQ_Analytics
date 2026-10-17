from pathlib import Path
import unittest


class DashboardContractTests(unittest.TestCase):
    def test_required_dashboard_routes_and_selected_visuals_are_connected(self):
        app = (Path(__file__).parents[1] / 'src/dineiq/ui/app.py').read_text(encoding='utf-8')
        for route in ('Executive', 'Menu', 'Customers', 'Wastage', 'Forecast', 'Spark vs Python'):
            self.assertIn(f'active_page == "{route}"', app)
        for renderer in ('render_kpi_overview', 'render_menu_ranking', 'render_demand_forecast',
                         'render_channel_sales', 'render_customer_segments', 'render_price_demand', 'render_ordering_activity'):
            self.assertIn(renderer + '(', app)
        for legacy in ('_render_gauge', '_render_radar', '_plotly_pie', '_render_table_visual_summary'):
            self.assertNotIn(legacy, app)

    def test_forecast_and_saved_model_service_boundaries_remain_present(self):
        app = (Path(__file__).parents[1] / 'src/dineiq/ui/app.py').read_text(encoding='utf-8')
        for token in ('"forecast_test"', '_load_wastage_predictor', 'model_risk_probability', '_load_live_predictor', 'DemandRequest('):
            self.assertIn(token, app)


if __name__ == '__main__':
    unittest.main()
