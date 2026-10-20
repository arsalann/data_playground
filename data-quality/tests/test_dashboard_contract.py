"""Protect the SQL-free presentation contract without warehouse credentials.

Numerical regression expectations run through `bruin semantic check`.
"""

from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[2]
PROJECT = ROOT / "data-quality" / "dashboard-dac"


class DashboardContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dashboard = yaml.safe_load(
            (PROJECT / "dashboards" / "data-quality.yml").read_text()
        )
        cls.widgets = [
            widget
            for row in cls.dashboard["rows"]
            for widget in row["widgets"]
            if widget["type"] in {"chart", "table", "metric"}
        ]

    def test_no_handwritten_queries_or_static_data(self):
        prohibited = {"sql", "query", "queries", "file", "data"}

        def walk(value):
            if isinstance(value, dict):
                self.assertFalse(prohibited.intersection(value), value)
                for nested in value.values():
                    walk(nested)
            elif isinstance(value, list):
                for nested in value:
                    walk(nested)

        walk(self.dashboard)

    def test_every_existing_data_widget_uses_shared_semantics(self):
        self.assertEqual(len(self.widgets), 23)
        self.assertEqual(len({widget["id"] for widget in self.widgets}), 23)
        for widget in self.widgets:
            name = widget.get("model", self.dashboard["model"])
            canonical = ROOT / "semantic" / f"{name}.yml"
            local = PROJECT / "semantic" / f"{name}.yml"
            self.assertEqual(local.resolve(), canonical.resolve())
            model = yaml.safe_load(canonical.read_text())
            self.assertTrue(widget["metrics"], widget["id"])
            self.assertNotIn("SELECT", model["source"]["table"].upper())

    def test_all_widgets_keep_inclusive_date_filters(self):
        self.assertEqual(
            [control["name"] for control in self.dashboard["filters"]],
            ["start_date", "end_date"],
        )
        for widget in self.widgets:
            self.assertIn(
                {
                    "dimension": "dt",
                    "operator": "between",
                    "value": {
                        "start": "{{ filters.start_date }}",
                        "end": "{{ filters.end_date }}",
                    },
                },
                widget["filters"],
            )

    def test_chart_context_and_methodology_are_present(self):
        rows = self.dashboard["rows"]
        for index, row in enumerate(rows):
            widget = row["widgets"][0]
            if widget["type"] != "chart":
                continue
            self.assertEqual(widget["col"], 12)
            self.assertEqual(rows[index - 1]["widgets"][0]["type"], "text")
            self.assertEqual(rows[index + 1]["widgets"][0]["type"], "text")
            self.assertEqual(rows[index - 1]["tab"], row["tab"])
            self.assertEqual(rows[index + 1]["tab"], row["tab"])
        self.assertEqual(rows[-1]["widgets"][0]["name"], "Methodology")
        self.assertEqual(rows[-1]["tab"], "Daily Detail")


if __name__ == "__main__":
    unittest.main()
