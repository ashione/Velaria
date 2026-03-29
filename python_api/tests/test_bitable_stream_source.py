import unittest

from velaria import BitableClient, group_rows_by_field, group_rows_count_by_field


class _FakeBitableClient(BitableClient):
    def __init__(self):
        super().__init__("demo_app_id", "demo_app_secret")
        self.calls = []

    def _request_with_token(self, method, url, body=None):
        self.calls.append((method, url, body))
        page_token = self.calls[-1][2].get("page_token")
        if page_token == "page_2":
            return {
                "code": 0,
                "data": {
                    "items": [
                        {"fields": {"负责人": "Bob", "count": 3}},
                    ],
                    "has_more": False,
                },
            }
        if page_token == "page_1":
            return {
                "code": 0,
                "data": {
                    "items": [
                        {"fields": {"负责人": "Bob", "count": 2}},
                    ],
                    "has_more": False,
                },
            }
        if page_token == "page_2_no_more":
            return {
                "code": 0,
                "data": {
                    "items": [
                        {"fields": {"负责人": "Bob", "count": 3}},
                    ],
                    "has_more": False,
                },
            }
        return {
            "code": 0,
            "data": {
                "items": [
                    {"fields": {"负责人": "Alice", "count": 1}},
                    {"fields": {"负责人": None, "count": 2}},
                ],
                "has_more": True,
                "page_token": "page_1",
            },
        }


class BitableStreamSourceTest(unittest.TestCase):
    def test_parse_bitable_url(self):
        client = BitableClient("app", "secret")
        app_token, table_id, view_id = client.parse_bitable_url(
            "https://example.com/base/app_token_example/tables/table_token_example?view=view_token_example"
        )
        self.assertEqual(app_token, "app_token_example")
        self.assertEqual(table_id, "table_token_example")
        self.assertEqual(view_id, "view_token_example")

    def test_group_by_owner(self):
        rows = [
            {"负责人": "Alice"},
            {"负责人": "Alice"},
            {"负责人": "Bob"},
            {"负责人": None},
        ]
        grouped = group_rows_by_field(rows, "负责人")
        grouped_counts = group_rows_count_by_field(rows, "负责人")
        self.assertIn("Alice", grouped)
        self.assertEqual(len(grouped["Alice"]), 2)
        self.assertEqual(grouped_counts["Alice"], 2)
        self.assertEqual(grouped_counts[""], 1)
        self.assertEqual(grouped_counts["Bob"], 1)

    def test_group_rows_handles_non_string_owner(self):
        rows = [
            {"负责人": {"name": "Alice"}},
            {"负责人": [1, 2, 3]},
            {"负责人": ["Charlie"]},
        ]
        grouped = group_rows_by_field(rows, "负责人")
        self.assertIn("Alice", grouped)
        self.assertIn("1", grouped)
        self.assertIn("Charlie", grouped)

    def test_list_records_pagination(self):
        client = _FakeBitableClient()
        records = client.list_records("app_token", "table_id", view_id=None, page_size=2)
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["负责人"], "Alice")
        self.assertEqual(records[2]["负责人"], "Bob")
        self.assertEqual(len(client.calls), 2)


if __name__ == "__main__":
    unittest.main()
