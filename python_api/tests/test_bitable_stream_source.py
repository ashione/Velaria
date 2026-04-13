import unittest
import urllib.parse

from velaria import BitableClient, group_rows_by_field, group_rows_count_by_field
from velaria.bitable import BitableClientError


class _FakeBitableClient(BitableClient):
    def __init__(self):
        super().__init__("demo_app_id", "demo_app_secret")
        self.calls = []

    def _request_with_token(self, method, url, body=None):
        self.calls.append((method, url, body))
        query = urllib.parse.urlparse(url).query
        page_token = urllib.parse.parse_qs(query).get("page_token", [None])[0]
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

    def test_parse_bitable_url_supports_query_table_style(self):
        client = BitableClient("app", "secret")
        app_token, table_id, view_id = client.parse_bitable_url(
            "https://bytedance.larkoffice.com/base/AcaBbdonCa4yRZsCybfcqELtncg"
            "?table=tblAvynRUWurGlqt&view=vew6pYpU2B"
        )
        self.assertEqual(app_token, "AcaBbdonCa4yRZsCybfcqELtncg")
        self.assertEqual(table_id, "tblAvynRUWurGlqt")
        self.assertEqual(view_id, "vew6pYpU2B")

    def test_parse_bitable_url_supports_wiki_query_table_style(self):
        client = BitableClient("app", "secret")
        app_token, table_id, view_id = client.parse_bitable_url(
            "https://bytedance.larkoffice.com/wiki/FdRXwUUdui08trkJsDbccSSjnEc"
            "?table=tblBdT1ddR5woaPk&view=vewGw8i12c"
        )
        self.assertEqual(app_token, "FdRXwUUdui08trkJsDbccSSjnEc")
        self.assertEqual(table_id, "tblBdT1ddR5woaPk")
        self.assertEqual(view_id, "vewGw8i12c")

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
        self.assertIn("page_size=2", client.calls[0][1])
        self.assertIn("page_token=page_1", client.calls[1][1])
        self.assertEqual(client.calls[1][2]["page_token"], "page_1")

    def test_view_access_error_mentions_base_view_read(self):
        class _ViewDeniedClient(BitableClient):
            def __init__(self):
                super().__init__("demo_app_id", "demo_app_secret")
                self.calls = 0

            def _request_with_token(self, method, url, body=None):
                self.calls += 1
                return {"code": 99991672, "msg": "Access denied"}

        client = _ViewDeniedClient()
        with self.assertRaisesRegex(BitableClientError, "base:view:read"):
            client.list_records("app_token", "table_id", view_id="vew123", page_size=2)
        self.assertEqual(client.calls, 2)

    def test_view_access_http_error_still_retries_and_mentions_base_view_read(self):
        class _ViewDeniedHttpClient(BitableClient):
            def __init__(self):
                super().__init__("demo_app_id", "demo_app_secret")
                self.calls = 0

            def _request_with_token(self, method, url, body=None):
                self.calls += 1
                raise BitableClientError("HTTP request failed (400): {\"code\":99991672}")

        client = _ViewDeniedHttpClient()
        with self.assertRaisesRegex(BitableClientError, "base:view:read"):
            client.list_records("app_token", "table_id", view_id="vew123", page_size=2)
        self.assertEqual(client.calls, 2)

    def test_view_that_matches_full_table_is_rejected(self):
        class _SameScopedAndUnscopedClient(BitableClient):
            def __init__(self):
                super().__init__("demo_app_id", "demo_app_secret")
                self.calls = []

            def _ensure_view_access(self, app_token, table_id, view_id):
                return None

            def _request_with_token(self, method, url, body=None):
                self.calls.append((method, url, body))
                return {
                    "code": 0,
                    "data": {
                        "items": [
                            {"record_id": "rec1", "fields": {"负责人": "Alice"}},
                            {"record_id": "rec2", "fields": {"负责人": "Bob"}},
                        ],
                        "has_more": True,
                        "page_token": "page_1",
                    },
                }

        client = _SameScopedAndUnscopedClient()
        with self.assertRaisesRegex(BitableClientError, "did not change the returned record page"):
            client.list_records("app_token", "table_id", view_id="vew123", page_size=2)


if __name__ == "__main__":
    unittest.main()
