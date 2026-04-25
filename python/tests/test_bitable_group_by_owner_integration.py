import os
import unittest

from velaria import BitableClient, group_rows_count_by_field


class BitableGroupByOwnerIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.app_id = os.getenv("FEISHU_BITABLE_APP_ID")
        self.app_secret = os.getenv("FEISHU_BITABLE_APP_SECRET")
        self.bitable_url = os.getenv("FEISHU_BITABLE_BASE_URL")
        self.owner_field = os.getenv("FEISHU_BITABLE_OWNER_FIELD")
        if not self.app_id or not self.app_secret:
            self.skipTest("FEISHU_BITABLE_APP_ID/FEISHU_BITABLE_APP_SECRET missing")
        if not self.bitable_url:
            self.skipTest("FEISHU_BITABLE_BASE_URL missing")
        if not self.owner_field:
            self.skipTest("FEISHU_BITABLE_OWNER_FIELD missing")

    def test_group_by_owner_from_bitable(self):
        client = BitableClient(
            app_id=self.app_id,
            app_secret=self.app_secret,
        )
        records = client.list_records_from_url(self.bitable_url)
        self.assertIsInstance(records, list)
        if not records:
            self.skipTest("Bitable table has no rows in this environment")
        grouped = group_rows_count_by_field(records, self.owner_field)
        self.assertGreaterEqual(len(grouped), 1)
        self.assertTrue(sum(grouped.values()) >= 1)


if __name__ == "__main__":
    unittest.main()
