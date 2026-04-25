import unittest
from pathlib import Path
import tempfile

import velaria

try:
    import pandas as pd  # noqa: F401
    import openpyxl  # noqa: F401
except Exception:  # pragma: no cover - optional runtime dependency
    pd = None
    openpyxl = None


class ReadExcelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if pd is None:
            raise unittest.SkipTest("pandas/openpyxl are required to read xlsx")

    @staticmethod
    def _mock_file() -> Path:
        return (
            Path(__file__).resolve().parent / "fixtures" / "employee_import_mock.xlsx"
        )

    def test_read_excel_fixture_file(self):
        fixture = self._mock_file()
        self.assertTrue(
            fixture.exists(),
            f"mock excel file not found: {fixture}",
        )

        session = velaria.Session()
        vdf = velaria.read_excel(session, str(fixture), sheet_name="员工")
        out = vdf.to_rows()

        self.assertEqual(out["schema"], ["employee_id", "name", "join_date", "dept"])
        self.assertEqual(len(out["rows"]), 3)
        self.assertEqual(out["rows"][0][0], "E1001")
        self.assertEqual(out["rows"][0][1], "Alice")
        self.assertEqual(out["rows"][1][2], "2024-02-05")
        self.assertEqual(out["rows"][2][3], "R&D")

    def test_read_excel_roundtrip(self):
        rows = {
            "employee_id": ["A1", "A2"],
            "join_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "value": [10, 20],
        }
        df = pd.DataFrame(rows)

        session = velaria.Session()

        with tempfile.TemporaryDirectory(prefix="velaria-excel-test-") as tmpdir:
            xlsx = Path(tmpdir) / "input.xlsx"
            df.to_excel(xlsx, index=False, sheet_name="people")

            vdf = velaria.read_excel(session, str(xlsx), sheet_name="people")
            out = vdf.to_rows()

            self.assertEqual(out["schema"], ["employee_id", "join_date", "value"])
            self.assertEqual(len(out["rows"]), 2)
            self.assertEqual(out["rows"][0][0], "A1")
            self.assertEqual(out["rows"][0][1], "2024-01-01")
            self.assertEqual(out["rows"][1][2], 20)


if __name__ == "__main__":
    unittest.main()
