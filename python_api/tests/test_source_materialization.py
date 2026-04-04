import pathlib
import tempfile
import time
import unittest

import velaria


class SourceMaterializationTest(unittest.TestCase):
    @staticmethod
    def _build_large_csv() -> str:
        lines = ["id,name,amount"]
        for index in range(1, 20001):
            lines.append(f"{index},user_{index},{index * 1.25:.2f}")
        return "\n".join(lines) + "\n"

    def _assert_sql_after_materialization_is_reused(self, format_name: str, expected_filename: str):
        with tempfile.TemporaryDirectory(prefix="velaria-source-materialization-") as tmp:
            root = pathlib.Path(tmp)
            csv_path = root / "input.csv"
            cache_dir = root / "cache"
            csv_path.write_text(self._build_large_csv(), encoding="utf-8")
            first_session = velaria.Session()
            first_df = first_session.read_csv(
                str(csv_path),
                materialization=True,
                materialization_dir=str(cache_dir),
                materialization_format=format_name,
            )
            first_session.create_temp_view("source_input", first_df)
            first = first_session.sql(
                "SELECT COUNT(*) AS row_count, SUM(amount) AS total_amount "
                "FROM source_input "
                "WHERE id > 15000"
            ).to_rows()
            data_files = list(cache_dir.rglob(expected_filename))
            self.assertEqual(len(data_files), 1)
            meta_files = list(cache_dir.rglob("meta.txt"))
            self.assertEqual(len(meta_files), 1)
            first_mtime = data_files[0].stat().st_mtime_ns

            time.sleep(0.05)
            second_session = velaria.Session()
            second_df = second_session.read_csv(
                str(csv_path),
                materialization=True,
                materialization_dir=str(cache_dir),
                materialization_format=format_name,
            )
            second_session.create_temp_view("source_input", second_df)
            second = second_session.sql(
                "SELECT COUNT(*) AS row_count, SUM(amount) AS total_amount "
                "FROM source_input "
                "WHERE id > 15000"
            ).to_rows()
            second_mtime = data_files[0].stat().st_mtime_ns

            self.assertEqual(first["schema"], ["row_count", "total_amount"])
            self.assertEqual(
                first["rows"],
                [
                    [5000, 109378125.0],
                ],
            )
            self.assertEqual(second, first)
            self.assertEqual(first_mtime, second_mtime)

            arrow_rows = second_session.read_csv(
                str(csv_path),
                materialization=True,
                materialization_dir=str(cache_dir),
                materialization_format=format_name,
            ).to_arrow().to_pylist()
            self.assertEqual(len(arrow_rows), 20000)
            self.assertEqual(arrow_rows[0]["id"], 1)
            self.assertEqual(arrow_rows[-1]["name"], "user_20000")

    def test_binary_row_batch_materialization_reuses_cached_file(self):
        self._assert_sql_after_materialization_is_reused("binary_row_batch", "table.bin")

    def test_nanoarrow_ipc_materialization_reuses_cached_file(self):
        self._assert_sql_after_materialization_is_reused("nanoarrow_ipc", "table.nanoarrow")


if __name__ == "__main__":
    unittest.main()
