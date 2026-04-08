import pathlib
import tempfile
import unittest

import velaria


class FileSourcePythonApiTest(unittest.TestCase):
    def test_probe_and_read_auto_infer_json_source(self):
        session = velaria.Session()
        with tempfile.TemporaryDirectory(prefix="velaria-py-probe-json-") as tmp:
            json_path = pathlib.Path(tmp) / "events.jsonl"
            json_path.write_text(
                '{"user_id":1,"name":"alice","score":1.5}\n'
                '{"user_id":2,"name":"bob","score":2.5}\n',
                encoding="utf-8",
            )

            probe = session.probe(str(json_path))
            self.assertEqual(probe["kind"], "json")
            self.assertEqual(probe["final_format"], "json_lines")
            self.assertGreaterEqual(probe["score"], 90)
            self.assertEqual(probe["confidence"], "high")
            self.assertEqual(probe["schema"], ["user_id", "name", "score"])
            self.assertEqual(probe["suggested_table_name"], "events")
            self.assertGreaterEqual(len(probe["candidates"]), 1)
            self.assertEqual(probe["candidates"][0]["format"], "json_lines")
            self.assertGreaterEqual(len(probe["candidates"][0]["evidence"]), 1)
            self.assertEqual(probe["warnings"], [])

            auto_df = session.read(str(json_path))
            session.create_temp_view("events_auto", auto_df)
            sql_rows = session.sql(
                "SELECT name FROM events_auto WHERE user_id > 1"
            ).to_rows()
            self.assertEqual(sql_rows["rows"], [["bob"]])

            rows = auto_df.to_rows()
            self.assertEqual(rows["schema"], ["user_id", "name", "score"])
            self.assertEqual(rows["rows"], [[1, "alice", 1.5], [2, "bob", 2.5]])

    def test_read_line_file_supports_split_and_regex_modes(self):
        session = velaria.Session()
        with tempfile.TemporaryDirectory(prefix="velaria-py-line-source-") as tmp:
            root = pathlib.Path(tmp)
            split_path = root / "split.log"
            regex_path = root / "regex.log"
            split_path.write_text("1001|ok|12.5\n1002|fail|9.5\n", encoding="utf-8")
            regex_path.write_text(
                "uid=7 action=click latency=11\nuid=8 action=view latency=13\n",
                encoding="utf-8",
            )

            split_rows = session.read_line_file(
                str(split_path),
                mappings=[("user_id", 0), ("state", 1), ("score", 2)],
                split_delimiter="|",
            ).to_rows()
            self.assertEqual(split_rows["schema"], ["user_id", "state", "score"])
            self.assertEqual(split_rows["rows"], [[1001, "ok", 12.5], [1002, "fail", 9.5]])

            complex_split_path = root / "split_complex.log"
            complex_split_path.write_text(
                "1003|true|null|C:\\\\logs\\\\app\n1004|false|7|hello\\tworld\n",
                encoding="utf-8",
            )
            complex_split_rows = session.read_line_file(
                str(complex_split_path),
                mappings=[("user_id", 0), ("ok", 1), ("score", 2), ("note", 3)],
                split_delimiter="|",
            ).to_rows()
            self.assertEqual(complex_split_rows["rows"][0], [1003, True, None, r"C:\\logs\\app"])

            regex_rows = session.read_line_file(
                str(regex_path),
                mappings=[("uid", 1), ("action", 2), ("latency", 3)],
                mode="regex",
                regex_pattern=r"^uid=(\d+) action=(\w+) latency=(\d+)$",
            ).to_rows()
            self.assertEqual(regex_rows["schema"], ["uid", "action", "latency"])
            self.assertEqual(regex_rows["rows"], [[7, "click", 11], [8, "view", 13]])

            regex_complex_path = root / "regex_complex.log"
            regex_complex_path.write_text(
                'uid=9 action="open file" latency=17 ok=true note=C:\\\\logs\\\\app\n'
                'uid=10 action="sync job" latency=21 ok=false note=hello\\\\nworld\n',
                encoding="utf-8",
            )
            regex_complex_rows = session.read_line_file(
                str(regex_complex_path),
                mappings=[("uid", 1), ("action", 2), ("latency", 3), ("ok", 4), ("note", 5)],
                mode="regex",
                regex_pattern=r'^uid=(\d+) action="([^"]+)" latency=(\d+) ok=(true|false) note=(.+)$',
            ).to_rows()
            self.assertEqual(regex_complex_rows["rows"][0], [9, "open file", 17, True, r"C:\\logs\\app"])
            self.assertEqual(regex_complex_rows["rows"][1][4], r"hello\\nworld")

    def test_read_json_supports_json_lines_json_array_and_materialization(self):
        session = velaria.Session()
        with tempfile.TemporaryDirectory(prefix="velaria-py-json-source-") as tmp:
            root = pathlib.Path(tmp)
            jsonl_path = root / "events.jsonl"
            json_array_path = root / "events.json"
            cache_dir = root / "cache"
            jsonl_path.write_text(
                '{"user_id":1,"name":"alice","score":1.5}\n'
                '{"user_id":2,"name":"bob","score":2.5}\n',
                encoding="utf-8",
            )
            json_array_path.write_text(
                '[{"event":"open","cost":1.5},{"event":"close","cost":2}]',
                encoding="utf-8",
            )

            jsonl_rows = session.read_json(
                str(jsonl_path),
                columns=["user_id", "name", "score"],
                materialization=True,
                materialization_dir=str(cache_dir),
                materialization_format="nanoarrow_ipc",
            ).to_rows()
            self.assertEqual(jsonl_rows["schema"], ["user_id", "name", "score"])
            self.assertEqual(jsonl_rows["rows"], [[1, "alice", 1.5], [2, "bob", 2.5]])
            self.assertTrue(any(cache_dir.rglob("*")))

            json_array_rows = session.read_json(
                str(json_array_path),
                columns=["event", "cost"],
                format="json_array",
            ).to_rows()
            self.assertEqual(json_array_rows["schema"], ["event", "cost"])
            self.assertEqual(json_array_rows["rows"], [["open", 1.5], ["close", 2]])

            json_complex_path = root / "events_complex.jsonl"
            json_complex_path.write_text(
                '{"user_id":3,"name":"al\\"ice","ok":true,"note":"line\\nnext","score":null}\n'
                '{"user_id":4,"name":"tab\\tuser","ok":false,"note":"C:\\\\logs","score":7}\n',
                encoding="utf-8",
            )
            json_complex_rows = session.read_json(
                str(json_complex_path),
                columns=["user_id", "name", "ok", "note", "score"],
            ).to_rows()
            self.assertEqual(json_complex_rows["rows"][0], [3, 'al"ice', True, "line\nnext", None])
            self.assertEqual(json_complex_rows["rows"][1], [4, "tab\tuser", False, r"C:\logs", 7])


if __name__ == "__main__":
    unittest.main()
