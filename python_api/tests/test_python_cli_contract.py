import io
import json
import pathlib
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

import velaria_cli


class _FakeArrowResult:
    @property
    def schema(self):
        return mock.Mock(names=["row_id", "score"])

    def to_pylist(self):
        return [{"row_id": 0, "score": 0.0}]


class _FakeDataFrame:
    def to_arrow(self):
        return _FakeArrowResult()


class PythonCliContractTest(unittest.TestCase):
    def test_vector_cli_delegates_to_session_contract(self):
        fake_session = mock.Mock()
        fake_session.read_csv.return_value = mock.Mock(name="df")
        fake_session.vector_search.return_value = _FakeDataFrame()
        fake_session.explain_vector_search.return_value = (
            "mode=exact-scan\n"
            "metric=cosine\n"
            "dimension=3\n"
            "top_k=2\n"
            "candidate_rows=3\n"
            "filter_pushdown=false\n"
            "acceleration=flat-buffer+heap-topk\n"
        )

        with tempfile.TemporaryDirectory(prefix="velaria-cli-contract-") as tmp:
            csv_path = pathlib.Path(tmp) / "vectors.csv"
            csv_path.write_text("id,embedding\n1,[1 0 0]\n", encoding="utf-8")
            with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = velaria_cli._run_vector_search(
                        csv_path=csv_path,
                        vector_column="embedding",
                        query_vector="1.0,0.0,0.0",
                        metric="cosine",
                        top_k=2,
                    )

        self.assertEqual(exit_code, 0)
        fake_session.read_csv.assert_called_once_with(str(csv_path))
        fake_session.create_temp_view.assert_called_once()
        fake_session.vector_search.assert_called_once_with(
            table="input_table",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
        )
        fake_session.explain_vector_search.assert_called_once_with(
            table="input_table",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
        )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["metric"], "cosine")
        self.assertEqual(payload["top_k"], 2)
        self.assertEqual(payload["schema"], ["row_id", "score"])
        self.assertEqual(payload["rows"], [{"row_id": 0, "score": 0.0}])
        self.assertIn("mode=exact-scan", payload["explain"])
        self.assertIn("candidate_rows=3", payload["explain"])
        self.assertIn("filter_pushdown=false", payload["explain"])


if __name__ == "__main__":
    unittest.main()
