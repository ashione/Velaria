import unittest

import pyarrow as pa

from velaria import Session


class VectorSearchTest(unittest.TestCase):
    def test_vector_search_metrics_and_explain(self):
        session = Session()
        vectors = pa.array(
            [[1.0, 0.0, 0.0], [0.9, 0.1, 0.0], [0.0, 1.0, 0.0]],
            type=pa.list_(pa.float32(), 3),
        )
        table = pa.table({"id": [1, 2, 3], "embedding": vectors})
        df = session.create_dataframe_from_arrow(table)
        session.create_temp_view("vec_src_py", df)

        cosine = session.vector_search(
            table="vec_src_py",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
        ).to_rows()
        self.assertEqual(cosine["rows"][0][0], 0)

        dot = session.vector_search(
            table="vec_src_py",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=1,
            metric="dot",
        ).to_rows()
        self.assertEqual(dot["rows"][0][0], 0)

        explain = session.explain_vector_search(
            table="vec_src_py",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
        )
        self.assertIn("mode=exact-scan", explain)
        self.assertIn("metric=cosine", explain)
        self.assertIn("top_k=2", explain)
        self.assertIn("acceleration=flat-buffer+heap-topk", explain)

    def test_vector_dimension_mismatch(self):
        session = Session()
        vectors = pa.array([[1.0, 0.0], [0.0, 1.0]], type=pa.list_(pa.float32(), 2))
        table = pa.table({"embedding": vectors})
        df = session.create_dataframe_from_arrow(table)
        session.create_temp_view("vec_mismatch", df)

        with self.assertRaises(RuntimeError):
            session.vector_search(
                table="vec_mismatch",
                vector_column="embedding",
                query_vector=[1.0, 0.0, 0.0],
                top_k=1,
                metric="l2",
            )


if __name__ == "__main__":
    unittest.main()
