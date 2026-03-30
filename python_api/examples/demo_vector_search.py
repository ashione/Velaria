import json

import pyarrow as pa

from velaria import Session


def main():
    session = Session()
    vectors = pa.FixedSizeListArray.from_arrays(
        pa.array(
            [
                1.0,
                0.0,
                0.0,
                0.9,
                0.1,
                0.0,
                0.0,
                1.0,
                0.0,
            ],
            type=pa.float32(),
        ),
        3,
    )
    table = pa.table({"id": [1, 2, 3], "embedding": vectors})
    df = session.create_dataframe_from_arrow(table)
    session.create_temp_view("vec_demo", df)

    result = session.vector_search(
        table="vec_demo",
        vector_column="embedding",
        query_vector=[1.0, 0.0, 0.0],
        top_k=2,
        metric="cosine",
    ).to_arrow()
    explain = session.explain_vector_search(
        table="vec_demo",
        vector_column="embedding",
        query_vector=[1.0, 0.0, 0.0],
        top_k=2,
        metric="cosine",
    )

    print(
        json.dumps(
            {
                "schema": result.schema.names,
                "rows": result.to_pylist(),
                "explain": explain,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
