import json

import pyarrow as pa

from velaria import Session


def main():
    session = Session()

    source = pa.table(
        {
            "name": ["alice", "alice", "bob", "carol"],
            "score": [10, 15, 20, 3],
        }
    )

    df = session.create_dataframe_from_arrow(source)
    session.create_temp_view("scores_arrow", df)

    result = session.sql(
        """
        SELECT name, SUM(score) AS total
        FROM scores_arrow
        WHERE score > 5
        GROUP BY name
        HAVING total > 20
        LIMIT 10
        """
    ).to_arrow()

    print(
        json.dumps(
            {
                "schema": result.schema.names,
                "rows": result.to_pylist(),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
