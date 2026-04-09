import json
import pathlib
import tempfile

from velaria import (
    HashEmbeddingProvider,
    Session,
    build_embedding_rows,
    embed_query_text,
    load_embedding_dataframe,
    materialize_embeddings,
)


def main():
    records = [
        {
            "doc_id": "doc-1",
            "text": "title: alpha\nsummary: apac support runbook",
            "bucket": 1,
            "source_updated_at": "2026-04-09T00:00:00Z",
        },
        {
            "doc_id": "doc-2",
            "text": "title: beta\nsummary: emea billing guide",
            "bucket": 2,
            "source_updated_at": "2026-04-09T00:00:00Z",
        },
    ]
    provider = HashEmbeddingProvider(dimension=8)
    rows = build_embedding_rows(records, template_version="text-v1")

    with tempfile.TemporaryDirectory(prefix="velaria-embedding-demo-") as tmp:
        path = pathlib.Path(tmp) / "docs_embeddings__text-v1__hash-demo__dim-8.parquet"
        table = materialize_embeddings(
            rows,
            provider=provider,
            model="hash-demo",
            output_path=path,
        )

        session = Session()
        df = load_embedding_dataframe(session, path)
        session.create_temp_view("docs_embeddings", df)
        query = embed_query_text(
            "title: alpha\nsummary: apac support runbook",
            provider=provider,
            model="hash-demo",
        )
        result = session.hybrid_search(
            table="docs_embeddings",
            vector_column="embedding",
            query_vector=query,
            top_k=2,
            metric="cosine",
        ).to_arrow()

        print(
            json.dumps(
                {
                    "output_path": str(path),
                    "schema": table.schema.names,
                    "query_rows": result.to_pylist(),
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
