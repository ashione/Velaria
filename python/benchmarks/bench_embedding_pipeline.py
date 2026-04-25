import json
import argparse
import pathlib
import tempfile
import time

from velaria import (
    DEFAULT_LOCAL_EMBEDDING_MODEL,
    HashEmbeddingProvider,
    Session,
    SentenceTransformerEmbeddingProvider,
    build_embedding_rows,
    embed_query_text,
    load_embedding_dataframe,
    materialize_embeddings,
)


def _make_records(count: int) -> list[dict[str, object]]:
    return [
        {
            "doc_id": f"doc-{idx}",
            "title": f"title {idx}",
            "summary": f"summary {idx % 97}",
            "tags": [f"group-{idx % 8}", f"lang-{idx % 3}"],
            "bucket": idx % 16,
            "source_updated_at": idx,
        }
        for idx in range(count)
    ]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark embedding batch generation and online query.")
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--dimension", type=int, default=128, help="Only used for hash provider.")
    parser.add_argument(
        "--provider",
        choices=["hash", "minilm"],
        default="hash",
        help="Embedding provider to benchmark.",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Embedding model name. Used by minilm provider and recorded in payload.",
    )
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--metric", choices=["cosine", "dot", "l2"], default="cosine")
    return parser.parse_args()


def _make_provider(args: argparse.Namespace):
    if args.model is None:
        args.model = DEFAULT_LOCAL_EMBEDDING_MODEL if args.provider == "minilm" else "hash-demo"
    if args.provider == "minilm":
        return SentenceTransformerEmbeddingProvider(model_name=args.model)
    return HashEmbeddingProvider(dimension=args.dimension)


def main():
    args = _parse_args()
    rows = args.rows
    provider = _make_provider(args)
    records = _make_records(rows)

    t0 = time.perf_counter()
    embedding_rows = build_embedding_rows(records, template_version="text-v1")
    t1 = time.perf_counter()

    with tempfile.TemporaryDirectory(prefix="velaria-embedding-bench-") as tmp:
        output_path = pathlib.Path(tmp) / "docs_embeddings.parquet"
        t2 = time.perf_counter()
        table = materialize_embeddings(
            embedding_rows,
            provider=provider,
            model=args.model,
            output_path=output_path,
            batch_size=args.batch_size,
        )
        t3 = time.perf_counter()

        session = Session()
        df = load_embedding_dataframe(session, output_path)
        session.create_temp_view("docs_embeddings_bench", df)
        query_text = "title: title 17\nsummary: summary 17"

        t4 = time.perf_counter()
        query_vector = embed_query_text(
            query_text,
            provider=provider,
            model=args.model,
        )
        t5 = time.perf_counter()

        t6 = time.perf_counter()
        result = session.hybrid_search(
            table="docs_embeddings_bench",
            vector_column="embedding",
            query_vector=query_vector,
            top_k=args.top_k,
            metric=args.metric,
        ).to_arrow()
        t7 = time.perf_counter()

        payload = {
            "bench": "embedding-pipeline",
            "provider": provider.provider_name,
            "model": args.model,
            "rows": rows,
            "dimension": table.schema.field("embedding").type.list_size,
            "build_rows_ms": round((t1 - t0) * 1000.0, 3),
            "materialize_ms": round((t3 - t2) * 1000.0, 3),
            "batch_rows_per_sec": round(rows / max(t3 - t2, 1e-9), 3),
            "online_query_embed_ms": round((t5 - t4) * 1000.0, 3),
            "online_query_search_ms": round((t7 - t6) * 1000.0, 3),
            "online_query_result_rows": len(result.to_pylist()),
            "top_k": args.top_k,
            "metric": args.metric,
            "output_path": str(output_path),
        }
        print(json.dumps(payload))


if __name__ == "__main__":
    main()
