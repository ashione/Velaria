import argparse
import json
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


ISSUES = [
    {
        "issue_id": "checkout_timeout",
        "doc_issue": "checkout payment timeout",
        "query_issue": "payment page keeps hanging",
        "service": "billing gateway",
    },
    {
        "issue_id": "refund_delay",
        "doc_issue": "refund processing delay",
        "query_issue": "money back request is very slow",
        "service": "refund service",
    },
    {
        "issue_id": "invoice_missing",
        "doc_issue": "invoice generation missing",
        "query_issue": "customer receipt is not showing up",
        "service": "invoice worker",
    },
    {
        "issue_id": "login_lockout",
        "doc_issue": "login session lockout",
        "query_issue": "users keep getting signed out and blocked",
        "service": "identity auth",
    },
    {
        "issue_id": "search_stale",
        "doc_issue": "catalog search stale index",
        "query_issue": "product lookup results are outdated",
        "service": "catalog search",
    },
    {
        "issue_id": "shipment_retry",
        "doc_issue": "shipment webhook retry storm",
        "query_issue": "delivery callback keeps retrying nonstop",
        "service": "logistics webhook",
    },
]

REGIONS = [
    ("asia pacific", "APAC"),
    ("europe middle east africa", "EMEA"),
    ("north america", "NA"),
    ("latin america", "LATAM"),
]

SEVERITIES = [
    ("critical", "sev0"),
    ("high", "sev1"),
    ("medium", "sev2"),
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare embedding providers on shifted-distribution recall.")
    parser.add_argument(
        "--providers",
        default="hash,minilm",
        help="Comma-separated providers from: hash,minilm",
    )
    parser.add_argument("--dimension", type=int, default=64, help="Only used by hash provider.")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--metric", choices=["cosine", "dot", "l2"], default="cosine")
    parser.add_argument("--query-limit", type=int, default=0, help="0 means all generated queries.")
    parser.add_argument("--model", default=DEFAULT_LOCAL_EMBEDDING_MODEL)
    return parser.parse_args()


def _make_provider(name: str, *, dimension: int, model: str):
    if name == "minilm":
        return SentenceTransformerEmbeddingProvider(model_name=model)
    if name == "hash":
        return HashEmbeddingProvider(dimension=dimension)
    raise ValueError(f"unsupported provider: {name}")


def _build_docs_and_queries():
    docs = []
    queries = []
    ordinal = 0
    for issue in ISSUES:
        for region_index, (doc_region, query_region) in enumerate(REGIONS):
            doc_severity, query_severity = SEVERITIES[ordinal % len(SEVERITIES)]
            doc_id = f"{issue['issue_id']}__{region_index}"
            docs.append(
                {
                    "doc_id": doc_id,
                    "title": f"runbook for {issue['doc_issue']}",
                    "summary": (
                        f"service {issue['service']} in region {doc_region}. "
                        f"severity {doc_severity}. use this operational guide to mitigate customer impact."
                    ),
                    "bucket": region_index,
                    "source_updated_at": ordinal,
                }
            )
            queries.append(
                {
                    "doc_id": doc_id,
                    "text": (
                        f"How do I handle {query_severity} {query_region} incident where "
                        f"{issue['query_issue']} on {issue['service']}?"
                    ),
                }
            )
            ordinal += 1
    return docs, queries


def _evaluate_provider(
    provider_name: str,
    *,
    provider,
    model: str,
    batch_size: int,
    top_k: int,
    metric: str,
    query_limit: int,
) -> dict[str, object]:
    docs, queries = _build_docs_and_queries()
    if query_limit > 0:
        queries = queries[:query_limit]

    rows = build_embedding_rows(docs, template_version="text-v1")

    with tempfile.TemporaryDirectory(prefix=f"velaria-recall-{provider_name}-") as tmp:
        path = pathlib.Path(tmp) / "docs_embeddings.parquet"
        t0 = time.perf_counter()
        materialize_embeddings(
            rows,
            provider=provider,
            model=model,
            output_path=path,
            batch_size=batch_size,
        )
        t1 = time.perf_counter()

        session = Session()
        df = load_embedding_dataframe(session, path)
        session.create_temp_view("docs_recall_bench", df)

        recall1 = 0
        recall5 = 0
        mrr = 0.0
        embed_ms = 0.0
        search_ms = 0.0
        failures = []
        for item in queries:
            t2 = time.perf_counter()
            query_vector = embed_query_text(item["text"], provider=provider, model=model)
            t3 = time.perf_counter()
            result = session.hybrid_search(
                table="docs_recall_bench",
                vector_column="embedding",
                query_vector=query_vector,
                top_k=top_k,
                metric=metric,
            ).to_arrow().to_pylist()
            t4 = time.perf_counter()
            embed_ms += (t3 - t2) * 1000.0
            search_ms += (t4 - t3) * 1000.0

            ranked = [row["doc_id"] for row in result]
            target = item["doc_id"]
            if ranked and ranked[0] == target:
                recall1 += 1
            if target in ranked:
                recall5 += 1
                mrr += 1.0 / (ranked.index(target) + 1)
            elif len(failures) < 5:
                failures.append({"target": target, "top_k": ranked, "query": item["text"]})

        count = len(queries)
        return {
            "bench": "embedding-recall",
            "provider": provider.provider_name,
            "provider_key": provider_name,
            "model": model,
            "documents": len(docs),
            "queries": count,
            "top_k": top_k,
            "metric": metric,
            "batch_embedding_ms": round((t1 - t0) * 1000.0, 3),
            "batch_docs_per_sec": round(len(docs) / max(t1 - t0, 1e-9), 3),
            "avg_query_embed_ms": round(embed_ms / max(count, 1), 3),
            "avg_query_search_ms": round(search_ms / max(count, 1), 3),
            "recall_at_1": round(recall1 / max(count, 1), 4),
            "recall_at_5": round(recall5 / max(count, 1), 4),
            "mrr_at_5": round(mrr / max(count, 1), 4),
            "sample_failures": failures,
        }


def main():
    args = _parse_args()
    providers = [item.strip() for item in args.providers.split(",") if item.strip()]
    for provider_name in providers:
        provider = _make_provider(provider_name, dimension=args.dimension, model=args.model)
        payload = _evaluate_provider(
            provider_name,
            provider=provider,
            model=args.model if provider_name == "minilm" else "hash-demo",
            batch_size=args.batch_size,
            top_k=args.top_k,
            metric=args.metric,
            query_limit=args.query_limit,
        )
        print(json.dumps(payload))


if __name__ == "__main__":
    main()
