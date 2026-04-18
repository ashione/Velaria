import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from velaria import (
    EmbeddingProvider,
    DEFAULT_LOCAL_EMBEDDING_MODEL,
    DEFAULT_EMBEDDING_WARMUP_TEXT,
    DEFAULT_LOCAL_EMBEDDING_MODEL_DIR,
    EMBEDDING_CACHE_DIR_ENV,
    EMBEDDING_MODEL_DIR_ENV,
    HashEmbeddingProvider,
    Session,
    SentenceTransformerEmbeddingProvider,
    build_embedding_rows,
    build_file_embeddings,
    build_mixed_text_embedding_rows,
    default_embedding_model_dir,
    download_embedding_model,
    embed_query_text,
    format_embedding_version,
    is_embedding_model_ready,
    load_embedding_dataframe,
    materialize_embeddings,
    materialize_mixed_text_embeddings,
    materialize_mixed_text_embeddings_stream,
    query_file_embeddings,
    read_embedding_table,
    render_text_template,
    resolve_embedding_model_name,
    run_mixed_text_hybrid_search,
    run_file_mixed_text_hybrid_search,
    select_embedding_updates,
    stream_mixed_text_embeddings_to_parquet,
)


class StaticEmbeddingProvider(EmbeddingProvider):
    provider_name = "static"

    def __init__(self, mapping):
        self._mapping = mapping

    def embed(self, texts, *, model, batch_size=None):
        del model, batch_size
        out = []
        for text in texts:
            if text not in self._mapping:
                raise RuntimeError(f"missing embedding for text: {text}")
            out.append(list(self._mapping[text]))
        return out


class RaisingEmbeddingProvider(EmbeddingProvider):
    provider_name = "raising"

    def embed(self, texts, *, model, batch_size=None):
        del texts, model, batch_size
        raise RuntimeError("embedding provider failed")


class RecordingEmbeddingProvider(EmbeddingProvider):
    provider_name = "recording"

    def __init__(self, mapping):
        self._mapping = mapping
        self.calls = []

    def embed(self, texts, *, model, batch_size=None):
        self.calls.append(
            {
                "texts": list(texts),
                "model": model,
                "batch_size": batch_size,
            }
        )
        return [list(self._mapping[text]) for text in texts]


class EmbeddingPipelineTest(unittest.TestCase):
    def _install_fake_sentence_transformers(self, vector_map):
        captured = {}

        class FakeSentenceTransformer:
            def __init__(self, model_name, **kwargs):
                captured["model_name"] = model_name
                captured["kwargs"] = kwargs

            def encode(self, texts, **kwargs):
                captured["texts"] = list(texts)
                captured["encode_kwargs"] = kwargs
                return [list(vector_map[text]) for text in texts]

        fake_module = types.SimpleNamespace(SentenceTransformer=FakeSentenceTransformer)
        original = sys.modules.get("sentence_transformers")
        sys.modules["sentence_transformers"] = fake_module
        return captured, original

    def _restore_module(self, name, original):
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original

    def test_sentence_transformer_provider_uses_all_minilm_default(self):
        captured, original = self._install_fake_sentence_transformers(
            {
                "alpha": [1.0, 0.0, 0.0],
                "beta": [1.0, 0.0, 0.0],
            }
        )
        with tempfile.TemporaryDirectory(prefix="velaria-no-local-model-") as tmp:
            fake_local = pathlib.Path(tmp) / "missing-model"
            self.assertFalse(fake_local.exists())
            try:
                with mock.patch("velaria.embedding.DEFAULT_LOCAL_EMBEDDING_MODEL_DIR", fake_local):
                    provider = SentenceTransformerEmbeddingProvider()
                out = provider.embed(["alpha", "beta"], model=DEFAULT_LOCAL_EMBEDDING_MODEL, batch_size=8)
            finally:
                self._restore_module("sentence_transformers", original)

        self.assertEqual(captured["model_name"], DEFAULT_LOCAL_EMBEDDING_MODEL)
        self.assertEqual(captured["texts"], ["alpha", "beta"])
        self.assertEqual(captured["encode_kwargs"]["batch_size"], 8)
        self.assertTrue(captured["encode_kwargs"]["normalize_embeddings"])
        self.assertEqual(out, [[1.0, 0.0, 0.0], [1.0, 0.0, 0.0]])

    def test_sentence_transformer_provider_prefers_local_model_dir(self):
        captured, original = self._install_fake_sentence_transformers({"alpha": [1.0, 0.0, 0.0]})
        with tempfile.TemporaryDirectory(prefix="velaria-local-model-") as tmp:
            fake_local = pathlib.Path(tmp) / "all-MiniLM-L6-v2"
            fake_local.mkdir()
            try:
                with mock.patch("velaria.embedding.DEFAULT_LOCAL_EMBEDDING_MODEL_DIR", fake_local):
                    provider = SentenceTransformerEmbeddingProvider()
                    provider.embed(["alpha"], model=DEFAULT_LOCAL_EMBEDDING_MODEL)
            finally:
                self._restore_module("sentence_transformers", original)
        self.assertEqual(captured["model_name"], str(fake_local))

    def test_sentence_transformer_provider_env_overrides_local_model_dir_and_cache(self):
        captured, original = self._install_fake_sentence_transformers({"alpha": [1.0, 0.0, 0.0]})
        with tempfile.TemporaryDirectory(prefix="velaria-embed-model-") as model_dir, tempfile.TemporaryDirectory(
            prefix="velaria-embed-cache-"
        ) as cache_dir:
            try:
                with mock.patch.dict(
                    "os.environ",
                    {
                        EMBEDDING_MODEL_DIR_ENV: model_dir,
                        EMBEDDING_CACHE_DIR_ENV: cache_dir,
                    },
                    clear=False,
                ):
                    provider = SentenceTransformerEmbeddingProvider()
                    provider.embed(["alpha"], model=DEFAULT_LOCAL_EMBEDDING_MODEL)
            finally:
                self._restore_module("sentence_transformers", original)
        self.assertEqual(captured["model_name"], model_dir)
        self.assertEqual(captured["kwargs"]["cache_folder"], cache_dir)

    def test_resolve_embedding_model_name_prefers_env_then_repo_local(self):
        with tempfile.TemporaryDirectory(prefix="velaria-embed-model-") as model_dir:
            with mock.patch.dict("os.environ", {EMBEDDING_MODEL_DIR_ENV: model_dir}, clear=False):
                self.assertEqual(resolve_embedding_model_name(DEFAULT_LOCAL_EMBEDDING_MODEL), model_dir)
        with tempfile.TemporaryDirectory(prefix="velaria-local-model-") as tmp:
            fake_local = pathlib.Path(tmp) / "all-MiniLM-L6-v2"
            fake_local.mkdir()
            with mock.patch.dict("os.environ", {}, clear=True):
                with mock.patch("velaria.embedding.DEFAULT_LOCAL_EMBEDDING_MODEL_DIR", fake_local):
                    self.assertEqual(
                        resolve_embedding_model_name(DEFAULT_LOCAL_EMBEDDING_MODEL),
                        str(fake_local),
                    )
        with tempfile.TemporaryDirectory(prefix="velaria-no-local-model-") as tmp:
            fake_local = pathlib.Path(tmp) / "missing-model"
            with mock.patch.dict("os.environ", {}, clear=True):
                with mock.patch("velaria.embedding.DEFAULT_LOCAL_EMBEDDING_MODEL_DIR", fake_local):
                    self.assertEqual(
                        resolve_embedding_model_name(DEFAULT_LOCAL_EMBEDDING_MODEL),
                        DEFAULT_LOCAL_EMBEDDING_MODEL,
                    )

    def test_sentence_transformer_provider_integrates_with_pipeline_and_hybrid_search(self):
        captured, original = self._install_fake_sentence_transformers(
            {
                "alpha doc": [1.0, 0.0, 0.0],
                "beta doc": [0.0, 1.0, 0.0],
                "gamma doc": [0.8, 0.2, 0.0],
            }
        )
        try:
            provider = SentenceTransformerEmbeddingProvider(
                model_name=DEFAULT_LOCAL_EMBEDDING_MODEL,
                default_batch_size=2,
            )
            rows = [
                {
                    "doc_id": "doc-1",
                    "text": "alpha doc",
                    "bucket": 1,
                    "text_template_version": "text-v1",
                    "source_updated_at": 1,
                },
                {
                    "doc_id": "doc-2",
                    "text": "beta doc",
                    "bucket": 2,
                    "text_template_version": "text-v1",
                    "source_updated_at": 2,
                },
                {
                    "doc_id": "doc-3",
                    "text": "gamma doc",
                    "bucket": 1,
                    "text_template_version": "text-v1",
                    "source_updated_at": 3,
                },
            ]
            session = Session()
            with tempfile.TemporaryDirectory(prefix="velaria-minilm-integration-") as tmp:
                path = pathlib.Path(tmp) / "docs_embeddings.parquet"
                materialize_embeddings(
                    rows,
                    provider=provider,
                    model=DEFAULT_LOCAL_EMBEDDING_MODEL,
                    output_path=path,
                    batch_size=2,
                )
                self.assertEqual(captured["model_name"], DEFAULT_LOCAL_EMBEDDING_MODEL)
                self.assertEqual(captured["encode_kwargs"]["batch_size"], 2)
                self.assertTrue(captured["encode_kwargs"]["normalize_embeddings"])

                df = load_embedding_dataframe(session, path)
                session.create_temp_view("docs_embed_minilm_py", df)
                query_vector = embed_query_text(
                    "alpha doc",
                    provider=provider,
                    model=DEFAULT_LOCAL_EMBEDDING_MODEL,
                )
                result = session.hybrid_search(
                    table="docs_embed_minilm_py",
                    vector_column="embedding",
                    query_vector=query_vector,
                    top_k=2,
                    metric="cosine",
                ).to_arrow()
                self.assertEqual(result.column("doc_id").to_pylist()[0], "doc-1")

                sql_rows = session.sql(
                    "SELECT doc_id, bucket, vector_score "
                    "FROM docs_embed_minilm_py "
                    "WHERE bucket = 1 "
                    "HYBRID SEARCH embedding QUERY '[1 0 0]' METRIC cosine TOP_K 2"
                ).to_rows()
                self.assertEqual(sql_rows["rows"][0][0], "doc-1")
        finally:
            self._restore_module("sentence_transformers", original)

    def test_hash_provider_warmup_is_noop_metadata(self):
        provider = HashEmbeddingProvider(dimension=12)
        payload = provider.warmup()
        self.assertEqual(payload["provider"], "hash")
        self.assertEqual(payload["model"], "hash-demo")
        self.assertEqual(payload["dimension"], 12)
        self.assertEqual(payload["texts"], 0)

    def test_download_embedding_model_uses_existing_local_dir(self):
        with tempfile.TemporaryDirectory(prefix="velaria-embed-local-") as tmp:
            model_dir = pathlib.Path(tmp)
            for name in ("config.json", "modules.json", "tokenizer.json"):
                (model_dir / name).write_text("{}", encoding="utf-8")
            self.assertTrue(is_embedding_model_ready(model_dir))
            with mock.patch("velaria.embedding.importlib.import_module") as import_module:
                resolved = download_embedding_model(DEFAULT_LOCAL_EMBEDDING_MODEL, target_dir=model_dir)
            self.assertEqual(resolved, model_dir)
            import_module.assert_not_called()

    def test_download_embedding_model_calls_hub_when_missing(self):
        original = sys.modules.get("huggingface_hub")
        captured = {}
        fake_hub = types.SimpleNamespace(
            snapshot_download=lambda **kwargs: captured.update(kwargs),
        )
        sys.modules["huggingface_hub"] = fake_hub
        with tempfile.TemporaryDirectory(prefix="velaria-embed-dl-") as tmp:
            target = pathlib.Path(tmp) / "all-MiniLM-L6-v2"
            resolved = download_embedding_model(DEFAULT_LOCAL_EMBEDDING_MODEL, target_dir=target)
        self._restore_module("huggingface_hub", original)
        self.assertEqual(resolved, target)
        self.assertEqual(captured["repo_id"], DEFAULT_LOCAL_EMBEDDING_MODEL)
        self.assertEqual(captured["local_dir"], str(target))

    def test_sentence_transformer_warmup_downloads_and_encodes(self):
        st_captured, st_original = self._install_fake_sentence_transformers(
            {DEFAULT_EMBEDDING_WARMUP_TEXT: [1.0, 0.0, 0.0]}
        )
        hub_original = sys.modules.get("huggingface_hub")
        hub_captured = {}
        sys.modules["huggingface_hub"] = types.SimpleNamespace(
            snapshot_download=lambda **kwargs: hub_captured.update(kwargs),
        )
        with tempfile.TemporaryDirectory(prefix="velaria-embed-dl-") as tmp:
            target = pathlib.Path(tmp) / "all-MiniLM-L6-v2"
            provider = SentenceTransformerEmbeddingProvider(model_name=DEFAULT_LOCAL_EMBEDDING_MODEL)
            with mock.patch("velaria.embedding.is_embedding_model_ready", return_value=False):
                payload = provider.warmup(download_if_missing=True, target_dir=target)
        self._restore_module("sentence_transformers", st_original)
        self._restore_module("huggingface_hub", hub_original)
        self.assertEqual(hub_captured["repo_id"], DEFAULT_LOCAL_EMBEDDING_MODEL)
        self.assertEqual(hub_captured["local_dir"], str(target))
        self.assertEqual(st_captured["model_name"], str(target))
        self.assertEqual(st_captured["texts"], [DEFAULT_EMBEDDING_WARMUP_TEXT])
        self.assertEqual(payload["dimension"], 3)

    def test_sentence_transformer_provider_missing_dependency_is_actionable(self):
        original = sys.modules.get("sentence_transformers")
        sys.modules.pop("sentence_transformers", None)
        provider = SentenceTransformerEmbeddingProvider()
        import importlib as _importlib

        real_import_module = _importlib.import_module

        def _missing(name, package=None):
            if name == "sentence_transformers":
                raise ImportError("missing")
            return real_import_module(name, package)

        try:
            with mock.patch("velaria.embedding.importlib.import_module", side_effect=_missing):
                with self.assertRaisesRegex(ImportError, "uv sync --project python_api --extra embedding"):
                    provider.embed(["alpha"], model=DEFAULT_LOCAL_EMBEDDING_MODEL)
        finally:
            self._restore_module("sentence_transformers", original)

    def test_hash_provider_is_deterministic_and_dimension_stable(self):
        provider = HashEmbeddingProvider(dimension=8)
        first = provider.embed(["alpha", "beta"], model="hash-demo", batch_size=1)
        second = provider.embed(["alpha", "beta"], model="hash-demo", batch_size=4)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 2)
        self.assertEqual(len(first[0]), 8)

    def test_render_text_template_prefers_text_and_has_stable_field_order(self):
        self.assertEqual(render_text_template({"text": "alpha"}), "alpha")
        rendered = render_text_template(
            {
                "title": "Alpha",
                "summary": "Guide",
                "tags": ["ops", "apac"],
                "body": "Steps",
            }
        )
        self.assertEqual(
            rendered,
            "title: Alpha\nsummary: Guide\nbody: Steps\ntags: ops, apac",
        )

    def test_build_embedding_rows_canonicalizes_fields(self):
        rows = build_embedding_rows(
            [
                {
                    "id": "doc-1",
                    "title": "Alpha",
                    "summary": "Guide",
                    "bucket": 1,
                    "updated_at": "2026-04-09T00:00:00Z",
                }
            ],
            template_version="text-v1",
            doc_id_field="id",
            source_updated_at_field="updated_at",
        )
        self.assertEqual(rows[0]["doc_id"], "doc-1")
        self.assertEqual(rows[0]["text_template_version"], "text-v1")
        self.assertEqual(rows[0]["source_updated_at"], "2026-04-09T00:00:00Z")
        self.assertEqual(rows[0]["bucket"], 1)
        self.assertEqual(rows[0]["text"], "title: Alpha\nsummary: Guide")

    def test_build_embedding_rows_stringifies_sequence_metadata_fields(self):
        rows = build_embedding_rows(
            [
                {
                    "doc_id": "doc-1",
                    "title": "Alpha",
                    "tags": ["ops", "apac"],
                    "source_updated_at": 1,
                }
            ],
            template_version="text-v1",
        )
        self.assertEqual(rows[0]["tags"], "ops, apac")

    def test_build_mixed_text_embedding_rows_uses_explicit_text_fields(self):
        rows = build_mixed_text_embedding_rows(
            [
                {
                    "doc_id": "doc-1",
                    "title": "Alpha",
                    "summary": "Guide",
                    "tags": ["ops", "apac"],
                    "bucket": 1,
                    "source_updated_at": 1,
                }
            ],
            template_version="text-v2",
            text_fields=("title", "summary", "tags"),
        )
        self.assertEqual(
            rows[0]["text"],
            "title: Alpha\nsummary: Guide\ntags: ops, apac",
        )
        self.assertEqual(rows[0]["bucket"], 1)

    def test_select_embedding_updates_uses_version_and_source_updated_at(self):
        records = [
            {"doc_id": "a", "source_updated_at": 1},
            {"doc_id": "b", "source_updated_at": 2},
            {"doc_id": "c", "source_updated_at": 3},
        ]
        existing = pa.table(
            {
                "doc_id": ["a", "b"],
                "source_updated_at": [1, 1],
                "embedding_version": ["text-v1__m__dim-3", "text-v1__m__dim-3"],
            }
        )
        updates = select_embedding_updates(
            records,
            existing,
            embedding_version="text-v1__m__dim-3",
        )
        self.assertEqual([item["doc_id"] for item in updates], ["b", "c"])

        version_updates = select_embedding_updates(
            records,
            existing,
            embedding_version="text-v2__m__dim-3",
        )
        self.assertEqual([item["doc_id"] for item in version_updates], ["a", "b", "c"])

    def test_materialize_embeddings_writes_fixed_size_list_parquet(self):
        provider = StaticEmbeddingProvider(
            {
                "alpha": [1.0, 0.0, 0.0],
                "beta": [0.0, 1.0, 0.0],
            }
        )
        rows = [
            {
                "doc_id": "a",
                "text": "alpha",
                "bucket": 1,
                "text_template_version": "text-v1",
                "source_updated_at": 1,
            },
            {
                "doc_id": "b",
                "text": "beta",
                "bucket": 2,
                "text_template_version": "text-v1",
                "source_updated_at": 2,
            },
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-embedding-pipeline-") as tmp:
            path = pathlib.Path(tmp) / "docs_embeddings.parquet"
            table = materialize_embeddings(
                rows,
                provider=provider,
                model="static-demo",
                output_path=path,
            )
            self.assertEqual(
                format_embedding_version(
                    template_version="text-v1",
                    model_name="static-demo",
                    dimension=3,
                ),
                table.column("embedding_version")[0].as_py(),
            )
            self.assertEqual(table.schema.field("embedding").type, pa.list_(pa.float32(), 3))
            parquet_table = pq.read_table(path)
            self.assertEqual(parquet_table.schema.field("embedding").type, pa.list_(pa.float32(), 3))
            self.assertEqual(parquet_table.column("doc_id").to_pylist(), ["a", "b"])

    def test_materialize_embeddings_provider_error_propagates(self):
        with self.assertRaisesRegex(RuntimeError, "embedding provider failed"):
            materialize_embeddings(
                [
                    {
                        "doc_id": "a",
                        "text": "alpha",
                        "text_template_version": "text-v1",
                        "source_updated_at": 1,
                    }
                ],
                provider=RaisingEmbeddingProvider(),
                model="broken",
            )

    def test_embedding_dataset_integrates_with_hybrid_search_and_sql(self):
        provider = StaticEmbeddingProvider(
            {
                "alpha doc": [1.0, 0.0, 0.0],
                "beta doc": [0.0, 1.0, 0.0],
                "gamma doc": [0.8, 0.2, 0.0],
            }
        )
        rows = [
            {
                "doc_id": "doc-1",
                "text": "alpha doc",
                "title": "Alpha",
                "bucket": 1,
                "text_template_version": "text-v1",
                "source_updated_at": 1,
            },
            {
                "doc_id": "doc-2",
                "text": "beta doc",
                "title": "Beta",
                "bucket": 2,
                "text_template_version": "text-v1",
                "source_updated_at": 2,
            },
            {
                "doc_id": "doc-3",
                "text": "gamma doc",
                "title": "Gamma",
                "bucket": 1,
                "text_template_version": "text-v1",
                "source_updated_at": 3,
            },
        ]
        session = Session()
        with tempfile.TemporaryDirectory(prefix="velaria-embedding-integration-") as tmp:
            path = pathlib.Path(tmp) / "docs_embeddings.parquet"
            materialize_embeddings(
                rows,
                provider=provider,
                model="static-demo",
                output_path=path,
            )
            df = load_embedding_dataframe(session, path)
            session.create_temp_view("docs_embed_py", df)

            query_vector = embed_query_text("alpha doc", provider=provider, model="static-demo")
            result = session.hybrid_search(
                table="docs_embed_py",
                vector_column="embedding",
                query_vector=query_vector,
                top_k=2,
                metric="cosine",
            ).to_arrow()
            self.assertEqual(result.column("doc_id").to_pylist()[0], "doc-1")

            sql_rows = session.sql(
                "SELECT doc_id, bucket, vector_score "
                "FROM docs_embed_py "
                "WHERE bucket = 1 "
                "HYBRID SEARCH embedding QUERY '[1 0 0]' METRIC cosine TOP_K 2"
            ).to_rows()
            self.assertEqual(sql_rows["rows"][0][0], "doc-1")
            self.assertEqual(sql_rows["schema"], ["doc_id", "bucket", "vector_score"])

            reloaded = read_embedding_table(path)
            self.assertEqual(reloaded.column("model_name").to_pylist(), ["static-demo"] * 3)

    def test_mixed_text_pipeline_runs_embedding_then_query_then_hybrid_search(self):
        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout\ntags: billing, checkout": [1.0, 0.0, 0.0],
                "title: Beta\nsummary: Refund delay in worker queue\ntags: refund, queue": [0.0, 1.0, 0.0],
                "title: Gamma\nsummary: Checkout retry storm\ntags: billing, retry": [0.8, 0.2, 0.0],
                "payment page hangs during checkout": [1.0, 0.0, 0.0],
            }
        )
        records = [
            {
                "doc_id": "doc-1",
                "title": "Alpha",
                "summary": "Payment page timeout",
                "tags": ["billing", "checkout"],
                "bucket": 1,
                "source_updated_at": 1,
            },
            {
                "doc_id": "doc-2",
                "title": "Beta",
                "summary": "Refund delay in worker queue",
                "tags": ["refund", "queue"],
                "bucket": 2,
                "source_updated_at": 2,
            },
            {
                "doc_id": "doc-3",
                "title": "Gamma",
                "summary": "Checkout retry storm",
                "tags": ["billing", "retry"],
                "bucket": 1,
                "source_updated_at": 3,
            },
        ]
        session = Session()
        with tempfile.TemporaryDirectory(prefix="velaria-mixed-pipeline-") as tmp:
            path = pathlib.Path(tmp) / "docs_embeddings.parquet"
            table = materialize_mixed_text_embeddings(
                records,
                provider=provider,
                model="static-demo",
                template_version="text-v1",
                text_fields=("title", "summary", "tags"),
                output_path=path,
            )
            self.assertEqual(table.schema.field("embedding").type, pa.list_(pa.float32(), 3))
            result = run_mixed_text_hybrid_search(
                session,
                path,
                provider=provider,
                model="static-demo",
                query_text="payment page hangs during checkout",
                top_k=2,
                metric="cosine",
                where_sql="bucket = 1 AND doc_id = 'doc-1'",
            ).to_rows()
            doc_id_index = result["schema"].index("doc_id")
            self.assertEqual(result["rows"][0][doc_id_index], "doc-1")

    def test_materialize_mixed_text_embeddings_stream_batches_provider_calls(self):
        provider = RecordingEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout": [1.0, 0.0, 0.0],
                "title: Beta\nsummary: Refund delay in worker queue": [0.0, 1.0, 0.0],
                "title: Gamma\nsummary: Checkout retry storm": [0.8, 0.2, 0.0],
            }
        )
        records = [
            {"doc_id": "doc-1", "title": "Alpha", "summary": "Payment page timeout", "source_updated_at": 1},
            {"doc_id": "doc-2", "title": "Beta", "summary": "Refund delay in worker queue", "source_updated_at": 2},
            {"doc_id": "doc-3", "title": "Gamma", "summary": "Checkout retry storm", "source_updated_at": 3},
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-stream-embed-") as tmp:
            output_path = pathlib.Path(tmp) / "bitable_embeddings.parquet"
            table = materialize_mixed_text_embeddings_stream(
                records,
                provider=provider,
                model="static-demo",
                template_version="text-v1",
                text_fields=("title", "summary"),
                batch_size=2,
                output_path=output_path,
            )
            self.assertTrue(output_path.exists())

        self.assertEqual(table.column("doc_id").to_pylist(), ["doc-1", "doc-2", "doc-3"])
        self.assertEqual([len(call["texts"]) for call in provider.calls], [2, 1])

    def test_materialize_mixed_text_embeddings_stream_accepts_arrow_table(self):
        provider = RecordingEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout": [1.0, 0.0, 0.0],
                "title: Beta\nsummary: Refund delay in worker queue": [0.0, 1.0, 0.0],
                "title: Gamma\nsummary: Checkout retry storm": [0.8, 0.2, 0.0],
            }
        )
        source = pa.table(
            {
                "doc_id": ["doc-1", "doc-2", "doc-3"],
                "title": ["Alpha", "Beta", "Gamma"],
                "summary": [
                    "Payment page timeout",
                    "Refund delay in worker queue",
                    "Checkout retry storm",
                ],
                "source_updated_at": [1, 2, 3],
            }
        )
        table = materialize_mixed_text_embeddings_stream(
            source,
            provider=provider,
            model="static-demo",
            template_version="text-v1",
            text_fields=("title", "summary"),
            batch_size=2,
        )
        self.assertEqual(table.column("doc_id").to_pylist(), ["doc-1", "doc-2", "doc-3"])
        self.assertEqual([len(call["texts"]) for call in provider.calls], [2, 1])

    def test_stream_mixed_text_embeddings_to_parquet_requires_parquet_suffix(self):
        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout": [1.0, 0.0, 0.0],
            }
        )
        records = [
            {"doc_id": "doc-1", "title": "Alpha", "summary": "Payment page timeout", "source_updated_at": 1},
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-stream-embed-suffix-") as tmp:
            output_path = pathlib.Path(tmp) / "bitable_embeddings.arrow"
            with self.assertRaisesRegex(
                ValueError, "stream_mixed_text_embeddings_to_parquet requires a \\.parquet output path"
            ):
                stream_mixed_text_embeddings_to_parquet(
                    records,
                    provider=provider,
                    model="static-demo",
                    template_version="text-v1",
                    text_fields=("title", "summary"),
                    output_path=output_path,
                )

    def test_build_file_embeddings_reads_csv_and_materializes_embedding_table(self):
        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout\ntags: billing, checkout": [1.0, 0.0, 0.0],
                "title: Beta\nsummary: Refund delay in worker queue\ntags: refund, queue": [0.0, 1.0, 0.0],
            }
        )
        session = Session()
        with tempfile.TemporaryDirectory(prefix="velaria-file-embed-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "doc_id,title,summary,tags,bucket,source_updated_at\n"
                "doc-1,Alpha,Payment page timeout,\"billing, checkout\",1,1\n"
                "doc-2,Beta,Refund delay in worker queue,\"refund, queue\",2,2\n",
                encoding="utf-8",
            )
            out_path = pathlib.Path(tmp) / "docs_embeddings.parquet"
            table = build_file_embeddings(
                session,
                csv_path,
                provider=provider,
                model="static-demo",
                template_version="text-v1",
                input_type="csv",
                text_columns=("title", "summary", "tags"),
                output_path=out_path,
            )
            self.assertEqual(table.column("doc_id").to_pylist(), ["doc-1", "doc-2"])
            self.assertEqual(table.schema.field("embedding").type, pa.list_(pa.float32(), 3))

    def test_build_file_embeddings_reads_parquet_and_materializes_embedding_table(self):
        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout\ntags: billing, checkout": [1.0, 0.0, 0.0],
                "title: Beta\nsummary: Refund delay in worker queue\ntags: refund, queue": [0.0, 1.0, 0.0],
            }
        )
        session = Session()
        with tempfile.TemporaryDirectory(prefix="velaria-file-embed-parquet-") as tmp:
            parquet_path = pathlib.Path(tmp) / "docs.parquet"
            pq.write_table(
                pa.table(
                    {
                        "doc_id": ["doc-1", "doc-2"],
                        "title": ["Alpha", "Beta"],
                        "summary": ["Payment page timeout", "Refund delay in worker queue"],
                        "tags": ["billing, checkout", "refund, queue"],
                        "source_updated_at": [1, 2],
                    }
                ),
                parquet_path,
            )
            table = build_file_embeddings(
                session,
                parquet_path,
                provider=provider,
                model="static-demo",
                template_version="text-v1",
                input_type="parquet",
                text_columns=("title", "summary", "tags"),
            )
            self.assertEqual(table.column("doc_id").to_pylist(), ["doc-1", "doc-2"])
            self.assertEqual(table.schema.field("embedding").type, pa.list_(pa.float32(), 3))

    def test_run_file_mixed_text_hybrid_search_builds_and_queries_from_csv(self):
        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout\ntags: billing, checkout": [1.0, 0.0, 0.0],
                "title: Beta\nsummary: Refund delay in worker queue\ntags: refund, queue": [0.0, 1.0, 0.0],
                "title: Gamma\nsummary: Checkout retry storm\ntags: billing, retry": [0.8, 0.2, 0.0],
                "payment page hangs during checkout": [1.0, 0.0, 0.0],
            }
        )
        session = Session()
        with tempfile.TemporaryDirectory(prefix="velaria-file-query-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "doc_id,title,summary,tags,bucket,source_updated_at\n"
                "doc-1,Alpha,Payment page timeout,\"billing, checkout\",1,1\n"
                "doc-2,Beta,Refund delay in worker queue,\"refund, queue\",2,2\n"
                "doc-3,Gamma,Checkout retry storm,\"billing, retry\",1,3\n",
                encoding="utf-8",
            )
            result = run_file_mixed_text_hybrid_search(
                session,
                csv_path,
                provider=provider,
                model="static-demo",
                query_text="payment page hangs during checkout",
                template_version="text-v1",
                input_type="csv",
                text_columns=("title", "summary", "tags"),
                where_column="bucket",
                where_op="=",
                where_value=1,
                top_k=2,
                metric="cosine",
            ).to_rows()
            doc_id_index = result["schema"].index("doc_id")
            self.assertEqual(result["rows"][0][doc_id_index], "doc-1")

    def test_run_file_mixed_text_hybrid_search_builds_and_queries_from_line_file(self):
        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout": [1.0, 0.0, 0.0],
                "title: Beta\nsummary: Refund delay in worker queue": [0.0, 1.0, 0.0],
                "payment timeout": [1.0, 0.0, 0.0],
            }
        )
        session = Session()
        with tempfile.TemporaryDirectory(prefix="velaria-file-query-line-") as tmp:
            line_path = pathlib.Path(tmp) / "docs.log"
            line_path.write_text(
                "Alpha|Payment page timeout\n"
                "Beta|Refund delay in worker queue\n",
                encoding="utf-8",
            )
            result = run_file_mixed_text_hybrid_search(
                session,
                line_path,
                provider=provider,
                model="static-demo",
                query_text="payment timeout",
                template_version="text-v1",
                input_type="line",
                mappings="title:0,summary:1",
                line_mode="split",
                delimiter="|",
                text_columns=("title", "summary"),
                top_k=1,
                metric="cosine",
            ).to_rows()
            title_index = result["schema"].index("title")
            self.assertEqual(result["rows"][0][title_index], "Alpha")


if __name__ == "__main__":
    unittest.main()
