import asyncio
import json
import os
import pathlib
import sys
import types
import tempfile
import unittest
from unittest import mock

from velaria.agentic_store import AgenticStore
from velaria.ai_runtime.functions import (
    execute_local_function,
    load_velaria_skill_text,
    tool_definitions,
    velaria_agent_instructions,
)
from velaria.ai_runtime.mcp_server import SKILL_URI, _handle_request


class AiRuntimeAgentTest(unittest.TestCase):
    def test_local_functions_read_sql_and_report_structured_errors(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agent-functions-") as tmp:
            csv_path = pathlib.Path(tmp) / "sales.csv"
            csv_path.write_text("region,amount\ncn,10\nus,7\ncn,5\n", encoding="utf-8")

            preview = execute_local_function("velaria_read", {"path": str(csv_path), "limit": 2})
            self.assertTrue(preview["ok"])
            self.assertEqual(preview["schema"], ["region", "amount"])
            self.assertEqual(len(preview["rows"]), 2)

            result = execute_local_function(
                "velaria_sql",
                {
                    "source_path": str(csv_path),
                    "table_name": "input_table",
                    "query": "SELECT region, SUM(amount) AS total FROM input_table GROUP BY region ORDER BY region",
                },
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["schema"], ["region", "total"])
            self.assertEqual(result["rows"][0]["region"], "cn")

            failure = execute_local_function("missing_function", {})
            self.assertFalse(failure["ok"])
            self.assertIn("unknown Velaria local function", failure["error"])

    def test_dataset_import_and_process_are_first_class_functions(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agent-dataset-") as tmp:
            csv_path = pathlib.Path(tmp) / "sales.csv"
            csv_path.write_text("region,amount\ncn,10\nus,7\ncn,5\n", encoding="utf-8")
            home = pathlib.Path(tmp) / "home"
            with mock.patch.dict(os.environ, {"VELARIA_HOME": str(home)}):
                imported = execute_local_function(
                    "velaria_dataset_import",
                    {
                        "path": str(csv_path),
                        "source_id": "sales",
                        "name": "Sales",
                        "table_name": "sales",
                    },
                )
                self.assertTrue(imported["ok"])
                self.assertEqual(imported["source_id"], "sales")
                self.assertEqual(imported["schema"], ["region", "amount"])
                with AgenticStore() as store:
                    source = store.get_source("sales")
                self.assertEqual(source["spec"]["path"], str(csv_path))

                processed = execute_local_function(
                    "velaria_dataset_process",
                    {
                        "source_path": str(csv_path),
                        "table_name": "input_table",
                        "query": (
                            "SELECT region, SUM(amount) AS total "
                            "FROM input_table GROUP BY region ORDER BY region"
                        ),
                        "save_run": False,
                    },
                )
                self.assertTrue(processed["ok"])
                self.assertEqual(processed["function"], "velaria_dataset_process")
                self.assertEqual(processed["schema"], ["region", "total"])
                self.assertEqual(processed["row_count"], 2)
                self.assertEqual(processed["rows"][0]["region"], "cn")

    def test_dataset_import_downloads_url_to_velaria_workspace_first(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agent-url-dataset-") as tmp:
            source_csv = pathlib.Path(tmp) / "remote-sales.csv"
            source_csv.write_text("region,amount\ncn,10\n", encoding="utf-8")
            workspace = pathlib.Path(tmp) / "runtime"
            home = pathlib.Path(tmp) / "home"

            def fake_urlretrieve(url, target):
                pathlib.Path(target).write_text(source_csv.read_text(encoding="utf-8"), encoding="utf-8")
                return str(target), None

            with mock.patch.dict(
                os.environ,
                {"VELARIA_HOME": str(home), "VELARIA_RUNTIME_WORKSPACE": str(workspace)},
            ):
                with mock.patch("urllib.request.urlretrieve", side_effect=fake_urlretrieve):
                    imported = execute_local_function(
                        "velaria_dataset_import",
                        {
                            "path": "https://example.test/data/sales.csv",
                            "source_id": "remote_sales",
                            "table_name": "sales",
                        },
                    )

            self.assertTrue(imported["ok"])
            self.assertEqual(imported["source_url"], "https://example.test/data/sales.csv")
            self.assertTrue(pathlib.Path(imported["source_path"]).exists())
            self.assertIn(str(workspace / "imports" / "downloads"), imported["source_path"])
            self.assertEqual(imported["schema"], ["region", "amount"])
            with mock.patch.dict(os.environ, {"VELARIA_HOME": str(home)}):
                with AgenticStore() as store:
                    source = store.get_source("remote_sales")
            self.assertEqual(source["spec"]["source_url"], "https://example.test/data/sales.csv")

    def test_dataset_download_is_first_class_url_tool(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agent-url-download-") as tmp:
            source_csv = pathlib.Path(tmp) / "remote-sales.csv"
            source_csv.write_text("region,amount\ncn,10\n", encoding="utf-8")
            workspace = pathlib.Path(tmp) / "runtime"

            def fake_urlretrieve(url, target):
                pathlib.Path(target).write_text(source_csv.read_text(encoding="utf-8"), encoding="utf-8")
                return str(target), None

            with mock.patch.dict(os.environ, {"VELARIA_RUNTIME_WORKSPACE": str(workspace)}):
                with mock.patch("urllib.request.urlretrieve", side_effect=fake_urlretrieve):
                    downloaded = execute_local_function(
                        "velaria_dataset_download",
                        {"url": "https://example.test/data/sales.csv", "limit": 1},
                    )

            self.assertTrue(downloaded["ok"])
            self.assertEqual(downloaded["function"], "velaria_dataset_download")
            self.assertEqual(downloaded["source_url"], "https://example.test/data/sales.csv")
            self.assertIn(str(workspace / "imports" / "downloads"), downloaded["source_path"])
            self.assertEqual(downloaded["schema"], ["region", "amount"])
            self.assertEqual(downloaded["row_count"], 1)
            self.assertEqual(downloaded["rows"], [{"region": "cn", "amount": 10}])

    def test_dataset_process_saved_run_surfaces_run_metadata(self):
        def fake_main(argv):
            print(
                json.dumps(
                    {
                        "ok": True,
                        "run_id": "run_dataset",
                        "result": {
                            "schema": ["region", "total"],
                            "rows": [{"region": "cn", "total": 15}],
                        },
                        "artifacts": [{"artifact_id": "artifact_dataset", "run_id": "run_dataset"}],
                    }
                )
            )
            return 0

        with mock.patch("velaria.cli.main", side_effect=fake_main):
            result = execute_local_function(
                "velaria_dataset_process",
                {
                    "source_path": "/tmp/sales.csv",
                    "query": "SELECT region FROM input_table",
                    "save_run": True,
                },
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["function"], "velaria_dataset_process")
        self.assertEqual(result["run_id"], "run_dataset")
        self.assertEqual(result["artifacts"][0]["artifact_id"], "artifact_dataset")
        self.assertEqual(result["schema"], ["region", "total"])
        self.assertEqual(result["row_count"], 1)

    def test_dataset_process_uses_input_table_when_query_references_it(self):
        captured = {}

        def fake_main(argv):
            captured["argv"] = argv
            print(
                json.dumps(
                    {
                        "ok": True,
                        "run_id": "run_dataset",
                        "result": {"schema": ["region"], "rows": [{"region": "cn"}]},
                    }
                )
            )
            return 0

        with mock.patch("velaria.cli.main", side_effect=fake_main):
            result = execute_local_function(
                "velaria_dataset_process",
                {
                    "source_path": "/tmp/sales.csv",
                    "table_name": "sales",
                    "query": "SELECT region FROM input_table",
                    "save_run": True,
                },
            )
        self.assertTrue(result["ok"])
        table_arg = captured["argv"][captured["argv"].index("--table") + 1]
        self.assertEqual(table_arg, "input_table")
        self.assertEqual(result["table_name"], "input_table")

    def test_cli_function_surfaces_run_and_artifact_metadata(self):
        def fake_main(argv):
            print(
                json.dumps(
                    {
                        "ok": True,
                        "run_id": "run_test",
                        "artifacts": [{"artifact_id": "artifact_test"}],
                    }
                )
            )
            return 0

        with mock.patch("velaria.cli.main", side_effect=fake_main):
            result = execute_local_function("velaria_cli_run", {"argv": ["run", "list"]})
        self.assertTrue(result["ok"])
        self.assertEqual(result["function"], "velaria_cli_run")
        self.assertEqual(result["run_id"], "run_test")
        self.assertEqual(result["artifacts"][0]["artifact_id"], "artifact_test")

    def test_mcp_server_exposes_tools_and_skill_resource(self):
        init = _handle_request({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
        self.assertEqual(init["result"]["serverInfo"]["name"], "velaria")

        tools = _handle_request({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        names = {tool["name"] for tool in tools["result"]["tools"]}
        self.assertIn("velaria_read", names)
        self.assertIn("velaria_sql", names)
        self.assertIn("velaria_dataset_import", names)
        self.assertIn("velaria_dataset_download", names)
        self.assertIn("velaria_dataset_process", names)
        import_tool = next(tool for tool in tools["result"]["tools"] if tool["name"] == "velaria_dataset_import")
        self.assertEqual(import_tool["annotations"]["destructiveHint"], False)
        self.assertEqual(import_tool["annotations"]["openWorldHint"], True)
        self.assertIn("HTTP(S)", import_tool["description"])
        download_tool = next(tool for tool in tools["result"]["tools"] if tool["name"] == "velaria_dataset_download")
        self.assertEqual(download_tool["annotations"]["openWorldHint"], True)
        self.assertIn("url", download_tool["inputSchema"]["required"])

        resource = _handle_request(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "resources/read",
                "params": {"uri": SKILL_URI},
            }
        )
        text = resource["result"]["contents"][0]["text"]
        self.assertIn("Velaria Local Python Skill", text)
        self.assertEqual(text, load_velaria_skill_text())

    def test_mcp_stdio_transport_lists_and_calls_tools(self):
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        async def exercise():
            params = StdioServerParameters(
                command=sys.executable,
                args=["-m", "velaria.ai_runtime.mcp_server"],
                env=dict(os.environ),
            )
            async with stdio_client(params) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    tools = await session.list_tools()
                    names = {tool.name for tool in tools.tools}
                    self.assertIn("velaria_schema", names)
                    self.assertIn("velaria_dataset_download", names)
                    self.assertIn("velaria_dataset_process", names)
                    process_tool = next(tool for tool in tools.tools if tool.name == "velaria_dataset_process")
                    self.assertFalse(process_tool.annotations.destructiveHint)
                    self.assertTrue(process_tool.annotations.openWorldHint)

                    result = await session.call_tool(
                        "velaria_schema",
                        {"table_name": "input_table"},
                    )
                    self.assertFalse(result.isError)
                    payload = json.loads(result.content[0].text)
                    self.assertTrue(payload["ok"])
                    self.assertEqual(payload["function"], "velaria_schema")

                    resource = await session.read_resource(SKILL_URI)
                    self.assertIn("Velaria Local Python Skill", resource.contents[0].text)

        asyncio.run(exercise())

    def test_agent_instructions_reference_skill_without_inlining_it(self):
        instructions = velaria_agent_instructions()
        skill = load_velaria_skill_text()
        tool_names = {tool["name"] for tool in tool_definitions()}
        self.assertIn("velaria_dataset_import", tool_names)
        self.assertIn("velaria_dataset_download", tool_names)
        self.assertIn("velaria_dataset_process", tool_names)
        self.assertIn("You are Velaria Agent", instructions)
        self.assertIn("product identity", instructions)
        self.assertIn("velaria://skills/velaria-python-local", instructions)
        self.assertIn("Available Velaria local functions", instructions)
        self.assertIn("Default workflow policy for data tasks", instructions)
        self.assertIn("first get the data into a Velaria-processable local format", instructions)
        self.assertIn("then call the Velaria local functions", instructions)
        self.assertIn("For HTTP(S) URLs", instructions)
        self.assertIn("Do not write curl, wget, Python download code", instructions)
        self.assertIn("Do not probe `velaria_cli.py --help`", instructions)
        self.assertIn("Do not use web search to discover Velaria tools", instructions)
        self.assertIn("tool_search", instructions)
        self.assertIn("velaria_dataset_download", instructions)
        self.assertIn("velaria_dataset_import", instructions)
        self.assertIn("velaria_dataset_process", instructions)
        self.assertIn("velaria_sql", instructions)
        self.assertNotIn(skill, instructions)
        self.assertNotIn("## 1. 环境准备", instructions)

    def test_codex_runtime_config_injects_velaria_catalog_and_mcp(self):
        from velaria.ai_runtime.codex_runtime import CodexRuntime

        class FakeClient:
            instances = []

            @classmethod
            def connect_stdio(cls, **kwargs):
                inst = cls()
                inst.connect_kwargs = kwargs
                inst.started_config = None
                cls.instances.append(inst)
                return inst

            async def start(self):
                return None

            async def start_thread(self, config):
                self.started_config = config
                return types.SimpleNamespace(thread_id="codex-thread-1")

            async def close(self):
                return None

        with tempfile.TemporaryDirectory(prefix="velaria-codex-runtime-") as tmp:
            local_codex_home = pathlib.Path(tmp) / "local-codex"
            local_codex_home.mkdir()
            (local_codex_home / "auth.json").write_text('{"mode":"test"}', encoding="utf-8")
            (local_codex_home / "config.toml").write_text(
                '[plugins."github@openai-curated"]\nenabled = true\n',
                encoding="utf-8",
            )
            skill_dir = pathlib.Path(tmp) / "skill-dir"
            skill_file = skill_dir / "velaria_python_local" / "SKILL.md"
            skill_file.parent.mkdir(parents=True)
            skill_file.write_text("# Configured Velaria Skill\n", encoding="utf-8")
            try:
                runtime = CodexRuntime(
                    runtime_workspace=tmp,
                    skill_dir=str(skill_dir),
                    cwd=str(pathlib.Path.cwd()),
                    proxy_env={
                        "http_proxy": "http://127.0.0.1:7897",
                        "https_proxy": "http://127.0.0.1:7897",
                        "all_proxy": "socks5://127.0.0.1:7897",
                    },
                )
            except ImportError as exc:
                raise unittest.SkipTest("codex-app-server-sdk is not installed") from exc
            runtime._client_cls = FakeClient
            try:
                with mock.patch.dict(os.environ, {"CODEX_HOME": str(local_codex_home)}):
                    session_id = asyncio.run(runtime.start_thread({}))
                self.assertTrue(session_id.startswith("ai_session_"))
                fake = FakeClient.instances[0]
                runtime_cwd = str(pathlib.Path(tmp) / "workspace")
                self.assertEqual(fake.connect_kwargs["cwd"], runtime_cwd)
                self.assertEqual(fake.connect_kwargs["env"]["HOME"], str(pathlib.Path(tmp)))
                self.assertEqual(fake.connect_kwargs["env"]["CODEX_HOME"], str(pathlib.Path(tmp) / ".codex"))
                self.assertEqual(
                    fake.connect_kwargs["env"]["UV_CACHE_DIR"],
                    str(pathlib.Path(tmp) / "workspace" / ".cache" / "uv"),
                )
                self.assertEqual(fake.connect_kwargs["env"]["VELARIA_HOME"], str(pathlib.Path(tmp)))
                self.assertEqual(fake.connect_kwargs["env"]["VELARIA_WORKSPACE"], str(pathlib.Path(tmp)))
                self.assertEqual(fake.connect_kwargs["env"]["VELARIA_RUNTIME_WORKSPACE"], str(pathlib.Path(tmp)))
                self.assertEqual(fake.connect_kwargs["env"]["http_proxy"], "http://127.0.0.1:7897")
                self.assertEqual(fake.connect_kwargs["env"]["HTTPS_PROXY"], "http://127.0.0.1:7897")
                self.assertEqual(fake.connect_kwargs["env"]["all_proxy"], "socks5://127.0.0.1:7897")
                self.assertEqual(fake.connect_kwargs["env"]["no_proxy"], "127.0.0.1,localhost,::1")
                isolated_config = pathlib.Path(fake.connect_kwargs["env"]["CODEX_HOME"]) / "config.toml"
                isolated_config_text = isolated_config.read_text(encoding="utf-8")
                self.assertIn('model = "gpt-5.4-mini"', isolated_config_text)
                self.assertIn('model_reasoning_effort = "none"', isolated_config_text)
                self.assertIn("[sandbox_workspace_write]", isolated_config_text)
                self.assertIn("network_access = true", isolated_config_text)
                self.assertNotIn("plugins", isolated_config_text)
                self.assertEqual(
                    json.loads((pathlib.Path(fake.connect_kwargs["env"]["CODEX_HOME"]) / "auth.json").read_text()),
                    {"mode": "test"},
                )
                config = fake.started_config
                self.assertEqual(config.model, "gpt-5.4-mini")
                self.assertEqual(config.cwd, runtime_cwd)
                self.assertEqual(config.config["model_reasoning_effort"], "none")
                self.assertEqual(config.config["sandbox_workspace_write"]["network_access"], True)
                self.assertIn("You are Velaria Agent", config.base_instructions)
                self.assertIn("You are Velaria Agent", config.developer_instructions)
                self.assertIn("velaria://skills/velaria-python-local", config.developer_instructions)
                self.assertIn("velaria_sql", config.developer_instructions)
                self.assertNotIn("## 1. 环境准备", config.developer_instructions)
                self.assertIn("mcp_servers", config.config)
                self.assertIn("velaria", config.config["mcp_servers"])
                mcp = config.config["mcp_servers"]["velaria"]
                self.assertEqual(mcp["args"], ["-m", "velaria.ai_runtime.mcp_server"])
                self.assertEqual(mcp["default_tools_approval_mode"], "approve")
                self.assertIn("velaria_dataset_import", mcp["enabled_tools"])
                self.assertIn("velaria_dataset_download", mcp["enabled_tools"])
                self.assertEqual(mcp["env"]["VELARIA_HOME"], str(pathlib.Path(tmp)))
                self.assertEqual(
                    mcp["env"]["UV_CACHE_DIR"],
                    str(pathlib.Path(tmp) / "workspace" / ".cache" / "uv"),
                )
                self.assertEqual(mcp["env"]["http_proxy"], "http://127.0.0.1:7897")
                self.assertEqual(mcp["env"]["ALL_PROXY"], "socks5://127.0.0.1:7897")
                self.assertEqual(mcp["env"]["VELARIA_WORKSPACE"], str(pathlib.Path(tmp)))
                self.assertEqual(mcp["env"]["VELARIA_RUNTIME_WORKSPACE"], str(pathlib.Path(tmp)))
                self.assertEqual(mcp["env"]["VELARIA_SKILL_DIR"], str(skill_dir.resolve()))
                self.assertEqual(mcp["env"]["VELARIA_SKILL_PATH"], str(skill_file.resolve()))
                self.assertTrue(pathlib.Path(mcp["env"]["VELARIA_SKILL_PATH"]).exists())
                self.assertNotIn(str(pathlib.Path.cwd()), json.dumps(mcp["env"]))
                status = runtime.status(session_id)
                self.assertEqual(status["cwd"], runtime_cwd)
                self.assertEqual(status["reasoning_effort"], "none")
                self.assertTrue(status["network_access"])
                self.assertNotIn("project_cwd", status)
            finally:
                runtime.shutdown()

    def test_codex_runtime_normalizes_mcp_function_events(self):
        from velaria.ai_runtime.codex_runtime import _codex_sdk_event

        call_event = types.SimpleNamespace(
            step_type="tool",
            text="",
            data={
                "params": {
                    "event": {
                        "payload": {
                            "type": "function_call",
                            "namespace": "mcp__velaria__",
                            "name": "velaria_dataset_process",
                            "arguments": "{}",
                        }
                    }
                }
            },
        )
        normalized_call = _codex_sdk_event(call_event)
        self.assertEqual(normalized_call["type"], "tool_call")
        self.assertEqual(normalized_call["content"], "mcp__velaria__.velaria_dataset_process")

        output = json.dumps({"ok": True, "function": "velaria_dataset_process", "run_id": "run_dataset"})
        result_event = types.SimpleNamespace(
            step_type="tool",
            text="",
            data={
                "params": {
                    "event": {
                        "payload": {
                            "type": "function_call_output",
                            "call_id": "call_1",
                            "output": f"Wall time: 0.1 seconds\nOutput:\n{output}",
                        }
                    }
                }
            },
        )
        normalized_result = _codex_sdk_event(result_event)
        self.assertEqual(normalized_result["type"], "tool_result")
        self.assertIn('"run_id": "run_dataset"', normalized_result["content"])

        mcp_event = types.SimpleNamespace(
            step_type="tool",
            text="",
            data={
                "params": {
                    "item": {
                        "type": "mcpToolCall",
                        "server": "velaria",
                        "tool": "velaria_read",
                        "status": "completed",
                        "result": {
                            "structuredContent": {
                                "ok": True,
                                "function": "velaria_read",
                                "schema": ["region", "amount"],
                            }
                        },
                    }
                }
            },
        )
        normalized_mcp = _codex_sdk_event(mcp_event)
        self.assertEqual(normalized_mcp["type"], "tool_result")
        self.assertIn('"function": "velaria_read"', normalized_mcp["content"])

    def test_codex_runtime_uses_explicit_model_when_configured(self):
        from velaria.ai_runtime.codex_runtime import CodexRuntime

        class FakeClient:
            @classmethod
            def connect_stdio(cls, **kwargs):
                return cls()

            async def start(self):
                return None

            async def start_thread(self, config):
                self.started_config = config
                return types.SimpleNamespace(thread_id="codex-thread-1")

            async def close(self):
                return None

        with tempfile.TemporaryDirectory(prefix="velaria-codex-runtime-model-") as tmp:
            try:
                runtime = CodexRuntime(model="gpt-5.4-mini", reasoning_effort="low", runtime_workspace=tmp)
            except ImportError as exc:
                raise unittest.SkipTest("codex-app-server-sdk is not installed") from exc
            fake = FakeClient()
            runtime._client = fake
            try:
                asyncio.run(runtime.start_thread({}))
                self.assertEqual(fake.started_config.model, "gpt-5.4-mini")
                self.assertEqual(fake.started_config.config["model_reasoning_effort"], "low")
            finally:
                runtime.shutdown()

    def test_codex_runtime_allows_network_to_be_disabled(self):
        from velaria.ai_runtime.codex_runtime import CodexRuntime

        class FakeClient:
            @classmethod
            def connect_stdio(cls, **kwargs):
                inst = cls()
                inst.connect_kwargs = kwargs
                return inst

            async def start(self):
                return None

            async def start_thread(self, config):
                self.started_config = config
                return types.SimpleNamespace(thread_id="codex-thread-1")

            async def close(self):
                return None

        with tempfile.TemporaryDirectory(prefix="velaria-codex-runtime-network-") as tmp:
            try:
                runtime = CodexRuntime(runtime_workspace=tmp, network_access=False)
            except ImportError as exc:
                raise unittest.SkipTest("codex-app-server-sdk is not installed") from exc
            runtime._client_cls = FakeClient
            try:
                asyncio.run(runtime.start_thread({}))
                fake = runtime._client
                config = fake.started_config
                self.assertEqual(config.config["sandbox_workspace_write"]["network_access"], False)
                isolated_config = pathlib.Path(fake.connect_kwargs["env"]["CODEX_HOME"]) / "config.toml"
                self.assertIn("network_access = false", isolated_config.read_text(encoding="utf-8"))
                self.assertFalse(runtime.status()["network_access"])
            finally:
                runtime.shutdown()

    def test_codex_runtime_prewarms_ephemeral_thread(self):
        from velaria.ai_runtime.codex_runtime import CodexRuntime

        class FakeThread:
            def __init__(self):
                self.prompts = []

            async def chat_once(self, prompt):
                self.prompts.append(prompt)
                return types.SimpleNamespace(final_text="READY")

        class FakeClient:
            @classmethod
            def connect_stdio(cls, **kwargs):
                inst = cls()
                inst.started_configs = []
                inst.thread = FakeThread()
                return inst

            async def start(self):
                return None

            async def start_thread(self, config):
                self.started_configs.append(config)
                return self.thread

            async def close(self):
                return None

        with tempfile.TemporaryDirectory(prefix="velaria-codex-runtime-prewarm-") as tmp:
            try:
                runtime = CodexRuntime(runtime_workspace=tmp)
            except ImportError as exc:
                raise unittest.SkipTest("codex-app-server-sdk is not installed") from exc
            runtime._client_cls = FakeClient
            try:
                with mock.patch.dict(os.environ, {}, clear=False):
                    os.environ.pop("VELARIA_PREWARM_TURN", None)
                    asyncio.run(runtime.prewarm())
                    fake = runtime._client
                    self.assertTrue(fake.started_configs[0].ephemeral)
                    self.assertEqual(fake.thread.prompts, [])
                    asyncio.run(runtime.prewarm())
                    self.assertEqual(len(fake.started_configs), 1)
            finally:
                runtime.shutdown()

    def test_codex_runtime_prewarm_turn_is_explicit(self):
        from velaria.ai_runtime.codex_runtime import CodexRuntime

        class FakeThread:
            def __init__(self):
                self.prompts = []

            async def chat_once(self, prompt):
                self.prompts.append(prompt)
                return types.SimpleNamespace(final_text="READY")

        class FakeClient:
            @classmethod
            def connect_stdio(cls, **kwargs):
                inst = cls()
                inst.thread = FakeThread()
                return inst

            async def start(self):
                return None

            async def start_thread(self, config):
                return self.thread

            async def close(self):
                return None

        with tempfile.TemporaryDirectory(prefix="velaria-codex-runtime-prewarm-turn-") as tmp:
            try:
                runtime = CodexRuntime(runtime_workspace=tmp)
            except ImportError as exc:
                raise unittest.SkipTest("codex-app-server-sdk is not installed") from exc
            runtime._client_cls = FakeClient
            try:
                with mock.patch.dict(os.environ, {"VELARIA_PREWARM_TURN": "1"}):
                    asyncio.run(runtime.prewarm())
                self.assertEqual(
                    runtime._client.thread.prompts,
                    ["Velaria runtime warmup. Reply with exactly: READY"],
                )
            finally:
                runtime.shutdown()

    def test_codex_runtime_filters_protocol_items_before_cli_rendering(self):
        from velaria.ai_runtime.codex_runtime import _codex_sdk_event

        user_event = types.SimpleNamespace(
            step_type="itemCompleted",
            text="",
            data={
                "params": {
                    "item": {
                        "type": "userMessage",
                        "content": [{"type": "text", "text": "hello"}],
                    }
                }
            },
        )
        self.assertIsNone(_codex_sdk_event(user_event))

        empty_reasoning = types.SimpleNamespace(
            step_type="itemCompleted",
            text="",
            data={"params": {"item": {"type": "reasoning", "summary": [], "content": []}}},
        )
        self.assertIsNone(_codex_sdk_event(empty_reasoning))

        assistant_event = types.SimpleNamespace(
            step_type="itemCompleted",
            text="",
            data={
                "params": {
                    "item": {
                        "type": "assistantMessage",
                        "content": [{"type": "text", "text": "Hello."}],
                    }
                }
            },
        )
        self.assertEqual(
            _codex_sdk_event(assistant_event),
            {"type": "assistant_text", "content": "Hello.", "data": assistant_event.data},
        )
