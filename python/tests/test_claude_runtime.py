"""Dedicated unit tests for ClaudeAgentRuntime with mocked claude_agent_sdk."""

import asyncio
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock


# ---------------------------------------------------------------------------
# Mock the entire claude_agent_sdk package before importing the runtime
# ---------------------------------------------------------------------------

class FakeClaudeAgentOptions:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class FakeAssistantMessage:
    def __init__(self, text=""):
        self.content = [types.SimpleNamespace(text=text)]


class FakeResultMessage:
    def __init__(self, result_text=""):
        self.result = result_text


class FakeThinkingMessage:
    def __init__(self, thinking_text=""):
        self.content = [types.SimpleNamespace(thinking=thinking_text)]


class FakeClaudeSDKClient:
    def __init__(self, options=None, custom_tools=None):
        self.options = options
        self.custom_tools = custom_tools or []
        self.prompts: list[str] = []
        self._responses: list = []

    def set_responses(self, *responses):
        self._responses = list(responses)

    async def query(self, prompt=""):
        self.prompts.append(prompt)
        for r in self._responses:
            yield r


# Standalone query function (used by generate_sql)
_fake_query_responses: list = []


def _set_fake_query_responses(*responses):
    global _fake_query_responses
    _fake_query_responses = list(responses)


async def fake_claude_query(prompt="", options=None):
    for r in _fake_query_responses:
        yield r


# Install mocks
_mock_sdk = types.ModuleType("claude_agent_sdk")
_mock_sdk.ClaudeSDKClient = FakeClaudeSDKClient
_mock_sdk.query = fake_claude_query
_mock_types = types.ModuleType("claude_agent_sdk.types")
_mock_types.ClaudeAgentOptions = FakeClaudeAgentOptions
_mock_sdk.types = _mock_types

sys.modules["claude_agent_sdk"] = _mock_sdk
sys.modules["claude_agent_sdk.types"] = _mock_types


# Now safe to import
from velaria.ai_runtime import create_runtime  # noqa: E402
from velaria.ai_runtime.claude_runtime import (  # noqa: E402
    ClaudeAgentRuntime,
    _apply_proxy_env,
    _build_sql_user_message,
    _claude_sdk_event,
    _claude_sdk_event_text,
    _claude_runtime_path,
    _coerce_bool,
    _normalize_proxy_env,
    _parse_sql_json,
    _proxy_env_from_process,
    _resolve_velaria_skill_path,
    _runtime_cwd,
    _runtime_workspace,
    _workspace_key,
)
from velaria.ai_runtime.agent import normalize_runtime_event  # noqa: E402
from velaria.ai_runtime.functions import tool_definitions  # noqa: E402


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class ClaudeRuntimeInitTest(unittest.TestCase):
    def test_init_defaults(self):
        rt = ClaudeAgentRuntime(api_key="")
        self.assertEqual(rt.model, "claude-sonnet-4-20250514")
        self.assertEqual(rt.provider, "anthropic")
        self.assertEqual(rt.auth_mode, "oauth")
        self.assertTrue(rt.reuse_local_config)
        self.assertEqual(rt.api_key, "")
        self.assertEqual(rt.base_url, "")
        self.assertEqual(rt.reasoning_effort, "none")
        self.assertTrue(rt.network_access)

    def test_init_with_reasoning_and_network(self):
        rt = ClaudeAgentRuntime(api_key="", reasoning_effort="high", network_access=False)
        self.assertEqual(rt.reasoning_effort, "high")
        self.assertFalse(rt.network_access)

    def test_init_with_api_key_mode(self):
        rt = ClaudeAgentRuntime(
            api_key="sk-test",
            auth_mode="api_key",
            base_url="https://api.example.com/v1",
            model="claude-opus-4-20250514",
            reuse_local_config=False,
            reasoning_effort="low",
            network_access=False,
        )
        self.assertEqual(rt.auth_mode, "api_key")
        self.assertEqual(rt.api_key, "sk-test")
        self.assertEqual(rt.base_url, "https://api.example.com/v1")
        self.assertEqual(rt.model, "claude-opus-4-20250514")
        self.assertFalse(rt.reuse_local_config)
        self.assertEqual(rt.reasoning_effort, "low")
        self.assertFalse(rt.network_access)

    def test_init_creates_runtime_workspace(self):
        with tempfile.TemporaryDirectory(prefix="velaria-claude-test-") as tmp:
            ws = pathlib.Path(tmp) / "runtime-ws"
            rt = ClaudeAgentRuntime(api_key="", runtime_workspace=str(ws), cwd=tmp)
            self.assertTrue(ws.exists())
            self.assertTrue((ws / "workspace").exists())

    def test_runtime_env_oauth_reuse_config(self):
        rt = ClaudeAgentRuntime(api_key="", auth_mode="oauth")
        env = rt._runtime_env()
        self.assertIsInstance(env, dict)
        self.assertIn("VELARIA_WORKSPACE", env)
        self.assertIn("VELARIA_RUNTIME_WORKSPACE", env)

    def test_runtime_env_api_key_mode(self):
        rt = ClaudeAgentRuntime(
            api_key="sk-test",
            auth_mode="api_key",
            base_url="https://api.example.com/v1",
            reuse_local_config=False,
        )
        env = rt._runtime_env()
        self.assertEqual(env["ANTHROPIC_API_KEY"], "sk-test")
        self.assertEqual(env["ANTHROPIC_BASE_URL"], "https://api.example.com/v1")
        self.assertIn("HOME", env)
        self.assertIn("CLAUDE_CONFIG_DIR", env)

    def test_runtime_env_includes_skill_paths(self):
        with tempfile.TemporaryDirectory(prefix="velaria-skill-") as tmp:
            skill_dir = pathlib.Path(tmp).resolve() / "skills"
            skill_file = skill_dir / "velaria_python_local" / "SKILL.md"
            skill_file.parent.mkdir(parents=True, exist_ok=True)
            skill_file.write_text("# Test Skill\n", encoding="utf-8")
            rt = ClaudeAgentRuntime(api_key="", skill_dir=str(skill_dir))
            env = rt._runtime_env()
            self.assertEqual(env["VELARIA_SKILL_DIR"], str(skill_dir.resolve()))

    def test_runtime_env_no_skill_paths_when_not_provided(self):
        rt = ClaudeAgentRuntime(api_key="")
        env = rt._runtime_env()
        self.assertNotIn("VELARIA_SKILL_DIR", env)
        self.assertNotIn("VELARIA_SKILL_PATH", env)

    def test_status_reports_all_fields(self):
        rt = ClaudeAgentRuntime(
            api_key="sk-test",
            auth_mode="api_key",
            base_url="https://api.example.com/v1",
            reuse_local_config=False,
            reasoning_effort="high",
            network_access=False,
        )
        status = rt.status()
        self.assertEqual(status["runtime"], "claude")
        self.assertEqual(status["provider"], "anthropic")
        self.assertEqual(status["auth_mode"], "api_key")
        self.assertEqual(status["model"], "claude-sonnet-4-20250514")
        self.assertEqual(status["reasoning_effort"], "high")
        self.assertFalse(status["network_access"])
        self.assertFalse(status["reuse_local_config"])
        self.assertIn("workspace", status)
        self.assertIn("cwd", status)
        self.assertIn("tools", status)
        self.assertIsInstance(status["tools"], list)
        self.assertIn("velaria_read", status["tools"])

    def test_available_tools_matches_definitions(self):
        rt = ClaudeAgentRuntime(api_key="")
        names = rt.available_tools()
        expected = {t["name"] for t in tool_definitions()}
        self.assertEqual(set(names), expected)


class ClaudeRuntimeSessionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="velaria-claude-session-")
        ws = pathlib.Path(self.tmp.name) / "runtime-ws"
        self.rt = ClaudeAgentRuntime(api_key="", runtime_workspace=str(ws), cwd=self.tmp.name)

    def tearDown(self):
        self.rt.shutdown()
        self.tmp.cleanup()

    def test_start_thread_creates_session(self):
        session_id = asyncio.run(self.rt.start_thread({"schema": ["a", "b"], "table_name": "test"}))
        self.assertTrue(session_id.startswith("ai_session_"))
        self.assertIn(session_id, self.rt._sessions)

    def test_resume_thread_active_session(self):
        session_id = asyncio.run(self.rt.start_thread({}))
        self.assertTrue(asyncio.run(self.rt.resume_thread(session_id)))

    def test_resume_thread_nonexistent(self):
        self.assertFalse(asyncio.run(self.rt.resume_thread("nonexistent")))

    def test_resume_thread_closed_session(self):
        session_id = asyncio.run(self.rt.start_thread({}))
        asyncio.run(self.rt.close_thread(session_id))
        self.assertFalse(asyncio.run(self.rt.resume_thread(session_id)))

    def test_close_thread_removes_from_memory(self):
        session_id = asyncio.run(self.rt.start_thread({}))
        asyncio.run(self.rt.close_thread(session_id))
        self.assertNotIn(session_id, self.rt._sessions)

    def test_list_sessions(self):
        sid1 = asyncio.run(self.rt.start_thread({}))
        sid2 = asyncio.run(self.rt.start_thread({}))
        sessions = asyncio.run(self.rt.list_sessions())
        ids = {s["session_id"] for s in sessions}
        self.assertIn(sid1, ids)
        self.assertIn(sid2, ids)

    def test_create_session_alias_for_start_thread(self):
        session_id = asyncio.run(self.rt.create_session({"a": 1}))
        self.assertTrue(session_id.startswith("ai_session_"))


class ClaudeRuntimeGenerateSqlTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="velaria-claude-sql-")
        ws = pathlib.Path(self.tmp.name) / "runtime-ws"
        self.rt = ClaudeAgentRuntime(api_key="", runtime_workspace=str(ws), cwd=self.tmp.name)
        self.session_id = asyncio.run(self.rt.start_thread({}))

    def tearDown(self):
        self.rt.shutdown()
        self.tmp.cleanup()

    def test_generate_sql_parses_json_response(self):
        _set_fake_query_responses(
            FakeAssistantMessage('{"sql": "SELECT * FROM t", "explanation": "get all rows"}'),
        )
        result = asyncio.run(self.rt.generate_sql(self.session_id, "show all", ["a", "b"], "t"))
        self.assertEqual(result["sql"], "SELECT * FROM t")
        self.assertEqual(result["explanation"], "get all rows")

    def test_generate_sql_strips_markdown_fences(self):
        _set_fake_query_responses(
            FakeAssistantMessage('```json\n{"sql": "SELECT 1", "explanation": "one"}\n```'),
        )
        result = asyncio.run(self.rt.generate_sql(self.session_id, "one row", ["x"], "t"))
        self.assertEqual(result["sql"], "SELECT 1")

    def test_generate_sql_fallback_when_not_json(self):
        _set_fake_query_responses(FakeAssistantMessage("SELECT x FROM t WHERE x > 10"))
        result = asyncio.run(self.rt.generate_sql(self.session_id, "filter x", ["x"], "t"))
        self.assertEqual(result["sql"], "SELECT x FROM t WHERE x > 10")

    def test_generate_sql_handles_result_message(self):
        _set_fake_query_responses(
            FakeResultMessage('{"sql": "SELECT a, b FROM t", "explanation": "project columns"}'),
        )
        result = asyncio.run(self.rt.generate_sql(self.session_id, "project", ["a", "b"], "t"))
        self.assertEqual(result["sql"], "SELECT a, b FROM t")

    def test_generate_sql_updates_session_activity(self):
        _set_fake_query_responses(
            FakeAssistantMessage('{"sql": "SELECT 1", "explanation": "test"}'),
        )
        asyncio.run(self.rt.generate_sql(self.session_id, "test", ["x"], "t"))
        entry = self.rt.registry.lookup(self.session_id)
        self.assertIsNotNone(entry["last_active_at"])

    def test_generate_sql_includes_sample_rows(self):
        captured_prompts = []

        async def capture_query(prompt="", options=None):
            captured_prompts.append(prompt)
            yield FakeAssistantMessage('{"sql": "SELECT 1", "explanation": "x"}')

        with mock.patch("claude_agent_sdk.query", side_effect=capture_query):
            asyncio.run(self.rt.generate_sql(
                self.session_id, "query with samples", ["name", "score"], "students",
                [{"name": "Alice", "score": 95}, {"name": "Bob", "score": 87}],
            ))
        self.assertTrue(any("Alice" in p for p in captured_prompts))
        self.assertTrue(any("Sample rows" in p for p in captured_prompts))


class ClaudeRuntimeSendMessageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="velaria-claude-msg-")
        ws = pathlib.Path(self.tmp.name) / "runtime-ws"
        self.rt = ClaudeAgentRuntime(api_key="", runtime_workspace=str(ws), cwd=self.tmp.name)
        self.session_id = asyncio.run(self.rt.start_thread(
            {"source_path": "/tmp/data.csv", "table_name": "input_table"}
        ))

    def tearDown(self):
        self.rt.shutdown()
        self.tmp.cleanup()

    def test_send_message_error_on_unknown_session(self):
        async def collect():
            return [e async for e in self.rt.send_message("bad_session", "hello")]
        events = asyncio.run(collect())
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "error")

    def test_send_message_creates_client_with_custom_tools(self):
        class CaptureClient:
            def __init__(self, options=None, custom_tools=None):
                self.options = options
                self.custom_tools = custom_tools or []

            async def query(self, prompt=""):
                yield FakeAssistantMessage("hello from claude")

        with mock.patch("claude_agent_sdk.ClaudeSDKClient", CaptureClient):
            async def collect():
                return [e async for e in self.rt.send_message(self.session_id, "hi")]
            events = asyncio.run(collect())
        self.assertTrue(any(e.type == "assistant_text" for e in events))

    def test_send_message_normalizes_thinking_event(self):
        class CaptureClient:
            def __init__(self, options=None, custom_tools=None):
                pass

            async def query(self, prompt=""):
                yield FakeThinkingMessage("Let me analyze the data...")

        with mock.patch("claude_agent_sdk.ClaudeSDKClient", CaptureClient):
            async def collect():
                return [e async for e in self.rt.send_message(self.session_id, "analyze")]
            events = asyncio.run(collect())
        self.assertTrue(any(e.type == "thinking" for e in events))

    def test_send_message_normalizes_error_event(self):
        class CaptureClient:
            def __init__(self, options=None, custom_tools=None):
                pass

            async def query(self, prompt=""):
                yield types.SimpleNamespace()

        with mock.patch("claude_agent_sdk.ClaudeSDKClient", CaptureClient):
            async def collect():
                return [e async for e in self.rt.send_message(self.session_id, "boom")]
            events = asyncio.run(collect())
        self.assertTrue(len(events) > 0)

    def test_send_message_tool_handler_injects_dataset_context(self):
        captured_input = {}

        class CaptureClient:
            def __init__(self, options=None, custom_tools=None):
                self.options = options
                self.custom_tools = custom_tools or []

            async def query(self, prompt=""):
                if self.custom_tools:
                    tool = self.custom_tools[0]
                    handler = tool["handler"]
                    captured_input["name"] = tool["name"]
                    captured_input["result"] = handler({"limit": 5})
                yield FakeAssistantMessage("ok")

        with mock.patch("claude_agent_sdk.ClaudeSDKClient", CaptureClient):
            async def collect():
                return [e async for e in self.rt.send_message(self.session_id, "read some data")]
            asyncio.run(collect())
        # Verify tool was called and result is a valid JSON string
        self.assertIn("name", captured_input)
        self.assertIn("result", captured_input)
        result = json.loads(captured_input["result"])
        self.assertIsInstance(result, dict)

    def test_send_message_updates_activity_on_completion(self):
        class CaptureClient:
            def __init__(self, options=None, custom_tools=None):
                pass

            async def query(self, prompt=""):
                yield FakeAssistantMessage("done")

        with mock.patch("claude_agent_sdk.ClaudeSDKClient", CaptureClient):
            async def collect():
                return [e async for e in self.rt.send_message(self.session_id, "test")]
            asyncio.run(collect())
        entry = self.rt.registry.lookup(self.session_id)
        self.assertIsNotNone(entry["last_active_at"])


class ClaudeRuntimeHelpersTest(unittest.TestCase):
    def test_build_sql_user_message_basic(self):
        msg = _build_sql_user_message("count rows", ["name", "age"], "people")
        self.assertIn("Table: people", msg)
        self.assertIn("Request: count rows", msg)

    def test_parse_sql_json_plain(self):
        result = _parse_sql_json('{"sql": "SELECT 1", "explanation": "test"}')
        self.assertEqual(result["sql"], "SELECT 1")

    def test_parse_sql_json_with_fence(self):
        result = _parse_sql_json('```json\n{"sql": "SELECT 2", "explanation": "two"}\n```')
        self.assertEqual(result["sql"], "SELECT 2")

    def test_parse_sql_json_raw_text(self):
        result = _parse_sql_json("SELECT * FROM t")
        self.assertEqual(result["sql"], "SELECT * FROM t")

    def test_parse_sql_json_empty(self):
        result = _parse_sql_json("")
        self.assertEqual(result["sql"], "")

    def test_workspace_key(self):
        key = _workspace_key(pathlib.Path("/home/user/my_project"))
        self.assertIn("my_project-", key)
        parts = key.split("-")
        self.assertEqual(len(parts[-1]), 12)

    def test_claude_runtime_path_empty(self):
        self.assertEqual(_claude_runtime_path(""), "")

    def test_claude_runtime_path_nonexistent(self):
        with self.assertRaises(RuntimeError):
            _claude_runtime_path("/nonexistent/path/to/claude")

    def test_resolve_velaria_skill_path_from_dir(self):
        with tempfile.TemporaryDirectory(prefix="velaria-skill-") as tmp:
            skill_dir = pathlib.Path(tmp).resolve() / "skills"
            skill_file = skill_dir / "velaria_python_local" / "SKILL.md"
            skill_file.parent.mkdir(parents=True, exist_ok=True)
            skill_file.write_text("# Skill\n", encoding="utf-8")
            result = _resolve_velaria_skill_path("", skill_dir)
            self.assertIsNotNone(result)
            self.assertEqual(str(result), str(skill_file.resolve()))

    def test_resolve_velaria_skill_path_from_direct_path(self):
        with tempfile.TemporaryDirectory(prefix="velaria-skill-") as tmp:
            skill_file = pathlib.Path(tmp).resolve() / "custom_skill.md"
            skill_file.write_text("# Custom\n", encoding="utf-8")
            result = _resolve_velaria_skill_path(str(skill_file), None)
            self.assertIsNotNone(result)
            self.assertEqual(str(result), str(skill_file.resolve()))

    def test_resolve_velaria_skill_path_missing(self):
        result = _resolve_velaria_skill_path("", None)
        self.assertIsNone(result)


class ClaudeRuntimeConfigIntegrationTest(unittest.TestCase):
    def test_create_runtime_selects_claude_when_explicit(self):
        with tempfile.TemporaryDirectory(prefix="velaria-factory-") as tmp:
            rt = create_runtime({
                "runtime": "claude", "auth_mode": "oauth",
                "runtime_workspace": tmp, "cwd": tmp,
            })
            self.assertIsInstance(rt, ClaudeAgentRuntime)
            rt.shutdown()

    def test_create_runtime_selects_claude_when_auto_and_sdk_available(self):
        with tempfile.TemporaryDirectory(prefix="velaria-factory-") as tmp:
            rt = create_runtime({
                "runtime": "auto", "runtime_workspace": tmp, "cwd": tmp,
            })
            self.assertIsInstance(rt, ClaudeAgentRuntime)
            rt.shutdown()

    def test_create_runtime_passes_all_config_fields(self):
        with tempfile.TemporaryDirectory(prefix="velaria-factory-full-") as tmp:
            skill_dir = pathlib.Path(tmp).resolve() / "skills"
            skill_file = skill_dir / "velaria_python_local" / "SKILL.md"
            skill_file.parent.mkdir(parents=True, exist_ok=True)
            skill_file.write_text("# Test\n", encoding="utf-8")
            rt = create_runtime({
                "runtime": "claude", "provider": "anthropic",
                "auth_mode": "api_key", "api_key": "sk-test",
                "base_url": "https://api.test.example/v1",
                "model": "claude-opus-4-20250514",
                "reasoning_effort": "high", "network_access": False,
                "claude_runtime_path": "", "runtime_workspace": tmp,
                "skill_dir": str(skill_dir), "skill_path": str(skill_file),
                "cwd": tmp,
            })
            self.assertEqual(rt.reasoning_effort, "high")
            self.assertFalse(rt.network_access)
            rt.shutdown()

    def test_create_runtime_default_reasoning_and_network(self):
        with tempfile.TemporaryDirectory(prefix="velaria-factory-defaults-") as tmp:
            rt = create_runtime({
                "runtime": "claude", "runtime_workspace": tmp, "cwd": tmp,
            })
            self.assertEqual(rt.reasoning_effort, "none")
            self.assertTrue(rt.network_access)
            rt.shutdown()

    def test_prewarm_creates_client(self):
        rt = ClaudeAgentRuntime(api_key="")
        self.assertFalse(rt._prewarm_complete)
        asyncio.run(rt.prewarm())
        self.assertTrue(rt._prewarm_complete)

    def test_prewarm_is_idempotent(self):
        rt = ClaudeAgentRuntime(api_key="")
        asyncio.run(rt.prewarm())
        asyncio.run(rt.prewarm())
        self.assertTrue(rt._prewarm_complete)

    def test_prewarm_with_turn_env(self):
        with mock.patch.dict(os.environ, {"VELARIA_PREWARM_TURN": "1"}):
            rt = ClaudeAgentRuntime(api_key="")
            asyncio.run(rt.prewarm())
            self.assertTrue(rt._prewarm_complete)


class ClaudeRuntimeProxyEnvTest(unittest.TestCase):
    def test_init_stores_normalized_proxy_env(self):
        rt = ClaudeAgentRuntime(
            api_key="",
            proxy_env={"http_proxy": "http://127.0.0.1:7897", "HTTPS_PROXY": "http://127.0.0.1:7897"},
        )
        self.assertEqual(rt._proxy_env["http_proxy"], "http://127.0.0.1:7897")
        self.assertEqual(rt._proxy_env["https_proxy"], "http://127.0.0.1:7897")

    def test_runtime_env_injects_proxy_when_configured(self):
        rt = ClaudeAgentRuntime(
            api_key="", auth_mode="oauth",
            proxy_env={"http_proxy": "http://127.0.0.1:7897"},
        )
        env = rt._runtime_env()
        self.assertEqual(env["http_proxy"], "http://127.0.0.1:7897")
        self.assertIn("no_proxy", env)

    def test_runtime_env_no_proxy_when_not_configured(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            rt = ClaudeAgentRuntime(api_key="", auth_mode="oauth")
            env = rt._runtime_env()
            self.assertNotIn("http_proxy", env)

    def test_status_reports_proxy_flag(self):
        rt = ClaudeAgentRuntime(api_key="", proxy_env={"http_proxy": "http://127.0.0.1:7897"})
        self.assertTrue(rt.status()["proxy"])

    def test_status_reports_no_proxy_flag(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            rt = ClaudeAgentRuntime(api_key="")
            self.assertFalse(rt.status()["proxy"])


class ClaudeRuntimeEnvTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="velaria-claude-env-")
        ws = pathlib.Path(self.tmp.name) / "runtime-ws"
        self.rt = ClaudeAgentRuntime(api_key="", runtime_workspace=str(ws), cwd=self.tmp.name)

    def tearDown(self):
        self.rt.shutdown()
        self.tmp.cleanup()

    def test_env_includes_velaria_home(self):
        env = self.rt._runtime_env()
        self.assertIn("VELARIA_HOME", env)

    def test_env_includes_uv_cache_dir(self):
        env = self.rt._runtime_env()
        self.assertIn("UV_CACHE_DIR", env)

    def test_env_includes_workspace_vars(self):
        env = self.rt._runtime_env()
        self.assertEqual(env["VELARIA_WORKSPACE"], str(self.rt.runtime_workspace))
        self.assertEqual(env["VELARIA_RUNTIME_WORKSPACE"], str(self.rt.runtime_workspace))

    def test_env_isolated_mode_overrides_home(self):
        rt = ClaudeAgentRuntime(
            api_key="sk-test", auth_mode="api_key", reuse_local_config=False,
            runtime_workspace=str(pathlib.Path(self.tmp.name) / "isolated-ws"),
            cwd=self.tmp.name,
        )
        env = rt._runtime_env()
        self.assertEqual(env["HOME"], str(rt.runtime_workspace))
        self.assertIn("CLAUDE_CONFIG_DIR", env)
        rt.shutdown()


class ClaudeRuntimeFactoryProxyTest(unittest.TestCase):
    def test_factory_passes_proxy_env_to_claude(self):
        with tempfile.TemporaryDirectory(prefix="velaria-factory-proxy-") as tmp:
            rt = create_runtime({
                "runtime": "claude", "runtime_workspace": tmp, "cwd": tmp,
                "proxy_env": {"http_proxy": "http://127.0.0.1:7897"},
            })
            self.assertEqual(rt._proxy_env["http_proxy"], "http://127.0.0.1:7897")
            rt.shutdown()

    def test_factory_empty_proxy_env(self):
        with tempfile.TemporaryDirectory(prefix="velaria-factory-noproxy-") as tmp:
            rt = create_runtime({"runtime": "claude", "runtime_workspace": tmp, "cwd": tmp})
            self.assertEqual(rt._proxy_env, {})
            rt.shutdown()


class ClaudeRuntimeShutdownTest(unittest.TestCase):
    def test_shutdown_clears_sessions(self):
        rt = ClaudeAgentRuntime(api_key="")
        session_id = asyncio.run(rt.start_thread({}))
        self.assertIn(session_id, rt._sessions)
        rt.shutdown()
        self.assertEqual(len(rt._sessions), 0)

    def test_shutdown_marks_closed_before_close(self):
        rt = ClaudeAgentRuntime(api_key="")
        session_id = asyncio.run(rt.start_thread({}))
        entry_before = rt.registry.lookup(session_id)
        self.assertEqual(entry_before["status"], "active")
        rt.shutdown()
        self.assertNotIn(session_id, rt._sessions)


class ClaudeRuntimeEventNormTest(unittest.TestCase):
    def test_assistant_message_text_extracted(self):
        msg = types.SimpleNamespace(
            content=[types.SimpleNamespace(text="Hello "), types.SimpleNamespace(text="World")],
        )
        content, msg_type = _claude_sdk_event_text(msg)
        self.assertEqual(content, "Hello World")
        self.assertEqual(msg_type, "SimpleNamespace")

    def test_result_message_text_extracted(self):
        msg = types.SimpleNamespace(result='{"sql": "SELECT 1"}')
        content, msg_type = _claude_sdk_event_text(msg)
        self.assertEqual(content, '{"sql": "SELECT 1"}')
        self.assertEqual(msg_type, "ResultMessage")

    def test_thinking_block_text_extracted(self):
        msg = types.SimpleNamespace(
            content=[types.SimpleNamespace(thinking="Let me think...")],
        )
        content, msg_type = _claude_sdk_event_text(msg)
        self.assertEqual(content, "Let me think...")
        self.assertEqual(msg_type, "thinking")

    def test_claude_sdk_event_normalizes_assistant(self):
        msg = types.SimpleNamespace(
            content=[types.SimpleNamespace(text="Hello")],
        )
        event = _claude_sdk_event(msg, session_id="s1")
        self.assertEqual(event.type, "assistant_text")
        self.assertEqual(event.content, "Hello")
        self.assertEqual(event.session_id, "s1")

    def test_claude_sdk_event_normalizes_thinking(self):
        msg = types.SimpleNamespace(
            content=[types.SimpleNamespace(thinking="Hmm...")],
        )
        event = _claude_sdk_event(msg)
        self.assertEqual(event.type, "thinking")


class ClaudeRuntimeCoerceBoolTest(unittest.TestCase):
    def test_true_bool(self):
        self.assertTrue(_coerce_bool(True, False))

    def test_false_bool(self):
        self.assertFalse(_coerce_bool(False, True))

    def test_string_values(self):
        for v in ("1", "true", "yes", "on"):
            self.assertTrue(_coerce_bool(v, False))
        for v in ("0", "false", "no", "off"):
            self.assertFalse(_coerce_bool(v, True))

    def test_none_returns_default(self):
        self.assertTrue(_coerce_bool(None, True))
        self.assertFalse(_coerce_bool(None, False))


class NormalizeRuntimeEventTest(unittest.TestCase):
    def test_assistant_message_normalized(self):
        e = normalize_runtime_event("AssistantMessage", "Hello")
        self.assertEqual(e.type, "assistant_text")

    def test_tool_use_block_normalized(self):
        e = normalize_runtime_event("ToolUseBlock", "", data={"tool_name": "velaria_read"})
        self.assertEqual(e.type, "tool_call")

    def test_tool_result_block_normalized(self):
        e = normalize_runtime_event("ToolResultBlock", '{"ok": true}')
        self.assertEqual(e.type, "tool_result")

    def test_thinking_normalized(self):
        e = normalize_runtime_event("thinking", "Hmm...")
        self.assertEqual(e.type, "thinking")

    def test_error_normalized(self):
        e = normalize_runtime_event("error", "something failed")
        self.assertEqual(e.type, "error")

    def test_unknown_type_passthrough(self):
        e = normalize_runtime_event("ResultMessage", '{"key": "val"}')
        self.assertEqual(e.type, "ResultMessage")


if __name__ == "__main__":
    unittest.main()
