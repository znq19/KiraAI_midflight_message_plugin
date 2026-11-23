"""Test harness: drives midflight/main.py exactly the way KiraAI does.

No KiraAI installation is required — ``tests/core/`` provides the minimal
stubs, and the hook call order below mirrors the framework:

    ON_LLM_REQUEST          core/message_manager.py  -> _track_run_start
    ON_LLM_RESPONSE         core/agent/agent_executor.py -> _ensure_stop_checkpoint
    ON_TOOL_RESULT  x N     core/agent/func_tool_manager.py -> _handle_tool_result
                            (N = len(resp.tool_calls): the framework loops over
                             *every* tool call of one LLM response)
    ON_IM_BATCH_MESSAGE     core/message_manager.py  -> on_batch_dedup

The last point is what the regression test below is about.
"""
import asyncio
import importlib.util
import sys
import time
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent
# ``core`` must resolve to tests/core/ (the stubs), never to a real KiraAI install
sys.path.insert(0, str(TESTS_DIR))

from core.chat.message_utils import KiraMessageBatchEvent  # noqa: E402
from core.chat.message_elements import Text  # noqa: E402

DEFAULT_SID = "qq:dm:1025040690"

# Plugin configuration exercising every code path used by the tests.
CFG = {
    "config_version": 2,
    "section_basic": {"enabled": True, "inject_timeout_steps": 2, "debug": False},
    "section_flow": {"flow_method_group": "all", "flow_method_dm": "any",
                     "accept_poke": True, "wake_keywords": []},
    "section_stop": {"stop_enabled": True, "stop_words": ["停", "停止"],
                     "stop_match_mode": "contains"},
    "section_scope": {},
    "section_limits": {"max_inject_per_run": 0, "freshness_seconds": -1,
                       "max_length": 0, "block_patterns": []},
    "section_inject": {"template": "", "inject_hint": True, "inject_hint_text": "HINT"},
    "section_media": {},
    "section_overrides": {},
}

_loaded = {}


def load_plugin(plugin_dir=None):
    """Import <plugin_dir>/main.py under a path-unique module name."""
    d = Path(plugin_dir or REPO_ROOT).resolve()
    key = str(d)
    if key in _loaded:
        return _loaded[key]
    src = d / "main.py"
    if not src.exists():
        raise FileNotFoundError(f"main.py not found in {d}")
    spec = importlib.util.spec_from_file_location("midflight_under_test_" + d.name, src)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _loaded[key] = mod
    return mod


async def make_plugin(plugin_dir=None):
    """Return (plugin, ctx, sid) with initialize() already run."""
    mod = load_plugin(plugin_dir)
    ctx = FakeCtx()
    plugin = mod.MidflightMessagePlugin(ctx, CFG)
    await plugin.initialize()
    return plugin, ctx, DEFAULT_SID


# ----------------------------------------------------------------- factories

class FakeSender:
    def __init__(self, uid, nick="林校森"):
        self.user_id = uid
        self.nickname = nick


class FakeMessage:
    """Shaped like KiraAI's KiraIMMessage (only the fields the plugin reads)."""

    def __init__(self, mid, text, uid="1025040690", nick="林校森"):
        self.message_id = mid
        self.chain = [Text(text)]
        self.timestamp = time.time()
        self.sender = FakeSender(uid, nick)
        self.self_id = "bot"
        self.group = None
        self.is_mentioned = False
        self.message_str = None

    def is_group_message(self):
        return False


class FakeSession:
    sid = DEFAULT_SID


class FakeBuffer:
    """Shaped like core.chat.session_session.SessionBuffer."""

    def __init__(self):
        self.buffer = []
        self.lock = asyncio.Lock()

    def get_length(self):
        return len(self.buffer)

    def flush(self):
        out = list(self.buffer)
        self.buffer.clear()
        return out


class FakeConfig:
    def get_config(self, key, default=None):
        return {
            "bot_config.agent.max_tool_loop": 2,          # <- the user's setting
            "bot_config.agent.tool_call_timeout": 60,
            "bot_config.bot.max_buffer_messages": 5,
            "bot_config.bot.max_message_interval": 30,
        }.get(key, default)


class FakeCtx:
    def __init__(self):
        self.config = FakeConfig()
        self.buffers = {}
        self.flushes = []

    def get_buffer(self, sid):
        return self.buffers.setdefault(sid, FakeBuffer())

    def get_default_llm_client(self):
        class _Model:
            model_config = {}

        class _Client:
            model = _Model()
        return _Client()

    def get_timezone(self):
        return None

    def get_plugin_inst(self, pid):
        return None

    async def flush_session_messages(self, sid, extra_event=None):
        self.flushes.append(sid)
        return True


class FakeToolResult:
    def __init__(self, text="No files found."):
        self.text = text
        self.attachments = []


class FakeResp:
    """Shaped like core.provider.LLMResponse (fields the plugin reads)."""

    def __init__(self, step_index, tool_calls):
        self.agent_step_index = step_index
        self.tool_calls = tool_calls
        self.text_response = ""
        self.reasoning_content = ""
        self.tool_results = []


class BufferedShim:
    """SessionBuffer holds KiraMessageEvent-like objects (``.message`` accessor)."""

    def __init__(self, message):
        self.message = message

    def is_group_message(self):
        return False


def batch(*messages):
    return KiraMessageBatchEvent(timestamp=int(time.time()), session=FakeSession(),
                                 messages=list(messages))


def start_run(plugin, sid, msg_id="100", text="跑个任务"):
    """ON_LLM_REQUEST: register the run the way the framework does."""
    ev = batch(FakeMessage(msg_id, text))
    return ev


async def drive_on_llm_request(plugin, event):
    await plugin._track_run_start(event)


async def drive_on_llm_response(plugin, event, step_index, tool_calls):
    await plugin._ensure_stop_checkpoint(event, FakeResp(step_index, tool_calls))


async def drive_on_tool_result(plugin, event, tool_result=None):
    await plugin._handle_tool_result(event, tool_result or FakeToolResult())


async def drive_on_batch_message(plugin, event):
    await plugin.on_batch_dedup(event)
