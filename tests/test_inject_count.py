"""Midflight v1.3.3 回归：_run_inject_count 必须始终写 (count, ts) 元组。

v1.3.2 事故：实际注入写点漏改仍写纯 int，导致每个工具边界之后
  - _gc() 解包 (_, ts) 抛 TypeError: cannot unpack non-iterable int object
  - 读取端 .get(run_id, (0, now))[0] 抛 TypeError: 'int' object is not subscriptable
异常被 on_tool_result 自捕获，表现为日志反复报错且该边界注入失效。

本用例：在飞轮 + buffer 里有可注入消息 → 走 _handle_tool_result 注入路径，
断言写入的是元组、计数正确，随后强制执行 _gc() 不得抛异常、新鲜条目保留。

用法: python3 tests/test_inject_count.py <midflight/main.py 所在目录>
"""
import asyncio
import importlib.util
import sys
import time
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from core.chat import Group, KiraIMMessage, MessageChain, Session, User  # noqa: E402
from core.chat.message_elements import Text  # noqa: E402
from core.chat.message_utils import KiraMessageBatchEvent  # noqa: E402

SID = "qq:gm:10001"


def load(path):
    spec = importlib.util.spec_from_file_location(
        "mf_inject_count_" + Path(path).parent.name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class Cfg:
    def get_config(self, key, default=None):
        return {"bot_config.agent.max_tool_loop": 2,
                "bot_config.agent.tool_call_timeout": 60,
                "bot_config.bot.max_buffer_messages": 5,
                "bot_config.bot.max_message_interval": 30}.get(key, default)


class Buf:
    def __init__(self):
        self.buffer = []
        self.lock = asyncio.Lock()

    def get_length(self):
        return len(self.buffer)

    def flush(self):
        out = list(self.buffer)
        self.buffer.clear()
        return out


class Ctx:
    def __init__(self):
        self.config = Cfg()
        self.buffers = {}
        self.plugin_mgr = None
        self.plugin = None
        self.published = []

    def get_buffer(self, sid):
        return self.buffers.setdefault(sid, Buf())

    def get_default_llm_client(self):
        class _M:
            model_config = {}
        class _C:
            model = _M()
        return _C()

    def get_timezone(self):
        return None

    def get_plugin_inst(self, pid):
        return None

    async def flush_session_messages(self, sid, extra_event=None):
        buf = self.get_buffer(sid)
        async with buf.lock:
            msgs = buf.flush()
        if msgs:
            await self.publish_event(KiraMessageBatchEvent(
                timestamp=int(time.time()), session=Session(),
                messages=[getattr(m, "message", m) for m in msgs]))
        return True

    async def publish_event(self, event):
        if getattr(self, "plugin", None) is not None:
            await self.plugin.on_batch_dedup(event)
            self.published.append((event.event_id[:8], event.is_stopped))


CFG = {
    "section_basic": {"enabled": True, "inject_timeout_steps": 2, "debug": False},
    "section_flow": {"flow_method_group": "all", "flow_method_dm": "any",
                     "accept_poke": True, "wake_keywords": []},
    "section_stop": {"stop_enabled": True, "stop_words": ["停"], "stop_match_mode": "contains"},
    "section_scope": {}, "section_limits": {"max_inject_per_run": 0, "freshness_seconds": -1,
                                            "max_length": 0, "block_patterns": []},
    "section_inject": {"template": "", "inject_hint": True, "inject_hint_text": "HINT"},
    "section_media": {}, "section_overrides": {},
}


def msg(mid, text, mentioned=True):
    return KiraIMMessage(timestamp=time.time(), sender=User("20001", "小明"),
                         group=Group("10001", "测试群"), message_id=str(mid),
                         self_id="10000", chain=MessageChain([Text(text)]),
                         is_mentioned=mentioned)


class Shim:
    def __init__(self, m):
        self.message = m

    def is_group_message(self):
        return True


def batch(*msgs):
    return KiraMessageBatchEvent(timestamp=int(time.time()), session=Session(), messages=list(msgs))


def _tool():
    return SimpleNamespace(text="tool ok", attachments=[])


async def make_plugin(path):
    mod = load(path)
    ctx = Ctx()
    p = mod.MidflightMessagePlugin(ctx, CFG)
    await p.initialize()
    p.debug = False
    ctx.plugin = p
    return p, ctx


async def case_inject_writes_tuple_and_gc_safe(path):
    """注入路径写入元组 + _gc 不炸 + 计数可累加读取。"""
    p, ctx = await make_plugin(path)
    ev = batch(msg(1001, "跑个任务"))
    await p._track_run_start(ev)
    run_id = getattr(ev, "event_id", None) or SID

    # 第一次工具边界：注入 1 条（功能断言：文本必须真实搭车进 tool_result）
    ctx.get_buffer(SID).buffer.append(Shim(msg(1002, "中途补充一")))
    t1 = _tool()
    await p._handle_tool_result(ev, t1)
    entry = p._run_inject_count.get(run_id)
    assert isinstance(entry, tuple), f"写入必须是元组，实际 {type(entry)}: {entry!r}"
    assert entry[0] == 1, f"第一次注入后计数应为 1，实际 {entry!r}"
    assert "中途补充一" in t1.text, f"注入文本未真实进入 tool_result: {t1.text!r}"
    assert "HINT" in t1.text, "引导语应随注入一起进入 tool_result"

    # 第二次工具边界：再注入 1 条（读取端 used 走 [0] 下标，int 会在这里炸）
    ctx.get_buffer(SID).buffer.append(Shim(msg(1003, "中途补充二")))
    t2 = _tool()
    await p._handle_tool_result(ev, t2)
    entry = p._run_inject_count.get(run_id)
    assert isinstance(entry, tuple) and entry[0] == 2, f"计数应累加到 2，实际 {entry!r}"
    assert "中途补充二" in t2.text, f"第二次注入文本未进入 tool_result: {t2.text!r}"

    # 强制执行 _gc：新鲜条目必须保留，且绝不抛 TypeError
    p._last_gc = 0
    p._gc()
    entry = p._run_inject_count.get(run_id)
    assert isinstance(entry, tuple) and entry[0] == 2, f"gc 不应淘汰新鲜条目，实际 {entry!r}"

    # 伪造过期条目：必须被正确淘汰（验证元组解包路径）
    p._run_inject_count["stale-run"] = (5, time.time() - 7200)
    p._last_gc = 0
    p._gc()
    assert "stale-run" not in p._run_inject_count, "过期条目应被淘汰"
    await p.terminate()
    return {"注入写入元组": True, "计数累加": True, "注入真实搭车": True,
            "gc安全": True, "过期淘汰": True}


async def case_quota_enforced_with_tuple(path):
    """配额功能验证：max_inject_per_run=1 时第二条溢出放回 buffer 头、计数不再涨。

    int 事故期间这条链是断的（配额读取 TypeError → 整段注入逻辑被异常吞掉），
    本用例证明修复后配额统计真实生效而非仅仅不报错。
    """
    cfg = {**CFG, "section_limits": {**CFG["section_limits"], "max_inject_per_run": 1}}
    mod = load(path)
    ctx = Ctx()
    p = mod.MidflightMessagePlugin(ctx, cfg)
    await p.initialize()
    p.debug = False
    ctx.plugin = p
    ev = batch(msg(2001, "跑个任务"))
    await p._track_run_start(ev)
    run_id = getattr(ev, "event_id", None) or SID

    # 同一边界塞入 2 条：第 1 条注入、第 2 条溢出放回 buffer 头部
    buf = ctx.get_buffer(SID)
    buf.buffer.append(Shim(msg(2002, "第一条")))
    buf.buffer.append(Shim(msg(2003, "第二条溢出")))
    t = _tool()
    await p._handle_tool_result(ev, t)
    entry = p._run_inject_count.get(run_id)
    assert isinstance(entry, tuple) and entry[0] == 1, f"配额内只应计 1 条，实际 {entry!r}"
    assert "第一条" in t.text and "第二条溢出" not in t.text, \
        f"溢出消息不应进入 tool_result: {t.text!r}"
    assert len(buf.buffer) == 1 and \
        getattr(buf.buffer[0].message, "message_id", None) == "2003", \
        "溢出消息应放回 buffer 头部（保持原顺序）"

    # 下一个边界：配额已用完，buffer 里的消息不再被消费、计数不涨、不抛异常
    t2 = _tool()
    await p._handle_tool_result(ev, t2)
    entry = p._run_inject_count.get(run_id)
    assert isinstance(entry, tuple) and entry[0] == 1, f"配额用完后计数不应再涨，实际 {entry!r}"
    assert "第二条溢出" not in t2.text, "配额用完后不应再注入"
    assert len(buf.buffer) == 1, "未被消费的消息应继续留在 buffer"
    await p.terminate()
    return {"配额上限生效": True, "溢出放回buffer": True, "计数封顶": True}


async def main():
    path = sys.argv[1] if len(sys.argv) > 1 else str(HERE.parent / "main.py")
    path = str(Path(path) / "main.py") if Path(path).is_dir() else path
    for case in (case_inject_writes_tuple_and_gc_safe, case_quota_enforced_with_tuple):
        r = await case(path)
        for k, v in r.items():
            print(f"  PASS {case.__name__}/{k}: {v}")
    print("test_inject_count: ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
