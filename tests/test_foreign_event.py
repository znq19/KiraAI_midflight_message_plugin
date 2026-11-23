"""Midflight：**外来事件**（第三方插件自造的桩事件）不得污染"运行中"状态机。

背景（2026-09-16 用户日志实证）：
  子代理插件（KiraAI-subagent-plugin）用 `_make_stub_event(task.sid)` 造 KiraMessageBatchEvent
  ——**携带发起会话的真实 sid**（为了 file 插件等按会话作用域的管控对子代理与主 AI 语义一致），
  然后由 `_SubagentLLMShim.execute_tool` / 框架 AgentExecutor 用它派发
  ON_TOOL_RESULT / ON_LLM_RESPONSE。

  v1.3.0 的 `_touch_run` 遇到「同 sid、不同事件对象」时会**以最新事件为准重建**运行中状态：
    · 主轮早已结束 → 桩事件凭空造出"幽灵运行中" → 该会话后续消息批次被全部拦截转入
      流入队列，却永远等不到工具边界去注入；
    · 下一个桩事件边界还会把流入队列里的**用户消息灌进子代理的工具结果**并标记已消费
      ——主 AI 永远看不到这条消息（之后 flush 回来还会被"已消费"去重整批掐掉）。

  修法（v1.3.1）：运行中状态**只能由 ON_LLM_REQUEST 建立**；工具边界只允许刷新
  "同一个事件对象"的轮，其余一律忽略（不注入、不还原、不重建）。

用法: python3 tests/test_foreign_event.py [<plugin_dir> ...]
      （与其它套件一样，可直接对比修复前后两个 checkout：
        python3 tests/test_foreign_event.py . ../midflight-1.3.0）
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
from core.provider import LLMResponse  # noqa: E402

SID = "qq:gm:10001"


def load(path):
    spec = importlib.util.spec_from_file_location("mf_foreign_" + Path(path).parent.name, path)
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
        self.published_texts = []

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
            self.published_texts.extend(
                "".join(getattr(e, "text", "") for e in getattr(m, "chain", []))
                for m in (getattr(event, "messages", None) or []))


CFG = {
    "config_version": 2,
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


class StubAdapter:
    """照抄子代理插件的 _STUB_ADAPTER 形状。"""
    enabled = True
    adapter_id = "subagent"
    name = "subagent"
    platform = "subagent"
    description = "SubAgent stub adapter"


def stub_batch(extra=None):
    """照抄子代理插件 _make_stub_event：同 sid、新事件对象、dummy 消息。"""
    dummy = KiraIMMessage(timestamp=int(time.time()), sender=User("subagent", "subagent"),
                          group=Group("10001", "测试群"), message_id="subagent_stub",
                          self_id="subagent", chain=MessageChain([]), is_mentioned=False)
    ev = KiraMessageBatchEvent(timestamp=int(time.time()), session=Session(),
                               messages=[dummy])
    ev.adapter = StubAdapter()
    ev.extra = dict(extra or {})
    return ev


async def make_plugin(path):
    mod = load(path)
    ctx = Ctx()
    p = mod.MidflightMessagePlugin(ctx, CFG)
    await p.initialize()
    p.debug = False
    ctx.plugin = p
    return p, ctx


def _resp(idx, tools):
    r = LLMResponse(text_response="<msg />", tool_calls=tools)
    r.agent_step_index = idx
    return r


def _tool():
    return SimpleNamespace(text="tool ok", attachments=[])


async def final_check(p, ctx, mid=9901):
    """再发一条消息，看是否被拦且没人处理。"""
    ev = batch(msg(mid, "这条会被拦吗"))
    await p.on_batch_dedup(ev)
    return {"再次发言被拦截": ev.is_stopped,
            "卡在流入队列": sum(len(v) for v in p._pending_inject.values())}


# ---------------------------------------------------------------- 场景

async def f1_idle_stub_never_creates_run(path):
    """空闲时桩事件到达 → 不得凭空造出"运行中"；后续消息照常放行。"""
    p, ctx = await make_plugin(path)
    stub = stub_batch()
    await p._ensure_stop_checkpoint(stub, _resp(1, [{"id": "s1"}]))
    await p._handle_tool_result(stub, _tool())
    run_created = p._get_active_run(SID) is not None
    r = {"桩事件未被登记为运行中": not run_created}
    r.update(await final_check(p, ctx))
    await p.terminate()
    return r


async def f2_stub_must_not_touch_real_run(path):
    """真实轮进行中：桩事件不得篡改本轮，更不得把用户消息灌进子代理的工具结果。"""
    p, ctx = await make_plugin(path)
    real = batch(msg(1001, "跑个任务"))
    await p._track_run_start(real)
    await p._ensure_stop_checkpoint(real, _resp(1, [{"id": "t1"}]))

    # 运行中插话：被拦进流入队列（正常行为）
    b = batch(msg(2001, "这是用户在插话"))
    await p.on_batch_dedup(b)
    queued_before = sum(len(v) for v in p._pending_inject.values())

    # 子代理后台任务执行工具：桩事件派发 ON_TOOL_RESULT（+ ON_LLM_RESPONSE）
    stub = stub_batch()
    await p._ensure_stop_checkpoint(stub, _resp(1, [{"id": "s1"}]))
    stub_tool = _tool()
    await p._handle_tool_result(stub, stub_tool)

    queued_after = sum(len(v) for v in p._pending_inject.values())
    run = p._run_active.get(SID)
    trampled = (run is None) or (run.get("event") is not real)
    polluted = "这是用户在插话" in (stub_tool.text or "")

    r = {"插话已入队": queued_before == 1,
         "桩事件未吃掉队列": queued_after == 1,
         "真实轮状态未被篡改": not trampled,
         "用户消息没被灌进子代理工具结果": not polluted}

    # 真实轮的工具边界仍应正常注入（功能未失效）
    real_tool = _tool()
    await p._handle_tool_result(real, real_tool)
    r["真实轮工具边界仍能注入"] = "这是用户在插话" in (real_tool.text or "")
    r["注入后队列已空"] = sum(len(v) for v in p._pending_inject.values()) == 0
    await p.terminate()
    return r


async def f3_normal_run_after_stub(path):
    """桩事件之后仍能正常开新轮（对照：修复前这里会被幽灵拦截）。"""
    p, ctx = await make_plugin(path)
    # 先来一轮真实轮 + 桩事件污染窗口
    real = batch(msg(1001, "跑个任务"))
    await p._track_run_start(real)
    stub = stub_batch()
    await p._handle_tool_result(stub, _tool())
    await p._ensure_stop_checkpoint(real, _resp(2, []))     # 主轮结束

    b1 = batch(msg(3001, "主轮结束后的新消息"))
    await p.on_batch_dedup(b1)
    first_stopped = b1.is_stopped
    # 结束之后来的桩事件边界也不得复活状态
    await p._handle_tool_result(stub_batch(), _tool())
    b2 = batch(msg(3002, "再一条"))
    await p.on_batch_dedup(b2)
    r = {"主轮后新批次未拦": not first_stopped,
         "迟到桩事件未复活状态": p._get_active_run(SID) is None,
         "后续批次未拦": not b2.is_stopped,
         "流入队列为空": sum(len(v) for v in p._pending_inject.values()) == 0}
    await p.terminate()
    return r


async def f4_final_result_is_authoritative(path):
    """ON_FINAL_RESULT（框架 v2.34.4 起派发）= 一轮结束的权威信号 → 收尾 + 还原 + 立即放行。"""
    p, ctx = await make_plugin(path)
    if not hasattr(p, "_on_final_result"):
        return {"该版本无 ON_FINAL_RESULT 钩子": True}
    real = batch(msg(1001, "跑个任务"))
    await p._track_run_start(real)
    await p._ensure_stop_checkpoint(real, _resp(1, [{"id": "t1"}]))
    b = batch(msg(2001, "插话"))
    await p.on_batch_dedup(b)
    queued = sum(len(v) for v in p._pending_inject.values())

    await p._on_final_result(real, SimpleNamespace(step_results=[]))
    r = {"插话已入队": queued == 1,
         "运行中已清": p._get_active_run(SID) is None,
         "队列已清": sum(len(v) for v in p._pending_inject.values()) == 0,
         "消息已还原并立即 flush 成交付批次": any(
             "插话" in t for t in ctx.published_texts),
         "还原批次已放行(不循环)": any(stopped is False for _eid, stopped in ctx.published)}
    r.update(await final_check(p, ctx, 9902))
    await p.terminate()
    return r


async def f5_final_result_ignores_foreign(path):
    """外来事件（桩事件）调 _on_final_result 不得动真实轮状态。"""
    p, ctx = await make_plugin(path)
    if not hasattr(p, "_on_final_result"):
        return {"该版本无 ON_FINAL_RESULT 钩子": True}
    real = batch(msg(1001, "跑个任务"))
    await p._track_run_start(real)
    await p._on_final_result(stub_batch(), SimpleNamespace(step_results=[]))
    alive = p._run_active.get(SID)
    r = {"真实轮未被误清": alive is not None and alive.get("event") is real}
    # 收尾钩子重复调用（框架重放/多个 hook）必须幂等
    await p._on_final_result(real, SimpleNamespace(step_results=[]))
    await p._on_final_result(real, SimpleNamespace(step_results=[]))
    r["重复调用幂等"] = p._get_active_run(SID) is None
    await p.terminate()
    return r


async def f6_late_boundary_after_finish(path):
    """轮已收尾后的迟到工具边界（一次响应多个 tool_calls）→ 忽略（回归保护）。"""
    p, ctx = await make_plugin(path)
    real = batch(msg(1001, "跑个任务"))
    await p._track_run_start(real)
    await p._handle_tool_result(real, _tool())            # 第一个边界（正常）
    await p._ensure_stop_checkpoint(real, _resp(2, []))   # 最终步 → 收尾
    await p._handle_tool_result(real, _tool())            # 迟到边界
    r = {"收尾后无残留": p._get_active_run(SID) is None}
    r.update(await final_check(p, ctx, 9903))
    await p.terminate()
    return r


# 这些键为真 = 失败（沿用 test_stuck_paths 的 final_check 口径）；其余键为真 = 通过
BAD_WHEN_TRUE = {"再次发言被拦截", "卡在流入队列", "状态残留", "队列残留"}
KEEP_ZERO = {"卡在流入队列", "队列残留"}


def _judge(r):
    bad = []
    for k, v in r.items():
        if k in KEEP_ZERO:
            if v:
                bad.append(k)
        elif k in BAD_WHEN_TRUE:
            if v:
                bad.append(k)
        elif v is False:
            bad.append(k)
    return bad


SCENARIOS = [
    ("F1 空闲时桩事件不造「运行中」", f1_idle_stub_never_creates_run),
    ("F2 真实轮进行中：桩事件不篡改状态、不吃队列、不灌消息", f2_stub_must_not_touch_real_run),
    ("F3 桩事件后仍能正常开新轮（对照）", f3_normal_run_after_stub),
    ("F4 ON_FINAL_RESULT 权威收尾（清状态+还原+放行）", f4_final_result_is_authoritative),
    ("F5 ON_FINAL_RESULT 对外来事件不响应 + 幂等", f5_final_result_ignores_foreign),
    ("F6 收尾后的迟到边界被忽略（回归）", f6_late_boundary_after_finish),
]


async def main():
    dirs = sys.argv[1:] or ["."]
    failed = 0
    for path in [str(Path(d) / "main.py") for d in dirs]:
        print("=" * 78)
        print(f"### {path}")
        for name, fn in SCENARIOS:
            try:
                r = await fn(path)
                bad = _judge(r)
                if bad:
                    failed += 1
                    print(f"  ✗ FAIL   {name}")
                    print(f"            {r}")
                else:
                    print(f"  ✓ PASS   {name}")
                    print(f"            {r}")
            except Exception as e:
                failed += 1
                print(f"  ERR       {name}: {type(e).__name__}: {e}")
    print("=" * 78)
    print("ALL TESTS PASSED" if failed == 0 else f"{failed} CHECK(S) FAILED")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
