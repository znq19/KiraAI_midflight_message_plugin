"""Midflight：除了"最后一步多个 tool_calls"之外，还有哪些情况会留下"幽灵运行中"。

用法: python3 repro_full/test_stuck_paths.py <midflight/main.py> [<...>]

每个场景之后都做同一件事：**再发一条用户消息**，看它会不会被拦截
（被拦截 = 用户之后的发言都被吞、又没人处理 ⇒ 只能等心跳超时兜底）。
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
    spec = importlib.util.spec_from_file_location("mf_stuck_" + Path(path).parent.name, path)
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
        # 模拟框架：flush 出的新批次会回到 ON_IM_BATCH_MESSAGE ⇒ 插件守卫再看一次
        if getattr(self, "plugin", None) is not None:
            await self.plugin.on_batch_dedup(event)
            self.published.append((event.event_id[:8], event.is_stopped))


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


async def make_plugin(path):
    mod = load(path)
    ctx = Ctx()
    p = mod.MidflightMessagePlugin(ctx, CFG)
    await p.initialize()
    p.debug = False
    ctx.plugin = p
    return p, ctx


async def final_check(p, ctx, mid=9001):
    """再发一条消息，看是否被拦且没人处理。"""
    ev = batch(msg(mid, "这条会被拦吗"))
    await p.on_batch_dedup(ev)
    return {"再次发言被拦截": ev.is_stopped,
            "卡在流入队列": sum(len(v) for v in p._pending_inject.values())}


def _resp(idx, tools):
    r = LLMResponse(text_response="<msg />", tool_calls=tools)
    r.agent_step_index = idx
    return r


def _tool():
    return SimpleNamespace(text="tool ok", attachments=[])


async def s1_stop_word_tool_boundary(path):
    """运行中命中停止词（工具边界 drain 路径）→ 本轮被停 ⇒ 状态清了没？"""
    p, ctx = await make_plugin(path)
    ev = batch(msg(1001, "跑个任务"))
    await p._track_run_start(ev)
    await p._ensure_stop_checkpoint(ev, _resp(1, [{"id": "t1"}]))
    ctx.get_buffer(SID).buffer.append(Shim(msg(1002, "停")))
    await p._handle_tool_result(ev, _tool())
    r = {"本轮被停": ev.is_stopped, "状态残留": p._get_active_run(SID) is not None}
    r.update(await final_check(p, ctx))
    await p.terminate()
    return r


async def s2_stop_word_batch_path(path):
    """运行中收到"停"批次（批次拦截路径）→ 新一轮被停 ⇒ 状态清了没？"""
    p, ctx = await make_plugin(path)
    ev = batch(msg(1001, "跑个任务"))
    p._run_active[SID] = {"event": ev, "ts": time.time(), "ending": False}
    b = batch(msg(1002, "停"))
    await p.on_batch_dedup(b)
    r = {"本轮被停": b.is_stopped and ev.is_stopped,
         "状态残留": p._get_active_run(SID) is not None}
    r.update(await final_check(p, ctx))
    await p.terminate()
    return r


async def s3_run_dies_after_llm_request(path):
    """ON_LLM_REQUEST 已登记，但本轮根本没跑起来（被别的插件 stop / 异常）。

    先发一条消息（此刻会被拦，预期行为），再看最终状态：
      有看门狗 → grace 秒后应自愈（队列清空 + 运行中标记清掉 + 后续消息不再被拦）
      没看门狗 → 只能等心跳超时（默认 180s），此刻仍卡住
    """
    p, ctx = await make_plugin(path)
    watchdog = hasattr(p, "inject_grace_seconds")
    if watchdog:
        p.inject_grace_seconds = 1
        p._ensure_watchdog()
    ev = batch(msg(1001, "跑个任务"))
    await p._track_run_start(ev)                 # 之后没有任何收尾信号
    b = batch(msg(8001, "后续发言"))
    await p.on_batch_dedup(b)
    first = b.is_stopped
    if watchdog:
        await asyncio.sleep(5.5)                 # 等看门狗（软放行 + 升级硬放行）
    r = {"首次被拦(预期)": first,
         "状态残留": p._get_active_run(SID) is not None,
         "队列残留": sum(len(v) for v in p._pending_inject.values())}
    r.update(await final_check(p, ctx, 8002))
    await p.terminate()
    return r


async def s4_normal_run_no_text(path):
    """一轮正常跑完（有工具、无最终文字）= 1.2.9 已修的那条，作对照。"""
    p, ctx = await make_plugin(path)
    ev = batch(msg(1001, "跑个任务"))
    await p._track_run_start(ev)
    await p._ensure_stop_checkpoint(ev, _resp(2, [{"id": "t1"}]))
    for _ in range(3):
        await p._handle_tool_result(ev, _tool())
    r = {"状态残留": p._get_active_run(SID) is not None}
    r.update(await final_check(p, ctx))
    await p.terminate()
    return r


async def s5_watchdog(path):
    """本轮没有收尾信号（没跑起来 / 中途异常）→ 看门狗应在 grace 秒后放行。"""
    p, ctx = await make_plugin(path)
    if not hasattr(p, "inject_grace_seconds"):
        return {"不支持看门狗": True}
    p.inject_grace_seconds = 1                      # 测试用：1 秒
    p._ensure_watchdog()
    ev = batch(msg(1001, "跑个任务"))
    await p._track_run_start(ev)                    # 之后没有任何收尾信号
    b = batch(msg(7001, "后续发言"))
    await p.on_batch_dedup(b)
    first = b.is_stopped
    await asyncio.sleep(5.5)                        # 等看门狗（软放行 + 硬放行）
    leaked = sum(len(v) for v in p._pending_inject.values())
    run_cleared = p._get_active_run(SID) is None
    let_through = any(stopped is False for _eid, stopped in ctx.published)
    probe = await final_check(p, ctx, 7002)
    await p.terminate()
    return {"首次被拦": first, "看门狗后队列残留": leaked, "运行中已清": run_cleared,
            "还原批次已放行(不循环)": let_through, **probe}


async def s6_watchdog_no_false_hard(path):
    """软放行之后本轮又有活动（说明只是这一步慢）→ 升级为硬放行必须被撤销。"""
    p, ctx = await make_plugin(path)
    if not hasattr(p, "inject_grace_seconds"):
        return {"不支持看门狗": True}
    p.inject_grace_seconds = 1
    p._ensure_watchdog()
    ev = batch(msg(1001, "跑个任务"))
    await p._track_run_start(ev)
    b = batch(msg(7101, "插话"))
    await p.on_batch_dedup(b)
    await asyncio.sleep(1.6)                    # 等软放行发生
    soft_released = sum(len(v) for v in p._pending_inject.values()) == 0
    still_tracked = p._get_active_run(SID) is not None
    await p._handle_tool_result(ev, _tool())    # ★ 活动：本轮只是这一步慢，活过来了
    await asyncio.sleep(2.0)                    # 再等一个升级窗口
    alive = p._get_active_run(SID) is not None   # 不该被硬清
    await p.terminate()
    return {"软放行发生": soft_released, "软放行后仍保留状态": still_tracked,
            "有活动后未被硬清": alive}


async def s7_last_step_no_text_is_immediate(path):
    """「步数用尽、最后一步不发文字」：消息该在工具边界当场注入/还原，不该等看门狗。"""
    p, ctx = await make_plugin(path)
    if hasattr(p, "inject_grace_seconds"):
        p.inject_grace_seconds = 30          # 把看门狗调得很迟钝，验证"根本不靠它"
        p._ensure_watchdog()
    ev = batch(msg(1001, "跑个任务"))
    await p._track_run_start(ev)
    await p._ensure_stop_checkpoint(ev, _resp(2, [{"id": "t1"}]))   # step2/2 = 最后一步，带工具
    # 运行中插话：被拦进流入队列
    b = batch(msg(2001, "插话一句"))
    await p.on_batch_dedup(b)
    queued = sum(len(v) for v in p._pending_inject.values())
    t0 = time.time()
    await p._handle_tool_result(ev, _tool())     # 末步工具边界 → 应立即收尾
    dt = time.time() - t0
    left = sum(len(v) for v in p._pending_inject.values())
    published = len(ctx.published)                 # 还原后立即 flush → 成新批次
    await p.terminate()
    return {"插话被拦": queued == 1, "边界处耗时(秒)": round(dt, 3),
            "当场收尾(不等看门狗)": dt < 0.5, "已还原并立即放行": published >= 1,
            "流入队列已空": left == 0}


async def s8_watchdog_off_timeout_release(path):
    """看门狗关闭（默认）时，"本轮没跑起来"这类路径靠心跳超时放行 —— 不丢，但要等。

    这里把 _active_timeout 调成 1 秒来模拟心跳过期：新消息到达时应触发
    "运行中判定超时" → 把积压的拦截消息还原回缓冲 + 放行新批次。
    """
    p, ctx = await make_plugin(path)
    if hasattr(p, "inject_grace_seconds"):
        p.inject_grace_seconds = 0            # 默认：看门狗关闭
    p._active_timeout = 1.0                   # 模拟心跳过期（真实默认 = LLM 超时 + 工具超时）
    ev = batch(msg(1001, "跑个任务"))
    await p._track_run_start(ev)              # 之后没有任何收尾信号
    b1 = batch(msg(8001, "第一条"))
    await p.on_batch_dedup(b1)
    stuck_in_queue = sum(len(v) for v in p._pending_inject.values())
    await asyncio.sleep(1.2)                  # 等心跳过期
    b2 = batch(msg(8002, "第二条"))
    await p.on_batch_dedup(b2)
    released = sum(len(v) for v in p._pending_inject.values())
    restored = ctx.get_buffer(SID).get_length()
    run_gone = p._get_active_run(SID) is None
    await p.terminate()
    return {"（关看门狗）第一条被拦": stuck_in_queue == 1,
            "心跳过期后队列已清": released == 0,
            "旧消息已还原回缓冲": restored >= 1,
            "新批次已放行": not b2.is_stopped,
            "运行中标记已清": run_gone}


SCENARIOS = [
    ("S1 停止词（工具边界路径）", s1_stop_word_tool_boundary),
    ("S2 停止词（批次拦截路径）", s2_stop_word_batch_path),
    ("S3 本轮没跑起来（ON_LLM_REQUEST 后被 stop / 异常）", s3_run_dies_after_llm_request),
    ("S4 正常跑完（对照：1.2.9 已修）", s4_normal_run_no_text),
    ("S5 看门狗兜底（无收尾信号）", s5_watchdog),
    ("S6 看门狗不误伤：这一步慢（有活动）不硬清状态", s6_watchdog_no_false_hard),
    ("S7 步数用尽无文字：末步边界当场收尾（不等看门狗）", s7_last_step_no_text_is_immediate),
    ("S8 关看门狗时：靠心跳超时放行（不丢，但要等）", s8_watchdog_off_timeout_release),
]


async def main():
    dirs = sys.argv[1:] or ["."]
    for path in [str(Path(d) / "main.py") for d in dirs]:
        print("=" * 78)
        print(f"### {path}")
        for name, fn in SCENARIOS:
            try:
                r = await fn(path)
                stuck = r.get("状态残留") and r.get("再次发言被拦截")
                mark = "✗ 卡住" if stuck else ("✓ 干净" if not r.get("状态残留") else "≈ 残留但不拦")
                print(f"  {mark:<9}{name}")
                print(f"            {r}")
            except Exception as e:
                print(f"  ERR       {name}: {type(e).__name__}: {e}")


asyncio.run(main())
