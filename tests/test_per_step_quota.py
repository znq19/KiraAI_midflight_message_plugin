"""Per-step quota / dynamic config / final-boundary regression tests.

Run: python3 tests/test_per_step_quota.py [plugin_dir]
"""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import harness as H  # noqa: E402

SID = H.DEFAULT_SID


class LiveCfg:
    def __init__(self, **overrides):
        self.values = {
            "bot_config.agent.max_tool_loop": 50,
            "bot_config.agent.tool_call_timeout": 60,
            "bot_config.bot.max_buffer_messages": 5,
            "bot_config.bot.max_message_interval": 2,
        }
        self.values.update(overrides)

    def get_config(self, key, default=None):
        return self.values.get(key, default)


def cfg(step=0, run=0, flow="any", wake=None):
    return {
        **H.CFG,
        "config_version": 2,
        "section_flow": {
            **H.CFG["section_flow"],
            "flow_method_dm": flow,
            "flow_method_group": flow,
            "wake_keywords": list(wake or []),
        },
        "section_limits": {
            **H.CFG["section_limits"],
            "max_inject_per_step": step,
            "max_inject_per_run": run,
        },
    }


def msg(mid, text, mentioned=False):
    m = H.FakeMessage(mid, text)
    m.is_mentioned = mentioned
    return m


async def make(plugin_path, config, live=None):
    mod = H.load_plugin(plugin_path)
    ctx = H.FakeCtx()
    ctx.config = live if live is not None else LiveCfg()
    p = mod.MidflightMessagePlugin(ctx, config)
    await p.initialize()
    return p, ctx


async def start(p, mid="100", text="跑个任务"):
    ev = H.batch(H.FakeMessage(mid, text))
    await p._track_run_start(ev)
    return ev


async def step(p, ev, index, tools=True):
    await p._ensure_stop_checkpoint(
        ev, H.FakeResp(index, [{"id": f"c{index}", "type": "function",
                                "function": {"name": "probe", "arguments": "{}"}}] if tools else [])
    )


async def boundary(p, ev):
    t = H.FakeToolResult()
    await p._handle_tool_result(ev, t)
    return t


async def case_step_quota_resets(path):
    p, ctx = await make(path, cfg(step=2, run=0))
    await p.initialize()
    ev = await start(p)
    buf = ctx.get_buffer(SID)
    for i in range(6):
        buf.buffer.append(H.BufferedShim(msg(str(200 + i), f"m{i}")))
    injected = []
    for i in (1, 2, 3):
        await step(p, ev, i)
        t = await boundary(p, ev)
        injected += [j for j in range(6) if f"m{j}" in t.text]
    assert injected == [0, 1, 2, 3, 4, 5], injected
    await p.terminate()
    return {"每步 2 条、跨 step 重置": True, "6 条最终全部注入": True}


async def case_same_step_shared(path):
    p, ctx = await make(path, cfg(step=2, run=0))
    ev = await start(p)
    buf = ctx.get_buffer(SID)
    for i in range(3):
        buf.buffer.append(H.BufferedShim(msg(str(300 + i), f"m{i}")))
    await step(p, ev, 1)
    t1 = await boundary(p, ev)
    t2 = await boundary(p, ev)
    assert "m0" in t1.text and "m1" in t1.text and "m2" not in t1.text, t1.text
    assert "m2" not in t2.text, t2.text
    assert len(p._pending_inject.get(SID, [])) == 1
    await p.terminate()
    return {"同一步多个工具边界共享 2 条": True, "溢出留到下一 step": True}


async def case_run_cap(path):
    p, ctx = await make(path, cfg(step=2, run=3))
    ev = await start(p)
    buf = ctx.get_buffer(SID)
    for i in range(5):
        buf.buffer.append(H.BufferedShim(msg(str(400 + i), f"m{i}")))
    counts = []
    for i in (1, 2, 3):
        await step(p, ev, i)
        t = await boundary(p, ev)
        counts.append(sum(f"m{j}" in t.text for j in range(5)))
    assert counts == [2, 1, 0], counts
    assert len(p._pending_inject.get(SID, [])) == 2
    await p._finish_run(SID)
    assert len(ctx.flushes) == 1, ctx.flushes
    await p.terminate()
    return {"整轮 3 条硬上限": True, "溢出轮末 flush": True}


async def case_flow_methods(path):
    # any：所有消息都可注入
    p, ctx = await make(path, cfg(step=5, run=0, flow="any"))
    ev = await start(p)
    buf = ctx.get_buffer(SID)
    for i in range(3):
        buf.buffer.append(H.BufferedShim(msg(str(500 + i), f"m{i}")))
    await step(p, ev, 1)
    t = await boundary(p, ev)
    assert all(f"m{i}" in t.text for i in range(3))
    await p.terminate()

    # all：只有唤醒词 hey 通过，普通消息不消耗配额
    p, ctx = await make(path, cfg(step=2, run=0, flow="all", wake=["hey"]))
    ev = await start(p)
    buf = ctx.get_buffer(SID)
    buf.buffer.append(H.BufferedShim(msg("601", "hey 看这个")))
    buf.buffer.append(H.BufferedShim(msg("602", "普通闲聊")))
    await step(p, ev, 1)
    t = await boundary(p, ev)
    assert "hey 看这个" in t.text and "普通闲聊" not in t.text
    assert p._run_step_count[ev.event_id][1] == 1, "只有注入的消息消耗步配额"
    await p.terminate()

    # at：只有 is_mentioned 通过，普通/唤醒词不通过
    p, ctx = await make(path, cfg(step=2, run=0, flow="at", wake=[]))
    ev = await start(p)
    buf = ctx.get_buffer(SID)
    buf.buffer.append(H.BufferedShim(msg("701", "@bot 看这个", mentioned=True)))
    buf.buffer.append(H.BufferedShim(msg("702", "普通闲聊")))
    await step(p, ev, 1)
    t = await boundary(p, ev)
    assert "@bot 看这个" in t.text and "普通闲聊" not in t.text
    await p.terminate()
    return {"any 全注入": True, "all 只注入唤醒词": True, "at 只注入 @": True,
            "被过滤消息不消耗配额": True}


async def case_wait_not_abandon_quota(path):
    c = cfg(step=1, run=0)
    c["section_basic"]["inject_timeout_steps"] = 1
    p, ctx = await make(path, c)
    ev = await start(p)
    buf = ctx.get_buffer(SID)
    for i in range(3):
        buf.buffer.append(H.BufferedShim(msg(str(750 + i), f"m{i}")))
    got = []
    for i in (1, 2, 3):
        await step(p, ev, i)
        t = await boundary(p, ev)
        got += [j for j in range(3) if f"m{j}" in t.text]
    assert got == [0, 1, 2], got
    await p.terminate()
    return {"步配额阻塞不被 inject_timeout_steps 误放弃": True}


async def case_dynamic_and_unlimited(path):
    live = LiveCfg(**{"bot_config.agent.max_tool_loop": 5})
    p, ctx = await make(path, cfg(step=0, run=0), live=live)
    c = p._eff_config(None)
    assert c["_max_inject_per_step"] == 5 and c["_max_inject"] == 25, c
    await p.terminate()

    live = LiveCfg()
    p, ctx = await make(path, cfg(step=0, run=0), live=live)
    c = p._eff_config(None)
    assert c["_max_inject_per_step"] == 5 and c["_max_inject"] == 250, c
    live.values["bot_config.bot.max_buffer_messages"] = 4
    live.values["bot_config.agent.max_tool_loop"] = 30
    c = p._eff_config(None)
    assert c["_max_inject_per_step"] == 4 and c["_max_inject"] == 120, c
    await p.terminate()

    live = LiveCfg(**{"bot_config.agent.max_tool_loop": -1})
    p, ctx = await make(path, cfg(step=5, run=0), live=live)
    c = p._eff_config(None)
    assert c["_max_inject"] == -1, c
    await p.terminate()

    p, ctx = await make(path, cfg(step=-1, run=-1))
    ev = await start(p)
    buf = ctx.get_buffer(SID)
    for i in range(10):
        buf.buffer.append(H.BufferedShim(msg(str(800 + i), f"m{i}")))
    await step(p, ev, 1)
    t = await boundary(p, ev)
    assert all(f"m{i}" in t.text for i in range(10)), "两个 -1 应全部注入"
    await p.terminate()
    return {"动态 5/250→4/120": True, "max_tool_loop=-1 整轮无限制": True,
            "配额 -1 无限制": True}


async def case_terminate_restore(path):
    p, ctx = await make(path, cfg(step=5, run=0))
    p._pending_inject[SID] = [(H.BufferedShim(msg("950", "待还原")), "待还原", 0)]
    await p.terminate()
    assert len(ctx.flushes) == 1, ctx.flushes
    assert len(ctx.get_buffer(SID).buffer) == 1, "卸载时应还原 pending 到 buffer"
    return {"卸载/重载还原未注入消息": True}


async def case_final_boundaries(path):
    # 11/50 无工具：立即收尾并 flush，不注入
    p, ctx = await make(path, cfg(step=5, run=0))
    ev = await start(p)
    p._pending_inject[SID] = [(H.BufferedShim(msg("900", "待注入")), "待注入", 0)]
    await step(p, ev, 11, tools=False)
    assert SID not in p._run_active and not p._pending_inject.get(SID)
    assert len(ctx.flushes) == 1
    await p.terminate()

    # 50/50 有工具：ending，工具边界收尾，不注入
    p, ctx = await make(path, cfg(step=5, run=0))
    ev = await start(p)
    p._pending_inject[SID] = [(H.BufferedShim(msg("901", "待注入")), "待注入", 0)]
    await step(p, ev, 50, tools=True)
    assert p._run_active[SID]["ending"] is True
    t = await boundary(p, ev)
    assert "待注入" not in t.text
    assert SID not in p._run_active and not p._pending_inject.get(SID)
    assert len(ctx.flushes) == 1
    await p.terminate()

    # 50/50 无工具：立即收尾
    p, ctx = await make(path, cfg(step=5, run=0))
    ev = await start(p)
    p._pending_inject[SID] = [(H.BufferedShim(msg("902", "待注入")), "待注入", 0)]
    await step(p, ev, 50, tools=False)
    assert SID not in p._run_active
    assert len(ctx.flushes) == 1
    await p.terminate()
    return {"11/50 文本收尾": True, "50/50 工具边界不注入": True,
            "50/50 文本收尾": True}


async def main():
    path = sys.argv[1] if len(sys.argv) > 1 else str(HERE.parent)
    path = str(Path(path).parent) if Path(path).is_file() else str(Path(path))
    results = []
    for case in (case_step_quota_resets, case_same_step_shared, case_run_cap,
                 case_flow_methods, case_wait_not_abandon_quota,
                 case_dynamic_and_unlimited, case_terminate_restore,
                 case_final_boundaries):
        results.append(await case(path))
    for r in results:
        print("  PASS", r)
    print("test_per_step_quota: ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
