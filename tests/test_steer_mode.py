"""Steer mode (injection_mode=steer) regression tests.

Run: python3 tests/test_steer_mode.py [plugin_dir]
"""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import harness as H  # noqa: E402
from core.chat.message_elements import Image  # noqa: E402

SID = H.DEFAULT_SID


class LiveCfg:
    def __init__(self, loop=50, buffer=5):
        self.values = {
            "bot_config.agent.max_tool_loop": loop,
            "bot_config.agent.tool_call_timeout": 60,
            "bot_config.bot.max_buffer_messages": buffer,
            "bot_config.bot.max_message_interval": 2,
            "bot_config.capabilities.image_recognition.mode": "vlm_description",
        }

    def get_config(self, key, default=None):
        return self.values.get(key, default)


class SteerCtx(H.FakeCtx):
    def __init__(self, result=True):
        super().__init__()
        self.config = LiveCfg()
        self.steer_result = result
        self.steer_calls = []
        self.message_processor = None

    def steer_main_agent(self, sid, message):
        self.steer_calls.append((sid, message))
        return self.steer_result


class MediaService:
    async def build_native_content(self, message, session_id):
        return [
            {"type": "text", "text": message.message_str or ""},
            {"type": "kira_image_ref", "path": "session/x.png",
             "mime_type": "image/png", "detail": "high"},
        ]


class Processor:
    def __init__(self, media):
        self.im_workflow = SimpleNamespace(
            ctx=SimpleNamespace(message_media=media))


def cfg(mode="steer", passthrough=False, step=5, run=0):
    return {
        **H.CFG,
        "config_version": 2,
        "section_basic": {
            **H.CFG["section_basic"],
            "injection_mode": mode,
            "system_trigger_passthrough": passthrough,
        },
        "section_flow": {**H.CFG["section_flow"], "flow_method_dm": "any",
                         "flow_method_group": "any"},
        "section_limits": {
            **H.CFG["section_limits"],
            "max_inject_per_step": step,
            "max_inject_per_run": run,
        },
    }


async def make(path, config, result=True, media=None):
    mod = H.load_plugin(path)
    ctx = SteerCtx(result=result)
    if media is not None:
        ctx.message_processor = Processor(media)
    p = mod.MidflightMessagePlugin(ctx, config)
    await p.initialize()
    return p, ctx


async def start(p, mid="100"):
    ev = H.batch(H.FakeMessage(mid, "任务"))
    await p._track_run_start(ev)
    return ev


async def step(p, ev, idx, tools=True):
    await p._ensure_stop_checkpoint(
        ev, H.FakeResp(idx, [{"id": f"c{idx}", "type": "function",
                              "function": {"name": "probe", "arguments": "{}"}}] if tools else []))


async def case_batch_steer_true(path):
    p, ctx = await make(path, cfg())
    ev = await start(p)
    p._wait_steps[SID] = 2
    b = H.batch(H.FakeMessage("201", "m1"), H.FakeMessage("202", "m2"))
    await p.on_batch_dedup(b)
    assert b.is_stopped, "steer accepted must stop batch"
    assert len(ctx.steer_calls) == 1, ctx.steer_calls
    assert not p._pending_inject.get(SID), p._pending_inject
    assert SID not in p._wait_steps, "steer success must clear stale wait counter"
    assert p._run_inject_count.get(ev.event_id, (0, 0))[0] == 2
    await p.terminate()
    return {"批次 steer 接受": True, "批次被掐": True, "计数 2": True}


async def case_batch_steer_false(path):
    p, ctx = await make(path, cfg(), result=False)
    ev = await start(p)
    b = H.batch(H.FakeMessage("211", "m1"), H.FakeMessage("212", "m2"))
    await p.on_batch_dedup(b)
    assert b.is_stopped, "false must fall back to defer, not let a parallel turn through"
    assert len(ctx.steer_calls) == 1
    assert len(p._pending_inject.get(SID, [])) == 2, p._pending_inject
    assert p._run_inject_count.get(ev.event_id, (0, 0))[0] == 0
    await p._finish_run(SID)
    assert len(ctx.flushes) == 1
    await p.terminate()
    return {"steer False 不消费": True, "转入轮末队列": True, "轮末 flush": True}


async def case_tool_steer(path):
    p, ctx = await make(path, cfg())
    ev = await start(p)
    ctx.get_buffer(SID).buffer.append(H.BufferedShim(H.FakeMessage("221", "m1")))
    await step(p, ev, 1)
    t = H.FakeToolResult()
    await p._handle_tool_result(ev, t)
    assert len(ctx.steer_calls) == 1
    assert t.text == "No files found.", "steer mode must not touch tool_result"
    assert not p._pending_inject.get(SID)
    assert p._run_inject_count.get(ev.event_id, (0, 0))[0] == 1
    await p.terminate()
    return {"工具边界 steer": True, "不写 tool_result": True}


async def case_tool_steer_false(path):
    p, ctx = await make(path, cfg(), result=False)
    ev = await start(p)
    ctx.get_buffer(SID).buffer.append(H.BufferedShim(H.FakeMessage("231", "m1")))
    await step(p, ev, 1)
    t = H.FakeToolResult()
    await p._handle_tool_result(ev, t)
    assert t.text == "No files found."
    assert len(p._pending_inject.get(SID, [])) == 1
    assert p._run_inject_count.get(ev.event_id, (0, 0))[0] == 0
    await p.terminate()
    return {"工具边界 steer False 只 defer": True, "不写 tool_result": True}


async def case_stop_and_control(path):
    p, ctx = await make(path, cfg())
    ev = await start(p)
    stop = H.FakeMessage("241", "停")
    normal = H.FakeMessage("242", "普通")
    b = H.batch(stop, normal)
    await p.on_batch_dedup(b)
    assert b.is_stopped
    assert not ctx.steer_calls, "stop must not call steer"
    assert any(getattr(x.message, "message_id", None) == "242"
               for x in ctx.get_buffer(SID).buffer), ctx.get_buffer(SID).buffer
    assert ctx.flushes, "stop leftover should be flushed to a new turn"
    await p.terminate()

    p, ctx = await make(path, cfg())
    ev = await start(p)
    control = H.batch(H.FakeMessage("251", "normal"))
    control.extra = {"merger_handoff": True}
    await p.on_batch_dedup(control)
    assert not control.is_stopped and not ctx.steer_calls
    await p.terminate()
    return {"停止词不 steer": True, "control 批次不接管": True}


async def case_system_trigger_a(path):
    p, ctx = await make(path, cfg(passthrough=False))
    ev = await start(p)
    m = H.FakeMessage("261", "主动提示")
    m.sender.user_id = "system_proactive_dm"
    m.is_mentioned = True
    b = H.batch(m)
    await p.on_batch_dedup(b)
    assert b.is_stopped and len(ctx.steer_calls) == 1, ctx.steer_calls
    await p.terminate()

    p, ctx = await make(path, cfg(passthrough=True))
    ev = await start(p)
    m = H.FakeMessage("271", "主动提示")
    m.sender.user_id = "system_proactive_dm"
    m.is_mentioned = True
    b = H.batch(m)
    await p.on_batch_dedup(b)
    assert not b.is_stopped and not ctx.steer_calls, (b.is_stopped, ctx.steer_calls)
    await p.terminate()
    return {"system trigger A: false→steer": True, "true→放行": True}


async def case_media_and_notice(path):
    p, ctx = await make(path, cfg(), media=MediaService())
    ev = await start(p)
    m = H.FakeMessage("281", "看图")
    m.chain.append(Image())
    m.message_str = "[Image attached]"
    b = H.batch(m)
    await p.on_batch_dedup(b)
    assert len(ctx.steer_calls) == 1
    msg = ctx.steer_calls[0][1]
    assert not isinstance(msg, str), "native image should produce OpenAIMessage"
    assert msg.content[0]["type"] == "text"
    assert any(p.get("type") == "kira_image_ref" for p in msg.content), msg.content
    assert not p._pending_inject.get(SID)
    await p.terminate()

    # 媒体服务不可用：正常文本 steer，不回落 tool_result
    p, ctx = await make(path, cfg(), media=None)
    ev = await start(p)
    m = H.FakeMessage("282", "看图")
    m.chain.append(Image())
    m.message_str = "[Image attached]"
    b = H.batch(m)
    await p.on_batch_dedup(b)
    assert len(ctx.steer_calls) == 1
    assert isinstance(ctx.steer_calls[0][1], str)
    assert not p._pending_inject.get(SID)
    await p.terminate()

    p, ctx = await make(path, cfg())
    ev = await start(p)
    n = H.FakeMessage("291", "通知")
    n.is_notice = True
    b = H.batch(n)
    await p.on_batch_dedup(b)
    assert len(ctx.steer_calls) == 1
    await p.terminate()
    return {"媒体 native steer": True, "通知 steer": True}


async def case_quota_idle_final_toolresult(path):
    # quota 1 / batch 2 -> defer, no steer; next boundary steers one
    p, ctx = await make(path, cfg(step=1))
    ev = await start(p)
    b = H.batch(H.FakeMessage("301", "m1"), H.FakeMessage("302", "m2"))
    await p.on_batch_dedup(b)
    assert not ctx.steer_calls and len(p._pending_inject.get(SID, [])) == 2
    await step(p, ev, 1)
    t = H.FakeToolResult()
    await p._handle_tool_result(ev, t)
    assert len(ctx.steer_calls) == 1
    assert p._run_inject_count.get(ev.event_id, (0, 0))[0] == 1
    await p.terminate()

    # idle: pass, no steer
    p, ctx = await make(path, cfg())
    b = H.batch(H.FakeMessage("311", "idle"))
    await p.on_batch_dedup(b)
    assert not b.is_stopped and not ctx.steer_calls
    await p.terminate()

    # 50/50 with tools: ending, no steer
    p, ctx = await make(path, cfg())
    ev = await start(p)
    p._pending_inject[SID] = [(H.BufferedShim(H.FakeMessage("321", "pending")), "pending", 0)]
    await step(p, ev, 50)
    await p._handle_tool_result(ev, H.FakeToolResult())
    assert not ctx.steer_calls
    assert len(ctx.flushes) == 1
    await p.terminate()

    # tool_result mode: no steer, old path
    p, ctx = await make(path, cfg(mode="tool_result"))
    ev = await start(p)
    b = H.batch(H.FakeMessage("331", "old"))
    await p.on_batch_dedup(b)
    assert b.is_stopped and not ctx.steer_calls
    assert len(p._pending_inject.get(SID, [])) == 1
    await p.terminate()
    return {"配额延后/下一边界": True, "空闲不 steer": True,
            "最终步不 steer": True, "tool_result 模式不 steer": True}


async def case_no_steer_api(path):
    mod = H.load_plugin(path)
    ctx = H.FakeCtx()
    ctx.config = LiveCfg()
    p = mod.MidflightMessagePlugin(ctx, cfg())
    await p.initialize()
    ev = await start(p)
    b = H.batch(H.FakeMessage("341", "fallback"))
    await p.on_batch_dedup(b)
    assert b.is_stopped
    assert len(p._pending_inject.get(SID, [])) == 1
    await p.terminate()
    return {"无 steer 接口自动回退 tool_result": True}


async def main():
    path = sys.argv[1] if len(sys.argv) > 1 else str(HERE.parent)
    path = str(Path(path).parent) if Path(path).is_file() else str(Path(path))
    results = []
    for case in (case_batch_steer_true, case_batch_steer_false, case_tool_steer,
                 case_tool_steer_false, case_stop_and_control, case_system_trigger_a,
                 case_media_and_notice, case_quota_idle_final_toolresult,
                 case_no_steer_api):
        results.append(await case(path))
    for r in results:
        print("  PASS", r)
    print("test_steer_mode: ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
