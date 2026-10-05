"""Midflight v1.3.4：幽灵轮（跟踪中的事件已被 stop）即时收尾回归测试。

背景（线上实测）：session_merger 的跨会话 handoff 在 ON_STEP_RESULT 阶段 stop
当前轮；核心的 ON_FINAL_RESULT 循环「每个 handler 后检查 is_stopped 就 break」
（core/message_manager.py），排在 S版（HIGH=50）之后的本插件收尾 handler
（MEDIUM=0）轮不到执行 ⇒ 幽灵运行中 ⇒ 之后该会话的消息全被拦截进流入队列，
却永远等不到工具边界（那一轮已死），私聊表现为「永久挂起」。

用法: python3 tests/test_stopped_event_sweep.py [<插件目录> ...]

场景（每个场景收尾后都再发一条用户消息验证不再被拦截）：
  G1  handoff 式 stop 后，下一条消息批次【不被拦截】，幽灵当场收尾
  G2  拦截发生在 stop 之前：清扫器（_sweep_once）收尾，pending 还原且【带 flush】
  G3  真运行中的轮（is_stopped=False）清扫器【不误伤】
  G4  心跳超时路径：还原【带 flush】+ 立墓碑 + 标 _bypass（防再拦死循环）
  G5  收尾幂等：stop 收尾后再补发 ON_FINAL_RESULT / 再扫一次，不报错、不重复还原
"""
import asyncio
import importlib.util
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from core.chat import Group, KiraIMMessage, MessageChain, Session, User  # noqa: E402
from core.chat.message_elements import Text  # noqa: E402
from core.chat.message_utils import KiraMessageBatchEvent  # noqa: E402

SID = "qq:dm:1025040690"


def load(path):
    spec = importlib.util.spec_from_file_location("mf_sweep_" + Path(path).parent.name, path)
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
        self.plugin = None
        self.published = []   # [(event_id8, is_stopped)] flush 出的批次再进守卫

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
                timestamp=int(time.time()), session=Session(session_type="dm", session_id="1025040690"),
                messages=[getattr(m, "message", m) for m in msgs]))
        return True

    async def publish_event(self, event):
        # 模拟框架：flush 出的新批次会回到 ON_IM_BATCH_MESSAGE ⇒ 守卫再看一次
        if self.plugin is not None:
            await self.plugin.on_batch_dedup(event)
            self.published.append((event.event_id[:8], event.is_stopped))


CFG = {
    "section_basic": {"enabled": True, "inject_timeout_steps": 2, "sweeper_interval": 12,
                      "debug": False},
    "section_flow": {"flow_method_group": "all", "flow_method_dm": "any",
                     "accept_poke": True, "wake_keywords": []},
    "section_stop": {"stop_enabled": True, "stop_words": ["停"], "stop_match_mode": "contains"},
    "section_scope": {}, "section_limits": {"max_inject_per_run": 0, "freshness_seconds": -1,
                                            "max_length": 0, "block_patterns": []},
    "section_inject": {"template": "", "inject_hint": True, "inject_hint_text": "HINT"},
    "section_media": {}, "section_overrides": {},
}


def msg(mid, text):
    return KiraIMMessage(timestamp=time.time(), sender=User("1025040690", "小明"),
                         group=None, message_id=str(mid),
                         self_id="10000", chain=MessageChain([Text(text)]))


def dm_session():
    return Session(session_type="dm", session_id="1025040690")


def batch(*msgs):
    return KiraMessageBatchEvent(timestamp=int(time.time()), session=dm_session(),
                                 messages=list(msgs))


class Resp:
    def __init__(self, step_index, tool_calls):
        self.agent_step_index = step_index
        self.tool_calls = tool_calls
        self.text_response = ""


class ToolRes:
    def __init__(self):
        self.text = "ok"
        self.attachments = []


async def make_plugin(path):
    mod = load(path)
    ctx = Ctx()
    plugin = mod.MidflightMessagePlugin(ctx, CFG)
    await plugin.initialize()
    ctx.plugin = plugin
    return mod, plugin, ctx


async def start_running_round(plugin, mid="100"):
    """模拟框架跑起一轮：ON_LLM_REQUEST → ON_LLM_RESPONSE(带工具) → ON_TOOL_RESULT。"""
    ev = batch(msg(mid, "帮我跑个任务"))
    await plugin._track_run_start(ev)
    await plugin._ensure_stop_checkpoint(ev, Resp(1, ["session_send"]))
    await plugin._handle_tool_result(ev, ToolRes())
    return ev


async def scenario_g1(path):
    """handoff 式 stop（final_result 不送达本插件）后，下一条消息不被拦截。"""
    mod, plugin, ctx = await make_plugin(path)
    try:
        ev = await start_running_round(plugin)
        assert SID in plugin._run_active, "轮应已标记为运行中"
        # merger 在 ON_STEP_RESULT 掐停本轮；核心的 final_result 循环 break，
        # 本插件的 _on_final_result 轮不到执行（这里【故意不调用】它来复现）
        ev.stop()
        assert plugin._run_active.get(SID) is not None, "复现前提：幽灵运行中已残留"

        # 用户再发一条消息
        ev2 = batch(msg("101", "怎么样了"))
        await plugin.on_batch_dedup(ev2)
        assert not ev2.is_stopped, "G1 失败：handoff 后的新消息仍被拦截"
        assert SID not in plugin._run_active, "G1 失败：幽灵运行中未被即时清扫"
        assert plugin._finished_run.get(SID) == ev.event_id, "G1 失败：未立墓碑"
        assert not plugin._pending_inject.get(SID), "G1 失败：流入队列有残留"
        print("  G1 PASS：handoff 式 stop 后下一条消息不被拦截，幽灵当场收尾")
    finally:
        await plugin.terminate()


async def scenario_g2(path):
    """拦截发生在 stop 之前：清扫器收尾并把 pending 还原【带 flush】。"""
    mod, plugin, ctx = await make_plugin(path)
    try:
        ev = await start_running_round(plugin)
        # 轮还在真跑（未 stop）：新消息被合法拦截进流入队列
        ev_mid = batch(msg("102", "顺便问下"))
        await plugin.on_batch_dedup(ev_mid)
        assert ev_mid.is_stopped, "复现前提：运行中批次应被拦截"
        assert len(plugin._pending_inject.get(SID) or []) == 1, "复现前提：流入队列应有 1 条"

        # 此刻轮被 handoff 掐停（死轮再没有工具边界，final_result 也轮不到本插件）
        ev.stop()

        swept = await plugin._sweep_once()
        assert swept == 1, "G2 失败：清扫器未发现幽灵轮"
        assert SID not in plugin._run_active, "G2 失败：幽灵运行中未被清扫"
        assert not plugin._pending_inject.get(SID), "G2 失败：流入队列未清空"
        # 还原必须带 flush：flush 出的新批次回到守卫时应【放行】（不再拦截）
        assert len(ctx.published) == 1 and ctx.published[0][1] is False, \
            f"G2 失败：还原的消息未 flush 成新轮（published={ctx.published}）"

        # 之后再发消息也正常
        ev3 = batch(msg("103", "在吗"))
        await plugin.on_batch_dedup(ev3)
        assert not ev3.is_stopped, "G2 失败：清扫后新消息仍被拦截"
        print("  G2 PASS：清扫器收尾，拦截消息还原并 flush 成新轮（不丢、不再拦）")
    finally:
        await plugin.terminate()


async def scenario_g3(path):
    """真运行中的轮：is_stopped 恒为 False，清扫器与各入口绝不误伤。"""
    mod, plugin, ctx = await make_plugin(path)
    try:
        ev = await start_running_round(plugin)
        swept = await plugin._sweep_once()
        assert swept == 0, "G3 失败：清扫器误伤了在飞的轮"
        assert SID in plugin._run_active, "G3 失败：在飞的轮被清掉"
        # 工具边界：is_stopped=False 时行为与 v1.3.3 完全一致（正常心跳）
        await plugin._handle_tool_result(ev, ToolRes())
        assert SID in plugin._run_active, "G3 失败：工具边界误清了在飞的轮"
        # 正常收尾（最终文本步）仍然有效
        await plugin._ensure_stop_checkpoint(ev, Resp(2, []))
        assert SID not in plugin._run_active, "G3 失败：正常收尾失效"
        print("  G3 PASS：在飞的轮不被误伤，正常收尾路径不变")
    finally:
        await plugin.terminate()


async def scenario_g4(path):
    """心跳超时路径：还原【带 flush】+ 立墓碑 + 标 _bypass。"""
    mod, plugin, ctx = await make_plugin(path)
    try:
        ev = await start_running_round(plugin)
        ev_mid = batch(msg("104", "插一句"))
        await plugin.on_batch_dedup(ev_mid)
        assert ev_mid.is_stopped, "复现前提：运行中批次应被拦截"
        # 模拟心跳超时（一轮异常拖太久，没有任何收尾信号）
        plugin._run_active[SID]["ts"] = time.time() - plugin._active_timeout - 1

        ev2 = batch(msg("105", "还在吗"))
        await plugin.on_batch_dedup(ev2)
        assert not ev2.is_stopped, "G4 失败：超时后新消息仍被拦截"
        assert SID not in plugin._run_active, "G4 失败：超时后运行中标记未清"
        assert plugin._finished_run.get(SID) == ev.event_id, "G4 失败：超时路径未立墓碑"
        await asyncio.sleep(0.05)  # 让 create_task 的还原 flush 跑完
        assert len(ctx.published) == 1 and ctx.published[0][1] is False, \
            f"G4 失败：超时还原未带 flush（published={ctx.published}）"
        assert plugin._bypass, "G4 失败：超时还原未标 _bypass（可能再被拦回去）"
        print("  G4 PASS：心跳超时路径带 flush 还原 + 立墓碑 + 防再拦")
    finally:
        await plugin.terminate()


async def scenario_g5(path):
    """收尾幂等：stop 收尾后迟到的 ON_FINAL_RESULT / 重复清扫不报错、不重复还原。"""
    mod, plugin, ctx = await make_plugin(path)
    try:
        ev = await start_running_round(plugin)
        ev.stop()
        swept = await plugin._sweep_once()
        assert swept == 1, "复现前提：清扫器应收尾一次"
        # 迟到的 final_result（核心若没 break 的正常路径）与重复清扫都必须幂等
        await plugin._on_final_result(ev, None)
        swept2 = await plugin._sweep_once()
        assert swept2 == 0, "G5 失败：重复清扫不幂等"
        assert len(ctx.published) == 0, "G5 失败：无 pending 时不应有 flush"
        # 迟到的工具边界（同一轮的其余 tool_calls）不得复活幽灵
        await plugin._handle_tool_result(ev, ToolRes())
        assert SID not in plugin._run_active, "G5 失败：迟到边界复活了幽灵"
        print("  G5 PASS：收尾幂等，迟到信号不复活幽灵")
    finally:
        await plugin.terminate()


async def run_all(path):
    print(f"== {path}")
    await scenario_g1(path)
    await scenario_g2(path)
    await scenario_g3(path)
    await scenario_g4(path)
    await scenario_g5(path)


def main():
    dirs = sys.argv[1:] or [str(HERE.parent)]
    for d in dirs:
        main_py = Path(d).resolve() / "main.py"
        if not main_py.exists():
            print(f"SKIP {d}: main.py not found")
            continue
        asyncio.run(run_all(str(main_py)))
    print("test_stopped_event_sweep: ALL PASS")


if __name__ == "__main__":
    main()
