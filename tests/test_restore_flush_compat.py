"""Regression: restored intercept messages must survive the framework's
``flush_session_messages`` on BOTH framework generations.

Bug (v1.3.4 and earlier)
------------------------
``_BufferedMsgShim`` only exposed ``.message_types``.  The framework's
``flush_session_messages`` (core/message_manager.py) reads the message-type
set off the **last buffered event** to build the new batch:

* 2.x (<= v2.34.x):  ``message_types=last_event.message_types``   -> shim OK
* 3.0 (dev-v3):      ``supported_elements = last_event.supported_elements``
                     -> AttributeError on the shim

Observed in the field (3.0, v1.3.4)::

    [Midflight] 本轮已结束，3 条拦截消息还原走正常管线
    ERROR [Midflight] 还原消息回 buffer 异常（已自捕获）
    File ".../core/message_manager.py", line 254, in flush_session_messages
        supported_elements = last_event.supported_elements

The messages were put back into the buffer *before* the flush raised, so they
were not lost — but they were stuck there until the chat plugin's next
debounce cycle (potentially forever if the user went quiet).

Second symptom (same spot): ``getattr(batch_event, "message_types", None)``
on 3.0 goes through the ``@deprecated`` property alias and spams
``DeprecationWarning: ... use supported_elements instead`` for every
intercepted batch.

Fix (v1.3.5): the shim reads ``supported_elements`` first, falls back to
``message_types``, and exposes **both** names.  This test drives the real
intercept -> finish -> restore+flush path against faithful per-generation
reimplementations of ``flush_session_messages`` and asserts:

  F1  gen3 batch event: interception emits NO DeprecationWarning
  F2  gen3: restore+flush builds the new batch (shim carries supported_elements)
  F3  gen3: flushed batch keeps message order and content
  F4  gen2: same flow works with message_types
  F5  reverse guard: the OLD shim shape (message_types only) really does crash
      the gen3 flush — proves this test would have caught the bug
  F6  shim exposes both names, same value, from either generation's batch event

Run it against any checkout:  python3 tests/test_restore_flush_compat.py [plugin_dir]
"""
import asyncio
import sys
import time
import types
import uuid
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import (DEFAULT_SID, FakeMessage, FakeSession, batch,  # noqa: E402
                     drive_on_batch_message, drive_on_llm_request,
                     drive_on_llm_response, load_plugin, make_plugin)

HERE = Path(__file__).resolve()


# ------------------------------------------------------- 3.0-shaped batch event

class Gen3BatchEvent:
    """Mirrors 3.0 core/chat/message_utils.py KiraMessageBatchEvent:
    ``supported_elements`` is the real dataclass field; ``message_types``
    survives only as a @deprecated property alias that warns on every access."""

    def __init__(self, *messages):
        self.supported_elements = ["text", "image"]
        self.timestamp = int(time.time())
        self.event_id = uuid.uuid4().hex
        self.adapter = None
        self.session = FakeSession()
        self.messages = list(messages)
        self.extra = None
        self._is_stopped = False

    @property
    def message_types(self):
        warnings.warn("message_types is deprecated; use supported_elements instead",
                      DeprecationWarning, stacklevel=2)
        return self.supported_elements

    @property
    def sid(self):
        return self.session.sid

    @property
    def is_stopped(self):
        return self._is_stopped

    def stop(self):
        self._is_stopped = True

    def is_group_message(self):
        return False


# -------------------- faithful per-generation flush_session_messages semantics
# (verbatim attribute access of core/message_manager.py in each generation;
#  FakeCtx.flush_session_messages is a no-op, so we swap in the real behaviour)

async def flush_like_gen2(ctx, sid):
    """v2.34.8: batch built with message_types=last_event.message_types."""
    buffer = ctx.get_buffer(sid)
    async with buffer.lock:
        pending = buffer.flush()
    if not pending:
        return False
    last_event = pending[-1]
    ctx.built_batches.append({
        "message_types": last_event.message_types,   # <- 2.x attribute access
        "adapter": last_event.adapter,
        "session": last_event.session,
        "messages": [m.message for m in pending],
    })
    return True


async def flush_like_gen3(ctx, sid):
    """dev-v3 (3.0): supported_elements = last_event.supported_elements."""
    buffer = ctx.get_buffer(sid)
    async with buffer.lock:
        pending = buffer.flush()
    if not pending:
        return False
    last_event = pending[-1]
    supported_elements = last_event.supported_elements  # <- 3.0 attribute access
    ctx.built_batches.append({
        "supported_elements": supported_elements,
        "adapter": last_event.adapter,
        "session": last_event.session,
        "messages": [m.message for m in pending],
    })
    return True


def _install_flush(ctx, fn):
    ctx.built_batches = []

    async def _flush(sid, extra_event=None):
        return await fn(ctx, sid)

    ctx.flush_session_messages = _flush


async def _drive_intercept_and_finish(plugin, run_ev, mid_batch):
    """Shared flow: run starts -> mid_batch intercepted -> run ends (final text
    step) -> plugin restores intercepted messages and flushes."""
    await drive_on_llm_request(plugin, run_ev)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await drive_on_batch_message(plugin, mid_batch)
    assert mid_batch.is_stopped, "批次未被拦截（前置条件失败）"
    sid = DEFAULT_SID
    assert len(plugin._pending_inject.get(sid, [])) == len(mid_batch.messages), \
        "拦截消息未进流入队列（前置条件失败）"
    # 最终文本步（无 tool_calls）=> _finish_run => _restore_to_buffer(flush=True)
    await drive_on_llm_response(plugin, run_ev, 1, [])
    return caught


async def scenario_gen3(path):
    """3.0: intercept from a supported_elements batch, restore must flush."""
    plugin, ctx, sid = await make_plugin(path)
    try:
        _install_flush(ctx, flush_like_gen3)
        run_ev = batch(FakeMessage("100", "跑个任务"))
        m1, m2 = FakeMessage("200", "香香，你发这张看看"), FakeMessage("201", "用md")
        mid = Gen3BatchEvent(m1, m2)

        caught = await _drive_intercept_and_finish(plugin, run_ev, mid)

        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert not dep, f"F1 失败：3.0 批次事件触发 DeprecationWarning: {dep[0].message if dep else ''}"
        print("  F1 PASS：3.0 批次事件拦截全程零 DeprecationWarning")

        assert len(ctx.built_batches) == 1, \
            "F2 失败：还原后 flush 没有产出新批次（shim 缺 supported_elements？）"
        built = ctx.built_batches[0]
        assert built["supported_elements"] == mid.supported_elements, \
            "F2 失败：新批次 supported_elements 与来源批次不一致"
        print("  F2 PASS：3.0 还原+flush 成功成批，supported_elements 透传正确")

        assert built["messages"] == [m1, m2], "F3 失败：成批消息顺序/内容被改变"
        assert ctx.get_buffer(sid).get_length() == 0, "F3 失败：buffer 未清空"
        print("  F3 PASS：3.0 成批消息逐条一致、顺序不变、buffer 已清空")
    finally:
        await plugin.terminate()


async def scenario_gen2(path):
    """2.x: same flow with a message_types batch event — must keep working."""
    plugin, ctx, sid = await make_plugin(path)
    try:
        _install_flush(ctx, flush_like_gen2)
        run_ev = batch(FakeMessage("100", "跑个任务"))
        m1, m2 = FakeMessage("300", "在吗"), FakeMessage("301", "看下这个")
        mid = batch(m1, m2)
        mid.message_types = ["text"]

        await _drive_intercept_and_finish(plugin, run_ev, mid)

        assert len(ctx.built_batches) == 1, "F4 失败：2.x 还原后 flush 没有产出新批次"
        built = ctx.built_batches[0]
        assert built["message_types"] == ["text"], "F4 失败：2.x 新批次 message_types 不对"
        assert built["messages"] == [m1, m2], "F4 失败：2.x 成批消息被改变"
        print("  F4 PASS：2.x 还原+flush 成功成批，message_types 透传正确")
    finally:
        await plugin.terminate()


async def scenario_reverse_guard(path):
    """Reverse validation: the v1.3.4 shim shape (message_types only) MUST crash
    the gen3 flush — otherwise this suite isn't modelling the real bug."""
    ctx_plugin, ctx, sid = await make_plugin(path)
    try:
        old_shim = types.SimpleNamespace(  # v1.3.4 _BufferedMsgShim 的访问形状
            message=FakeMessage("400", "旧形状"), message_types=["text"],
            adapter=None, session=FakeSession())
        buffer = ctx.get_buffer(sid)
        async with buffer.lock:
            buffer.buffer[:0] = [old_shim]
        crashed = False
        try:
            await flush_like_gen3(ctx, sid)
        except AttributeError:
            crashed = True
        assert crashed, "F5 失败：旧形状 shim 在 3.0 flush 下没炸 —— 测试没有建模真实 bug"
        print("  F5 PASS：反向验证成立 —— 旧形状 shim 必炸 3.0 flush（AttributeError）")
    finally:
        await ctx_plugin.terminate()


def scenario_shim_shape(path):
    """F6: the shim exposes BOTH names with the same value, from either
    generation's batch event."""
    mod = load_plugin(path)
    m = FakeMessage("500", "形状检查")

    g3 = Gen3BatchEvent(m)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        shim3 = mod._BufferedMsgShim(m, g3)
    assert not [w for w in caught if issubclass(w.category, DeprecationWarning)], \
        "F6 失败：从 3.0 批次构造 shim 触发 DeprecationWarning"
    assert shim3.supported_elements == g3.supported_elements, "F6 失败：shim.supported_elements 缺失/不等"
    assert shim3.message_types == g3.supported_elements, "F6 失败：shim.message_types 未同步"

    g2 = batch(m)
    g2.message_types = ["text"]
    shim2 = mod._BufferedMsgShim(m, g2)
    assert shim2.message_types == ["text"], "F6 失败：2.x 批次构造 shim 丢 message_types"
    assert shim2.supported_elements == ["text"], "F6 失败：2.x 批次构造 shim 未补 supported_elements"
    print("  F6 PASS：shim 双名共存、同值，两代批次事件构造均正确")


async def run_all(path):
    print(f"== {path}")
    await scenario_gen3(path)
    await scenario_gen2(path)
    await scenario_reverse_guard(path)
    scenario_shim_shape(path)


def main():
    dirs = sys.argv[1:] or [str(HERE.parent)]
    for d in dirs:
        main_py = Path(d).resolve() / "main.py"
        if not main_py.exists():
            print(f"SKIP {d}: main.py not found")
            continue
        asyncio.run(run_all(str(Path(d).resolve())))
    print("test_restore_flush_compat: ALL PASS")


if __name__ == "__main__":
    main()
