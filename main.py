"""随时插话（Midflight Inbox）

核心哲学：零拦截、自然流入。
- 不改变任何消息的处理策略，不打断聊天插件的防抖/合并；
- 流入通道有两条，互为补充：
  1) 工具边界 drain：在 bot 连续执行的每个工具调用边界（ON_TOOL_RESULT），
     把该会话 SessionBuffer 里"恰好已到达、还没来得及成为下一轮"的消息，
     以与官方内置 kira-ai 插件完全一致的格式（[时间] [message_id] [昵称, user_id] | 内容）
     追加进 tool_result.text；
  2) 批次拦截：若消息先被聊天插件防抖 flush 成批次（如 S版 QueueMerge 会把
     运行中到达的批次扣进 pending 排队），则在 ON_IM_BATCH_MESSAGE(SYS_HIGH，
     先于 QueueMerge 的 HIGH) 识别"本会话正在跑"，直接把批次消息转入流入队列，
     掐掉批次，等下一个工具边界注入——消息不再被排队到本轮结束后；
- 被过滤掉的消息会原样放回 buffer / 放行批次，照常走聊天插件开新轮；
- 已消费 message_id 去重（TTL 10 分钟），若已注入消息又被 flush 成新批次，
  掐掉完全重复的批次，防止二次回复。

仅使用官方插件 API，不修改 KiraAI 本体。
"""

import asyncio
import fnmatch
import re
import time
from datetime import datetime

from core.plugin import BasePlugin, logger, on, Priority
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.chat.message_elements import (
    Text, At, Reply, Poke,
    Record, Image, Sticker, File, Video, Forward,
)


# 已消费 message_id 的存活时间（秒）
DEDUP_TTL = 600
# 自动读取官方配置失败时的保守内置兜底
FALLBACK_MAX_INJECT = 5
FALLBACK_FRESHNESS = 30
# “运行中”判定的兜底超时（秒）：自动 = LLM 超时 + 工具超时（与 S版 QueueMerge 同源公式）
FALLBACK_ACTIVE_TIMEOUT = 180.0
# 默认流入引导语：让 bot 意识到可以边回边干（KiraAI 每个 LLM 步都会把
# <msg> 文本响应即时发出，所以中间步回复用户是原生支持的）
DEFAULT_INJECT_HINT = (
    "（以上是任务途中收到的新消息。回复规则：如果新消息需要你回应，必须在本轮回复中先输出 "
    "<msg><text>回应内容</text></msg>，再继续调用工具——发消息和继续任务在同一轮里同时进行，"
    "不要等任务结束才回。如果新消息不需要回应，直接继续任务即可。）"
)
# 唤醒词回退链：按序尝试读取已安装聊天插件的唤醒词配置
# （plugin_id 候选, 可能的配置键）
# 注意：Z版（KiraAI_Default-Chat-Z-）的 manifest plugin_id 就是带全角括号的
# "default-chat（z）"（已核实 znq19/KiraAI_Default-Chat-Z-），不是 z-chat 之类
WAKE_KEYWORD_SOURCES = [
    (("default-chat（z）",),
     ("waking_words", "wake_keywords", "wake_words")),
    (("s-chat", "s_chat", "schat", "sustained_chat", "sustained-chat"),
     ("waking_words", "wake_keywords", "wake_words")),
    (("default-chat",),
     ("waking_words", "wake_keywords", "wake_words")),
]

# 触发型系统事件的 sender user_id：S版私聊主动回复（system_proactive_dm）、
# 定时任务（system_scheduled）。它们携带"开新一轮"的提示词与工具黑名单等特殊
# 语义，不是用户闲聊——默认照常走注入判定（可拦进在飞轮），但**默认不触发停止词**
# （notice_skip_stop 默认开即覆盖：见 _skip_stop_for）；仅当配置
# system_trigger_passthrough 开启时，本集合及 system_ 前缀 sender 的消息才
# 既不判停止词也不转入流入队列，原样留在批次里放行开新轮。
# 注意与系统提醒类消息区分：框架 publish_notice 的提醒（is_notice=True 或
# message_id="system_message"）同样由 notice_skip_stop 控制（见 _is_notice）。
SYSTEM_SENDER_IDS = {"system_proactive_dm", "system_scheduled"}

# 可被 overrides 覆盖的键
OVERRIDABLE_KEYS = {
    "enabled", "flow_method_group", "flow_method_dm", "accept_poke",
    "wake_keywords", "stop_enabled", "stop_words", "stop_match_mode",
    "stop_whitelist_enabled", "stop_whitelist_users",
    "whitelist_enabled", "whitelist_users", "max_inject_per_run",
    "freshness_seconds", "max_length", "block_patterns", "template",
    "inject_hint", "inject_hint_text", "inject_timeout_steps",
    "inject_grace_seconds", "debug",
    "system_trigger_passthrough", "notice_skip_stop",
}


class _BufferedMsgShim:
    """把批次里的 KiraIMMessage 包装成与 buffer 中 KiraMessageEvent 相同的访问形状
    （.message / .is_group_message() / .message_types / .adapter / .session），
    使批次拦截路径与 buffer drain 路径可以走完全相同的过滤与注入代码。"""

    __slots__ = ("message", "message_types", "adapter", "session", "_is_group")

    def __init__(self, message, batch_event):
        self.message = message
        self.message_types = getattr(batch_event, "message_types", None)
        self.adapter = getattr(batch_event, "adapter", None)
        self.session = getattr(batch_event, "session", None)
        try:
            self._is_group = bool(batch_event.is_group_message())
        except Exception:
            self._is_group = getattr(message, "group", None) is not None

    def is_group_message(self):
        return self._is_group


class MidflightMessagePlugin(BasePlugin):
    """随时插话插件主类"""

    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        cfg = cfg or {}

        basic = cfg.get("section_basic", {}) or {}
        self.enabled = bool(basic.get("enabled", True))
        self.inject_timeout_steps = self._to_int(basic.get("inject_timeout_steps", 2), 2)
        # 流入队列看门狗（**默认 0 = 关闭**）：拦截进流入队列后，若这么多秒内一直没搭上
        # 工具边界，就把消息还原走正常管线，不再死等"运行中判定超时"（默认 180s）。
        #
        # 为什么默认关：它是**纯挂钟计时**，不感知 LLM/工具快慢（框架在一次 LLM 调用
        # 期间不给插件任何信号），所以单步耗时可能几十秒以上的环境（慢推理模型、
        # 图生成/联网检索这类慢工具）会被它误放——那条消息就不搭车了（不丢，只是改走
        # 正常队列）。而"最可能卡住"的路径（命中停止词）已经在代码里**当场收尾**，
        # 不依赖它。剩下会导致卡住的只有"一个工具边界都没有"的罕见路径：
        #   ① 本轮在 ON_LLM_REQUEST 阶段被别的插件 stop / 中途异常被 EventBus 吞掉；
        #   ② 最后一步的工具调用全部被 max_tool_calls_per_turn 跳过。
        # 想让这两条也快速自愈（≈2×grace，而不是 180s）时再打开，取值 ≥ 你环境里
        # 最慢的一次"LLM 调用 + 工具执行"耗时。
        self.inject_grace_seconds = self._to_int(basic.get("inject_grace_seconds", 0), 0)
        # 触发型系统事件放行（默认关 = 照常注入判定，但 notice_skip_stop 默认开
        # 时仍不触发停止词）：开启后 sender.user_id ∈ SYSTEM_SENDER_IDS 或以
        # system_ 开头的消息在运行中拦截时不判停止词、
        # 不转注入，原样留在批次里放行开新轮（保留其提示词/工具黑名单语义）
        self.system_trigger_passthrough = bool(basic.get("system_trigger_passthrough", False))
        self.debug = bool(basic.get("debug", False))

        flow = cfg.get("section_flow", {}) or {}
        self.flow_method_group = str(flow.get("flow_method_group", "all") or "all")
        self.flow_method_dm = str(flow.get("flow_method_dm", "any") or "any")
        self.accept_poke = bool(flow.get("accept_poke", True))
        self.wake_keywords = [str(w) for w in (flow.get("wake_keywords") or []) if str(w).strip()]

        stop = cfg.get("section_stop", {}) or {}
        self.stop_enabled = bool(stop.get("stop_enabled", False))
        self.stop_words = [str(w) for w in (stop.get("stop_words") or []) if str(w)]
        self.stop_match_mode = str(stop.get("stop_match_mode", "contains") or "contains")
        self.stop_whitelist_enabled = bool(stop.get("stop_whitelist_enabled", False))
        self.stop_whitelist_users = [str(u) for u in (stop.get("stop_whitelist_users") or []) if str(u).strip()]
        # 系统来源的消息（is_notice=True 或 message_id="system_message" 的框架
        # publish_notice 提醒，如 S/Z 的骚扰/休眠提醒；以及 sender.user_id 为
        # system_proactive_dm / system_scheduled / system_ 前缀的触发型系统事件）
        # 永不参与停止词判定，但仍照常走注入过滤可以流入在飞轮。关掉则一视同仁
        # 可触发停止。
        self.notice_skip_stop = bool(stop.get("notice_skip_stop", True))

        scope = cfg.get("section_scope", {}) or {}
        self.session_blacklist = [str(s) for s in (scope.get("session_blacklist") or []) if str(s).strip()]
        self.whitelist_enabled = bool(scope.get("whitelist_enabled", False))
        self.whitelist_users = [str(u) for u in (scope.get("whitelist_users") or []) if str(u).strip()]

        limits = cfg.get("section_limits", {}) or {}
        self.max_inject_per_run = self._to_int(limits.get("max_inject_per_run", 0), 0)
        self.freshness_seconds = self._to_int(limits.get("freshness_seconds", 0), 0)
        self.max_length = self._to_int(limits.get("max_length", 0), 0)
        self.block_patterns = [str(p) for p in (limits.get("block_patterns") or []) if str(p).strip()]

        inject = cfg.get("section_inject", {}) or {}
        self.template = str(inject.get("template", "") or "")
        self.inject_hint = bool(inject.get("inject_hint", True))
        self.inject_hint_text = str(inject.get("inject_hint_text", "") or "") or DEFAULT_INJECT_HINT

        media = cfg.get("section_media", {}) or {}
        # 媒体流入开关：语音/图片/文件/合并转发，默认全部允许
        self.allow_record = bool(media.get("allow_record", True))
        self.allow_image = bool(media.get("allow_image", True))
        self.allow_file = bool(media.get("allow_file", True))
        self.allow_forward = bool(media.get("allow_forward", True))

        overrides = cfg.get("section_overrides", {}) or {}
        raw_overrides = overrides.get("overrides", {})
        self.overrides = raw_overrides if isinstance(raw_overrides, dict) else {}

        # ---- 运行时状态（terminate 全部清理）----
        # {sid: {message_id: consumed_ts}}
        self._consumed: dict[str, dict[str, float]] = {}
        # {event_id(一轮 agent 执行): (已注入条数, 最后更新时间戳)}
        self._run_inject_count: dict[str, tuple[int, float]] = {}
        # {sid: 有候选消息但未被消费的连续工具边界数}
        self._wait_steps: dict[str, int] = {}
        # {sid: {"event": 运行中的批次事件对象, "ts": 最近心跳, "ending": 末步标记}}
        self._run_active: dict[str, dict] = {}
        # {sid: 已收尾那一轮的 event_id} —— 同一轮里后续的工具边界不得把本轮
        # 重新标记为“运行中”（一次 LLM 响应带多个 tool_calls 会逐个派发
        # ON_TOOL_RESULT，第一个边界就已收尾；重建会留下幽灵运行中状态）
        self._finished_run: dict[str, str] = {}
        # {sid: [(批次消息 shim, 纯文本, 入队时间)]} —— 批次拦截来的待注入消息
        self._pending_inject: dict[str, list] = {}
        # {dedup_key: ts} —— 已被看门狗"释放过"的消息（放行标记）：
        # 还原回缓冲后它们会随下次 flush 重新成批次，这张表保证不再被本插件拦截
        # （防"还原 → 重新成批 → 又被拦"死循环）。TTL 与 _consumed 一致。
        self._bypass: dict[str, float] = {}
        # 流入队列看门狗任务
        self._watchdog_task = None
        # 看门狗两段式放行：
        #   {sid: 软放行次数}；{sid: 软放行时刻} —— 软放行只放行消息、保留运行中状态，
        #   再过 grace 秒仍毫无活动（期间任何 LLM 响应/工具边界都会清零，见
        #   _note_activity）就升级为"硬放行"：清运行中标记 + 释放 QueueMerge
        self._wd_strikes: dict[str, int] = {}
        self._wd_escalate: dict[str, float] = {}
        # 上次清理时间
        self._last_gc: float = 0.0
        # 自动解析后的生效值（initialize 中解析）
        self._eff_max_inject: int = 0
        self._eff_freshness: int = 0
        self._active_timeout: float = FALLBACK_ACTIVE_TIMEOUT

    # ============ 生命周期 ============

    async def initialize(self):
        """解析自动配置项并记日志；可重入。"""
        # 单轮最多流入条数：0 = 自动读 bot_config.bot.max_buffer_messages
        if self.max_inject_per_run <= 0:
            auto = self._read_core_config("bot_config.bot.max_buffer_messages")
            self._eff_max_inject = self._to_int(auto, FALLBACK_MAX_INJECT)
            if self._eff_max_inject <= 0:
                self._eff_max_inject = FALLBACK_MAX_INJECT
            self._log_debug(f"max_inject_per_run 自动读取: {self._eff_max_inject}")
        else:
            self._eff_max_inject = self.max_inject_per_run

        # 新鲜度：-1 = 不限（默认；buffer 里的消息本来都是本轮执行期间到达的，
        # 配合 S版/Z版 等带合并防抖的聊天插件时，消息常已在队列里等了十几秒，
        # 限时会导致永远赶不上工具边界）；0 = 自动读 bot_config.bot.max_message_interval；
        # >0 = 自定义秒数
        if self.freshness_seconds < 0:
            self._eff_freshness = -1
            self._log_debug("freshness_seconds = -1，不限流入时间")
        elif self.freshness_seconds == 0:
            auto = self._read_core_config("bot_config.bot.max_message_interval")
            self._eff_freshness = self._to_int(auto, FALLBACK_FRESHNESS)
            if self._eff_freshness <= 0:
                self._eff_freshness = FALLBACK_FRESHNESS
            self._log_debug(f"freshness_seconds 自动读取: {self._eff_freshness}")
        else:
            self._eff_freshness = self.freshness_seconds

        # 唤醒词：留空 = 按 Z版 → S版 → 官方 default-chat 顺序自动沿用
        if not self.wake_keywords:
            words, source = self._resolve_wake_keywords()
            if words:
                self.wake_keywords = words
                logger.info(f"[Midflight] 唤醒词自动沿用 {source}: {self.wake_keywords}")
            else:
                self._log_debug("未能从任何聊天插件读取唤醒词，keyword 流入通道不生效")

        # “运行中”判定超时：自动 = LLM 超时 + 工具调用超时（与 S版 QueueMerge 同源），
        # 覆盖“两次心跳之间最多夹一次 LLM 调用 + 一轮工具执行”的最长无活动窗口
        try:
            llm_timeout = 120.0
            client = self.ctx.get_default_llm_client()
            if client is not None:
                mc = getattr(getattr(client, "model", None), "model_config", None) or {}
                llm_timeout = float(mc.get("timeout", 120) or 120)
            tool_timeout = float(self._read_core_config("bot_config.agent.tool_call_timeout") or 60)
            self._active_timeout = llm_timeout + tool_timeout
        except Exception:
            self._active_timeout = FALLBACK_ACTIVE_TIMEOUT
        self._log_debug(f"运行中判定超时: {self._active_timeout}s")

        logger.info(
            f"[Midflight] 随时插话 v{self._self_version()} 已加载 | enabled={self.enabled} "
            f"群聊={self.flow_method_group} 私聊={self.flow_method_dm} "
            f"poke={self.accept_poke} 停止词={'开' if self.stop_enabled else '关(默认)'} "
            f"上限={self._eff_max_inject} 新鲜度={self._eff_freshness}s "
            f"引导语={'开' if self.inject_hint else '关'} "
            f"看门狗={'关' if self.inject_grace_seconds <= 0 else str(self.inject_grace_seconds) + 's'}"
        )
        self._ensure_watchdog()

    def _ensure_watchdog(self):
        """启动流入队列看门狗（幂等，可重入）。"""
        if self.inject_grace_seconds <= 0:
            return
        try:
            if self._watchdog_task is None or self._watchdog_task.done():
                self._watchdog_task = asyncio.create_task(self._watchdog_loop())
        except Exception as e:
            # 不阻止插件加载，但必须可见——否则用户配置了看门狗却实际没有
            logger.warning(f"[Midflight] 流入看门狗启动失败（不影响插件其余功能）: {e}")
            self._watchdog_task = None

    async def terminate(self):
        """可重入：清理全部运行时状态。"""
        try:
            if self._watchdog_task is not None and not self._watchdog_task.done():
                self._watchdog_task.cancel()
            self._watchdog_task = None
        except Exception:
            pass
        try:
            self._consumed.clear()
            self._run_inject_count.clear()
            self._wait_steps.clear()
            self._run_active.clear()
            self._finished_run.clear()
            self._pending_inject.clear()
            self._bypass.clear()
            self._wd_strikes.clear()
            self._wd_escalate.clear()
            self._last_gc = 0.0
        except Exception:
            pass
        logger.info("[Midflight] 消息流入插件已卸载")

    # ============ 注入通道：工具结果边界 ============

    @on.tool_result(priority=Priority.MEDIUM)
    async def on_tool_result(self, event: KiraMessageBatchEvent, tool_result, *_):
        """每个工具调用结果返回后触发：drain 该 sid 的 buffer，过滤后注入/停止。"""
        try:
            await self._handle_tool_result(event, tool_result)
        except Exception:
            logger.exception("[Midflight] on_tool_result 处理异常（已自捕获，不影响主流程）")

    async def _handle_tool_result(self, event: KiraMessageBatchEvent, tool_result):
        cfg = self._eff_config(None)
        if not cfg["enabled"]:
            return

        sid = getattr(event, "sid", None)
        if not sid:
            session = getattr(event, "session", None)
            sid = getattr(session, "sid", None)
        if not sid:
            return

        cfg = self._eff_config(sid)
        if not cfg["enabled"]:
            return
        if self._in_blacklist(sid):
            self._log_debug(f"{sid} 在会话黑名单中，跳过")
            return

        # 运行心跳：tool_result 触发即证明该 sid 的 agent 轮仍在执行。
        # 返回 None = 本轮已在更早的工具边界收尾（最后一步可能一次派发多个
        # ON_TOOL_RESULT），此时必须直接退出：既不注入（注入进的是本轮永远
        # 不会再被模型读到的工具结果），也不能重建“运行中”标记（幽灵状态）
        run = self._touch_run(sid, event)
        if run is None:
            return

        # 末步标记（最后一步仍带工具调用）：本边界之后不会再有 LLM 调用，
        # 注入会丢，直接收尾并把待注入消息还原走正常管线
        if run.get("ending"):
            self._log_debug(f"{sid} 末步工具边界，不再注入，收尾还原")
            await self._finish_run(sid)
            return

        queued = [item[0] for item in self._pending_inject.pop(sid, [])]

        buffer = self.ctx.get_buffer(sid)
        if (buffer is None or buffer.get_length() == 0) and not queued:
            self._wait_steps.pop(sid, None)
            return

        # 候选等待步数上限：超过则完全不动 buffer，留给聊天插件正常开新轮
        timeout = cfg["inject_timeout_steps"]
        waited = self._wait_steps.get(sid, 0)
        if timeout > 0 and waited >= timeout:
            if queued and buffer is not None:
                # 批次拦截来的消息等超了：还原回 buffer 走正常管线，绝不卡住
                async with buffer.lock:
                    buffer.buffer[:0] = queued
                self._log_debug(f"{sid} 拦截消息等待超上限，已还原回 buffer")
            self._log_debug(f"{sid} 候选消息已等待 {waited} 个边界（上限 {timeout}），不再消费")
            # 重置等待计数：本边界放弃消费，但下一边界重新从 0 计数。
            # 若不重置，计数永久停在上限，之后所有新消息（哪怕通过全部过滤）
            # 都会在到达这里时被直接放弃，插话功能从此失效（卡死）。
            self._wait_steps.pop(sid, None)
            return

        self._gc()

        # 在 buffer.lock 内 drain，与官方 flush_session_messages 同一互斥域
        pending = []
        if buffer is not None:
            async with buffer.lock:
                pending = buffer.flush()

        # 批次拦截来的消息排在 buffer 消息之前（它们到达更早、已被 flush 过）
        pending = queued + pending

        if not pending:
            return

        consumed_map = self._consumed.setdefault(sid, {})
        stop_hit = []   # 命中停止词（消费，不注入）
        injectable = []  # 通过全部过滤（消费，注入）
        rejected = []    # 未通过过滤（放回 buffer，走官方管线）
        now = time.time()

        for msg_event in pending:
            try:
                key = self._dedup_key(getattr(msg_event, "message", None))
                if key and key in consumed_map:
                    # 已被消费过（注入过），直接丢弃，不放回，防重复
                    self._log_debug(f"{sid} 消息 {key} 已消费，丢弃防重")
                    continue
                text = self._plain_text(msg_event)
                message = getattr(msg_event, "message", None)
                if cfg["system_trigger_passthrough"] and self._is_system_sender(message):
                    # 触发型系统事件（S版主动回复/定时任务）不消费：放回 buffer
                    # 走官方管线照常开新轮（提示词/工具黑名单语义完整保留）
                    rejected.append(msg_event)
                    continue
                # 系统来源消息（publish_notice 提醒与 system_* 触发型事件）可注入
                # 在飞轮，但永不参与停止词判定（notice_skip_stop，默认开）
                if (cfg["stop_enabled"] and not self._skip_stop_for(message, cfg)
                        and self._stop_allowed(msg_event, cfg) and self._match_stop(text, cfg)):
                    stop_hit.append(msg_event)
                    continue
                if self._pass_filters(msg_event, text, cfg, now):
                    injectable.append((msg_event, text))
                else:
                    rejected.append(msg_event)
            except Exception:
                # 单条识别失败：放回 buffer，绝不丢消息
                rejected.append(msg_event)
                logger.exception("[Midflight] 单条消息过滤异常，已放回原流程")

        # 单轮流入条数上限：超额部分放回 buffer 留给聊天插件开新轮
        run_id = getattr(event, "event_id", None) or sid
        used = self._run_inject_count.get(run_id, (0, now))[0]
        quota = max(0, cfg["_max_inject"] - used)
        overflow = injectable[quota:]
        injectable = injectable[:quota]

        # 放回未消费的消息（保持原顺序置于 buffer 头部）
        put_back = rejected + [m for m, _ in overflow]
        if put_back:
            async with buffer.lock:
                buffer.buffer[:0] = put_back

        # 停止与注入互斥：命中停止词只 stop 不注入；
        # 同批已通过过滤的消息放回 buffer 走官方管线，绝不丢消息
        if stop_hit:
            leftover = [m for m, _ in injectable]
            if leftover:
                async with buffer.lock:
                    buffer.buffer[:0] = leftover
            for m in stop_hit:
                key = self._dedup_key(getattr(m, "message", None))
                if key:
                    consumed_map[key] = now
            self._wait_steps.pop(sid, None)
            logger.info(f"[Midflight] {sid} 命中停止词，停止本轮后续步骤")
            event.stop()
            # ⚠ 本轮马上会被框架结束，但**不会**再有 ON_LLM_RESPONSE / 工具边界来收尾
            # （框架在 ON_STEP_RESULT 看到 is_stopped 后直接 break）⇒ 必须在这里收尾。
            # 否则 _run_active 会留下"幽灵运行中"：之后该会话的消息全被拦截转入流入队列，
            # 却永远等不到工具边界去处理，只能等心跳超时（默认 180s）兜底。
            await self._finish_run(sid)
            return

        if not injectable:
            # 有候选但都没消费：累计等待步数
            self._wait_steps[sid] = waited + 1
            return

        # 原生样式文本化（与官方批次 message_format_to_text 一致）
        lines = []
        n = 0
        for msg_event, text in injectable:
            n += 1
            try:
                message = getattr(msg_event, "message", None)
                # 优先复用框架渲染时已写入的 message_str（框架在派发钩子前已完成
                # 文本化，媒体 caption 也已被聊天插件占位，不会触发 VLM）；
                # 为空（buffer 路径未经渲染）再回退 message_format_to_text
                native = str(getattr(message, "message_str", "") or "")
                if not native:
                    chain = getattr(message, "chain", None)
                    native = await self.ctx.message_processor.message_format_to_text(chain) if chain else text
            except Exception:
                native = text
            native = native or text
            if cfg["max_length"] > 0 and len(native) > cfg["max_length"]:
                native = native[: cfg["max_length"]] + "…"
            if cfg["template"]:
                lines.append(self._render_template(cfg["template"], msg_event, native, n))
            else:
                # 默认：与官方内置 kira-ai 插件 _format_user_message 完全一致的格式
                # 群聊: [时间] [message_id: x] [group_name: x group_id: x user_nickname: x, user_id: x] | 内容
                # 私聊: [时间] [message_id: x] [user_nickname: x, user_id: x] | 内容
                lines.append(self._format_native(msg_event, native))

        inject_block = "\n".join(line for line in lines if line)
        if not inject_block.strip():
            self._wait_steps[sid] = waited + 1
            return
        # 引导语：让 bot 意识到可以边回边干（只进上下文，不会发出去）
        if cfg.get("inject_hint") and cfg.get("inject_hint_text"):
            inject_block = inject_block + "\n" + cfg["inject_hint_text"]

        for m, _ in injectable:
            key = self._dedup_key(getattr(m, "message", None))
            if key:
                consumed_map[key] = now
        # 计数与心跳成对写入：_gc 按 ts 淘汰、读取端按 [0] 取计数，
        # 任何写纯 int 的旁路都会让两处同时炸掉（v1.3.2 回归事故）
        self._run_inject_count[run_id] = (used + len(injectable), now)
        self._wait_steps.pop(sid, None)

        base = getattr(tool_result, "text", "") or ""
        tool_result.text = (base + "\n" if base else "") + inject_block

        # 媒体附件透传：native 多模态模式下官方文本化只产出 "[Image attached]"，
        # 图片字节不会随搭车文本进请求。把消息里的 Image/Record/File 元素挂到
        # ToolResult.attachments（官方支持，tool.py:60），assemble_result 会自动
        # 落盘并把可访问路径写进工具结果，bot 后续可直接读取原图/原文件。
        # 与 S版/Z版 的并行媒体识别兼容：它们已替换为占位/描述文本的链里不再含
        # 原始媒体元素，此处自然为空，互不影响。
        try:
            attachments = getattr(tool_result, "attachments", None)
            if isinstance(attachments, list):
                for msg_event, _ in injectable:
                    chain = getattr(getattr(msg_event, "message", None), "chain", None) or []
                    for ele in chain:
                        if isinstance(ele, (Image, Record, File)) and hasattr(ele, "to_path"):
                            attachments.append(ele)
        except Exception:
            self._log_debug("媒体附件透传失败（已忽略，不影响文本流入）")

        logger.info(f"[Midflight] {sid} 流入 {len(injectable)} 条消息到当前轮")

    # ============ 去重兜底：掐掉完全重复的批次 ============

    @on.llm_request()
    async def _track_run_start(self, event, *_):
        """ON_LLM_REQUEST 在每轮 agent 启动前触发（且只带批次事件）——
        这是"一轮确实要开始执行"的最早可靠信号，在此标记运行开始。
        相比在 ON_IM_BATCH_MESSAGE 放行时标记，此处标记不会被
        session_merger 等下游 handler 拦停批次的情况污染（批次被拦 = 不会走到这里）。"""
        try:
            if not isinstance(event, KiraMessageBatchEvent):
                return
            sid = getattr(event, "sid", None) or getattr(getattr(event, "session", None), "sid", None)
            if sid:
                # 新轮开始：旧轮若没收到收尾事件（异常路径），其 pending 消息
                # 与等待计数会残留并被新轮继承——旧轮消息错注入新轮、新轮
                # 继承旧轮超限计数。先清理再登记（含旧的“已收尾”记录）。
                self._restore_pending_silent(sid)
                self._finished_run.pop(sid, None)
                self._run_active[sid] = {"event": event, "ts": time.time(), "ending": False}
        except Exception:
            pass

    @on.llm_response()
    async def _ensure_stop_checkpoint(self, event, resp=None, *_):
        """三件事：
        1) 空 handler 兜底：agent_executor 的 is_stopped 停止检查位于
           ON_LLM_RESPONSE handler 循环体内（agent_executor.py:149-164）。
           若环境中没有任何插件注册该事件，循环体不执行，event.stop() 将无法
           阻止后续 LLM 步。注册此 handler 保证停止检查点必然被执行。
        2) 运行心跳：LLM 响应到达即证明该 sid 的 agent 轮仍在执行。
        3) 收尾检测：无 tool_calls 的响应 = 最终文本步，本轮即将结束，
           立即清除"运行中"标记并还原批次拦截来的待注入消息（若有）。"""
        try:
            sid = getattr(event, "sid", None) or getattr(getattr(event, "session", None), "sid", None)
            if not sid:
                return
            run = self._run_active.get(sid)
            if run is None or run.get("event") is not event:
                return
            tool_calls = getattr(resp, "tool_calls", None)
            if not tool_calls:
                # 最终文本步：本轮结束
                await self._finish_run(sid)
                return
            run["ts"] = time.time()
            self._note_activity(sid)
            # 末步仍带工具：该步工具执行完 agent 即结束（无最终文本步），
            # 打标记让下一个工具边界不再注入（注入会丢），直接收尾还原
            idx = getattr(resp, "agent_step_index", None)
            if idx:
                max_steps = self._to_int(self._read_core_config("bot_config.agent.max_tool_loop"), 2)
                if max_steps > 0 and int(idx) >= max_steps:
                    run["ending"] = True
        except Exception:
            logger.exception("[Midflight] llm_response 处理异常（已自捕获）")

    @on.final_result()
    async def _on_final_result(self, event, final_result=None, *_):
        """ON_FINAL_RESULT（框架 v2.34.4 起真正派发）＝「一轮 agent 执行结束」的权威信号。

        框架在 agent 循环结束、消息已发出、记忆尚未写入时派发一次；**本轮被 stop 也照样
        派发**（handler 循环之外）。因此它能覆盖 `_ensure_stop_checkpoint` 覆盖不到的缝：
        在 ON_LLM_RESPONSE / ON_STEP_RESULT 阶段被别的插件 stop 时，框架的 handler 循环
        会 `break`，本插件的收尾 handler（优先级 0，排在 S/Z 的 50 之后）可能根本没执行，
        运行中标记就会残留 → 之后该会话的消息全被拦截。

        只在「事件对象就是本插件跟踪的那一轮」时收尾；外来事件（第三方桩事件）不响应。
        框架在批次于 ON_IM_BATCH_MESSAGE / ON_LLM_REQUEST 阶段就被 return 时**不派发**
        本钩子——那两种情况本轮根本没跑起来，本来就没有状态需要清。"""
        try:
            sid = getattr(event, "sid", None) or getattr(getattr(event, "session", None), "sid", None)
            if not sid:
                return
            run = self._run_active.get(sid)
            if run is None or run.get("event") is not event:
                return
            await self._finish_run(sid)
        except Exception:
            logger.exception("[Midflight] final_result 处理异常（已自捕获）")

    @on.im_batch_message(priority=Priority.SYS_HIGH)
    async def on_batch_dedup(self, event: KiraMessageBatchEvent, *_):
        """批次守卫（必须先于 S版 QueueMerge 等 HIGH 优先级 handler 执行）：
        1) QueueMerge 自推送批次直接放行（避免拦截它推送的积压）；
        2) 全部消息已被本插件消费过的批次 → 掐掉，防重复开轮；
        3) 该 sid 正在跑 agent 轮 → 批次消息不再排队：命中停止词则停止当前轮，
           通过过滤的转入待注入队列（下一个工具边界注入），未通过的放回 buffer，
           然后掐掉批次（阻止 QueueMerge pending / 另开平行轮）；
        4) 该 sid 空闲 → 放行（运行开始的标记改在 ON_LLM_REQUEST 做）。

        优先级说明：框架约定 SYS_HIGH 保留给系统插件，但本插件的职责就是
        "在队列合并类插件把消息扣进 pending 之前截住运行中的批次"，
        同优先级时执行顺序取决于插件加载顺序（os.listdir，不确定），
        只有 SYS_HIGH 能保证确定性。框架排序按 int 比较，功能上安全。
        """
        try:
            if not self.enabled:
                return
            sid = getattr(event, "sid", None)
            if not sid:
                return

            # 会话级 override 可能禁用本插件：与 drain 路径同一判定，
            # 否则 override 禁用的会话运行中批次仍会被拦截转入 pending，
            # 但 _handle_tool_result 因 enabled=False 不消费，消息卡到轮结束
            cfg = self._eff_config(sid)
            if not cfg["enabled"]:
                return

            # QueueMerge 自推送批次（积压重放）绝对放行
            extra = getattr(event, "extra", None) or {}
            if extra.get("_qm_self"):
                return

            # session_merger 的跨会话控制批次（ROUTE/handoff）绝对放行：
            # 它们携带合并上文、工具禁用等特殊语义，不是用户闲聊，不能流入
            if extra.get("merger_handoff") or self._is_control_batch(event):
                return

            messages = getattr(event, "messages", None) or []

            # 1) 看门狗已释放过的批次：绝对放行。
            # 看门狗把等待超时的拦截消息还原回缓冲后，它们会随下次 flush 重新成批次；
            # 若不放行就又会被拦回去（"还原→成批→再拦"死循环），这里按消息 id 记住。
            if self._bypass and messages:
                _keys = [self._dedup_key(m) for m in messages]
                if _keys and all(k and k in self._bypass for k in _keys):
                    logger.info(f"[Midflight] {sid} 批次 {getattr(event, 'event_id', '')} "
                                f"为看门狗已释放的消息，放行不再拦截")
                    return

            # 2) 全消费批次去重
            consumed_map = self._consumed.get(sid)
            if consumed_map and messages:
                keys = [self._dedup_key(m) for m in messages]
                if keys and all(k and k in consumed_map for k in keys):
                    logger.info(f"[Midflight] {sid} 批次 {getattr(event, 'event_id', '')} 全部为已消费消息，掐掉重复轮")
                    event.stop()
                    # QueueMerge 可能已放行该批次并标记 inflight：批次被掐 = run
                    # 不会发生，它的 inflight 会卡到 stall 兜底（~180s），期间新消息
                    # 全部排队。顺手释放，避免卡死。
                    self._release_queue_merge(sid, str(getattr(event, "event_id", "") or ""))
                    return

            # 2) 运行中批次拦截
            run = self._get_active_run(sid)
            if run is None:
                # 空闲：放行。运行开始的标记在 ON_LLM_REQUEST 做（批次被
                # session_merger 等下游拦停时不会误标）。
                return

            if self._in_blacklist(sid) or not messages:
                return

            cfg = self._eff_config(sid)
            now = time.time()
            stop_hit, injectable, rejected = [], [], []
            for m in messages:
                # ① 触发型系统事件放行（system_trigger_passthrough，默认关=照常注入判定）：
                # 开启后 S版主动回复/定时任务等 system_* sender 的消息既不判停止词也
                # 不转注入，原样留在批次里——一条都不消费时自然走下方"没有要消费的
                # 消息：批次原样放行"路径，系统提示词照常开新轮（新轮请求与工具
                # 黑名单语义完整保留）
                if cfg["system_trigger_passthrough"] and self._is_system_sender(m):
                    continue
                shim = _BufferedMsgShim(m, event)
                try:
                    key = self._dedup_key(m)
                    if key and consumed_map and key in consumed_map:
                        continue  # 部分已消费：跳过该条
                    text = self._plain_text(shim)
                    # ② 系统来源消息（publish_notice 提醒与 system_* 触发型事件）可注入
                    # 在飞轮，但永不参与停止词判定（notice_skip_stop，默认开；关掉则
                    # 一视同仁可停轮）
                    if (cfg["stop_enabled"] and not self._skip_stop_for(m, cfg)
                            and self._stop_allowed(shim, cfg) and self._match_stop(text, cfg)):
                        stop_hit.append(shim)
                        continue
                    if self._pass_filters(shim, text, cfg, now):
                        injectable.append((shim, text))
                    else:
                        rejected.append(shim)
                except Exception:
                    rejected.append(shim)
                    logger.exception("[Midflight] 批次消息过滤异常，已放回原流程")

            if not stop_hit and not injectable:
                return  # 没有要消费的消息：批次原样放行给 QueueMerge / 官方

            # 停止与注入互斥（与 drain 路径同一语义）
            consumed_map = self._consumed.setdefault(sid, {})
            buffer = self.ctx.get_buffer(sid)
            if stop_hit:
                put_back = rejected + [s for s, _ in injectable]
                if put_back and buffer is not None:
                    async with buffer.lock:
                        buffer.buffer[:0] = put_back
                for s in stop_hit:
                    key = self._dedup_key(getattr(s, "message", None))
                    if key:
                        consumed_map[key] = now
                run["event"].stop()  # 停的是正在跑的那一轮的事件对象
                event.stop()
                logger.info(f"[Midflight] {sid} 批次消息命中停止词，停止当前轮")
                # 同上：本轮被停后不会再有收尾事件，必须当场收尾，否则留下幽灵运行中
                await self._finish_run(sid)
                return

            # 注入：转入待注入队列，下一个工具边界注入
            if rejected and buffer is not None:
                async with buffer.lock:
                    buffer.buffer[:0] = rejected
            q = self._pending_inject.setdefault(sid, [])
            for shim, text in injectable:
                q.append((shim, text, now))
            event.stop()
            logger.info(f"[Midflight] {sid} 拦截运行中批次 {getattr(event, 'event_id', '')}，{len(injectable)} 条转入流入队列")
        except Exception:
            logger.exception("[Midflight] on_batch_dedup 异常（已自捕获）")

    # ============ 运行状态跟踪 ============

    @staticmethod
    def _is_control_batch(event) -> bool:
        """识别跨会话/合并类插件的控制批次（按文本标记，与 session_merger 的
        判定逻辑同源：cross_session.py 的 ROUTE_MARKER 与官方旧版跨会话投递模板）。"""
        try:
            parts = []
            for m in (getattr(event, "messages", None) or []):
                s = str(getattr(m, "message_str", "") or "")
                if not s:
                    chain = getattr(m, "chain", None) or []
                    s = "".join(getattr(e, "text", "") for e in chain if isinstance(e, Text))
                parts.append(s)
            blob = "\n".join(parts)
            if not blob:
                return False
            if "[merge_cross_session_request]" in blob:
                return True
            # 官方旧版跨会话投递 notice（弱上下文，需独立成轮做禁工具处理）
            if "跨会话消息" in blob and "不需要再次调用跨会话工具" in blob:
                return True
        except Exception:
            pass
        return False

    def _touch_run(self, sid: str, event):
        """工具边界心跳：刷新该 sid 正在执行的 agent 轮。

        返回生效的 run 字典；**不是本插件跟踪的那一轮时返回 None**（调用方必须直接退出）。

        ⚠️ 运行中状态**只能由 ON_LLM_REQUEST（_track_run_start）建立**：框架里每一轮
        agent 都必然先经过 ON_LLM_REQUEST，工具边界只可能是它的后续。所以这里遇到
        「没有登记过任何轮」或「事件对象不是本轮」时，只可能是：
          · **外来事件**——第三方插件自造的桩事件（例如子代理插件的
            `_make_stub_event`：它携带**真实会话 sid**，但 event_id/事件对象都不是
            框架的批次，且永远不会走 handle_im_batch_message）；
          · 一轮结束后的**迟到边界**。
        两种情况都必须**直接忽略**（不注入、不还原、不重建）：
          · 若按“以最新事件为准重建”处理，桩事件会凭空造出一个“幽灵运行中” →
            该会话此后所有消息批次被 `on_batch_dedup` 拦截转入流入队列，却再也等不到
            工具边界去注入；而下一个桩事件边界还会把流入队列里的**用户消息灌进子代理
            的工具结果**并标记已消费（主 AI 永远看不到，之后 flush 回来还会被去重掐掉）。
          · 迟到的旧轮边界同理（本轮已结束，重建只会留下幽灵）。
        心跳过期（_get_active_run 超时）那条路径**不需要**靠工具边界抢救：超时释放时
        已把流入队列还原回缓冲，新批次会照常放行（见 S8 场景）。"""
        eid = getattr(event, "event_id", None)
        if eid and self._finished_run.get(sid) == eid:
            # 本轮已在更早的工具边界收尾 —— 框架对「一次 LLM 响应里的多个
            # tool_calls」会逐个派发 ON_TOOL_RESULT，第一个边界已触发
            # _finish_run；若这里重建 run，就会留下一个“幽灵运行中”状态
            # （心跳时间停在最后一个工具边界），此后该会话的新消息批次会被
            # on_batch_dedup 误判为“运行中”而拦截转入流入队列，但本轮根本不
            # 会再有工具边界来注入 → 消息卡住，直到心跳超时兜底才被还原。
            return None
        run = self._run_active.get(sid)
        if run is None or run.get("event") is not event:
            self._log_debug(
                f"{sid} 工具边界来自非本轮事件（event_id={eid}），已忽略"
                f"（不注入、不还原、不重建运行中状态）"
            )
            return None
        run["ts"] = time.time()
        self._note_activity(sid)
        return run

    def _get_active_run(self, sid: str):
        """取该 sid 的运行中状态；超过活动超时视为已结束（异常路径兜底）。"""
        run = self._run_active.get(sid)
        if run is None:
            return None
        if time.time() - float(run.get("ts", 0)) > self._active_timeout:
            self._log_debug(f"{sid} 运行心跳超 {self._active_timeout}s 未更新，判定已结束")
            self._run_active.pop(sid, None)
            self._restore_pending_silent(sid)
            self._wait_steps.pop(sid, None)
            return None
        return run

    async def _finish_run(self, sid: str):
        """一轮结束：清标记，并把批次拦截来的待注入消息还原回 buffer 后主动 flush，
        让它们立刻走正常管线成为新一轮（不卡住、不丢失）。"""
        run = self._run_active.pop(sid, None)
        self._wait_steps.pop(sid, None)
        # 本轮注入计数一并清理：残留会随轮数无限增长
        # （_gc 只按时间戳淘汰过期项，不负责在途轮次）
        if run is not None:
            eid = getattr(run.get("event"), "event_id", None)
            if eid:
                self._run_inject_count.pop(eid, None)
                # 记下“本轮已收尾”：同一轮里剩下的工具边界（一次 LLM 响应带多个
                # tool_calls 时会逐个派发 ON_TOOL_RESULT）不得把本轮重新标记为
                # 运行中；新的一轮会用新的 event_id，不受影响。
                self._finished_run[sid] = eid
        items = self._pending_inject.pop(sid, None)
        if not items:
            return
        shims = [item[0] for item in items]
        logger.info(f"[Midflight] {sid} 本轮已结束，{len(shims)} 条拦截消息还原走正常管线")
        await self._restore_to_buffer(sid, shims, flush=True)

    def _restore_pending_silent(self, sid: str):
        """同步兜底还原（不 flush）：用于心跳超时等无法 await 的场景，
        消息回到 buffer，等聊天插件下一次防抖/合并自然带出。
        还原 pending = 本轮插话窗口结束，等待计数一并清零。"""
        self._wait_steps.pop(sid, None)
        items = self._pending_inject.pop(sid, None)
        if not items:
            return
        try:
            buffer = self.ctx.get_buffer(sid)
            if buffer is not None:
                shims = [item[0] for item in items]
                buffer.buffer[:0] = shims
                logger.info(f"[Midflight] {sid} {len(shims)} 条拦截消息已还原回 buffer")
        except Exception:
            logger.exception("[Midflight] 还原拦截消息异常（已自捕获）")

    async def _restore_to_buffer(self, sid: str, shims: list, flush: bool = False):
        """把消息还原回 SessionBuffer（保持原顺序置于头部），可选立即 flush。"""
        if not shims:
            return
        try:
            buffer = self.ctx.get_buffer(sid)
            if buffer is None:
                return
            async with buffer.lock:
                buffer.buffer[:0] = shims
            if flush:
                await self.ctx.flush_session_messages(sid)
        except Exception:
            logger.exception("[Midflight] 还原消息回 buffer 异常（已自捕获）")

    # ============ 流入队列看门狗 ============
    #
    # 为什么需要它：本插件靠"工具边界 + 末步标记"推断一轮的结束，但框架并**没有**
    # 一个"本轮结束"的钩子（ON_FINAL_RESULT 在框架里从未派发，已核实）。以下情况
    # 都会让收尾信号永远不来，留下"幽灵运行中"：
    #   · 本轮被别的插件在 ON_LLM_REQUEST / ON_STEP_RESULT 阶段 stop（本轮根本没跑起来）；
    #   · 运行中途抛异常被 event bus 静默吞掉（handle_im_batch_message 外层无 try）；
    #   · 最后一步的工具调用全部被 max_tool_calls_per_turn 跳过（一个 ON_TOOL_RESULT 都没有）；
    #   · 插件自身 bug / 未来框架行为变化。
    # 症状都一样：之后该会话的消息全被拦截转入流入队列，却永远等不到工具边界去处理，
    # 只能等"运行中判定超时"（LLM 超时 + 工具超时，默认 180s）兜底 —— 对只有 1~2 步的
    # 用户尤其明显（一轮只有几秒，兜底窗口却是 180 秒，长两个数量级）。
    # 看门狗就是"搭不上车就别硬搭"：等待超过 inject_grace_seconds 就还原走正常管线。

    async def _watchdog_loop(self):
        try:
            while True:
                grace = float(self.inject_grace_seconds or 0)
                # 每个 grace 至少检查两次（默认 10s → 2s 一次）；grace 调小时自动变密，
                # 保证"软放行 → 再等一个 grace → 硬放行"的总延迟 ≈ 2×grace
                await asyncio.sleep(max(0.5, min(2.0, grace / 2)) if grace > 0 else 2.0)
                if grace <= 0:
                    continue
                now = time.time()
                self._gc()
                # ① 有积压且等够了 → 软放行（只放行消息，保留"运行中"状态）
                for sid in list(self._pending_inject.keys()):
                    items = self._pending_inject.get(sid) or []
                    if not items:
                        self._pending_inject.pop(sid, None)
                        continue
                    try:
                        oldest = min(float(it[2]) for it in items if len(it) > 2)
                    except Exception:
                        oldest = now
                    if now - oldest >= grace:
                        self._wd_strikes[sid] = self._wd_strikes.get(sid, 0) + 1
                        await self._release_pending(sid, now - oldest, hard=False)
                # ② 软放行后又过了一个 grace 仍毫无活动（期间任何 LLM 响应/工具边界
                #    都会被 _note_activity 清零）→ 断定本轮已结束/卡死：硬放行
                for sid in list(self._wd_escalate.keys()):
                    if sid not in self._wd_strikes:
                        self._wd_escalate.pop(sid, None)
                        continue
                    if now - self._wd_escalate[sid] >= grace:
                        await self._escalate_dead_run(sid)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("[Midflight] 流入看门狗异常（已自捕获）")

    def _note_activity(self, sid: str):
        """本轮有任何活动（LLM 响应 / 工具边界）→ 撤销看门狗的放行计数与升级计时。"""
        if self._wd_strikes:
            self._wd_strikes.pop(sid, None)
        if self._wd_escalate:
            self._wd_escalate.pop(sid, None)

    async def _release_pending(self, sid: str, waited: float, hard: bool = False):
        """等太久还没搭上工具边界：把拦截消息还原走正常管线。

        还原的消息会记进 `_bypass`（放行标记）——它们随下次 flush 重新成批次时，
        批次守卫会直接放行，不会又被拦回来（否则就是"还原 → 成批 → 再拦"死循环）。

        `hard=True` 时会一并 `_escalate_dead_run()`（清运行中标记 + 释放 QueueMerge）。
        """
        items = self._pending_inject.pop(sid, None) or []
        shims = [it[0] for it in items]
        now = time.time()
        for it in items:
            try:
                key = self._dedup_key(getattr(it[0], "message", None))
                if key:
                    self._bypass[key] = now
            except Exception:
                continue
        if shims:
            if hard:
                logger.warning(
                    f"[Midflight] {sid} {len(shims)} 条拦截消息等待 {waited:.0f}s 仍未搭上工具边界"
                    f"（本轮无收尾信号），已还原走正常管线"
                )
            else:
                logger.warning(
                    f"[Midflight] {sid} {len(shims)} 条拦截消息等待 {waited:.0f}s 仍未搭上工具边界，"
                    f"先还原走正常管线（保留本轮状态：若只是这一步慢，后续消息仍可搭车）"
                )
                # 记下"软放行时刻"：再过一个 grace 仍毫无活动 → 升级为硬放行
                self._wd_escalate[sid] = now
            await self._restore_to_buffer(sid, shims, flush=True)
        if hard:
            await self._escalate_dead_run(sid)

    async def _escalate_dead_run(self, sid: str):
        """软放行后又一个 grace 仍毫无活动 ⇒ 断定本轮不会再有工具边界（真·结束/卡死）。

        清掉"运行中"标记并立墓碑（防止迟到的工具边界把它复活成幽灵运行中）；
        顺手释放 S/Z 版 QueueMerge 攥着的 in-flight —— 否则它会把后续消息扣在
        自己的 pending 里、等 stall 兜底（同样 ~180s）。
        """
        self._wd_escalate.pop(sid, None)
        self._wd_strikes.pop(sid, None)
        self._wait_steps.pop(sid, None)
        run = self._run_active.pop(sid, None)
        eid = getattr(run.get("event"), "event_id", None) if run else None
        if eid:
            self._finished_run[sid] = eid
            self._run_inject_count.pop(eid, None)
            self._release_queue_merge(sid, eid)
        logger.warning(
            f"[Midflight] {sid} 软放行后仍无任何活动（LLM 响应 / 工具边界），判定本轮已结束："
            f"已清除运行中标记" + ("，并释放 QueueMerge 的 in-flight" if eid else "")
        )

    # ============ 过滤器 ============

    def _pass_filters(self, msg_event, text: str, cfg: dict, now: float) -> bool:
        message = getattr(msg_event, "message", None)
        if message is None:
            return False

        # 白名单（默认关）
        if cfg["whitelist_enabled"]:
            sender_id = str(getattr(getattr(message, "sender", None), "user_id", "") or "")
            if sender_id not in cfg["whitelist_users"]:
                return False

        # 新鲜度
        ts = getattr(message, "timestamp", 0) or 0
        try:
            if cfg["_freshness"] > 0 and now - float(ts) > cfg["_freshness"]:
                self._log_debug(f"消息 {getattr(message, 'message_id', '')} 超出新鲜度窗口，跳过")
                return False
        except Exception:
            pass

        # 内容正则黑名单
        for pat in cfg["block_patterns"]:
            try:
                if re.search(pat, text):
                    return False
            except re.error:
                continue

        # 媒体类型开关：含被禁用类型媒体的消息不流入（放回 buffer 走官方管线）。
        # 媒体内容本身由官方 message_format_to_text 统一转换（语音→STT 文字、
        # 图片/表情→VLM 描述+落盘路径、文件/视频≤10MB→落盘路径、转发→递归展开），
        # 流入后 bot 不仅能看到描述，还能用 file_read 类工具读取原文件。
        if not self._media_allowed(msg_event, cfg):
            return False

        # 流入方式
        return self._match_flow_method(msg_event, text, cfg)

    def _match_flow_method(self, msg_event, text: str, cfg: dict) -> bool:
        try:
            is_group = bool(msg_event.is_group_message())
        except Exception:
            is_group = getattr(getattr(msg_event, "message", None), "group", None) is not None
        method = cfg["flow_method_group"] if is_group else cfg["flow_method_dm"]

        if method == "any":
            return True

        message = getattr(msg_event, "message", None)
        chain = getattr(message, "chain", None) or []
        self_id = str(getattr(message, "self_id", "") or "")

        hit_at = False
        hit_reply = False
        for ele in chain:
            if isinstance(ele, At) and self_id and ele.pid == self_id:
                hit_at = True
            elif isinstance(ele, Reply):
                hit_reply = True
        # 部分适配器只在 is_mentioned 上体现 @
        if not hit_at and getattr(message, "is_mentioned", None) is True and not cfg["wake_keywords"]:
            hit_at = True

        hit_keyword = bool(cfg["wake_keywords"]) and any(w in text for w in cfg["wake_keywords"])
        hit_poke = cfg["accept_poke"] and self._is_poke(msg_event)

        if method == "at":
            return hit_at
        if method == "reply":
            return hit_reply
        if method == "keyword":
            return hit_keyword
        # all：@ / 回复 / 唤醒词 / 戳一戳 任一即可
        return hit_at or hit_reply or hit_keyword or hit_poke

    def _media_allowed(self, msg_event, cfg: dict) -> bool:
        """检查消息链中的媒体元素是否被对应开关允许（防御式，异常视为允许）。"""
        try:
            message = getattr(msg_event, "message", None)
            chain = getattr(message, "chain", None) or []
            for ele in chain:
                if isinstance(ele, Record) and not cfg["allow_record"]:
                    return False
                if isinstance(ele, (Image, Sticker)) and not cfg["allow_image"]:
                    return False
                if isinstance(ele, (File, Video)) and not cfg["allow_file"]:
                    return False
                if isinstance(ele, Forward) and not cfg["allow_forward"]:
                    return False
            return True
        except Exception:
            return True

    def _is_poke(self, msg_event) -> bool:
        """戳一戳识别（防御式，识别不了就跳过不报错）。"""
        try:
            message = getattr(msg_event, "message", None)
            if message is None:
                return False
            chain = getattr(message, "chain", None) or []
            for ele in chain:
                if isinstance(ele, Poke):
                    return True
            # OneBot 风格 notice：notify/poke
            if getattr(message, "is_notice", False):
                raw = getattr(message, "raw_message", None)
                if isinstance(raw, dict):
                    if raw.get("sub_type") == "poke":
                        return True
                    if raw.get("notice_type") == "notify" and "poke" in str(raw.get("sub_type", "")):
                        return True
        except Exception:
            pass
        return False

    def _stop_allowed(self, msg_event, cfg: dict) -> bool:
        """停止词用户白名单（默认关=人人可停；开启后仅名单内 user_id 可触发停止）。"""
        if not cfg.get("stop_whitelist_enabled"):
            return True
        message = getattr(msg_event, "message", None)
        sender_id = str(getattr(getattr(message, "sender", None), "user_id", "") or "")
        return sender_id in cfg.get("stop_whitelist_users", [])

    def _match_stop(self, text: str, cfg: dict) -> bool:
        if not text or not cfg["stop_words"]:
            return False
        mode = cfg["stop_match_mode"]
        for w in cfg["stop_words"]:
            try:
                if mode == "exact" and text.strip() == w:
                    return True
                if mode == "regex" and re.search(w, text):
                    return True
                if mode == "contains" and w in text:
                    return True
            except re.error:
                continue
        return False

    # ============ 工具方法 ============

    def _eff_config(self, sid: str | None) -> dict:
        """全局配置 + 会话级 overrides（支持通配）合成生效配置。"""
        cfg = {
            "enabled": self.enabled,
            "flow_method_group": self.flow_method_group,
            "flow_method_dm": self.flow_method_dm,
            "accept_poke": self.accept_poke,
            "wake_keywords": self.wake_keywords,
            "stop_enabled": self.stop_enabled,
            "stop_words": self.stop_words,
            "stop_match_mode": self.stop_match_mode,
            "stop_whitelist_enabled": self.stop_whitelist_enabled,
            "stop_whitelist_users": self.stop_whitelist_users,
            "whitelist_enabled": self.whitelist_enabled,
            "whitelist_users": self.whitelist_users,
            "max_inject_per_run": self.max_inject_per_run,
            "freshness_seconds": self.freshness_seconds,
            "max_length": self.max_length,
            "block_patterns": self.block_patterns,
            "template": self.template,
            "inject_hint": self.inject_hint,
            "inject_hint_text": self.inject_hint_text,
            "inject_timeout_steps": self.inject_timeout_steps,
            "inject_grace_seconds": self.inject_grace_seconds,
            "system_trigger_passthrough": self.system_trigger_passthrough,
            "notice_skip_stop": self.notice_skip_stop,
            "debug": self.debug,
            "allow_record": self.allow_record,
            "allow_image": self.allow_image,
            "allow_file": self.allow_file,
            "allow_forward": self.allow_forward,
            "_max_inject": self._eff_max_inject,
            "_freshness": self._eff_freshness,
        }
        if sid and self.overrides:
            for pattern, ov in self.overrides.items():
                if not isinstance(ov, dict):
                    continue
                try:
                    if fnmatch.fnmatchcase(sid, str(pattern)):
                        for k, v in ov.items():
                            if k in OVERRIDABLE_KEYS:
                                cfg[k] = v
                        # 覆盖后重算自动项
                        if self._to_int(cfg["max_inject_per_run"], 0) > 0:
                            cfg["_max_inject"] = self._to_int(cfg["max_inject_per_run"], 0)
                        ov_fresh = self._to_int(cfg["freshness_seconds"], 0)
                        if ov_fresh != 0:
                            cfg["_freshness"] = ov_fresh  # -1=不限，>0=自定义秒数
                except Exception:
                    continue
        cfg["_max_inject"] = max(1, self._to_int(cfg.get("_max_inject"), FALLBACK_MAX_INJECT))
        cfg["_freshness"] = max(0, self._to_int(cfg.get("_freshness"), FALLBACK_FRESHNESS))
        if not isinstance(cfg.get("wake_keywords"), list):
            cfg["wake_keywords"] = []
        if not isinstance(cfg.get("stop_words"), list):
            cfg["stop_words"] = []
        if not isinstance(cfg.get("whitelist_users"), list):
            cfg["whitelist_users"] = []
        if not isinstance(cfg.get("stop_whitelist_users"), list):
            cfg["stop_whitelist_users"] = []
        if not isinstance(cfg.get("block_patterns"), list):
            cfg["block_patterns"] = []
        cfg["whitelist_users"] = [str(u) for u in cfg["whitelist_users"]]
        cfg["stop_whitelist_users"] = [str(u) for u in cfg["stop_whitelist_users"]]
        return cfg

    def _in_blacklist(self, sid: str) -> bool:
        for pattern in self.session_blacklist:
            try:
                if fnmatch.fnmatchcase(sid, pattern):
                    return True
            except Exception:
                continue
        return False

    def _release_queue_merge(self, sid: str, event_id: str):
        """掐掉批次后 best-effort 释放 S版/Z版 QueueMerge 的 inflight 标记。
        若 QueueMerge 已放行该批次（inflight=它），批次被掐意味着 run 永远不会
        发生，QueueMerge 会卡到 stall 兜底（默认约 180s）才自愈，期间新消息全部
        排队。找不到实例/属性就跳过，绝不报错。"""
        for pid in ("sustained-chat", "default-chat（z）"):
            try:
                inst = self.ctx.get_plugin_inst(pid)
                sched = getattr(inst, "merge_scheduler", None) if inst is not None else None
                if sched is None:
                    continue
                inflight = getattr(sched, "_inflight", None)
                if isinstance(inflight, dict) and inflight.get(sid) == event_id:
                    inflight.pop(sid, None)
                    # v2.5.16/v1.8.7 起 QueueMerge 还会记事件对象与末步标记，一并清掉
                    ev = getattr(sched, "_inflight_event", None)
                    if isinstance(ev, dict):
                        ev.pop(sid, None)
                    since = getattr(sched, "_inflight_since", None)
                    if isinstance(since, dict):
                        since.pop(sid, None)
                    marked = getattr(sched, "_final_marked", None)
                    if marked is not None and hasattr(marked, "discard"):
                        marked.discard(sid)
                    logger.info(f"[Midflight] {sid} 已释放 {pid} QueueMerge 的 inflight 标记")
            except Exception:
                continue

    @staticmethod
    def _is_notice(message) -> bool:
        """识别系统提醒类消息（框架 publish_notice 的产物，plugin_context.py:203-244：
        is_notice=True 且 message_id 恒为 "system_message"；群聊 sender.user_id 为
        "unknown"、私聊为 sid）。如 S/Z 的骚扰/休眠提醒。防御式取值，异常视为非提醒。"""
        try:
            if getattr(message, "is_notice", False):
                return True
            return str(getattr(message, "message_id", "") or "") == "system_message"
        except Exception:
            return False

    @staticmethod
    def _is_system_sender(message) -> bool:
        """识别触发型系统事件的 sender（S版私聊主动回复 system_proactive_dm、
        定时任务 system_scheduled，及其他 system_ 前缀的合成事件 sender）。
        防御式取值，异常视为非系统 sender。"""
        try:
            sender_id = str(getattr(getattr(message, "sender", None), "user_id", "") or "")
            return sender_id in SYSTEM_SENDER_IDS or sender_id.startswith("system_")
        except Exception:
            return False

    def _skip_stop_for(self, message, cfg: dict) -> bool:
        """notice_skip_stop 开启时，系统来源的消息——publish_notice 提醒
        （_is_notice）与触发型系统事件（_is_system_sender）——都不参与停止词
        判定（即所有 system 来源消息默认不能触发停止），但仍照常走注入过滤。
        供批次拦截与工具边界 drain 两处停止词判定复用，避免逻辑漂移。"""
        if not cfg["notice_skip_stop"]:
            return False
        return self._is_notice(message) or self._is_system_sender(message)

    @staticmethod
    def _dedup_key(message) -> str:
        """去重键 = message_id + 时间戳复合键。官方 publish_notice 的系统消息
        message_id 是写死的常量 "system_message"（plugin_context.py:185），
        单用 id 会把不同时间的后台任务结果误判成同一条已消费消息而整批误杀；
        同一消息真被重放时时间戳不变，仍能正确去重。"""
        try:
            mid = str(getattr(message, "message_id", "") or "")
            if not mid:
                return ""
            ts = int(float(getattr(message, "timestamp", 0) or 0))
            return f"{mid}@{ts}"
        except Exception:
            return ""

    def _plain_text(self, msg_event) -> str:
        """提取纯文本（仅 Text 元素），防御式取值。"""
        try:
            chain = getattr(getattr(msg_event, "message", None), "chain", None) or []
            return "".join(ele.text for ele in chain if isinstance(ele, Text))
        except Exception:
            return ""

    def _format_native(self, msg_event, text: str) -> str:
        """生成与官方一致的消息格式。优先直接调用官方内置 kira-ai 插件的
        _format_user_message（builtin_plugins/kira-ai/main.py:44-59），
        自动跟随官方格式演进；取不到实例/方法时退化为内置复刻版；
        任何异常最终退化为裸文本，绝不报错。"""
        try:
            message = getattr(msg_event, "message", None) or msg_event
            # 官方函数读取 msg.message_str：buffer/拦截路径的消息可能尚未被
            # handle_im_batch_message 文本化过，先补上（与官方赋值完全一致）
            try:
                if not getattr(message, "message_str", None):
                    message.message_str = text
            except Exception:
                pass
            inst = self.ctx.get_plugin_inst("kira-ai")
            fn = getattr(inst, "_format_user_message", None) if inst is not None else None
            if callable(fn):
                result = fn(message)
                if isinstance(result, str) and result:
                    return result
        except Exception:
            pass
        # 兜底：复刻官方格式（官方函数不可用时的静态快照）
        try:
            message = getattr(msg_event, "message", None) or msg_event
            ts = float(getattr(message, "timestamp", 0) or time.time())
            tz = self.ctx.get_timezone()
            dt = datetime.fromtimestamp(ts, tz=tz) if tz else datetime.fromtimestamp(ts)
            date_str = dt.strftime("%b %d %Y %H:%M %a")
            mid = getattr(message, "message_id", "")
            sender = getattr(message, "sender", None)
            nick = getattr(sender, "nickname", "") if sender else ""
            uid = getattr(sender, "user_id", "") if sender else ""
            is_notice = bool(getattr(message, "is_notice", False))
            try:
                is_group = bool(message.is_group_message())
            except Exception:
                try:
                    is_group = bool(msg_event.is_group_message())
                except Exception:
                    is_group = getattr(message, "group", None) is not None
            if is_group:
                group = getattr(message, "group", None)
                gn = getattr(group, "group_name", "") if group else ""
                gid = getattr(group, "group_id", "") if group else ""
                if is_notice:
                    return f"[{date_str}] Notice [group_id: {gid}, user_id: {uid}] | {text}"
                return (f"[{date_str}] [message_id: {mid}] "
                        f"[group_name: {gn} group_id: {gid} "
                        f"user_nickname: {nick}, user_id: {uid}] | {text}")
            if is_notice:
                return f"[{date_str}] Notice [user_id: {uid}] | {text}"
            return f"[{date_str}] [message_id: {mid}] [user_nickname: {nick}, user_id: {uid}] | {text}"
        except Exception:
            return text

    def _render_template(self, template: str, msg_event, text: str, n: int) -> str:
        message = getattr(msg_event, "message", None)
        nickname = str(getattr(getattr(message, "sender", None), "nickname", "") or "")
        user_id = str(getattr(getattr(message, "sender", None), "user_id", "") or "")
        message_id = str(getattr(message, "message_id", "") or "")
        group = getattr(message, "group", None)
        group_name = str(getattr(group, "group_name", "") or "") if group else ""
        group_id = str(getattr(group, "group_id", "") or "") if group else ""
        ts = getattr(message, "timestamp", 0) or 0
        try:
            time_str = time.strftime("%H:%M:%S", time.localtime(float(ts)))
        except Exception:
            time_str = ""
        try:
            return template.format(
                sender_nickname=nickname, text=text, time=time_str, n=n,
                user_id=user_id, message_id=message_id,
                group_name=group_name, group_id=group_id)
        except Exception:
            return text

    @staticmethod
    def _self_version() -> str:
        """动态读取同目录 manifest.json 的版本号（启动日志用，杜绝硬编码漂移）。"""
        try:
            import json as _json
            from pathlib import Path as _Path
            m = _json.loads((_Path(__file__).parent / "manifest.json").read_text(encoding="utf-8"))
            return str(m.get("version") or "?")
        except Exception:
            return "?"

    def _read_core_config(self, key: str):
        try:
            return self.ctx.config.get_config(key)
        except Exception:
            return None

    def _resolve_wake_keywords(self) -> tuple[list, str]:
        """按 Z版 → S版 → 官方 default-chat 顺序尝试读取已安装聊天插件的唤醒词。"""
        for id_candidates, key_candidates in WAKE_KEYWORD_SOURCES:
            for pid in id_candidates:
                cfg = self._get_other_plugin_cfg(pid)
                if not cfg:
                    continue
                words = self._find_keywords_in_cfg(cfg, key_candidates)
                if words:
                    return words, pid
        # 兜底：遍历已安装插件，找 id 以 chat 结尾且有唤醒词配置的
        try:
            mgr = self.ctx.plugin_mgr
            if mgr is not None:
                for info in mgr.list_plugins():
                    pid = getattr(info, "plugin_id", "") or ""
                    if pid == "midflight_message_plugin" or "chat" not in pid:
                        continue
                    cfg = self._get_other_plugin_cfg(pid)
                    words = self._find_keywords_in_cfg(
                        cfg, ("waking_words", "wake_keywords", "wake_words")) if cfg else []
                    if words:
                        return words, pid
        except Exception:
            pass
        return [], ""

    def _get_other_plugin_cfg(self, plugin_id: str) -> dict:
        """读取其他插件配置：优先实例 plugin_cfg，回退配置文件。全部失败返回 {}。"""
        try:
            inst = self.ctx.get_plugin_inst(plugin_id)
            if inst is not None:
                cfg = getattr(inst, "plugin_cfg", None)
                if isinstance(cfg, dict) and cfg:
                    return cfg
        except Exception:
            pass
        try:
            mgr = self.ctx.plugin_mgr
            if mgr is not None:
                cfg = mgr.get_plugin_config(plugin_id)
                if isinstance(cfg, dict):
                    return cfg
        except Exception:
            pass
        return {}

    def _find_keywords_in_cfg(self, cfg: dict, keys) -> list:
        """在插件配置中查找唤醒词列表（兼容平铺与 section 嵌套）。"""
        for key in keys:
            val = cfg.get(key)
            words = self._as_str_list(val)
            if words:
                return words
        # section 嵌套
        for val in cfg.values():
            if isinstance(val, dict):
                for key in keys:
                    words = self._as_str_list(val.get(key))
                    if words:
                        return words
        return []

    @staticmethod
    def _as_str_list(val) -> list:
        if isinstance(val, (list, tuple)):
            return [str(w) for w in val if str(w).strip()]
        return []

    def _gc(self):
        """定期清理过期的去重记录与计数。"""
        now = time.time()
        if now - self._last_gc < 60:
            return
        self._last_gc = now
        cutoff = now - DEDUP_TTL
        for sid in list(self._consumed.keys()):
            m = {k: v for k, v in self._consumed[sid].items() if v > cutoff}
            if m:
                self._consumed[sid] = m
            else:
                self._consumed.pop(sid, None)
        # 单轮注入计数：按最后更新时间淘汰（一轮正常远短于去重窗口）。
        # 绝不整表清空——那会把仍在执行中的轮次计数清零，导致
        # max_inject_per_run 失效、单轮超量注入；容量上限只淘汰最旧项兜底
        if self._run_inject_count:
            for k in [k for k, (_, ts) in self._run_inject_count.items() if ts <= cutoff]:
                self._run_inject_count.pop(k, None)
            while len(self._run_inject_count) > 200:
                oldest = min(self._run_inject_count,
                             key=lambda k: self._run_inject_count[k][1])
                self._run_inject_count.pop(oldest, None)
        if len(self._wait_steps) > 200:
            self._wait_steps.clear()
        # 看门狗放行标记：与去重表同 TTL（消息已还原/已处理，标记过期后自然失效）
        if self._bypass:
            self._bypass = {k: v for k, v in self._bypass.items() if v > cutoff}

    def _log_debug(self, msg: str):
        if self.debug:
            logger.info(f"[Midflight][debug] {msg}")
        else:
            logger.debug(f"[Midflight] {msg}")

    @staticmethod
    def _to_int(val, default: int) -> int:
        try:
            return int(val)
        except (TypeError, ValueError):
            return default