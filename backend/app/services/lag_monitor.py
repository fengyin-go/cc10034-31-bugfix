"""延迟监控领域服务：监控CDC通道的同步延迟和吞吐量。

上游：cdc_channel 产生通道状态 → lag_monitor 监控
消费：snapshot_compare 根据延迟情况触发快照比对；
     audit_trail 的异常汇总消费延迟触发记录，生成值班处置告警记录。

延迟判定约定：
- 每条延迟触发记录只使用一份已生效阈值：采样时刻生效的通道阈值版本；
- 采样与接收之间发生阈值交接时，记录同时保留两侧阈值版本供值班核对；
- 同一通道同一采样位置只登记一次，重复提交不重复登记、不放大异常计数；
- 延迟秒数等于阈值时保持既有判定语义（>= 即告警），严重延迟为阈值的 5 倍。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.pipeline import pipeline_bus
from app.services.base import BaseModuleService
from app.store import store


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def _normalize_time(value: Any) -> str:
    """把时间输入规整为 ISO 字符串；无法解析时返回空串，由调用方回退。"""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text).isoformat()
    except ValueError:
        return ""


class LagMonitorService(BaseModuleService):
    MODULE = "lag_monitor"
    ENTITY = "延迟指标"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["指标时间", "所属通道", "延迟秒数", "积压事件数", "吞吐量", "源位点", "目标位点"]
    REQUIRED_FIELDS = ["指标时间", "所属通道", "延迟秒数"]
    STATUS_ORDER = ["正常", "延迟告警", "严重延迟"]
    ACTION_RULES = {"确认告警": "延迟告警", "标记恢复": "严重延迟"}
    NEGATIVE_ACTIONS = []

    SEVERE_MULTIPLIER = 5  # 严重延迟 = 生效阈值 × 5，保持 60s→300s 的既有语义

    # ---- 阈值版本管理 ----

    def set_channel_threshold(self, channel_name: str, threshold_sec: float,
                              effective_at: str | None = None) -> dict[str, Any]:
        """登记通道延迟阈值的新版本，返回版本记录。"""
        version = pipeline_bus.set_threshold_version(channel_name, threshold_sec, effective_at)
        pipeline_bus.write_audit(
            "阈值调整", channel_name,
            f"通道 {channel_name} 延迟阈值调整为 {float(threshold_sec):g}s "
            f"(v{version['version']}，生效于 {version['effective_at']})",
            "lag_monitor", "已记录",
        )
        return version

    def threshold_for(self, channel_name: str, at: str | None = None) -> dict[str, Any]:
        """查询通道在指定时刻生效的阈值版本。"""
        return pipeline_bus.threshold_at(channel_name, at)

    # ---- 指标采集 ----

    def _estimate_lag(self) -> tuple[float, int, str, str]:
        """按管道运行统计估算当前延迟（无副作用）。"""
        parse_total = pipeline_bus.parse_stats["total_parsed"]
        output_total = pipeline_bus.pipeline_metrics["total_output"]
        lag_events = max(parse_total - output_total, 0)
        lag_seconds = max(lag_events / 100.0, 0.5)  # 估算延迟
        ts = int(datetime.now().timestamp() % 10000)
        return (
            lag_seconds,
            lag_events,
            f"mysql-bin.{ts:06d}:{parse_total * 100}",
            f"mysql-bin.{ts:06d}:{output_total * 100}",
        )

    def collect_lag_metrics(self, channel_name: str) -> dict[str, Any]:
        """采集通道延迟指标（估算口径），缓冲并写采集审计。"""
        lag_seconds, lag_events, source_pos_str, target_pos_str = self._estimate_lag()
        metric = pipeline_bus.compute_lag(
            channel_name, lag_seconds, lag_events, source_pos_str, target_pos_str)
        pipeline_bus.write_audit(
            "延迟采集", channel_name,
            f"通道 {channel_name} 延迟 {metric['延迟秒数']}s 积压 {lag_events} 事件",
            "lag_monitor", "已记录",
        )
        return metric

    # ---- 阈值评估 ----

    def evaluate_lag_threshold(self, metric: dict[str, Any],
                               threshold_sec: float | None = None) -> dict[str, Any]:
        """按实际生效阈值评估延迟指标。

        threshold_sec 缺省时取通道当前生效阈值；等于阈值按既有语义（>=）判定为告警。
        """
        lag_sec = _to_float(metric.get("延迟秒数"))
        backlog = _to_int(metric.get("积压事件数"))
        if threshold_sec is None:
            threshold_sec = pipeline_bus.threshold_at(
                str(metric.get("所属通道", "")))["threshold_sec"]
        threshold_sec = float(threshold_sec)
        severe_sec = threshold_sec * self.SEVERE_MULTIPLIER
        status = "正常"
        if lag_sec >= severe_sec:
            status = "严重延迟"
        elif lag_sec >= threshold_sec:
            status = "延迟告警"
        severity = 0 if status == "正常" else (1 if status == "延迟告警" else 2)
        return {
            "channel": metric.get("所属通道"),
            "lag_sec": lag_sec,
            "backlog": backlog,
            "threshold_sec": threshold_sec,
            "severe_threshold_sec": severe_sec,
            "status": status,
            "severity_level": severity,
            "threshold_breached": severity > 0,
        }

    # ---- 样本提交：形成延迟触发记录 ----

    def submit_lag_sample(self, channel_name: str, lag_seconds: Any, *,
                          sampled_at: Any = None, sample_pos: str | None = None,
                          backlog_events: Any = 0, source_pos: str = "",
                          target_pos: str = "", throughput: Any = None,
                          threshold_override: float | None = None) -> dict[str, Any]:
        """提交延迟样本，按采样时生效阈值形成延迟触发记录。

        同一通道同一采样位置只登记一次；重复提交返回首次记录，不重复告警。
        """
        received = datetime.now()
        sampled_iso = _normalize_time(sampled_at) or received.isoformat()
        pos = str(sample_pos or source_pos or f"{channel_name}@{sampled_iso}")

        existing = pipeline_bus.find_lag_trigger(channel_name, pos)
        if existing is not None:
            return {
                "metric": None,
                "evaluation": None,
                "trigger": existing,
                "entry": store.find(self.MODULE, int(existing.get("entry_id", -1))),
                "alert_triggered": bool(existing.get("alerted")),
                "duplicate": True,
            }

        sample_ver = pipeline_bus.threshold_at(channel_name, sampled_iso)
        receive_ver = pipeline_bus.threshold_at(channel_name, received.isoformat())
        if threshold_override is not None:
            effective_sec = float(threshold_override)
            threshold_source = "手动指定"
        else:
            # 每条记录只使用一份已生效阈值：采样时刻生效的版本
            effective_sec = float(sample_ver["threshold_sec"])
            threshold_source = "通道配置"

        metric = pipeline_bus.compute_lag(
            channel_name, _to_float(lag_seconds), _to_int(backlog_events),
            source_pos or pos, target_pos,
        )
        metric["指标时间"] = sampled_iso
        if throughput not in (None, ""):
            metric["吞吐量"] = str(throughput)

        evaluation = self.evaluate_lag_threshold(
            {"所属通道": channel_name, "延迟秒数": metric["延迟秒数"],
             "积压事件数": metric["积压事件数"]},
            threshold_sec=effective_sec,
        )

        trigger = {
            "channel": channel_name,
            "sample_pos": pos,
            "lag_seconds": metric["延迟秒数"],
            "sampled_at": sampled_iso,
            "received_at": received.isoformat(),
            "threshold_sec": effective_sec,
            "threshold_version": sample_ver["version"],
            "threshold_source": threshold_source,
            "sample_threshold_sec": float(sample_ver["threshold_sec"]),
            "sample_threshold_version": sample_ver["version"],
            "receive_threshold_sec": float(receive_ver["threshold_sec"]),
            "receive_threshold_version": receive_ver["version"],
            "threshold_handoff": sample_ver["version"] != receive_ver["version"],
            "status": evaluation["status"],
            "alerted": evaluation["threshold_breached"],
            "backlog_events": metric["积压事件数"],
            "source_pos": metric["源位点"],
            "target_pos": metric["目标位点"],
        }
        trigger, is_new = pipeline_bus.record_lag_trigger(trigger)
        if is_new:
            entry = self._register_metric_entry(metric, evaluation)
            trigger["entry_id"] = entry["id"]
            self._audit_trigger(trigger)
        else:
            entry = store.find(self.MODULE, int(trigger.get("entry_id", -1)))
        return {
            "metric": metric,
            "evaluation": evaluation,
            "trigger": trigger,
            "entry": entry,
            "alert_triggered": bool(trigger.get("alerted")),
            "duplicate": not is_new,
        }

    def _register_metric_entry(self, metric: dict[str, Any],
                               evaluation: dict[str, Any]) -> dict[str, Any]:
        """把评估后的延迟指标登记到模块列表，状态与阈值校验结果一致。"""
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: metric.get(field) for field in self.LIST_FIELDS})
        entry["status"] = evaluation["status"]
        entry["pending"] = evaluation["threshold_breached"]
        entry["abnormal"] = evaluation["threshold_breached"]
        rows.append(entry)
        return entry

    def _audit_trigger(self, trigger: dict[str, Any]) -> None:
        """按触发结果写审计：告警写异常告警记录，正常写采集记录。"""
        handoff = (
            f"采样时阈值 v{trigger['sample_threshold_version']}"
            f"={trigger['sample_threshold_sec']:g}s，"
            f"接收时阈值 v{trigger['receive_threshold_version']}"
            f"={trigger['receive_threshold_sec']:g}s"
        )
        if trigger["alerted"]:
            pipeline_bus.write_audit(
                "延迟告警", trigger["channel"],
                f"延迟异常告警：通道 {trigger['channel']} 延迟 {trigger['lag_seconds']}s "
                f"达到生效阈值 {trigger['threshold_sec']:g}s"
                f"(v{trigger['threshold_version']})，采样位置 {trigger['sample_pos']}",
                "lag_monitor", "已告警", remark=handoff,
            )
        else:
            pipeline_bus.write_audit(
                "延迟采集", trigger["channel"],
                f"通道 {trigger['channel']} 延迟 {trigger['lag_seconds']}s "
                f"低于生效阈值 {trigger['threshold_sec']:g}s(v{trigger['threshold_version']})",
                "lag_monitor", "已记录", remark=handoff,
            )

    # ---- 通用登记入口：路由到样本提交，保证页面登记也按阈值校验 ----

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        result = self.submit_lag_sample(
            str(values.get("所属通道") or "").strip(),
            _to_float(values.get("延迟秒数")),
            sampled_at=str(values.get("指标时间") or "").strip() or None,
            sample_pos=str(values.get("源位点") or "").strip() or None,
            backlog_events=_to_int(values.get("积压事件数")),
            source_pos=str(values.get("源位点") or "").strip(),
            target_pos=str(values.get("目标位点") or "").strip(),
            throughput=str(values.get("吞吐量") or "").strip() or None,
        )
        return result["entry"], []

    # ---- 趋势与按需告警 ----

    def compute_lag_trend(self, buffer_count: int = 20) -> dict[str, Any]:
        """计算延迟趋势。"""
        recent = pipeline_bus.lag_buffer[-buffer_count:]
        if len(recent) < 2:
            return {"trend": "insufficient_data", "count": len(recent)}
        first_lag = float(recent[0].get("延迟秒数", 0))
        last_lag = float(recent[-1].get("延迟秒数", 0))
        diff = last_lag - first_lag
        trend = "稳定"
        if diff > 10:
            trend = "恶化"
        elif diff < -10:
            trend = "改善"
        return {
            "window_count": len(recent),
            "first_lag": first_lag,
            "last_lag": last_lag,
            "delta": round(diff, 1),
            "trend": trend,
            "avg_lag": round(sum(float(m.get("延迟秒数", 0)) for m in recent) / len(recent), 1),
        }

    def alert_if_needed(self, channel_name: str,
                        threshold_sec: float | None = None) -> dict[str, Any]:
        """按需触发告警：采集当前估算指标，并按传入阈值（缺省取通道生效阈值）校验。"""
        lag_seconds, lag_events, source_pos_str, target_pos_str = self._estimate_lag()
        return self.submit_lag_sample(
            channel_name, lag_seconds,
            sample_pos=source_pos_str,
            backlog_events=lag_events,
            source_pos=source_pos_str,
            target_pos=target_pos_str,
            threshold_override=threshold_sec,
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "确认告警":
            pipeline_bus.write_audit(
                "延迟确认", entry.get("所属通道", ""),
                f"延迟告警 {entry_id} 已确认",
                "lag_monitor", "已确认"
            )
        return entry, msg
