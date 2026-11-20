"""审计轨迹领域服务：记录CDC管道配置变更和事件处理的全链审计。

上游：所有服务模块都会写入审计日志到 pipeline_bus
消费：审计查询消费端通过本服务查询历史轨迹
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class AuditTrailService(BaseModuleService):
    MODULE = "audit_trail"
    ENTITY = "审计日志"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["审计时间", "操作类型", "目标对象", "变更摘要", "操作来源", "结果状态", "审计备注"]
    REQUIRED_FIELDS = ["审计时间", "操作类型", "目标对象"]
    STATUS_ORDER = ["已记录", "已复核", "异常标记"]
    ACTION_RULES = {"复核记录": "已记录", "标记异常": "已记录"}
    NEGATIVE_ACTIONS = []

    def query_audit_logs(self, from_time: str = "", to_time: str = "",
                         operation_type: str = "", source_module: str = "",
                         limit: int = 100) -> list[dict[str, Any]]:
        """查询审计日志。”

        支持按时间范围、操作类型、来源模块筛选。
        """
        result: list[dict[str, Any]] = []
        for entry in reversed(pipeline_bus.audit_log):
            entry_time = entry.get("审计时间", "")
            if from_time and entry_time < from_time:
                continue
            if to_time and entry_time > to_time:
                continue
            if operation_type and operation_type not in entry.get("操作类型", ""):
                continue
            if source_module and source_module != entry.get("操作来源", ""):
                continue
            result.append(dict(entry))
            if len(result) >= limit:
                break
        return result

    def audit_summary(self) -> dict[str, Any]:
        """审计日志汇总统计。"""
        logs = pipeline_bus.audit_log
        by_module: dict[str, int] = {}
        by_operation: dict[str, int] = {}
        by_status: dict[str, int] = {}
        for entry in logs:
            module = entry.get("操作来源", "unknown")
            op = entry.get("操作类型", "unknown")
            status = entry.get("结果状态", "unknown")
            by_module[module] = by_module.get(module, 0) + 1
            by_operation[op] = by_operation.get(op, 0) + 1
            by_status[status] = by_status.get(status, 0) + 1
        return {
            "total_entries": len(logs),
            "by_module": by_module,
            "by_operation": by_operation,
            "by_status": by_status,
            "last_entry_time": logs[-1]["审计时间"] if logs else "",
            "generated_at": datetime.now().isoformat(),
        }

    def find_anomalies(self, window_count: int = 500) -> list[dict[str, Any]]:
        """从审计日志中检测异常模式。"""
        recent = pipeline_bus.audit_log[-window_count:]
        anomalies = []
        error_keywords = ["失败", "异常", "错误", "failed", "error"]
        for entry in recent:
            summary = entry.get("变更摘要", "")
            status = entry.get("结果状态", "")
            if any(kw in summary for kw in error_keywords) or status == "异常标记":
                anomalies.append({
                    "entry": entry,
                    "risk": "high" if "失败" in summary or "严重" in summary else "medium",
                    "detected_at": datetime.now().isoformat(),
                })
        return anomalies

    def lag_anomaly_summary(self) -> dict[str, Any]:
        """异常汇总：消费延迟触发记录，按实际触发的通道和阈值版本生成值班处置告警记录。

        同一通道同一采样位置只计一次，重复写入不放大异常计数。
        """
        alerts: dict[tuple[str, str], dict[str, Any]] = {}
        duplicates = 0
        for trigger in pipeline_bus.lag_triggers:
            if not trigger.get("alerted"):
                continue
            key = (str(trigger.get("channel", "")), str(trigger.get("sample_pos", "")))
            if key in alerts:
                duplicates += 1
                continue
            alerts[key] = {
                "alert_key": f"{trigger.get('channel')}#{trigger.get('sample_pos')}",
                "channel": trigger.get("channel"),
                "sample_pos": trigger.get("sample_pos"),
                "lag_seconds": trigger.get("lag_seconds"),
                "threshold_sec": trigger.get("threshold_sec"),
                "threshold_version": trigger.get("threshold_version"),
                "status": trigger.get("status"),
                "sampled_at": trigger.get("sampled_at"),
                "received_at": trigger.get("received_at"),
                "sample_threshold_sec": trigger.get("sample_threshold_sec"),
                "sample_threshold_version": trigger.get("sample_threshold_version"),
                "receive_threshold_sec": trigger.get("receive_threshold_sec"),
                "receive_threshold_version": trigger.get("receive_threshold_version"),
                "threshold_handoff": trigger.get("threshold_handoff"),
                "handler": "值班处置",
            }
        items = list(alerts.values())
        by_channel: dict[str, int] = {}
        by_version: dict[str, int] = {}
        for item in items:
            channel = str(item["channel"])
            by_channel[channel] = by_channel.get(channel, 0) + 1
            version_key = f"{channel}@v{item['threshold_version']}"
            by_version[version_key] = by_version.get(version_key, 0) + 1
        return {
            "alert_total": len(items),
            "channels": sorted(by_channel),
            "by_channel": by_channel,
            "by_threshold_version": by_version,
            "duplicates_suppressed": duplicates,
            "alerts": items,
            "generated_at": datetime.now().isoformat(),
        }

    def find_lag_alerts(self, channel: str = "", threshold_sec: float | None = None,
                        threshold_version: int | None = None) -> list[dict[str, Any]]:
        """回查已发出的延迟告警：按当时采用的阈值或阈值版本筛选。"""
        result: list[dict[str, Any]] = []
        for alert in self.lag_anomaly_summary()["alerts"]:
            if channel and alert["channel"] != channel:
                continue
            if threshold_sec is not None and float(alert["threshold_sec"]) != float(threshold_sec):
                continue
            if threshold_version is not None and int(alert["threshold_version"]) != int(threshold_version):
                continue
            result.append(alert)
        return result

    def compliance_check(self) -> dict[str, Any]:
        """合规性检查。"""
        logs = pipeline_bus.audit_log
        total = len(logs)
        required_ops = ["DDL分析", "Schema同步", "通道启动", "路由分发"]
        op_coverage: dict[str, bool] = {}
        for op in required_ops:
            op_coverage[op] = any(op in entry.get("操作类型", "") for entry in logs)
        all_covered = all(op_coverage.values())
        return {
            "total_audit_entries": total,
            "required_operation_coverage": op_coverage,
            "all_required_covered": all_covered,
            "compliance_status": "合规" if all_covered else "部分覆盖",
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "复核记录":
            pipeline_bus.write_audit(
                "审计复核", entry.get("目标对象", ""),
                f"审计记录 {entry_id} 已复核",
                "audit_trail", "已复核"
            )
        return entry, msg