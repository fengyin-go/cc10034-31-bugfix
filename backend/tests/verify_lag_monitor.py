"""延迟监控阈值修复验证脚本。

复现并核对问题单场景：
1. orders-low 延迟 51s 对通道阈值 40s  —— 应触发延迟告警并写审计；
2. orders-high 延迟 61s 对通道阈值 90s —— 应为正常，不产生告警；
3. orders_ingest 采到 60s 样本后网络中断，样本到达前阈值 40s→90s，
   补交后按采样时生效阈值（40s）判定，记录可同时核对采样/接收两侧设置；
4. 同一通道同一采样位置重复提交只登记一次，异常计数不放大；
5. 审计异常汇总按实际触发的通道和版本生成值班处置告警记录；
6. 已发出的告警可按当时采用的阈值回查；
7. 等于阈值的边界保持既有判定语义（>= 告警，5 倍严重延迟）。

运行：cd backend && .venv/bin/python tests/verify_lag_monitor.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.pipeline import pipeline_bus
from app.services.audit_trail import AuditTrailService
from app.services.lag_monitor import LagMonitorService
from app.store import store

lag = LagMonitorService()
audit = AuditTrailService()

FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def audit_entries(op: str, target: str) -> list[dict]:
    return [e for e in pipeline_bus.audit_log
            if e.get("操作类型") == op and e.get("目标对象") == target]


# ---------------------------------------------------------------- 阈值设置
lag.set_channel_threshold("orders-low", 40, effective_at="2026-11-20T09:00:00")
lag.set_channel_threshold("orders-high", 90, effective_at="2026-11-20T09:00:00")
lag.set_channel_threshold("orders_ingest", 40, effective_at="2026-11-20T09:00:00")

# -------------------------------------------------- 场景1 orders-low 51s/40s
r1 = lag.submit_lag_sample(
    "orders-low", 51, sampled_at="2026-11-20T10:00:00",
    sample_pos="mysql-bin.000101:4000", backlog_events=120)
check("orders-low 51s vs 阈值40s 触发延迟告警",
      r1["alert_triggered"] and r1["trigger"]["status"] == "延迟告警",
      f"status={r1['trigger']['status']}")
check("orders-low 触发记录采用阈值 40s",
      r1["trigger"]["threshold_sec"] == 40.0 and r1["trigger"]["threshold_version"] == 1)
check("orders-low 列表指标状态为延迟告警",
      r1["entry"]["status"] == "延迟告警" and r1["entry"]["abnormal"] is True)
check("orders-low 告警审计已写入（不再丢失应报记录）",
      len(audit_entries("延迟告警", "orders-low")) == 1,
      f"延迟告警条数={len(audit_entries('延迟告警', 'orders-low'))}")

# -------------------------------------------------- 场景2 orders-high 61s/90s
r2 = lag.submit_lag_sample(
    "orders-high", 61, sampled_at="2026-11-20T10:01:00",
    sample_pos="mysql-bin.000102:5000", backlog_events=80)
check("orders-high 61s vs 阈值90s 判定正常（不再误报）",
      not r2["alert_triggered"] and r2["trigger"]["status"] == "正常",
      f"status={r2['trigger']['status']}")
check("orders-high 触发记录采用阈值 90s",
      r2["trigger"]["threshold_sec"] == 90.0)
check("orders-high 列表指标状态为正常",
      r2["entry"]["status"] == "正常" and r2["entry"]["abnormal"] is False)
check("orders-high 无告警审计",
      len(audit_entries("延迟告警", "orders-high")) == 0)

# --------------------------- 场景3 orders_ingest 阈值交接（采样40s → 接收90s）
# 样本在 10:04:30 采到（阈值 v1=40s 生效中），网络中断；
# 10:05:00 阈值调整为 v2=90s；随后补交的 60s 样本到达。
lag.set_channel_threshold("orders_ingest", 90, effective_at="2026-11-20T10:05:00")
r3 = lag.submit_lag_sample(
    "orders_ingest", 60, sampled_at="2026-11-20T10:04:30",
    sample_pos="mysql-bin.000103:6600", backlog_events=200)
t3 = r3["trigger"]
check("orders_ingest 60s 样本按采样时阈值 40s 判定告警",
      r3["alert_triggered"] and t3["status"] == "延迟告警")
check("orders_ingest 记录只采用一份已生效阈值（采样时 v1=40s）",
      t3["threshold_sec"] == 40.0 and t3["threshold_version"] == 1)
check("orders_ingest 记录可核对采样/接收两侧设置",
      t3["sample_threshold_sec"] == 40.0 and t3["receive_threshold_sec"] == 90.0
      and t3["sample_threshold_version"] == 1 and t3["receive_threshold_version"] == 2)
check("orders_ingest 记录标记了阈值交接",
      t3["threshold_handoff"] is True)

# -------------------------------------------------- 场景4 重复提交去重
triggers_before = len(pipeline_bus.lag_triggers)
audit_before = len(pipeline_bus.audit_log)
rows_before = len(store.rows("lag_monitor"))
r3_dup = lag.submit_lag_sample(
    "orders_ingest", 60, sampled_at="2026-11-20T10:04:30",
    sample_pos="mysql-bin.000103:6600", backlog_events=200)
check("重复补交同一采样位置被去重",
      r3_dup["duplicate"] is True
      and r3_dup["trigger"]["trigger_id"] == t3["trigger_id"])
check("重复提交不新增触发记录/审计/列表登记",
      len(pipeline_bus.lag_triggers) == triggers_before
      and len(pipeline_bus.audit_log) == audit_before
      and len(store.rows("lag_monitor")) == rows_before)

# -------------------------------------------------- 场景5 审计异常汇总消费
summary = audit.lag_anomaly_summary()
check("异常汇总只处理实际触发的通道 orders-low 与 orders_ingest",
      summary["channels"] == ["orders-low", "orders_ingest"],
      f"channels={summary['channels']}")
check("异常汇总告警数为 2（orders-high 误报已消除）",
      summary["alert_total"] == 2)
check("异常汇总按通道和阈值版本生成值班处置记录",
      summary["by_threshold_version"] == {"orders-low@v1": 1, "orders_ingest@v1": 1}
      and all(a["handler"] == "值班处置" for a in summary["alerts"]),
      f"by_threshold_version={summary['by_threshold_version']}")
summary_again = audit.lag_anomaly_summary()
check("重复生成汇总不放大异常计数",
      summary_again["alert_total"] == 2 and summary_again["duplicates_suppressed"] == 0)

# -------------------------------------------------- 场景6 按当时采用阈值回查
check("按原阈值 40s 回查到 2 条已发告警",
      {a["channel"] for a in audit.find_lag_alerts(threshold_sec=40)}
      == {"orders-low", "orders_ingest"})
check("按阈值 90s 回查无告警（61s 未达 90s）",
      audit.find_lag_alerts(threshold_sec=90) == [])
check("按通道+版本回查 orders_ingest@v1 有 1 条",
      len(audit.find_lag_alerts(channel="orders_ingest", threshold_version=1)) == 1)

# -------------------------------------------------- 场景7 边界语义（独立通道）
lag.set_channel_threshold("boundary-chan", 40, effective_at="2026-11-20T09:00:00")
eq = lag.submit_lag_sample("boundary-chan", 40, sampled_at="2026-11-20T10:00:00",
                           sample_pos="mysql-bin.000201:100")
check("等于阈值 40s 仍判定告警（保持 >= 语义）",
      eq["trigger"]["status"] == "延迟告警")
under = lag.submit_lag_sample("boundary-chan", 39.9, sampled_at="2026-11-20T10:00:30",
                              sample_pos="mysql-bin.000201:200")
check("低于阈值 39.9s 判定正常", under["trigger"]["status"] == "正常")
severe = lag.submit_lag_sample("boundary-chan", 200, sampled_at="2026-11-20T10:01:00",
                               sample_pos="mysql-bin.000201:300")
check("达到阈值 5 倍（200s）判定严重延迟", severe["trigger"]["status"] == "严重延迟")

# -------------------------------------------------- 场景8 传入阈值不再被忽略
ev_low = lag.evaluate_lag_threshold(
    {"所属通道": "orders-low", "延迟秒数": 51, "积压事件数": 0}, threshold_sec=40)
ev_high = lag.evaluate_lag_threshold(
    {"所属通道": "orders-high", "延迟秒数": 61, "积压事件数": 0}, threshold_sec=90)
check("评估器使用传入阈值：51s vs 40s → 延迟告警", ev_low["status"] == "延迟告警")
check("评估器使用传入阈值：61s vs 90s → 正常", ev_high["status"] == "正常")
probe = lag.alert_if_needed("probe-chan", threshold_sec=0.4)
check("alert_if_needed 不再忽略传入秒数（估算0.5s vs 0.4s → 告警）",
      probe["alert_triggered"] is True and probe["evaluation"]["threshold_sec"] == 0.4)

print()
if FAILURES:
    print(f"共 {len(FAILURES)} 项未通过：")
    for name in FAILURES:
        print(f"  - {name}")
    sys.exit(1)
print("全部核对通过。")
