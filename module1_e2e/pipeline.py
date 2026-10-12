"""E2E 管线：五服务串联。

流出顺序：
  1 geocode-arbiter  双源仲裁 → CONFIRMED 才过空间门
  2 identity-service 同源查重 → 有未决重复候选即挂起（人工队列）
  3 assignment-service 门内店才可建立主访关系
  4 service-rules    规则门（合同相位/时间窗）
  5 dq-workbench     全部异常落工单+审计

验收不变量（E2E 测试锁定）：
  G1 未过门禁（空间 BLOCK/身份挂起/规则 BLOCK）的数据不可进入规划：
     planning_eligible() 只返回全门通过的店
  G2 门禁外的每次拦截都有工单与审计事件（可追溯）
  G3 规划侧对门禁结果的读取是只读快照（无回写路径）
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from geocode_arbiter import Record as GeoRecord, Verdict as GeoVerdict, arbitrate
from geocode_arbiter.providers.base import Provider as GeoProvider

from identity_service import Domain as IdDomain, IdentityService, SourceRecord
from assignment_service import AssignmentService, Role
from service_rules import (
    AdmissionAction,
    RuleSetVersion,
    aggregate,
    evaluate_outlet_day,
)
from dq_workbench import Finding, IssueStatus, Layer, Severity, Workbench


@dataclass
class OutletIntake:
    """规划输入的门店记录（业务包 GCJ-02 标准帧）。"""

    outlet_id: str
    address: str
    filled_adcode: str
    filled_lon: float
    filled_lat: float
    poi_confidence: float = 0.9
    official_adcode: str = ""


@dataclass
class GateState:
    """单店门禁状态（只读快照语义）。"""

    outlet_id: str
    spatial_pass: bool = False
    identity_pass: bool = False
    rules_pass: bool = False
    assigned_rep: str = ""
    hold_reasons: list[str] = field(default_factory=list)

    @property
    def planning_eligible(self) -> bool:
        """G1：全门通过才可进规划。"""
        return (self.spatial_pass and self.identity_pass
                and self.rules_pass and bool(self.assigned_rep))


class Pipeline:
    def __init__(
        self,
        geo_primary: GeoProvider,
        geo_secondary: GeoProvider,
        ruleset: RuleSetVersion,
        *,
        identity: IdentityService | None = None,
        assignment: AssignmentService | None = None,
        workbench: Workbench | None = None,
    ):
        self.identity = identity
        self.assignment = assignment
        self.workbench = workbench
        self._geo_primary = geo_primary
        self._geo_secondary = geo_secondary
        self._ruleset = ruleset

    # ---- 门 1：空间 ----
    def spatial_gate(self, intake: OutletIntake) -> bool:
        d = arbitrate(
            GeoRecord(
                outlet_id=intake.outlet_id,
                filled_adcode=intake.filled_adcode,
                filled_lon=intake.filled_lon,
                filled_lat=intake.filled_lat,
                address=intake.address,
                poi_confidence=intake.poi_confidence,
                official_adcode=intake.official_adcode,
            ),
            self._geo_primary,
            self._geo_secondary,
        )
        ok = d.verdict is GeoVerdict.CONFIRMED
        if not ok and self.workbench:
            self.workbench.ingest([Finding(
                finding_id=f"f_geo_{intake.outlet_id}_{d.created_at}",
                layer=Layer.SPATIAL,
                rule_id="R018/R019/R031",
                subject=intake.outlet_id,
                severity=Severity.C1,
                message=f"geo verdict={d.verdict.value}",
                evidence=[json.dumps(d.to_json(), ensure_ascii=False)],
            )])
        return ok

    # ---- 门 2：身份 ----
    def identity_gate(self, intake: OutletIntake) -> bool:
        if self.identity is None:
            return True
        # 同源重复即挂起：identity-service 的 open queue 有该店 → 不放行
        for item, _meta in self.identity.open_queue():
            cand = item.candidate
            if intake.outlet_id in (cand.left, cand.right):
                if self.workbench:
                    self.workbench.ingest([Finding(
                        finding_id=f"f_id_{intake.outlet_id}",
                        layer=Layer.IDENTITY,
                        rule_id="R023",
                        subject=intake.outlet_id,
                        severity=Severity.C2,
                        message="存在未决重复候选（人工队列）",
                        evidence=[cand.candidate_id],
                    )])
                return False
        return True

    # ---- 门 3：规则 ----
    def rules_gate(self, outlet_id: str, weekday: int, slot: int, visit_time: str) -> bool:
        evals = evaluate_outlet_day(self._ruleset, outlet_id, weekday, slot, visit_time)
        d = aggregate(evals)
        ok = d.action is AdmissionAction.ALLOW
        if not ok and self.workbench:
            self.workbench.ingest([Finding(
                finding_id=f"f_rules_{outlet_id}_{weekday}_{slot}",
                layer=Layer.STATISTICAL,
                rule_id="RULES_GATE",
                subject=outlet_id,
                severity=Severity.C1 if not d.hard_gates_all_pass else Severity.C2,
                message=f"rules verdict={d.action.value}: {d.rationale}",
            )])
        return ok

    # ---- 全流程 ----
    def process(
        self,
        intakes: list[OutletIntake],
        rep_by_outlet: dict[str, str],
        weekday: int = 1,
        slot: int = 0,
        visit_time: str = "10:00",
        valid_from: str = "2026-01-01",
        decided_by: str = "e2e",
    ) -> dict[str, GateState]:
        states: dict[str, GateState] = {}
        for it in intakes:
            st = GateState(outlet_id=it.outlet_id)
            st.spatial_pass = self.spatial_gate(it)
            if not st.spatial_pass:
                st.hold_reasons.append("SPATIAL_GATE")
                states[it.outlet_id] = st
                continue  # 门禁外：不建身份/关系/规划（G1）
            st.identity_pass = self.identity_gate(it)
            if not st.identity_pass:
                st.hold_reasons.append("IDENTITY_GATE")
                states[it.outlet_id] = st
                continue
            st.rules_pass = self.rules_gate(it.outlet_id, weekday, slot, visit_time)
            if not st.rules_pass:
                st.hold_reasons.append("RULES_GATE")
                states[it.outlet_id] = st
                continue
            # 门内：建主访关系
            rep = rep_by_outlet.get(it.outlet_id)
            if rep and self.assignment:
                try:
                    self.assignment.assign(
                        it.outlet_id, rep, Role.PRIMARY_SELLING, valid_from,
                        decided_by=decided_by,
                        source_approved_decision_id="e2e-pipeline",
                    )
                    st.assigned_rep = rep
                except Exception as exc:  # 冲突等 → 挂起留痕
                    st.hold_reasons.append(f"ASSIGNMENT:{exc}")
                    if self.workbench:
                        self.workbench.ingest([Finding(
                            finding_id=f"f_asg_{it.outlet_id}",
                            layer=Layer.IDENTITY,
                            rule_id="ASSIGNMENT",
                            subject=it.outlet_id,
                            severity=Severity.C2,
                            message=str(exc),
                        )])
            states[it.outlet_id] = st
        if self.workbench:
            self.workbench.ingest([Finding(
                finding_id="f_run_summary",
                layer=Layer.STATISTICAL,
                rule_id="PIPELINE_RUN",
                subject="*",
                severity=Severity.C3,
                message=f"processed={len(intakes)} eligible={sum(1 for s in states.values() if s.planning_eligible)}",
            )])
        return states

    # ---- G1 规划入口：只读快照 ----
    @staticmethod
    def planning_eligible(states: dict[str, GateState]) -> list[str]:
        return sorted(s.outlet_id for s in states.values() if s.planning_eligible)
