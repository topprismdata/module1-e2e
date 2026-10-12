"""E2E 测试：五服务串联 + G1/G2 门禁不变量。"""
from __future__ import annotations

import pytest

from assignment_service import AssignmentService
from dq_workbench import Layer, Workbench
from geocode_arbiter.providers.offline import ScriptedProvider
from identity_service import Domain, IdentityService, SourceRecord
from module1_e2e.pipeline import GateState, OutletIntake, Pipeline
from service_rules import Rule, RuleSetVersion, RuleType, Severity

TIANHE = "440106"
POS_A = (113.2644, 23.1291)


def _intake(oid="O1", lon=113.2644, lat=23.1291):
    return OutletIntake(
        outlet_id=oid, address="广州市天河区测试路1号",
        filled_adcode=TIANHE, filled_lon=lon, filled_lat=lat,
        poi_confidence=0.9, official_adcode=TIANHE)


def _ruleset():
    return RuleSetVersion("v1", (
        Rule("R-CAD", RuleType.CADENCE_SLOTS, Severity.C1, True,
             weekday_slots={1: [0, 1], 3: [0]}),
        Rule("R-FREQ", RuleType.VISIT_FREQUENCY, Severity.C2, False, visits_per_week=3),
    ))


@pytest.fixture()
def env(tmp_path):
    identity = IdentityService(tmp_path / "id.db")
    assignment = AssignmentService(tmp_path / "asg.db")
    workbench = Workbench(tmp_path / "wb.db")
    pipe = Pipeline(
        geo_primary=ScriptedProvider("tencent", {POS_A: TIANHE}),
        geo_secondary=ScriptedProvider("amap", {POS_A: TIANHE}),
        ruleset=_ruleset(),
        identity=identity,
        assignment=assignment,
        workbench=workbench,
    )
    return pipe, identity, assignment, workbench


def test_clean_outlet_reaches_planning(env):
    pipe, _, assignment, _ = env
    states = pipe.process([_intake("O1")], {"O1": "repA"})
    st = states["O1"]
    assert st.planning_eligible
    assert assignment.active_for_outlet("O1")[0].rep_id == "repA"
    assert Pipeline.planning_eligible(states) == ["O1"]


def test_g1_spatial_blocked_never_reaches_planning(env):
    pipe, _, assignment, workbench = env
    # 腾讯反解 ≠ 填报且高德也≠ → 不 CONFIRMED → 空间门拦截
    pipe._geo_primary = ScriptedProvider("tencent", {POS_A: "440112"})
    pipe._geo_secondary = ScriptedProvider("amap", {POS_A: "440112"})
    states = pipe.process([_intake("O1")], {"O1": "repA"})
    st = states["O1"]
    assert not st.planning_eligible and "SPATIAL_GATE" in st.hold_reasons
    # G2: 拦截有工单审计（findings 落 workbench）
    assert wb_has_finding(workbench, "geo verdict")
    # 门禁外：无主访关系（G1）
    assert assignment.active_for_outlet("O1") == []


def test_g1_identity_hold_suspends(env):
    pipe, identity, assignment, workbench = env
    # 预置同源重复候选：O1 的两条源记录进 identity 队列
    identity.add_record(SourceRecord("O1", "crm", "C1", Domain.SERVICE_LOCATION,
                                     name="永辉天河店", lon=POS_A[0], lat=POS_A[1],
                                     adcode=TIANHE, brand="yonghui"))
    identity.add_record(SourceRecord("O1dup", "crm", "C2", Domain.SERVICE_LOCATION,
                                     name="永辉天河店", lon=POS_A[0] + 0.0001,
                                     lat=POS_A[1] + 0.0001, adcode=TIANHE, brand="yonghui"))
    identity.run_matching()
    states = pipe.process([_intake("O1")], {"O1": "repA"})
    st = states["O1"]
    assert not st.planning_eligible and "IDENTITY_GATE" in st.hold_reasons
    assert wb_has_finding(workbench, "未决重复候选")
    assert assignment.active_for_outlet("O1") == []


def test_rules_block_suspends(env):
    pipe, _, assignment, workbench = env
    pipe._ruleset = RuleSetVersion("v1", (
        Rule("R-CAD", RuleType.CADENCE_SLOTS, Severity.C1, True,
             weekday_slots={3: [0]}),  # 周一无槽位 → fail-closed BLOCK
    ))
    states = pipe.process([_intake("O1")], {"O1": "repA"}, weekday=1)
    st = states["O1"]
    assert not st.planning_eligible and "RULES_GATE" in st.hold_reasons
    assert wb_has_finding(workbench, "rules verdict=BLOCK")


def test_g3_planning_view_is_readonly_snapshot(env):
    pipe, _, _, _ = env
    states = pipe.process([_intake("O1")], {"O1": "repA"})
    eligible = Pipeline.planning_eligible(states)
    # 快照函数纯读：重复调用结果一致，不改变状态
    assert Pipeline.planning_eligible(states) == eligible
    assert all(isinstance(states[k], GateState) for k in states)


def wb_has_finding(wb: Workbench, needle: str) -> bool:
    rows = wb.db.execute("SELECT doc FROM findings").fetchall()
    return any(needle in r[0] for r in rows)
