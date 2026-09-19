"""Risk-scoring combination logic — the part that must never be wrong."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services/risk-scoring"))

from main import combine, to_decision

from libs.common.schemas import Decision


def sig(source, score, reasons=None):
    return {"source": source, "score": score, "reasons": reasons or []}


def test_all_three_signals_blend_by_weight():
    score, missing = combine({
        "RULES": sig("RULES", 40), "ML": sig("ML", 20), "GRAPH": sig("GRAPH", 0)})
    assert missing == []
    assert 22 <= score <= 24          # .40*40 + .35*20 = 23.0


def test_missing_signal_renormalises_instead_of_deflating():
    full, _ = combine({"RULES": sig("RULES", 80), "ML": sig("ML", 80),
                       "GRAPH": sig("GRAPH", 80)})
    partial, missing = combine({"RULES": sig("RULES", 80), "ML": sig("ML", 80)})
    assert missing == ["GRAPH"]
    assert partial >= full            # uncertainty premium, never a discount


def test_hard_rule_hit_forces_a_high_score():
    score, _ = combine({
        "RULES": sig("RULES", 60, [{"code": "R007", "description": "x",
                                    "weight": 60, "evidence": {}}]),
        "ML": sig("ML", 0), "GRAPH": sig("GRAPH", 0)})
    assert score >= 90


def test_decision_thresholds():
    assert to_decision(10) is Decision.APPROVE
    assert to_decision(45) is Decision.CHALLENGE
    assert to_decision(75) is Decision.REVIEW
    assert to_decision(95) is Decision.DECLINE
