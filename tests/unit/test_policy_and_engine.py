import json
import os
import time

import pytest

from safety_gate.engine import Engine
from safety_gate.policy import Policy, PolicyError, PolicyStore, load_policy, parse_policy
from safety_gate.redact import mask_pii

IBAN = "DE89 3704 0044 0532 0130 00"
BASE = {
    "version": "v1",
    "defaults": {"rules": [
        {"id": "project-names", "type": "keyword", "terms": ["Project Falcon"], "action": "block",
         "reason": "confidential_term"},
        {"id": "customer-numbers", "type": "regex", "pattern": r"\bKD-\d{6}\b", "action": "redact"},
    ]},
    "tenants": {"t-helpdesk": {"pii": "redact"}, "t-short": {"max_prompt_chars": 10}},
}


def request(prompt, tenant="t-1"):
    return {"request_id": "r-1", "tenant_id": tenant, "prompt": prompt}


def engine_for(data, tmp_path):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return Engine(PolicyStore(str(path), check_every_s=0)), path


# --- masking -------------------------------------------------------------------
def test_mask_keeps_only_the_start_of_an_iban_and_nothing_of_an_email():
    assert mask_pii(f"IBAN {IBAN} mail a.b@example.com") == "IBAN DE89 •••• mail ••••@••••"


# --- policy file ---------------------------------------------------------------
def test_without_a_policy_file_the_built_in_checks_apply():
    decision = Engine().evaluate(request(f"my IBAN {IBAN}"))
    assert decision.reasons == ("pii",) and decision.policy_version == "builtin"


def test_keyword_rule_blocks_with_its_own_reason(tmp_path):
    engine, _ = engine_for(BASE, tmp_path)
    decision = engine.evaluate(request("Status of project falcon please"))
    assert decision.verdict == "BLOCK"
    assert decision.reasons == ("confidential_term",)
    assert decision.policy_version == "v1"


def test_keyword_is_found_when_disguised_with_invisible_characters(tmp_path):
    engine, _ = engine_for(BASE, tmp_path)
    assert engine.evaluate(request("Status of Project Fal​con")).verdict == "BLOCK"


def test_keyword_does_not_match_inside_another_word(tmp_path):
    engine, _ = engine_for(BASE, tmp_path)
    assert engine.evaluate(request("We sell Project Falconry gloves")).verdict == "PASS"


def test_redact_rule_removes_the_text_and_lets_the_request_pass(tmp_path):
    engine, _ = engine_for(BASE, tmp_path)
    decision = engine.evaluate(request("Why was customer KD-123456 charged twice?"))
    assert decision.verdict == "PASS"
    assert decision.forward["prompt"] == "Why was customer [REDACTED] charged twice?"
    assert decision.redactions == ("customer-numbers",)


def test_tenant_can_redact_personal_data_instead_of_blocking(tmp_path):
    engine, _ = engine_for(BASE, tmp_path)
    blocked = engine.evaluate(request("mail a@b.com", tenant="t-1"))
    redacted = engine.evaluate(request("mail a@b.com", tenant="t-helpdesk"))
    assert blocked.reasons == ("pii",)
    assert redacted.verdict == "PASS" and redacted.forward["prompt"] == "mail ••••@••••"


def test_tenant_limit_overrides_the_default(tmp_path):
    engine, _ = engine_for(BASE, tmp_path)
    assert engine.evaluate(request("a" * 11, tenant="t-short")).reasons == ("size",)
    assert engine.evaluate(request("a" * 11, tenant="t-1")).verdict == "PASS"


def test_tenant_inherits_the_default_rules(tmp_path):
    engine, _ = engine_for(BASE, tmp_path)
    assert engine.evaluate(request("Project Falcon", tenant="t-helpdesk")).reasons == ("confidential_term",)


def test_unchanged_request_is_forwarded_as_the_same_object(tmp_path):
    engine, _ = engine_for(BASE, tmp_path)
    req = request("hello")
    assert engine.evaluate(req).forward is req


@pytest.mark.parametrize("broken, message", [
    ({"defaults": {}}, "version"),
    ({"version": "1", "defualts": {}}, "unknown setting"),
    ({"version": "1", "defaults": {"pii": "maybe"}}, "pii"),
    ({"version": "1", "defaults": {"max_prompt_chars": 0}}, "max_prompt_chars"),
    ({"version": "1", "defaults": {"rules": [{"id": "x", "type": "regex", "pattern": "("}]}}, "does not compile"),
    ({"version": "1", "defaults": {"rules": [{"id": "x", "type": "keyword", "terms": []}]}}, "terms"),
    ({"version": "1", "defaults": {"rules": [{"id": "x", "type": "keyword", "terms": ["a"], "action": "allow"}]}},
     "action"),
    ({"version": "1", "defaults": {"rules": [{"id": "x", "type": "keyword", "terms": ["a"]},
                                             {"id": "x", "type": "keyword", "terms": ["b"]}]}}, "unique"),
    ({"version": "1", "semantic": {"threshold": 2}}, "threshold"),
    ("just text", "mapping"),
], ids=["no-version", "typo", "bad-choice", "bad-limit", "bad-regex", "no-terms", "bad-action", "duplicate-id",
        "bad-threshold", "not-a-mapping"])
def test_invalid_policy_is_rejected_with_a_clear_message(broken, message):
    with pytest.raises(PolicyError, match=message):
        parse_policy(broken)


def test_gate_does_not_start_with_a_broken_policy_file(tmp_path):
    path = tmp_path / "policy.yaml"
    path.write_text("version: [unclosed", encoding="utf-8")
    with pytest.raises(PolicyError):
        PolicyStore(str(path))


def test_yaml_example_policy_loads():
    policy = load_policy(os.path.join(os.path.dirname(__file__), "..", "..", "examples", "policy.yaml"))
    assert isinstance(policy, Policy)
    assert policy.for_tenant("t-bank").max_prompt_chars == 2000
    assert policy.for_tenant("someone-else").max_prompt_chars == 4000


# --- reload without restart -----------------------------------------------------
def _rewrite(path, data):
    path.write_text(data if isinstance(data, str) else json.dumps(data), encoding="utf-8")
    stamp = time.time() + 5                      # make sure the change is noticed, whatever the clock resolution
    os.utime(path, (stamp, stamp))


def test_changed_policy_is_picked_up_without_a_restart(tmp_path):
    engine, path = engine_for(BASE, tmp_path)
    assert engine.evaluate(request("Project Eagle")).verdict == "PASS"

    changed = json.loads(json.dumps(BASE))
    changed["version"] = "v2"
    changed["defaults"]["rules"][0]["terms"].append("Project Eagle")
    _rewrite(path, changed)

    decision = engine.evaluate(request("Project Eagle"))
    assert decision.verdict == "BLOCK" and decision.policy_version == "v2"


def test_broken_new_version_is_rejected_and_the_old_rules_stay_active(tmp_path):
    engine, path = engine_for(BASE, tmp_path)
    _rewrite(path, "{ this is not valid")

    decision = engine.evaluate(request("Project Falcon"))
    assert decision.verdict == "BLOCK" and decision.policy_version == "v1"     # checks were never switched off
    assert engine.store.reload_errors == 1


# --- what is kept for review -------------------------------------------------------
def test_review_copy_masks_personal_data_and_company_terms(tmp_path):
    engine, _ = engine_for(BASE, tmp_path)
    kept = engine.for_review(request(f"Project Falcon, IBAN {IBAN}, KD-123456"))
    assert kept["prompt"] == "[REDACTED], IBAN DE89 ••••, [REDACTED]"
    assert kept["request_id"] == "r-1"


def test_review_copy_of_a_huge_prompt_is_cut():
    kept = Engine().for_review(request("a" * 10_000))
    assert len(kept["prompt"]) < 4_100 and "truncated, 10000 characters" in kept["prompt"]
