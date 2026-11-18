import json

import pytest

from safety_gate.killswitch import DISABLED, ENABLED, KillSwitch, parse_command


def command(mode, **extra):
    return json.dumps({"mode": mode, **extra}).encode()


# --- parsing ---

@pytest.mark.parametrize("mode", [ENABLED, DISABLED])
def test_valid_modes_are_parsed(mode):
    assert parse_command(command(mode)).mode == mode


@pytest.mark.parametrize("typed", ["disabled", "Disabled", "  DISABLED  "],
                         ids=["lowercase", "capitalised", "padded"])
def test_mode_is_case_and_space_insensitive(typed):
    assert parse_command(command(typed)).mode == DISABLED


def test_reason_and_issuer_are_kept():
    parsed = parse_command(command(DISABLED, reason="injection wave", issued_by="oncall"))
    assert (parsed.reason, parsed.issued_by) == ("injection wave", "oncall")


@pytest.mark.parametrize("raw", [
    None,
    b"not json",
    b"\xff\xfe",
    b'["DISABLED"]',
    b'{"reason": "no mode"}',
    b'{"mode": "PAUSED"}',
    b'{"mode": 1}',
], ids=["tombstone", "not-json", "bad-bytes", "not-an-object", "missing-mode", "unknown-mode", "non-string-mode"])
def test_invalid_commands_are_rejected(raw):
    assert parse_command(raw) is None


# --- state ---

def test_switch_starts_enabled():
    assert KillSwitch().disabled is False


def test_disable_then_enable():
    switch = KillSwitch()
    assert switch.apply(command(DISABLED, reason="incident 42")) is True
    assert switch.disabled is True
    assert switch.reason == "incident 42"

    assert switch.apply(command(ENABLED)) is True
    assert switch.disabled is False
    assert switch.reason == ""


def test_last_command_wins():
    switch = KillSwitch()
    for mode in (DISABLED, ENABLED, DISABLED):
        switch.apply(command(mode))
    assert switch.disabled is True


def test_garbage_does_not_enable_a_disabled_gate():
    switch = KillSwitch()
    switch.apply(command(DISABLED))
    assert switch.apply(b"not json") is False
    assert switch.disabled is True          # fail closed: stays off


def test_garbage_does_not_disable_an_enabled_gate():
    switch = KillSwitch()
    assert switch.apply(b'{"mode": "PAUSED"}') is False
    assert switch.disabled is False
