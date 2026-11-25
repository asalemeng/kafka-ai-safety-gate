"""The injection filter measured on a small labelled set in four languages.

The set (tests/data/injection_samples.jsonl) was written for this project. It
is a regression check, not an independent benchmark: it shows the filter still
catches the common phrasings, blocks no harmless prompt in the set, and it keeps
the reworded attacks the filter is known to miss visible.
"""
import json
from pathlib import Path

import pytest

from safety_gate.validator import injection_language

SAMPLES = [json.loads(line) for line in
           (Path(__file__).parent.parent / "data" / "injection_samples.jsonl").read_text(encoding="utf-8").splitlines()]
LANGUAGES = ("en", "de", "ar", "franco")


def flagged(sample) -> bool:
    return injection_language(sample["text"]) is not None


@pytest.mark.parametrize("language", LANGUAGES)
def test_no_harmless_prompt_is_blocked(language):
    wrong = [s["text"] for s in SAMPLES if s["lang"] == language and s["label"] == "benign" and flagged(s)]
    assert wrong == []


@pytest.mark.parametrize("language", LANGUAGES)
def test_most_attacks_are_caught(language):
    attacks = [s for s in SAMPLES if s["lang"] == language and s["label"] == "attack"]
    caught = sum(flagged(s) for s in attacks)
    assert caught / len(attacks) >= 0.75, f"{language}: only {caught} of {len(attacks)} attacks caught"


def test_the_set_contains_attacks_the_filter_misses():
    """Honesty check: if this fails, either the filter improved or the hard examples were removed."""
    missed = [s for s in SAMPLES if s["label"] == "attack" and not flagged(s)]
    assert len(missed) == 8
