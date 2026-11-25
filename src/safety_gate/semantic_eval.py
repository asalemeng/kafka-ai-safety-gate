"""Measure the similarity check on labelled examples.

    python -m safety_gate.semantic_eval --policy examples/policy.yaml \
        --examples tests/data/semantic_eval.jsonl --tenant t-bank

Each line of the examples file: {"label": "attack" | "confidential" | "benign", "text": "...", "note": "..."}
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

from safety_gate.policy import load_policy
from safety_gate.semantic import SemanticIndex, build_index


@dataclass(frozen=True)
class Report:
    true_positives: int
    false_negatives: int
    false_positives: int
    true_negatives: int
    missed: tuple[dict, ...]
    wrongly_blocked: tuple[dict, ...]

    @property
    def recall(self) -> float:
        found = self.true_positives + self.false_negatives
        return self.true_positives / found if found else 1.0

    @property
    def precision(self) -> float:
        flagged = self.true_positives + self.false_positives
        return self.true_positives / flagged if flagged else 1.0


def load_examples(path: str | Path) -> list[dict]:
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


def evaluate(index: SemanticIndex, examples: list[dict], tenant_id: str | None = None) -> Report:
    tp = fn = fp = tn = 0
    missed, wrong = [], []
    for example in examples:
        flagged = index.check(example["text"], tenant_id) is not None
        harmful = example["label"] != "benign"
        if harmful and flagged:
            tp += 1
        elif harmful:
            fn += 1
            missed.append(example)
        elif flagged:
            fp += 1
            wrong.append(example)
        else:
            tn += 1
    return Report(tp, fn, fp, tn, tuple(missed), tuple(wrong))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Precision and recall of the similarity check.")
    parser.add_argument("--policy", required=True)
    parser.add_argument("--examples", required=True)
    parser.add_argument("--tenant", default=None)
    args = parser.parse_args(argv)

    index = build_index(load_policy(args.policy).semantic)
    if index is None:
        print("the policy has no similarity sources", file=sys.stderr)
        return 1
    report = evaluate(index, load_examples(args.examples), args.tenant)
    total = report.true_positives + report.false_negatives + report.false_positives + report.true_negatives
    print(f"examples: {total}   threshold: {index.threshold}")
    print(f"recall:    {report.recall:.2f}  ({report.true_positives} of "
          f"{report.true_positives + report.false_negatives} harmful prompts caught)")
    print(f"precision: {report.precision:.2f}  ({report.false_positives} harmless prompt(s) wrongly blocked)")
    for title, rows in (("missed", report.missed), ("wrongly blocked", report.wrongly_blocked)):
        for row in rows:
            print(f"  {title}: [{row.get('note') or row['label']}] {row['text'][:70]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
