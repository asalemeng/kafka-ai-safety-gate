"""Similarity check: is this prompt close to a known attack or to a confidential document?

This is the simplified version of a semantic layer. It compares overlapping
character sequences ("n-grams") after normalisation, so it needs no model, no
GPU and no network, and it works the same way for every script.

What it catches: copies, near-copies, re-ordered or lightly reworded text, and
text hidden inside a longer prompt.
What it does NOT catch: the same meaning in completely different words, or a
translation into a language that is not in the reference texts. For that, plug
in a multilingual embedding model behind the same `check()` interface.
Measured numbers: `python -m safety_gate.semantic_eval`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

from safety_gate.policy import SemanticConfig
from safety_gate.textnorm import normalise

NGRAM_SIZES = (4, 5)
CHUNK_CHARS = 160        # reference documents are cut into pieces of about this size
MIN_CHUNK_CHARS = 16     # shorter pieces match by accident too easily
WINDOW_CHARS = 320       # the prompt is scanned in overlapping windows of this size
_SENTENCE_END = re.compile(r"(?<=[.!?؟。\n])\s*")


def ngrams(text: str) -> frozenset[str]:
    text = normalise(text)
    return frozenset(text[i:i + n] for n in NGRAM_SIZES for i in range(len(text) - n + 1))


def chunks(text: str) -> list[str]:
    """Split a document into pieces of whole sentences, at most CHUNK_CHARS long."""
    pieces: list[str] = []
    current = ""
    for sentence in (s.strip() for s in _SENTENCE_END.split(text)):
        if not sentence:
            continue
        while len(sentence) > CHUNK_CHARS:                    # a very long sentence is cut up
            pieces.append(sentence[:CHUNK_CHARS])
            sentence = sentence[CHUNK_CHARS // 2:]
        if current and len(current) + 1 + len(sentence) > CHUNK_CHARS:
            pieces.append(current)
            current = sentence
        else:
            current = f"{current} {sentence}".strip()
    if current:
        pieces.append(current)
    return [p for p in pieces if len(normalise(p)) >= MIN_CHUNK_CHARS]


def windows(prompt: str) -> list[str]:
    if len(prompt) <= WINDOW_CHARS:
        return [prompt]
    step = WINDOW_CHARS // 2
    return [prompt[i:i + WINDOW_CHARS] for i in range(0, len(prompt) - step, step)]


@dataclass(frozen=True)
class Match:
    source_id: str
    kind: str        # "attack" or "confidential"
    score: float     # share of the reference piece that also appears in the prompt, 0..1

    @property
    def reason(self) -> str:
        return f"semantic_{self.kind}"


@dataclass(frozen=True)
class _Chunk:
    source_id: str
    kind: str
    tenants: frozenset[str]
    size: int        # number of n-grams in the piece


class SemanticIndex:
    def __init__(self, threshold: float = 0.5) -> None:
        self.threshold = threshold
        self._chunks: list[_Chunk] = []
        self._postings: dict[str, list[int]] = {}

    def __len__(self) -> int:
        return len(self._chunks)

    def add(self, source_id: str, kind: str, text: str, tenants=(), one_per_line: bool = False) -> int:
        """Add reference text. Returns how many pieces were indexed."""
        if one_per_line:
            pieces = [line.strip() for line in text.splitlines()
                      if line.strip() and not line.lstrip().startswith("#")]
            pieces = [p for p in pieces if len(normalise(p)) >= MIN_CHUNK_CHARS]
        else:
            pieces = chunks(text)
        for piece in pieces:
            grams = ngrams(piece)
            index = len(self._chunks)
            self._chunks.append(_Chunk(source_id, kind, frozenset(tenants), len(grams)))
            for gram in grams:
                self._postings.setdefault(gram, []).append(index)
        return len(pieces)

    def best(self, prompt: str, tenant_id: str | None = None) -> Match | None:
        """The closest reference piece this tenant is checked against, whatever its score."""
        best: Match | None = None
        for window in windows(prompt):
            hits: dict[int, int] = {}
            for gram in ngrams(window):
                for index in self._postings.get(gram, ()):
                    hits[index] = hits.get(index, 0) + 1
            for index, shared in hits.items():
                chunk = self._chunks[index]
                if chunk.tenants and tenant_id not in chunk.tenants:
                    continue
                score = shared / chunk.size
                if best is None or score > best.score:
                    best = Match(chunk.source_id, chunk.kind, score)
        return best

    def check(self, prompt: str, tenant_id: str | None = None) -> Match | None:
        """A Match if the prompt is too close to a reference piece, else None."""
        match = self.best(prompt, tenant_id)
        return match if match is not None and match.score >= self.threshold else None


def builtin_text(name: str) -> str:
    if not re.fullmatch(r"[a-z0-9_]+", name):
        raise ValueError(f"unknown built-in source {name!r}")
    try:
        return resources.files("safety_gate").joinpath("data", f"{name}.txt").read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ValueError(f"unknown built-in source {name!r}") from None


def build_index(config: SemanticConfig) -> SemanticIndex | None:
    """Build the index a policy asks for, or None if the check is switched off."""
    if not config.enabled or not config.sources:
        return None
    index = SemanticIndex(config.threshold)
    for source in config.sources:
        if source.builtin:
            text = builtin_text(source.builtin)
        elif source.path:
            text = Path(source.path).read_text(encoding="utf-8")
        else:
            continue
        index.add(source.id, source.kind, text, source.tenants, one_per_line=source.kind == "attack")
    return index
