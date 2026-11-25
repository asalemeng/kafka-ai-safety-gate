"""Company rules, kept in a file so the security team can change them without a code change.

    version: "2026-11-25"
    defaults:
      max_prompt_chars: 4000
      pii: block                # block | redact | off
      prompt_injection: block   # block | off
      rules:
        - id: project-names
          type: keyword         # keyword | regex
          terms: ["Project Falcon"]
          action: block         # block | redact
          reason: confidential_term
    tenants:
      t-bank:
        max_prompt_chars: 2000
        pii: redact
    semantic:
      enabled: true
      threshold: 0.5
      sources:
        - {id: known-attacks, kind: attack, builtin: known_attacks}
        - {id: board-papers, kind: confidential, path: confidential/board.txt, tenants: [t-bank]}

A tenant inherits the defaults and adds its own rules. Every decision records
the policy `version`, so it is always clear which rule set blocked a request.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

from safety_gate.textnorm import normalise
from safety_gate.validator import MAX_PROMPT_CHARS

log = logging.getLogger("policy")

MAX_REGEX_CHARS = 500
BUILTIN_VERSION = "builtin"


class PolicyError(ValueError):
    """The policy file is not valid. The message says where."""


@dataclass(frozen=True)
class Rule:
    id: str
    action: str                      # "block" or "redact"
    reason: str
    pattern: re.Pattern              # matched against the prompt as written
    normalised: re.Pattern | None    # matched against normalise(prompt); keyword rules only

    def matches(self, prompt: str, normalised_prompt: str) -> bool:
        if self.pattern.search(prompt):
            return True
        return bool(self.normalised and self.normalised.search(normalised_prompt))


@dataclass(frozen=True)
class TenantPolicy:
    max_prompt_chars: int = MAX_PROMPT_CHARS
    pii: str = "block"
    prompt_injection: str = "block"
    rules: tuple[Rule, ...] = ()


@dataclass(frozen=True)
class SemanticSource:
    id: str
    kind: str                        # "attack" or "confidential"
    path: str | None = None
    builtin: str | None = None
    tenants: tuple[str, ...] = ()    # empty = applies to every tenant


@dataclass(frozen=True)
class SemanticConfig:
    enabled: bool = False
    threshold: float = 0.5
    sources: tuple[SemanticSource, ...] = ()


@dataclass(frozen=True)
class Policy:
    version: str = BUILTIN_VERSION
    defaults: TenantPolicy = field(default_factory=TenantPolicy)
    tenants: dict[str, TenantPolicy] = field(default_factory=dict)
    semantic: SemanticConfig = field(default_factory=SemanticConfig)

    def for_tenant(self, tenant_id) -> TenantPolicy:
        return self.tenants.get(tenant_id, self.defaults) if isinstance(tenant_id, str) else self.defaults


# --- parsing -----------------------------------------------------------------
def _only(data: dict, allowed: set[str], where: str) -> None:
    unknown = set(data) - allowed
    if unknown:
        raise PolicyError(f"{where}: unknown setting(s) {sorted(unknown)}; allowed: {sorted(allowed)}")


def _choice(value, allowed: tuple[str, ...], where: str) -> str:
    if value not in allowed:
        raise PolicyError(f"{where}: must be one of {list(allowed)}, got {value!r}")
    return value


def _rule(data, where: str) -> Rule:
    if not isinstance(data, dict):
        raise PolicyError(f"{where}: a rule must be a mapping")
    _only(data, {"id", "type", "terms", "pattern", "action", "reason"}, where)
    rule_id = data.get("id")
    if not isinstance(rule_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", rule_id):
        raise PolicyError(f"{where}: 'id' must be 1-64 letters, digits, '_', '.' or '-'")
    where = f"{where} ({rule_id})"
    kind = _choice(data.get("type"), ("keyword", "regex"), f"{where}: type")
    action = _choice(data.get("action", "block"), ("block", "redact"), f"{where}: action")
    reason = data.get("reason", f"policy:{rule_id}")
    if not isinstance(reason, str) or not reason:
        raise PolicyError(f"{where}: 'reason' must be text")

    if kind == "keyword":
        terms = data.get("terms")
        if not isinstance(terms, list) or not terms or not all(isinstance(t, str) and t.strip() for t in terms):
            raise PolicyError(f"{where}: 'terms' must be a non-empty list of words or phrases")
        alternatives = "|".join(re.escape(t.strip()) for t in terms)
        folded = "|".join(re.escape(normalise(t)) for t in terms)
        return Rule(rule_id, action, reason,
                    re.compile(rf"(?<!\w)(?:{alternatives})(?!\w)", re.IGNORECASE),
                    re.compile(rf"(?<!\w)(?:{folded})(?!\w)"))

    source = data.get("pattern")
    if not isinstance(source, str) or not source:
        raise PolicyError(f"{where}: a regex rule needs a 'pattern'")
    if len(source) > MAX_REGEX_CHARS:
        raise PolicyError(f"{where}: pattern is longer than {MAX_REGEX_CHARS} characters")
    try:
        return Rule(rule_id, action, reason, re.compile(source, re.IGNORECASE), None)
    except re.error as exc:
        raise PolicyError(f"{where}: pattern does not compile: {exc}") from None


def _tenant(data, base: TenantPolicy, where: str) -> TenantPolicy:
    if not isinstance(data, dict):
        raise PolicyError(f"{where}: must be a mapping")
    _only(data, {"max_prompt_chars", "pii", "prompt_injection", "rules"}, where)
    limit = data.get("max_prompt_chars", base.max_prompt_chars)
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise PolicyError(f"{where}: max_prompt_chars must be a positive whole number")
    rules = data.get("rules", [])
    if not isinstance(rules, list):
        raise PolicyError(f"{where}: rules must be a list")
    parsed = tuple(_rule(r, f"{where}: rule {i + 1}") for i, r in enumerate(rules))
    merged = base.rules + parsed
    ids = [r.id for r in merged]
    if len(ids) != len(set(ids)):
        raise PolicyError(f"{where}: rule ids must be unique, got {ids}")
    return TenantPolicy(
        max_prompt_chars=limit,
        pii=_choice(data.get("pii", base.pii), ("block", "redact", "off"), f"{where}: pii"),
        prompt_injection=_choice(data.get("prompt_injection", base.prompt_injection),
                                 ("block", "off"), f"{where}: prompt_injection"),
        rules=merged,
    )


def _semantic(data, base_dir: Path) -> SemanticConfig:
    if not isinstance(data, dict):
        raise PolicyError("semantic: must be a mapping")
    _only(data, {"enabled", "threshold", "sources"}, "semantic")
    threshold = data.get("threshold", SemanticConfig.threshold)
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not 0 < threshold <= 1:
        raise PolicyError("semantic: threshold must be a number between 0 and 1")
    sources = []
    for i, src in enumerate(data.get("sources", [])):
        where = f"semantic: source {i + 1}"
        if not isinstance(src, dict):
            raise PolicyError(f"{where}: must be a mapping")
        _only(src, {"id", "kind", "path", "builtin", "tenants"}, where)
        if not isinstance(src.get("id"), str) or not src["id"]:
            raise PolicyError(f"{where}: needs an 'id'")
        if bool(src.get("path")) == bool(src.get("builtin")):
            raise PolicyError(f"{where}: give exactly one of 'path' or 'builtin'")
        tenants = src.get("tenants", [])
        if not isinstance(tenants, list) or not all(isinstance(t, str) for t in tenants):
            raise PolicyError(f"{where}: tenants must be a list of tenant ids")
        path = str((base_dir / src["path"]).resolve()) if src.get("path") else None
        sources.append(SemanticSource(
            id=src["id"],
            kind=_choice(src.get("kind"), ("attack", "confidential"), f"{where}: kind"),
            path=path, builtin=src.get("builtin"), tenants=tuple(tenants),
        ))
    return SemanticConfig(enabled=bool(data.get("enabled", True)), threshold=float(threshold), sources=tuple(sources))


def parse_policy(data, base_dir: Path | str = ".") -> Policy:
    if not isinstance(data, dict):
        raise PolicyError("the policy file must contain a mapping at the top level")
    _only(data, {"version", "defaults", "tenants", "semantic"}, "policy")
    version = data.get("version")
    if not isinstance(version, (str, int, float)) or isinstance(version, bool) or str(version).strip() == "":
        raise PolicyError("policy: 'version' is required (any label, e.g. a date)")
    defaults = _tenant(data.get("defaults", {}), TenantPolicy(), "defaults")
    tenants_raw = data.get("tenants", {})
    if not isinstance(tenants_raw, dict):
        raise PolicyError("tenants: must be a mapping of tenant id to settings")
    tenants = {str(name): _tenant(value, defaults, f"tenant {name}") for name, value in tenants_raw.items()}
    semantic = _semantic(data["semantic"], Path(base_dir)) if "semantic" in data else SemanticConfig()
    return Policy(version=str(version), defaults=defaults, tenants=tenants, semantic=semantic)


def load_policy(path: str | os.PathLike) -> Policy:
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PolicyError(f"cannot read policy file {path}: {exc}") from None
    try:
        if path.suffix.lower() == ".json":
            data = json.loads(text)
        else:
            import yaml
            data = yaml.safe_load(text)
    except Exception as exc:   # both parsers raise their own error types
        raise PolicyError(f"{path} is not valid {'JSON' if path.suffix.lower() == '.json' else 'YAML'}: {exc}") from None
    return parse_policy(data, base_dir=path.parent)


class PolicyStore:
    """Holds the active policy and reloads the file when it changes.

    A broken new version is rejected and the previous one stays active: a typo
    in the file must never switch the checks off.
    """

    def __init__(self, path: str | None, check_every_s: float = 2.0) -> None:
        self.path = path
        self.check_every_s = check_every_s
        self.reload_errors = 0
        self._checked = time.monotonic()
        self._mtime = self._stat()
        self._policy = load_policy(path) if path else Policy()   # start-up: an invalid file is fatal

    def _stat(self) -> float | None:
        if not self.path:
            return None
        try:
            return os.stat(self.path).st_mtime
        except OSError:
            return None

    def current(self) -> Policy:
        if self.path and time.monotonic() - self._checked >= self.check_every_s:
            self._checked = time.monotonic()
            mtime = self._stat()
            if mtime != self._mtime:
                self._mtime = mtime
                try:
                    self._policy = load_policy(self.path)
                    log.warning("policy reloaded: version %s", self._policy.version)
                except PolicyError as exc:
                    self.reload_errors += 1
                    log.error("new policy rejected, keeping version %s: %s", self._policy.version, exc)
        return self._policy

    def keep(self, policy: Policy) -> None:
        """Go back to `policy` after a newer version turned out to be unusable."""
        self.reload_errors += 1
        self._policy = policy
