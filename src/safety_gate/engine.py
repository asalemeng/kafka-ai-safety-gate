"""The decision: built-in checks, then company rules, then the similarity check."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field

from safety_gate.policy import Policy, PolicyStore, TenantPolicy
from safety_gate.redact import mask_deep, mask_pii
from safety_gate.semantic import SemanticIndex, build_index
from safety_gate.textnorm import normalise
from safety_gate.validator import MAX_PROMPT_CHARS, has_pii, injection_language, schema_ok

log = logging.getLogger("engine")

REVIEW_MAX_CHARS = MAX_PROMPT_CHARS      # longest text kept for review in the DLQ
REDACTED = "[REDACTED]"


@dataclass(frozen=True)
class Decision:
    verdict: str                              # "PASS" or "BLOCK"
    reasons: tuple[str, ...] = ()
    forward: dict | None = None               # PASS: what goes to the model (maybe redacted)
    redactions: tuple[str, ...] = ()          # PASS: which rules removed text
    policy_version: str = "builtin"
    details: dict = field(default_factory=dict)


class Engine:
    def __init__(self, store: PolicyStore | None = None) -> None:
        self.store = store or PolicyStore(None)
        policy = self.store.current()
        self._policy: Policy = policy
        self._index: SemanticIndex | None = build_index(policy.semantic)   # start-up: errors are fatal

    # --- which rules are active ---------------------------------------------
    def _active(self) -> tuple[Policy, SemanticIndex | None]:
        policy = self.store.current()
        if policy is not self._policy:
            try:
                index = build_index(policy.semantic)
            except (OSError, ValueError) as exc:
                log.error("policy %s rejected (similarity sources): %s; keeping version %s",
                          policy.version, exc, self._policy.version)
                self.store.keep(self._policy)          # stay on the previous, working rule set
            else:
                self._policy, self._index = policy, index
        return self._policy, self._index

    @property
    def policy_version(self) -> str:
        return self._policy.version

    # --- the decision -------------------------------------------------------
    def evaluate(self, request) -> Decision:
        policy, index = self._active()
        if not schema_ok(request):
            return Decision("BLOCK", ("schema",), policy_version=policy.version)

        rules = policy.for_tenant(request["tenant_id"])
        prompt: str = request["prompt"]
        folded = normalise(prompt)
        reasons: list[str] = []
        redactions: list[str] = []
        details: dict = {}

        if len(prompt) > rules.max_prompt_chars:
            reasons.append("size")

        if rules.pii != "off" and has_pii(prompt):
            if rules.pii == "block":
                reasons.append("pii")
            else:
                prompt = mask_pii(prompt)
                redactions.append("pii")

        if rules.prompt_injection == "block":
            language = injection_language(prompt)
            if language is not None:
                reasons.append("prompt_injection")
                details["injection_language"] = language

        for rule in rules.rules:
            if rule.action == "block":
                if rule.matches(prompt, folded):
                    reasons.append(rule.reason)
                continue
            prompt, cut = rule.pattern.subn(REDACTED, prompt)
            if cut:
                redactions.append(rule.id)
            if rule.normalised and rule.normalised.search(normalise(prompt)):
                # the term is still there in disguise (odd spacing, look-alike
                # letters); it cannot be cut out safely, so the request is blocked
                reasons.append(rule.reason)

        if index is not None and len(prompt) <= rules.max_prompt_chars:
            match = index.check(prompt, request["tenant_id"])
            if match is not None:
                reasons.append(match.reason)
                details["semantic_source"] = match.source_id
                details["semantic_score"] = round(match.score, 3)

        if reasons:
            return Decision("BLOCK", tuple(dict.fromkeys(reasons)), policy_version=policy.version, details=details)

        forward = request if not redactions else {**request, "prompt": prompt}
        return Decision("PASS", forward=forward, redactions=tuple(redactions), policy_version=policy.version)

    # --- what may be stored for review --------------------------------------
    def _mask_text(self, rules: TenantPolicy):
        def mask(text: str) -> str:
            text = mask_pii(text)
            for rule in rules.rules:
                text = rule.pattern.sub(REDACTED, text)
            if len(text) > REVIEW_MAX_CHARS:
                text = f"{text[:REVIEW_MAX_CHARS]}…[truncated, {len(text)} characters]"
            return text
        return mask

    def for_review(self, value, reasons: tuple[str, ...] = ()):
        """A copy of a rejected request (or raw text) that is safe to keep in the DLQ.

        Personal data and company terms are masked. A prompt blocked for being
        close to a confidential document is not stored at all, only its hash.
        """
        tenant = value.get("tenant_id") if isinstance(value, dict) else None
        rules = self._policy.for_tenant(tenant)
        masked = mask_deep(value, self._mask_text(rules))
        if "semantic_confidential" in reasons and isinstance(masked, dict) and isinstance(value.get("prompt"), str):
            digest = hashlib.sha256(value["prompt"].encode("utf-8")).hexdigest()
            masked["prompt"] = f"[WITHHELD: similar to a confidential document, sha256={digest}]"
        return masked
