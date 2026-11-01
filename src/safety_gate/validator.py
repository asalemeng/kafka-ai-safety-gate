import re
from dataclasses import dataclass

# --- Part 1: settings ---
MAX_PROMPT_CHARS = 4000
REQUIRED_FIELDS = ("request_id", "tenant_id", "prompt")

EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
IBAN_RE = re.compile(r"\b[A-Z]{2}\d{2}(?:\s?[A-Z0-9]{4}){2,7}(?:\s?[A-Z0-9]{1,4})?\b")

INJECTION_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in (
        r"ignore (all |the )?(previous|prior|above) instructions",
        r"reveal (your|the) system prompt",
        r"you are now (in )?developer mode",
    )
]


# --- Part 2: the result ---
@dataclass(frozen=True)
class ValidationResult:
    verdict: str                 # "PASS" or "BLOCK"
    reasons: tuple[str, ...] = ()


def validate(request) -> ValidationResult:
    # --- Part 3: schema check ---
    if not isinstance(request, dict):
        return ValidationResult("BLOCK", ("schema",))

    for field in REQUIRED_FIELDS:
        value = request.get(field)
        if not isinstance(value, str) or not value.strip():
            return ValidationResult("BLOCK", ("schema",))

    # --- Part 4: content checks ---
    prompt = request["prompt"]
    reasons = []
    if len(prompt) > MAX_PROMPT_CHARS:
        reasons.append("size")
    if EMAIL_RE.search(prompt) or IBAN_RE.search(prompt):
        reasons.append("pii")
    if any(p.search(prompt) for p in INJECTION_PATTERNS):
        reasons.append("prompt_injection")

    return ValidationResult("BLOCK" if reasons else "PASS", tuple(reasons))