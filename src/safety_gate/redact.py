"""Masking of personal data before anything is stored for review."""
from safety_gate.validator import EMAIL_RE, IBAN_RE

MASK = "••••"


def mask_pii(text: str) -> str:
    """IBANs keep country code and check digits only (DE89 ••••); e-mails are fully masked."""
    text = IBAN_RE.sub(lambda m: f"{m.group(0)[:4]} {MASK}", text)
    return EMAIL_RE.sub(f"{MASK}@{MASK}", text)


def mask_deep(value, mask=mask_pii):
    """Apply `mask` to every string inside a JSON-like value (keys are left alone)."""
    if isinstance(value, str):
        return mask(value)
    if isinstance(value, list):
        return [mask_deep(v, mask) for v in value]
    if isinstance(value, dict):
        return {k: mask_deep(v, mask) for k, v in value.items()}
    return value
