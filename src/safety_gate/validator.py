"""Built-in checks: schema, size, personal data, prompt injection."""
import re
from dataclasses import dataclass

from safety_gate.textnorm import normalise

# --- Part 1: settings ---
MAX_PROMPT_CHARS = 4000
REQUIRED_FIELDS = ("request_id", "tenant_id", "prompt")

EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
IBAN_RE = re.compile(r"\b[A-Z]{2}\d{2}(?:\s?[A-Z0-9]{4}){2,7}(?:\s?[A-Z0-9]{1,4})?\b")

# Patterns are matched against normalise(prompt): lower case, Arabic letter
# forms unified (أ إ آ -> ا, ة -> ه, ى -> ي), diacritics and zero-width
# characters removed. Write new patterns in that normalised form.
# This is a keyword filter. It stops the common phrasings, not a determined
# attacker who rewords; see "Limits" in the README.
_INJECTION_SOURCES = {
    "en": (
        r"\b(ignore|disregard|forget) ((all|the|any|your) )*(previous|prior|above|earlier) (instructions|rules|prompts?)",
        r"\b(reveal|show me|print|tell me) (your|the) system prompt",
        r"\byou are now (in )?developer mode",
    ),
    "de": (
        r"\b(ignoriere?|vergiss|missachte|übergehe) (bitte )?((alle|die|deine|sämtliche|all) )*"
        r"(vorherigen|bisherigen|vorigen|obigen|früheren|vorangegangenen) "
        r"(anweisungen|instruktionen|befehle|regeln|vorgaben)",
        r"\b(zeige?|verrate?|nenne?|gib|offenbare) (mir )?(bitte )?(deinen?|den|die|das) "
        r"(system-? ?prompt|systemanweisung(en)?|systemnachricht)",
        r"\bdu bist (jetzt|nun|ab sofort) im (entwickler-? ?modus|developer[- ]?mod(e|us))",
    ),
    # Modern Standard Arabic and Egyptian Arabic
    "ar": (
        r"(?<!\w)(تجاهل|تجاهلي|انس|انسي|اهمل|طنش|سيبك من|لا تتبع|متتبعش|متسمعش) "
        r"((كل|جميع) )?(ال)?(تعليمات|اوامر|ارشادات|توجيهات)",
        r"(?<!\w)(اكشف|اظهر|اعرض|اطبع|وريني|قول ?لي|قل ?لي|اعطني|اديني|هات)( لي| عن)? "
        r".{0,20}(تعليمات|برومبت|موجه) (ال)?(نظام|سيستم)",
        r"(?<!\w)(انت|انتي) (الان|دلوقتي|دلوقت|حاليا) (في |ف )?(وضع|مود) (ال)?(مطور|مطورين|ديفلوبر)",
    ),
    # Franco-Arabic (Arabic typed in Latin letters and digits)
    "franco": (
        r"\b(tagahal|tgahal|tegahal|ensa|insa|sebak men|sibak men|tanesh|tannesh|tanish) (kol )?"
        r"((el|al|il)[ -]?)?(ta3limat|ta3leemat|ta3lemat|awamer|2awamer|instructions|rules)",
        r"\b(2oly|2olly|2olli|oly|olly|wareeny|warini|warrini|ekshef|tala3|talla3|edeeny|eddini)\b"
        r".{0,30}\b(system prompt|el prompt|ta3limat el (system|nezam|sistem))",
        r"\b(enta|inta|enty) (delwa2ty|delwa2ti|dilwa2ti|dlw2ty|now) (fe |fi |f )?"
        r"(developer mode|wad3 el (developer|motawer))",
    ),
}
INJECTION_PATTERNS = {
    language: tuple(re.compile(p) for p in patterns)
    for language, patterns in _INJECTION_SOURCES.items()
}


# --- Part 2: the result ---
@dataclass(frozen=True)
class ValidationResult:
    verdict: str                 # "PASS" or "BLOCK"
    reasons: tuple[str, ...] = ()


# --- Part 3: single checks (also used by the policy engine) ---
def schema_ok(request) -> bool:
    if not isinstance(request, dict):
        return False
    for field in REQUIRED_FIELDS:
        value = request.get(field)
        if not isinstance(value, str) or not value.strip():
            return False
    return True


def has_pii(prompt: str) -> bool:
    return bool(EMAIL_RE.search(prompt) or IBAN_RE.search(prompt))


def injection_language(prompt: str) -> str | None:
    """Language of the first injection pattern that matches, or None."""
    text = normalise(prompt)
    for language, patterns in INJECTION_PATTERNS.items():
        if any(p.search(text) for p in patterns):
            return language
    return None


def validate(request, max_prompt_chars: int = MAX_PROMPT_CHARS) -> ValidationResult:
    if not schema_ok(request):
        return ValidationResult("BLOCK", ("schema",))

    prompt = request["prompt"]
    reasons = []
    if len(prompt) > max_prompt_chars:
        reasons.append("size")
    if has_pii(prompt):
        reasons.append("pii")
    if injection_language(prompt) is not None:
        reasons.append("prompt_injection")

    return ValidationResult("BLOCK" if reasons else "PASS", tuple(reasons))
