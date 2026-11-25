"""Text normalisation shared by the pattern checks and the similarity check.

Attackers hide trigger phrases behind cosmetic changes: Arabic diacritics,
different letter forms, zero-width characters, full-width Latin letters. All
checks run on the normalised text so those tricks do not help.
"""
import re
import unicodedata

# zero-width and direction marks, soft hyphen, Arabic tatweel (the stretching character)
_INVISIBLE = re.compile("[​-‏‪-‮⁠-⁤﻿­ـ]")
_ARABIC_MARKS = re.compile("[ؐ-ًؚ-ٰٟۖ-ۭ]")   # tashkeel
_SPACES = re.compile(r"\s+")
_ARABIC_FOLD = str.maketrans({
    "أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا",
    "ى": "ي", "ئ": "ي", "ؤ": "و", "ة": "ه",
})


def normalise(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = _INVISIBLE.sub("", text)
    text = _ARABIC_MARKS.sub("", text)
    text = text.translate(_ARABIC_FOLD)
    text = text.casefold()
    return _SPACES.sub(" ", text).strip()
