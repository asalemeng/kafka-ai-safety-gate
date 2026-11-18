"""Kill switch: an operator command that makes the gate reject every request."""
import json
from dataclasses import dataclass

ENABLED = "ENABLED"
DISABLED = "DISABLED"
MODES = (ENABLED, DISABLED)


@dataclass(frozen=True)
class Command:
    mode: str
    reason: str = ""
    issued_by: str = ""


def parse_command(raw: bytes | None) -> Command | None:
    """Return the command carried by a control message, or None if it is not valid."""
    if raw is None:                      # tombstone on the compacted topic
        return None
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict):
        return None

    mode = data.get("mode")
    if not isinstance(mode, str):
        return None
    mode = mode.strip().upper()          # an operator typing "disabled" during an incident still works
    if mode not in MODES:
        return None

    return Command(
        mode=mode,
        reason=str(data.get("reason", "")),
        issued_by=str(data.get("issued_by", "")),
    )


class KillSwitch:
    """Current state of the switch. Starts ENABLED; only valid commands change it."""

    def __init__(self) -> None:
        self._last: Command | None = None

    @property
    def disabled(self) -> bool:
        return self._last is not None and self._last.mode == DISABLED

    @property
    def reason(self) -> str:
        return self._last.reason if self.disabled else ""

    @property
    def last_reason(self) -> str:
        """Reason given with the most recent command, whichever way it switched."""
        return self._last.reason if self._last else ""

    def apply(self, raw: bytes | None) -> bool:
        """Apply one control message. Returns True if it was a valid command."""
        command = parse_command(raw)
        if command is None:
            return False                 # garbage never flips the switch, in either direction
        self._last = command
        return True
