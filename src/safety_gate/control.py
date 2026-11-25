"""Reading the kill switch from the control topic. Used by the gate and the model service."""
from __future__ import annotations

import logging
import time

from confluent_kafka import OFFSET_BEGINNING, TopicPartition

from safety_gate.killswitch import KillSwitch

log = logging.getLogger("control")


class SwitchUnknown(RuntimeError):
    """The state of the kill switch could not be confirmed. Callers must stop, not guess."""


class SwitchWatcher:
    """Keeps a KillSwitch up to date with every command on the control topic.

    The switch is rebuilt from the whole topic on every start and the consumer
    never commits offsets, so a restarted process remembers it was disabled.
    """

    def __init__(self, consumer, topic: str, timeout: float = 10.0, clock=time.monotonic) -> None:
        self.consumer = consumer
        self.topic = topic
        self.timeout = timeout
        self.clock = clock
        self.switch = KillSwitch()
        self._next_offset = 0
        self._checked_at: float | None = None
        consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])

    def refresh(self) -> KillSwitch:
        """Apply every command written so far. Raises SwitchUnknown if that is not possible."""
        try:
            marks = self.consumer.get_watermark_offsets(
                TopicPartition(self.topic, 0), timeout=self.timeout, cached=False)
        except Exception as exc:
            raise SwitchUnknown(f"kill switch state unknown: {exc}") from exc
        if marks is None:
            raise SwitchUnknown("kill switch state unknown: control topic did not answer")
        low, high = marks
        self._next_offset = max(self._next_offset, low)

        deadline = self.clock() + self.timeout
        while self._next_offset < high:
            if self.clock() > deadline:
                raise SwitchUnknown("kill switch state unknown: could not read the control topic to its end")
            msg = self.consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            was_disabled = self.switch.disabled
            self.switch.apply(msg.value())
            self._next_offset = msg.offset() + 1
            if self.switch.disabled != was_disabled:
                log.warning("kill switch %s (%s)",
                            "ON: rejecting everything" if self.switch.disabled else "OFF",
                            self.switch.last_reason or "no reason given")
        self._checked_at = self.clock()
        return self.switch

    def refresh_if_older(self, max_age_s: float) -> KillSwitch:
        """Re-read the switch unless it was confirmed less than `max_age_s` seconds ago."""
        if self._checked_at is None or max_age_s <= 0 or self.clock() - self._checked_at >= max_age_s:
            return self.refresh()
        return self.switch
