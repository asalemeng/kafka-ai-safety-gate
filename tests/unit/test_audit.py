import pytest

from safety_gate.audit import GENESIS, AuditEntry, append, chain_hash, verify


def build_chain(events):
    entries, last = [], None
    for event in events:
        last = append(last, event)
        entries.append(last)
    return entries


EVENTS = [
    {"topic": "clean", "request_id": "r-1"},
    {"topic": "dlq", "request_id": "r-2", "reasons": ["pii"]},
    {"topic": "clean", "request_id": "r-3"},
]


# --- hashing ---

def test_same_event_gives_same_hash():
    assert chain_hash(GENESIS, 0, {"a": 1}) == chain_hash(GENESIS, 0, {"a": 1})


def test_key_order_does_not_matter():
    assert chain_hash(GENESIS, 0, {"a": 1, "b": 2}) == chain_hash(GENESIS, 0, {"b": 2, "a": 1})


@pytest.mark.parametrize("changed", [
    {"a": 2},            # value changed
    {"a": 1, "b": 0},    # field added
    {"A": 1},            # key changed
], ids=["value-changed", "field-added", "key-changed"])
def test_any_change_changes_hash(changed):
    assert chain_hash(GENESIS, 0, {"a": 1}) != chain_hash(GENESIS, 0, changed)


def test_seq_is_part_of_hash():
    assert chain_hash(GENESIS, 0, {"a": 1}) != chain_hash(GENESIS, 1, {"a": 1})


# --- building the chain ---

def test_first_entry_chains_from_genesis():
    first = append(None, EVENTS[0])
    assert first.seq == 0
    assert first.prev_hash == GENESIS


def test_entries_link_to_each_other():
    chain = build_chain(EVENTS)
    assert [e.seq for e in chain] == [0, 1, 2]
    assert chain[1].prev_hash == chain[0].hash
    assert chain[2].prev_hash == chain[1].hash


def test_round_trip_through_dict():
    entry = append(None, EVENTS[0])
    assert AuditEntry.from_dict(entry.to_dict()) == entry


# --- verifying ---

def test_empty_chain_is_valid():
    assert verify([]) is None


def test_intact_chain_is_valid():
    assert verify(build_chain(EVENTS)) is None


def test_tampered_event_is_detected_at_its_seq():
    chain = build_chain(EVENTS)
    tampered = AuditEntry(seq=1, prev_hash=chain[1].prev_hash, hash=chain[1].hash,
                          event={"topic": "clean", "request_id": "r-2"})   # dlq -> clean
    chain[1] = tampered
    assert verify(chain) == 1


def test_recomputed_hash_after_tampering_is_still_detected():
    # an attacker who rewrites entry 1 AND its hash still breaks the link from entry 2
    chain = build_chain(EVENTS)
    forged = append(chain[0], {"topic": "clean", "request_id": "r-2"})
    chain[1] = forged
    assert verify(chain) == 2


def test_deleted_entry_is_detected():
    chain = build_chain(EVENTS)
    del chain[1]
    assert verify(chain) == 2


def test_swapped_entries_are_detected():
    chain = build_chain(EVENTS)
    chain[1], chain[2] = chain[2], chain[1]
    assert verify(chain) == 2
