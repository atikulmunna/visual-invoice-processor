from __future__ import annotations

import pytest

from app.state_machine import (
    TERMINAL_STATES,
    InvalidTransitionError,
    can_transition,
    transition_state,
)


def test_valid_transitions() -> None:
    assert transition_state("NEW", "CLAIMED") == "CLAIMED"
    assert transition_state("CLAIMED", "EXTRACTED") == "EXTRACTED"
    assert transition_state("EXTRACTED", "VALIDATED") == "VALIDATED"
    assert transition_state("VALIDATED", "STORED") == "STORED"
    assert transition_state("STORED", "ARCHIVED") == "ARCHIVED"


def test_invalid_transition_raises() -> None:
    with pytest.raises(InvalidTransitionError, match="Invalid transition"):
        transition_state("NEW", "ARCHIVED")


def test_unknown_state_raises() -> None:
    with pytest.raises(InvalidTransitionError, match="Unknown state"):
        transition_state("MISSING", "CLAIMED")


def test_terminal_states_have_no_outbound_transitions() -> None:
    for state in TERMINAL_STATES:
        assert not can_transition(state, "NEW")
        with pytest.raises(InvalidTransitionError):
            transition_state(state, "NEW")

