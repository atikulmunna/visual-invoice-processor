from __future__ import annotations

from typing import Final


class InvalidTransitionError(ValueError):
    pass


TERMINAL_STATES: Final[set[str]] = {"ARCHIVED", "FAILED"}

ALLOWED_TRANSITIONS: Final[dict[str, set[str]]] = {
    "NEW": {"CLAIMED", "FAILED"},
    "CLAIMED": {"EXTRACTED", "FAILED"},
    "EXTRACTED": {"VALIDATED", "REVIEW_REQUIRED", "FAILED"},
    "VALIDATED": {"STORED", "REVIEW_REQUIRED", "FAILED"},
    "REVIEW_REQUIRED": {"CLAIMED", "FAILED"},
    "STORED": {"ARCHIVED", "FAILED"},
    "ARCHIVED": set(),
    "FAILED": set(),
}


def can_transition(from_state: str, to_state: str) -> bool:
    from_norm = from_state.strip().upper()
    to_norm = to_state.strip().upper()
    return to_norm in ALLOWED_TRANSITIONS.get(from_norm, set())


def transition_state(from_state: str, to_state: str) -> str:
    from_norm = from_state.strip().upper()
    to_norm = to_state.strip().upper()

    if from_norm not in ALLOWED_TRANSITIONS:
        raise InvalidTransitionError(f"Unknown state: {from_state}")
    if to_norm not in ALLOWED_TRANSITIONS:
        raise InvalidTransitionError(f"Unknown state: {to_state}")
    if to_norm not in ALLOWED_TRANSITIONS[from_norm]:
        raise InvalidTransitionError(f"Invalid transition: {from_norm} -> {to_norm}")
    return to_norm

