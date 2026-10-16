from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from app import alpha_admin


class _FakeStore:
    """Notes each call the commands make."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        def record(*args: Any, **kwargs: Any) -> Any:
            self.calls.append((name, args, kwargs))
            return _RESULTS.get(name)

        return record


_RESULTS: dict[str, Any] = {
    "create_user": SimpleNamespace(username="tester.one", org_id="org-1"),
    "list_users": [SimpleNamespace(username="tester.one", is_active=True, documents_used=3, document_limit=20)],
    "set_base_currency": "USD",
}


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> _FakeStore:
    fake = _FakeStore()
    monkeypatch.setattr(alpha_admin.AlphaStore, "from_env", classmethod(lambda cls: fake))
    monkeypatch.setattr(alpha_admin, "generate_password", lambda: "generated-secret")
    return fake


@pytest.mark.parametrize(
    ("argv", "call"),
    [
        (["enable", "--username", "a"], ("set_user_active", ("a", True), {})),
        (["disable", "--username", "a"], ("set_user_active", ("a", False), {})),
        (["reset", "--username", "a"], ("reset_user_usage", ("a",), {})),
        (["org-add-member", "--org-id", "o", "--username", "a"], ("add_member", ("o", "a"), {"role": "member"})),
        (["org-remove-member", "--org-id", "o", "--username", "a"], ("remove_member", ("o", "a"), {})),
        (["org-set-currency", "--org-id", "o", "--currency", "usd"], ("set_base_currency", ("o", "usd"), {})),
    ],
)
def test_each_command_reaches_the_store(store: _FakeStore, argv: list[str], call: tuple[Any, ...]) -> None:
    assert alpha_admin.main(argv) == 0
    assert store.calls == [call]


def test_new_testers_get_a_generated_password_unless_one_is_given(
    store: _FakeStore, capsys: pytest.CaptureFixture[str]
) -> None:
    alpha_admin.main(["create", "--username", "tester.one"])
    alpha_admin.main(["create", "--username", "tester.two", "--password", "chosen-pass", "--document-limit", "5"])

    assert store.calls[0] == ("create_user", ("tester.one", "generated-secret"), {"document_limit": 20, "max_users": 10})
    assert store.calls[1][1] == ("tester.two", "chosen-pass")
    assert store.calls[1][2]["document_limit"] == 5
    assert "temporary password: generated-secret" in capsys.readouterr().out


def test_list_shows_each_tester_and_their_usage(store: _FakeStore, capsys: pytest.CaptureFixture[str]) -> None:
    alpha_admin.main(["list"])

    assert capsys.readouterr().out.strip() == "tester.one: active=True used=3/20"


def test_unknown_commands_are_refused(store: _FakeStore) -> None:
    with pytest.raises(SystemExit) as exit_info:
        alpha_admin.main(["delete-everything"])

    assert exit_info.value.code == 2
    assert store.calls == []
