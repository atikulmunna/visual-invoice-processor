from __future__ import annotations

import os

import pytest

from app.config import load_aws_parameter_secrets


class _FakeSsmClient:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.calls: list[dict[str, object]] = []

    def get_parameters(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        names = kwargs["Names"]
        assert isinstance(names, list)
        return {
            "Parameters": [
                {"Name": name, "Value": self.values[name]}
                for name in names
                if name in self.values
            ]
        }


def test_load_aws_parameter_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.setenv("POSTGRES_PARAMETER_NAME", "/app/postgres")
    monkeypatch.setenv("OPENROUTER_PARAMETER_NAME", "/app/openrouter")
    client = _FakeSsmClient({"/app/postgres": "postgresql://example", "/app/openrouter": "key"})

    load_aws_parameter_secrets(client)

    assert os.environ["POSTGRES_DSN"] == "postgresql://example"
    assert os.environ["OPENROUTER_API_KEY"] == "key"
    assert client.calls == [
        {"Names": ["/app/openrouter", "/app/postgres"], "WithDecryption": True}
    ]


def test_load_aws_parameter_secrets_reports_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.setenv("POSTGRES_PARAMETER_NAME", "/app/postgres")
    monkeypatch.delenv("OPENROUTER_PARAMETER_NAME", raising=False)

    with pytest.raises(RuntimeError, match="/app/postgres"):
        load_aws_parameter_secrets(_FakeSsmClient({}))
