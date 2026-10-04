"""The browser session cookie, shared by every route that signs people in or out."""

from __future__ import annotations

from fastapi import Response

SESSION_COOKIE_NAME = "invoice_alpha_session"
SESSION_MAX_AGE_SECONDS = 7 * 24 * 60 * 60


def set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE_NAME, path="/", secure=True, samesite="lax")
