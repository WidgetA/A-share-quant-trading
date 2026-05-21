"""Dashboard cookie bridge for the trading API shared secret."""

from fastapi import FastAPI, Request

TRADING_API_KEY_COOKIE = "trading_api_key"
TRADING_API_KEY_HEADER = b"x-api-key"


def install_trading_api_key_cookie_middleware(app: FastAPI) -> None:
    """Install middleware that reuses the persisted Trading API Key in the dashboard.

    The `/api/trading/*` routes already trust `X-API-Key`. Browser sessions can
    lose `localStorage`, so this middleware stores the configured key as an
    HttpOnly same-site cookie and copies it into the header for same-origin
    requests that did not explicitly send one.
    """

    @app.middleware("http")
    async def trading_api_key_cookie_middleware(request: Request, call_next):
        cookie_key = request.cookies.get(TRADING_API_KEY_COOKIE, "")
        if cookie_key:
            headers = list(request.scope.get("headers") or [])
            has_api_key = any(name.lower() == TRADING_API_KEY_HEADER for name, _ in headers)
            if not has_api_key:
                headers.append((TRADING_API_KEY_HEADER, cookie_key.encode("latin-1")))
                request.scope["headers"] = headers

        response = await call_next(request)

        from src.common.config import get_trading_api_key

        configured_key = get_trading_api_key()
        if configured_key:
            response.set_cookie(
                key=TRADING_API_KEY_COOKIE,
                value=configured_key,
                max_age=60 * 60 * 24 * 365,
                httponly=True,
                samesite="strict",
            )
        return response
