"""FastAPI application entrypoint for PurpleBank backend.

The app exposes public read endpoints for guests and secured workflow endpoints
for authenticated users/admins.
"""

from urllib.parse import urlsplit

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.routers import admin_users, auth, public_sequences, sequence_requests

settings = get_settings()
frontend_origin = settings.frontend_origin


def build_cors_origins(origin: str) -> list[str]:
    """Build local-dev-friendly CORS allowlist from one configured origin.

    When developers switch between localhost and 127.0.0.1 in browser URL,
    both should work to avoid confusing "Failed to fetch" CORS errors.
    """

    parsed = urlsplit(origin)
    if parsed.hostname not in {"localhost", "127.0.0.1"}:
        return [origin]

    port = f":{parsed.port}" if parsed.port else ""
    scheme = parsed.scheme or "http"
    return [f"{scheme}://localhost{port}", f"{scheme}://127.0.0.1{port}"]

app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=build_cors_origins(frontend_origin),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    """Lightweight health endpoint for local checks."""

    return {"status": "ok"}


app.include_router(auth.router, prefix=settings.api_prefix)
app.include_router(public_sequences.router, prefix=settings.api_prefix)
app.include_router(sequence_requests.router, prefix=settings.api_prefix)
app.include_router(sequence_requests.admin_router, prefix=settings.api_prefix)
app.include_router(admin_users.router, prefix=settings.api_prefix)
