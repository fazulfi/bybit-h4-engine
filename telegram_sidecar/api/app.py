from __future__ import annotations

from fastapi import FastAPI

from telegram_sidecar.api.routes import router

app = FastAPI(title="Bybit Sidecar API")
app.include_router(router, prefix="/api/v1")
