import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="FlyerDeals API", version="0.1.0")

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://jonathan-pearce.github.io")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
