from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .revenue_tests import cutoff_testing, mus_sampling, target_testing

app = FastAPI(title="Revenue Testing SaaS API", version="0.1.0")

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

# Mount the frontend folder so any static assets can be served.
app.mount("/frontend", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")


@app.get("/")
def root() -> FileResponse:
    return FileResponse(str(FRONTEND_DIR / "index.html"))


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/revenue-tests/target")
def run_target_testing() -> dict:
    return {
        "status": "not_implemented",
        "message": "target_testing now requires a GL DataFrame, performance_materiality, and risk_level. Wire this to an upload/ingestion endpoint next.",
    }


@app.get("/revenue-tests/mus")
def run_mus_sampling() -> dict:
    return {
        "status": "not_implemented",
        "message": "mus_sampling now requires a GL DataFrame plus PM/risk inputs. Wire this to an upload/ingestion endpoint next.",
    }


@app.get("/revenue-tests/cutoff")
def run_cutoff_testing() -> dict:
    return {"result": cutoff_testing()}

