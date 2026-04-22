from fastapi import FastAPI

from .revenue_tests import cutoff_testing, mus_sampling, target_testing

app = FastAPI(title="Revenue Testing SaaS API", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/revenue-tests/target")
def run_target_testing() -> dict:
    return {"result": target_testing()}


@app.get("/revenue-tests/mus")
def run_mus_sampling() -> dict:
    return {"result": mus_sampling()}


@app.get("/revenue-tests/cutoff")
def run_cutoff_testing() -> dict:
    return {"result": cutoff_testing()}

