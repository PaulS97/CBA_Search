from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware

from .schemas import ProcessDocumentsRequest, RunQuestionsRequest
from .services import (
    get_latest_results,
    get_process_progress,
    get_qa_progress,
    process_documents_service,
    request_qa_cancel,
    run_questions_service,
)


app = FastAPI(
    title="CBA Search Local RAG",
    version="0.1.0",
    description="Local-first legal document processing and question answering for indexed CBAs.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def healthcheck() -> dict:
    return {"ok": True}


@app.post("/process-documents")
async def process_documents(request: ProcessDocumentsRequest) -> dict:
    try:
        return await run_in_threadpool(process_documents_service, request)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/run-questions")
async def run_questions(request: RunQuestionsRequest) -> dict:
    try:
        return await run_in_threadpool(run_questions_service, request)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/run-questions/cancel")
def cancel_run_questions() -> dict:
    return request_qa_cancel()


@app.get("/latest-results")
def latest_results() -> dict:
    return get_latest_results()


@app.get("/process-progress")
def process_progress() -> dict:
    return get_process_progress()


@app.get("/qa-progress")
def qa_progress() -> dict:
    return get_qa_progress()
