from __future__ import annotations

import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock

from ingest_folder import run_ingest
from answer_question_set import run_question_set

from .schemas import ProcessDocumentsRequest, QuestionInput, RunQuestionsRequest


LATEST_RESULTS = {
    "process_documents": None,
    "question_run": None,
}
PROCESS_PROGRESS_LOCK = Lock()
PROCESS_PROGRESS = {
    "status": "idle",
    "phase": "idle",
    "root": None,
    "name_contains": None,
    "total_documents": 0,
    "completed_documents": 0,
    "current_document": None,
    "current_path": None,
    "current_status": None,
    "error": None,
    "summary": None,
    "started_at": None,
    "finished_at": None,
}
QA_PROGRESS_LOCK = Lock()
QA_CANCEL_EVENT = Event()
QA_PROGRESS = {
    "status": "idle",
    "cancel_requested": False,
    "current_question_index": 0,
    "total_questions": 0,
    "current_question_text": None,
    "current_document_index": 0,
    "total_documents": 0,
    "current_document_name": None,
    "completed_pairs": 0,
    "total_pairs": 0,
    "percent_complete": 0,
    "message": None,
    "error": None,
    "started_at": None,
    "finished_at": None,
}

DATE_PARSE_FORMATS = (
    "%B %d, %Y",
    "%b %d, %Y",
    "%B %d %Y",
    "%b %d %Y",
    "%d %B %Y",
    "%d %b %Y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%m.%d.%Y",
)
DATE_SEARCH_PATTERNS = (
    re.compile(
        r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|"
        r"dec(?:ember)?)\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s+\d{4}\b",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"\b\d{1,2}\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|"
        r"dec(?:ember)?)\s+\d{4}\b",
        flags=re.IGNORECASE,
    ),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b\d{4}/\d{2}/\d{2}\b"),
    re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b"),
)


def slugify_question_id(value: str) -> str:
    """Turn a user-facing question name into a stable snake-like identifier."""
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "question"


def utc_now_iso() -> str:
    """Return a compact UTC timestamp for in-memory status payloads."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def update_process_progress(payload: dict) -> None:
    """Merge a progress update into the in-memory process status payload."""
    with PROCESS_PROGRESS_LOCK:
        PROCESS_PROGRESS.update(payload)
        if payload.get("status") == "running" and PROCESS_PROGRESS.get("started_at") is None:
            PROCESS_PROGRESS["started_at"] = utc_now_iso()
        if payload.get("status") in {"completed", "error"}:
            PROCESS_PROGRESS["finished_at"] = utc_now_iso()


def start_process_progress(request: ProcessDocumentsRequest) -> None:
    """Initialize the in-memory process progress state for a new ingest run."""
    with PROCESS_PROGRESS_LOCK:
        PROCESS_PROGRESS.clear()
        PROCESS_PROGRESS.update(
            {
                "status": "running",
                "phase": "starting",
                "root": request.root,
                "name_contains": request.name_contains,
                "total_documents": 0,
                "completed_documents": 0,
                "current_document": None,
                "current_path": None,
                "current_status": None,
                "error": None,
                "summary": None,
                "started_at": utc_now_iso(),
                "finished_at": None,
            }
        )


def update_qa_progress(payload: dict) -> None:
    """Merge a progress update into the in-memory QA status payload."""
    with QA_PROGRESS_LOCK:
        QA_PROGRESS.update(payload)
        if payload.get("status") == "running" and QA_PROGRESS.get("started_at") is None:
            QA_PROGRESS["started_at"] = utc_now_iso()
        if payload.get("status") in {"completed", "cancelled", "error"}:
            QA_PROGRESS["finished_at"] = utc_now_iso()


def start_qa_progress(request: RunQuestionsRequest) -> None:
    """Initialize the in-memory QA progress state for a new question run."""
    QA_CANCEL_EVENT.clear()
    with QA_PROGRESS_LOCK:
        QA_PROGRESS.clear()
        QA_PROGRESS.update(
            {
                "status": "running",
                "cancel_requested": False,
                "current_question_index": 0,
                "total_questions": len(request.questions),
                "current_question_text": None,
                "current_document_index": 0,
                "total_documents": 0,
                "current_document_name": None,
                "completed_pairs": 0,
                "total_pairs": 0,
                "percent_complete": 0,
                "message": "Preparing question run.",
                "error": None,
                "started_at": utc_now_iso(),
                "finished_at": None,
            }
        )


def is_qa_cancel_requested() -> bool:
    """Return whether the current question run has been asked to stop."""
    return QA_CANCEL_EVENT.is_set()


def request_qa_cancel() -> dict:
    """Mark the current question run for cooperative cancellation."""
    QA_CANCEL_EVENT.set()
    with QA_PROGRESS_LOCK:
        if QA_PROGRESS.get("status") in {"running", "cancel_requested"}:
            QA_PROGRESS.update(
                {
                    "status": "cancel_requested",
                    "cancel_requested": True,
                    "message": "Cancellation requested. Waiting for the current question to finish.",
                }
            )
        return deepcopy(QA_PROGRESS)


def strip_date_ordinals(text: str) -> str:
    """Convert forms like '1st' or '22nd' into plain day numbers."""
    return re.sub(r"\b(\d{1,2})(st|nd|rd|th)\b", r"\1", text, flags=re.IGNORECASE)


def clean_date_candidate(value: str) -> str:
    """Normalize spacing and punctuation before attempting date parsing."""
    cleaned = strip_date_ordinals(value.strip())
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\s*,\s*", ", ", cleaned)
    cleaned = re.sub(r"\s+\.", ".", cleaned)
    return cleaned.strip(" .")


def format_human_date(parsed_date: datetime) -> str:
    """Render parsed dates consistently for display."""
    return f"{parsed_date.strftime('%B')} {parsed_date.day}, {parsed_date.year}"


def try_parse_date_candidate(candidate: str) -> tuple[str | None, str | None]:
    """Attempt to parse one candidate string into display and sortable forms."""
    cleaned = clean_date_candidate(candidate)
    if not cleaned:
        return None, None

    variants = [cleaned]
    title_variant = cleaned.title()
    if title_variant not in variants:
        variants.append(title_variant)

    for variant in variants:
        for date_format in DATE_PARSE_FORMATS:
            try:
                parsed = datetime.strptime(variant, date_format)
            except ValueError:
                continue
            return format_human_date(parsed), parsed.strftime("%Y-%m-%d")

    return None, None


def normalize_date_answer(value: str | None) -> tuple[str | None, str | None]:
    """
    Convert date-like answers into a consistent 'Month Day, Year' display form.

    If the whole answer is not directly parseable, search for an embedded date
    substring so answers like 'Effective April 1st, 2016' still normalize cleanly.
    """
    if not isinstance(value, str):
        return value, None

    cleaned = value.strip()
    if not cleaned or cleaned.upper() in {"ERROR", "UNCLEAR"}:
        return cleaned, None

    normalized_answer, sort_value = try_parse_date_candidate(cleaned)
    if normalized_answer:
        return normalized_answer, sort_value

    searchable = strip_date_ordinals(cleaned)
    for pattern in DATE_SEARCH_PATTERNS:
        match = pattern.search(searchable)
        if not match:
            continue
        normalized_answer, sort_value = try_parse_date_candidate(match.group(0))
        if normalized_answer:
            return normalized_answer, sort_value

    return cleaned, None


def normalize_long_row(long_row: dict, question: dict | None) -> dict:
    """Apply UI-facing normalization that depends on the question metadata."""
    normalized = dict(long_row)

    if question and question.get("answer_type") == "date":
        normalized_answer, sort_value = normalize_date_answer(normalized.get("answer"))
        normalized["answer"] = normalized_answer
        normalized["normalized_sort_value"] = sort_value

    return normalized


def normalize_questions(question_inputs: list[QuestionInput]) -> list[dict]:
    """
    Convert API question objects into the existing question-set schema while
    preserving a user-friendly display name for the frontend.
    """
    if not question_inputs:
        raise ValueError("At least one question is required.")

    normalized: list[dict] = []
    seen_ids: set[str] = set()

    for index, question in enumerate(question_inputs, start=1):
        base_id = question.question_id.strip() if question.question_id else slugify_question_id(question.question_name)
        question_id = base_id
        suffix = 2
        while question_id in seen_ids:
            question_id = f"{base_id}_{suffix}"
            suffix += 1

        seen_ids.add(question_id)
        normalized.append(
            {
                "question_name": question.question_name.strip(),
                "question_id": question_id,
                "question_text": question.question_text.strip(),
                "answer_type": question.answer_type,
                "unit": question.unit.strip() if isinstance(question.unit, str) and question.unit.strip() else None,
                "description": (
                    question.description.strip()
                    if isinstance(question.description, str) and question.description.strip()
                    else None
                ),
                "ranking_direction": question.ranking_direction,
            }
        )

    return normalized


def build_process_response(result: dict) -> dict:
    """Shape the ingest result for the API."""
    return {
        "summary": result["summary"],
        "records": result["records"],
        "log_path": result.get("log_path"),
        "root": result["root"],
        "name_contains": result["name_contains"],
        "force": result["force"],
        "dry_run": result["dry_run"],
    }


def build_wide_results(matched_docs: list[dict], questions: list[dict], long_rows: list[dict]) -> list[dict]:
    """
    Build a nested wide representation optimized for the UI while preserving
    all answer metadata per question.
    """
    rows_by_pair = {
        (row["doc_id"], row["question_id"]): row
        for row in long_rows
    }

    wide_results: list[dict] = []
    for doc in matched_docs:
        row = {
            "doc_id": doc["doc_id"],
            "filename": doc["filename"],
            "source_path": doc["source_path"],
            "source_bytes": doc.get("source_bytes"),
            "source_mtime": doc.get("source_mtime"),
            "answers": {},
        }

        for question in questions:
            long_row = rows_by_pair.get((doc["doc_id"], question["question_id"]), {})
            row["answers"][question["question_id"]] = {
                "answer": long_row.get("answer"),
                "value": long_row.get("value"),
                "unit": long_row.get("unit"),
                "confidence": long_row.get("confidence"),
                "citation_pages": long_row.get("citation_pages", []),
                "supporting_excerpt": long_row.get("supporting_excerpt", ""),
                "quote": long_row.get("quote", ""),
                "notes": long_row.get("notes", ""),
                "normalized_sort_value": long_row.get("normalized_sort_value"),
            }

        wide_results.append(row)

    return wide_results


def get_ranked_question(questions: list[dict]) -> dict | None:
    """Return the first question configured with a supported ranking direction."""
    for question in questions:
        if question.get("ranking_direction") in {"ascending", "descending"}:
            return question
    return None


def get_row_sort_value(row: dict, question_id: str):
    """
    Extract the best available sort value for one wide-result row.

    Prefer machine-friendly normalized values when present, then fall back to the
    displayed answer text. Missing values sort last.
    """
    answer_cell = row.get("answers", {}).get(question_id, {})
    for candidate in (
        answer_cell.get("normalized_sort_value"),
        answer_cell.get("answer"),
    ):
        if candidate is None:
            continue
        if isinstance(candidate, str):
            stripped = candidate.strip()
            if stripped:
                return stripped.lower()
            continue
        return candidate
    return None


def sort_wide_results(wide_results: list[dict], questions: list[dict]) -> list[dict]:
    """
    Sort rows using only the first question that declares a ranking direction.

    The sort stays stable, and rows with missing values are kept at the end.
    """
    ranked_question = get_ranked_question(questions)
    if not ranked_question:
        return wide_results

    question_id = ranked_question["question_id"]
    reverse = ranked_question["ranking_direction"] == "descending"

    present_rows: list[dict] = []
    missing_rows: list[dict] = []
    for row in wide_results:
        sort_value = get_row_sort_value(row, question_id)
        if sort_value is None:
            missing_rows.append(row)
            continue

        sortable_row = dict(row)
        sortable_row["_sort_value"] = sort_value
        present_rows.append(sortable_row)

    present_rows.sort(key=lambda row: row["_sort_value"], reverse=reverse)

    sorted_rows: list[dict] = []
    for row in present_rows:
        cleaned_row = dict(row)
        cleaned_row.pop("_sort_value", None)
        sorted_rows.append(cleaned_row)

    sorted_rows.extend(missing_rows)
    return sorted_rows


def build_question_run_response(run_result: dict, questions: list[dict]) -> dict:
    """Shape the question-set result for the API and frontend table."""
    questions_by_id = {
        question["question_id"]: question
        for question in questions
    }
    normalized_long_results = [
        normalize_long_row(row, questions_by_id.get(row["question_id"]))
        for row in run_result["results"]
    ]
    wide_results = build_wide_results(run_result["matched_docs"], questions, normalized_long_results)
    wide_results = sort_wide_results(wide_results, questions)

    response = {
        "summary": run_result["summary"],
        "questions": questions,
        "wide_results": wide_results,
        "long_results": normalized_long_results,
        "output_paths": {
            "long_csv": run_result["summary"]["long_csv_path"],
            "long_json": run_result["summary"]["long_json_path"],
            "wide_csv": run_result["summary"]["wide_csv_path"],
            "wide_json": run_result["summary"]["wide_json_path"],
        },
        "source_path_filter": run_result["source_path_filter"],
    }
    return response


def process_documents_service(request: ProcessDocumentsRequest) -> dict:
    """Run the ingest pipeline from the API."""
    start_process_progress(request)
    try:
        result = run_ingest(
            root=Path(request.root),
            name_contains=request.name_contains,
            force=request.force,
            dry_run=request.dry_run,
            verbose=False,
            write_log=True,
            progress_callback=update_process_progress,
        )
    except Exception as exc:
        update_process_progress(
            {
                "status": "error",
                "phase": "error",
                "error": str(exc),
            }
        )
        raise
    response = build_process_response(result)
    LATEST_RESULTS["process_documents"] = deepcopy(response)
    return response


def run_questions_service(request: RunQuestionsRequest) -> dict:
    """Run the multi-question pipeline from the API."""
    questions = normalize_questions(request.questions)
    start_qa_progress(request)
    try:
        run_result = run_question_set(
            source_path_contains=request.source_path_contains,
            questions=questions,
            doc_ids=request.doc_ids,
            questions_file=None,
            limit_docs=request.limit_docs,
            limit_questions=request.limit_questions,
            verbose=False,
            output_prefix=request.output_prefix,
            write_outputs_flag=True,
            log_progress=False,
            progress_callback=update_qa_progress,
            cancel_requested=is_qa_cancel_requested,
        )
    except Exception as exc:
        update_qa_progress(
            {
                "status": "error",
                "message": "Question run failed.",
                "error": str(exc),
            }
        )
        raise
    response = build_question_run_response(run_result, questions)
    LATEST_RESULTS["question_run"] = deepcopy(response)
    return response


def get_latest_results() -> dict:
    """Return the latest in-memory process and question-run results."""
    return deepcopy(LATEST_RESULTS)


def get_process_progress() -> dict:
    """Return the latest in-memory ingest progress snapshot."""
    with PROCESS_PROGRESS_LOCK:
        return deepcopy(PROCESS_PROGRESS)


def get_qa_progress() -> dict:
    """Return the latest in-memory question-run progress snapshot."""
    with QA_PROGRESS_LOCK:
        return deepcopy(QA_PROGRESS)
