#!/usr/bin/env python3
# Usage:
#   python answer_question_set.py --source-path-contains "/Users/paulseham/Documents/CBA_Search/Industry Data Project/Air Canada" --questions-file "/Users/paulseham/Documents/CBA_Search/questions.example.json"
#   python answer_question_set.py --source-path-contains "/path/subtree" --questions-file "/path/questions.json" --output-prefix "air_canada_question_set"

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Callable

try:
    from answer_folder import (
        CACHE_ROOT,
        OUTPUTS_DIR,
        discover_indexed_docs,
        matches_source_filter,
    )
except ImportError as exc:
    CACHE_ROOT = Path(__file__).resolve().parent / "_rag_cache"
    OUTPUTS_DIR = Path(__file__).resolve().parent / "outputs"
    discover_indexed_docs = None
    matches_source_filter = None
    DISCOVERY_IMPORT_ERROR = exc
else:
    DISCOVERY_IMPORT_ERROR = None

try:
    from answer_with_rag import answer_one_document, prepare_answering_runtime
except ImportError as exc:
    answer_one_document = None
    prepare_answering_runtime = None
    ANSWER_IMPORT_ERROR = exc
else:
    ANSWER_IMPORT_ERROR = None


VALID_ANSWER_TYPES = {"number", "boolean", "true_false", "date", "string_short", "short_answer"}
VALID_RANKING_DIRECTIONS = {"ascending", "descending", None}
LONG_FIELDNAMES = [
    "doc_id",
    "filename",
    "source_path",
    "source_bytes",
    "source_mtime",
    "question_id",
    "question_text",
    "answer_type",
    "configured_unit",
    "ranking_direction",
    "answer",
    "value",
    "unit",
    "quote",
    "supporting_excerpt",
    "citation_pages",
    "confidence",
    "notes",
]


def parse_args() -> argparse.Namespace:
    """Parse question-set answering options."""
    parser = argparse.ArgumentParser(
        description="Run the one-document RAG answer pipeline across multiple documents and multiple questions."
    )
    parser.add_argument(
        "--source-path-contains",
        required=True,
        help="Only include cached documents whose source_path contains this substring.",
    )
    parser.add_argument(
        "--questions-file",
        required=True,
        help="Path to the JSON file containing the question objects.",
    )
    parser.add_argument(
        "--limit-docs",
        type=int,
        help="Optional maximum number of matched documents to process after sorting.",
    )
    parser.add_argument(
        "--limit-questions",
        type=int,
        help="Optional maximum number of questions to process after validation.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print extra logging about skipped cache directories.",
    )
    parser.add_argument(
        "--output-prefix",
        default="question_set_results",
        help="Prefix for outputs/<prefix>_long.* and outputs/<prefix>_wide.*",
    )
    return parser.parse_args()


def ensure_reusable_pipeline() -> None:
    """Fail fast if the discovery or single-document answer helpers are unavailable."""
    errors: list[str] = []
    if discover_indexed_docs is None or matches_source_filter is None:
        errors.append(f"answer_folder.py import failed: {DISCOVERY_IMPORT_ERROR}")
    if answer_one_document is None or prepare_answering_runtime is None:
        errors.append(f"answer_with_rag.py import failed: {ANSWER_IMPORT_ERROR}")

    if errors:
        raise RuntimeError("Required imports failed:\n" + "\n".join(errors))


def write_json(path: Path, payload: object) -> None:
    """Write JSON with stable formatting for inspection and debugging."""
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def load_questions(questions_file: Path) -> list[dict]:
    """Load and validate the question set JSON file."""
    if not questions_file.exists():
        raise FileNotFoundError(f"Questions file not found: {questions_file}")
    if not questions_file.is_file():
        raise FileNotFoundError(f"Questions path is not a file: {questions_file}")

    try:
        payload = json.loads(questions_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to read {questions_file}: {exc}") from exc

    return validate_questions(payload, questions_file)


def validate_questions(payload: object, questions_file: Path) -> list[dict]:
    """Validate the question objects and normalize optional fields."""
    if not isinstance(payload, list):
        raise RuntimeError(f"{questions_file} must contain a JSON array of question objects.")

    validated: list[dict] = []
    seen_ids: set[str] = set()

    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise RuntimeError(f"Question entry {index} in {questions_file} is not a JSON object.")

        question_id = item.get("question_id")
        question_text = item.get("question_text")
        answer_type = item.get("answer_type")
        unit = item.get("unit")
        description = item.get("description")
        ranking_direction = item.get("ranking_direction")

        if not isinstance(question_id, str) or not question_id.strip():
            raise RuntimeError(f"Question entry {index} in {questions_file} has an invalid question_id.")
        if question_id in seen_ids:
            raise RuntimeError(f"Duplicate question_id in {questions_file}: {question_id}")
        if not isinstance(question_text, str) or not question_text.strip():
            raise RuntimeError(f"Question {question_id} has an invalid question_text.")
        if answer_type not in VALID_ANSWER_TYPES:
            raise RuntimeError(
                f"Question {question_id} has invalid answer_type {answer_type!r}. "
                f"Expected one of {sorted(VALID_ANSWER_TYPES)}."
            )
        if unit is not None and not isinstance(unit, str):
            raise RuntimeError(f"Question {question_id} has invalid unit {unit!r}.")
        if description is not None and not isinstance(description, str):
            raise RuntimeError(f"Question {question_id} has invalid description {description!r}.")
        if ranking_direction not in VALID_RANKING_DIRECTIONS:
            raise RuntimeError(
                f"Question {question_id} has invalid ranking_direction {ranking_direction!r}. "
                "Expected 'ascending', 'descending', or null."
            )

        validated.append(
            {
                "question_id": question_id,
                "question_text": question_text.strip(),
                "answer_type": answer_type,
                "unit": unit.strip() if isinstance(unit, str) else None,
                "description": description.strip() if isinstance(description, str) else None,
                "ranking_direction": ranking_direction,
            }
        )
        seen_ids.add(question_id)

    return validated


def serialize_citation_pages(value: list[int]) -> str:
    """Convert citation page arrays into a CSV-friendly semicolon-separated string."""
    return ";".join(str(page) for page in value)


def make_error_result(doc_meta: dict, question_obj: dict, error: str) -> dict:
    """Shape one failed document-question pair into the normalized answer schema."""
    return {
        "doc_id": doc_meta["doc_id"],
        "question": question_obj["question_text"],
        "answer": "ERROR",
        "value": None,
        "unit": question_obj.get("unit"),
        "quote": "",
        "supporting_excerpt": "",
        "citation_pages": [],
        "confidence": "low",
        "notes": error,
    }


def build_long_row(doc_meta: dict, question_obj: dict, result: dict) -> dict:
    """Flatten one document-question answer into the canonical long-format row."""
    return {
        "doc_id": doc_meta["doc_id"],
        "filename": doc_meta["filename"],
        "source_path": doc_meta["source_path"],
        "source_bytes": doc_meta.get("source_bytes"),
        "source_mtime": doc_meta.get("source_mtime"),
        "question_id": question_obj["question_id"],
        "question_text": question_obj["question_text"],
        "answer_type": question_obj["answer_type"],
        "configured_unit": question_obj.get("unit"),
        "ranking_direction": question_obj.get("ranking_direction"),
        "answer": result.get("answer"),
        "value": result.get("value"),
        "unit": result.get("unit"),
        "quote": result.get("quote"),
        "supporting_excerpt": result.get("supporting_excerpt"),
        "citation_pages": result.get("citation_pages", []),
        "confidence": result.get("confidence"),
        "notes": result.get("notes"),
    }


def normalize_csv_value(value):
    """Keep CSV output blank for missing values instead of writing 'None'."""
    if value is None:
        return ""
    return value


def long_row_to_csv_row(long_row: dict) -> dict:
    """Convert the canonical long row to a CSV-safe representation."""
    csv_row = dict(long_row)
    csv_row["citation_pages"] = serialize_citation_pages(long_row.get("citation_pages", []))
    for key, value in list(csv_row.items()):
        csv_row[key] = normalize_csv_value(value)
    return csv_row


def pivot_long_to_wide(long_rows: list[dict], questions: list[dict], docs: list[dict]) -> list[dict]:
    """
    Build one wide row per document, preserving the question-file ordering for
    all question-specific output columns.
    """
    wide_rows: list[dict] = []
    rows_by_pair = {
        (row["doc_id"], row["question_id"]): row
        for row in long_rows
    }

    for doc_meta in docs:
        wide_row = {
            "doc_id": doc_meta["doc_id"],
            "filename": doc_meta["filename"],
            "source_path": doc_meta["source_path"],
            "source_bytes": normalize_csv_value(doc_meta.get("source_bytes")),
            "source_mtime": normalize_csv_value(doc_meta.get("source_mtime")),
        }

        for question in questions:
            prefix = question["question_id"]
            row = rows_by_pair.get((doc_meta["doc_id"], prefix), {})
            wide_row[f"{prefix}__answer"] = normalize_csv_value(row.get("answer"))
            wide_row[f"{prefix}__value"] = normalize_csv_value(row.get("value"))
            wide_row[f"{prefix}__unit"] = normalize_csv_value(row.get("unit"))
            wide_row[f"{prefix}__confidence"] = normalize_csv_value(row.get("confidence"))
            wide_row[f"{prefix}__citation_pages"] = serialize_citation_pages(
                row.get("citation_pages", [])
            )
            wide_row[f"{prefix}__supporting_excerpt"] = normalize_csv_value(
                row.get("supporting_excerpt")
            )
            wide_row[f"{prefix}__notes"] = normalize_csv_value(row.get("notes"))

        wide_rows.append(wide_row)

    return wide_rows


def write_csv(path: Path, rows: list[dict]) -> None:
    """Write CSV rows using the key order from the first row when available."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        if rows:
            fieldnames = list(rows[0].keys())
        else:
            fieldnames = []

        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            for row in rows:
                writer.writerow(row)


def write_empty_csv(path: Path, fieldnames: list[str]) -> None:
    """Write a CSV with just a header row when there are no data rows yet."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()


def print_pair_result(doc_meta: dict, question_obj: dict, result: dict) -> None:
    """Print the requested concise per-pair progress line."""
    filename = doc_meta["filename"]
    question_id = question_obj["question_id"]
    if result["answer"] == "ERROR":
        print(f"[ERROR] {filename} | {question_id} -> {result['notes']}")
    else:
        print(f"[OK] {filename} | {question_id} -> {result['answer']}")


def print_summary(
    matched_docs: int,
    questions_loaded: int,
    attempted_pairs: int,
    succeeded: int,
    failed: int,
    long_csv_path: Path,
    long_json_path: Path,
    wide_csv_path: Path,
) -> None:
    """Print the final run summary."""
    print("Summary:")
    print(f"  matched_docs: {matched_docs}")
    print(f"  questions_loaded: {questions_loaded}")
    print(f"  attempted_pairs: {attempted_pairs}")
    print(f"  succeeded: {succeeded}")
    print(f"  failed: {failed}")
    print(f"  long_csv_path: {long_csv_path}")
    print(f"  long_json_path: {long_json_path}")
    print(f"  wide_csv_path: {wide_csv_path}")


def build_wide_fieldnames(questions: list[dict]) -> list[str]:
    """Generate the wide-export columns in question-file order."""
    fieldnames = ["doc_id", "filename", "source_path", "source_bytes", "source_mtime"]
    for question in questions:
        prefix = question["question_id"]
        fieldnames.extend(
            [
                f"{prefix}__answer",
                f"{prefix}__value",
                f"{prefix}__unit",
                f"{prefix}__confidence",
                f"{prefix}__citation_pages",
                f"{prefix}__supporting_excerpt",
                f"{prefix}__notes",
            ]
        )
    return fieldnames


def emit_progress(progress_callback: Callable[[dict], None] | None, payload: dict) -> None:
    """Send one QA progress update when a callback is provided."""
    if progress_callback is not None:
        progress_callback(payload)


def is_cancel_requested(cancel_requested: Callable[[], bool] | None) -> bool:
    """Return True when a cooperative cancel callback says the run should stop."""
    return bool(cancel_requested and cancel_requested())


def run_question_set(
    source_path_contains: str | None,
    questions: list[dict],
    *,
    doc_ids: list[str] | None = None,
    questions_file: Path | None = None,
    limit_docs: int | None = None,
    limit_questions: int | None = None,
    verbose: bool = False,
    output_prefix: str = "question_set_results",
    write_outputs_flag: bool = True,
    log_progress: bool = False,
    progress_callback: Callable[[dict], None] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> dict:
    """
    Run the document-question grid using the reusable one-document answer
    pipeline and return both long-format and wide-format results.
    """
    ensure_reusable_pipeline()

    validated_questions = validate_questions(questions, questions_file or Path("<in-memory-questions>"))
    if limit_questions is not None:
        validated_questions = validated_questions[: max(limit_questions, 0)]

    indexed_docs = discover_indexed_docs(CACHE_ROOT, verbose=verbose)
    if doc_ids is not None:
        requested_doc_ids = {
            doc_id.strip()
            for doc_id in doc_ids
            if isinstance(doc_id, str) and doc_id.strip()
        }
        matched_docs = [
            doc for doc in indexed_docs if doc["doc_id"] in requested_doc_ids
        ]
    else:
        if not source_path_contains:
            raise RuntimeError("Either source_path_contains or doc_ids must be provided.")
        matched_docs = [
            doc for doc in indexed_docs if matches_source_filter(doc["source_path"], source_path_contains)
        ]
    matched_docs.sort(key=lambda doc: (doc["source_path"], doc["doc_id"]))

    if limit_docs is not None:
        matched_docs = matched_docs[: max(limit_docs, 0)]

    total_questions = len(validated_questions)
    total_documents = len(matched_docs)
    total_pairs = total_documents * total_questions

    emit_progress(
        progress_callback,
        {
            "status": "running",
            "message": "Preparing question run.",
            "current_question_index": 0,
            "total_questions": total_questions,
            "current_question_text": None,
            "current_document_index": 0,
            "total_documents": total_documents,
            "current_document_name": None,
            "completed_pairs": 0,
            "total_pairs": total_pairs,
            "percent_complete": 0,
        },
    )

    runtime = None
    if matched_docs and validated_questions:
        if is_cancel_requested(cancel_requested):
            emit_progress(
                progress_callback,
                {
                    "status": "cancelled",
                    "message": "Question run cancelled before answering began.",
                    "current_question_index": 0,
                    "total_questions": total_questions,
                    "current_question_text": None,
                    "current_document_index": 0,
                    "total_documents": total_documents,
                    "current_document_name": None,
                    "completed_pairs": 0,
                    "total_pairs": total_pairs,
                    "percent_complete": 0,
                },
            )
        else:
            runtime = prepare_answering_runtime()

    long_rows: list[dict] = []
    succeeded = 0
    failed = 0
    cancelled = is_cancel_requested(cancel_requested)

    for document_index, doc_meta in enumerate(matched_docs, start=1):
        if cancelled:
            break
        for question_index, question_obj in enumerate(validated_questions, start=1):
            if is_cancel_requested(cancel_requested):
                cancelled = True
                break
            completed_pairs = len(long_rows)
            percent_complete = (
                round((completed_pairs / total_pairs) * 100, 1)
                if total_pairs > 0
                else 0
            )
            emit_progress(
                progress_callback,
                {
                    "status": "running",
                    "message": (
                        f"Running question {question_index} of {total_questions} "
                        f"for {doc_meta['filename']}."
                    ),
                    "current_question_index": question_index,
                    "total_questions": total_questions,
                    "current_question_text": question_obj["question_text"],
                    "current_document_index": document_index,
                    "total_documents": total_documents,
                    "current_document_name": doc_meta["filename"],
                    "completed_pairs": completed_pairs,
                    "total_pairs": total_pairs,
                    "percent_complete": percent_complete,
                },
            )
            try:
                answer_payload = answer_one_document(
                    doc_meta["doc_id"],
                    question_obj["question_text"],
                    answer_type=question_obj.get("answer_type"),
                    description=question_obj.get("description"),
                    runtime=runtime,
                )
                result = answer_payload["result"]
                succeeded += 1
            except Exception as exc:
                result = make_error_result(doc_meta, question_obj, str(exc))
                failed += 1

            if log_progress:
                print_pair_result(doc_meta, question_obj, result)
            long_rows.append(build_long_row(doc_meta, question_obj, result))

            completed_pairs = len(long_rows)
            cancellation_pending = is_cancel_requested(cancel_requested)
            percent_complete = (
                round((completed_pairs / total_pairs) * 100, 1)
                if total_pairs > 0
                else 100
            )
            emit_progress(
                progress_callback,
                {
                    "status": "cancel_requested" if cancellation_pending else "running",
                    "message": (
                        "Cancellation requested. Finishing current question run step."
                        if cancellation_pending
                        else (
                            f"Completed {completed_pairs} of {total_pairs} "
                            f"document-question pairs."
                        )
                    ),
                    "current_question_index": question_index,
                    "total_questions": total_questions,
                    "current_question_text": question_obj["question_text"],
                    "current_document_index": document_index,
                    "total_documents": total_documents,
                    "current_document_name": doc_meta["filename"],
                    "completed_pairs": completed_pairs,
                    "total_pairs": total_pairs,
                    "percent_complete": percent_complete,
                },
            )
            if cancellation_pending:
                cancelled = True
                break

    long_csv_rows = [long_row_to_csv_row(row) for row in long_rows]
    wide_rows = pivot_long_to_wide(long_rows, validated_questions, matched_docs)

    long_csv_path = OUTPUTS_DIR / f"{output_prefix}_long.csv"
    long_json_path = OUTPUTS_DIR / f"{output_prefix}_long.json"
    wide_csv_path = OUTPUTS_DIR / f"{output_prefix}_wide.csv"
    wide_json_path = OUTPUTS_DIR / f"{output_prefix}_wide.json"
    wide_fieldnames = build_wide_fieldnames(validated_questions)

    result = {
        "source_path_filter": source_path_contains,
        "doc_ids": doc_ids,
        "questions_file": str(questions_file) if questions_file else None,
        "document_count": len(matched_docs),
        "question_count": len(validated_questions),
        "pair_count": len(matched_docs) * len(validated_questions),
        "results": long_rows,
        "wide_rows": wide_rows,
        "matched_docs": matched_docs,
        "summary": {
            "status": "cancelled" if cancelled else "completed",
            "cancelled": cancelled,
            "matched_docs": len(matched_docs),
            "questions_loaded": len(validated_questions),
            "attempted_pairs": len(long_rows),
            "succeeded": succeeded,
            "failed": failed,
            "long_csv_path": str(long_csv_path),
            "long_json_path": str(long_json_path),
            "wide_csv_path": str(wide_csv_path),
            "wide_json_path": str(wide_json_path),
        },
    }

    if write_outputs_flag:
        OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
        if long_csv_rows:
            write_csv(long_csv_path, long_csv_rows)
        else:
            write_empty_csv(long_csv_path, LONG_FIELDNAMES)

        if wide_rows:
            write_csv(wide_csv_path, wide_rows)
        else:
            write_empty_csv(wide_csv_path, wide_fieldnames)

        write_json(
            long_json_path,
            {
                "source_path_filter": source_path_contains,
                "doc_ids": doc_ids,
                "questions_file": str(questions_file) if questions_file else None,
                "document_count": len(matched_docs),
                "question_count": len(validated_questions),
                "pair_count": len(matched_docs) * len(validated_questions),
                "results": long_rows,
            },
        )
        write_json(
            wide_json_path,
            {
                "source_path_filter": source_path_contains,
                "doc_ids": doc_ids,
                "questions_file": str(questions_file) if questions_file else None,
                "document_count": len(matched_docs),
                "question_count": len(validated_questions),
                "rows": wide_rows,
            },
        )

    emit_progress(
        progress_callback,
        {
            "status": "cancelled" if cancelled else "completed",
            "message": "Question run cancelled." if cancelled else "Question run completed.",
            "current_question_index": total_questions if total_questions and not cancelled else 0,
            "total_questions": total_questions,
            "current_question_text": None,
            "current_document_index": total_documents if total_documents and not cancelled else 0,
            "total_documents": total_documents,
            "current_document_name": None,
            "completed_pairs": len(long_rows),
            "total_pairs": total_pairs,
            "percent_complete": (
                round((len(long_rows) / total_pairs) * 100, 1)
                if total_pairs > 0
                else 0
            ),
        },
    )

    return result


def main() -> int:
    args = parse_args()
    questions_file = Path(args.questions_file).expanduser().resolve()

    try:
        questions = load_questions(questions_file)
        result = run_question_set(
            source_path_contains=args.source_path_contains,
            questions=questions,
            questions_file=questions_file,
            limit_docs=args.limit_docs,
            limit_questions=args.limit_questions,
            verbose=args.verbose,
            output_prefix=args.output_prefix,
            write_outputs_flag=True,
            log_progress=True,
        )
    except (FileNotFoundError, RuntimeError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print_summary(
        matched_docs=result["summary"]["matched_docs"],
        questions_loaded=result["summary"]["questions_loaded"],
        attempted_pairs=result["summary"]["attempted_pairs"],
        succeeded=result["summary"]["succeeded"],
        failed=result["summary"]["failed"],
        long_csv_path=Path(result["summary"]["long_csv_path"]),
        long_json_path=Path(result["summary"]["long_json_path"]),
        wide_csv_path=Path(result["summary"]["wide_csv_path"]),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
