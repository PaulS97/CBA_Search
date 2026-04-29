#!/usr/bin/env python3
# Usage:
#   python ingest_folder.py --root "/Users/paulseham/Documents/CBA_Search/Industry Data Project/Air Canada"
#   python ingest_folder.py --root "/path/to/folder" --dry-run --verbose
#   python ingest_folder.py --root "/path/to/folder" --force

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

try:
    from extract_pages import build_doc_id, extract_pdf
except ImportError as exc:
    build_doc_id = None
    extract_pdf = None
    EXTRACT_IMPORT_ERROR = exc
else:
    EXTRACT_IMPORT_ERROR = None

try:
    from chunk_pages import chunk_document
except ImportError as exc:
    chunk_document = None
    CHUNK_IMPORT_ERROR = exc
else:
    CHUNK_IMPORT_ERROR = None

try:
    from index_chroma import index_document
except ImportError as exc:
    index_document = None
    INDEX_IMPORT_ERROR = exc
else:
    INDEX_IMPORT_ERROR = None


PROJECT_ROOT = Path(__file__).resolve().parent
CACHE_ROOT = PROJECT_ROOT / "_rag_cache"
INGEST_RUNS_DIR = CACHE_ROOT / "ingest_runs"
SOURCE_MTIME_TOLERANCE = 1e-6

STATUS_NEW = "NEW"
STATUS_EXTRACTED_ONLY = "EXTRACTED_ONLY"
STATUS_CHUNKED_ONLY = "CHUNKED_ONLY"
STATUS_INDEXED = "INDEXED"
STATUS_STALE_SOURCE = "STALE_SOURCE"
STATUS_BROKEN_CACHE = "BROKEN_CACHE"
STATUS_FAILED = "FAILED"

ACTION_SKIP = "SKIP"
ACTION_PROCESS_FULL = "PROCESS_FULL"
ACTION_PROCESS_CHUNK_AND_INDEX = "PROCESS_CHUNK_AND_INDEX"
ACTION_PROCESS_INDEX_ONLY = "PROCESS_INDEX_ONLY"
ACTION_FORCED_REPROCESS = "FORCED_REPROCESS"
ACTION_DRY_RUN = "DRY_RUN"
QUERY_OPERATORS = {"AND", "OR", "NOT"}
DEFAULT_NAME_QUERY = "cba or (collective and agreement)"


def parse_args() -> argparse.Namespace:
    """Parse the folder-ingest options."""
    parser = argparse.ArgumentParser(
        description="Discover candidate CBAs in a folder tree and run only the missing RAG pipeline steps."
    )
    parser.add_argument("--root", required=True, help="Root folder to scan recursively.")
    parser.add_argument(
        "--name-contains",
        default=DEFAULT_NAME_QUERY,
        help=(
            "Case-insensitive file query supporting AND, OR, NOT, parentheses, and quotes. "
            f"Default: {DEFAULT_NAME_QUERY}"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run extraction, chunking, and indexing for every candidate.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and classify only. Do not run any processing steps.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print doc ids, cache paths, and classification details.",
    )
    return parser.parse_args()


def ensure_pipeline_functions() -> None:
    """Fail fast if the reusable pipeline entry points are not importable."""
    errors: list[str] = []
    if build_doc_id is None or extract_pdf is None:
        errors.append(f"extract_pages.py import failed: {EXTRACT_IMPORT_ERROR}")
    if chunk_document is None:
        errors.append(f"chunk_pages.py import failed: {CHUNK_IMPORT_ERROR}")
    if index_document is None:
        errors.append(f"index_chroma.py import failed: {INDEX_IMPORT_ERROR}")

    if errors:
        raise RuntimeError("Pipeline imports failed:\n" + "\n".join(errors))


def write_json(path: Path, payload: object) -> None:
    """Write a JSON file with stable formatting."""
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def utc_timestamp_slug() -> str:
    """Build a filesystem-friendly UTC timestamp for ingest run logs."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def uses_boolean_query_syntax(query: str) -> bool:
    """Return True when the query uses operators, parentheses, or quotes."""
    if any(character in query for character in ('"', "(", ")")):
        return True

    for word in query.split():
        if word.upper() in QUERY_OPERATORS:
            return True

    return False


def tokenize_query(query: str) -> list[dict]:
    """Turn a query string into TERM / operator / parenthesis tokens."""
    tokens: list[dict] = []
    index = 0

    while index < len(query):
        character = query[index]

        if character.isspace():
            index += 1
            continue

        if character == "(":
            tokens.append({"type": "LPAREN"})
            index += 1
            continue

        if character == ")":
            tokens.append({"type": "RPAREN"})
            index += 1
            continue

        if character == '"':
            end_index = index + 1
            while end_index < len(query) and query[end_index] != '"':
                end_index += 1

            if end_index >= len(query):
                raise RuntimeError("Unterminated quote in file query.")

            phrase = query[index + 1 : end_index].strip().lower()
            if not phrase:
                raise RuntimeError("Quoted phrases in file query cannot be empty.")

            tokens.append({"type": "TERM", "value": phrase})
            index = end_index + 1
            continue

        end_index = index
        while end_index < len(query) and not query[end_index].isspace() and query[end_index] not in '()"':
            end_index += 1

        value = query[index:end_index]
        operator = value.upper()
        if operator in QUERY_OPERATORS:
            tokens.append({"type": operator})
        else:
            tokens.append({"type": "TERM", "value": value.lower()})

        index = end_index

    return insert_implicit_and(tokens)


def insert_implicit_and(tokens: list[dict]) -> list[dict]:
    """Treat adjacent terms / groups as implicit AND expressions."""
    if not tokens:
        return []

    normalized: list[dict] = [tokens[0]]
    for token in tokens[1:]:
        previous = normalized[-1]
        previous_can_end = previous["type"] in {"TERM", "RPAREN"}
        current_can_start = token["type"] in {"TERM", "LPAREN", "NOT"}
        if previous_can_end and current_can_start:
            normalized.append({"type": "AND"})
        normalized.append(token)

    return normalized


class QueryParser:
    """Recursive-descent parser for the boolean file query language."""

    def __init__(self, tokens: list[dict]) -> None:
        self.tokens = tokens
        self.index = 0

    def parse(self) -> dict:
        expression = self.parse_or_expression()
        if self.peek() is not None:
            raise RuntimeError("Unexpected token at end of file query.")
        return expression

    def parse_or_expression(self) -> dict:
        expression = self.parse_and_expression()
        while self.match("OR"):
            expression = {
                "type": "OR",
                "left": expression,
                "right": self.parse_and_expression(),
            }
        return expression

    def parse_and_expression(self) -> dict:
        expression = self.parse_not_expression()
        while self.match("AND"):
            expression = {
                "type": "AND",
                "left": expression,
                "right": self.parse_not_expression(),
            }
        return expression

    def parse_not_expression(self) -> dict:
        if self.match("NOT"):
            return {
                "type": "NOT",
                "operand": self.parse_not_expression(),
            }
        return self.parse_primary()

    def parse_primary(self) -> dict:
        token = self.peek()
        if token is None:
            raise RuntimeError("Unexpected end of file query.")

        if token["type"] == "TERM":
            self.index += 1
            return {"type": "TERM", "value": token["value"]}

        if self.match("LPAREN"):
            expression = self.parse_or_expression()
            if not self.match("RPAREN"):
                raise RuntimeError("Missing closing parenthesis in file query.")
            return expression

        raise RuntimeError(f"Unexpected token in file query: {token['type']}")

    def peek(self) -> dict | None:
        if self.index >= len(self.tokens):
            return None
        return self.tokens[self.index]

    def match(self, token_type: str) -> bool:
        token = self.peek()
        if token is None or token["type"] != token_type:
            return False
        self.index += 1
        return True


def evaluate_query_expression(expression: dict, searchable_text: str) -> bool:
    """Evaluate a parsed query expression against one lower-cased path string."""
    expression_type = expression["type"]

    if expression_type == "TERM":
        return expression["value"] in searchable_text

    if expression_type == "NOT":
        return not evaluate_query_expression(expression["operand"], searchable_text)

    if expression_type == "AND":
        return evaluate_query_expression(expression["left"], searchable_text) and evaluate_query_expression(
            expression["right"], searchable_text
        )

    if expression_type == "OR":
        return evaluate_query_expression(expression["left"], searchable_text) or evaluate_query_expression(
            expression["right"], searchable_text
        )

    raise RuntimeError(f"Unsupported query expression node: {expression_type}")


def build_path_matcher(query: str) -> Callable[[Path], bool]:
    """
    Build one matcher function for the provided query.

    Plain text without operators stays backward-compatible and behaves as a
    case-insensitive substring search across the filename only.
    """
    normalized_query = query.strip().lower()
    if not normalized_query:
        return lambda path: True

    if not uses_boolean_query_syntax(query):
        return lambda path: normalized_query in path.name.lower()

    tokens = tokenize_query(query)
    if not tokens:
        return lambda path: True

    expression = QueryParser(tokens).parse()
    return lambda path: evaluate_query_expression(expression, path.name.lower())


def discover_candidate_pdfs(root: Path, name_contains: str) -> list[Path]:
    """
    Recursively walk the root tree and return the matching PDF files in a stable,
    sorted order.
    """
    if not root.exists():
        raise FileNotFoundError(f"Root folder not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Root path is not a directory: {root}")

    matches_query = build_path_matcher(name_contains)
    candidates: list[Path] = []

    for path in sorted(root.rglob("*"), key=lambda item: str(item).lower()):
        if not path.is_file():
            continue
        if path.suffix.lower() != ".pdf":
            continue
        if not matches_query(path):
            continue
        candidates.append(path.resolve())

    return candidates


def compute_doc_id(pdf_path: Path) -> str:
    """Compute the shared doc_id using the exact extraction pipeline logic."""
    if build_doc_id is None:
        raise RuntimeError(f"Unable to import build_doc_id from extract_pages.py: {EXTRACT_IMPORT_ERROR}")

    file_stat = pdf_path.stat()
    return build_doc_id(pdf_path, file_stat.st_size, file_stat.st_mtime_ns)


def load_source_json(path: Path) -> dict | None:
    """Load source.json when present, or return None if it does not exist."""
    if not path.exists():
        return None

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to read {path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"{path} must contain a JSON object.")

    return payload


def validate_source_payload(source_payload: dict, doc_id: str) -> None:
    """Ensure source.json has the minimal fields the ingest logic depends on."""
    required_fields = {"doc_id", "source_path", "bytes", "mtime"}
    missing = sorted(required_fields - set(source_payload))
    if missing:
        raise RuntimeError(f"source.json is missing required fields: {', '.join(missing)}")

    if source_payload.get("doc_id") != doc_id:
        raise RuntimeError(
            f"source.json doc_id mismatch: expected {doc_id}, found {source_payload.get('doc_id')!r}."
        )


def source_mismatch_reasons(pdf_path: Path, source_payload: dict) -> list[str]:
    """Return a list of concrete reasons when the cached source no longer matches the file."""
    file_stat = pdf_path.stat()
    reasons: list[str] = []

    current_path = str(pdf_path.resolve())
    stored_path = source_payload.get("source_path")
    if stored_path != current_path:
        reasons.append(f"source_path differs (cached={stored_path!r}, current={current_path!r})")

    stored_bytes = source_payload.get("bytes")
    if stored_bytes != file_stat.st_size:
        reasons.append(f"bytes differ (cached={stored_bytes!r}, current={file_stat.st_size})")

    stored_mtime = source_payload.get("mtime")
    if not isinstance(stored_mtime, (int, float)):
        reasons.append(f"mtime is invalid in source.json ({stored_mtime!r})")
    else:
        if abs(float(stored_mtime) - file_stat.st_mtime) > SOURCE_MTIME_TOLERANCE:
            reasons.append(
                f"mtime differs (cached={float(stored_mtime)!r}, current={file_stat.st_mtime!r})"
            )

    return reasons


def source_matches_current_file(pdf_path: Path, source_payload: dict) -> bool:
    """Return True when source.json still describes the current PDF on disk."""
    return not source_mismatch_reasons(pdf_path, source_payload)


def validate_jsonl(path: Path, required_keys: set[str], label: str) -> int:
    """
    Parse a JSONL file and ensure every record is an object with the required keys.

    This treats malformed or empty cache artifacts as broken cache state.
    """
    record_count = 0

    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                if not raw_line.strip():
                    continue

                try:
                    payload = json.loads(raw_line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"{label} line {line_number} is invalid JSON: {exc}") from exc

                if not isinstance(payload, dict):
                    raise RuntimeError(f"{label} line {line_number} is not a JSON object.")

                missing = sorted(required_keys - set(payload))
                if missing:
                    raise RuntimeError(
                        f"{label} line {line_number} is missing keys: {', '.join(missing)}"
                    )

                record_count += 1
    except OSError as exc:
        raise RuntimeError(f"Unable to read {path}: {exc}") from exc

    if record_count == 0:
        raise RuntimeError(f"{label} did not contain any usable records.")

    return record_count


def planned_action_for_status(status: str) -> str:
    """Map a cache classification to the normal next step."""
    if status == STATUS_INDEXED:
        return ACTION_SKIP
    if status in {STATUS_NEW, STATUS_STALE_SOURCE, STATUS_BROKEN_CACHE}:
        return ACTION_PROCESS_FULL
    if status == STATUS_EXTRACTED_ONLY:
        return ACTION_PROCESS_CHUNK_AND_INDEX
    if status == STATUS_CHUNKED_ONLY:
        return ACTION_PROCESS_INDEX_ONLY
    return ACTION_SKIP


def select_action(status: str, force: bool, dry_run: bool) -> str:
    """Turn the status plus CLI flags into the action taken for this record."""
    if dry_run:
        return ACTION_DRY_RUN
    if force:
        return ACTION_FORCED_REPROCESS
    return planned_action_for_status(status)


def make_record(pdf_path: Path, doc_id: str, status: str, error: str | None = None) -> dict:
    """Create the standard per-document record shape."""
    return {
        "pdf_path": str(pdf_path),
        "filename": pdf_path.name,
        "doc_id": doc_id,
        "status": status,
        "classified_status": status,
        "action": planned_action_for_status(status),
        "planned_action": planned_action_for_status(status),
        "error": error,
        "cache_dir": str(CACHE_ROOT / doc_id),
        "matched_filter": True,
    }


def classify_document(pdf_path: Path, doc_id: str) -> dict:
    """
    Inspect the cache directory for one candidate PDF and classify it into one of
    the requested pipeline states.
    """
    cache_dir = CACHE_ROOT / doc_id
    record = make_record(pdf_path, doc_id, STATUS_NEW)

    if not cache_dir.exists():
        record["reason"] = "Cache directory does not exist yet."
        return record

    if not cache_dir.is_dir():
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "Cache path exists but is not a directory."
        return record

    source_path = cache_dir / "source.json"
    pages_path = cache_dir / "pages.jsonl"
    chunks_path = cache_dir / "chunks.jsonl"
    indexed_ok_path = cache_dir / "indexed.ok"

    try:
        source_payload = load_source_json(source_path)
    except RuntimeError as exc:
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "source.json is unreadable."
        record["error"] = str(exc)
        return record

    if source_payload is None:
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "source.json is missing while the cache directory exists."
        return record

    try:
        validate_source_payload(source_payload, doc_id)
    except RuntimeError as exc:
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "source.json is malformed or inconsistent."
        record["error"] = str(exc)
        return record

    stale_reasons = source_mismatch_reasons(pdf_path, source_payload)
    if stale_reasons:
        record["status"] = STATUS_STALE_SOURCE
        record["classified_status"] = STATUS_STALE_SOURCE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "source.json does not match the current file."
        record["stale_reasons"] = stale_reasons
        return record

    pages_exists = pages_path.exists()
    chunks_exists = chunks_path.exists()
    indexed_exists = indexed_ok_path.exists()

    if chunks_exists and not pages_exists:
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "chunks.jsonl exists but pages.jsonl is missing."
        return record

    if indexed_exists and not (pages_exists and chunks_exists):
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "indexed.ok exists but the upstream cache artifacts are incomplete."
        return record

    if not pages_exists:
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "pages.jsonl is missing while the cache directory exists."
        return record

    try:
        pages_count = validate_jsonl(pages_path, {"page", "text"}, "pages.jsonl")
        record["pages_records"] = pages_count
    except RuntimeError as exc:
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "pages.jsonl is unreadable."
        record["error"] = str(exc)
        return record

    if not chunks_exists and not indexed_exists:
        record["status"] = STATUS_EXTRACTED_ONLY
        record["classified_status"] = STATUS_EXTRACTED_ONLY
        record["action"] = ACTION_PROCESS_CHUNK_AND_INDEX
        record["planned_action"] = ACTION_PROCESS_CHUNK_AND_INDEX
        record["reason"] = "pages.jsonl exists, but chunks.jsonl and indexed.ok are missing."
        return record

    if not chunks_exists and indexed_exists:
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "indexed.ok exists but chunks.jsonl is missing."
        return record

    try:
        chunk_count = validate_jsonl(
            chunks_path,
            {"chunk_id", "doc_id", "page_start", "page_end", "word_count", "text"},
            "chunks.jsonl",
        )
        record["chunk_records"] = chunk_count
    except RuntimeError as exc:
        record["status"] = STATUS_BROKEN_CACHE
        record["classified_status"] = STATUS_BROKEN_CACHE
        record["action"] = ACTION_PROCESS_FULL
        record["planned_action"] = ACTION_PROCESS_FULL
        record["reason"] = "chunks.jsonl is unreadable."
        record["error"] = str(exc)
        return record

    if indexed_exists:
        record["status"] = STATUS_INDEXED
        record["classified_status"] = STATUS_INDEXED
        record["action"] = ACTION_SKIP
        record["planned_action"] = ACTION_SKIP
        record["reason"] = "source.json matches and all required cache artifacts exist."
        return record

    record["status"] = STATUS_CHUNKED_ONLY
    record["classified_status"] = STATUS_CHUNKED_ONLY
    record["action"] = ACTION_PROCESS_INDEX_ONLY
    record["planned_action"] = ACTION_PROCESS_INDEX_ONLY
    record["reason"] = "pages.jsonl and chunks.jsonl exist, but indexed.ok is missing."
    return record


def process_document(record: dict, force: bool, dry_run: bool) -> dict:
    """
    Execute only the necessary steps for one document, while keeping failures
    isolated to that document's record.
    """
    record["planned_action"] = planned_action_for_status(record["classified_status"])
    record["action"] = select_action(record["classified_status"], force=force, dry_run=dry_run)

    if record["action"] in {ACTION_SKIP, ACTION_DRY_RUN}:
        return record

    pdf_path = Path(record["pdf_path"])
    doc_id = record["doc_id"]

    try:
        if record["action"] in {ACTION_PROCESS_FULL, ACTION_FORCED_REPROCESS}:
            extract_pdf(pdf_path)
            chunk_document(doc_id)
            index_result = index_document(doc_id, force=True, run_sanity_query_after=False)
            record["completed_steps"] = ["extract_pages", "chunk_pages", "index_chroma"]
            record["index_result"] = index_result
            return record

        if record["action"] == ACTION_PROCESS_CHUNK_AND_INDEX:
            chunk_document(doc_id)
            index_result = index_document(doc_id, force=False, run_sanity_query_after=False)
            record["completed_steps"] = ["chunk_pages", "index_chroma"]
            record["index_result"] = index_result
            return record

        if record["action"] == ACTION_PROCESS_INDEX_ONLY:
            index_result = index_document(doc_id, force=False, run_sanity_query_after=False)
            record["completed_steps"] = ["index_chroma"]
            record["index_result"] = index_result
            return record

        return record
    except Exception as exc:
        record["status"] = STATUS_FAILED
        record["error"] = str(exc)
        return record


def print_document_status(record: dict, verbose: bool) -> None:
    """Print one concise line per document, with optional classification detail."""
    path_text = record["pdf_path"]
    if record["status"] == STATUS_FAILED:
        print(f"[FAILED] {path_text} -> {record['error']}")
    else:
        print(f"[{record['status']}] {record['action']} {path_text}")

    if not verbose:
        return

    print(f"  doc_id: {record['doc_id']}")
    print(f"  cache_dir: {record['cache_dir']}")
    if "reason" in record:
        print(f"  reason: {record['reason']}")
    if record.get("stale_reasons"):
        for stale_reason in record["stale_reasons"]:
            print(f"  stale_reason: {stale_reason}")
    if record["action"] == ACTION_DRY_RUN:
        print(f"  planned_action: {record['planned_action']}")
    if record.get("completed_steps"):
        print(f"  completed_steps: {', '.join(record['completed_steps'])}")
    if record.get("error") and record["status"] != STATUS_FAILED:
        print(f"  detail: {record['error']}")


def build_summary(records: list[dict]) -> dict:
    """Compute the aggregate ingest totals for CLI output and API responses."""
    return {
        "candidates_found": len(records),
        "skipped_indexed": sum(
            1
            for record in records
            if record["classified_status"] == STATUS_INDEXED and record["action"] == ACTION_SKIP
        ),
        "processed_full": sum(
            1 for record in records if record["action"] in {ACTION_PROCESS_FULL, ACTION_FORCED_REPROCESS}
        ),
        "processed_chunk_and_index": sum(
            1 for record in records if record["action"] == ACTION_PROCESS_CHUNK_AND_INDEX
        ),
        "processed_index_only": sum(
            1 for record in records if record["action"] == ACTION_PROCESS_INDEX_ONLY
        ),
        "stale_source": sum(
            1 for record in records if record["classified_status"] == STATUS_STALE_SOURCE
        ),
        "broken_cache": sum(
            1 for record in records if record["classified_status"] == STATUS_BROKEN_CACHE
        ),
        "failed": sum(1 for record in records if record["status"] == STATUS_FAILED),
        "dry_run_only": sum(1 for record in records if record["action"] == ACTION_DRY_RUN),
    }


def print_summary(records: list[dict]) -> None:
    """Print the requested aggregate totals at the end of the run."""
    summary = build_summary(records)
    print("Summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")


def write_ingest_run_log(root: Path, args: argparse.Namespace, records: list[dict]) -> Path:
    """Persist the per-document ingest results for later inspection."""
    INGEST_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = INGEST_RUNS_DIR / f"{utc_timestamp_slug()}.json"
    payload = {
        "root": str(root),
        "name_contains": args.name_contains,
        "force": args.force,
        "dry_run": args.dry_run,
        "verbose": args.verbose,
        "records": records,
    }
    write_json(log_path, payload)
    return log_path


def failure_record(pdf_path: Path, doc_id: str | None, error: str) -> dict:
    """Create a per-document record when classification or processing fails early."""
    return {
        "pdf_path": str(pdf_path),
        "filename": pdf_path.name,
        "doc_id": doc_id or "",
        "status": STATUS_FAILED,
        "classified_status": STATUS_FAILED,
        "action": ACTION_SKIP,
        "planned_action": ACTION_SKIP,
        "error": error,
        "cache_dir": str(CACHE_ROOT / doc_id) if doc_id else "",
        "matched_filter": True,
    }


def emit_progress(progress_callback: Callable[[dict], None] | None, payload: dict) -> None:
    """Send one ingest progress update when a callback is provided."""
    if progress_callback is not None:
        progress_callback(payload)


def run_ingest(
    root: Path,
    name_contains: str = DEFAULT_NAME_QUERY,
    force: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    write_log: bool = True,
    progress_callback: Callable[[dict], None] | None = None,
) -> dict:
    """
    Run the folder ingest pipeline and return both the per-document records and
    the aggregate summary for reuse by the API layer.
    """
    ensure_pipeline_functions()
    resolved_root = Path(root).expanduser().resolve()
    candidates = discover_candidate_pdfs(resolved_root, name_contains)
    total_documents = len(candidates)

    emit_progress(
        progress_callback,
        {
            "status": "running",
            "phase": "starting",
            "root": str(resolved_root),
            "name_contains": name_contains,
            "total_documents": total_documents,
            "completed_documents": 0,
            "current_document": None,
            "current_path": None,
            "current_status": None,
            "error": None,
            "summary": None,
        },
    )

    records: list[dict] = []
    for pdf_path in candidates:
        doc_id: str | None = None
        emit_progress(
            progress_callback,
            {
                "status": "running",
                "phase": "processing",
                "root": str(resolved_root),
                "name_contains": name_contains,
                "total_documents": total_documents,
                "completed_documents": len(records),
                "current_document": pdf_path.name,
                "current_path": str(pdf_path),
                "current_status": None,
                "error": None,
                "summary": None,
            },
        )
        try:
            doc_id = compute_doc_id(pdf_path)
            record = classify_document(pdf_path, doc_id)
            record = process_document(record, force=force, dry_run=dry_run)
        except Exception as exc:
            record = failure_record(pdf_path, doc_id, str(exc))

        records.append(record)
        emit_progress(
            progress_callback,
            {
                "status": "running",
                "phase": "processing",
                "root": str(resolved_root),
                "name_contains": name_contains,
                "total_documents": total_documents,
                "completed_documents": len(records),
                "current_document": pdf_path.name,
                "current_path": str(pdf_path),
                "current_status": record["status"],
                "error": None,
                "summary": None,
            },
        )
        if verbose:
            print_document_status(record, verbose=True)

    result = {
        "root": str(resolved_root),
        "name_contains": name_contains,
        "force": force,
        "dry_run": dry_run,
        "records": records,
        "summary": build_summary(records),
        "log_path": None,
    }

    if write_log:
        INGEST_RUNS_DIR.mkdir(parents=True, exist_ok=True)
        log_path = INGEST_RUNS_DIR / f"{utc_timestamp_slug()}.json"
        write_json(log_path, result)
        result["log_path"] = str(log_path)

    emit_progress(
        progress_callback,
        {
            "status": "completed",
            "phase": "completed",
            "root": str(resolved_root),
            "name_contains": name_contains,
            "total_documents": total_documents,
            "completed_documents": total_documents,
            "current_document": None,
            "current_path": None,
            "current_status": None,
            "error": None,
            "summary": result["summary"],
        },
    )

    return result


def main() -> int:
    args = parse_args()

    try:
        result = run_ingest(
            root=Path(args.root),
            name_contains=args.name_contains,
            force=args.force,
            dry_run=args.dry_run,
            verbose=False,
            write_log=True,
        )
    except (FileNotFoundError, NotADirectoryError, RuntimeError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    for record in result["records"]:
        print_document_status(record, verbose=args.verbose)

    print_summary(result["records"])
    if args.verbose and result.get("log_path"):
        print(f"Ingest log: {result['log_path']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
