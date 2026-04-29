#!/usr/bin/env python3
# Usage:
#   python answer_folder.py --source-path-contains "/Users/paulseham/Documents/CBA_Search/Industry Data Project/Air Canada"
#   python answer_folder.py --source-path-contains "/path/subtree" --question "What is the effective date of this agreement?"
#   python answer_folder.py --source-path-contains "/path/subtree" --limit 5 --verbose

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

try:
    from answer_with_rag import (
        CACHE_ROOT,
        DEFAULT_QUESTION,
        OUTPUTS_DIR,
        answer_one_document,
        prepare_answering_runtime,
    )
except ImportError as exc:
    CACHE_ROOT = Path(__file__).resolve().parent / "_rag_cache"
    OUTPUTS_DIR = Path(__file__).resolve().parent / "outputs"
    DEFAULT_QUESTION = "How many sick leave days with pay are pilots entitled to?"
    answer_one_document = None
    prepare_answering_runtime = None
    ANSWER_IMPORT_ERROR = exc
else:
    ANSWER_IMPORT_ERROR = None


IGNORED_CACHE_DIRS = {"chroma_db", "ingest_runs", "__pycache__"}


def parse_args() -> argparse.Namespace:
    """Parse folder-answering options."""
    parser = argparse.ArgumentParser(
        description="Run the one-question RAG answer pipeline across multiple indexed documents."
    )
    parser.add_argument(
        "--source-path-contains",
        required=True,
        help="Only include cached documents whose source_path contains this substring.",
    )
    parser.add_argument(
        "--question",
        default=DEFAULT_QUESTION,
        help="Question to ask of each matched document.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print extra logging about skipped cache directories and per-document details.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of matched documents to process after sorting.",
    )
    parser.add_argument(
        "--output-prefix",
        default="folder_results",
        help="Prefix for outputs/<prefix>.csv and outputs/<prefix>.json.",
    )
    return parser.parse_args()


def ensure_answer_pipeline() -> None:
    """Fail fast if the reusable single-document answer path is unavailable."""
    if answer_one_document is None or prepare_answering_runtime is None:
        raise RuntimeError(f"Unable to import answer pipeline from answer_with_rag.py: {ANSWER_IMPORT_ERROR}")


def write_json(path: Path, payload: object) -> None:
    """Write JSON with stable formatting for inspection and debugging."""
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def load_source_payload(source_json_path: Path) -> dict | None:
    """Load one cached source.json file, or return None if it is missing."""
    if not source_json_path.exists():
        return None

    try:
        payload = json.loads(source_json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to read {source_json_path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"{source_json_path} must contain a JSON object.")

    return payload


def discover_indexed_docs(cache_root: Path, verbose: bool = False) -> list[dict]:
    """
    Scan `_rag_cache` for document cache folders that are ready for answering.

    A document is considered answerable for this folder runner only if it has
    `source.json`, `chunks.jsonl`, and `indexed.ok`.
    """
    documents: list[dict] = []

    if not cache_root.exists():
        return documents

    for cache_dir in sorted(cache_root.iterdir(), key=lambda path: path.name.lower()):
        if not cache_dir.is_dir():
            continue
        if cache_dir.name in IGNORED_CACHE_DIRS:
            continue

        source_json_path = cache_dir / "source.json"
        indexed_ok_path = cache_dir / "indexed.ok"
        chunks_path = cache_dir / "chunks.jsonl"

        if not source_json_path.exists():
            if verbose:
                print(f"[SKIP] {cache_dir} -> source.json missing")
            continue
        if not indexed_ok_path.exists():
            if verbose:
                print(f"[SKIP] {cache_dir} -> indexed.ok missing")
            continue
        if not chunks_path.exists():
            if verbose:
                print(f"[SKIP] {cache_dir} -> chunks.jsonl missing")
            continue

        try:
            source_payload = load_source_payload(source_json_path)
        except RuntimeError as exc:
            if verbose:
                print(f"[SKIP] {cache_dir} -> {exc}")
            continue

        if source_payload is None:
            if verbose:
                print(f"[SKIP] {cache_dir} -> source.json missing")
            continue

        source_path = source_payload.get("source_path")
        if not isinstance(source_path, str) or not source_path:
            if verbose:
                print(f"[SKIP] {cache_dir} -> source.json missing valid source_path")
            continue

        filename = source_payload.get("filename")
        if not isinstance(filename, str) or not filename:
            filename = Path(source_path).name

        doc_id = source_payload.get("doc_id")
        if not isinstance(doc_id, str) or not doc_id:
            doc_id = cache_dir.name

        documents.append(
            {
                "doc_id": doc_id,
                "filename": filename,
                "source_path": source_path,
                "source_bytes": source_payload.get("bytes"),
                "source_mtime": source_payload.get("mtime"),
                "cache_dir": str(cache_dir),
            }
        )

    return documents


def matches_source_filter(source_path: str, needle: str) -> bool:
    """MVP source filter: exact case-sensitive substring match."""
    return needle in source_path


def make_error_result(doc_meta: dict, question: str, error: str) -> dict:
    """Shape per-document failures into the same schema as successful answers."""
    return {
        "doc_id": doc_meta["doc_id"],
        "question": question,
        "answer": "ERROR",
        "value": None,
        "unit": None,
        "quote": "",
        "supporting_excerpt": "",
        "citation_pages": [],
        "confidence": "low",
        "notes": error,
    }


def serialize_csv_row(doc_meta: dict, result: dict) -> dict:
    """Flatten one document result into a CSV row."""
    citation_pages = result.get("citation_pages", [])
    citation_text = ";".join(str(page) for page in citation_pages)

    return {
        "doc_id": doc_meta["doc_id"],
        "filename": doc_meta["filename"],
        "source_path": doc_meta["source_path"],
        "source_bytes": doc_meta.get("source_bytes"),
        "source_mtime": doc_meta.get("source_mtime"),
        "question": result.get("question"),
        "answer": result.get("answer"),
        "value": result.get("value"),
        "unit": result.get("unit"),
        "citation_pages": citation_text,
        "confidence": result.get("confidence"),
        "supporting_excerpt": result.get("supporting_excerpt"),
        "notes": result.get("notes"),
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    """Write the folder answer results to CSV."""
    fieldnames = [
        "doc_id",
        "filename",
        "source_path",
        "source_bytes",
        "source_mtime",
        "question",
        "answer",
        "value",
        "unit",
        "citation_pages",
        "confidence",
        "supporting_excerpt",
        "notes",
    ]

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def print_result_line(doc_meta: dict, result: dict) -> None:
    """Print one concise per-document status line."""
    filename = doc_meta["filename"]
    if result["answer"] == "ERROR":
        print(f"[ERROR] {filename} -> {result['notes']}")
    else:
        print(f"[OK] {filename} -> answer={result['answer']}")


def print_summary(
    matched_docs: int,
    succeeded: int,
    failed: int,
    csv_path: Path,
    json_path: Path,
) -> None:
    """Print the requested concise folder-run summary."""
    print("Summary:")
    print(f"  matched_docs: {matched_docs}")
    print(f"  succeeded: {succeeded}")
    print(f"  failed: {failed}")
    print(f"  csv_path: {csv_path}")
    print(f"  json_path: {json_path}")


def main() -> int:
    args = parse_args()

    try:
        ensure_answer_pipeline()
        indexed_docs = discover_indexed_docs(CACHE_ROOT, verbose=args.verbose)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    matched_docs = [
        doc for doc in indexed_docs if matches_source_filter(doc["source_path"], args.source_path_contains)
    ]
    matched_docs.sort(key=lambda doc: (doc["source_path"], doc["doc_id"]))

    if args.limit is not None:
        matched_docs = matched_docs[: max(args.limit, 0)]

    runtime = None
    if matched_docs:
        try:
            runtime = prepare_answering_runtime()
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 1

    results_json: list[dict] = []
    csv_rows: list[dict] = []
    succeeded = 0
    failed = 0

    for doc_meta in matched_docs:
        try:
            answer_payload = answer_one_document(doc_meta["doc_id"], args.question, runtime=runtime)
            result = answer_payload["result"]
            succeeded += 1
        except Exception as exc:
            result = make_error_result(doc_meta, args.question, str(exc))
            failed += 1

        print_result_line(doc_meta, result)
        csv_rows.append(serialize_csv_row(doc_meta, result))
        results_json.append(
            {
                "doc_id": doc_meta["doc_id"],
                "filename": doc_meta["filename"],
                "source_path": doc_meta["source_path"],
                "source_bytes": doc_meta.get("source_bytes"),
                "source_mtime": doc_meta.get("source_mtime"),
                "result": result,
            }
        )

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUTS_DIR / f"{args.output_prefix}.csv"
    json_path = OUTPUTS_DIR / f"{args.output_prefix}.json"

    try:
        write_csv(csv_path, csv_rows)
        write_json(
            json_path,
            {
                "question": args.question,
                "source_path_filter": args.source_path_contains,
                "matched_count": len(matched_docs),
                "results": results_json,
            },
        )
    except OSError as exc:
        print(f"Failed to write outputs: {exc}", file=sys.stderr)
        return 1

    print_summary(
        matched_docs=len(matched_docs),
        succeeded=succeeded,
        failed=failed,
        csv_path=csv_path,
        json_path=json_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
