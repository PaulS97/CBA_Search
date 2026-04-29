#!/usr/bin/env python3
# Usage:
#   python chunk_pages.py --doc-id "<doc_id>"
#   python chunk_pages.py --pdf "/path/to/original.pdf"

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

try:
    from extract_pages import build_doc_id
except ImportError as exc:
    build_doc_id = None
    BUILD_DOC_ID_IMPORT_ERROR = exc
else:
    BUILD_DOC_ID_IMPORT_ERROR = None


PROJECT_ROOT = Path(__file__).resolve().parent
CACHE_ROOT = PROJECT_ROOT / "_rag_cache"
CHUNK_WORDS_TARGET = 300
CHUNK_WORDS_OVERLAP = 60
PREVIEW_CHARS = 120


def parse_args() -> argparse.Namespace:
    """Require exactly one lookup method: a doc_id or the original PDF path."""
    parser = argparse.ArgumentParser(
        description="Create overlapping, page-aware text chunks from cached PDF pages."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--doc-id", help="Document id for an already extracted PDF cache entry.")
    group.add_argument("--pdf", help="Path to the original PDF used during extraction.")
    return parser.parse_args()


def write_json(path: Path, payload: dict) -> None:
    """Write a small JSON file with stable formatting."""
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def normalize_text(text: str) -> str:
    """Collapse all whitespace so chunk text is clean and consistent."""
    return re.sub(r"\s+", " ", text).strip()


def derive_doc_id_from_pdf(pdf_path: Path) -> str:
    """
    Reuse the exact doc_id logic from extract_pages.py so `--pdf` resolves to the
    same cache folder that page extraction already created.
    """
    if build_doc_id is None:
        raise RuntimeError(
            f"Unable to import build_doc_id from extract_pages.py: {BUILD_DOC_ID_IMPORT_ERROR}"
        )

    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    if not pdf_path.is_file():
        raise FileNotFoundError(f"PDF path is not a file: {pdf_path}")

    file_stat = pdf_path.stat()
    return build_doc_id(pdf_path, file_stat.st_size, file_stat.st_mtime_ns)


def resolve_doc_id(args: argparse.Namespace) -> str:
    """Turn CLI input into one concrete document id."""
    if args.doc_id:
        return args.doc_id

    pdf_path = Path(args.pdf).expanduser()
    return derive_doc_id_from_pdf(pdf_path)


def load_words_with_pages(pages_path: Path) -> tuple[list[str], list[int]]:
    """
    Read cached pages in order and flatten them into:
    - one list of document words
    - one list of page numbers aligned 1:1 with those words

    That alignment lets us chunk across page boundaries while still recovering
    page_start and page_end for each sliding window.
    """
    all_words: list[str] = []
    word_pages: list[int] = []

    with pages_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue

            try:
                record = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSON in {pages_path} on line {line_number}: {exc}") from exc

            page_number = record.get("page")
            text = record.get("text", "")

            if not isinstance(page_number, int):
                raise RuntimeError(f"Invalid page number in {pages_path} on line {line_number}.")
            if not isinstance(text, str):
                text = str(text)

            # Empty or whitespace-only pages are skipped, but the surrounding pages
            # still join naturally because chunking operates on the flattened word list.
            normalized = normalize_text(text)
            if not normalized:
                continue

            page_words = normalized.split()
            all_words.extend(page_words)
            word_pages.extend([page_number] * len(page_words))

    return all_words, word_pages


def build_chunks(doc_id: str, words: list[str], word_pages: list[int]) -> list[dict]:
    """
    Build overlapping chunks from the full-document word stream.

    The chunk window moves forward by `target - overlap` words each time, which
    preserves the requested overlap even when the chunk crosses one or more pages.
    """
    if CHUNK_WORDS_TARGET <= CHUNK_WORDS_OVERLAP:
        raise RuntimeError("Chunk target must be larger than chunk overlap.")
    if len(words) != len(word_pages):
        raise RuntimeError("Word/page alignment error while building chunks.")

    chunks: list[dict] = []
    start_index = 0
    chunk_number = 1
    total_words = len(words)

    while start_index < total_words:
        end_index = min(start_index + CHUNK_WORDS_TARGET, total_words)
        chunk_words = words[start_index:end_index]
        if not chunk_words:
            break

        chunks.append(
            {
                "chunk_id": f"chunk-{chunk_number:04d}",
                "doc_id": doc_id,
                "page_start": word_pages[start_index],
                "page_end": word_pages[end_index - 1],
                "word_count": len(chunk_words),
                "text": " ".join(chunk_words),
            }
        )

        if end_index >= total_words:
            break

        start_index = end_index - CHUNK_WORDS_OVERLAP
        chunk_number += 1

    return chunks


def write_chunks(cache_dir: Path, chunks: list[dict]) -> None:
    """Write one chunk record per line for downstream embedding/indexing steps."""
    chunks_path = cache_dir / "chunks.jsonl"
    with chunks_path.open("w", encoding="utf-8") as handle:
        for chunk in chunks:
            handle.write(json.dumps(chunk, ensure_ascii=False) + "\n")


def build_chunk_manifest(doc_id: str, chunks: list[dict]) -> dict:
    """Capture the key chunking parameters and resulting chunk size range."""
    word_counts = [chunk["word_count"] for chunk in chunks]
    return {
        "doc_id": doc_id,
        "chunk_count": len(chunks),
        "chunk_words_target": CHUNK_WORDS_TARGET,
        "chunk_words_overlap": CHUNK_WORDS_OVERLAP,
        "min_chunk_words": min(word_counts),
        "max_chunk_words": max(word_counts),
    }


def chunk_document(doc_id: str) -> dict:
    """
    Build chunks for one cached document and write the chunk artifacts back into
    that document's cache directory.
    """
    cache_dir = CACHE_ROOT / doc_id
    pages_path = cache_dir / "pages.jsonl"
    if not pages_path.exists():
        raise RuntimeError(f"Missing extracted pages file: {pages_path}. Run extract_pages.py first.")

    words, word_pages = load_words_with_pages(pages_path)
    if not words:
        raise RuntimeError(f"No usable text found in {pages_path}.")

    chunks = build_chunks(doc_id, words, word_pages)
    if not chunks:
        raise RuntimeError(f"No chunks were created for doc_id {doc_id}.")

    manifest = build_chunk_manifest(doc_id, chunks)
    cache_dir.mkdir(parents=True, exist_ok=True)
    write_chunks(cache_dir, chunks)
    write_json(cache_dir / "chunk_manifest.json", manifest)

    return {
        "doc_id": doc_id,
        "cache_dir": str(cache_dir),
        "chunks": chunks,
        "manifest": manifest,
    }


def print_summary(doc_id: str, chunks: list[dict], manifest: dict) -> None:
    """Keep stdout short, but include enough detail to sanity check the output."""
    print(f"doc_id: {doc_id}")
    print(f"chunk_count: {manifest['chunk_count']}")
    print(f"min_chunk_words: {manifest['min_chunk_words']}")
    print(f"max_chunk_words: {manifest['max_chunk_words']}")

    for chunk in chunks[:3]:
        preview = chunk["text"][:PREVIEW_CHARS]
        print(
            f"{chunk['chunk_id']}: {chunk['page_start']}-{chunk['page_end']} {preview}"
        )


def main() -> int:
    args = parse_args()

    try:
        doc_id = resolve_doc_id(args)
    except (FileNotFoundError, RuntimeError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    try:
        result = chunk_document(doc_id)
    except OSError as exc:
        print(f"Failed to write chunk outputs for doc_id {doc_id}: {exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print_summary(doc_id, result["chunks"], result["manifest"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
