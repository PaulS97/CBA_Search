#!/usr/bin/env python3
# Usage:
#   python extract_pages.py
#   python extract_pages.py --pdf "/path/to/file.pdf"

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

try:
    import fitz
except ImportError:
    fitz = None


DEFAULT_PDF_PATH = Path(
    "/Users/paulseham/Documents/CBA_Search/Industry Data Project/Air Canada/ACPA CBA 2020-2023.pdf"
)
PROJECT_ROOT = Path(__file__).resolve().parent
CACHE_ROOT = PROJECT_ROOT / "_rag_cache"
LOW_TEXT_THRESHOLD = 50
SEARCH_PHRASE = "sick leave"
MAX_SEARCH_HITS = 10
SNIPPET_WIDTH = 120


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract text from a PDF page by page and cache the results."
    )
    parser.add_argument(
        "--pdf",
        default=str(DEFAULT_PDF_PATH),
        help="Path to the PDF file to extract.",
    )
    return parser.parse_args()


def build_doc_id(pdf_path: Path, file_size: int, mtime_ns: int) -> str:
    stem = re.sub(r"[^a-z0-9]+", "-", pdf_path.stem.lower()).strip("-") or "document"
    seed = f"{pdf_path.name}|{file_size}|{mtime_ns}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return f"{stem}-{digest}"


def write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def non_whitespace_count(text: str) -> int:
    return len(re.sub(r"\s+", "", text))


def make_snippet(text: str, match_start: int, match_end: int, width: int = SNIPPET_WIDTH) -> str:
    match_length = match_end - match_start
    if match_length >= width:
        raw_snippet = text[match_start:match_end]
    else:
        extra = width - match_length
        left = extra // 2
        right = extra - left
        snippet_start = max(0, match_start - left)
        snippet_end = min(len(text), match_end + right)

        current_width = snippet_end - snippet_start
        if current_width < width and snippet_start == 0:
            snippet_end = min(len(text), snippet_end + (width - current_width))
        elif current_width < width and snippet_end == len(text):
            snippet_start = max(0, snippet_start - (width - current_width))

        raw_snippet = text[snippet_start:snippet_end]

    snippet = re.sub(r"\s+", " ", raw_snippet).strip()
    return snippet


def find_phrase_hits(page_texts: list[tuple[int, str]], phrase: str) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    phrase_lower = phrase.lower()

    for page_number, text in page_texts:
        match_start = text.lower().find(phrase_lower)
        if match_start == -1:
            continue

        match_end = match_start + len(phrase)
        hits.append((page_number, make_snippet(text, match_start, match_end)))
        if len(hits) >= MAX_SEARCH_HITS:
            break

    return hits


def extract_pdf(pdf_path: Path) -> tuple[str, int, int, list[int], list[tuple[int, str]]]:
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    if not pdf_path.is_file():
        raise FileNotFoundError(f"PDF path is not a file: {pdf_path}")
    if fitz is None:
        raise RuntimeError("PyMuPDF is not installed. Install dependencies from requirements.txt first.")

    file_stat = pdf_path.stat()
    doc_id = build_doc_id(pdf_path, file_stat.st_size, file_stat.st_mtime_ns)
    cache_dir = CACHE_ROOT / doc_id
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    page_texts: list[tuple[int, str]] = []
    low_text_pages: list[int] = []
    total_chars = 0

    source_payload = {
        "doc_id": doc_id,
        "source_path": str(pdf_path.resolve()),
        "filename": pdf_path.name,
        "bytes": file_stat.st_size,
        "mtime": file_stat.st_mtime,
    }

    try:
        document = fitz.open(str(pdf_path))
    except Exception as exc:
        raise RuntimeError(f"Unable to read PDF: {pdf_path}: {exc}") from exc

    try:
        pages_path = cache_dir / "pages.jsonl"
        with document, pages_path.open("w", encoding="utf-8") as pages_file:
            for page_index in range(document.page_count):
                page_number = page_index + 1
                page = document.load_page(page_index)
                text = page.get_text("text") or ""
                page_texts.append((page_number, text))
                total_chars += len(text)

                if non_whitespace_count(text) < LOW_TEXT_THRESHOLD:
                    low_text_pages.append(page_number)

                payload = {"page": page_number, "text": text}
                pages_file.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as exc:
        raise RuntimeError(f"Failed while extracting text from: {pdf_path}: {exc}") from exc

    manifest_payload = {
        "page_count": len(page_texts),
        "total_chars": total_chars,
        "low_text_pages": low_text_pages,
    }

    write_json(cache_dir / "source.json", source_payload)
    write_json(cache_dir / "manifest.json", manifest_payload)

    return doc_id, len(page_texts), total_chars, low_text_pages, page_texts


def main() -> int:
    args = parse_args()
    pdf_path = Path(args.pdf).expanduser()

    try:
        doc_id, page_count, total_chars, low_text_pages, page_texts = extract_pdf(pdf_path)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"Filesystem error for {pdf_path}: {exc}", file=sys.stderr)
        return 1

    print(f"doc_id: {doc_id}")
    print(f"page_count: {page_count}")
    print(f"total_chars: {total_chars}")
    print(f"low_text_pages: {len(low_text_pages)}")

    hits = find_phrase_hits(page_texts, SEARCH_PHRASE)
    if hits:
        print(f'"{SEARCH_PHRASE}" matches (top {len(hits)} pages):')
        for page_number, snippet in hits:
            print(f"  page {page_number}: {snippet}")
    else:
        print(f'No matches found for "{SEARCH_PHRASE}".')

    return 0


if __name__ == "__main__":
    sys.exit(main())
