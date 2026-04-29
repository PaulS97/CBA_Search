#!/usr/bin/env python3
# Usage:
#   python index_chroma.py --doc-id "<doc_id>"
#   python index_chroma.py --pdf "/path/to/original.pdf"
#   python index_chroma.py --doc-id "<doc_id>" --force

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

try:
    import chromadb
except ImportError:
    chromadb = None

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    from openai import OpenAI, OpenAIError
except ImportError:
    OpenAI = None

    class OpenAIError(Exception):
        """Fallback error type used when the OpenAI SDK is unavailable."""


try:
    from extract_pages import build_doc_id
except ImportError as exc:
    build_doc_id = None
    BUILD_DOC_ID_IMPORT_ERROR = exc
else:
    BUILD_DOC_ID_IMPORT_ERROR = None


PROJECT_ROOT = Path(__file__).resolve().parent
CACHE_ROOT = PROJECT_ROOT / "_rag_cache"
CHROMA_DB_PATH = CACHE_ROOT / "chroma_db"
COLLECTION_NAME = "cba_chunks"
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_BATCH_SIZE = 50
SANITY_QUERY_TEXT = "How many sick leave days with pay are pilots entitled to?"


def parse_args() -> argparse.Namespace:
    """Require one document selector and optionally allow a forced re-index."""
    parser = argparse.ArgumentParser(
        description="Embed cached chunks with OpenAI and store them in a persistent Chroma collection."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--doc-id", help="Document id for an already chunked cache entry.")
    group.add_argument("--pdf", help="Path to the original PDF used during extraction.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-indexing even if indexed.ok is newer than chunks.jsonl.",
    )
    return parser.parse_args()


def write_json(path: Path, payload: dict) -> None:
    """Write a small JSON file with stable formatting."""
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def utc_now_iso() -> str:
    """Generate a compact UTC timestamp for manifests and marker files."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def derive_doc_id_from_pdf(pdf_path: Path) -> str:
    """
    Reuse the same doc_id algorithm as the earlier pipeline steps so `--pdf`
    resolves to the existing cache folder instead of creating a second identity.
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
    return derive_doc_id_from_pdf(Path(args.pdf).expanduser())


def load_chunk_records(chunks_path: Path, expected_doc_id: str) -> tuple[list[dict], int, int]:
    """
    Parse chunks.jsonl and prepare deterministic Chroma records.

    The function returns:
    - records ready for embedding/upsert
    - total chunks seen in the file
    - chunks skipped because their text was empty after stripping
    """
    records: list[dict] = []
    chunks_seen = 0
    skipped_empty = 0

    with chunks_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue

            try:
                chunk = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSON in {chunks_path} on line {line_number}: {exc}") from exc

            chunks_seen += 1

            chunk_doc_id = chunk.get("doc_id")
            chunk_id = chunk.get("chunk_id")
            text = chunk.get("text", "")
            page_start = chunk.get("page_start")
            page_end = chunk.get("page_end")
            word_count = chunk.get("word_count")

            if chunk_doc_id != expected_doc_id:
                raise RuntimeError(
                    f"Chunk doc_id mismatch in {chunks_path} on line {line_number}: "
                    f"expected {expected_doc_id}, found {chunk_doc_id!r}."
                )
            if not isinstance(chunk_id, str) or not chunk_id:
                raise RuntimeError(f"Missing or invalid chunk_id in {chunks_path} on line {line_number}.")
            if not isinstance(page_start, int) or not isinstance(page_end, int):
                raise RuntimeError(
                    f"Missing or invalid page range in {chunks_path} on line {line_number}."
                )
            if not isinstance(word_count, int):
                raise RuntimeError(f"Missing or invalid word_count in {chunks_path} on line {line_number}.")
            if not isinstance(text, str):
                text = str(text)

            document_text = text.strip()
            if not document_text:
                skipped_empty += 1
                continue

            records.append(
                {
                    "id": f"{expected_doc_id}:{chunk_id}",
                    "document": document_text,
                    "metadata": {
                        "doc_id": chunk_doc_id,
                        "chunk_id": chunk_id,
                        "page_start": page_start,
                        "page_end": page_end,
                        "word_count": word_count,
                    },
                }
            )

    return records, chunks_seen, skipped_empty


def should_skip_indexing(chunks_path: Path, indexed_ok_path: Path, force: bool) -> bool:
    """
    Skip re-indexing when the success marker is at least as new as chunks.jsonl,
    unless the caller explicitly requested a forced refresh.
    """
    if force or not indexed_ok_path.exists():
        return False
    return indexed_ok_path.stat().st_mtime >= chunks_path.stat().st_mtime


def ensure_indexing_dependencies() -> None:
    """Fail fast with a clear message if the indexing packages are unavailable."""
    missing: list[str] = []
    if chromadb is None:
        missing.append("chromadb")
    if OpenAI is None:
        missing.append("openai")
    if load_dotenv is None:
        missing.append("python-dotenv")

    if missing:
        raise RuntimeError(
            "Missing indexing dependencies: "
            + ", ".join(missing)
            + ". Install requirements.txt before running index_chroma.py."
        )


def load_indexing_config() -> tuple[str, str]:
    """
    Read `.env` and the current process environment, then return the API key and
    embedding model name used for this indexing run.
    """
    load_dotenv(PROJECT_ROOT / ".env")

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is missing. Set it in the environment or in /Users/paulseham/Documents/CBA_Search/.env."
        )

    embedding_model = os.getenv("OPENAI_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL).strip()
    if not embedding_model:
        embedding_model = DEFAULT_EMBEDDING_MODEL

    return api_key, embedding_model


def batched(records: list[dict], batch_size: int) -> Iterator[list[dict]]:
    """Yield fixed-size slices to keep embedding requests and Chroma upserts manageable."""
    for start_index in range(0, len(records), batch_size):
        yield records[start_index : start_index + batch_size]


def embed_texts(client: OpenAI, model: str, texts: list[str]) -> list[list[float]]:
    """Generate one embedding vector per text and validate the response length."""
    try:
        response = client.embeddings.create(model=model, input=texts)
    except OpenAIError as exc:
        raise RuntimeError(f"OpenAI embedding request failed: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Unexpected error during OpenAI embedding request: {exc}") from exc

    embeddings = [item.embedding for item in response.data]
    if len(embeddings) != len(texts):
        raise RuntimeError(
            f"Embedding response size mismatch: expected {len(texts)}, received {len(embeddings)}."
        )

    return embeddings


def get_collection(db_path: Path):
    """Create or reopen the persistent Chroma collection used for all chunk records."""
    try:
        client = chromadb.PersistentClient(path=str(db_path))
        return client.get_or_create_collection(name=COLLECTION_NAME)
    except Exception as exc:
        raise RuntimeError(f"Failed to open Chroma collection at {db_path}: {exc}") from exc


def upsert_records(collection, client: OpenAI, embedding_model: str, records: list[dict]) -> int:
    """
    Embed and upsert records in batches.

    Chroma's `upsert` keeps ids deterministic and updates existing records rather
    than duplicating them, which matches the requested re-indexing behavior.
    """
    indexed_count = 0

    for batch in batched(records, DEFAULT_BATCH_SIZE):
        texts = [record["document"] for record in batch]
        embeddings = embed_texts(client, embedding_model, texts)

        try:
            collection.upsert(
                ids=[record["id"] for record in batch],
                documents=texts,
                metadatas=[record["metadata"] for record in batch],
                embeddings=embeddings,
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to upsert records into Chroma: {exc}") from exc

        indexed_count += len(batch)

    return indexed_count


def build_index_manifest(doc_id: str, embedding_model: str, chunk_count_indexed: int) -> dict:
    """Capture the key facts about this indexing run for later inspection."""
    return {
        "doc_id": doc_id,
        "collection_name": COLLECTION_NAME,
        "embedding_model": embedding_model,
        "chunk_count_indexed": chunk_count_indexed,
        "db_path": str(CHROMA_DB_PATH),
        "indexed_at": utc_now_iso(),
    }


def write_index_marker(indexed_ok_path: Path, manifest: dict) -> None:
    """A lightweight success marker used to skip redundant indexing runs."""
    indexed_ok_path.write_text(
        "\n".join(
            [
                "ok",
                f"doc_id={manifest['doc_id']}",
                f"collection_name={manifest['collection_name']}",
                f"embedding_model={manifest['embedding_model']}",
                f"chunk_count_indexed={manifest['chunk_count_indexed']}",
                f"indexed_at={manifest['indexed_at']}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def print_summary(
    doc_id: str,
    chunks_seen: int,
    chunks_indexed: int,
    skipped_empty: int,
    skipped_due_to_indexed_ok: bool,
) -> None:
    """Keep stdout compact but explicit about what happened."""
    print(f"doc_id: {doc_id}")
    print(f"collection_name: {COLLECTION_NAME}")
    print(f"chunks_seen: {chunks_seen}")
    print(f"chunks_indexed: {chunks_indexed}")
    print(f"skipped_empty: {skipped_empty}")
    print(f"db_path: {CHROMA_DB_PATH}")
    print(f"skipped_due_to_indexed_ok: {skipped_due_to_indexed_ok}")


def run_sanity_query(collection, client: OpenAI, embedding_model: str, doc_id: str) -> None:
    """
    Run one small filtered similarity query against the current document so the
    user can sanity check that the persisted vectors are searchable.
    """
    query_embedding = embed_texts(client, embedding_model, [SANITY_QUERY_TEXT])[0]

    try:
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=3,
            where={"doc_id": doc_id},
            include=["metadatas"],
        )
    except Exception as exc:
        raise RuntimeError(f"Sanity query against Chroma failed: {exc}") from exc

    ids = results.get("ids", [[]])
    metadatas = results.get("metadatas", [[]])
    first_ids = ids[0] if ids else []
    first_metadatas = metadatas[0] if metadatas else []

    if not first_ids:
        print("sanity_query_matches: none")
        return

    print("sanity_query_matches:")
    for record_id, metadata in zip(first_ids, first_metadatas):
        chunk_id = metadata.get("chunk_id", record_id)
        page_start = metadata.get("page_start", "?")
        page_end = metadata.get("page_end", "?")
        print(f"  {chunk_id}: {page_start}-{page_end}")


def index_document(doc_id: str, force: bool = False, run_sanity_query_after: bool = False) -> dict:
    """
    Index one chunked document into the persistent Chroma collection and return a
    summary of what happened.
    """
    cache_dir = CACHE_ROOT / doc_id
    chunks_path = cache_dir / "chunks.jsonl"
    indexed_ok_path = cache_dir / "indexed.ok"
    index_manifest_path = cache_dir / "index_manifest.json"

    if not chunks_path.exists():
        raise RuntimeError(f"Missing chunk file: {chunks_path}. Run chunk_pages.py first.")

    records, chunks_seen, skipped_empty = load_chunk_records(chunks_path, doc_id)
    if not records:
        raise RuntimeError(f"No usable chunk text found in {chunks_path}.")

    skipped_due_to_indexed_ok = should_skip_indexing(chunks_path, indexed_ok_path, force)
    result = {
        "doc_id": doc_id,
        "collection_name": COLLECTION_NAME,
        "chunks_seen": chunks_seen,
        "chunks_indexed": 0,
        "skipped_empty": skipped_empty,
        "db_path": str(CHROMA_DB_PATH),
        "skipped_due_to_indexed_ok": skipped_due_to_indexed_ok,
    }

    if skipped_due_to_indexed_ok:
        return result

    ensure_indexing_dependencies()
    api_key, embedding_model = load_indexing_config()
    openai_client = OpenAI(api_key=api_key)
    CHROMA_DB_PATH.mkdir(parents=True, exist_ok=True)
    collection = get_collection(CHROMA_DB_PATH)

    chunks_indexed = upsert_records(collection, openai_client, embedding_model, records)
    manifest = build_index_manifest(doc_id, embedding_model, chunks_indexed)
    write_json(index_manifest_path, manifest)
    write_index_marker(indexed_ok_path, manifest)

    result.update(
        {
            "chunks_indexed": chunks_indexed,
            "embedding_model": embedding_model,
            "index_manifest": manifest,
            "skipped_due_to_indexed_ok": False,
        }
    )

    if run_sanity_query_after:
        run_sanity_query(collection, openai_client, embedding_model, doc_id)

    return result


def main() -> int:
    args = parse_args()

    try:
        doc_id = resolve_doc_id(args)
    except (FileNotFoundError, RuntimeError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    try:
        result = index_document(doc_id, force=args.force, run_sanity_query_after=False)
    except (RuntimeError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print_summary(
        doc_id=doc_id,
        chunks_seen=result["chunks_seen"],
        chunks_indexed=result["chunks_indexed"],
        skipped_empty=result["skipped_empty"],
        skipped_due_to_indexed_ok=result["skipped_due_to_indexed_ok"],
    )

    if not result["skipped_due_to_indexed_ok"]:
        try:
            api_key, embedding_model = load_indexing_config()
            openai_client = OpenAI(api_key=api_key)
            collection = get_collection(CHROMA_DB_PATH)
            run_sanity_query(collection, openai_client, embedding_model, doc_id)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
