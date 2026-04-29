#!/usr/bin/env python3
# Usage:
#   python answer_with_rag.py --doc-id "<doc_id>"
#   python answer_with_rag.py --pdf "/path/to/original.pdf"
#   python answer_with_rag.py --doc-id "<doc_id>" --question "How many sick leave days with pay are pilots entitled to?"

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

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
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
COLLECTION_NAME = "cba_chunks"
DEFAULT_QUESTION = "How many sick leave days with pay are pilots entitled to?"
DEFAULT_CHAT_MODEL = "gpt-4.1-mini"
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"
RETRIEVAL_K = 8


def parse_args() -> argparse.Namespace:
    """Require one document selector and accept an optional question override."""
    parser = argparse.ArgumentParser(
        description="Retrieve evidence from the local Chroma index and answer a question using only that evidence."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--doc-id", help="Document id for an already indexed cache entry.")
    group.add_argument("--pdf", help="Path to the original PDF used during extraction.")
    parser.add_argument(
        "--question",
        default=DEFAULT_QUESTION,
        help="Question to answer from the indexed document.",
    )
    parser.add_argument(
        "--answer-type",
        help="Optional answer type metadata to thread through the answer pipeline.",
    )
    parser.add_argument(
        "--description",
        help="Optional question hint/description to thread through the answer pipeline.",
    )
    return parser.parse_args()


def write_json(path: Path, payload: dict) -> None:
    """Write a JSON file with stable formatting for easy debugging."""
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def derive_doc_id_from_pdf(pdf_path: Path) -> str:
    """
    Reuse the shared doc_id logic so `--pdf` resolves to the same cached document
    identity used by extraction, chunking, and indexing.
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


def ensure_dependencies() -> None:
    """Fail fast if the installed environment is missing required packages."""
    missing: list[str] = []
    if chromadb is None:
        missing.append("chromadb")
    if OpenAI is None:
        missing.append("openai")

    if missing:
        raise RuntimeError(
            "Missing dependencies: "
            + ", ".join(missing)
            + ". Install requirements.txt before running answer_with_rag.py."
        )


def load_config() -> tuple[str, str, str]:
    """
    Load environment configuration from `.env` when available, while still working
    if the key is already present in the process environment.
    """
    if load_dotenv is not None:
        load_dotenv(PROJECT_ROOT / ".env")

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is missing. Set it in the environment or in /Users/paulseham/Documents/CBA_Search/.env."
        )

    chat_model = os.getenv("OPENAI_MODEL", DEFAULT_CHAT_MODEL).strip() or DEFAULT_CHAT_MODEL
    embedding_model = (
        os.getenv("OPENAI_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL).strip()
        or DEFAULT_EMBEDDING_MODEL
    )
    return api_key, chat_model, embedding_model


def build_effective_question(question: str, description: str | None = None) -> str:
    """
    Combine the base question with an optional hint while keeping the original
    question text available for result payloads and displays.
    """
    if not isinstance(description, str) or not description.strip():
        return question
    return f"Question: {question}\nHint: {description.strip()}"


def prepare_answering_runtime() -> dict:
    """
    Create the shared runtime objects needed to answer one or many questions
    against the local Chroma index.
    """
    ensure_dependencies()
    api_key, model_name, embedding_model = load_config()
    collection = get_collection(CHROMA_DB_PATH)
    openai_client = OpenAI(api_key=api_key)
    return {
        "openai_client": openai_client,
        "collection": collection,
        "model_name": model_name,
        "embedding_model": embedding_model,
    }


def get_collection(db_path: Path):
    """Open the persistent Chroma database and fetch the existing collection."""
    if not db_path.exists():
        raise RuntimeError(
            f"Chroma DB path is missing: {db_path}. Run index_chroma.py first."
        )

    try:
        client = chromadb.PersistentClient(path=str(db_path))
        return client.get_collection(name=COLLECTION_NAME)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to open Chroma collection '{COLLECTION_NAME}' at {db_path}: {exc}"
        ) from exc


def embed_query(client: OpenAI, embedding_model: str, question: str) -> list[float]:
    """Generate the question embedding used for the Chroma similarity query."""
    try:
        response = client.embeddings.create(model=embedding_model, input=[question])
    except OpenAIError as exc:
        raise RuntimeError(f"OpenAI embedding request failed: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Unexpected error during OpenAI embedding request: {exc}") from exc

    if not response.data:
        raise RuntimeError("OpenAI embedding response did not contain any vectors.")
    return response.data[0].embedding


def retrieve_chunks(collection, query_embedding: list[float], doc_id: str) -> list[dict]:
    """
    Query Chroma for the top-k chunk hits for one document and normalize the
    response into a flat list of chunk records.
    """
    try:
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=RETRIEVAL_K,
            where={"doc_id": doc_id},
            include=["documents", "metadatas", "distances"],
        )
    except Exception as exc:
        raise RuntimeError(f"Chroma retrieval failed: {exc}") from exc

    ids = results.get("ids", [[]])
    documents = results.get("documents", [[]])
    metadatas = results.get("metadatas", [[]])
    distances = results.get("distances", [[]])

    first_ids = ids[0] if ids else []
    first_documents = documents[0] if documents else []
    first_metadatas = metadatas[0] if metadatas else []
    first_distances = distances[0] if distances else []

    retrieved: list[dict] = []
    for index, record_id in enumerate(first_ids):
        metadata = first_metadatas[index] if index < len(first_metadatas) else {}
        text = first_documents[index] if index < len(first_documents) else ""
        distance = first_distances[index] if index < len(first_distances) else None

        retrieved.append(
            {
                "record_id": record_id,
                "chunk_id": metadata.get("chunk_id", ""),
                "doc_id": metadata.get("doc_id", doc_id),
                "page_start": metadata.get("page_start"),
                "page_end": metadata.get("page_end"),
                "word_count": metadata.get("word_count"),
                "text": text or "",
                "distance": distance,
            }
        )

    return retrieved


def sort_retrieved_chunks(chunks: list[dict]) -> list[dict]:
    """Sort by page order so the downstream evidence reads like the original document."""
    return sorted(
        chunks,
        key=lambda chunk: (
            chunk.get("page_start", 10**9),
            chunk.get("page_end", 10**9),
            chunk.get("chunk_id", ""),
        ),
    )


def merge_evidence_blocks(chunks: list[dict]) -> list[dict]:
    """
    Merge adjacent or overlapping retrieved chunks into larger evidence blocks.

    This prevents the prompt from presenting fragmented text when the top results
    land on consecutive chunk windows from the same document section.
    """
    if not chunks:
        return []

    blocks: list[dict] = []
    current = {
        "page_start": chunks[0]["page_start"],
        "page_end": chunks[0]["page_end"],
        "chunk_ids": [chunks[0]["chunk_id"]],
        "texts": [chunks[0]["text"]],
        "text": chunks[0]["text"],
    }

    for chunk in chunks[1:]:
        if chunk["page_start"] <= current["page_end"] + 1:
            current["page_end"] = max(current["page_end"], chunk["page_end"])
            current["chunk_ids"].append(chunk["chunk_id"])
            current["texts"].append(chunk["text"])
            current["text"] = current["text"] + "\n\n" + chunk["text"]
            continue

        blocks.append(current)
        current = {
            "page_start": chunk["page_start"],
            "page_end": chunk["page_end"],
            "chunk_ids": [chunk["chunk_id"]],
            "texts": [chunk["text"]],
            "text": chunk["text"],
        }

    blocks.append(current)
    return blocks


def build_prompts(
    question: str,
    evidence_blocks: list[dict],
    answer_type: str | None = None,
) -> tuple[str, str]:
    """Assemble the strict-answer instructions and the evidence payload."""
    system_prompt = (
        "You answer questions only from the supplied evidence blocks.\n"
        "Do not use outside knowledge.\n"
        "If the answer cannot be determined from the evidence, return UNCLEAR.\n"
        "Return strict JSON only with these keys exactly:\n"
        "{\n"
        '  "question": string,\n'
        '  "quote": string,\n'
        '  "supporting_excerpt": string,\n'
        '  "citation_pages": [int, ...],\n'
        '  "notes": string, \n'
        '  "value": number or null,\n'
        '  "unit": string or null,\n'
        '  "answer": string,\n'
        '  "confidence": "high" | "medium" | "low"\n'
        "}\n"
        "The answer must be supported by the evidence.\n"
        "Use a short extracted answer when possible.\n"
        "Quote the most relevant supporting text verbatim or near-verbatim.\n"
        "Return a supporting_excerpt between 50 and 250 words taken from the evidence blocks.\n"
        "The supporting_excerpt should be the main passage that justifies the answer.\n"
        "Prefer contiguous text and preserve original wording as much as possible.\n"
        "Do not invent or summarize beyond the evidence.\n"
        "Cite page numbers only from the provided evidence block metadata."
    )
    if answer_type == "date":
        system_prompt += (
            "\nThis question expects a date answer.\n"
            "Format the answer as Month Day, Year.\n"
            "Set value to null for date answers.\n"
            "Set unit to null for date answers.\n"
            "If only month and year are given, assume the first day of that month and state that assumption in notes.\n"
            "If only a year is given, assume January 1 of that year and state that assumption in notes.\n"
        )

    elif answer_type == "number":
        system_prompt += (
            "\nThis question expects a numeric answer.\n"
            "Set value to the final numeric value that answers the question.\n"
            "Set unit to the unit requested or implied by the question, if available.\n"
            "If the evidence states a rate in a different unit, convert it to the unit requested by the question when the conversion is direct.\n"
            "Format answer as value plus the final unit, not the source rate unit.\n"
            "If a conversion is performed, explain it briefly in notes.\n"
        )

    elif answer_type in {"boolean", "true_false"}:
        system_prompt += (
            "\nThis question expects a true/false answer.\n"
            'Set answer to "true" or "false".\n'
            "Set value to 1 for true and 0 for false.\n"
            "Set unit to null for boolean answers.\n"
        )

    elif answer_type in {"string_short", "short_answer"}:
        system_prompt += (
            "\nThis question expects a short text answer.\n"
            "Set answer to a concise text phrase supported by the evidence.\n"
            "Set value to null for string answers.\n"
            "Set unit to null for string answers.\n"
        )

    evidence_lines: list[str] = []
    for index, block in enumerate(evidence_blocks, start=1):
        evidence_lines.append(
            "\n".join(
                [
                    f"Evidence Block {index}",
                    f"Pages: {block['page_start']}-{block['page_end']}",
                    f"Chunk IDs: {', '.join(block['chunk_ids'])}",
                    "Text:",
                    block["text"],
                ]
            )
        )

    user_prompt = (
        f"Question: {question}\n\n"
        "Evidence Blocks:\n\n"
        + "\n\n---\n\n".join(evidence_lines)
    )
    return system_prompt, user_prompt


def call_chat_model(client: OpenAI, model_name: str, system_prompt: str, user_prompt: str) -> str:
    """Call the chat model and return the raw text response for JSON parsing."""
    try:
        response = client.chat.completions.create(
            model=model_name,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
    except OpenAIError as exc:
        raise RuntimeError(f"OpenAI chat request failed: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Unexpected error during OpenAI chat request: {exc}") from exc

    try:
        content = response.choices[0].message.content
    except (AttributeError, IndexError, TypeError) as exc:
        raise RuntimeError(f"OpenAI chat response did not contain message content: {exc}") from exc

    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
            elif hasattr(item, "type") and getattr(item, "type") == "text":
                parts.append(getattr(item, "text", ""))
        merged = "".join(parts).strip()
        if merged:
            return merged

    raise RuntimeError("OpenAI chat response content was empty or in an unsupported format.")


def parse_json_response(raw_text: str) -> dict:
    """Parse model output as JSON, tolerating fenced JSON if necessary."""
    candidate = raw_text.strip()

    for attempt in (candidate, strip_code_fences(candidate)):
        if not attempt:
            continue
        try:
            parsed = json.loads(attempt)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            raise RuntimeError("Model response parsed successfully, but it was not a JSON object.")
        return parsed

    raise RuntimeError("Model returned invalid JSON.")


def strip_code_fences(text: str) -> str:
    """Handle common model behavior where JSON is wrapped in markdown fences."""
    if text.startswith("```") and text.endswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            return "\n".join(lines[1:-1]).strip()
    return text


def normalize_citation_pages(value) -> list[int]:
    """Deduplicate and sort citation page numbers while tolerating loose model output."""
    if not isinstance(value, list):
        return []

    pages: list[int] = []
    for item in value:
        if isinstance(item, int):
            pages.append(item)
        elif isinstance(item, float) and item.is_integer():
            pages.append(int(item))
        elif isinstance(item, str) and item.strip().isdigit():
            pages.append(int(item.strip()))

    return sorted(set(pages))


def normalize_numeric_value(value):
    """Convert numeric strings to numbers so result.json is consistent."""
    if value is None or isinstance(value, (int, float)):
        return value

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            if "." in stripped:
                return float(stripped)
            return int(stripped)
        except ValueError:
            return None

    return None


def normalize_result(result: dict, question: str) -> dict:
    """Force the parsed model JSON into the expected result shape."""
    normalized = {
        "question": question,
        "answer": str(result.get("answer", "UNCLEAR")).strip() or "UNCLEAR",
        "value": normalize_numeric_value(result.get("value")),
        "unit": None,
        "quote": str(result.get("quote", "")).strip(),
        "citation_pages": normalize_citation_pages(result.get("citation_pages")),
        "confidence": str(result.get("confidence", "low")).strip().lower() or "low",
        "notes": str(result.get("notes", "")).strip(),
        "supporting_excerpt": str(result.get("supporting_excerpt", "")).strip(),
    }

    unit = result.get("unit")
    if unit is not None:
        unit_text = str(unit).strip()
        normalized["unit"] = unit_text or None

    if normalized["confidence"] not in {"high", "medium", "low"}:
        normalized["confidence"] = "low"

    return normalized


def write_outputs(result: dict, audit: dict) -> None:
    """Persist both the concise result and the full audit record."""
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    write_json(OUTPUTS_DIR / "result.json", result)
    write_json(OUTPUTS_DIR / "audit.json", audit)


def print_summary(doc_id: str, result: dict) -> None:
    """Keep stdout short and human-readable."""
    print(f"doc_id: {doc_id}")
    print(f"question: {result['question']}")
    print(f"answer: {result['answer']}")
    print(f"value: {result['value']}")
    print(f"unit: {result['unit']}")
    print(f"citation_pages: {result['citation_pages']}")
    print(f"confidence: {result['confidence']}")
    print(f"supporting_excerpt: {result['supporting_excerpt']}")


def answer_one_document(
    doc_id: str,
    question: str,
    answer_type: str | None = None,
    description: str | None = None,
    runtime: dict | None = None,
) -> dict:
    """
    Run the full one-document RAG answer flow and return both the normalized
    result payload and the full audit payload.
    """
    local_runtime = runtime or prepare_answering_runtime()
    openai_client = local_runtime["openai_client"]
    collection = local_runtime["collection"]
    model_name = local_runtime["model_name"]
    embedding_model = local_runtime["embedding_model"]

    effective_question = build_effective_question(question, description)
    # Retrieval stays anchored to the base question text; the optional
    # description/hint is only threaded into the generation prompt.
    query_embedding = embed_query(openai_client, embedding_model, question)
    retrieved_chunks = retrieve_chunks(collection, query_embedding, doc_id)
    if not retrieved_chunks:
        raise RuntimeError(f"No retrieval results found for doc_id {doc_id}.")

    sorted_chunks = sort_retrieved_chunks(retrieved_chunks)
    evidence_blocks = merge_evidence_blocks(sorted_chunks)
    system_prompt, user_prompt = build_prompts(
        effective_question,
        evidence_blocks,
        answer_type=answer_type,
    )

    raw_model_output = call_chat_model(openai_client, model_name, system_prompt, user_prompt)
    try:
        parsed_result = parse_json_response(raw_model_output)
    except RuntimeError as exc:
        raise RuntimeError(f"{exc}\nRaw model output:\n{raw_model_output}") from exc

    normalized_result = normalize_result(parsed_result, question)
    result_payload = {"doc_id": doc_id, **normalized_result}
    audit_payload = {
        "doc_id": doc_id,
        "question": question,
        "effective_question": effective_question,
        "answer_type": answer_type,
        "description": description,
        "retrieved_chunks": sorted_chunks,
        "evidence_blocks": evidence_blocks,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "model_name": model_name,
        "result": result_payload,
        "raw_model_output": raw_model_output,
    }

    return {"result": result_payload, "audit": audit_payload}



def main() -> int:
    args = parse_args()

    try:
        doc_id = resolve_doc_id(args)
        answer_payload = answer_one_document(
            doc_id,
            args.question,
            answer_type=args.answer_type,
            description=args.description,
        )
    except (FileNotFoundError, RuntimeError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    try:
        write_outputs(answer_payload["result"], answer_payload["audit"])
    except OSError as exc:
        print(f"Failed to write outputs: {exc}", file=sys.stderr)
        return 1

    print_summary(doc_id, answer_payload["result"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
