# CBA Search

Search a corpus of collective bargaining agreements using retrieval-augmented generation (RAG) to extract relevant context and generate structured outputs in a dashboard for analysis.

## Overview

CBA Search allows you to ask structured questions across a set of contracts and receive consistent, analyzable results. It combines document retrieval with LLM-based reasoning to produce normalized outputs (e.g., dates, numeric values, boolean answers) that can be reviewed in a table format.

## Features

- 📄 Ingest and index contract PDFs
- 🔍 Retrieval-augmented question answering (RAG)
- 🧠 Structured outputs (answer, value, unit, quote, confidence, etc.)
- 📊 Dashboard-style table view across documents
- 🧩 Custom question builder with answer types:
  - Date
  - Number
  - True / False
  - Short Answer
- ⏱️ Progress tracking and cancellation support
- 📤 CSV export of results

## How It Works

1. **Ingest Documents**
   - Extract text from PDFs
   - Chunk and embed into a vector database

2. **Ask Questions**
   - Define structured questions
   - Retrieve relevant passages per document

3. **Generate Answers**
   - LLM processes retrieved context
   - Outputs structured JSON responses

4. **Review Results**
   - View answers across all documents in a table
   - Export results for further analysis
