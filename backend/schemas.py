from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


AnswerType = Literal["number", "boolean", "true_false", "date", "string_short", "short_answer"]
RankingDirection = Literal["ascending", "descending"]


class ProcessDocumentsRequest(BaseModel):
    root: str
    name_contains: str = "cba or (collective and agreement)"
    force: bool = False
    dry_run: bool = False


class QuestionInput(BaseModel):
    question_name: str = Field(..., min_length=1)
    question_id: str | None = None
    question_text: str = Field(..., min_length=1)
    answer_type: AnswerType
    unit: str | None = None
    description: str | None = None
    ranking_direction: RankingDirection | None = None


class RunQuestionsRequest(BaseModel):
    source_path_contains: str | None = None
    doc_ids: list[str] | None = None
    questions: list[QuestionInput]
    output_prefix: str = "ui_results"
    limit_docs: int | None = None
    limit_questions: int | None = None
