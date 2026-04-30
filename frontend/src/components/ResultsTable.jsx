import { useState } from "react";

import { exportWideResultsCsv } from "../lib/exportWideResultsCsv";

function getDisplayColumns(question) {
  return {
    answer: true,
    notes: true,
    quote: true,
    ...(question.displayColumns || {})
  };
}

function isTrueFalseAnswerType(answerType) {
  return answerType === "boolean" || answerType === "true_false";
}

function isShortAnswerType(answerType) {
  return answerType === "string_short" || answerType === "short_answer";
}

function getNormalizedAnswerType(answerType) {
  if (answerType === "date" || answerType === "number") {
    return answerType;
  }
  if (isTrueFalseAnswerType(answerType)) {
    return "true_false";
  }
  if (isShortAnswerType(answerType)) {
    return "short_answer";
  }
  return "short_answer";
}

function formatTrueFalseAnswer(cell) {
  if (!cell) {
    return "";
  }

  if (typeof cell.answer === "string") {
    const normalizedAnswer = cell.answer.trim().toLowerCase();
    if (normalizedAnswer === "true" || normalizedAnswer === "yes") {
      return "yes";
    }
    if (normalizedAnswer === "false" || normalizedAnswer === "no") {
      return "no";
    }
  }

  if (cell.value === 1) {
    return "yes";
  }
  if (cell.value === 0) {
    return "no";
  }

  return cell.answer || "";
}

function formatAnswer(cell, question) {
  if (!cell) {
    return "";
  }
  if (isTrueFalseAnswerType(question?.answer_type)) {
    return formatTrueFalseAnswer(cell);
  }
  if (cell.value !== null && cell.value !== undefined && cell.value !== "") {
    return cell.unit ? `${cell.value} ${cell.unit}` : String(cell.value);
  }
  return cell.answer || "";
}

function formatPages(pages) {
  if (!pages?.length) {
    return "";
  }
  return pages.join(", ");
}

function formatDisclosurePreview(text) {
  if (!text) {
    return "";
  }

  const collapsed = text.replace(/\s+/g, " ").trim();
  if (collapsed.length <= 88) {
    return collapsed;
  }

  return `${collapsed.slice(0, 88).trim()}...`;
}

function getQuestionFilterState(filters, questionId) {
  return (
    filters[questionId] || {
      selectedValues: [],
      query: ""
    }
  );
}

function isUnclearAnswer(cell) {
  return typeof cell?.answer === "string" && cell.answer.trim().toLowerCase() === "unclear";
}

function getDateFilterValue(cell) {
  if (isUnclearAnswer(cell)) {
    return "unclear";
  }

  if (typeof cell?.normalized_sort_value === "string") {
    const match = cell.normalized_sort_value.match(/^(\d{4})-/);
    if (match) {
      return match[1];
    }
  }

  if (typeof cell?.answer === "string") {
    const match = cell.answer.match(/\b(\d{4})\b/);
    if (match) {
      return match[1];
    }
  }

  return "";
}

function getNumberFilterValue(cell) {
  if (isUnclearAnswer(cell)) {
    return "unclear";
  }

  if (cell?.value === null || cell?.value === undefined || cell.value === "") {
    return "";
  }
  return String(cell.value);
}

function getTrueFalseFilterValue(cell) {
  const normalizedAnswer = formatTrueFalseAnswer(cell).trim().toLowerCase();
  if (normalizedAnswer === "yes" || normalizedAnswer === "no") {
    return normalizedAnswer;
  }
  return "unclear";
}

function getFilterValue(cell, question) {
  switch (getNormalizedAnswerType(question?.answer_type)) {
    case "date":
      return getDateFilterValue(cell);
    case "number":
      return getNumberFilterValue(cell);
    case "true_false":
      return getTrueFalseFilterValue(cell);
    default:
      return formatAnswer(cell, question).trim();
  }
}

function getDiscreteFilterOptions(rows, question) {
  const answerType = getNormalizedAnswerType(question?.answer_type);
  if (answerType === "short_answer") {
    return [];
  }

  const optionsByValue = new Map();
  for (const row of rows) {
    const cell = row.answers?.[question.question_id];
    const value = getFilterValue(cell, question);
    if (!value) {
      continue;
    }

    if (answerType === "number") {
      optionsByValue.set(value, {
        value,
        label: value === "unclear" ? "unclear" : formatAnswer(cell, question),
        sortValue: value === "unclear" ? Number.POSITIVE_INFINITY : Number(value)
      });
      continue;
    }

    if (answerType === "date") {
      optionsByValue.set(value, {
        value,
        label: value,
        sortValue: value === "unclear" ? Number.POSITIVE_INFINITY : Number(value)
      });
      continue;
    }

    if (answerType === "true_false") {
      optionsByValue.set(value, {
        value,
        label: value,
        sortValue: ["yes", "no", "unclear"].indexOf(value)
      });
    }
  }

  return [...optionsByValue.values()].sort((left, right) => {
    if (left.sortValue !== right.sortValue) {
      return left.sortValue - right.sortValue;
    }
    return left.label.localeCompare(right.label);
  });
}

function hasActiveFilter(question, filters) {
  if (!getDisplayColumns(question).answer) {
    return false;
  }

  const filterState = getQuestionFilterState(filters, question.question_id);
  if (getNormalizedAnswerType(question?.answer_type) === "short_answer") {
    return filterState.query.trim() !== "";
  }
  return filterState.selectedValues.length > 0;
}

function rowMatchesQuestionFilter(row, question, filters) {
  if (!getDisplayColumns(question).answer) {
    return true;
  }

  const filterState = getQuestionFilterState(filters, question.question_id);
  const answerType = getNormalizedAnswerType(question?.answer_type);
  const cell = row.answers?.[question.question_id];

  if (answerType === "short_answer") {
    const query = filterState.query.trim().toLowerCase();
    if (!query) {
      return true;
    }
    return formatAnswer(cell, question).toLowerCase().includes(query);
  }

  if (!filterState.selectedValues.length) {
    return true;
  }

  const value = getFilterValue(cell, question);
  return filterState.selectedValues.includes(value);
}

function filterWideResults(rows, questions, filters) {
  return rows.filter((row) =>
    questions.every((question) => rowMatchesQuestionFilter(row, question, filters))
  );
}

function getRenderedColumnCount(questions) {
  let count = 2;
  for (const question of questions) {
    const displayColumns = getDisplayColumns(question);
    if (displayColumns.answer) {
      count += 1;
    }
    if (displayColumns.notes) {
      count += 1;
    }
    if (displayColumns.quote) {
      count += 1;
    }
  }
  return count;
}

export default function ResultsTable({ resultSet }) {
  const [answerFilters, setAnswerFilters] = useState({});

  if (!resultSet?.wide_results?.length) {
    return (
      <section className="panel">
        <div className="panelHeader">
          <div>
            <p className="eyebrow">Step 3</p>
            <h2>Results</h2>
          </div>
          <p className="panelHint">Run a question set to see one row per document here.</p>
        </div>
        <div className="emptyState">
          <strong>No results yet</strong>
          <p>The wide results table will appear here after the first question run.</p>
        </div>
      </section>
    );
  }

  const filteredRows = filterWideResults(resultSet.wide_results, resultSet.questions, answerFilters);
  const activeFilterCount = resultSet.questions.filter((question) =>
    hasActiveFilter(question, answerFilters)
  ).length;
  const renderedColumnCount = getRenderedColumnCount(resultSet.questions);

  function handleExportCsv() {
    exportWideResultsCsv({
      ...resultSet,
      wide_results: filteredRows
    });
  }

  function toggleDiscreteFilter(questionId, value) {
    setAnswerFilters((current) => {
      const existing = getQuestionFilterState(current, questionId);
      const isSelected = existing.selectedValues.includes(value);
      return {
        ...current,
        [questionId]: {
          ...existing,
          selectedValues: isSelected
            ? existing.selectedValues.filter((item) => item !== value)
            : [...existing.selectedValues, value]
        }
      };
    });
  }

  function updateTextFilter(questionId, query) {
    setAnswerFilters((current) => ({
      ...current,
      [questionId]: {
        ...getQuestionFilterState(current, questionId),
        query
      }
    }));
  }

  function clearQuestionFilter(questionId) {
    setAnswerFilters((current) => ({
      ...current,
      [questionId]: {
        selectedValues: [],
        query: ""
      }
    }));
  }

  return (
    <section className="panel resultsPanel">
      <div className="panelHeader">
        <div>
          <p className="eyebrow">Step 3</p>
          <h2>Results</h2>
        </div>
        <div className="resultsHeaderActions">
          <p className="panelHint">
            One row per document, with answer, compact notes, and quoted language for each
            question.
          </p>
          <button className="secondaryButton exportButton" type="button" onClick={handleExportCsv}>
            Export CSV
          </button>
        </div>
      </div>

      <div className="summaryGrid">
        <div className="summaryCard">
          <span>Matched Documents</span>
          <strong>{resultSet.summary?.matched_docs ?? 0}</strong>
        </div>
        <div className="summaryCard">
          <span>Questions Loaded</span>
          <strong>{resultSet.summary?.questions_loaded ?? 0}</strong>
        </div>
        <div className="summaryCard">
          <span>Succeeded</span>
          <strong>{resultSet.summary?.succeeded ?? 0}</strong>
        </div>
        <div className="summaryCard summaryCard-warning">
          <span>Failed</span>
          <strong>{resultSet.summary?.failed ?? 0}</strong>
        </div>
      </div>

      {activeFilterCount > 0 ? (
        <p className="resultsFilterSummary">
          Showing {filteredRows.length} of {resultSet.wide_results.length} documents.
          {" "}
          {activeFilterCount} answer filter{activeFilterCount === 1 ? "" : "s"} active.
        </p>
      ) : null}

      <div className="resultsTableViewport">
        <table className="resultsTable">
          <thead>
            <tr>
              <th className="stickyCol stickyColOne">
                <div className="cellWrap cellWrap-filename">Filename</div>
              </th>
              <th>
                <div className="cellWrap cellWrap-path">Source Path</div>
              </th>
              {resultSet.questions.map((question) => (
                <FragmentColumns
                  question={question}
                  rows={resultSet.wide_results}
                  filterState={getQuestionFilterState(answerFilters, question.question_id)}
                  onToggleDiscreteFilter={toggleDiscreteFilter}
                  onUpdateTextFilter={updateTextFilter}
                  onClearFilter={clearQuestionFilter}
                  key={question.question_id}
                />
              ))}
            </tr>
          </thead>
          <tbody>
            {filteredRows.length ? filteredRows.map((row) => (
              <tr key={row.doc_id}>
                <td className="stickyCol stickyColOne stickyCell">
                  <div className="cellWrap cellWrap-filename">{row.filename}</div>
                </td>
                <td className="pathCell">
                  <div className="cellWrap cellWrap-path">{row.source_path}</div>
                </td>
                {resultSet.questions.map((question) => {
                  const cell = row.answers[question.question_id];
                  return (
                    <FragmentCells
                      key={`${row.doc_id}-${question.question_id}`}
                      question={question}
                      cell={cell}
                    />
                  );
                })}
              </tr>
            )) : (
              <tr>
                <td className="emptyFilterCell" colSpan={renderedColumnCount}>
                  No documents match the current answer filters.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function FragmentColumns({
  question,
  rows,
  filterState,
  onToggleDiscreteFilter,
  onUpdateTextFilter,
  onClearFilter
}) {
  const displayColumns = getDisplayColumns(question);
  const answerType = getNormalizedAnswerType(question?.answer_type);
  const active = hasActiveFilter(question, { [question.question_id]: filterState });
  const filterOptions = getDiscreteFilterOptions(rows, question);

  return (
    <>
      {displayColumns.answer ? (
        <th className="answerColumnHeader">
          <div className="answerHeaderStack">
            <span>{question.question_name}: Answer</span>
            <details className="answerFilterDisclosure">
              <summary>
                <span className="answerFilterSummaryLabel">
                  {active ? "Filter active" : "Filter answers"}
                </span>
              </summary>
              <div className="answerFilterPanel">
                {answerType === "short_answer" ? (
                  <label className="answerFilterField">
                    <span>Contains</span>
                    <input
                      type="text"
                      value={filterState.query}
                      onChange={(event) => onUpdateTextFilter(question.question_id, event.target.value)}
                      placeholder="Search answer text"
                    />
                  </label>
                ) : filterOptions.length ? (
                  <div className="answerFilterOptions">
                    {filterOptions.map((option) => (
                      <label className="answerFilterOption" key={option.value}>
                        <input
                          type="checkbox"
                          checked={filterState.selectedValues.includes(option.value)}
                          onChange={() => onToggleDiscreteFilter(question.question_id, option.value)}
                        />
                        <span>{option.label}</span>
                      </label>
                    ))}
                  </div>
                ) : (
                  <p className="answerFilterEmpty">No answer values available to filter.</p>
                )}
                {active ? (
                  <button
                    className="answerFilterClear"
                    type="button"
                    onClick={() => onClearFilter(question.question_id)}
                  >
                    Clear
                  </button>
                ) : null}
              </div>
            </details>
          </div>
        </th>
      ) : null}
      {displayColumns.notes ? <th>{question.question_name}: Notes</th> : null}
      {displayColumns.quote ? <th>{question.question_name}: Quote</th> : null}
    </>
  );
}

function FragmentCells({ question, cell }) {
  const displayColumns = getDisplayColumns(question);
  const pagesText = formatPages(cell?.citation_pages);
  const notesPreview = formatDisclosurePreview(cell?.notes);
  const quotePreview = formatDisclosurePreview(cell?.quote);

  return (
    <>
      {displayColumns.answer ? (
        <td className="answerColumnCell">
          <div className="answerCell">
            <strong>{formatAnswer(cell, question)}</strong>
            <span className="mutedText">{pagesText ? `pp. ${pagesText}` : ""}</span>
          </div>
        </td>
      ) : null}
      {displayColumns.notes ? (
        <td className="notesCell">
          {cell?.notes ? (
            <details className="quoteDisclosure">
              <summary>
                <span className="caretLabel">Show notes</span>
                <span className="quotePreview quotePreviewClamped">{notesPreview}</span>
              </summary>
              <div className="disclosureBody">{cell.notes}</div>
            </details>
          ) : (
            <span className="mutedText">No notes</span>
          )}
        </td>
      ) : null}
      {displayColumns.quote ? (
        <td className="quoteCell">
          {cell?.quote ? (
            <details className="quoteDisclosure">
              <summary>
                <span className="caretLabel">Show quote</span>
                <span className="quotePreview">{quotePreview}</span>
              </summary>
              <blockquote>{cell.quote}</blockquote>
            </details>
          ) : (
            <span className="mutedText">No quoted language</span>
          )}
        </td>
      ) : null}
    </>
  );
}
