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

function formatTrueFalseAnswer(cell) {
  if (!cell) {
    return "";
  }

  if (typeof cell.answer === "string") {
    const normalizedAnswer = cell.answer.trim().toLowerCase();
    if (normalizedAnswer === "true" || normalizedAnswer === "false") {
      return normalizedAnswer;
    }
  }

  if (cell.value === 1) {
    return "true";
  }
  if (cell.value === 0) {
    return "false";
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

export default function ResultsTable({ resultSet }) {
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

  function handleExportCsv() {
    exportWideResultsCsv(resultSet);
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
                <FragmentColumns question={question} key={question.question_id} />
              ))}
            </tr>
          </thead>
          <tbody>
            {resultSet.wide_results.map((row) => (
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
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function FragmentColumns({ question }) {
  const displayColumns = getDisplayColumns(question);

  return (
    <>
      {displayColumns.answer ? <th className="answerColumnHeader">{question.question_name}: Answer</th> : null}
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
