const ANSWER_TYPES = [
  { value: "number", label: "Number" },
  { value: "true_false", label: "True / False" },
  { value: "date", label: "Date" },
  { value: "short_answer", label: "Short Answer" }
];

const RANKING_OPTIONS = [
  { value: "", label: "blank" },
  { value: "ascending", label: "ascending" },
  { value: "descending", label: "descending" }
];

const QUESTION_NAME_PLACEHOLDER = "New Question";
const QUESTION_TEXT_PLACEHOLDER =
  "Ask your question succinctly using terminology found in contract language (e.g., \"What is the sick leave accrual policy?\").";
const DESCRIPTION_PLACEHOLDER =
  "If necessary, give the AI guidance on how to extract the correct answer from relevant contract passages. (e.g., \"Return the answer in sick leave days accrued per year. If the contract has a different accrual rate such as hour per month, apply the necessary conversion to return an answer in sick leave days accrued per year.\")";

export default function QuestionCard({ question, onChange, onRemove, disableRemove }) {
  const displayColumns = {
    answer: true,
    notes: true,
    quote: true,
    ...(question.displayColumns || {})
  };

  function toggleDisplayColumn(columnKey, checked) {
    onChange("displayColumns", {
      ...displayColumns,
      [columnKey]: checked
    });
  }

  return (
    <article className="questionCard">
      <div className="questionCardHeader">
        <div>
          <h3>{question.questionName || "New Question"}</h3>
          <p>Define the question exactly as you would ask it in a legal memo.</p>
        </div>
        <button
          type="button"
          className="ghostButton"
          onClick={onRemove}
          disabled={disableRemove}
        >
          Remove Question
        </button>
      </div>

      <div className="questionGrid">
        <label className="field">
          <span>Question Name</span>
          <input
            type="text"
            value={question.questionName}
            onChange={(event) => onChange("questionName", event.target.value)}
            placeholder={QUESTION_NAME_PLACEHOLDER}
          />
        </label>

        <label className="field">
          <span>Answer Type</span>
          <select
            value={question.answerType}
            onChange={(event) => onChange("answerType", event.target.value)}
          >
            {ANSWER_TYPES.map((option) => (
              <option value={option.value} key={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label className="field fieldWide">
          <span>Question Text</span>
          <textarea
            rows="3"
            value={question.questionText}
            onChange={(event) => onChange("questionText", event.target.value)}
            placeholder={QUESTION_TEXT_PLACEHOLDER}
          />
        </label>

        {question.answerType === "number" ? (
          <label className="field">
            <span>Unit</span>
            <input
              type="text"
              value={question.unit}
              onChange={(event) => onChange("unit", event.target.value)}
              placeholder="days"
            />
          </label>
        ) : null}

        <label className="field">
          <span>Ranking Direction</span>
          <select
            value={question.rankingDirection}
            onChange={(event) => onChange("rankingDirection", event.target.value)}
          >
            {RANKING_OPTIONS.map((option) => (
              <option value={option.value} key={option.label}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label className="field fieldWide">
          <span>Optional Description / Answer Hint</span>
          <textarea
            rows="2"
            value={question.description}
            onChange={(event) => onChange("description", event.target.value)}
            placeholder={DESCRIPTION_PLACEHOLDER}
          />
        </label>
      </div>

      <div className="displayColumnsSection">
        <span className="displayColumnsLabel">Show Columns</span>
        <div className="displayColumnsGrid">
          <label className="displayColumnOption">
            <input
              type="checkbox"
              checked={displayColumns.answer}
              onChange={(event) => toggleDisplayColumn("answer", event.target.checked)}
            />
            <span>Answer</span>
          </label>
          <label className="displayColumnOption">
            <input
              type="checkbox"
              checked={displayColumns.notes}
              onChange={(event) => toggleDisplayColumn("notes", event.target.checked)}
            />
            <span>Notes</span>
          </label>
          <label className="displayColumnOption">
            <input
              type="checkbox"
              checked={displayColumns.quote}
              onChange={(event) => toggleDisplayColumn("quote", event.target.checked)}
            />
            <span>Quote</span>
          </label>
        </div>
      </div>
    </article>
  );
}
