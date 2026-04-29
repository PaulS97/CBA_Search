import QuestionCard from "./QuestionCard";

export default function QuestionBuilderPanel({
  questions,
  onQuestionChange,
  onAddQuestion,
  onRemoveQuestion,
  onRunQuestions,
  onCancelRun,
  loading,
  cancelRequested,
  progress,
  error
}) {
  const totalPairs = progress?.total_pairs ?? 0;
  const completedPairs = progress?.completed_pairs ?? 0;
  const percentComplete =
    typeof progress?.percent_complete === "number"
      ? progress.percent_complete
      : totalPairs > 0
        ? Math.min(100, Math.round((completedPairs / totalPairs) * 100))
        : 0;
  const showProgress =
    loading || progress?.status === "running" || progress?.status === "cancel_requested";

  return (
    <section className="panel">
      <div className="panelHeader">
        <div>
          <p className="eyebrow">Step 2</p>
          <h2>Questions</h2>
        </div>
        <p className="panelHint">
          Build a reusable question set. The tool will run each question against the active
          processed document set from Step 1.
        </p>
      </div>

      <div className="questionStack">
        {questions.map((question) => (
          <QuestionCard
            key={question.clientId}
            question={question}
            onChange={(field, value) => onQuestionChange(question.clientId, field, value)}
            onRemove={() => onRemoveQuestion(question.clientId)}
            disableRemove={questions.length === 1}
          />
        ))}
      </div>

      <div className="actionRow">
        <button className="secondaryButton" type="button" onClick={onAddQuestion}>
          Add Question
        </button>
      </div>

      <div className="actionRow">
        <button className="primaryButton" onClick={onRunQuestions} disabled={loading}>
          {loading ? "Running Questions..." : "Run Questions"}
        </button>
        {loading ? (
          <button
            className="secondaryButton"
            type="button"
            onClick={onCancelRun}
            disabled={cancelRequested}
          >
            {cancelRequested ? "Cancelling..." : "Cancel Run"}
          </button>
        ) : null}
        {error ? <p className="errorText">{error}</p> : null}
      </div>

      {showProgress ? (
        <div className="progressCard">
          <div className="progressHeader">
            <strong>Running question set</strong>
            <span>
              {totalPairs > 0 ? `${completedPairs} / ${totalPairs}` : "Preparing..."}
            </span>
          </div>
          <div className="progressBarTrack" aria-hidden="true">
            <div
              className={`progressBarFill ${totalPairs === 0 ? "progressBarFill-indeterminate" : ""}`}
              style={totalPairs > 0 ? { width: `${percentComplete}%` } : undefined}
            />
          </div>
          <div className="progressDetailsGrid">
            <div className="progressDetail">
              <span>Question</span>
              <strong>
                {progress?.current_question_index || 0} / {progress?.total_questions || 0}
              </strong>
            </div>
            <div className="progressDetail">
              <span>Document</span>
              <strong>
                {progress?.current_document_index || 0} / {progress?.total_documents || 0}
              </strong>
            </div>
          </div>
          <p className="progressMeta">
            {progress?.current_question_text ? (
              <>
                Current question: <strong>{progress.current_question_text}</strong>
              </>
            ) : (
              "Validating questions and discovering indexed documents."
            )}
          </p>
          <p className="progressMeta">
            {progress?.current_document_name ? (
              <>
                Current document: <strong>{progress.current_document_name}</strong>
              </>
            ) : (
              progress?.message || "Preparing question run."
            )}
          </p>
          {progress?.status === "cancel_requested" ? (
            <p className="progressMeta">
              <strong>{progress.message}</strong>
            </p>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
