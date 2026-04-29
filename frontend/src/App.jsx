import { useEffect, useRef, useState } from "react";

import ProcessDocumentsPanel from "./components/ProcessDocumentsPanel";
import QuestionBuilderPanel from "./components/QuestionBuilderPanel";
import ResultsTable from "./components/ResultsTable";
import {
  cancelRunQuestions,
  fetchLatestResults,
  fetchProcessProgress,
  fetchQaProgress,
  processDocuments,
  runQuestions
} from "./lib/api";

function makeDisplayColumns(overrides = {}) {
  return {
    answer: true,
    notes: true,
    quote: true,
    ...overrides
  };
}

function normalizeDisplayColumns(value) {
  return makeDisplayColumns(value && typeof value === "object" ? value : {});
}

function attachDisplayColumns(apiQuestions, templateQuestions = []) {
  return apiQuestions.map((question, index) => ({
    ...question,
    displayColumns: normalizeDisplayColumns(templateQuestions[index]?.displayColumns)
  }));
}

function makeQuestion(overrides = {}) {
  return {
    clientId: crypto.randomUUID(),
    questionName: "",
    questionText: "",
    answerType: "number",
    unit: "",
    description: "",
    rankingDirection: "descending",
    displayColumns: makeDisplayColumns(),
    ...overrides
  };
}


const DEFAULT_QUESTIONS = [
  makeQuestion({
    questionName: "Effective Date",
    questionText: "What is this agreement's effective date?",
    answerType: "date",
    unit: "",
    description: "Return the effective date stated in the agreement.",
    rankingDirection: "descending"
  })
];



export default function App() {
  const [processForm, setProcessForm] = useState({
    root: "/Users/paulseham/Documents/CBA_Search/Industry Data Project/",
    nameContains: "(cba or collective or agreement or contract) and twu",
    dryRun: false,
    force: false
  });
  const [processResult, setProcessResult] = useState(null);
  const [processError, setProcessError] = useState("");
  const [processLoading, setProcessLoading] = useState(false);
  const [processProgress, setProcessProgress] = useState(null);

  const [questions, setQuestions] = useState(DEFAULT_QUESTIONS);
  const [questionResult, setQuestionResult] = useState(null);
  const [questionError, setQuestionError] = useState("");
  const [questionLoading, setQuestionLoading] = useState(false);
  const [questionCancelRequested, setQuestionCancelRequested] = useState(false);
  const [qaProgress, setQaProgress] = useState(null);
  const processPollRef = useRef(null);
  const qaPollRef = useRef(null);
  const visibleQuestionResult = questionResult
    ? {
        ...questionResult,
        questions: attachDisplayColumns(questionResult.questions, questions)
      }
    : null;

  useEffect(() => {
    fetchLatestResults()
      .then((payload) => {
        if (payload.process_documents) {
          setProcessResult(payload.process_documents);
        }
        if (payload.question_run) {
          setQuestionResult({
            ...payload.question_run,
            questions: attachDisplayColumns(payload.question_run.questions, questions)
          });
        }
      })
      .catch(() => {
        // The app still works even if the backend is not running at page load.
      });

    fetchProcessProgress()
      .then((payload) => {
        setProcessProgress(payload);
      })
      .catch(() => {
        // Ignore missing progress state at page load.
      });

    fetchQaProgress()
      .then((payload) => {
        setQaProgress(payload);
      })
      .catch(() => {
        // Ignore missing QA progress state at page load.
      });

    return () => {
      if (processPollRef.current !== null) {
        window.clearInterval(processPollRef.current);
      }
      if (qaPollRef.current !== null) {
        window.clearInterval(qaPollRef.current);
      }
    };
  }, []);

  function updateProcessField(field, value) {
    setProcessForm((current) => ({ ...current, [field]: value }));
  }

  async function pollProcessProgress() {
    try {
      const payload = await fetchProcessProgress();
      setProcessProgress(payload);
      return payload;
    } catch {
      return null;
    }
  }

  function startProcessProgressPolling() {
    if (processPollRef.current !== null) {
      window.clearInterval(processPollRef.current);
    }

    void pollProcessProgress();
    processPollRef.current = window.setInterval(() => {
      void pollProcessProgress();
    }, 700);
  }

  function stopProcessProgressPolling() {
    if (processPollRef.current !== null) {
      window.clearInterval(processPollRef.current);
      processPollRef.current = null;
    }
  }

  async function pollQaProgress() {
    try {
      const payload = await fetchQaProgress();
      setQaProgress(payload);
      return payload;
    } catch {
      return null;
    }
  }

  function startQaProgressPolling() {
    if (qaPollRef.current !== null) {
      window.clearInterval(qaPollRef.current);
    }

    void pollQaProgress();
    qaPollRef.current = window.setInterval(() => {
      void pollQaProgress();
    }, 700);
  }

  function stopQaProgressPolling() {
    if (qaPollRef.current !== null) {
      window.clearInterval(qaPollRef.current);
      qaPollRef.current = null;
    }
  }

  async function handleProcessDocuments() {
    setProcessError("");
    setProcessLoading(true);
    setProcessProgress({
      status: "running",
      phase: "starting",
      total_documents: 0,
      completed_documents: 0,
      current_document: null,
      current_path: null,
      current_status: null,
      error: null,
      summary: null
    });
    startProcessProgressPolling();

    try {
      const payload = await processDocuments({
        root: processForm.root,
        name_contains: processForm.nameContains,
        force: processForm.force,
        dry_run: processForm.dryRun
      });
      setProcessResult(payload);
    } catch (error) {
      setProcessError(error.message);
    } finally {
      await pollProcessProgress();
      stopProcessProgressPolling();
      setProcessLoading(false);
    }
  }

  function updateQuestion(clientId, field, value) {
    setQuestions((current) =>
      current.map((question) =>
        question.clientId !== clientId
          ? question
          : {
              ...question,
              [field]: field === "displayColumns" ? normalizeDisplayColumns(value) : value,
              unit:
                field === "answerType"
                  ? value === "number"
                    ? question.unit
                    : ""
                  : field === "unit"
                    ? value
                    : question.unit
            }
      )
    );
  }

  function getActiveProcessedDocIds() {
    const records = processResult?.records;
    if (!Array.isArray(records)) {
      return [];
    }

    const usableDocIds = new Set();
    for (const record of records) {
      if (!record?.doc_id || record.status === "FAILED" || record.action === "DRY_RUN") {
        continue;
      }

      const wasAlreadyIndexed = record.status === "INDEXED" && record.action === "SKIP";
      const wasIndexedDuringRun = Boolean(record.index_result);
      if (wasAlreadyIndexed || wasIndexedDuringRun) {
        usableDocIds.add(record.doc_id);
      }
    }

    return [...usableDocIds];
  }

  function addQuestion() {
    setQuestions((current) => [
      ...current,
      makeQuestion({
      })
    ]);
  }

  function removeQuestion(clientId) {
    setQuestions((current) => current.filter((question) => question.clientId !== clientId));
  }

  async function handleRunQuestions() {
    setQuestionError("");
    setQuestionCancelRequested(false);
    const docIds = getActiveProcessedDocIds();
    if (docIds.length === 0) {
      setQuestionError("Process documents before running questions.");
      return;
    }

    setQuestionLoading(true);
    setQaProgress({
      status: "running",
      current_question_index: 0,
      total_questions: questions.length,
      current_question_text: null,
      current_document_index: 0,
      total_documents: 0,
      current_document_name: null,
      completed_pairs: 0,
      total_pairs: 0,
      percent_complete: 0,
      message: "Preparing question run.",
      error: null
    });
    startQaProgressPolling();

    try {
      const payload = await runQuestions({
        doc_ids: docIds,
        output_prefix: "ui_results",
        questions: questions.map((question) => ({
          question_name: question.questionName,
          question_text: question.questionText,
          answer_type: question.answerType,
          unit: question.answerType === "number" ? question.unit || null : null,
          description: question.description || null,
          ranking_direction: question.rankingDirection || null
        }))
      });
      setQuestionResult({
        ...payload,
        questions: attachDisplayColumns(payload.questions, questions)
      });
    } catch (error) {
      setQuestionError(error.message);
    } finally {
      await pollQaProgress();
      stopQaProgressPolling();
      setQuestionCancelRequested(false);
      setQuestionLoading(false);
    }
  }

  async function handleCancelQuestionRun() {
    setQuestionError("");
    setQuestionCancelRequested(true);
    try {
      const payload = await cancelRunQuestions();
      setQaProgress(payload);
    } catch (error) {
      setQuestionCancelRequested(false);
      setQuestionError(error.message);
    }
  }

  return (
    <div className="appShell">
      <header className="hero">
        <div className="heroCopy">
          <p className="eyebrow">Local-first legal document RAG</p>
          <h1>CBA Search Workspace</h1>
          <p>
            Process collective agreements, define question sets, and review structured answers
            across many documents without leaving your local machine.
          </p>
        </div>
      </header>

      <main className="mainStack">
        <ProcessDocumentsPanel
          form={processForm}
          onChange={updateProcessField}
          onSubmit={handleProcessDocuments}
          loading={processLoading}
          progress={processProgress}
          summary={processResult?.summary}
          records={processResult?.records}
          error={processError}
        />

        <QuestionBuilderPanel
          questions={questions}
          onQuestionChange={updateQuestion}
          onAddQuestion={addQuestion}
          onRemoveQuestion={removeQuestion}
          onRunQuestions={handleRunQuestions}
          onCancelRun={handleCancelQuestionRun}
          loading={questionLoading}
          cancelRequested={questionCancelRequested}
          progress={qaProgress}
          error={questionError}
        />

        <ResultsTable resultSet={visibleQuestionResult} />
      </main>
    </div>
  );
}
