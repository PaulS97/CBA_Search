function isFailureRecord(record) {
  return record?.status === "FAILED" || Boolean(record?.error);
}

function sortRecordsForDisplay(records) {
  return records
    .map((record, index) => ({ record, index }))
    .sort((left, right) => {
      const leftFailureRank = isFailureRecord(left.record) ? 1 : 0;
      const rightFailureRank = isFailureRecord(right.record) ? 1 : 0;
      if (leftFailureRank !== rightFailureRank) {
        return leftFailureRank - rightFailureRank;
      }

      const leftName = (left.record.filename || left.record.pdf_path || "").toLowerCase();
      const rightName = (right.record.filename || right.record.pdf_path || "").toLowerCase();
      if (leftName !== rightName) {
        return leftName.localeCompare(rightName);
      }

      return left.index - right.index;
    })
    .map(({ record }) => record);
}

export default function ProcessDocumentsPanel({
  form,
  onChange,
  onSubmit,
  loading,
  progress,
  summary,
  records,
  error
}) {
  const totalDocuments = progress?.total_documents ?? 0;
  const completedDocuments = progress?.completed_documents ?? 0;
  const progressPercent =
    totalDocuments > 0 ? Math.min(100, Math.round((completedDocuments / totalDocuments) * 100)) : 0;
  const showProgress = loading || progress?.status === "running";
  const displayRecords = records?.length ? sortRecordsForDisplay(records) : [];

  return (
    <section className="panel">
      <div className="panelHeader">
        <div>
          <p className="eyebrow">Step 1</p>
          <h2>Process Documents</h2>
        </div>
        <p className="panelHint">
          Point the tool at a folder of agreements and it will only run the missing pipeline steps.
        </p>
      </div>

      <div className="formGrid">
        <label className="field fieldWide">
          <span>Root Folder Path</span>
          <input
            type="text"
            value={form.root}
            onChange={(event) => onChange("root", event.target.value)}
            placeholder="/Users/paulseham/Documents/CBA_Search/Industry Data Project/Air Canada"
          />
        </label>

        <label className="field">
          <span>File Name Query</span>
          <input
            type="text"
            value={form.nameContains}
            onChange={(event) => onChange("nameContains", event.target.value)}
            placeholder="cba or (collective and agreement)"
          />
        </label>

        <div className="checkGrid fieldWide">
          <label className="checkCard">
            <input
              type="checkbox"
              checked={form.dryRun}
              onChange={(event) => onChange("dryRun", event.target.checked)}
            />
            <div>
              <strong>Dry Run</strong>
              <span>Discover and classify only</span>
            </div>
          </label>

          <label className="checkCard">
            <input
              type="checkbox"
              checked={form.force}
              onChange={(event) => onChange("force", event.target.checked)}
            />
            <div>
              <strong>Force Reprocess</strong>
              <span>Re-run extract, chunk, and index</span>
            </div>
          </label>
        </div>
      </div>

      <div className="actionRow">
        <button className="primaryButton" onClick={onSubmit} disabled={loading}>
          {loading ? "Processing..." : "Process Documents"}
        </button>
        {error ? <p className="errorText">{error}</p> : null}
      </div>

      {showProgress ? (
        <div className="progressCard">
          <div className="progressHeader">
            <strong>
              {progress?.phase === "starting" && totalDocuments === 0
                ? "Preparing ingest run"
                : "Processing documents"}
            </strong>
            <span>
              {totalDocuments > 0 ? `${completedDocuments} / ${totalDocuments}` : "Scanning..."}
            </span>
          </div>
          <div className="progressBarTrack" aria-hidden="true">
            <div
              className={`progressBarFill ${totalDocuments === 0 ? "progressBarFill-indeterminate" : ""}`}
              style={totalDocuments > 0 ? { width: `${progressPercent}%` } : undefined}
            />
          </div>
          <p className="progressMeta">
            {progress?.current_document ? (
              <>
                Current document: <strong>{progress.current_document}</strong>
              </>
            ) : (
              "Discovering matching PDFs and checking cache state."
            )}
          </p>
        </div>
      ) : null}

      <div className="summaryGrid">
        <SummaryCard label="Candidates Found" value={summary?.candidates_found ?? 0} />
        <SummaryCard label="Processed Full" value={summary?.processed_full ?? 0} />
        <SummaryCard
          label="Processed Chunk + Index"
          value={summary?.processed_chunk_and_index ?? 0}
        />
        <SummaryCard label="Processed Index Only" value={summary?.processed_index_only ?? 0} />
        <SummaryCard label="Skipped Indexed" value={summary?.skipped_indexed ?? 0} />
        <SummaryCard label="Failed" value={summary?.failed ?? 0} tone="warning" />
      </div>

      {displayRecords.length ? (
        <div className="statusListViewport">
          <div className="statusList">
            {displayRecords.map((record) => (
              <div className="statusRow" key={`${record.doc_id}-${record.pdf_path}`}>
                <span className={`statusPill status-${record.status.toLowerCase()}`}>
                  {record.status}
                </span>
                <div className="statusText">
                  <strong>{record.filename}</strong>
                  <span>{record.action}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </section>
  );
}

function SummaryCard({ label, value, tone = "default" }) {
  return (
    <div className={`summaryCard summaryCard-${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}
