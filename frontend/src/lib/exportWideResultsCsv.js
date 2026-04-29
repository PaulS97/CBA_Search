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

function getDisplayColumns(question) {
  return {
    answer: true,
    notes: true,
    quote: true,
    ...(question.displayColumns || {})
  };
}

function escapeCsvValue(value) {
  const text = value === null || value === undefined ? "" : String(value);
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, "\"\"")}"`;
  }
  return text;
}

function buildHeaders(questions) {
  const headers = ["Filename", "Source Path"];

  for (const question of questions) {
    const displayColumns = getDisplayColumns(question);
    if (displayColumns.answer) {
      headers.push(`${question.question_name}: Answer / Value`);
    }
    if (displayColumns.notes) {
      headers.push(`${question.question_name}: Notes`);
    }
    if (displayColumns.quote) {
      headers.push(`${question.question_name}: Quote`);
    }
  }

  return headers;
}

function buildRows(resultSet) {
  return resultSet.wide_results.map((row) => {
    const values = [row.filename || "", row.source_path || ""];

    for (const question of resultSet.questions) {
      const cell = row.answers?.[question.question_id] || {};
      const displayColumns = getDisplayColumns(question);
      if (displayColumns.answer) {
        values.push(formatAnswer(cell, question));
      }
      if (displayColumns.notes) {
        values.push(cell.notes || "");
      }
      if (displayColumns.quote) {
        values.push(cell.quote || "");
      }
    }

    return values;
  });
}

function buildCsvText(headers, rows) {
  const lines = [
    headers.map(escapeCsvValue).join(","),
    ...rows.map((row) => row.map(escapeCsvValue).join(",")),
  ];
  return `${lines.join("\r\n")}\r\n`;
}

function makeExportFilename() {
  const timestamp = new Date().toISOString().replace(/[:]/g, "-").replace(/\..+/, "");
  return `cba_search_results_${timestamp}.csv`;
}

export function exportWideResultsCsv(resultSet) {
  if (!resultSet?.wide_results?.length || !resultSet?.questions?.length) {
    return;
  }

  const headers = buildHeaders(resultSet.questions);
  const rows = buildRows(resultSet);
  const csvText = buildCsvText(headers, rows);
  const blob = new Blob([csvText], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");

  link.href = url;
  link.download = makeExportFilename();
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}
