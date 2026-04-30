async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {})
    },
    ...options
  });

  if (!response.ok) {
    let detail = `Request failed with status ${response.status}`;
    try {
      const payload = await response.json();
      if (payload?.detail) {
        detail = payload.detail;
      }
    } catch {
      // Ignore JSON parse failures and keep the default message.
    }
    throw new Error(detail);
  }

  return response.json();
}

export function fetchLatestResults() {
  return request("/latest-results");
}

export function fetchProcessProgress() {
  return request("/process-progress");
}

export function fetchQaProgress() {
  return request("/qa-progress");
}

export function processDocuments(payload) {
  return request("/process-documents", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function chooseFolder() {
  return request("/choose-folder", {
    method: "POST"
  });
}

export function runQuestions(payload) {
  return request("/run-questions", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function cancelRunQuestions() {
  return request("/run-questions/cancel", {
    method: "POST"
  });
}
