import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/choose-folder": "http://127.0.0.1:8000",
      "/health": "http://127.0.0.1:8000",
      "/process-documents": "http://127.0.0.1:8000",
      "/process-progress": "http://127.0.0.1:8000",
      "/qa-progress": "http://127.0.0.1:8000",
      "/run-questions": "http://127.0.0.1:8000",
      "/latest-results": "http://127.0.0.1:8000"
    }
  }
});
