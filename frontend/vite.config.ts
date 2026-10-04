import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// FastAPI serves the built app under /app. In development, Vite proxies every
// server route to the local API on port 8000 so the session cookie is shared.
const api = "http://127.0.0.1:8000";

export default defineConfig({
  base: "/app/",
  plugins: [react()],
  server: {
    proxy: {
      "/api": api,
      "/backlog": api,
      "/login": api,
      "/logout": api,
      "/assets/icon.png": api,
    },
  },
  test: {
    include: ["src/**/*.test.ts"],
  },
});
