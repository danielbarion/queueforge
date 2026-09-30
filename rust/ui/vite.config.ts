import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev proxy: Vite on :5173 → management API on :15672.
// Cookie auth works same-origin because the browser talks only to Vite;
// Vite forwards /api, /healthz, /readyz to the broker.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:15672",
        changeOrigin: true,
      },
      "/healthz": {
        target: "http://127.0.0.1:15672",
        changeOrigin: true,
      },
      "/readyz": {
        target: "http://127.0.0.1:15672",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
  },
});
