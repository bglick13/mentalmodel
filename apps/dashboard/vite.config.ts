import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    /** Fail fast if another zombie Vite is holding the port instead of silently using 5174+. */
    strictPort: true,
    proxy: {
      "/api": {
        target: process.env.VITE_MENTALMODEL_API_URL ?? "http://127.0.0.1:8765",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
