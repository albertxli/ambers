import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// Development: `bun run dev` serves the page with hot reload and passes
// API calls on to a running `am open`. Start that with a fixed token and
// port, for example:
//
//   AMBERS_DEV_TOKEN=dev am open tests/data/survey.sav --no-browser --port 2627 --keep-alive
//
// The proxy speaks to the server as the command line does (a bearer
// token, the server's own host name, no foreign Origin), so the server's
// checks stay exactly as they are in a normal run.
const port = process.env.AMBERS_PORT ?? "2627";
const token = process.env.AMBERS_DEV_TOKEN ?? "dev";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    // Embedded into the `ambers` binary by crates/ambers-web/build.rs.
    outDir: "../crates/ambers-web/dist",
    emptyOutDir: true,
    target: "es2022",
  },
  server: {
    proxy: {
      "/api": {
        target: `http://127.0.0.1:${port}`,
        changeOrigin: true,
        headers: { Authorization: `Bearer ${token}` },
        configure: (proxy) => {
          proxy.on("proxyReq", (request) => request.removeHeader("origin"));
        },
      },
    },
  },
  test: {
    include: ["src/**/*.test.ts"],
  },
});
