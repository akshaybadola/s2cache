import path from "node:path";
import checker from "vite-plugin-checker";
import type { UserConfigFn } from "vite";
import react from '@vitejs/plugin-react';
import circleDependency from "vite-plugin-circular-dependency";

const getUserConfig: UserConfigFn = ({ command }) => ({
  // 1. Change base to '/' for standard browser routing
  base: '/',

  server: {
    port: Number(env.PORT) || 5173,
    // 2. Helpful for browser debugging
    open: true,
    host: true
  },
  plugins: [
    react(),
    // Remove viteSingleFile() during development as it can break HMR in browsers
    circleDependency(),
    ...(command === "build" ? [
      checker({ typescript: true }),
    ] : [])
  ],
  build: {
    emptyOutDir: true,
    sourcemap: true,
    outDir: "dist",
    // 3. Revert to standard code-splitting for browser performance
    rollupOptions: {
      input: path.resolve(__dirname, "index.html"),
    },
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
});
