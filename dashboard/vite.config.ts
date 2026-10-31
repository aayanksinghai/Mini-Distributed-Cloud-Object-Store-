import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// During `npm run dev`, proxy the real controller REST API so the dashboard runs same-origin
// exactly like it does behind Nginx in Docker Compose. Override with CONTROLLER_URL if needed.
const controllerUrl = 'http://localhost:8080';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': { target: controllerUrl, changeOrigin: true },
      '/actuator': { target: controllerUrl, changeOrigin: true },
    },
  },
});

