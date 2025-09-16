import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    // Ensure TS/TSX extensionless imports resolve reliably on case-sensitive FS
    extensions: [
      '.mjs', '.js', '.ts', '.jsx', '.tsx', '.json'
    ],
  },
  server: {
    port: 5173,
    strictPort: true,
  },
})
