import { defineConfig } from 'electron-vite'
import { resolve } from 'path'

export default defineConfig({
  main: {
    build: {
      outDir: 'dist/main',
      rollupOptions: {
        external: ['better-sqlite3']
      },
      lib: {
        entry: resolve(__dirname, 'electron/main/index.ts')
      }
    }
  },
  preload: {
    build: {
      outDir: 'dist/preload',
      rollupOptions: {
        external: ['better-sqlite3']
      },
      lib: {
        entry: resolve(__dirname, 'electron/preload/index.ts')
      }
    }
  },
  renderer: {}
})
