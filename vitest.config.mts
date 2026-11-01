import {defineConfig} from 'vitest/config';
export default defineConfig({test:{testTimeout:30000,include:['test/**/*.test.ts'],exclude:['test/**/*.integration.test.ts'],coverage:{provider:'v8',include:['dist/**/*.js'],exclude:['dist/main.js','dist/telemetry.js']}}});
