import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		include: ["contract/**/*.test.ts", "process/**/*.test.ts"],
		testTimeout: 60_000,
		hookTimeout: 60_000,
	},
});
