import globals from "globals";
import pluginJs from "@eslint/js";
import tseslint from "typescript-eslint";

export default [
  {
    // Without this, `npm run build` lints its own compiled output on every
    // run after the first (build = lint && tsc, and tsc writes dist/).
    ignores: ["dist/**", "node_modules/**", "coverage/**"],
  },
  {
    languageOptions: {
      globals: globals.node,
    },
    rules: {
      // 'no-unused-vars': 'error',
    },
  },
  pluginJs.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["tests/**/*.ts"],
    languageOptions: {
      globals: { ...globals.node, ...globals.jest },
    },
    rules: {
      // `jest.mock("mod", () => require("./mock"))` is deliberate: jest.mock is
      // hoisted above const declarations, so a factory closing over a
      // top-level import would hit a temporal-dead-zone error.
      "@typescript-eslint/no-require-imports": "off",
      // The fixtures cast freely to stand in for Kafka payloads, which the
      // handlers accept as untyped `order` arguments anyway.
      "@typescript-eslint/no-explicit-any": "off",
    },
  },
];
