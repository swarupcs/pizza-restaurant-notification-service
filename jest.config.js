/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  verbose: true,
  collectCoverage: true,
  coverageProvider: "v8",
  collectCoverageFrom: [
    "src/**/*.ts",
    "server.ts",
    "!tests/**",
    "!**/node_modules/**",
  ],
  // Nothing here talks to a network or a database — every suite is a unit
  // test against mocked kafkajs and nodemailer — so the 5s default is fine.
  transform: {
    "^.+\\.tsx?$": ["ts-jest", {}],
  },
};
