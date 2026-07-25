module.exports = {
  moduleFileExtensions: ["ts", "js"],
  transform: {
    '^.+\\.(ts|tsx)$': ['ts-jest', { tsconfig: 'test/tsconfig.json' }]
  },
  coverageDirectory: "coverage",
  // Only the shipped library. Test doubles were being measured too, which dragged the reported
  // numbers around for reasons that say nothing about the code under test.
  collectCoverageFrom: ["src/**/*.ts"],
  // A floor, not a target — set just under the current numbers so a regression fails the build while
  // ordinary refactoring does not. Raise it when coverage rises; do not lower it to make a build pass.
  coverageThreshold: {
    global: {
      statements: 95,
      branches: 88,
      functions: 95,
      lines: 96
    }
  },
  verbose: true,
  testMatch: ["**/*.spec.(ts)"],
  testEnvironment: "node",
}
