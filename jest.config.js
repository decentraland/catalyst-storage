module.exports = {
  moduleFileExtensions: ["ts", "js"],
  transform: {
    '^.+\\.(ts|tsx)$': ['ts-jest', { tsconfig: 'test/tsconfig.json' }]
  },
  coverageDirectory: "coverage",
  verbose: true,
  testMatch: ["**/*.spec.(ts)"],
  testEnvironment: "node",
}
