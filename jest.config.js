module.exports = {
  moduleFileExtensions: ["ts", "js"],
  transform: {
    '^.+\\.(ts|tsx)$': ['ts-jest', { tsconfig: 'test/tsconfig.json' }]
  },
  coverageDirectory: "coverage",
  // Only the shipped library. Test doubles were being measured too, which dragged the reported
  // numbers around for reasons that say nothing about the code under test.
  collectCoverageFrom: ["src/**/*.ts"],
  // A floor, not a target — set under the current numbers so a regression fails the build while ordinary
  // refactoring does not. Raise it when coverage rises; do not lower it to make a build pass.
  //
  // The MARGIN is deliberate and has to stay. Branch coverage here is not perfectly reproducible: several
  // branches are timing-dependent (cache eviction, pins, races), so repeated runs of the same tree measure a
  // spread of about 0.3%, and Linux CI has measured 0.1-0.3% below the same tree on macOS. The previous
  // branch floor of 88 sat inside that spread against a real 88.05-88.25, so the build was decided by which
  // way the dice fell rather than by the code. Keep each floor at least a point below the observed minimum.
  //
  // Observed over four runs at the time of writing: branches 91.24-91.45, statements 96.50-96.62,
  // functions 96.70, lines 97.66-97.73. Each floor below is at least a point under its observed minimum,
  // which is why only BRANCHES moves here: it gained three points and can carry the ratchet, while the other
  // three gained under one and a floor closer than that would be the same coin flip described above.
  coverageThreshold: {
    global: {
      statements: 95,
      branches: 90,
      functions: 95,
      lines: 96
    }
  },
  verbose: true,
  testMatch: ["**/*.spec.(ts)"],
  testEnvironment: "node",
}
