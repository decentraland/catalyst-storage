export default {
  moduleFileExtensions: ['ts', 'js'],
  transform: {
    '^.+\\.(ts|tsx)$': ['ts-jest', { tsconfig: 'test/tsconfig.json', useEsm: true }]
  },
  coverageDirectory: 'coverage',
  verbose: true,
  testMatch: ['**/*.spec.(ts)'],
  testEnvironment: 'node'
}
