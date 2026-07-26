const sdk = require('@dcl/eslint-config/sdk.config')

/**
 * Flat config (ESLint 9). `@dcl/eslint-config` v3 moved its toolchain from dependencies to peer
 * dependencies and exposes the same `sdk` ruleset through `sdk.config.js`, so the preset itself is
 * unchanged from the previous `.eslintrc.json` — only the format and the plugin ownership are.
 *
 * `.eslintignore` is not read by flat config; its entries live in the `ignores` block below.
 */
module.exports = [
  {
    ignores: ['coverage/', 'dist/', 'node_modules/', 'tmpbin', 'jest.config.js', 'eslint.config.js']
  },
  ...sdk,
  {
    files: ['**/*.ts'],
    languageOptions: {
      parserOptions: {
        // Both projects, so type-aware rules cover the tests as well as the shipped library.
        project: ['tsconfig.json', 'test/tsconfig.json']
      }
    },
    rules: {
      'prettier/prettier': [
        'error',
        {
          printWidth: 120,
          semi: false,
          singleQuote: true,
          trailingComma: 'none',
          tabWidth: 2
        }
      ]
    }
  }
]
