import { readdirSync, readFileSync, statSync } from 'fs'
import path from 'path'
import * as packageRoot from '../src'

/**
 * The `@public` tag and the actual package surface have to agree.
 *
 * `src/index.ts` lists the surface explicitly and the `exports` map blocks `dist/*` deep imports, so a
 * `@public` tag on something the root does not re-export documents an API nobody can reach — which is
 * exactly what had happened to fourteen symbols across eight files after the surface was deliberately
 * shrunk. This walks the source rather than trusting a review to catch the next one.
 */
const SRC = path.join(__dirname, '..', 'src')

/** Every `.ts` file under `src/`, so a new module cannot be added outside this check. */
function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((entry) => {
    const full = path.join(dir, entry)
    if (statSync(full).isDirectory()) return sourceFiles(full)
    return full.endsWith('.ts') ? [full] : []
  })
}

/** The declaration a doc comment belongs to: the first exported name after the tag. */
const EXPORTED_DECLARATION =
  /^\s*export\s+(?:declare\s+)?(?:async\s+)?(?:function|class|const|let|type|interface|enum)\s+([A-Za-z_$][\w$]*)/m

function taggedPublic(): { file: string; symbol: string }[] {
  const found: { file: string; symbol: string }[] = []
  for (const file of sourceFiles(SRC)) {
    const text = readFileSync(file, 'utf8')
    for (const tag of text.matchAll(/@public\b/g)) {
      const declaration = EXPORTED_DECLARATION.exec(text.slice(tag.index! + tag[0].length))
      if (declaration) found.push({ file: path.relative(SRC, file), symbol: declaration[1] })
    }
  }
  return found
}

/** Type-only exports have no runtime presence, so they come from the index's own re-export list. */
function typeOnlyExports(): Set<string> {
  const index = readFileSync(path.join(SRC, 'index.ts'), 'utf8')
  const names = new Set<string>()
  for (const group of index.matchAll(/export type \{([^}]*)\}/g)) {
    for (const name of group[1].split(',')) names.add(name.trim())
  }
  return names
}

describe('when a symbol is annotated @public', () => {
  let reachable: Set<string>
  let tagged: { file: string; symbol: string }[]

  beforeEach(() => {
    reachable = new Set([...Object.keys(packageRoot), ...typeOnlyExports()])
    tagged = taggedPublic()
  })

  it('should be reachable from the package root', () => {
    const unreachable = tagged.filter(({ symbol }) => !reachable.has(symbol))

    expect(unreachable).toEqual([])
  })

  it('should have found the tags at all, so a broken scan cannot pass vacuously', () => {
    expect(tagged.length).toBeGreaterThan(5)
  })
})
