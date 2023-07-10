import { createReadStream, readFileSync } from 'fs'
import { bufferToStream, streamToBuffer } from '../src/content-item'

describe('Buffer utils', () => {
  it('unit test small', async () => {
    const b = Buffer.from('123')
    const s = bufferToStream(b)
    expect(await streamToBuffer(s)).toEqual(b)
  })

  it('unit test small uses buffer', async () => {
    const b = Buffer.from('123')
    for await (const chunk of bufferToStream(b)) {
      expect(Buffer.isBuffer(chunk)).toBe(true)
    }
  })

  it('streamToBuffer package.json', async () => {
    const stream = createReadStream(__filename)
    const raw = readFileSync(__filename)
    expect(await streamToBuffer(stream)).toEqual(raw)
  })

  it('streamToBuffer package.json uses buffer', async () => {
    for await (const chunk of createReadStream(__filename)) {
      expect(Buffer.isBuffer(chunk)).toBe(true)
    }
  })

  it('unit test big', async () => {
    const b = Buffer.from(new Uint8Array(1000000).fill(0))
    const s = bufferToStream(b)
    expect(await streamToBuffer(s)).toEqual(b)
  })
})
