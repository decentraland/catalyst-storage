import { Readable } from 'stream'
import { isAbortError, markAsNonCancellationError, runStoreWithSignal } from '../src/cancellation'
import { bufferToStream } from '../src/content-item'

describe('runStoreWithSignal', () => {
  describe('when the abort hook throws', () => {
    let reason: Error
    let outcome: 'resolved' | unknown

    beforeEach(async () => {
      // The hook runs inside the signal's event dispatch: a teardown failure must neither escape
      // as an uncaught exception nor replace the caller's cancellation reason.
      reason = new Error('cancelled')
      const controller = new AbortController()
      const source = new Readable({ read() {} })
      let rejectOperation: (error: Error) => void = () => undefined
      const operation = new Promise<never>((_, reject) => {
        rejectOperation = reject
      })
      const failingHook = (): void => {
        // A hook that throws never reports tearing transport down, so the rejection it causes is
        // shaped like what the destroyed source produces (a premature close) — provenance for that
        // comes from the listener's own destroy, not from the hook.
        rejectOperation(Object.assign(new Error('Premature close'), { code: 'ERR_STREAM_PREMATURE_CLOSE' }))
        throw new Error('teardown exploded')
      }
      const pending = runStoreWithSignal(source, controller.signal, () => operation, failingHook)
      controller.abort(reason)
      outcome = await pending.then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should reject with the abort reason despite the hook failure', () => {
      expect(outcome).toBe(reason)
    })
  })

  describe('when destroying the source throws', () => {
    let reason: Error
    let transportError: Error
    let hookCalled: jest.Mock
    let outcome: 'resolved' | unknown

    beforeEach(async () => {
      // A failing stream teardown must not prevent the backend hook from running.
      reason = new Error('cancelled')
      const controller = new AbortController()
      const source = new Readable({ read() {} })
      source.destroy = (() => {
        throw new Error('destroy exploded')
      }) as typeof source.destroy
      let rejectOperation: (error: Error) => void = () => undefined
      const operation = new Promise<never>((_, reject) => {
        rejectOperation = reject
      })
      // The hook tears its transport down and that transport rejects with its own shape. This layer
      // does NOT credit transport shapes — a transport can raise one for reasons of its own — so the
      // rejection surfaces as itself; a backend that knows it caused the abort converts it to the
      // caller's reason at its own call site, as the S3 upload does.
      // The shape the AWS SDK v3 actually rejects an aborted request with. It is deliberately NOT
      // credited here — a transport can raise one for its own reasons — so it must surface as itself.
      transportError = Object.assign(new Error('transport torn down'), { name: 'AbortError', code: 'ABORT_ERR' })
      hookCalled = jest.fn(() => {
        rejectOperation(transportError)
      })
      const pending = runStoreWithSignal(source, controller.signal, () => operation, hookCalled)
      controller.abort(reason)
      outcome = await pending.then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should still run the abort hook', () => {
      expect(hookCalled).toHaveBeenCalled()
    })

    it('should surface the transport error it cannot attribute to the cancellation', () => {
      expect(outcome).toBe(transportError)
    })
  })

  describe('when a real failure races the abort', () => {
    let diskFullError: Error
    let outcome: 'resolved' | unknown

    beforeEach(async () => {
      // The operation fails for a reason the teardown did not cause (ENOSPC) while the signal
      // aborts: the real storage error must surface as itself, not as the cancellation reason.
      diskFullError = Object.assign(new Error('ENOSPC: no space left on device'), { code: 'ENOSPC' })
      const controller = new AbortController()
      const source = new Readable({ read() {} })
      let rejectOperation: (error: Error) => void = () => undefined
      const operation = new Promise<never>((_, reject) => {
        rejectOperation = reject
      })
      const pending = runStoreWithSignal(source, controller.signal, () => operation)
      controller.abort(new Error('cancelled'))
      rejectOperation(diskFullError)
      outcome = await pending.then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should surface the real error instead of the cancellation reason', () => {
      expect(outcome).toBe(diskFullError)
    })
  })

  describe('when the destroyed source rejects with a premature close', () => {
    let reason: Error
    let outcome: 'resolved' | unknown

    beforeEach(async () => {
      // The teardown-caused rejection shape (a destroyed source) IS translated to the reason.
      reason = new Error('cancelled')
      const controller = new AbortController()
      const source = new Readable({ read() {} })
      let rejectOperation: (error: Error) => void = () => undefined
      const operation = new Promise<never>((_, reject) => {
        rejectOperation = reject
      })
      const pending = runStoreWithSignal(source, controller.signal, () => operation)
      controller.abort(reason)
      rejectOperation(Object.assign(new Error('Premature close'), { code: 'ERR_STREAM_PREMATURE_CLOSE' }))
      outcome = await pending.then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should reject with the cancellation reason', () => {
      expect(outcome).toBe(reason)
    })
  })

  describe('when the source closed prematurely on its own before the abort', () => {
    let upstreamFault: Error
    let outcome: 'resolved' | unknown

    beforeEach(async () => {
      // The source died for a real upstream fault BEFORE the abort fired: the teardown destroyed
      // nothing, so the premature-close failure belongs to that fault and must surface as itself —
      // the public ERR_STREAM_PREMATURE_CLOSE shape alone is not proof of our teardown.
      upstreamFault = Object.assign(new Error('Premature close'), { code: 'ERR_STREAM_PREMATURE_CLOSE' })
      const controller = new AbortController()
      const source = new Readable({ read() {} })
      source.destroy()
      let rejectOperation: (error: Error) => void = () => undefined
      const operation = new Promise<never>((_, reject) => {
        rejectOperation = reject
      })
      const pending = runStoreWithSignal(source, controller.signal, () => operation)
      controller.abort(new Error('cancelled'))
      rejectOperation(upstreamFault)
      outcome = await pending.then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should surface the upstream fault instead of the cancellation reason', () => {
      expect(outcome).toBe(upstreamFault)
    })
  })

  describe('when the source had already ended but was not yet destroyed', () => {
    let prematureClose: Error
    let outcome: 'resolved' | unknown

    beforeEach(async () => {
      // An ended-but-undestroyed source (reachable with autoDestroy: false, or in the tick before an
      // auto-destroy lands) cannot be made to close prematurely by our teardown — the consumer
      // already got its 'end'. So a premature-close rejection here belongs to something else and
      // must surface as itself, even though the teardown did call destroy() as cleanup.
      prematureClose = Object.assign(new Error('Premature close'), { code: 'ERR_STREAM_PREMATURE_CLOSE' })
      const source = new Readable({ read() {}, autoDestroy: false })
      source.push(Buffer.from('fully-read'))
      source.push(null)
      source.resume()
      await new Promise<void>((resolve) => source.once('end', () => resolve()))
      expect(source.readableEnded).toBe(true)
      expect(source.destroyed).toBe(false)

      const controller = new AbortController()
      let rejectOperation: (error: Error) => void = () => undefined
      const operation = new Promise<never>((_, reject) => {
        rejectOperation = reject
      })
      const pending = runStoreWithSignal(source, controller.signal, () => operation)
      controller.abort(new Error('cancelled'))
      rejectOperation(prematureClose)
      outcome = await pending.then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should surface the premature close instead of the cancellation reason', () => {
      expect(outcome).toBe(prematureClose)
    })
  })

  describe('when an operation raises an abort-shaped error of its own', () => {
    let foreignAbortError: Error
    let outcome: 'resolved' | unknown

    beforeEach(async () => {
      // A custom stream or transport can raise an AbortError for reasons of its own that merely
      // coincide with the caller's cancellation. Nothing here handed it this signal, so the shape
      // alone must not earn translation — the caller needs to see the real failure.
      foreignAbortError = Object.assign(new Error('The custom transport aborted internally'), {
        name: 'AbortError',
        code: 'ABORT_ERR'
      })
      const controller = new AbortController()
      const source = new Readable({ read() {} })
      let rejectOperation: (error: Error) => void = () => undefined
      const operation = new Promise<never>((_, reject) => {
        rejectOperation = reject
      })
      const pending = runStoreWithSignal(source, controller.signal, () => operation)
      controller.abort(new Error('cancelled'))
      rejectOperation(foreignAbortError)
      outcome = await pending.then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should surface the foreign abort error instead of the cancellation reason', () => {
      expect(outcome).toBe(foreignAbortError)
    })
  })

  describe('when the abort reason is an explicit null', () => {
    let outcome: 'resolved' | unknown

    beforeEach(async () => {
      // The docs promise the caller observes their own cancellation cause — including null.
      const controller = new AbortController()
      controller.abort(null)
      const source = new Readable({ read() {} })
      outcome = await runStoreWithSignal(source, controller.signal, () => new Promise<never>(() => undefined)).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should reject with null instead of a synthesized error', () => {
      expect(outcome).toBe(null)
    })
  })

  describe('when a commit-phase error cannot be tagged', () => {
    let tagged: unknown

    beforeEach(() => {
      // A frozen error cannot carry the marker. Documented as acceptable degradation — it may then be
      // translated on abort — so what matters is that tagging does not throw over it.
      tagged = markAsNonCancellationError(Object.freeze(new Error('frozen commit failure')))
    })

    it('should return the error unchanged rather than throwing', () => {
      expect((tagged as Error).message).toBe('frozen commit failure')
    })
  })

  describe('when an error carries the ABORT_ERR code rather than the AbortError name', () => {
    it('should still be recognised as an abort shape', () => {
      // The shape the AWS SDK and some transports use; both spellings have to count.
      expect(isAbortError(Object.assign(new Error('torn down'), { code: 'ABORT_ERR' }))).toBe(true)
    })

    it('should not recognise an unrelated error', () => {
      expect(isAbortError(new Error('ENOSPC'))).toBe(false)
    })
  })

  describe('when the signal aborts with no reason at all', () => {
    let outcome: unknown

    beforeEach(async () => {
      // `abort()` with no argument gives a DOMException reason in Node, but a hand-rolled signal can leave it
      // undefined — and then a store must synthesize one rather than reject with `undefined`.
      const controller = new AbortController()
      const signal = Object.create(controller.signal, {
        aborted: { value: true },
        reason: { value: undefined }
      }) as AbortSignal
      outcome = await runStoreWithSignal(bufferToStream(Buffer.from('x')), signal, async () => 'stored').then(
        () => 'resolved',
        (error) => error
      )
    })

    it('should reject with a synthesized abort error', () => {
      expect((outcome as Error).message).toBe('The store operation was aborted.')
    })
  })
})
