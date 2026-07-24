import { Readable } from 'stream'
import { runStoreWithSignal } from '../src/cancellation'

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
        // Teardown-shaped, as a real aborted transport rejects (e.g. S3 RequestAbortedError).
        rejectOperation(Object.assign(new Error('transport torn down'), { code: 'RequestAbortedError' }))
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
      hookCalled = jest.fn(() =>
        rejectOperation(Object.assign(new Error('transport torn down'), { code: 'RequestAbortedError' }))
      )
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

    it('should reject with the abort reason', () => {
      expect(outcome).toBe(reason)
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
})
