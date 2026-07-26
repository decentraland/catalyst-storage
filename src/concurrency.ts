/**
 * Maps `items` with at most `limit` operations in flight, preserving input order.
 *
 * The batch surfaces (`existMultiple`, `fileInfoMultiple`) take an unbounded list of ids from their
 * caller, so mapping them with `Promise.all` issued one operation per id ALL AT ONCE. A few thousand
 * ids then meant a few thousand simultaneous `stat`s on the folder-based backend — which exhausts the
 * process file-descriptor limit and fails the whole batch with EMFILE — or a few thousand concurrent
 * `HeadObject` requests on S3, which the service answers with 503 SlowDown. Neither is a load the
 * caller asked for; both are an artifact of how the batch was mapped.
 *
 * On failure this deliberately differs from `Promise.all`: no further items are STARTED, and the
 * rejection is raised only once every operation already in flight has settled. `Promise.all` rejects
 * on the first failure while the rest of the batch keeps running against a storage the caller has
 * already been told is broken.
 */
export async function mapWithConcurrency<T, R>(
  items: readonly T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const results = new Array<R>(items.length)
  let next = 0
  let failure: unknown
  let failed = false

  const worker = async (): Promise<void> => {
    while (next < items.length && !failed) {
      const index = next++
      try {
        results[index] = await fn(items[index], index)
      } catch (error) {
        // Keep the FIRST failure: it is the one that best explains why the batch stopped, and later
        // ones are frequently just the same fault observed again.
        if (!failed) {
          failed = true
          failure = error
        }
        return
      }
    }
  }

  // At least one worker whenever there is work. A limit of 0 or below — or a non-finite one, where
  // `Math.max(1, Math.min(NaN, n))` is still NaN and `Array.from({length: NaN})` is empty — would
  // otherwise spawn none and resolve with a fully-holed array as if every item had been mapped.
  //
  // The two non-finite values are NOT the same request, and collapsing both onto `items.length`
  // resolved a caller's arithmetic slip into exactly the unbounded fan-out this helper exists to
  // prevent. `Infinity` is a deliberate "no limit"; `NaN` (a limit parsed out of missing config, or
  // any `undefined` that reached a multiplication) carries no intent at all, so it falls back to the
  // safe end rather than the dangerous one.
  const bounded = Number.isFinite(limit)
    ? Math.max(1, Math.min(limit, items.length))
    : limit === Infinity
      ? items.length
      : 1
  const workers = items.length === 0 ? 0 : bounded
  await Promise.all(Array.from({ length: workers }, () => worker()))
  if (failed) throw failure
  return results
}
