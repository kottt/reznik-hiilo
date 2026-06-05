# Reznik.HiLo

`Reznik.HiLo` is a small .NET 10 library that implements a production-oriented HiLo identifier generator for high-concurrency services.

## Architecture

HiLo splits ID generation into two responsibilities:

1. **Reserve `hi` durably** — usually with a database sequence or transactional counter.
2. **Allocate `lo` in memory** — generate `BlockSize` local IDs before reserving the next `hi`.

For a block size of `1024`, one database reservation produces `1024` IDs:

```text
id = hi * blockSize + lo
```

The public API is intentionally small:

- `IHiLoHiSource` — durable source of new `hi` values.
- `HiLoIdGenerator` — concurrent ID generator.

The generator accepts `blockSize` directly in its constructor instead of wrapping one setting in an options object.

## Concurrency model

The hot path of `HiLoIdGenerator.NextIdAsync` is intentionally boring:

1. Read the current range using `Volatile.Read`.
2. Reserve a `lo` value with `Interlocked.Increment`.
3. If the value is inside the range, return `hi * blockSize + lo`.

No `lock`, `Monitor`, `Mutex`, or `SemaphoreSlim` is used on the hot path.

When the current range is exhausted:

1. Multiple callers may observe exhaustion at the same time.
2. A single caller publishes a shared refresh signal with `Interlocked.CompareExchange`.
3. The refresh reserves the next `hi` without using an individual caller cancellation token, publishes a new range with `Volatile.Write`, and completes the shared signal.
4. Other callers spin briefly and then await that same signal instead of repeatedly yielding on the ThreadPool.
5. After the signal completes, callers retry the same simple path.

## Race conditions and prevention

### Duplicate `lo` values

**Race:** many threads request an ID from the same in-memory range concurrently.

**Prevention:** the range advances with `Interlocked.Increment`. Every caller receives a unique increment result, so two callers cannot receive the same `lo` value from the same range.

### Multiple database reservations for one exhausted range

**Race:** many threads discover exhaustion and attempt to fetch a new `hi` simultaneously.

**Prevention:** `Interlocked.CompareExchange` publishes exactly one shared refresh signal. Other callers await that signal instead of calling the database.

### Publishing a partially initialized range

**Race:** another thread could observe a range before all fields are initialized.

**Prevention:** ranges are immutable after construction and are published with `Volatile.Write`. Readers use `Volatile.Read`.

### Reusing an old exhausted range

**Race:** a caller can hold a reference to an exhausted range while another caller publishes a fresh one.

**Prevention:** overshooting an old range never returns an ID. The caller falls back to the slow path and retries by reading the latest range.

## Why this is lock-free or close to lock-free

The common path is lock-free in practice: one atomic increment reserves an ID, and at least one contending caller makes progress while the range has capacity.

The refill path has an unavoidable external dependency: the database. If the range is exhausted and the elected refresh is waiting on storage, other exhausted callers cannot complete until a new range is published. The implementation still avoids OS locks and blocking synchronization primitives; the compromise is a single-flight asynchronous refresh signal implemented with atomics. Individual caller cancellation can stop that caller from waiting, but it does not cancel the shared refresh needed by other callers.

## Comparison with a classic `lock` implementation

A classic implementation often looks like this:

```csharp
lock (_sync)
{
    if (lo > maxLo)
    {
        hi = FetchHiFromDatabase();
        lo = 0;
    }

    return hi * blockSize + lo++;
}
```

That approach is simple, but every ID request serializes through the same monitor. Under high load, even requests that only need an in-memory increment contend for the lock.

`Reznik.HiLo` keeps the database-minimizing behavior of classic HiLo while avoiding monitor contention for normal ID generation. Only exhausted ranges enter the slow path.

## Example

```csharp
IHiLoHiSource hiSource = new MyDatabaseHiSource();
var generator = new HiLoIdGenerator(hiSource, blockSize: 4096);

long id = await generator.NextIdAsync();
```

A production `IHiLoHiSource` must reserve values atomically and durably across every application instance before returning. Returned `hi` values must never be reused after crashes, rollbacks, retries, or caller cancellation. Safe approaches include:

- `SELECT NEXT VALUE FOR dbo.HiLoSequence` in SQL Server.
- `SELECT nextval('hilo_sequence')` in PostgreSQL.
- An atomic transactional update of a single-row counter table.

Unsafe read-then-write patterns such as `SELECT MAX(id) + 1` must not be used because concurrent processes can reserve the same range.

## Simplification rationale

The previous implementation worked, but it carried more structure than the problem needed. The refactor keeps the same concurrency behavior and removes incidental complexity.

| Before | After | Why |
| --- | --- | --- |
| Separate `HiLoOptions` class with one setting | `blockSize` constructor parameter plus `DefaultBlockSize` constant | One option does not justify an options abstraction. Direct parameters are clearer and still testable. |
| Separate internal `HiLoRange` file | Private nested `Range` inside `HiLoIdGenerator` | The range is an implementation detail used nowhere else. Nesting makes ownership explicit and reduces the surface area. |
| Public `InMemoryHiSource` in the library | Test-only counting sources in the test project | A fake storage provider is useful for tests, but production consumers should implement `IHiLoHiSource` against durable storage. |
| `_version` field and `Task.Yield()` polling to observe refresh completion | Shared `TaskCompletionSource` refresh signal | Waiters await one completion signal, avoiding ThreadPool churn during slow storage calls. |
| Per-call cancellation token passed into the shared storage reservation | Caller cancellation only cancels that caller's wait | One canceled request no longer aborts the range refresh needed by other callers. |
| Mutable static exhausted range sentinel | `null` means no current range | Avoids shared mutable state and cross-generator contention before first refresh. |
| Overflow detected by `checked` during ID allocation | Range-level validation before publishing | Overflow now fails explicitly at reservation time instead of surprising callers on the hot path. |
| Larger slow-path method with mixed concerns | Small helpers: `TryNext`, `GetOrStartRefresh`, `RefreshRangeAsync`, `WaitForRefreshAsync` | Each helper has one reason to change, lowering cyclomatic complexity without hiding the algorithm. |

## Multithreaded duplicate check

The test project includes a stress-style test that starts many tasks, generates IDs concurrently, stores them in a `ConcurrentDictionary<long, byte>`, and asserts:

- every insertion is unique;
- the final count is exactly the requested number of IDs;
- the database-like `hi` source was called only `ceil(idCount / blockSize)` times.

Run it with:

```bash
dotnet test
```

## Performance notes

Expected hot-path cost:

- one volatile reference read;
- one atomic increment;
- one arithmetic operation after range-level overflow validation.

Primary bottlenecks:

- durable `hi` source latency when a range is exhausted;
- too-small `blockSize`, which increases database reservations;
- extreme contention on the single `lo` counter cache line;
- shared-refresh wait time while durable storage is slow.

Larger block sizes reduce database traffic but increase the number of IDs that can be skipped after process crashes. This is a normal HiLo trade-off: IDs are unique, not guaranteed gap-free.

## License

MIT. See [LICENSE](LICENSE).
