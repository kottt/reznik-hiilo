namespace Reznik.HiLo;

/// <summary>
/// Thread-safe HiLo identifier generator optimized for highly concurrent callers.
/// </summary>
/// <remarks>
/// The hot path is a volatile read of the current range plus one atomic increment.
/// A single compare-exchange gate elects the caller that reserves a new
/// <c>hi</c> value when the current range is exhausted.
/// </remarks>
public sealed class HiLoIdGenerator
{
    /// <summary>
    /// Default number of <c>lo</c> values reserved for each <c>hi</c> value.
    /// </summary>
    public const int DefaultBlockSize = 1_024;

    private readonly IHiLoHiSource _hiSource;
    private readonly int _blockSize;
    private readonly int _spinBeforeYield;

    private Range? _range;
    private TaskCompletionSource? _refreshSignal;

    /// <summary>
    /// Creates a new generator.
    /// </summary>
    /// <param name="hiSource">Durable source of new <c>hi</c> values.</param>
    /// <param name="blockSize">Number of IDs generated from one <c>hi</c> value.</param>
    /// <param name="spinBeforeYield">
    /// Number of short spin iterations before an exhausted caller awaits the
    /// in-flight range refresh.
    /// </param>
    public HiLoIdGenerator(
        IHiLoHiSource hiSource,
        int blockSize = DefaultBlockSize,
        int spinBeforeYield = 32)
    {
        ArgumentNullException.ThrowIfNull(hiSource);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockSize);
        ArgumentOutOfRangeException.ThrowIfNegative(spinBeforeYield);

        _hiSource = hiSource;
        _blockSize = blockSize;
        _spinBeforeYield = spinBeforeYield;
    }

    /// <summary>
    /// Returns the next identifier.
    /// </summary>
    /// <remarks>
    /// In the common case this method completes synchronously and performs only
    /// one atomic increment. If the current range is exhausted, one caller starts
    /// the backing-source reservation and all other callers await the same
    /// refresh signal.
    /// </remarks>
    public ValueTask<long> NextIdAsync(CancellationToken cancellationToken = default)
    {
        return TryNext(out var id)
            ? ValueTask.FromResult(id)
            : NextIdSlowAsync(cancellationToken);
    }

    private bool TryNext(out long id)
    {
        var range = Volatile.Read(ref _range);
        if (range is not null && range.TryTake(out id))
        {
            return true;
        }

        id = default;
        return false;
    }

    private async ValueTask<long> NextIdSlowAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (TryNext(out var id))
            {
                return id;
            }

            await WaitForRefreshAsync(GetOrStartRefresh(), cancellationToken).ConfigureAwait(false);
        }
    }

    private Task GetOrStartRefresh()
    {
        var current = Volatile.Read(ref _refreshSignal);
        if (current is not null)
        {
            return current.Task;
        }

        var created = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var existing = Interlocked.CompareExchange(ref _refreshSignal, created, null);
        if (existing is not null)
        {
            return existing.Task;
        }

        if (CurrentRangeHasCapacity())
        {
            CompleteRefresh(created, exception: null);
            return created.Task;
        }

        _ = RefreshRangeAsync(created);
        return created.Task;
    }

    private bool CurrentRangeHasCapacity()
    {
        return Volatile.Read(ref _range)?.HasCapacity == true;
    }

    private async Task RefreshRangeAsync(TaskCompletionSource signal)
    {
        try
        {
            var hi = await _hiSource.GetNextHiAsync(CancellationToken.None).ConfigureAwait(false);
            Volatile.Write(ref _range, new Range(hi, _blockSize));
            CompleteRefresh(signal, exception: null);
        }
        catch (Exception exception)
        {
            CompleteRefresh(signal, exception);
        }
    }

    private void CompleteRefresh(TaskCompletionSource signal, Exception? exception)
    {
        Interlocked.CompareExchange(ref _refreshSignal, null, signal);

        if (exception is null)
        {
            signal.SetResult();
            return;
        }

        signal.SetException(exception);
    }

    private async ValueTask WaitForRefreshAsync(Task refreshTask, CancellationToken cancellationToken)
    {
        var spinner = new SpinWait();

        for (var i = 0; i < _spinBeforeYield && !refreshTask.IsCompleted; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            spinner.SpinOnce(sleep1Threshold: -1);
        }

        await refreshTask.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private sealed class Range
    {
        private readonly long _hi;
        private readonly int _blockSize;
        private readonly long _maxLo;
        private long _nextLo;

        internal Range(long hi, int blockSize)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(hi);

            var maxLo = blockSize - 1L;
            if (hi > (long.MaxValue - maxLo) / blockSize)
            {
                throw new InvalidOperationException(
                    $"The reserved hi value {hi} with block size {blockSize} exceeds the Int64 identifier space.");
            }

            _hi = hi;
            _blockSize = blockSize;
            _maxLo = maxLo;
        }

        internal bool HasCapacity => Volatile.Read(ref _nextLo) <= _maxLo;

        internal bool TryTake(out long id)
        {
            var lo = Interlocked.Increment(ref _nextLo) - 1L;
            if (lo <= _maxLo)
            {
                id = (_hi * _blockSize) + lo;
                return true;
            }

            id = default;
            return false;
        }
    }
}
