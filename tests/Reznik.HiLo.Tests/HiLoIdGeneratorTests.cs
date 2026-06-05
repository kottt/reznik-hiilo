using System.Collections.Concurrent;
using Reznik.HiLo;

namespace Reznik.HiLo.Tests;

public sealed class HiLoIdGeneratorTests
{
    [Fact]
    public async Task NextIdAsync_GeneratesUniqueIdsUnderHighConcurrency()
    {
        const int blockSize = 128;
        const int workers = 64;
        const int idsPerWorker = 2_000;

        var hiSource = new CountingHiSource();
        var generator = new HiLoIdGenerator(hiSource, blockSize);
        var ids = new ConcurrentDictionary<long, byte>();

        var tasks = Enumerable.Range(0, workers)
            .Select(_ => Task.Run(async () =>
            {
                for (var i = 0; i < idsPerWorker; i++)
                {
                    var id = await generator.NextIdAsync();
                    Assert.True(ids.TryAdd(id, 0), $"Duplicate ID was generated: {id}");
                }
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        var expectedCount = workers * idsPerWorker;
        Assert.Equal(expectedCount, ids.Count);
        Assert.Equal(Enumerable.Range(0, expectedCount).Select(i => (long)i), ids.Keys.Order());
        Assert.Equal((expectedCount + blockSize - 1) / blockSize, hiSource.ReservationCount);
    }

    [Fact]
    public async Task NextIdAsync_ReservesHiOnlyAfterCurrentRangeIsExhausted()
    {
        var hiSource = new CountingHiSource();
        var generator = new HiLoIdGenerator(hiSource, blockSize: 4);

        Assert.Equal(0, await generator.NextIdAsync());
        Assert.Equal(1, await generator.NextIdAsync());
        Assert.Equal(2, await generator.NextIdAsync());
        Assert.Equal(3, await generator.NextIdAsync());
        Assert.Equal(1, hiSource.ReservationCount);

        Assert.Equal(4, await generator.NextIdAsync());
        Assert.Equal(2, hiSource.ReservationCount);
    }

    [Fact]
    public async Task NextIdAsync_AllowsOnlyOneConcurrentHiReservationPerExhaustedRange()
    {
        const int blockSize = 8;
        const int callers = 64;

        var hiSource = new DelayedHiSource(TimeSpan.FromMilliseconds(30));
        var generator = new HiLoIdGenerator(hiSource, blockSize);

        var ids = await Task.WhenAll(Enumerable.Range(0, callers).Select(_ => generator.NextIdAsync().AsTask()));

        Assert.Equal(callers, ids.Distinct().Count());
        Assert.Equal((callers + blockSize - 1) / blockSize, hiSource.ReservationCount);
        Assert.Equal(1, hiSource.MaxConcurrentReservations);
    }

    [Fact]
    public async Task NextIdAsync_HonorsCancellationWhileWaitingForRefresh()
    {
        var releaseRefresh = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hiSource = new BlockingHiSource(releaseRefresh.Task);
        var generator = new HiLoIdGenerator(hiSource, blockSize: 1);

        var refreshingCaller = generator.NextIdAsync().AsTask();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await generator.NextIdAsync(cts.Token));

        releaseRefresh.SetResult();
        Assert.Equal(0, await refreshingCaller);
    }

    [Fact]
    public async Task NextIdAsync_CallerCancellationDoesNotCancelSharedRefresh()
    {
        var releaseRefresh = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hiSource = new BlockingHiSource(releaseRefresh.Task);
        var generator = new HiLoIdGenerator(hiSource, blockSize: 1);

        using var cts = new CancellationTokenSource();
        var canceledCaller = generator.NextIdAsync(cts.Token).AsTask();

        await hiSource.WaitUntilCalledAsync();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await canceledCaller);

        releaseRefresh.SetResult();

        Assert.Equal(0, await generator.NextIdAsync());
        Assert.Equal(1, hiSource.ReservationCount);
    }

    [Fact]
    public async Task NextIdAsync_RetriesAfterHiSourceFailure()
    {
        var hiSource = new FailingOnceHiSource();
        var generator = new HiLoIdGenerator(hiSource, blockSize: 4);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await generator.NextIdAsync());

        Assert.Equal(0, await generator.NextIdAsync());
        Assert.Equal(2, hiSource.Attempts);
    }

    [Fact]
    public async Task NextIdAsync_RejectsHiValuesThatOverflowInt64Ids()
    {
        var hiSource = new FixedHiSource(long.MaxValue);
        var generator = new HiLoIdGenerator(hiSource, blockSize: 2);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await generator.NextIdAsync());
    }

    [Fact]
    public async Task NextIdAsync_RejectsNegativeHiValues()
    {
        var hiSource = new FixedHiSource(-1);
        var generator = new HiLoIdGenerator(hiSource);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await generator.NextIdAsync());
    }

    [Fact]
    public async Task NextIdAsync_GeneratesUniqueIdsAcrossGeneratorsSharingOneHiSource()
    {
        const int blockSize = 16;
        var hiSource = new CountingHiSource();
        var first = new HiLoIdGenerator(hiSource, blockSize);
        var second = new HiLoIdGenerator(hiSource, blockSize);

        var ids = await Task.WhenAll(
            Enumerable.Range(0, blockSize).Select(_ => first.NextIdAsync().AsTask())
                .Concat(Enumerable.Range(0, blockSize).Select(_ => second.NextIdAsync().AsTask())));

        Assert.Equal(blockSize * 2, ids.Distinct().Count());
        Assert.Equal(2, hiSource.ReservationCount);
    }


    private sealed class CountingHiSource(long firstHi = 0) : IHiLoHiSource
    {
        private long _nextHi = firstHi - 1;

        public long ReservationCount => Volatile.Read(ref _nextHi) + 1;

        public ValueTask<long> GetNextHiAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(Interlocked.Increment(ref _nextHi));
        }
    }

    private sealed class DelayedHiSource(TimeSpan delay) : IHiLoHiSource
    {
        private long _nextHi = -1;
        private int _activeReservations;
        private int _maxConcurrentReservations;

        public long ReservationCount => Volatile.Read(ref _nextHi) + 1;

        public int MaxConcurrentReservations => Volatile.Read(ref _maxConcurrentReservations);

        public async ValueTask<long> GetNextHiAsync(CancellationToken cancellationToken = default)
        {
            var active = Interlocked.Increment(ref _activeReservations);
            UpdateMax(active);

            try
            {
                await Task.Delay(delay, cancellationToken);
                return Interlocked.Increment(ref _nextHi);
            }
            finally
            {
                Interlocked.Decrement(ref _activeReservations);
            }
        }

        private void UpdateMax(int active)
        {
            while (true)
            {
                var current = Volatile.Read(ref _maxConcurrentReservations);
                if (active <= current || Interlocked.CompareExchange(ref _maxConcurrentReservations, active, current) == current)
                {
                    return;
                }
            }
        }
    }

    private sealed class FailingOnceHiSource : IHiLoHiSource
    {
        private long _nextHi = -1;
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        public ValueTask<long> GetNextHiAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (Interlocked.Increment(ref _attempts) == 1)
            {
                throw new InvalidOperationException("Simulated hi-source failure.");
            }

            return ValueTask.FromResult(Interlocked.Increment(ref _nextHi));
        }
    }

    private sealed class FixedHiSource(long hi) : IHiLoHiSource
    {
        public ValueTask<long> GetNextHiAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(hi);
        }
    }

    private sealed class BlockingHiSource(Task releaseRefresh) : IHiLoHiSource
    {
        private readonly TaskCompletionSource _called = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private long _nextHi = -1;

        public long ReservationCount => Volatile.Read(ref _nextHi) + 1;

        public Task WaitUntilCalledAsync() => _called.Task;

        public async ValueTask<long> GetNextHiAsync(CancellationToken cancellationToken = default)
        {
            _called.TrySetResult();
            await releaseRefresh.WaitAsync(cancellationToken);
            return Interlocked.Increment(ref _nextHi);
        }
    }
}
