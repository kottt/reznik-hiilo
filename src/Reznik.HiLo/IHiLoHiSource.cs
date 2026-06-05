namespace Reznik.HiLo;

/// <summary>
/// Provides new <c>hi</c> values for the HiLo generator.
/// </summary>
/// <remarks>
/// Production implementations must reserve the returned value atomically and
/// durably across all application instances before returning it. A returned
/// <c>hi</c> value must never be reused after crashes, rollbacks, retries, or
/// caller cancellation. Database sequences and transactional counter updates are
/// typical safe implementations; read-then-write patterns such as
/// <c>SELECT MAX(id) + 1</c> are not safe.
/// </remarks>
public interface IHiLoHiSource
{
    /// <summary>
    /// Reserves and returns the next durable <c>hi</c> value.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the storage call.</param>
    /// <returns>A globally unique, monotonically increasing <c>hi</c> value.</returns>
    ValueTask<long> GetNextHiAsync(CancellationToken cancellationToken = default);
}
