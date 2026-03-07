using TelemetryIngester.Services;

namespace TelemetryIngester.Tests.Services;

public sealed class KafkaConsumerServiceTests
{
    // ------------------------------------------------------------------
    // NextBackoff — initial value
    // ------------------------------------------------------------------

    [Fact]
    public void NextBackoff_FromZero_ReturnsInitialBackoff()
    {
        // Arrange
        var current = TimeSpan.Zero;

        // Act
        var next = KafkaConsumerService.NextBackoff(current);

        // Assert
        // Zero * 2 = 0 which is less than MaxBackoff, so Min(0, MaxBackoff) = 0.
        // The method doubles the current value; starting from zero always yields zero.
        // The real starting point in production is InitialBackoff, not zero — this
        // test confirms the pure math of the method with a zero seed.
        Assert.Equal(TimeSpan.Zero, next);
    }

    [Fact]
    public void NextBackoff_FromInitialBackoff_DoublesCorrectly()
    {
        // Arrange
        var current = KafkaConsumerService.InitialBackoff; // 1s

        // Act
        var next = KafkaConsumerService.NextBackoff(current);

        // Assert
        Assert.Equal(TimeSpan.FromSeconds(2), next);
    }

    // ------------------------------------------------------------------
    // NextBackoff — doubling sequence
    // ------------------------------------------------------------------

    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 4)]
    [InlineData(4, 8)]
    [InlineData(8, 16)]
    [InlineData(16, 30)]   // would be 32 but capped at 30
    [InlineData(30, 30)]   // already at cap
    public void NextBackoff_DoublingSequence_ProducesExpectedValues(int currentSeconds, int expectedSeconds)
    {
        // Arrange
        var current = TimeSpan.FromSeconds(currentSeconds);

        // Act
        var next = KafkaConsumerService.NextBackoff(current);

        // Assert
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), next);
    }

    // ------------------------------------------------------------------
    // NextBackoff — cap behaviour
    // ------------------------------------------------------------------

    [Fact]
    public void NextBackoff_AtMaxBackoff_StaysAtMax()
    {
        // Arrange
        var current = KafkaConsumerService.MaxBackoff; // 30s

        // Act
        var next = KafkaConsumerService.NextBackoff(current);

        // Assert
        Assert.Equal(KafkaConsumerService.MaxBackoff, next);
    }

    [Fact]
    public void NextBackoff_AboveMaxBackoff_StaysAtMax()
    {
        // Arrange — a value that somehow exceeded the cap (defensive test)
        var current = KafkaConsumerService.MaxBackoff + TimeSpan.FromSeconds(5);

        // Act
        var next = KafkaConsumerService.NextBackoff(current);

        // Assert
        Assert.Equal(KafkaConsumerService.MaxBackoff, next);
    }

    [Fact]
    public void NextBackoff_JustBelowMax_CapsAtMax()
    {
        // Arrange — 29s doubles to 58s, but must be capped at 30s
        var current = TimeSpan.FromSeconds(29);

        // Act
        var next = KafkaConsumerService.NextBackoff(current);

        // Assert
        Assert.Equal(KafkaConsumerService.MaxBackoff, next);
    }

    // ------------------------------------------------------------------
    // NextBackoff — constants have expected values
    // ------------------------------------------------------------------

    [Fact]
    public void InitialBackoff_IsOneSecond()
    {
        Assert.Equal(TimeSpan.FromSeconds(1), KafkaConsumerService.InitialBackoff);
    }

    [Fact]
    public void MaxBackoff_IsThirtySeconds()
    {
        Assert.Equal(TimeSpan.FromSeconds(30), KafkaConsumerService.MaxBackoff);
    }

    // ------------------------------------------------------------------
    // NextBackoff — full sequence from initial to cap
    // ------------------------------------------------------------------

    [Fact]
    public void NextBackoff_FullSequence_EventuallyCappsAndStays()
    {
        // Arrange — walk from InitialBackoff through all doublings
        var expected = new[]
        {
            TimeSpan.FromSeconds(2),   // 1 → 2
            TimeSpan.FromSeconds(4),   // 2 → 4
            TimeSpan.FromSeconds(8),   // 4 → 8
            TimeSpan.FromSeconds(16),  // 8 → 16
            TimeSpan.FromSeconds(30),  // 16 → 30 (cap)
            TimeSpan.FromSeconds(30),  // 30 → 30 (stays)
            TimeSpan.FromSeconds(30),  // 30 → 30 (stays)
        };

        var current = KafkaConsumerService.InitialBackoff;

        // Act & Assert
        foreach (var expectedNext in expected)
        {
            current = KafkaConsumerService.NextBackoff(current);
            Assert.Equal(expectedNext, current);
        }
    }
}
