using Xunit;

namespace FakeCloud.Tests;

/// <summary>
/// E2E tests that require a running fakecloud server.
///
/// Start the server before running:
///   cargo run -- --port 4566
///
/// Then run:
///   dotnet test
///
/// Set FAKECLOUD_ENDPOINT to override the base URL (default:
/// http://localhost:4566). Each test returns early (passing vacuously) when
/// no server is reachable, mirroring the skip behavior of the sibling SDKs.
/// </summary>
public sealed class E2ETests : IAsyncLifetime
{
    private readonly string _endpoint =
        Environment.GetEnvironmentVariable("FAKECLOUD_ENDPOINT") ?? "http://localhost:4566";

    private FakeCloudClient? _fc;

    public async Task InitializeAsync()
    {
        var probe = new FakeCloudClient(_endpoint);
        try
        {
            await probe.HealthAsync();
        }
        catch (FakeCloudException)
        {
            return; // server not reachable — tests below no-op
        }
        _fc = probe;
        await _fc.ResetAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task HealthReturnsServerStatus()
    {
        if (_fc is null) return;
        var health = await _fc.HealthAsync();
        Assert.Equal("ok", health.Status);
        Assert.False(string.IsNullOrEmpty(health.Version));
        Assert.NotEmpty(health.Services!);
    }

    [Fact]
    public async Task ResetClearsState()
    {
        if (_fc is null) return;
        var reset = await _fc.ResetAsync();
        Assert.Equal("ok", reset.Status);
        var queues = await _fc.Sqs.GetMessagesAsync();
        Assert.True(queues.Queues is null || queues.Queues.Count == 0);
    }

    [Fact]
    public async Task ResetServiceClearsOneService()
    {
        if (_fc is null) return;
        var result = await _fc.ResetServiceAsync("sqs");
        Assert.Equal("sqs", result.Reset);
    }

    [Fact]
    public async Task SesEmailsStartsEmpty()
    {
        if (_fc is null) return;
        var emails = await _fc.Ses.GetEmailsAsync();
        Assert.True(emails.Emails is null || emails.Emails.Count == 0);
    }

    [Fact]
    public async Task SnsCertPemIsPemEncoded()
    {
        if (_fc is null) return;
        var pem = await _fc.Sns.GetCertPemAsync();
        Assert.Contains("BEGIN CERTIFICATE", pem);
    }

    [Fact]
    public async Task BedrockResponseRulesRoundTrip()
    {
        if (_fc is null) return;
        const string modelId = "anthropic.claude-3-haiku-20240307-v1:0";
        var set = await _fc.Bedrock.SetResponseRulesAsync(modelId, new[]
        {
            new BedrockResponseRule("buy now", """{"label":"spam"}"""),
            new BedrockResponseRule(null, """{"label":"ham"}"""),
        });
        Assert.Equal(modelId, set.ModelId);
        var cleared = await _fc.Bedrock.ClearResponseRulesAsync(modelId);
        Assert.Equal(modelId, cleared.ModelId);
    }

    [Fact]
    public async Task BedrockFaultQueueRoundTrip()
    {
        if (_fc is null) return;
        await _fc.Bedrock.QueueFaultAsync(new BedrockFaultRule("throttling"));
        var faults = await _fc.Bedrock.GetFaultsAsync();
        Assert.NotEmpty(faults.Faults!);
        await _fc.Bedrock.ClearFaultsAsync();
        var after = await _fc.Bedrock.GetFaultsAsync();
        Assert.True(after.Faults is null || after.Faults.Count == 0);
    }

    [Fact]
    public async Task ConfirmUnknownCognitoUserThrows404()
    {
        if (_fc is null) return;
        var err = await Assert.ThrowsAsync<FakeCloudException>(
            () => _fc.Cognito.ConfirmUserAsync(new ConfirmUserRequest("pool-missing", "nobody")));
        Assert.Equal(404, err.Status);
    }

    [Fact]
    public async Task TickProcessorsReturnCounts()
    {
        if (_fc is null) return;
        var ttl = await _fc.DynamoDb.TickTtlAsync();
        Assert.True(ttl.ExpiredItems >= 0);
        var lifecycle = await _fc.S3.TickLifecycleAsync();
        Assert.True(lifecycle.ExpiredObjects >= 0);
        var expiration = await _fc.Sqs.TickExpirationAsync();
        Assert.True(expiration.ExpiredMessages >= 0);
    }
}
