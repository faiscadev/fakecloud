using Xunit;

namespace FakeCloud.Tests;

/// <summary>
/// E2E tests against a real fakecloud server. The <see cref="FakeCloudServer"/>
/// collection fixture spawns the binary (or reuses one at
/// <c>FAKECLOUD_ENDPOINT</c>) and fails loudly if it cannot be reached, so
/// these tests always run for real and never pass vacuously.
/// </summary>
[Collection(FakeCloudServerCollection.Name)]
public sealed class E2ETests : IDisposable
{
    private readonly FakeCloudClient _fc;

    public E2ETests(FakeCloudServer server)
    {
        _fc = new FakeCloudClient(server.Endpoint);
        _fc.ResetAsync().GetAwaiter().GetResult();
    }

    public void Dispose() => _fc.Dispose();

    [Fact]
    public async Task HealthReturnsServerStatus()
    {
        var health = await _fc.HealthAsync();
        Assert.Equal("ok", health.Status);
        Assert.False(string.IsNullOrEmpty(health.Version));
        Assert.NotEmpty(health.Services!);
    }

    [Fact]
    public async Task ResetClearsState()
    {
        var reset = await _fc.ResetAsync();
        Assert.Equal("ok", reset.Status);
        var queues = await _fc.Sqs.GetMessagesAsync();
        Assert.True(queues.Queues is null || queues.Queues.Count == 0);
    }

    [Fact]
    public async Task ResetServiceClearsOneService()
    {
        var result = await _fc.ResetServiceAsync("sqs");
        Assert.Equal("sqs", result.Reset);
    }

    [Fact]
    public async Task SesEmailsStartsEmpty()
    {
        var emails = await _fc.Ses.GetEmailsAsync();
        Assert.True(emails.Emails is null || emails.Emails.Count == 0);
    }

    [Fact]
    public async Task SnsCertPemIsPemEncoded()
    {
        var pem = await _fc.Sns.GetCertPemAsync();
        Assert.Contains("BEGIN CERTIFICATE", pem);
    }

    [Fact]
    public async Task BedrockResponseRulesRoundTrip()
    {
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
        var err = await Assert.ThrowsAsync<FakeCloudException>(
            () => _fc.Cognito.ConfirmUserAsync(new ConfirmUserRequest("pool-missing", "nobody")));
        Assert.Equal(404, err.Status);
    }

    [Fact]
    public async Task TickProcessorsReturnCounts()
    {
        var ttl = await _fc.DynamoDb.TickTtlAsync();
        Assert.True(ttl.ExpiredItems >= 0);
        var lifecycle = await _fc.S3.TickLifecycleAsync();
        Assert.True(lifecycle.ExpiredObjects >= 0);
        var expiration = await _fc.Sqs.TickExpirationAsync();
        Assert.True(expiration.ExpiredMessages >= 0);
    }
}
