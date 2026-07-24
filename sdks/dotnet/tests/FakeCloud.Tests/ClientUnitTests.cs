using Xunit;

namespace FakeCloud.Tests;

public class ClientUnitTests
{
    [Fact]
    public void TrimsTrailingSlashesFromBaseUrl()
    {
        Assert.Equal("http://localhost:4566", FakeCloudClient.TrimTrailingSlashes("http://localhost:4566"));
        Assert.Equal("http://localhost:4566", FakeCloudClient.TrimTrailingSlashes("http://localhost:4566/"));
        Assert.Equal("http://localhost:4566", FakeCloudClient.TrimTrailingSlashes("http://localhost:4566///"));
    }

    [Fact]
    public void DefaultBaseUrlMatchesSiblingSdks()
    {
        var fc = new FakeCloudClient();
        Assert.Equal("http://localhost:4566", fc.BaseUrl);
    }

    [Fact]
    public void EncodePathTurnsSpacesIntoPercent20()
    {
        Assert.Equal("hello%20world", HttpTransport.EncodePath("hello world"));
        Assert.Equal("a%2Fb", HttpTransport.EncodePath("a/b"));
        Assert.Equal("plain", HttpTransport.EncodePath("plain"));
    }

    [Fact]
    public void ErrorCarriesStatusAndBody()
    {
        var err = new FakeCloudException(503, "upstream unavailable");
        Assert.Equal(503, err.Status);
        Assert.Equal("upstream unavailable", err.Body);
        Assert.Contains("503", err.Message);
        Assert.Contains("upstream unavailable", err.Message);
    }

    [Fact]
    public async Task NetworkFailureIsSurfacedAsFakeCloudException()
    {
        var fc = new FakeCloudClient("http://127.0.0.1:1");
        var err = await Assert.ThrowsAsync<FakeCloudException>(() => fc.HealthAsync());
        Assert.Equal(-1, err.Status);
    }

    [Fact]
    public void WsUrlSwapsSchemeAndAppendsStage()
    {
        var fc = new FakeCloudClient("http://localhost:4566");
        Assert.Equal(
            "ws://localhost:4566/_fakecloud/apigatewayv2/ws/api-1",
            fc.ApiGatewayV2.WsUrl("api-1"));
        Assert.Equal(
            "ws://localhost:4566/_fakecloud/apigatewayv2/ws/api-1?stage=prod",
            fc.ApiGatewayV2.WsUrl("api-1", "prod"));

        var https = new FakeCloudClient("https://fakecloud.example");
        Assert.StartsWith("wss://fakecloud.example/", https.ApiGatewayV2.WsUrl("api-1"));
    }
}
