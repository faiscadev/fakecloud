using System.Net;
using System.Text;
using Xunit;

namespace FakeCloud.Tests;

/// <summary>
/// Tests against a minimal in-process HTTP server that plays back canned
/// fakecloud responses, verifying URL construction, JSON mapping, and error
/// handling without a running fakecloud binary.
/// </summary>
public sealed class FakeServerTests : IDisposable
{
    private readonly HttpListener _listener;
    private readonly string _baseUrl;
    private readonly Dictionary<string, (int Status, string Body)> _routes = new();
    private readonly List<(string Method, string Path, string Body)> _requests = new();
    private readonly Task _serveLoop;

    public FakeServerTests()
    {
        var port = FreePort();
        _baseUrl = $"http://127.0.0.1:{port}";
        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
        _listener.Start();
        _serveLoop = Task.Run(ServeAsync);
    }

    private static int FreePort()
    {
        var l = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    private async Task ServeAsync()
    {
        while (_listener.IsListening)
        {
            HttpListenerContext ctx;
            try
            {
                ctx = await _listener.GetContextAsync();
            }
            catch (Exception)
            {
                return;
            }
            string reqBody;
            using (var reader = new StreamReader(ctx.Request.InputStream, Encoding.UTF8))
            {
                reqBody = await reader.ReadToEndAsync();
            }
            var key = ctx.Request.HttpMethod + " " + ctx.Request.Url!.PathAndQuery;
            lock (_requests)
            {
                _requests.Add((ctx.Request.HttpMethod, ctx.Request.Url.PathAndQuery, reqBody));
            }
            var (status, body) = _routes.TryGetValue(key, out var route)
                ? route
                : (404, """{"error":"not found"}""");
            ctx.Response.StatusCode = status;
            ctx.Response.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(body);
            await ctx.Response.OutputStream.WriteAsync(bytes);
            ctx.Response.Close();
        }
    }

    public void Dispose()
    {
        _listener.Stop();
        _listener.Close();
    }

    [Fact]
    public async Task DeserializesSesEmails()
    {
        _routes["GET /_fakecloud/ses/emails"] = (200, """
            {"emails":[{"messageId":"m-1","from":"a@example.com","to":["b@example.com"],
              "subject":"hi","htmlBody":"<b>hi</b>","textBody":"hi","timestamp":"2026-01-01T00:00:00Z",
              "headers":[["X-Test","1"]],"unknownFutureField":true}]}
            """);
        var fc = new FakeCloudClient(_baseUrl);
        var emails = (await fc.Ses.GetEmailsAsync()).Emails;
        Assert.NotNull(emails);
        var email = Assert.Single(emails!);
        Assert.Equal("m-1", email.MessageId);
        Assert.Equal("a@example.com", email.From);
        Assert.Equal(["b@example.com"], email.To);
        Assert.Equal("hi", email.Subject);
    }

    [Fact]
    public async Task PostJsonOmitsNullFieldsAndUsesCamelCase()
    {
        _routes["POST /_fakecloud/events/fire-rule"] = (200, """{"targets":[]}""");
        var fc = new FakeCloudClient(_baseUrl);
        await fc.Events.FireRuleAsync(new FireRuleRequest("my-rule"));
        var req = _requests.Single(r => r.Path == "/_fakecloud/events/fire-rule");
        Assert.Contains("\"ruleName\":\"my-rule\"", req.Body);
        Assert.DoesNotContain("busName", req.Body);
    }

    [Fact]
    public async Task PascalCaseWireFieldsRoundTrip()
    {
        _routes["GET /_fakecloud/credentials"] = (200, """
            {"AccessKeyId":"AKIA123","SecretAccessKey":"secret","Token":"tok",
             "Expiration":"2026-01-01T00:00:00Z","RoleArn":"arn:aws:iam::000000000000:role/test"}
            """);
        var fc = new FakeCloudClient(_baseUrl);
        var creds = await fc.CredentialsAsync();
        Assert.Equal("AKIA123", creds.AccessKeyId);
        Assert.Equal("arn:aws:iam::000000000000:role/test", creds.RoleArn);
    }

    [Fact]
    public async Task ConfirmUserSurfaces404ErrorBody()
    {
        _routes["POST /_fakecloud/cognito/confirm-user"] =
            (404, """{"confirmed":false,"error":"user not found"}""");
        var fc = new FakeCloudClient(_baseUrl);
        var err = await Assert.ThrowsAsync<FakeCloudException>(
            () => fc.Cognito.ConfirmUserAsync(new ConfirmUserRequest("pool-1", "nobody")));
        Assert.Equal(404, err.Status);
        Assert.Equal("user not found", err.Body);
    }

    [Fact]
    public async Task Non2xxThrowsWithStatusAndBody()
    {
        _routes["GET /_fakecloud/health"] = (503, "upstream unavailable");
        var fc = new FakeCloudClient(_baseUrl);
        var err = await Assert.ThrowsAsync<FakeCloudException>(() => fc.HealthAsync());
        Assert.Equal(503, err.Status);
        Assert.Equal("upstream unavailable", err.Body);
    }

    [Fact]
    public async Task EcsTaskFilterBuildsQueryString()
    {
        _routes["GET /_fakecloud/ecs/tasks?cluster=demo&status=RUNNING"] = (200, """{"tasks":[]}""");
        var fc = new FakeCloudClient(_baseUrl);
        var tasks = await fc.Ecs.GetTasksAsync("demo", "RUNNING");
        Assert.NotNull(tasks.Tasks);
        Assert.Empty(tasks.Tasks!);
    }

    [Fact]
    public async Task SnakeCaseWireFieldsMapViaAttributes()
    {
        _routes["GET /_fakecloud/dns/resolve?name=db.internal&type=A"] = (200, """
            {"name":"db.internal","type":"A","status":"ANSWERED","authoritative":true,
             "records":[{"name":"db.internal","type":"A","ttl":300,"value":"10.0.0.5"}],
             "external_cname":null}
            """);
        var fc = new FakeCloudClient(_baseUrl);
        var res = await fc.DnsResolveAsync("db.internal");
        Assert.Equal("ANSWERED", res.Status);
        Assert.True(res.Authoritative);
        Assert.Equal("10.0.0.5", Assert.Single(res.Records!).Value);
    }
}
