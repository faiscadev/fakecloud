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
    public async Task SetSoftwareTokenPostsSecret()
    {
        _routes["POST /_fakecloud/cognito/software-token"] = (200, """{"enrolled":true}""");
        var fc = new FakeCloudClient(_baseUrl);
        var resp = await fc.Cognito.SetSoftwareTokenAsync(
            new SetSoftwareTokenRequest("us-east-1_Local", "alice", "JBSWY3DPEHPK3PXP"));
        Assert.True(resp.Enrolled);
        Assert.Equal(
            """{"userPoolId":"us-east-1_Local","username":"alice","secretCode":"JBSWY3DPEHPK3PXP"}""",
            _requests.Single(r => r.Path == "/_fakecloud/cognito/software-token").Body);
    }

    [Fact]
    public async Task SetSoftwareTokenSurfaces404()
    {
        _routes["POST /_fakecloud/cognito/software-token"] =
            (404, """{"error":"user not found in pool"}""");
        var fc = new FakeCloudClient(_baseUrl);
        var err = await Assert.ThrowsAsync<FakeCloudException>(
            () => fc.Cognito.SetSoftwareTokenAsync(
                new SetSoftwareTokenRequest("us-east-1_Local", "nobody", "JBSWY3DPEHPK3PXP")));
        Assert.Equal(404, err.Status);
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
    public async Task FractionalEpochTimestampsDeserializeAsDouble()
    {
        // The server emits these epoch-seconds fields as f64, so serde_json
        // writes a trailing ".0" for whole values (e.g. 1735689600.0).
        // System.Text.Json in strict mode refuses to read a fractional number
        // into an integer CLR type, so IssuedAt/Timestamp must be double.
        _routes["GET /_fakecloud/cognito/tokens"] = (200, """
            {"tokens":[{"type":"access","username":"alice","poolId":"pool-1",
              "clientId":"c-1","issuedAt":1735689600.0}]}
            """);
        _routes["GET /_fakecloud/cognito/auth-events"] = (200, """
            {"events":[{"eventType":"SignIn","username":"alice","userPoolId":"pool-1",
              "clientId":"c-1","timestamp":1735689600.0,"success":true}]}
            """);
        var fc = new FakeCloudClient(_baseUrl);

        var token = Assert.Single((await fc.Cognito.GetTokensAsync()).Tokens!);
        Assert.Equal(1735689600.0, token.IssuedAt);

        var evt = Assert.Single((await fc.Cognito.GetAuthEventsAsync()).Events!);
        Assert.Equal(1735689600.0, evt.Timestamp);
        Assert.True(evt.Success);
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

    private const string QuotaJson = """
        {"serviceCode":"ec2","quotaCode":"L-0263D0A3","quotaName":"EC2-VPC Elastic IPs",
         "global":false,"adjustable":true,"unit":"None","defaultValue":5.0,"appliedValue":2.0,
         "usage":1.0,"enforceable":true,"enforced":true,"enforcementSource":"override"}
        """;

    private const string RequestJson = """
        {"accountId":"123456789012","requestId":"req-1","serviceCode":"ec2",
         "quotaCode":"L-0263D0A3","quotaName":"EC2-VPC Elastic IPs","region":"us-east-1",
         "desiredValue":10.0,"status":"APPROVED","caseId":null,
         "created":"2026-01-01T00:00:00+00:00","lastUpdated":"2026-01-01T00:00:01+00:00"}
        """;

    [Fact]
    public async Task ServiceQuotasGetQuotasBuildsQueryAndDeserializes()
    {
        _routes["GET /_fakecloud/service-quotas/quotas?accountId=123456789012&serviceCode=ec2"] =
            (200, "{\"accountId\":\"123456789012\",\"region\":\"us-east-1\",\"quotas\":[" + QuotaJson + "]}");
        _routes["GET /_fakecloud/service-quotas/quotas"] = (200, """
            {"accountId":"000000000000","region":"us-east-1","quotas":[
             {"serviceCode":"s3","quotaCode":"L-DC2B2D3D","quotaName":"Buckets","global":true,
              "adjustable":true,"unit":"None","defaultValue":10000,"appliedValue":10000,
              "usage":null,"enforceable":false,"enforced":false,
              "enforcementSource":"not_enforceable"}]}
            """);
        var fc = new FakeCloudClient(_baseUrl);

        var res = await fc.ServiceQuotas.GetQuotasAsync(accountId: "123456789012", serviceCode: "ec2");
        Assert.Equal("123456789012", res.AccountId);
        var q = Assert.Single(res.Quotas!);
        Assert.Equal("L-0263D0A3", q.QuotaCode);
        Assert.Equal(5.0, q.DefaultValue);
        Assert.Equal(2.0, q.AppliedValue);
        Assert.Equal(1.0, q.Usage);
        Assert.False(q.Global);
        Assert.True(q.Enforced);
        Assert.Equal("override", q.EnforcementSource);

        var all = Assert.Single((await fc.ServiceQuotas.GetQuotasAsync()).Quotas!);
        Assert.Null(all.Usage);
        Assert.True(all.Global);
        Assert.Equal("not_enforceable", all.EnforcementSource);
    }

    [Fact]
    public async Task ServiceQuotasPutQuotaDistinguishesOmittedNullAndTrue()
    {
        const string path = "/_fakecloud/service-quotas/quotas/ec2/L-0263D0A3";
        _routes["PUT " + path] = (200, QuotaJson);
        var fc = new FakeCloudClient(_baseUrl);

        await fc.ServiceQuotas.PutQuotaAsync("ec2", "L-0263D0A3", new PutServiceQuotaRequest(Value: 2));
        await fc.ServiceQuotas.PutQuotaAsync("ec2", "L-0263D0A3",
            new PutServiceQuotaRequest(Enforcement: QuotaEnforcement.Default));
        await fc.ServiceQuotas.PutQuotaAsync("ec2", "L-0263D0A3",
            new PutServiceQuotaRequest(AccountId: "123456789012", Region: "eu-west-1",
                Enforcement: QuotaEnforcement.Enforce));
        await fc.ServiceQuotas.PutQuotaAsync("ec2", "L-0263D0A3",
            new PutServiceQuotaRequest(Enforcement: QuotaEnforcement.Ignore));

        var bodies = _requests.Where(r => r.Method == "PUT" && r.Path == path).Select(r => r.Body).ToList();
        Assert.Equal(4, bodies.Count);
        Assert.Equal("{\"value\":2}", bodies[0]);
        Assert.Equal("{\"enforce\":null}", bodies[1]);
        Assert.Equal(
            "{\"accountId\":\"123456789012\",\"region\":\"eu-west-1\",\"enforce\":true}",
            bodies[2]);
        Assert.Equal("{\"enforce\":false}", bodies[3]);
    }

    [Fact]
    public async Task ServiceQuotasDeleteQuotaSendsScopeAsQuery()
    {
        _routes["DELETE /_fakecloud/service-quotas/quotas/ec2/L-0263D0A3?accountId=123456789012&region=us-east-1"] =
            (200, QuotaJson);
        var fc = new FakeCloudClient(_baseUrl);
        var q = await fc.ServiceQuotas.DeleteQuotaAsync("ec2", "L-0263D0A3", "123456789012", "us-east-1");
        Assert.Equal("ec2", q.ServiceCode);
    }

    [Fact]
    public async Task ServiceQuotasPutEnforcementSerializesOverrides()
    {
        const string resp = """
            {"enforceAll":true,
             "overrides":[{"serviceCode":"ec2","quotaCode":"L-0263D0A3","enforce":false}],
             "accountOverrides":[{"accountId":"123456789012","serviceCode":"ec2",
               "quotaCode":"L-1216C47A","enforce":true}]}
            """;
        _routes["PUT /_fakecloud/service-quotas/enforcement"] = (200, resp);
        _routes["GET /_fakecloud/service-quotas/enforcement"] = (200, resp);
        var fc = new FakeCloudClient(_baseUrl);

        var res = await fc.ServiceQuotas.PutEnforcementAsync(new PutServiceQuotaEnforcementRequest(
            EnforceAll: true,
            Overrides:
            [
                new ServiceQuotaOverrideChange("ec2", "L-0263D0A3", QuotaEnforcement.Ignore),
                new ServiceQuotaOverrideChange("ec2", "L-1216C47A", QuotaEnforcement.Enforce, "123456789012"),
                new ServiceQuotaOverrideChange("ec2", "L-34B43A08", QuotaEnforcement.Default),
            ]));
        var body = _requests.Single(r => r.Method == "PUT").Body;
        Assert.Equal(
            "{\"enforceAll\":true,\"overrides\":["
                + "{\"serviceCode\":\"ec2\",\"quotaCode\":\"L-0263D0A3\",\"enforce\":false},"
                + "{\"serviceCode\":\"ec2\",\"quotaCode\":\"L-1216C47A\",\"accountId\":\"123456789012\",\"enforce\":true},"
                + "{\"serviceCode\":\"ec2\",\"quotaCode\":\"L-34B43A08\",\"enforce\":null}]}",
            body);
        Assert.True(res.EnforceAll);
        Assert.False(Assert.Single(res.Overrides!).Enforce);
        var acct = Assert.Single(res.AccountOverrides!);
        Assert.Equal("123456789012", acct.AccountId);
        Assert.True(acct.Enforce);

        var got = await fc.ServiceQuotas.GetEnforcementAsync();
        Assert.Equal("L-1216C47A", Assert.Single(got.AccountOverrides!).QuotaCode);
    }

    [Fact]
    public async Task ServiceQuotasRequestApprovalRoundTrips()
    {
        _routes["PUT /_fakecloud/service-quotas/request-approval"] = (200, """{"mode":"manual"}""");
        _routes["GET /_fakecloud/service-quotas/request-approval"] = (200, """{"mode":"auto"}""");
        var fc = new FakeCloudClient(_baseUrl);

        Assert.Equal("manual", (await fc.ServiceQuotas.SetRequestApprovalAsync("manual")).Mode);
        Assert.Equal("{\"mode\":\"manual\"}", _requests.Single(r => r.Method == "PUT").Body);
        Assert.Equal("auto", (await fc.ServiceQuotas.GetRequestApprovalAsync()).Mode);
    }

    [Fact]
    public async Task ServiceQuotasRequestsListApproveAndDeny()
    {
        _routes["GET /_fakecloud/service-quotas/requests?accountId=123456789012&status=PENDING"] =
            (200, "{\"requests\":[" + RequestJson + "]}");
        _routes["POST /_fakecloud/service-quotas/requests/req-1/approve"] = (200, RequestJson);
        _routes["POST /_fakecloud/service-quotas/requests/req-2/deny"] = (200, RequestJson);
        _routes["POST /_fakecloud/service-quotas/requests/req-3/deny"] = (200, RequestJson);
        var fc = new FakeCloudClient(_baseUrl);

        var list = await fc.ServiceQuotas.GetRequestsAsync("123456789012", "PENDING");
        var r = Assert.Single(list.Requests!);
        Assert.Equal("req-1", r.RequestId);
        Assert.Equal(10.0, r.DesiredValue);
        Assert.Null(r.CaseId);
        Assert.Equal("2026-01-01T00:00:01+00:00", r.LastUpdated);

        Assert.Equal("APPROVED", (await fc.ServiceQuotas.ApproveRequestAsync("req-1")).Status);
        await fc.ServiceQuotas.DenyRequestAsync("req-2");
        await fc.ServiceQuotas.DenyRequestAsync("req-3", "CASE_CLOSED");

        Assert.Equal("", _requests.Single(x => x.Path.EndsWith("/req-1/approve")).Body);
        Assert.Equal("", _requests.Single(x => x.Path.EndsWith("/req-2/deny")).Body);
        Assert.Equal("{\"status\":\"CASE_CLOSED\"}", _requests.Single(x => x.Path.EndsWith("/req-3/deny")).Body);
    }

    [Fact]
    public async Task ServiceQuotasErrorPropagatesStatusAndBody()
    {
        _routes["PUT /_fakecloud/service-quotas/request-approval"] =
            (400, """{"error":"mode must be auto or manual, got \"sometimes\""}""");
        var fc = new FakeCloudClient(_baseUrl);
        var err = await Assert.ThrowsAsync<FakeCloudException>(
            () => fc.ServiceQuotas.SetRequestApprovalAsync("sometimes"));
        Assert.Equal(400, err.Status);
        Assert.Contains("mode must be auto or manual", err.Body);
    }
}
