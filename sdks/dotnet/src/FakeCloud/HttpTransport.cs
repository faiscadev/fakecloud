using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FakeCloud;

/// <summary>
/// Shared HTTP + JSON machinery for every sub-client.
/// Internal on purpose: users talk to <see cref="FakeCloudClient"/> and its
/// sub-clients, not to the transport layer.
/// </summary>
internal sealed class HttpTransport
{
    internal static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly HttpClient _http;

    internal HttpTransport(string baseUrl)
    {
        BaseUrl = baseUrl;
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
    }

    internal string BaseUrl { get; }

    internal static string EncodePath(string segment) => Uri.EscapeDataString(segment);

    internal async Task<T> GetAsync<T>(string path, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, Url(path));
        return await SendAsync<T>(req, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// GET an endpoint whose response is plain text (e.g. a PEM block).
    /// Throws <see cref="FakeCloudException"/> on non-2xx.
    /// </summary>
    internal async Task<string> GetTextAsync(string path, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, Url(path));
        var (status, body) = await ExecuteAsync(req, ct).ConfigureAwait(false);
        EnsureSuccess(status, body);
        return Encoding.UTF8.GetString(body);
    }

    /// <summary>
    /// GET a path and return the raw response body as bytes. Used for binary
    /// admin endpoints (Lambda function code, Lambda layer content) that
    /// respond with a zip archive rather than JSON.
    /// </summary>
    internal async Task<byte[]> GetBytesAsync(string path, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, Url(path));
        var (status, body) = await ExecuteAsync(req, ct).ConfigureAwait(false);
        EnsureSuccess(status, body);
        return body;
    }

    internal async Task<T> PostEmptyAsync<T>(string path, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, Url(path));
        return await SendAsync<T>(req, ct).ConfigureAwait(false);
    }

    internal async Task<T> PostJsonAsync<T>(string path, object body, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, Url(path))
        {
            Content = JsonContent(body),
        };
        return await SendAsync<T>(req, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// POST a JSON body where the server replies with no content (204) on
    /// success. Throws <see cref="FakeCloudException"/> on non-2xx; on
    /// success, returns the HTTP status code so callers can distinguish
    /// 200/201/204 if they care.
    /// </summary>
    internal async Task<int> PostJsonNoContentAsync(string path, object body, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, Url(path))
        {
            Content = JsonContent(body),
        };
        var (status, respBody) = await ExecuteAsync(req, ct).ConfigureAwait(false);
        EnsureSuccess(status, respBody);
        return status;
    }

    /// <summary>
    /// POST with no body where the server replies with no content (204) on
    /// success. Used by admin endpoints that take their input entirely from
    /// the URL path (e.g. <c>/acm/certificates/{id}/approve</c>).
    /// </summary>
    internal async Task<int> PostNoContentAsync(string path, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, Url(path));
        var (status, respBody) = await ExecuteAsync(req, ct).ConfigureAwait(false);
        EnsureSuccess(status, respBody);
        return status;
    }

    internal async Task<T> PostTextAsync<T>(string path, string body, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, Url(path))
        {
            Content = new StringContent(body, Encoding.UTF8, "text/plain"),
        };
        return await SendAsync<T>(req, ct).ConfigureAwait(false);
    }

    internal async Task<T> DeleteAsync<T>(string path, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Delete, Url(path));
        return await SendAsync<T>(req, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// POST JSON and parse the body even on a non-2xx status. Used by the
    /// Cognito confirm-user endpoint, which returns 404 with a JSON
    /// <c>error</c> field for unknown users.
    /// </summary>
    internal async Task<(int Status, T? Parsed, string RawBody)> PostJsonAllowingErrorAsync<T>(
        string path, object body, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, Url(path))
        {
            Content = JsonContent(body),
        };
        var (status, respBody) = await ExecuteAsync(req, ct).ConfigureAwait(false);
        var raw = Encoding.UTF8.GetString(respBody);
        try
        {
            return (status, JsonSerializer.Deserialize<T>(respBody, JsonOptions), raw);
        }
        catch (JsonException)
        {
            return (status, default, raw);
        }
    }

    private string Url(string path) => BaseUrl + path;

    private static ByteArrayContent JsonContent(object body)
    {
        byte[] payload;
        try
        {
            payload = JsonSerializer.SerializeToUtf8Bytes(body, body.GetType(), JsonOptions);
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            throw new FakeCloudException(-1, $"failed to encode request body: {e.Message}");
        }
        var content = new ByteArrayContent(payload);
        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
        return content;
    }

    private async Task<T> SendAsync<T>(HttpRequestMessage req, CancellationToken ct)
    {
        var (status, body) = await ExecuteAsync(req, ct).ConfigureAwait(false);
        EnsureSuccess(status, body);
        try
        {
            return JsonSerializer.Deserialize<T>(body, JsonOptions)
                ?? throw new FakeCloudException(status, "empty response body");
        }
        catch (JsonException e)
        {
            throw new FakeCloudException(status, $"failed to parse response: {e.Message}");
        }
    }

    private async Task<(int Status, byte[] Body)> ExecuteAsync(HttpRequestMessage req, CancellationToken ct)
    {
        try
        {
            using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseContentRead, ct)
                .ConfigureAwait(false);
            var body = await resp.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
            return ((int)resp.StatusCode, body);
        }
        catch (HttpRequestException e)
        {
            throw new FakeCloudException(-1, $"network error: {e.Message}");
        }
        catch (TaskCanceledException e) when (!ct.IsCancellationRequested)
        {
            throw new FakeCloudException(-1, $"request timed out: {e.Message}");
        }
    }

    private static void EnsureSuccess(int status, byte[] body)
    {
        if (status is < 200 or >= 300)
        {
            throw new FakeCloudException(status, Encoding.UTF8.GetString(body));
        }
    }
}
