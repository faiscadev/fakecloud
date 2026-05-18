package dev.fakecloud;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Shared HTTP + JSON machinery for every sub-client.
 *
 * <p>Package-private on purpose: users talk to {@link FakeCloud} and its sub-clients,
 * not to the transport layer.
 */
final class HttpTransport {
    private final String baseUrl;
    private final HttpClient http;
    private final ObjectMapper mapper;

    HttpTransport(String baseUrl) {
        this.baseUrl = baseUrl;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.mapper = new ObjectMapper();
    }

    String baseUrl() {
        return baseUrl;
    }

    static String encodePath(String segment) {
        // URLEncoder targets form encoding, which turns spaces into '+'. Path segments need %20.
        return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }

    <T> T get(String path, Class<T> type) {
        return send(HttpRequest.newBuilder(uri(path)).GET(), type);
    }

    <T> T get(String path, TypeReference<T> type) {
        return send(HttpRequest.newBuilder(uri(path)).GET(), type);
    }

    /**
     * GET an endpoint whose response is plain text (e.g. a PEM block).
     * Throws {@link FakeCloudError} on non-2xx.
     */
    String getText(String path) {
        HttpResponse<byte[]> resp = execute(HttpRequest.newBuilder(uri(path)).GET());
        int status = resp.statusCode();
        if (status < 200 || status >= 300) {
            throw new FakeCloudError(status, new String(resp.body(), StandardCharsets.UTF_8));
        }
        return new String(resp.body(), StandardCharsets.UTF_8);
    }

    <T> T postEmpty(String path, Class<T> type) {
        return send(HttpRequest.newBuilder(uri(path)).POST(HttpRequest.BodyPublishers.noBody()), type);
    }

    <T> T postJson(String path, Object body, Class<T> type) {
        byte[] payload = serialize(body);
        HttpRequest.Builder req = HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload));
        return send(req, type);
    }

    /**
     * POST a JSON body where the server replies with no content (204) on
     * success. Throws {@link FakeCloudError} on non-2xx; on success, returns
     * the HTTP status code so callers can distinguish 200/201/204 if they
     * care.
     */
    int postJsonNoContent(String path, Object body) {
        byte[] payload = serialize(body);
        HttpRequest.Builder req = HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload));
        HttpResponse<byte[]> resp = execute(req);
        int status = resp.statusCode();
        if (status < 200 || status >= 300) {
            throw new FakeCloudError(status, new String(resp.body(), StandardCharsets.UTF_8));
        }
        return status;
    }

    /**
     * POST with no body where the server replies with no content (204) on
     * success. Used by admin endpoints that take their input entirely from
     * the URL path (e.g. {@code /acm/certificates/{id}/approve}).
     */
    int postNoContent(String path) {
        HttpRequest.Builder req =
                HttpRequest.newBuilder(uri(path)).POST(HttpRequest.BodyPublishers.noBody());
        HttpResponse<byte[]> resp = execute(req);
        int status = resp.statusCode();
        if (status < 200 || status >= 300) {
            throw new FakeCloudError(status, new String(resp.body(), StandardCharsets.UTF_8));
        }
        return status;
    }

    <T> T postText(String path, String body, Class<T> type) {
        HttpRequest.Builder req = HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        return send(req, type);
    }

    <T> T delete(String path, Class<T> type) {
        return send(HttpRequest.newBuilder(uri(path)).DELETE(), type);
    }

    private URI uri(String path) {
        return URI.create(baseUrl + path);
    }

    private byte[] serialize(Object body) {
        try {
            return mapper.writeValueAsBytes(body);
        } catch (IOException e) {
            throw new FakeCloudError(-1, "failed to encode request body: " + e.getMessage());
        }
    }

    private <T> T send(HttpRequest.Builder builder, Class<T> type) {
        HttpResponse<byte[]> resp = execute(builder);
        return parse(resp, type, null);
    }

    private <T> T send(HttpRequest.Builder builder, TypeReference<T> type) {
        HttpResponse<byte[]> resp = execute(builder);
        return parse(resp, null, type);
    }

    /**
     * Perform a request that may legitimately return a non-2xx status whose body still
     * needs to be parsed (Cognito confirm-user returns 404 with a JSON {@code error} field).
     * The caller decides — on success, the body is parsed into {@code type}; on failure,
     * {@code onError} is called with the parsed body to decide whether to throw.
     */
    <T> T sendAllowingError(HttpRequest.Builder builder, Class<T> type) {
        HttpResponse<byte[]> resp = execute(builder);
        try {
            return mapper.readValue(resp.body(), type);
        } catch (IOException e) {
            throw new FakeCloudError(resp.statusCode(),
                    new String(resp.body(), StandardCharsets.UTF_8));
        }
    }

    int lastStatus(HttpResponse<byte[]> resp) {
        return resp.statusCode();
    }

    HttpResponse<byte[]> execute(HttpRequest.Builder builder) {
        try {
            return http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new FakeCloudError(-1, "network error: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FakeCloudError(-1, "interrupted: " + e.getMessage());
        }
    }

    private <T> T parse(HttpResponse<byte[]> resp, Class<T> cls, TypeReference<T> ref) {
        int status = resp.statusCode();
        if (status < 200 || status >= 300) {
            throw new FakeCloudError(status, new String(resp.body(), StandardCharsets.UTF_8));
        }
        try {
            if (cls != null) {
                return mapper.readValue(resp.body(), cls);
            }
            return mapper.readValue(resp.body(), ref);
        } catch (IOException e) {
            throw new FakeCloudError(status, "failed to parse response: " + e.getMessage());
        }
    }

    HttpRequest.Builder builder(String path) {
        return HttpRequest.newBuilder(uri(path));
    }
}
