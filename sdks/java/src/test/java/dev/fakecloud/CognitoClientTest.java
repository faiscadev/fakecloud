package dev.fakecloud;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import dev.fakecloud.Types.SetSoftwareTokenRequest;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Exercises the Cognito sub-client's software-token helper against an in-process mock server. */
class CognitoClientTest {

    private record Captured(String method, String uri, String body) {}

    private HttpServer server;
    private final List<Captured> captured = new ArrayList<>();
    private volatile int status = 200;
    private volatile String response = "{}";
    private FakeCloud fc;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            synchronized (captured) {
                captured.add(new Captured(
                        exchange.getRequestMethod(), exchange.getRequestURI().getRawPath(), body));
            }
            byte[] out = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, out.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(out);
            }
        });
        server.start();
        fc = new FakeCloud("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private Captured last() {
        synchronized (captured) {
            return captured.get(captured.size() - 1);
        }
    }

    @Test
    void setSoftwareTokenPostsSecretAndParsesResponse() {
        response = "{\"enrolled\":true}";
        var res = fc.cognito().setSoftwareToken(
                new SetSoftwareTokenRequest("us-east-1_Local", "alice", "JBSWY3DPEHPK3PXP"));
        assertEquals("POST", last().method());
        assertEquals("/_fakecloud/cognito/software-token", last().uri());
        assertEquals(
                "{\"userPoolId\":\"us-east-1_Local\",\"username\":\"alice\","
                        + "\"secretCode\":\"JBSWY3DPEHPK3PXP\"}",
                last().body());
        assertTrue(res.enrolled());
    }

    @Test
    void setSoftwareTokenSurfacesNotFound() {
        status = 404;
        response = "{\"error\":\"user not found in pool\"}";
        FakeCloudError err = assertThrows(
                FakeCloudError.class,
                () -> fc.cognito().setSoftwareToken(
                        new SetSoftwareTokenRequest("us-east-1_Local", "nobody", "JBSWY3DPEHPK3PXP")));
        assertEquals(404, err.status());
        assertTrue(err.body().contains("user not found"));
    }
}
