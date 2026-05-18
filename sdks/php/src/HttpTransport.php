<?php

declare(strict_types=1);

namespace FakeCloud;

/**
 * Shared HTTP + JSON machinery for every sub-client.
 *
 * @internal Not part of the public API.
 */
final class HttpTransport
{
    private string $baseUrl;

    public function __construct(string $baseUrl)
    {
        $this->baseUrl = rtrim($baseUrl, '/');
    }

    public function baseUrl(): string
    {
        return $this->baseUrl;
    }

    public static function encodePath(string $segment): string
    {
        return rawurlencode($segment);
    }

    public function get(string $path): array
    {
        return $this->send('GET', $path);
    }

    /**
     * GET a path that returns a raw (non-JSON) body. Used for download
     * endpoints that emit binary content like Lambda function code zips
     * and Lambda layer content. Returns the raw bytes; throws
     * {@see FakeCloudError} on non-2xx.
     */
    public function getRaw(string $path): string
    {
        $response = $this->execute('GET', $path);
        if ($response['status'] < 200 || $response['status'] >= 300) {
            throw new FakeCloudError($response['status'], $response['body']);
        }
        return $response['body'];
    }

    public function postEmpty(string $path): array
    {
        return $this->send('POST', $path);
    }

    public function postJson(string $path, array $body): array
    {
        $payload = json_encode($body, JSON_THROW_ON_ERROR);
        return $this->send('POST', $path, $payload, 'application/json');
    }

    /**
     * POST a JSON body where the server replies with no content (204) on
     * success. Throws {@see FakeCloudError} on non-2xx; on success returns
     * the HTTP status code.
     */
    public function postJsonNoContent(string $path, array $body): int
    {
        $payload = json_encode($body, JSON_THROW_ON_ERROR);
        $response = $this->execute('POST', $path, $payload, 'application/json');
        if ($response['status'] < 200 || $response['status'] >= 300) {
            throw new FakeCloudError($response['status'], $response['body']);
        }
        return $response['status'];
    }

    /**
     * POST with no body where the server replies with no content (204) on
     * success. Used by admin endpoints whose input lives entirely in the
     * URL path (e.g. /acm/certificates/{id}/approve).
     */
    public function postNoContent(string $path): int
    {
        $response = $this->execute('POST', $path);
        if ($response['status'] < 200 || $response['status'] >= 300) {
            throw new FakeCloudError($response['status'], $response['body']);
        }
        return $response['status'];
    }

    public function postText(string $path, string $body): array
    {
        return $this->send('POST', $path, $body, 'text/plain');
    }

    public function delete(string $path): array
    {
        return $this->send('DELETE', $path);
    }

    /**
     * Execute a request and return the raw response body + status code.
     * Used by CognitoClient for special error handling.
     *
     * @return array{body: string, status: int}
     */
    public function execute(string $method, string $path, ?string $body = null, ?string $contentType = null): array
    {
        $ch = curl_init($this->baseUrl . $path);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 10);
        curl_setopt($ch, CURLOPT_TIMEOUT, 30);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);

        $headers = [];
        if ($contentType !== null) {
            $headers[] = "Content-Type: {$contentType}";
        }
        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }
        if ($headers !== []) {
            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        }

        $responseBody = curl_exec($ch);
        if ($responseBody === false) {
            $error = curl_error($ch);
            curl_close($ch);
            throw new FakeCloudError(-1, "network error: {$error}");
        }

        $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        return ['body' => $responseBody, 'status' => $status];
    }

    private function send(string $method, string $path, ?string $body = null, ?string $contentType = null): array
    {
        $response = $this->execute($method, $path, $body, $contentType);

        if ($response['status'] < 200 || $response['status'] >= 300) {
            throw new FakeCloudError($response['status'], $response['body']);
        }

        $decoded = json_decode($response['body'], true, 512, JSON_THROW_ON_ERROR);
        return $decoded;
    }
}
