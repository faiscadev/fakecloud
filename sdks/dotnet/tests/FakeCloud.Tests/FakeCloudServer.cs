using System.Diagnostics;
using System.Net.Sockets;
using Xunit;

namespace FakeCloud.Tests;

/// <summary>
/// Spawns a fresh <c>fakecloud</c> binary on an ephemeral port for the lifetime
/// of the E2E test collection, then tears it down.
///
/// <para>
/// Mirrors the sibling SDKs (Java <c>FakeCloudServer</c>, Python
/// <c>_wait_for_ready</c>, Go <c>waitForReady</c>): it locates the release (or
/// debug fallback) binary under the workspace <c>target/</c> directory, starts
/// it, waits for the health endpoint to answer, and <b>fails loudly</b> if the
/// binary is missing or never becomes ready. E2E tests therefore run for real
/// and can never pass vacuously when the server is absent.
/// </para>
///
/// <para>
/// Set <c>FAKECLOUD_ENDPOINT</c> to point the E2E suite at an already-running
/// server instead; when set, no binary is spawned.
/// </para>
/// </summary>
public sealed class FakeCloudServer : IAsyncLifetime
{
    private Process? _process;

    public string Endpoint { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        var external = Environment.GetEnvironmentVariable("FAKECLOUD_ENDPOINT");
        if (!string.IsNullOrWhiteSpace(external))
        {
            Endpoint = external.TrimEnd('/');
            await WaitForReadyAsync(Endpoint, TimeSpan.FromSeconds(30)).ConfigureAwait(false);
            return;
        }

        var binary = LocateBinary();
        var port = FreePort();
        Endpoint = $"http://127.0.0.1:{port}";

        var psi = new ProcessStartInfo(binary)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("--addr");
        psi.ArgumentList.Add($"127.0.0.1:{port}");
        psi.ArgumentList.Add("--log-level");
        psi.ArgumentList.Add("warn");

        _process = Process.Start(psi)
            ?? throw new InvalidOperationException($"failed to start fakecloud binary at {binary}");

        // Drain stdout/stderr so the child never blocks on a full pipe buffer.
        _process.OutputDataReceived += (_, _) => { };
        _process.ErrorDataReceived += (_, _) => { };
        _process.BeginOutputReadLine();
        _process.BeginErrorReadLine();

        try
        {
            await WaitForReadyAsync(Endpoint, TimeSpan.FromSeconds(30)).ConfigureAwait(false);
        }
        catch
        {
            Kill();
            throw;
        }
    }

    public Task DisposeAsync()
    {
        Kill();
        return Task.CompletedTask;
    }

    private void Kill()
    {
        if (_process is null)
        {
            return;
        }

        try
        {
            if (!_process.HasExited)
            {
                _process.Kill(entireProcessTree: true);
                _process.WaitForExit(3000);
            }
        }
        catch (InvalidOperationException)
        {
            // Process already gone.
        }
        finally
        {
            _process.Dispose();
            _process = null;
        }
    }

    private static string LocateBinary()
    {
        var root = LocateRepoRoot();
        var exe = OperatingSystem.IsWindows() ? "fakecloud.exe" : "fakecloud";
        var release = Path.Combine(root, "target", "release", exe);
        var debug = Path.Combine(root, "target", "debug", exe);

        if (File.Exists(release))
        {
            return release;
        }

        if (File.Exists(debug))
        {
            return debug;
        }

        throw new InvalidOperationException(
            "fakecloud binary not found. Build it first with: cargo build --release\n"
            + $"  Looked for:\n    {release}\n    {debug}");
    }

    private static string LocateRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && dir is not null; i++)
        {
            if (File.Exists(Path.Combine(dir.FullName, "Cargo.toml"))
                && Directory.Exists(Path.Combine(dir.FullName, "crates")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        throw new InvalidOperationException(
            $"could not locate fakecloud repo root from {AppContext.BaseDirectory}");
    }

    private static int FreePort()
    {
        var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task WaitForReadyAsync(string endpoint, TimeSpan timeout)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var resp = await http.GetAsync($"{endpoint}/_fakecloud/health").ConfigureAwait(false);
                if (resp.IsSuccessStatusCode)
                {
                    return;
                }
            }
            catch (HttpRequestException)
            {
                // Server still warming up.
            }
            catch (TaskCanceledException)
            {
                // Per-probe timeout; retry until the overall deadline.
            }

            await Task.Delay(100).ConfigureAwait(false);
        }

        throw new TimeoutException($"fakecloud did not become ready at {endpoint} within {timeout.TotalSeconds:0}s");
    }
}

/// <summary>
/// Shares a single spawned <see cref="FakeCloudServer"/> across every test in
/// the E2E collection.
/// </summary>
[CollectionDefinition(Name)]
public sealed class FakeCloudServerCollection : ICollectionFixture<FakeCloudServer>
{
    public const string Name = "fakecloud-server";
}
