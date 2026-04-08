import { execSync, spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import * as net from "node:net";

let server: ChildProcess | undefined;

function findAvailablePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      if (addr && typeof addr === "object") {
        const port = addr.port;
        srv.close(() => resolve(port));
      } else {
        reject(new Error("Failed to get port"));
      }
    });
    srv.on("error", reject);
  });
}

async function waitForPort(port: number, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ok = await new Promise<boolean>((resolve) => {
      const socket = net.createConnection({ host: "127.0.0.1", port });
      socket.on("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.on("error", () => resolve(false));
    });
    if (ok) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(
    `fakecloud did not start within ${timeoutMs}ms on port ${port}`,
  );
}

export async function setup(): Promise<() => Promise<void>> {
  const repoRoot = resolve(__dirname, "../../../");
  const releaseBin = resolve(repoRoot, "target/release/fakecloud");
  const debugBin = resolve(repoRoot, "target/debug/fakecloud");

  let bin: string;
  if (existsSync(releaseBin)) {
    bin = releaseBin;
  } else if (existsSync(debugBin)) {
    bin = debugBin;
  } else {
    // Build it
    execSync("cargo build --release", { cwd: repoRoot, stdio: "inherit" });
    bin = releaseBin;
  }

  const port = await findAvailablePort();
  const endpoint = `http://127.0.0.1:${port}`;

  server = spawn(bin, ["--addr", `127.0.0.1:${port}`, "--log-level", "warn"], {
    stdio: ["ignore", "pipe", "pipe"],
  });

  server.on("error", (err) => {
    console.error("fakecloud failed to start:", err);
  });

  await waitForPort(port);

  // Make endpoint available to tests via env var
  process.env.FAKECLOUD_ENDPOINT = endpoint;

  return async () => {
    if (server) {
      server.kill("SIGTERM");
      // Wait for graceful shutdown
      await new Promise<void>((resolve) => {
        server!.on("exit", () => resolve());
        setTimeout(() => {
          server!.kill("SIGKILL");
          resolve();
        }, 3000);
      });
    }
  };
}
