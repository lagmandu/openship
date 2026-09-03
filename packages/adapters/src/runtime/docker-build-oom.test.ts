import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { Readable } from "node:stream";
import { DockerRuntime, getDockerBuildIdleTimeoutMs } from "./docker";
import { BuildLogger } from "./build-pipeline";

describe("Docker build OOM diagnostics and starvation detection", () => {
  const originalEnv = process.env.OPENSHIP_BUILD_IDLE_TIMEOUT_MS;

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env.OPENSHIP_BUILD_IDLE_TIMEOUT_MS = originalEnv;
    } else {
      delete process.env.OPENSHIP_BUILD_IDLE_TIMEOUT_MS;
    }
  });

  describe("getDockerBuildIdleTimeoutMs", () => {
    it("defaults to 10 minutes (600_000 ms)", () => {
      delete process.env.OPENSHIP_BUILD_IDLE_TIMEOUT_MS;
      expect(getDockerBuildIdleTimeoutMs()).toBe(10 * 60 * 1000);
    });

    it("respects OPENSHIP_BUILD_IDLE_TIMEOUT_MS when valid number provided", () => {
      process.env.OPENSHIP_BUILD_IDLE_TIMEOUT_MS = "300000";
      expect(getDockerBuildIdleTimeoutMs()).toBe(300_000);
    });

    it("falls back to default if environment variable is invalid or non-positive", () => {
      process.env.OPENSHIP_BUILD_IDLE_TIMEOUT_MS = "invalid";
      expect(getDockerBuildIdleTimeoutMs()).toBe(10 * 60 * 1000);

      process.env.OPENSHIP_BUILD_IDLE_TIMEOUT_MS = "-5";
      expect(getDockerBuildIdleTimeoutMs()).toBe(10 * 60 * 1000);
    });
  });

  describe("extractBuildFailureHint", () => {
    const runtime = new (DockerRuntime as any)({
      kind: "local",
      socketPath: "/var/run/docker.sock",
    });

    it("decodes exit code 137 as OOM killer with actionable hint", () => {
      const line = "The command '/bin/sh -c npm run build' returned a non-zero code: 137";
      const hint = runtime.extractBuildFailureHint(line);
      expect(hint).toContain("Out-Of-Memory (OOM) killer (exit code 137)");
      expect(hint).toContain("Increase the project's Build Memory in OpenShip Settings -> Resources");
    });

    it("decodes exit code 143 as SIGTERM", () => {
      const line = "The command '/bin/sh -c npm run build' returned a non-zero code: 143";
      const hint = runtime.extractBuildFailureHint(line);
      expect(hint).toContain("terminated by SIGTERM (exit code 143)");
    });

    it("passes ordinary non-zero exit codes through untouched", () => {
      const line = "The command '/bin/sh -c npm test' returned a non-zero code: 1";
      const hint = runtime.extractBuildFailureHint(line);
      expect(hint).toBe(line);
    });

    it("identifies JavaScript heap out of memory errors", () => {
      const line = "<--- Last few GCs --->\nFATAL ERROR: Ineffective mark-compacts near heap limit Allocation failed - JavaScript heap out of memory";
      const hint = runtime.extractBuildFailureHint(line);
      expect(hint).toContain("Process ran out of memory");
      expect(hint).toContain("NODE_OPTIONS=\"--max-old-space-size=...\"");
    });

    it("identifies npm ENOMEM errors", () => {
      const line = "npm ERR! code ENOMEM\nnpm ERR! syscall spawn";
      const hint = runtime.extractBuildFailureHint(line);
      expect(hint).toContain("npm ran out of memory while installing dependencies");
    });

    it("identifies Go runtime out of memory errors", () => {
      const line = "fatal error: runtime: out of memory";
      const hint = runtime.extractBuildFailureHint(line);
      expect(hint).toContain("Process ran out of memory");
    });
  });

  describe("streamDockerodeBuild memory starvation watchdog", () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it("detects pinned memory starvation (>=95%) and terminates immediately", async () => {
      const killMock = vi.fn().mockResolvedValue({});
      const statsMock = vi.fn().mockResolvedValue({
        memory_stats: {
          usage: 1020 * 1024 * 1024, // 1020MB
          limit: 1024 * 1024 * 1024, // 1024MB (99.6%)
        },
      });

      const listContainersMock = vi.fn().mockResolvedValue([
        {
          Id: "test-starving-container-123",
          Created: Math.floor(Date.now() / 1000),
        },
      ]);

      const mockDocker = {
        listContainers: listContainersMock,
        getContainer: vi.fn(() => ({
          stats: statsMock,
          kill: killMock,
        })),
        modem: {
          followProgress: (_stream: any, onFinished: any) => {
            // Keep stream open to simulate stalled build
          },
        },
      };

      const runtime = new (DockerRuntime as any)({
        kind: "local",
        socketPath: "/var/run/docker.sock",
      });
      (runtime as any)._docker = mockDocker;

      const log = new BuildLogger();
      const stream = new Readable({ read() {} });
      stream.on("error", () => {});

      let error: any;
      const buildPromise = runtime
        .streamDockerodeBuild(stream, log, undefined, {
          memoryMb: 1024,
          cpuCores: 1,
        })
        .catch((err: any) => {
          error = err;
        });

      // Advance timers to trigger the watchdog checks:
      // First check at 30s (consecutiveStarvingChecks = 1, logs warning)
      await vi.advanceTimersByTimeAsync(30_000);
      expect(listContainersMock).toHaveBeenCalled();
      expect(statsMock).toHaveBeenCalled();
      expect(killMock).not.toHaveBeenCalled();

      // Second check at 45s (consecutiveStarvingChecks = 2, triggers termination)
      await vi.advanceTimersByTimeAsync(15_000);

      await buildPromise;
      expect(error).toBeDefined();
      expect(error.message).toMatch(/container is memory-starved/);
      expect(killMock).toHaveBeenCalled();
    });

    it("includes resource constraint context when idle timeout expires", async () => {
      const mockDocker = {
        listContainers: vi.fn().mockResolvedValue([]),
        modem: {
          followProgress: (_stream: any, onFinished: any) => {
            // Stream stays open with no progress
          },
        },
      };

      const runtime = new (DockerRuntime as any)({
        kind: "local",
        socketPath: "/var/run/docker.sock",
      });
      (runtime as any)._docker = mockDocker;

      const log = new BuildLogger();
      const stream = new Readable({ read() {} });
      stream.on("error", () => {});

      let error: any;
      const buildPromise = runtime
        .streamDockerodeBuild(stream, log, undefined, {
          memoryMb: 512,
          cpuCores: 1,
        })
        .catch((err: any) => {
          error = err;
        });

      // Fast forward past the 10m idle timeout
      await vi.advanceTimersByTimeAsync(10 * 60 * 1000 + 1000);

      await buildPromise;
      expect(error).toBeDefined();
      expect(error.message).toMatch(
        /Docker build produced no output for 10 minutes and timed out.*constrained to 512MB RAM/,
      );
    });
  });
});
