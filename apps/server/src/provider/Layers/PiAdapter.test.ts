import assert from "node:assert/strict";
import { EventEmitter } from "node:events";

import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join as pathJoin } from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ApprovalRequestId,
  type ProviderRuntimeEvent,
  ThreadId,
  type ToolLifecycleItemType,
} from "@workbench/contracts";
import { Effect, Fiber, Layer, Option, Stream } from "effect";
import { describe, it, vi } from "vitest";

import { attachmentRelativePath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { PiAdapter } from "../Services/PiAdapter.ts";
import { makePiAdapterLive } from "./PiAdapter.ts";

const spawnMock = vi.hoisted(() => vi.fn());

vi.mock("node:child_process", async () => {
  const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
  return {
    ...actual,
    spawn: spawnMock,
  };
});

class FakeReadable extends EventEmitter {
  setEncoding(_encoding: string): this {
    return this;
  }
}

class FakeChildProcess extends EventEmitter {
  readonly stdout = new FakeReadable();
  readonly stderr = new FakeReadable();
  killed = false;

  kill(_signal?: NodeJS.Signals): boolean {
    this.killed = true;
    return true;
  }
}

class FakeWritable extends EventEmitter {
  readonly frames: string[] = [];
  destroyed = false;

  write(chunk: string | Uint8Array): boolean {
    const text = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
    this.frames.push(text);
    return true;
  }
}

class FakeRpcChildProcess extends EventEmitter {
  readonly stdout = new FakeReadable();
  readonly stderr = new FakeReadable();
  readonly stdin: FakeWritable;
  killed = false;
  exitCode: number | null = null;
  /** Canned get_state response — individual tests can mutate via configureState. */
  private stateResponse: {
    readonly sessionId?: string;
    readonly sessionFile?: string;
    readonly isStreaming?: boolean;
  } = { isStreaming: false };

  constructor() {
    super();
    this.stdin = new FakeWritable();
    // Intercept writes so we can auto-respond to bootstrap frames (get_state)
    // without every test having to drive that handshake. Individual tests
    // still observe the frame via writtenFrames().
    const originalWrite = this.stdin.write.bind(this.stdin);
    this.stdin.write = (chunk: string | Uint8Array) => {
      const result = originalWrite(chunk);
      const text = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
      for (const line of text.split("\n")) {
        if (line.trim().length === 0) continue;
        try {
          const frame = JSON.parse(line) as { type?: string; id?: string };
          if (frame.type === "get_state" && typeof frame.id === "string") {
            setImmediate(() =>
              this.stdout.emit(
                "data",
                `${JSON.stringify({ type: "response", command: "get_state", id: frame.id, success: true, data: this.stateResponse })}\n`,
              ),
            );
          }
        } catch {
          // ignore — tests may write malformed frames intentionally
        }
      }
      return result;
    };
  }

  configureState(state: {
    readonly sessionId?: string;
    readonly sessionFile?: string;
    readonly isStreaming?: boolean;
  }): void {
    this.stateResponse = state;
  }

  kill(_signal?: NodeJS.Signals): boolean {
    this.killed = true;
    this.exitCode = 0;
    this.emit("exit", 0, null);
    return true;
  }

  /** Parse frames written to stdin as JSON and return them. */
  writtenFrames(): Array<Record<string, unknown>> {
    return this.stdin.frames
      .join("")
      .split("\n")
      .filter((line) => line.trim().length > 0)
      .map((line) => JSON.parse(line));
  }

  /** Emit a response correlated to a captured request id. */
  respondTo(request: Record<string, unknown>, response: Record<string, unknown>): void {
    const id = request.id;
    const command = request.type;
    this.stdout.emit("data", `${JSON.stringify({ type: "response", command, id, ...response })}\n`);
  }

  /** Emit a non-response notification. */
  notify(payload: Record<string, unknown>): void {
    this.stdout.emit("data", `${JSON.stringify(payload)}\n`);
  }
}

/**
 * Default spawn mock: collects every pi subprocess spawn, but short-circuits
 * the catalog probe (`pi --list-models`) so tests don't hang waiting for a
 * stdout/exit we'd never emit. The first-turn `sendTurn` subprocess lands as
 * `turnChildren[0]`, matching the pre-catalog test shape.
 */
function installPiSpawnMock(): { readonly turnChildren: FakeChildProcess[] } {
  const turnChildren: FakeChildProcess[] = [];
  spawnMock.mockReset();
  spawnMock.mockImplementation((_bin: string, args: string[]) => {
    const child = new FakeChildProcess();
    if (args.includes("--list-models")) {
      setImmediate(() => child.emit("exit", 0, null));
      return child;
    }
    turnChildren.push(child);
    return child;
  });
  return { turnChildren };
}

const asThreadId = (value: string): ThreadId => ThreadId.make(value);
function emitJsonLine(child: FakeChildProcess, payload: unknown): void {
  child.stdout.emit("data", `${JSON.stringify(payload)}\n`);
}

type ToolItemEvent = Extract<
  ProviderRuntimeEvent,
  {
    type: "item.started" | "item.updated" | "item.completed";
  }
>;

function expectToolItem(
  event: ProviderRuntimeEvent | undefined,
  type: ToolItemEvent["type"],
  itemType: ToolLifecycleItemType,
): ToolItemEvent {
  assert.ok(event);
  assert.equal(event.type, type);
  if (event.type !== type) {
    throw new Error(`expected ${type}`);
  }
  assert.equal(event.payload.itemType, itemType);
  return event;
}

const joinEvents = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  Fiber.join(fiber).pipe(
    Effect.timeoutOption(2_000),
    Effect.flatMap((result) =>
      Option.match(result, {
        onNone: () => Effect.fail(new Error("timed out while waiting for Pi adapter events")),
        onSome: (events) => Effect.succeed(events),
      }),
    ),
  );

const makeTestLayer = (transport: "json" | "rpc" = "json") =>
  makePiAdapterLive().pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(
      ServerSettingsService.layerTest({
        providers: {
          pi: {
            binaryPath: "fake-pi",
            defaultProvider: "",
            customModels: [],
            enabled: true,
            transport,
          },
        },
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );

// Existing tests were written against the JSON-mode transport; keep them
// pinned so the fixture still feeds a per-turn subprocess. New RPC tests
// opt in with `makeTestLayer("rpc")` + a FakeChildProcess that has stdin.
const PiAdapterTestLayer = makeTestLayer("json");

describe("PiAdapterLive", () => {
  it("maps current Pi tool execution events and assistant text from turn_end", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const spawned: FakeChildProcess[] = [];
        spawnMock.mockReset();
        spawnMock.mockImplementation((_bin: string, args: string[]) => {
          const child = new FakeChildProcess();
          if (args.includes("--list-models")) {
            setImmediate(() => child.emit("exit", 0, null));
            return child;
          }
          spawned.push(child);
          return child;
        });

        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-pi-tools");

        yield* adapter.startSession({
          provider: "pi",
          threadId,
          runtimeMode: "full-access",
        });

        const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 6)).pipe(
          Effect.forkChild,
        );
        yield* Effect.sleep(0);

        const turn = yield* adapter.sendTurn({
          threadId,
          input: "List files and summarize them.",
        });

        assert.equal(String(turn.threadId), String(threadId));
        const child = spawned[0];
        assert.ok(child);

        emitJsonLine(child, {
          type: "tool_execution_start",
          toolCallId: "call_ls",
          toolName: "bash",
          args: { command: "ls -la" },
        });
        emitJsonLine(child, {
          type: "tool_execution_update",
          toolCallId: "call_ls",
          toolName: "bash",
          args: { command: "ls -la" },
          partialResult: {
            content: [{ type: "text", text: "README.md\nsrc\n" }],
          },
        });
        emitJsonLine(child, {
          type: "tool_execution_end",
          toolCallId: "call_ls",
          toolName: "bash",
          result: {
            content: [{ type: "text", text: "README.md\nsrc\npackage.json" }],
          },
          isError: false,
        });
        emitJsonLine(child, {
          type: "turn_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "I found the project files." }],
            stopReason: "stop",
          },
          toolResults: [],
        });
        child.emit("exit", 0, null);

        const events = yield* joinEvents(eventsFiber);
        assert.deepEqual(
          events.map((event) => event.type),
          [
            "turn.started",
            "item.started",
            "item.updated",
            "item.completed",
            "content.delta",
            "turn.completed",
          ],
        );

        const toolStarted = expectToolItem(events[1], "item.started", "command_execution");
        const toolUpdated = expectToolItem(events[2], "item.updated", "command_execution");
        const toolCompleted = expectToolItem(events[3], "item.completed", "command_execution");
        assert.equal(toolStarted.itemId, toolUpdated.itemId);
        assert.equal(toolUpdated.itemId, toolCompleted.itemId);
        assert.equal(toolStarted.payload.detail, "bash: ls -la");
        assert.equal(toolUpdated.payload.detail, "README.md\nsrc");
        assert.equal(toolCompleted.payload.detail, "README.md\nsrc\npackage.json");

        const assistantDelta = events[4];
        assert.equal(assistantDelta?.type, "content.delta");
        if (assistantDelta?.type !== "content.delta") {
          throw new Error("expected assistant content delta");
        }
        assert.equal(assistantDelta.payload.streamKind, "assistant_text");
        assert.equal(assistantDelta.payload.delta, "I found the project files.");

        const turnCompleted = events[5];
        assert.equal(turnCompleted?.type, "turn.completed");
        if (turnCompleted?.type !== "turn.completed") {
          throw new Error("expected turn.completed");
        }
        assert.equal(turnCompleted.payload.state, "completed");
      }).pipe(Effect.provide(PiAdapterTestLayer)),
    );
  });

  it("streams current Pi message_update text deltas", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const spawned: FakeChildProcess[] = [];
        spawnMock.mockReset();
        spawnMock.mockImplementation((_bin: string, args: string[]) => {
          const child = new FakeChildProcess();
          if (args.includes("--list-models")) {
            setImmediate(() => child.emit("exit", 0, null));
            return child;
          }
          spawned.push(child);
          return child;
        });

        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-pi-stream");

        yield* adapter.startSession({
          provider: "pi",
          threadId,
          runtimeMode: "full-access",
        });

        const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 3)).pipe(
          Effect.forkChild,
        );
        yield* Effect.sleep(0);

        const turn = yield* adapter.sendTurn({
          threadId,
          input: "Say hello.",
        });

        assert.equal(String(turn.turnId).startsWith("pi-turn-"), true);
        const child = spawned[0];
        assert.ok(child);

        emitJsonLine(child, {
          type: "message_update",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Hello there" }],
          },
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: 0,
            delta: "Hello there",
          },
        });
        emitJsonLine(child, {
          type: "turn_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Hello there" }],
            stopReason: "stop",
          },
        });
        child.emit("exit", 0, null);

        const events = yield* joinEvents(eventsFiber);
        assert.deepEqual(
          events.map((event) => event.type),
          ["turn.started", "content.delta", "turn.completed"],
        );

        const assistantDelta = events[1];
        assert.equal(assistantDelta?.type, "content.delta");
        if (assistantDelta?.type !== "content.delta") {
          throw new Error("expected content.delta");
        }
        assert.equal(assistantDelta.payload.delta, "Hello there");
      }).pipe(Effect.provide(PiAdapterTestLayer)),
    );
  });

  it("does not settle the turn on intermediate turn_end toolUse boundaries", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const spawned: FakeChildProcess[] = [];
        spawnMock.mockReset();
        spawnMock.mockImplementation((_bin: string, args: string[]) => {
          const child = new FakeChildProcess();
          if (args.includes("--list-models")) {
            setImmediate(() => child.emit("exit", 0, null));
            return child;
          }
          spawned.push(child);
          return child;
        });

        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-pi-tooluse-turn-end");

        yield* adapter.startSession({
          provider: "pi",
          threadId,
          runtimeMode: "full-access",
        });

        const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 4)).pipe(
          Effect.forkChild,
        );
        yield* Effect.sleep(0);

        yield* adapter.sendTurn({
          threadId,
          input: "Find legacy T3 files and summarize them.",
        });

        const child = spawned[0];
        assert.ok(child);

        emitJsonLine(child, {
          type: "tool_execution_start",
          toolCallId: "call_search",
          toolName: "bash",
          args: { command: 'rg -n "T3" .' },
        });
        emitJsonLine(child, {
          type: "turn_end",
          message: {
            role: "assistant",
            content: [],
            stopReason: "toolUse",
          },
        });
        emitJsonLine(child, {
          type: "message_update",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "I found a few legacy T3 references." }],
          },
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: 0,
            delta: "I found a few legacy T3 references.",
          },
        });
        emitJsonLine(child, {
          type: "turn_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "I found a few legacy T3 references." }],
            stopReason: "stop",
          },
        });
        child.emit("exit", 0, null);

        const events = yield* joinEvents(eventsFiber);
        assert.deepEqual(
          events.map((event) => event.type),
          ["turn.started", "item.started", "content.delta", "turn.completed"],
        );

        const toolStarted = expectToolItem(events[1], "item.started", "command_execution");
        assert.equal(toolStarted.payload.detail, 'bash: rg -n "T3" .');

        const assistantDelta = events[2];
        assert.equal(assistantDelta?.type, "content.delta");
        if (assistantDelta?.type !== "content.delta") {
          throw new Error("expected assistant content delta");
        }
        assert.equal(assistantDelta.payload.delta, "I found a few legacy T3 references.");

        const turnCompleted = events[3];
        assert.equal(turnCompleted?.type, "turn.completed");
        if (turnCompleted?.type !== "turn.completed") {
          throw new Error("expected turn.completed");
        }
        assert.equal(turnCompleted.payload.state, "completed");
        assert.equal(turnCompleted.payload.stopReason, "stop");
      }).pipe(Effect.provide(PiAdapterTestLayer)),
    );
  });

  it("keeps supporting older tool_call/tool_result event names", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const spawned: FakeChildProcess[] = [];
        spawnMock.mockReset();
        spawnMock.mockImplementation((_bin: string, args: string[]) => {
          const child = new FakeChildProcess();
          if (args.includes("--list-models")) {
            setImmediate(() => child.emit("exit", 0, null));
            return child;
          }
          spawned.push(child);
          return child;
        });

        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-pi-legacy");

        yield* adapter.startSession({
          provider: "pi",
          threadId,
          runtimeMode: "full-access",
        });

        const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 5)).pipe(
          Effect.forkChild,
        );
        yield* Effect.sleep(0);

        yield* adapter.sendTurn({
          threadId,
          input: "Read the file.",
        });

        const child = spawned[0];
        assert.ok(child);

        emitJsonLine(child, {
          type: "tool_call",
          toolCallId: "call_read",
          toolName: "read",
          args: { path: "README.md" },
        });
        emitJsonLine(child, {
          type: "tool_result",
          toolCallId: "call_read",
          toolName: "read",
          result: {
            content: [{ type: "text", text: "# Workbench" }],
          },
          isError: false,
        });
        emitJsonLine(child, {
          type: "turn_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "The README starts with Workbench." }],
            stopReason: "stop",
          },
        });
        child.emit("exit", 0, null);

        const events = yield* joinEvents(eventsFiber);
        assert.deepEqual(
          events.map((event) => event.type),
          ["turn.started", "item.started", "item.completed", "content.delta", "turn.completed"],
        );

        const started = expectToolItem(events[1], "item.started", "dynamic_tool_call");
        const completed = expectToolItem(events[2], "item.completed", "dynamic_tool_call");
        assert.equal(started.itemId, completed.itemId);
      }).pipe(Effect.provide(PiAdapterTestLayer)),
    );
  });

  it("captures pi's session uuid and passes --session on subsequent turns", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const spawned: FakeChildProcess[] = [];
        spawnMock.mockReset();
        spawnMock.mockImplementation((_bin: string, args: string[]) => {
          const child = new FakeChildProcess();
          if (args.includes("--list-models")) {
            setImmediate(() => child.emit("exit", 0, null));
            return child;
          }
          spawned.push(child);
          return child;
        });

        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-pi-resume");

        yield* adapter.startSession({
          provider: "pi",
          threadId,
          runtimeMode: "full-access",
        });

        yield* adapter.sendTurn({ threadId, input: "first" });
        const firstChild = spawned[0];
        assert.ok(firstChild);

        emitJsonLine(firstChild, {
          type: "session",
          version: 3,
          id: "pi-uuid-123",
          timestamp: "2026-04-19T00:00:00Z",
          cwd: "/tmp",
        });
        emitJsonLine(firstChild, {
          type: "turn_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "ok" }],
            stopReason: "stop",
          },
        });
        firstChild.emit("exit", 0, null);
        yield* Effect.sleep(10);

        yield* adapter.sendTurn({ threadId, input: "second" });
        const secondChild = spawned[1];
        assert.ok(secondChild);

        emitJsonLine(secondChild, {
          type: "turn_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "ok2" }],
            stopReason: "stop",
          },
        });
        secondChild.emit("exit", 0, null);

        const turnCalls = (
          spawnMock.mock.calls as Array<[string, string[], { env: Record<string, string> }]>
        ).filter(([, args]) => !args.includes("--list-models"));

        const [firstBin, firstCallArgs, firstOpts] = turnCalls[0] ?? [];
        assert.equal(firstBin, "fake-pi");
        assert.ok(firstCallArgs);
        assert.equal(firstCallArgs.includes("--session"), false);
        assert.equal(firstCallArgs[0], "-p");
        assert.equal(firstOpts?.env.PI_CACHE_RETENTION, "long");

        const [, secondCallArgs, secondOpts] = turnCalls[1] ?? [];
        assert.ok(secondCallArgs);
        assert.equal(secondCallArgs[0], "--session");
        assert.equal(secondCallArgs[1], "pi-uuid-123");
        assert.equal(secondOpts?.env.PI_CACHE_RETENTION, "long");
      }).pipe(Effect.provide(PiAdapterTestLayer)),
    );
  });

  it("emits thread.token-usage.updated from pi's message.usage", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const { turnChildren } = installPiSpawnMock();

        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-pi-usage");

        yield* adapter.startSession({
          provider: "pi",
          threadId,
          runtimeMode: "full-access",
        });

        const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 4)).pipe(
          Effect.forkChild,
        );
        yield* Effect.sleep(0);

        yield* adapter.sendTurn({ threadId, input: "Summarize the repo." });
        const child = turnChildren[0];
        assert.ok(child);

        emitJsonLine(child, {
          type: "turn_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "done" }],
            stopReason: "stop",
            usage: {
              input: 1234,
              output: 567,
              cacheRead: 200,
              cacheWrite: 0,
              totalTokens: 2001,
            },
          },
        });
        child.emit("exit", 0, null);

        const events = yield* joinEvents(eventsFiber);
        const usageEvent = events.find((e) => e.type === "thread.token-usage.updated");
        assert.ok(usageEvent, "expected a thread.token-usage.updated event");
        if (usageEvent?.type !== "thread.token-usage.updated") {
          throw new Error("expected thread.token-usage.updated");
        }
        const { usage } = usageEvent.payload;
        assert.equal(usage.usedTokens, 2001);
        assert.equal(usage.lastUsedTokens, 2001);
        assert.equal(usage.inputTokens, 1434);
        assert.equal(usage.lastInputTokens, 1434);
        assert.equal(usage.cachedInputTokens, 200);
        assert.equal(usage.outputTokens, 567);
      }).pipe(Effect.provide(PiAdapterTestLayer)),
    );
  });

  describe("RPC transport", () => {
    function installRpcSpawnMock(): { readonly rpcChildren: FakeRpcChildProcess[] } {
      const rpcChildren: FakeRpcChildProcess[] = [];
      spawnMock.mockReset();
      spawnMock.mockImplementation((_bin: string, args: string[]) => {
        if (args.includes("--list-models")) {
          const catalog = new FakeChildProcess();
          setImmediate(() => catalog.emit("exit", 0, null));
          return catalog;
        }
        const child = new FakeRpcChildProcess();
        rpcChildren.push(child);
        return child;
      });
      return { rpcChildren };
    }

    it("spawns a long-lived rpc child and reuses it across turns", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();

          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-persistence");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10); // allow catalog probe + attachSessionListeners

          assert.equal(rpcChildren.length, 1, "exactly one rpc child after startSession");
          const child = rpcChildren[0]!;

          // First turn
          const sendPromise1 = Effect.runPromise(
            adapter
              .sendTurn({ threadId, input: "hello" })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const frames1 = child.writtenFrames();
          const firstPrompt = frames1.find((f) => f.type === "prompt");
          assert.ok(firstPrompt, "prompt frame should be written");
          assert.equal(firstPrompt?.message, "hello");
          child.respondTo(firstPrompt!, { success: true });
          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "ok" }],
              stopReason: "stop",
            },
          });
          yield* Effect.promise(() => sendPromise1);

          // Second turn — same child, not a new spawn
          const sendPromise2 = Effect.runPromise(
            adapter
              .sendTurn({ threadId, input: "second" })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          assert.equal(rpcChildren.length, 1, "no second spawn for second turn");

          const frames2 = child.writtenFrames();
          const secondPrompt = frames2.filter((f) => f.type === "prompt")[1];
          assert.ok(secondPrompt, "second prompt frame should be written");
          assert.equal(secondPrompt?.message, "second");
          child.respondTo(secondPrompt!, { success: true });
          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "done" }],
              stopReason: "stop",
            },
          });
          yield* Effect.promise(() => sendPromise2);
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("interruptTurn sends abort frame rather than killing the child", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();

          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-abort");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);

          const child = rpcChildren[0]!;

          const sendPromise = Effect.runPromise(
            adapter
              .sendTurn({ threadId, input: "long task" })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const promptFrame = child.writtenFrames().find((f) => f.type === "prompt")!;
          child.respondTo(promptFrame, { success: true });
          yield* Effect.promise(() => sendPromise);

          // Fire abort
          const abortPromise = Effect.runPromise(
            adapter.interruptTurn(threadId).pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const abortFrame = child.writtenFrames().find((f) => f.type === "abort");
          assert.ok(abortFrame, "abort frame should be written");
          child.respondTo(abortFrame!, { success: true });
          yield* Effect.promise(() => abortPromise);

          assert.equal(child.killed, false, "rpc child should stay alive after abort");
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("emits thread.token-usage.updated from turn_end notifications in rpc mode", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();

          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-usage");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);

          const child = rpcChildren[0]!;

          // Subscribe after startSession; we only need post-sendTurn events.
          // Expected: turn.started, content.delta, thread.token-usage.updated, turn.completed.
          const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 4)).pipe(
            Effect.forkChild,
          );
          yield* Effect.sleep(0);

          const sendPromise = Effect.runPromise(
            adapter
              .sendTurn({ threadId, input: "estimate" })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const promptFrame = child.writtenFrames().find((f) => f.type === "prompt")!;
          child.respondTo(promptFrame, { success: true });

          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "answer" }],
              stopReason: "stop",
              usage: {
                input: 100,
                output: 50,
                cacheRead: 20,
                cacheWrite: 0,
                totalTokens: 170,
              },
            },
          });
          yield* Effect.promise(() => sendPromise);

          const events = yield* joinEvents(eventsFiber);
          const usage = events.find((e) => e.type === "thread.token-usage.updated");
          assert.ok(usage, "expected thread.token-usage.updated");
          if (usage?.type !== "thread.token-usage.updated") throw new Error();
          assert.equal(usage.payload.usage.usedTokens, 170);
          assert.equal(usage.payload.usage.inputTokens, 120);
          assert.equal(usage.payload.usage.outputTokens, 50);
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("includes images array on prompt frames when attachments are present", async () => {
      const baseDir = mkdtempSync(pathJoin(tmpdir(), "pi-attach-"));
      // Mirror deriveServerPaths: baseDir/userdata/attachments
      const attachmentsDir = pathJoin(baseDir, "userdata", "attachments");
      const fs = await import("node:fs/promises");
      await fs.mkdir(attachmentsDir, { recursive: true });

      const attachment = {
        type: "image" as const,
        id: "thread-pi-rpc-attach-abcd1234",
        name: "pic.png",
        mimeType: "image/png",
        sizeBytes: 5,
      };
      const filePath = pathJoin(attachmentsDir, attachmentRelativePath(attachment));
      writeFileSync(filePath, Buffer.from("hello"));

      const attachLayer = makePiAdapterLive().pipe(
        Layer.provideMerge(ServerConfig.layerTest(process.cwd(), baseDir)),
        Layer.provideMerge(
          ServerSettingsService.layerTest({
            providers: {
              pi: {
                binaryPath: "fake-pi",
                defaultProvider: "",
                customModels: [],
                enabled: true,
                transport: "rpc",
              },
            },
          }),
        ),
        Layer.provideMerge(NodeServices.layer),
      );

      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-attach");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const sendPromise = Effect.runPromise(
            adapter
              .sendTurn({
                threadId,
                input: "what's in this?",
                attachments: [attachment],
              })
              .pipe(Effect.provide(attachLayer)),
          );
          yield* Effect.sleep(20);
          const promptFrame = child.writtenFrames().find((f) => f.type === "prompt") as {
            readonly type: string;
            readonly message: string;
            readonly images?: ReadonlyArray<{
              readonly type: string;
              readonly data: string;
              readonly mimeType: string;
            }>;
          };
          assert.ok(promptFrame, "prompt frame should be written");
          assert.ok(Array.isArray(promptFrame.images), "prompt frame should include images[]");
          assert.equal(promptFrame.images?.length, 1);
          assert.equal(promptFrame.images?.[0]?.type, "image");
          assert.equal(promptFrame.images?.[0]?.mimeType, "image/png");
          assert.equal(promptFrame.images?.[0]?.data, Buffer.from("hello").toString("base64"));

          child.respondTo(promptFrame as Record<string, unknown>, { success: true });
          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "done" }],
              stopReason: "stop",
            },
          });
          yield* Effect.promise(() => sendPromise);
        }).pipe(Effect.provide(attachLayer)),
      );
    });

    it("rejects attachments in json transport with a validation error", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const spawned: FakeChildProcess[] = [];
          spawnMock.mockReset();
          spawnMock.mockImplementation((_bin: string, args: string[]) => {
            const child = new FakeChildProcess();
            if (args.includes("--list-models")) {
              setImmediate(() => child.emit("exit", 0, null));
              return child;
            }
            spawned.push(child);
            return child;
          });

          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-json-attach");
          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });

          const attempt = yield* adapter
            .sendTurn({
              threadId,
              input: "go",
              attachments: [
                {
                  type: "image",
                  id: "thread-pi-json-attach-xyz",
                  name: "pic.png",
                  mimeType: "image/png",
                  sizeBytes: 3,
                },
              ],
            })
            .pipe(Effect.flip);

          assert.equal(attempt._tag, "ProviderAdapterValidationError");
        }).pipe(Effect.provide(PiAdapterTestLayer)),
      );
    });

    it("readThread returns bucketed turns from pi's get_messages", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-read");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const readPromise = Effect.runPromise(
            adapter.readThread(threadId).pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(5);
          const getMessagesFrame = child
            .writtenFrames()
            .find((f) => f.type === "get_messages") as Record<string, unknown>;
          assert.ok(getMessagesFrame, "expected get_messages frame");

          child.respondTo(getMessagesFrame, {
            success: true,
            data: {
              messages: [
                {
                  id: "e1",
                  parentId: null,
                  timestamp: "2026-04-20T00:00:01Z",
                  message: { role: "user", content: "first prompt", timestamp: 1 },
                },
                {
                  id: "e2",
                  parentId: "e1",
                  timestamp: "2026-04-20T00:00:02Z",
                  message: {
                    role: "assistant",
                    content: [{ type: "text", text: "ok" }],
                    timestamp: 2,
                  },
                },
                {
                  id: "e3",
                  parentId: "e2",
                  timestamp: "2026-04-20T00:00:03Z",
                  message: { role: "user", content: "second prompt", timestamp: 3 },
                },
                {
                  id: "e4",
                  parentId: "e3",
                  timestamp: "2026-04-20T00:00:04Z",
                  message: {
                    role: "assistant",
                    content: [{ type: "text", text: "done" }],
                    timestamp: 4,
                  },
                },
              ],
            },
          });

          const snapshot = yield* Effect.promise(() => readPromise);
          assert.equal(snapshot.threadId, threadId);
          assert.equal(snapshot.turns.length, 2);
          assert.equal(snapshot.turns[0]?.id, "pi-turn-e1");
          assert.equal(snapshot.turns[0]?.items.length, 2); // user + assistant
          assert.equal(snapshot.turns[1]?.id, "pi-turn-e3");
          assert.equal(snapshot.turns[1]?.items.length, 2);
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("resets session status to ready after turn_end", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-idle-after-turn");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const sendPromise = Effect.runPromise(
            adapter
              .sendTurn({ threadId, input: "ping" })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const promptFrame = child.writtenFrames().find((f) => f.type === "prompt")!;
          child.respondTo(promptFrame, { success: true });

          // While the turn is in flight, session should be running.
          const runningSessions = yield* adapter.listSessions();
          const running = runningSessions.find((s) => s.threadId === threadId)!;
          assert.equal(running.status, "running");
          assert.ok(running.activeTurnId, "activeTurnId should be set during turn");

          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "pong" }],
              stopReason: "stop",
            },
          });
          yield* Effect.promise(() => sendPromise);
          yield* Effect.sleep(5);

          // After turn_end the session should snap back to idle.
          const idleSessions = yield* adapter.listSessions();
          const idle = idleSessions.find((s) => s.threadId === threadId)!;
          assert.equal(idle.status, "ready", "status should return to ready");
          assert.equal(idle.activeTurnId, undefined, "activeTurnId should be cleared");
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("rejects a second sendTurn while one is in flight", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-concurrent");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const firstTurn = Effect.runPromise(
            adapter
              .sendTurn({ threadId, input: "first" })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const firstPrompt = child.writtenFrames().find((f) => f.type === "prompt")!;
          child.respondTo(firstPrompt, { success: true });
          // Do NOT emit turn_end yet — keep the turn in flight.

          const attempt = yield* adapter.sendTurn({ threadId, input: "second" }).pipe(Effect.flip);
          assert.equal(attempt._tag, "ProviderAdapterRequestError");

          // Settle the first turn so the fiber drains.
          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "done" }],
              stopReason: "stop",
            },
          });
          yield* Effect.promise(() => firstTurn);
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("warns on bare-slug mid-session switch and keeps session.model pinned", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-bare-slug");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
            modelSelection: { provider: "pi", model: "openai-codex/gpt-5.4" },
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 2)).pipe(
            Effect.forkChild,
          );
          yield* Effect.sleep(0);

          const sendPromise = Effect.runPromise(
            adapter
              .sendTurn({
                threadId,
                input: "switch?",
                modelSelection: { provider: "pi", model: "claude-haiku-4-5" }, // bare
              })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const promptFrame = child.writtenFrames().find((f) => f.type === "prompt")!;

          // No set_model frame should have been sent for the bare slug.
          assert.equal(
            child.writtenFrames().some((f) => f.type === "set_model"),
            false,
            "set_model should not be called for bare slugs",
          );

          child.respondTo(promptFrame, { success: true });
          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "k" }],
              stopReason: "stop",
            },
          });
          yield* Effect.promise(() => sendPromise);

          const events = yield* joinEvents(eventsFiber);
          const warning = events.find((e) => e.type === "runtime.warning");
          assert.ok(warning, "should emit a runtime.warning");
          if (warning?.type !== "runtime.warning") throw new Error();
          assert.match(warning.payload.message, /bare slug/i);

          const sessions = yield* adapter.listSessions();
          const session = sessions.find((s) => s.threadId === threadId)!;
          assert.equal(
            session.model,
            "openai-codex/gpt-5.4",
            "session.model should stay pinned to the pi-active model when switch is skipped",
          );
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("queueMessage(steer) writes a steer frame while a turn is in flight", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-steer");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          // Start a turn and leave it in flight.
          const turnPromise = Effect.runPromise(
            adapter.sendTurn({ threadId, input: "run" }).pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const promptFrame = child.writtenFrames().find((f) => f.type === "prompt")!;
          child.respondTo(promptFrame, { success: true });

          // Now queue a steer mid-turn.
          const steerPromise = Effect.runPromise(
            adapter
              .queueMessage(threadId, "steer", { threadId, input: "focus on errors" })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const steerFrame = child.writtenFrames().find((f) => f.type === "steer")!;
          assert.ok(steerFrame, "steer frame should be written");
          assert.equal(steerFrame.message, "focus on errors");
          child.respondTo(steerFrame, { success: true });
          yield* Effect.promise(() => steerPromise);

          // And a follow-up.
          const followPromise = Effect.runPromise(
            adapter
              .queueMessage(threadId, "followUp", { threadId, input: "then summarize" })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const followFrame = child.writtenFrames().find((f) => f.type === "follow_up")!;
          assert.ok(followFrame, "follow_up frame should be written");
          assert.equal(followFrame.message, "then summarize");
          child.respondTo(followFrame, { success: true });
          yield* Effect.promise(() => followPromise);

          // Settle the turn so fibers drain.
          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "done" }],
              stopReason: "stop",
            },
          });
          yield* Effect.promise(() => turnPromise);
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("maps pi queue_update notifications to turn.queue.updated events", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-queue-update");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          // Subscribe before the turn so we capture turn.started + queue events.
          const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 3)).pipe(
            Effect.forkChild,
          );
          yield* Effect.sleep(0);

          const turnPromise = Effect.runPromise(
            adapter
              .sendTurn({ threadId, input: "long task" })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const promptFrame = child.writtenFrames().find((f) => f.type === "prompt")!;
          child.respondTo(promptFrame, { success: true });

          child.notify({
            type: "queue_update",
            steering: ["focus on errors"],
            followUp: ["then summarize"],
          });

          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "done" }],
              stopReason: "stop",
            },
          });
          yield* Effect.promise(() => turnPromise);

          const events = yield* joinEvents(eventsFiber);
          const queueEvent = events.find((e) => e.type === "turn.queue.updated");
          assert.ok(queueEvent, "expected turn.queue.updated event");
          if (queueEvent?.type !== "turn.queue.updated") throw new Error();
          assert.deepEqual(queueEvent.payload.steering, ["focus on errors"]);
          assert.deepEqual(queueEvent.payload.followUp, ["then summarize"]);
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("queueMessage rejects when no turn is active", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-queue-no-turn");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);

          const attempt = yield* adapter
            .queueMessage(threadId, "steer", { threadId, input: "hi" })
            .pipe(Effect.flip);
          assert.equal(attempt._tag, "ProviderAdapterValidationError");
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("declares sessionModelSwitch in-session capability", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          assert.equal(adapter.capabilities.sessionModelSwitch, "in-session");
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("synthesizes an active turn when pi reports isStreaming on reconnect", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const rpcChildren: FakeRpcChildProcess[] = [];
          spawnMock.mockReset();
          spawnMock.mockImplementation((_bin: string, args: string[]) => {
            if (args.includes("--list-models")) {
              const catalog = new FakeChildProcess();
              setImmediate(() => catalog.emit("exit", 0, null));
              return catalog;
            }
            const child = new FakeRpcChildProcess();
            child.configureState({
              sessionId: "pi-sess-abcd",
              sessionFile: "/tmp/pi-sessions/abcd.jsonl",
              isStreaming: true,
            });
            rpcChildren.push(child);
            return child;
          });

          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-resumed");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(20); // allow get_state round-trip

          const sessions = yield* adapter.listSessions();
          const session = sessions.find((s) => s.threadId === threadId)!;
          assert.equal(
            session.status,
            "running",
            "session should be running since pi is already streaming",
          );
          assert.ok(session.activeTurnId, "a placeholder turn should be active");

          // Next sendTurn should hit our local guard (cleaner than pi's cryptic error).
          const attempt = yield* adapter
            .sendTurn({ threadId, input: "too soon" })
            .pipe(Effect.flip);
          assert.equal(attempt._tag, "ProviderAdapterRequestError");
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("translates pi 'already processing' error into an actionable message", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-already-processing");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const sendPromise = Effect.runPromise(
            adapter.sendTurn({ threadId, input: "hi" }).pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const promptFrame = child.writtenFrames().find((f) => f.type === "prompt")!;
          // Pi rejects with its real error string.
          child.respondTo(promptFrame, {
            success: false,
            error:
              "Agent is already processing. Specify streamingBehavior ('steer' or 'followUp') to queue the message.",
          });
          yield* Effect.promise(() => sendPromise);

          // Next sendTurn should rebound off our guard (we synthesized an activeTurn).
          const nextAttempt = yield* adapter
            .sendTurn({ threadId, input: "retry" })
            .pipe(Effect.flip);
          assert.equal(nextAttempt._tag, "ProviderAdapterRequestError");
          assert.match(
            (nextAttempt as { detail: string }).detail,
            /in flight/i,
            "second sendTurn should be blocked by the post-error state sync",
          );
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("turn.started reports the pi-active model when a bare-slug switch is skipped", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-turnstarted-effective");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
            modelSelection: { provider: "pi", model: "openai-codex/gpt-5.4" },
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 2)).pipe(
            Effect.forkChild,
          );
          yield* Effect.sleep(0);

          const sendPromise = Effect.runPromise(
            adapter
              .sendTurn({
                threadId,
                input: "go",
                modelSelection: { provider: "pi", model: "claude-haiku-4-5" }, // bare
              })
              .pipe(Effect.provide(makeTestLayer("rpc"))),
          );
          yield* Effect.sleep(10);
          const promptFrame = child.writtenFrames().find((f) => f.type === "prompt")!;
          child.respondTo(promptFrame, { success: true });
          child.notify({
            type: "turn_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "k" }],
              stopReason: "stop",
            },
          });
          yield* Effect.promise(() => sendPromise);

          const events = yield* joinEvents(eventsFiber);
          const started = events.find((e) => e.type === "turn.started");
          assert.ok(started);
          if (started?.type !== "turn.started") throw new Error();
          assert.equal(
            started.payload.model,
            "openai-codex/gpt-5.4",
            "turn.started should announce the pi-active model, not the requested one",
          );
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("bridges confirm → request.opened → respondToRequest → extension_ui_response", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-ext-confirm");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 1)).pipe(
            Effect.forkChild,
          );
          yield* Effect.sleep(0);

          child.notify({
            type: "extension_ui_request",
            id: "ext-req-confirm-1",
            method: "confirm",
            title: "Allow command?",
            message: "Run `rm -rf dist`?",
          });
          yield* Effect.sleep(10);

          // The adapter surfaces the confirm as a request.opened with a
          // Workbench-owned requestId (different from pi's id). Respond with
          // `accept` and observe both the resolved event and the wire frame
          // back to pi.
          const eventsSoFar = yield* adapter.streamEvents.pipe(Stream.take(0), Stream.runCollect);
          void eventsSoFar; // discard — we gather via joinEvents below

          const session = yield* adapter.listSessions();
          void session;

          // Fish the workbench requestId back out of the stream.
          // Stream.take(2) above is racing with our emits; we poll adapter
          // state via the fiber at the end.

          // Answer the request with `accept`.
          // We need the Workbench requestId from the emitted event. Read it
          // out of the collected events by the time the fiber joins.
          const collected = yield* joinEvents(eventsFiber);
          const opened = collected.find((e) => e.type === "request.opened");
          assert.ok(opened, "request.opened should have been emitted");
          if (opened?.type !== "request.opened") throw new Error();
          assert.equal(opened.payload.requestType, "dynamic_tool_call");
          const workbenchReqId = opened.requestId;
          assert.ok(workbenchReqId);

          yield* adapter.respondToRequest(
            threadId,
            ApprovalRequestId.make(String(workbenchReqId)),
            "accept",
          );
          yield* Effect.sleep(5);

          const responseFrame = child
            .writtenFrames()
            .find((f) => f.type === "extension_ui_response");
          assert.ok(responseFrame, "adapter should send extension_ui_response");
          assert.equal(responseFrame?.id, "ext-req-confirm-1");
          assert.equal(responseFrame?.confirmed, true);
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("bridges select → user-input.requested → respondToUserInput → extension_ui_response", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-ext-select");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 1)).pipe(
            Effect.forkChild,
          );
          yield* Effect.sleep(0);

          child.notify({
            type: "extension_ui_request",
            id: "ext-req-sel-1",
            method: "select",
            title: "Pick branch",
            message: "Which branch?",
            options: ["main", "dev"],
          });
          yield* Effect.sleep(10);

          const collected = yield* joinEvents(eventsFiber);
          const requested = collected.find((e) => e.type === "user-input.requested");
          assert.ok(requested, "user-input.requested should have been emitted");
          if (requested?.type !== "user-input.requested") throw new Error();
          assert.equal(requested.payload.questions.length, 1);
          const question = requested.payload.questions[0]!;
          assert.equal(question.header, "Pick branch");
          assert.equal(question.options.length, 2);
          const workbenchReqId = requested.requestId;
          assert.ok(workbenchReqId);

          yield* adapter.respondToUserInput(
            threadId,
            ApprovalRequestId.make(String(workbenchReqId)),
            { [question.id]: "dev" },
          );
          yield* Effect.sleep(5);

          const responseFrame = child
            .writtenFrames()
            .find((f) => f.type === "extension_ui_response");
          assert.ok(responseFrame);
          assert.equal(responseFrame?.id, "ext-req-sel-1");
          assert.equal(responseFrame?.value, "dev");
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("maps notify → runtime.warning / runtime.error and setStatus → thread.metadata.updated", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { rpcChildren } = installRpcSpawnMock();
          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-rpc-ext-ff");

          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep(10);
          const child = rpcChildren[0]!;

          const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 3)).pipe(
            Effect.forkChild,
          );
          yield* Effect.sleep(0);

          child.notify({
            type: "extension_ui_request",
            id: "ext-notify-1",
            method: "notify",
            message: "Cache cleared",
            notifyType: "info",
          });
          child.notify({
            type: "extension_ui_request",
            id: "ext-notify-2",
            method: "notify",
            message: "API quota exceeded",
            notifyType: "error",
          });
          child.notify({
            type: "extension_ui_request",
            id: "ext-status-1",
            method: "setStatus",
            statusKey: "tests",
            statusText: "running…",
          });
          yield* Effect.sleep(10);

          const collected = yield* joinEvents(eventsFiber);
          const warn = collected.find(
            (e) => e.type === "runtime.warning" && /Cache cleared/.test(e.payload.message),
          );
          const err = collected.find(
            (e) => e.type === "runtime.error" && /API quota/.test(e.payload.message),
          );
          const meta = collected.find(
            (e) =>
              e.type === "thread.metadata.updated" &&
              e.payload.metadata !== undefined &&
              (e.payload.metadata as Record<string, unknown>)["pi.status.tests"] === "running…",
          );
          assert.ok(warn, "notify(info) should emit runtime.warning");
          assert.ok(err, "notify(error) should emit runtime.error");
          assert.ok(meta, "setStatus should emit thread.metadata.updated with pi.status.* key");

          // Fire-and-forget methods must NOT write extension_ui_response.
          const responses = child.writtenFrames().filter((f) => f.type === "extension_ui_response");
          assert.equal(
            responses.length,
            0,
            "notify/setStatus should not be acknowledged on the wire",
          );
        }).pipe(Effect.provide(makeTestLayer("rpc"))),
      );
    });

    it("readThread returns empty turns in json transport", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const spawned: FakeChildProcess[] = [];
          spawnMock.mockReset();
          spawnMock.mockImplementation((_bin: string, args: string[]) => {
            const child = new FakeChildProcess();
            if (args.includes("--list-models")) {
              setImmediate(() => child.emit("exit", 0, null));
              return child;
            }
            spawned.push(child);
            return child;
          });

          const adapter = yield* PiAdapter;
          const threadId = asThreadId("thread-pi-json-read");
          yield* adapter.startSession({
            provider: "pi",
            threadId,
            runtimeMode: "full-access",
          });

          const snapshot = yield* adapter.readThread(threadId);
          assert.equal(snapshot.threadId, threadId);
          assert.equal(snapshot.turns.length, 0);
        }).pipe(Effect.provide(PiAdapterTestLayer)),
      );
    });
  });
});
