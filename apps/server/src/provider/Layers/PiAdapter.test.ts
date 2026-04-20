import assert from "node:assert/strict";
import { EventEmitter } from "node:events";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  type ProviderRuntimeEvent,
  ThreadId,
  type ToolLifecycleItemType,
} from "@workbench/contracts";
import { Effect, Fiber, Layer, Option, Stream } from "effect";
import { describe, it, vi } from "vitest";

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

const PiAdapterTestLayer = makePiAdapterLive().pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(
    ServerSettingsService.layerTest({
      providers: {
        pi: {
          binaryPath: "fake-pi",
          defaultProvider: "",
          customModels: [],
          enabled: true,
        },
      },
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);

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
});
