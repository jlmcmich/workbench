/**
 * PiAdapterLive — pi coding agent adapter with two transports.
 *
 * **RPC transport (default, `providers.pi.transport: "rpc"`).** One long-lived
 * `pi --mode rpc` child per session. `sendTurn` writes a `{type:"prompt"}`
 * frame to stdin; notifications (`message_update`, `tool_execution_*`,
 * `turn_end`, `extension_ui_request`, etc) stream back over stdout and are
 * mapped onto the canonical `ProviderRuntimeEvent` stream. Interrupt is a
 * graceful `{type:"abort"}` RPC, not a process kill. Pi owns session
 * persistence at `~/.pi/agent/sessions/` and the single process keeps the
 * prompt cache warm across turns.
 *
 * **JSON transport (fallback, `providers.pi.transport: "json"`).** Legacy
 * per-turn subprocess path: `pi -p --mode json --model <slug> <prompt>`
 * spawned anew for each `sendTurn`, with `--session <uuid>` appended after
 * the first turn's session header is captured. Retained until the RPC path
 * has proven stable in the field (see Phase 2 plan).
 *
 * Extended prompt caching via `PI_CACHE_RETENTION=long` is enabled on both
 * transports.
 *
 * Unsupported features (approvals, structured user-input, rollback) still
 * fail fast with ProviderAdapterRequestError in both modes; `readThread`
 * returns an empty snapshot. Those are scheduled for Phase 2.2+.
 *
 * @module PiAdapterLive
 */
import { type ChildProcess, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";

import {
  type ChatAttachment,
  RuntimeItemId,
  EventId,
  type PiTransport,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ThreadId,
  type ThreadTokenUsageSnapshot,
  type ToolLifecycleItemType,
  TurnId,
} from "@workbench/contracts";
import { Effect, FileSystem, Layer, PubSub, Stream } from "effect";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { PiAdapter, type PiAdapterShape } from "../Services/PiAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";
import { createPiRpcClient, type PiRpcClientShape } from "./PiRpcClient.ts";

import {
  DEFAULT_PI_MODEL_SLUG,
  loadPiModelCatalog,
  normalizePiModelSlug,
  parsePiContextLabel,
  type PiCatalogEntry,
} from "../piRuntime.ts";

const PROVIDER = "pi" as const;
const DEFAULT_MODEL = DEFAULT_PI_MODEL_SLUG;

export interface PiAdapterLiveOptions {
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
}

interface PiTurnContext {
  readonly turnId: TurnId;
  /**
   * Per-turn subprocess (JSON transport only). Undefined when the turn is
   * running against a long-lived RPC session — the RPC child lives on
   * `PiSessionContext`, not on the turn.
   */
  readonly child: ChildProcess | undefined;
  /** Set true once we have emitted a terminal turn event (completed / aborted / failed). */
  settled: boolean;
  /** True once any assistant text delta has been emitted for the turn. */
  assistantTextSeen: boolean;
  /** Tracks in-flight tool calls so updates/completions can reuse canonical item ids. */
  readonly toolItems: Map<string, PiToolState>;
  /** Context-window capacity for the model driving this turn, if known. */
  readonly maxTokens: number | undefined;
}

interface PiSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  activeTurn: PiTurnContext | undefined;
  stopped: boolean;
  /** Which transport this session is running on. Decided at startSession time. */
  readonly transport: PiTransport;
  /**
   * pi's session UUID, captured from the first `{"type":"session","id":...}`
   * JSON-mode line or from `get_state` in RPC mode. Used to resume pi's own
   * session state across transport restarts.
   */
  piSessionUuid: string | undefined;
  /**
   * Last usage snapshot derived from `AssistantMessage.usage` on pi's
   * `message_end`/`turn_end` events, carried forward across turns so the
   * context-window UI reflects cumulative usage.
   */
  lastKnownTokenUsage: ThreadTokenUsageSnapshot | undefined;
  /**
   * Long-lived RPC transport handle (RPC transport only). Retained across
   * turns so a single process keeps pi's session memory and prompt cache
   * warm. Undefined for JSON-mode sessions.
   */
  rpc: PiRpcClientShape | undefined;
  rpcChild: ChildProcess | undefined;
  /**
   * Model string currently active on pi in RPC mode. When a subsequent
   * `sendTurn` requests a different model we issue `set_model` before
   * `prompt`; in JSON mode each turn spawn passes `--model` so this field
   * stays undefined.
   */
  rpcActiveModel: string | undefined;
  /** Max-tokens capacity for the active RPC model, for usage snapshot shaping. */
  rpcMaxTokens: number | undefined;
}

interface PiToolState {
  readonly itemId: string;
  readonly toolCallId: string;
  readonly toolName: string;
  readonly itemType: ToolLifecycleItemType;
  readonly title: string;
  readonly summary: string;
}

function nowIso(): string {
  return new Date().toISOString();
}

/**
 * Split a Workbench `{backend}/{model}` slug into the RPC-flag pair
 * pi expects on `--provider <backend> --model <model>`. Bare slugs return
 * `{ provider: undefined }` — pi then picks a default backend that supports
 * the model. Mirrors the routing rules documented in pi's `--mode rpc`
 * section of `packages/coding-agent/docs/rpc.md`.
 */
function splitPiModelSlug(slug: string): {
  readonly provider: string | undefined;
  readonly model: string;
} {
  const trimmed = normalizePiModelSlug(slug);
  const slash = trimmed.indexOf("/");
  if (slash <= 0 || slash === trimmed.length - 1) {
    return { provider: undefined, model: trimmed };
  }
  return {
    provider: trimmed.slice(0, slash),
    model: trimmed.slice(slash + 1),
  };
}

function killTurn(turn: PiTurnContext, signal: NodeJS.Signals = "SIGKILL") {
  const { child } = turn;
  if (!child) return; // RPC turn — abort is issued via rpc.call("abort") instead.
  if (!child.killed) {
    try {
      child.kill(signal);
    } catch {
      // Best effort — process may have already exited.
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function asTrimmedString(value: unknown): string | undefined {
  const text = asString(value)?.trim();
  return text && text.length > 0 ? text : undefined;
}

function truncateText(value: string, maxLength = 400): string {
  return value.length > maxLength ? `${value.slice(0, maxLength - 3)}...` : value;
}

function summarizeJson(value: unknown, maxLength = 400): string | undefined {
  try {
    return truncateText(JSON.stringify(value), maxLength);
  } catch {
    return undefined;
  }
}

function extractAssistantText(message: unknown): string {
  if (!isRecord(message)) return "";
  const content = message.content;
  if (!Array.isArray(content)) return "";
  const parts: Array<string> = [];
  for (const block of content) {
    if (isRecord(block) && block.type === "text" && typeof block.text === "string") {
      parts.push(block.text);
    }
  }
  return parts.join("");
}

function extractTextFromToolResult(value: unknown): string | undefined {
  if (!isRecord(value)) return undefined;
  const content = value.content;
  if (!Array.isArray(content)) return undefined;

  const textParts = content.flatMap((entry) => {
    if (!isRecord(entry)) return [];
    const text = asTrimmedString(entry.text);
    return text ? [text] : [];
  });

  return textParts.length > 0 ? truncateText(textParts.join("\n\n"), 1_000) : undefined;
}

function extractToolCallId(raw: Record<string, unknown>): string | undefined {
  return (
    asTrimmedString(raw.toolCallId) ??
    asTrimmedString(raw.callId) ??
    asTrimmedString(raw.id) ??
    asTrimmedString(raw.tool_use_id)
  );
}

function extractToolName(raw: Record<string, unknown>): string | undefined {
  return (
    asTrimmedString(raw.toolName) ??
    asTrimmedString(raw.name) ??
    asTrimmedString(raw.tool) ??
    asTrimmedString(raw.tool_name)
  );
}

function classifyPiToolItemType(toolName: string): ToolLifecycleItemType {
  const normalized = toolName.toLowerCase();
  if (
    normalized.includes("bash") ||
    normalized.includes("command") ||
    normalized.includes("shell") ||
    normalized.includes("terminal") ||
    normalized === "run"
  ) {
    return "command_execution";
  }
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("patch") ||
    normalized.includes("replace") ||
    normalized.includes("delete") ||
    normalized.includes("create")
  ) {
    return "file_change";
  }
  if (normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  if (
    normalized.includes("agent") ||
    normalized.includes("subagent") ||
    normalized.includes("sub-agent") ||
    normalized.includes("task")
  ) {
    return "collab_agent_tool_call";
  }
  if (normalized.includes("web")) {
    return "web_search";
  }
  if (normalized.includes("image")) {
    return "image_view";
  }
  return "dynamic_tool_call";
}

function titleForPiTool(itemType: ToolLifecycleItemType): string {
  switch (itemType) {
    case "command_execution":
      return "Command run";
    case "file_change":
      return "File change";
    case "mcp_tool_call":
      return "MCP tool call";
    case "collab_agent_tool_call":
      return "Subagent task";
    case "web_search":
      return "Web search";
    case "image_view":
      return "Image view";
    case "dynamic_tool_call":
      return "Tool call";
  }
}

function summarizeToolArgs(toolName: string, args: unknown): string {
  if (isRecord(args)) {
    const command =
      asTrimmedString(args.command) ?? asTrimmedString(args.cmd) ?? asTrimmedString(args.input);
    if (command) {
      return `${toolName}: ${truncateText(command)}`;
    }

    const path =
      asTrimmedString(args.path) ??
      asTrimmedString(args.filePath) ??
      asTrimmedString(args.filename) ??
      asTrimmedString(args.relativePath);
    if (path) {
      return `${toolName}: ${truncateText(path)}`;
    }
  }

  const serialized = summarizeJson(args);
  return serialized ? `${toolName}: ${serialized}` : toolName;
}

function summarizeToolResult(result: unknown, isError: boolean): string | undefined {
  const text = extractTextFromToolResult(result);
  if (text) {
    return text;
  }
  if (isRecord(result)) {
    const errorMessage =
      asTrimmedString(result.errorMessage) ??
      asTrimmedString(result.error) ??
      asTrimmedString(result.message);
    if (errorMessage) {
      return errorMessage;
    }
    const fullOutputPath = isRecord(result.details)
      ? asTrimmedString(result.details.fullOutputPath)
      : undefined;
    if (fullOutputPath) {
      return fullOutputPath;
    }
  }
  return isError ? "Tool failed." : undefined;
}

function extractMessageRole(message: unknown): string | undefined {
  if (!isRecord(message)) return undefined;
  if (typeof message.role === "string") return message.role;
  return undefined;
}

function extractStopReason(message: unknown): string | undefined {
  if (!isRecord(message)) return undefined;
  if (typeof message.stopReason === "string" && message.stopReason.trim().length > 0) {
    return message.stopReason.trim();
  }
  return undefined;
}

function isToolUseStopReason(stopReason: string | undefined): boolean {
  if (!stopReason) return false;
  const normalized = stopReason.trim().toLowerCase();
  return normalized === "tooluse" || normalized === "tool_use" || normalized === "tool-use";
}

function extractErrorMessage(raw: Record<string, unknown>): string | undefined {
  if (typeof raw.errorMessage === "string" && raw.errorMessage.trim().length > 0) {
    return raw.errorMessage.trim();
  }
  const message = raw.message;
  if (isRecord(message)) {
    if (typeof message.errorMessage === "string" && message.errorMessage.trim().length > 0) {
      return message.errorMessage.trim();
    }
    if (typeof message.error === "string" && message.error.trim().length > 0) {
      return message.error.trim();
    }
  }
  if (typeof raw.error === "string" && raw.error.trim().length > 0) {
    return raw.error.trim();
  }
  return undefined;
}

function asNonNegativeNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}

/**
 * Shape pi's `AssistantMessage.usage` into our canonical snapshot. Pi emits
 * `input`, `output`, `cacheRead`, `cacheWrite`, and `totalTokens` per message
 * (see pi-mono `packages/ai/src/types.ts`). `inputTokens` in the snapshot
 * sums prompt + cache-read + cache-write since that is the tokens fed to the
 * model. `usedTokens` reflects the cumulative request volume and is capped
 * at the model's context-window capacity when known.
 */
function normalizePiTokenUsage(
  value: unknown,
  maxTokens: number | undefined,
): ThreadTokenUsageSnapshot | undefined {
  if (!isRecord(value)) return undefined;

  const input = asNonNegativeNumber(value.input) ?? 0;
  const output = asNonNegativeNumber(value.output) ?? 0;
  const cacheRead = asNonNegativeNumber(value.cacheRead) ?? 0;
  const cacheWrite = asNonNegativeNumber(value.cacheWrite) ?? 0;
  const inputTokens = input + cacheRead + cacheWrite;

  const reportedTotal = asNonNegativeNumber(value.totalTokens);
  const derivedTotal = inputTokens + output;
  const totalProcessedTokens = reportedTotal && reportedTotal > 0 ? reportedTotal : derivedTotal;
  if (totalProcessedTokens <= 0) return undefined;

  const hasMax = typeof maxTokens === "number" && Number.isFinite(maxTokens) && maxTokens > 0;
  const usedTokens = hasMax ? Math.min(totalProcessedTokens, maxTokens) : totalProcessedTokens;

  return {
    usedTokens,
    lastUsedTokens: usedTokens,
    ...(totalProcessedTokens > usedTokens ? { totalProcessedTokens } : {}),
    ...(inputTokens > 0 ? { inputTokens, lastInputTokens: inputTokens } : {}),
    ...(cacheRead + cacheWrite > 0
      ? { cachedInputTokens: cacheRead + cacheWrite, lastCachedInputTokens: cacheRead + cacheWrite }
      : {}),
    ...(output > 0 ? { outputTokens: output, lastOutputTokens: output } : {}),
    ...(hasMax ? { maxTokens } : {}),
  };
}

/**
 * Bucket a pi session's `AgentMessage[]` (from `get_messages`) into
 * Workbench `ProviderThreadTurnSnapshot[]`. A "turn" in our model is one
 * user message plus everything pi produced before the next user message —
 * assistant messages, tool calls / results, bash executions, custom
 * entries. Each item is stored as-is so the orchestration layer can
 * decide how to project it; pi's own field names (`role`, `content`,
 * `toolCallId`, etc) are preserved.
 *
 * Messages that arrive before any user message (unusual, typically only
 * happens if pi opens a session with a seeded system prompt) are placed
 * in a synthetic bootstrap turn keyed by entry id.
 */
function mapPiMessagesToTurns(
  entries: ReadonlyArray<unknown>,
): ReadonlyArray<{ readonly id: TurnId; readonly items: ReadonlyArray<unknown> }> {
  const turns: Array<{ id: TurnId; items: Array<unknown> }> = [];
  let currentTurn: { id: TurnId; items: Array<unknown> } | undefined;

  for (const entry of entries) {
    if (!isRecord(entry)) continue;
    const message = isRecord(entry.message) ? entry.message : undefined;
    const role = asString(message?.role);
    const entryId = asTrimmedString(entry.id);
    const stableTurnId = entryId ? TurnId.make(`pi-turn-${entryId}`) : undefined;

    if (role === "user") {
      currentTurn = {
        id: stableTurnId ?? TurnId.make(`pi-turn-${randomUUID()}`),
        items: [entry],
      };
      turns.push(currentTurn);
      continue;
    }

    if (!currentTurn) {
      currentTurn = {
        id: stableTurnId ?? TurnId.make(`pi-turn-${randomUUID()}`),
        items: [],
      };
      turns.push(currentTurn);
    }
    currentTurn.items.push(entry);
  }

  return turns.map((turn) => ({ id: turn.id, items: turn.items }));
}

function buildEventBase(input: {
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly itemId?: string | undefined;
  readonly raw?: unknown;
}): Pick<
  ProviderRuntimeEvent,
  "eventId" | "provider" | "threadId" | "createdAt" | "turnId" | "itemId" | "raw"
> {
  return {
    eventId: EventId.make(randomUUID()),
    provider: PROVIDER,
    threadId: input.threadId,
    createdAt: nowIso(),
    ...(input.turnId ? { turnId: input.turnId } : {}),
    ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
    ...(input.raw !== undefined
      ? {
          // pi's NDJSON lines are not in the RuntimeEventRawSource union, so we
          // deliberately omit `raw` here to stay schema-compatible. Kept this
          // helper shape consistent with the other adapters for future lift.
        }
      : {}),
  };
}

export function makePiAdapterLive(options?: PiAdapterLiveOptions) {
  return Layer.effect(
    PiAdapter,
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsService;
      const serverConfig = yield* Effect.service(ServerConfig);
      const fileSystem = yield* FileSystem.FileSystem;
      const services = yield* Effect.context<never>();
      const nativeEventLogger =
        options?.nativeEventLogger ??
        (options?.nativeEventLogPath !== undefined
          ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, {
              stream: "native",
            })
          : undefined);

      const sessions = new Map<ThreadId, PiSessionContext>();
      const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();

      // Lazy pi catalog: populated the first time a capacity lookup is
      // requested. The catalog rarely changes and `pi --list-models` is
      // relatively slow, so we memoize across the adapter lifetime.
      let modelContextWindow: Map<string, number> | undefined;
      let modelContextWindowPromise: Promise<Map<string, number>> | undefined;
      const resolveModelContextWindow = async (binaryPath: string): Promise<Map<string, number>> => {
        if (modelContextWindow) return modelContextWindow;
        if (!modelContextWindowPromise) {
          modelContextWindowPromise = loadPiModelCatalog({ binaryPath })
            .then((entries: ReadonlyArray<PiCatalogEntry>) => {
              const map = new Map<string, number>();
              for (const entry of entries) {
                const capacity = parsePiContextLabel(entry.context);
                if (capacity === undefined) continue;
                map.set(entry.model, capacity);
                map.set(`${entry.backend}/${entry.model}`, capacity);
              }
              modelContextWindow = map;
              return map;
            })
            .catch(() => {
              const empty = new Map<string, number>();
              modelContextWindow = empty;
              return empty;
            });
        }
        return modelContextWindowPromise;
      };

      const emit = (event: ProviderRuntimeEvent) =>
        PubSub.publish(runtimeEvents, event).pipe(Effect.asVoid);
      const emitPromise = (event: ProviderRuntimeEvent) =>
        emit(event).pipe(Effect.runPromiseWith(services));

      /**
       * Resolve one ChatAttachment into pi's `{type:"image", data, mimeType}`
       * block. pi expects raw base64 (no `data:` URI prefix) and the same
       * mime-type string the user uploaded with. See
       * `packages/coding-agent/docs/rpc.md` §Image Content Format.
       */
      const resolvePiImageAttachment = (threadId: ThreadId, attachment: ChatAttachment) =>
        Effect.gen(function* () {
          const attachmentPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          });
          if (!attachmentPath) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "sendTurn",
              detail: `Invalid attachment id '${attachment.id}'.`,
            });
          }
          const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "sendTurn",
                  detail: `Failed to read attachment '${attachment.id}': ${cause instanceof Error ? cause.message : String(cause)}.`,
                  cause,
                }),
            ),
          );
          void threadId; // reserved for future per-thread attachment scoping
          return {
            type: "image" as const,
            data: Buffer.from(bytes).toString("base64"),
            mimeType: attachment.mimeType,
          };
        });

      const writeNativeEventBestEffort = (threadId: ThreadId, payload: unknown) => {
        if (!nativeEventLogger) return;
        const observedAt = nowIso();
        void nativeEventLogger
          .write(
            {
              observedAt,
              event: {
                id: randomUUID(),
                kind: "notification",
                provider: PROVIDER,
                createdAt: observedAt,
                method: "pi.cli.stdout",
                threadId,
                payload,
              },
            },
            threadId,
          )
          .pipe(Effect.runPromiseWith(services))
          .catch(() => undefined);
      };

      const emitUsageFromMessage = (
        ctx: PiSessionContext,
        turn: PiTurnContext,
        message: unknown,
      ) => {
        if (!isRecord(message)) return;
        const snapshot = normalizePiTokenUsage(message.usage, turn.maxTokens);
        if (!snapshot) return;
        ctx.lastKnownTokenUsage = snapshot;
        void emitPromise({
          ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
          type: "thread.token-usage.updated",
          payload: { usage: snapshot },
        }).catch(() => undefined);
      };

      const requireSession = (threadId: ThreadId) => {
        const ctx = sessions.get(threadId);
        if (!ctx) {
          return Effect.fail(
            new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
          );
        }
        if (ctx.stopped) {
          return Effect.fail(
            new ProviderAdapterSessionClosedError({ provider: PROVIDER, threadId }),
          );
        }
        return Effect.succeed(ctx);
      };

      const handlePiEvent = (
        ctx: PiSessionContext,
        turn: PiTurnContext | undefined,
        raw: unknown,
      ) => {
        if (!isRecord(raw)) return;
        writeNativeEventBestEffort(ctx.threadId, raw);

        const kind = typeof raw.type === "string" ? raw.type : undefined;
        switch (kind) {
          case "session": {
            const sessionUuid = asTrimmedString(raw.id);
            if (sessionUuid && !ctx.piSessionUuid) {
              ctx.piSessionUuid = sessionUuid;
            }
            return;
          }
          case "agent_start":
          case "turn_start":
          case "agent_end":
          case "plan_mode":
          case "attachment":
            return;
        }

        // Every other event is turn-scoped. If pi emits one when no turn is
        // active (e.g. during a racing abort, or from an extension after
        // turn_end), drop it quietly rather than crash.
        if (!turn) return;
        switch (kind) {

          case "plan":
          case "todos": {
            const planSource = Array.isArray(raw.plan)
              ? raw.plan
              : Array.isArray(raw.todos)
                ? raw.todos
                : undefined;
            if (!planSource) return;

            const plan = planSource.flatMap((entry) => {
              if (!isRecord(entry)) return [];
              const step =
                asTrimmedString(entry.step) ??
                asTrimmedString(entry.content) ??
                asTrimmedString(entry.title);
              if (!step) return [];
              const status =
                entry.status === "completed"
                  ? "completed"
                  : entry.status === "inProgress" || entry.status === "in_progress"
                    ? "inProgress"
                    : "pending";
              return [{ step, status }] as const;
            });
            if (plan.length === 0) return;

            void emitPromise({
              ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
              type: "turn.plan.updated",
              payload: {
                ...(asTrimmedString(raw.explanation)
                  ? { explanation: asTrimmedString(raw.explanation) }
                  : {}),
                plan,
              },
            }).catch(() => undefined);
            return;
          }

          case "message_start":
          case "message_end": {
            const role = extractMessageRole(raw.message);
            if (role !== "assistant") return;

            const text = extractAssistantText(raw.message);
            if (kind === "message_end" && text.length > 0 && !turn.assistantTextSeen) {
              turn.assistantTextSeen = true;
              void emitPromise({
                ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
                type: "content.delta",
                payload: {
                  streamKind: "assistant_text",
                  delta: text,
                },
              }).catch(() => undefined);
            }
            if (kind === "message_end") {
              emitUsageFromMessage(ctx, turn, raw.message);
            }
            return;
          }

          case "message_update": {
            const role = extractMessageRole(raw.message);
            if (role !== "assistant") return;

            const assistantMessageEvent = isRecord(raw.assistantMessageEvent)
              ? raw.assistantMessageEvent
              : undefined;
            const deltaType = asString(assistantMessageEvent?.type);
            if (deltaType === "text_delta") {
              const delta = asString(assistantMessageEvent?.delta);
              if (!delta || delta.length === 0) return;
              turn.assistantTextSeen = true;
              void emitPromise({
                ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
                type: "content.delta",
                payload: {
                  streamKind: "assistant_text",
                  delta,
                },
              }).catch(() => undefined);
              return;
            }
            if (deltaType === "thinking_delta") {
              const delta = asString(assistantMessageEvent?.delta);
              if (!delta || delta.length === 0) return;
              void emitPromise({
                ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
                type: "content.delta",
                payload: {
                  streamKind: "reasoning_text",
                  delta,
                },
              }).catch(() => undefined);
            }
            return;
          }

          case "tool_execution_start":
          case "tool_call": {
            const toolCallId = extractToolCallId(raw) ?? `pi-tool-${randomUUID()}`;
            const toolName = extractToolName(raw) ?? "Tool";
            const itemType = classifyPiToolItemType(toolName);
            const title = titleForPiTool(itemType);
            const summary = summarizeToolArgs(toolName, raw.args);
            const toolState: PiToolState = {
              itemId: `pi-item-${toolCallId}`,
              toolCallId,
              toolName,
              itemType,
              title,
              summary,
            };
            turn.toolItems.set(toolCallId, toolState);

            void emitPromise({
              ...buildEventBase({
                threadId: ctx.threadId,
                turnId: turn.turnId,
                itemId: toolState.itemId,
              }),
              type: "item.started",
              payload: {
                itemType,
                status: "inProgress",
                title,
                detail: summary,
                data: {
                  toolCallId,
                  toolName,
                  args: raw.args,
                },
              },
            }).catch(() => undefined);
            return;
          }

          case "tool_execution_update": {
            const toolCallId = extractToolCallId(raw);
            if (!toolCallId) return;
            const toolState = turn.toolItems.get(toolCallId);
            const toolName = toolState?.toolName ?? extractToolName(raw) ?? "Tool";
            const itemType = toolState?.itemType ?? classifyPiToolItemType(toolName);
            const title = toolState?.title ?? titleForPiTool(itemType);
            const detail =
              summarizeToolResult(raw.partialResult, false) ??
              toolState?.summary ??
              summarizeToolArgs(toolName, raw.args);

            void emitPromise({
              ...buildEventBase({
                threadId: ctx.threadId,
                turnId: turn.turnId,
                itemId: toolState?.itemId ?? `pi-item-${toolCallId}`,
              }),
              type: "item.updated",
              payload: {
                itemType,
                status: "inProgress",
                title,
                detail,
                data: {
                  toolCallId,
                  toolName,
                  args: raw.args,
                  partialResult: raw.partialResult,
                },
              },
            }).catch(() => undefined);
            return;
          }

          case "tool_execution_end":
          case "tool_result": {
            const toolCallId = extractToolCallId(raw);
            const toolState = toolCallId ? turn.toolItems.get(toolCallId) : undefined;
            const toolName = toolState?.toolName ?? extractToolName(raw) ?? "Tool";
            const itemType = toolState?.itemType ?? classifyPiToolItemType(toolName);
            const title = toolState?.title ?? titleForPiTool(itemType);
            const isError = raw.isError === true;
            const detail =
              summarizeToolResult(raw.result, isError) ??
              summarizeToolResult(raw.partialResult, isError) ??
              toolState?.summary;

            void emitPromise({
              ...buildEventBase({
                threadId: ctx.threadId,
                turnId: turn.turnId,
                itemId:
                  toolState?.itemId ??
                  (toolCallId ? `pi-item-${toolCallId}` : `pi-item-${randomUUID()}`),
              }),
              type: "item.completed",
              payload: {
                itemType,
                status: isError ? "failed" : "completed",
                title,
                ...(detail ? { detail } : {}),
                data: {
                  ...(toolCallId ? { toolCallId } : {}),
                  toolName,
                  args: raw.args,
                  result: raw.result,
                  isError,
                },
              },
            }).catch(() => undefined);

            if (toolCallId) {
              turn.toolItems.delete(toolCallId);
            }
            return;
          }

          case "turn_end": {
            const stopReason = extractStopReason(raw.message);
            if (isToolUseStopReason(stopReason)) {
              return;
            }

            const text = extractAssistantText(raw.message);
            if (text.length > 0 && !turn.assistantTextSeen) {
              turn.assistantTextSeen = true;
              void emitPromise({
                ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
                type: "content.delta",
                payload: {
                  streamKind: "assistant_text",
                  delta: text,
                },
              }).catch(() => undefined);
            }

            emitUsageFromMessage(ctx, turn, raw.message);

            if (turn.settled) return;
            turn.settled = true;

            const errorMessage = extractErrorMessage(raw);
            const isError =
              errorMessage !== undefined ||
              stopReason === "error" ||
              stopReason === "failed" ||
              stopReason === "failure";

            if (isError) {
              const detail = errorMessage ?? stopReason ?? "pi reported a failure.";
              void emitPromise({
                ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
                type: "turn.completed",
                payload: {
                  state: "failed",
                  ...(stopReason ? { stopReason } : {}),
                  errorMessage: detail,
                },
              }).catch(() => undefined);
            } else {
              void emitPromise({
                ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
                type: "turn.completed",
                payload: {
                  state: "completed",
                  ...(stopReason ? { stopReason } : {}),
                },
              }).catch(() => undefined);
            }
            clearActiveTurn(ctx, turn);
            return;
          }

          case "error":
          case "agent_error":
          case "extension_error": {
            if (turn.settled) return;
            turn.settled = true;
            const detail = extractErrorMessage(raw) ?? "pi emitted an error event.";
            void emitPromise({
              ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
              type: "turn.completed",
              payload: {
                state: "failed",
                errorMessage: detail,
              },
            }).catch(() => undefined);
            clearActiveTurn(ctx, turn);
            return;
          }

          default:
            // Unknown pi event types are ignored cleanly per the MVP scope.
            return;
        }
      };

      const attachTurnListeners = (
        ctx: PiSessionContext,
        turn: PiTurnContext,
        stderrBuf: { value: string },
      ) => {
        const child = turn.child;
        if (!child) return; // RPC mode uses session-level listeners instead.
        let stdoutBuffer = "";

        const consumeLines = (flush: boolean): void => {
          while (true) {
            const newlineIndex = stdoutBuffer.indexOf("\n");
            if (newlineIndex === -1) {
              if (flush && stdoutBuffer.trim().length > 0) {
                const line = stdoutBuffer.trim();
                stdoutBuffer = "";
                try {
                  handlePiEvent(ctx, turn, JSON.parse(line));
                } catch {
                  // Non-JSON trailing line — ignore.
                }
              }
              return;
            }
            const rawLine = stdoutBuffer.slice(0, newlineIndex);
            stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1);
            const trimmed = rawLine.trim();
            if (trimmed.length === 0) continue;
            try {
              handlePiEvent(ctx, turn, JSON.parse(trimmed));
            } catch {
              // Non-JSON noise from pi (e.g. stderr crossovers) — skip.
            }
          }
        };

        child.stdout?.setEncoding("utf8");
        child.stderr?.setEncoding("utf8");
        child.stdout?.on("data", (chunk: string) => {
          stdoutBuffer += chunk;
          consumeLines(false);
        });
        child.stderr?.on("data", (chunk: string) => {
          stderrBuf.value += chunk;
        });

        child.once("error", (err) => {
          if (turn.settled) return;
          turn.settled = true;
          void emitPromise({
            ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
            type: "turn.completed",
            payload: {
              state: "failed",
              errorMessage: err.message.trim() || "pi subprocess errored.",
            },
          }).catch(() => undefined);
          clearActiveTurn(ctx, turn);
        });

        child.once("exit", (code, signal) => {
          consumeLines(true);
          if (turn.settled) {
            clearActiveTurn(ctx, turn);
            return;
          }
          turn.settled = true;
          // pi may have exited without a turn_end event (e.g. crash, SIGKILL).
          if (signal === "SIGKILL" || signal === "SIGTERM") {
            void emitPromise({
              ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
              type: "turn.completed",
              payload: {
                state: "interrupted",
                stopReason: "interrupted",
              },
            }).catch(() => undefined);
          } else {
            const detail =
              stderrBuf.value.trim().length > 0
                ? stderrBuf.value.trim().slice(0, 2000)
                : `pi exited with code ${code ?? "unknown"} and no turn_end event.`;
            void emitPromise({
              ...buildEventBase({ threadId: ctx.threadId, turnId: turn.turnId }),
              type: "turn.completed",
              payload: {
                state: "failed",
                errorMessage: detail,
              },
            }).catch(() => undefined);
          }
          clearActiveTurn(ctx, turn);
        });
      };

      /**
       * Clear `ctx.activeTurn` and snap the session back to its idle shape.
       * Call-sites used to only reset `ctx.activeTurn`, which left
       * `session.status` stuck on `"running"` and `activeTurnId` dangling —
       * so `listSessions()` reported every thread as permanently busy after
       * the first turn. Pass `turnBeingCleared` when multiple turns could
       * be racing the pointer (e.g. the RPC session-exit path vs a `turn_end`
       * notification); we only reset if that turn is still the active one.
       */
      const clearActiveTurn = (ctx: PiSessionContext, turnBeingCleared?: PiTurnContext) => {
        if (turnBeingCleared && ctx.activeTurn !== turnBeingCleared) return;
        ctx.activeTurn = undefined;
        const { activeTurnId: _ignored, ...rest } = ctx.session;
        ctx.session = {
          ...rest,
          status: "ready",
          updatedAt: nowIso(),
        };
      };

      /**
       * Wire a newly-spawned RPC child to the session's event pipeline. On
       * notifications we dispatch through the canonical `handlePiEvent`;
       * on unexpected exit we emit `session.exited` and tear down any
       * in-flight turn as interrupted.
       */
      const attachSessionListeners = (ctx: PiSessionContext) => {
        const rpc = ctx.rpc;
        if (!rpc) return;

        rpc.onNotification((raw) => {
          handlePiEvent(ctx, ctx.activeTurn, raw);
        });

        rpc.onExit((code, signal) => {
          if (ctx.stopped) return; // Expected path: stopSession already tore down.

          const interrupted = signal === "SIGKILL" || signal === "SIGTERM";
          if (ctx.activeTurn && !ctx.activeTurn.settled) {
            ctx.activeTurn.settled = true;
            const turnId = ctx.activeTurn.turnId;
            void emitPromise({
              ...buildEventBase({ threadId: ctx.threadId, turnId }),
              type: "turn.completed",
              payload: interrupted
                ? { state: "interrupted", stopReason: "interrupted" }
                : {
                    state: "failed",
                    errorMessage: `pi rpc exited with code ${code ?? "unknown"} during turn.`,
                  },
            }).catch(() => undefined);
            clearActiveTurn(ctx);
          }

          ctx.stopped = true;
          void emitPromise({
            ...buildEventBase({ threadId: ctx.threadId }),
            type: "session.exited",
            payload: {
              reason: interrupted
                ? "pi rpc child killed"
                : `pi rpc child exited with code ${code ?? "unknown"}`,
              recoverable: false,
              exitKind: interrupted ? "graceful" : "error",
            },
          }).catch(() => undefined);
          sessions.delete(ctx.threadId);
        });
      };

      const startSession: PiAdapterShape["startSession"] = (input) =>
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }

          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            // Kill any running turn from the previous incarnation.
            if (existing.activeTurn) {
              killTurn(existing.activeTurn);
            }
            if (existing.rpc) {
              try {
                existing.rpc.dispose("replacing pi session");
              } catch {
                // ignore
              }
            }
            if (existing.rpcChild && !existing.rpcChild.killed) {
              try {
                existing.rpcChild.kill("SIGTERM");
              } catch {
                // ignore
              }
            }
            existing.stopped = true;
            sessions.delete(input.threadId);
          }

          const createdAt = nowIso();
          const modelSelection =
            input.modelSelection?.provider === PROVIDER ? input.modelSelection : undefined;

          const piSettings = yield* serverSettings.getSettings.pipe(
            Effect.map((s) => s.providers.pi),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          const binaryPath =
            piSettings.binaryPath.trim().length > 0 ? piSettings.binaryPath : "pi";
          const transport: PiTransport = piSettings.transport;
          const rawModelForSession =
            modelSelection?.model ?? (piSettings.defaultModel || DEFAULT_MODEL);
          const normalizedModel = normalizePiModelSlug(rawModelForSession);

          const session: ProviderSession = {
            provider: PROVIDER,
            status: "ready",
            runtimeMode: input.runtimeMode,
            ...(input.cwd ? { cwd: input.cwd } : {}),
            model: normalizedModel,
            threadId: input.threadId,
            createdAt,
            updatedAt: createdAt,
          };

          const ctx: PiSessionContext = {
            threadId: input.threadId,
            session,
            activeTurn: undefined,
            stopped: false,
            transport,
            piSessionUuid: undefined,
            lastKnownTokenUsage: undefined,
            rpc: undefined,
            rpcChild: undefined,
            rpcActiveModel: undefined,
            rpcMaxTokens: undefined,
          };

          if (transport === "rpc") {
            const capacityMap = yield* Effect.promise(() =>
              resolveModelContextWindow(binaryPath),
            );
            const { provider: rpcProvider, model: rpcModel } =
              splitPiModelSlug(normalizedModel);
            const args: string[] = ["--mode", "rpc"];
            if (rpcProvider) args.push("--provider", rpcProvider);
            args.push("--model", rpcModel);

            const rpcChild = yield* Effect.try({
              try: () =>
                spawn(binaryPath, args, {
                  stdio: ["pipe", "pipe", "pipe"],
                  shell: process.platform === "win32",
                  env: { ...process.env, PI_CACHE_RETENTION: "long" },
                  ...(input.cwd ? { cwd: input.cwd } : {}),
                }),
              catch: (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause instanceof Error ? cause.message : "Failed to spawn pi rpc.",
                  cause,
                }),
            });

            ctx.rpcChild = rpcChild;
            ctx.rpc = createPiRpcClient(rpcChild);
            ctx.rpcActiveModel = normalizedModel;
            ctx.rpcMaxTokens = capacityMap.get(rpcModel) ?? capacityMap.get(normalizedModel);
            attachSessionListeners(ctx);
          }

          sessions.set(input.threadId, ctx);

          yield* emit({
            ...buildEventBase({ threadId: input.threadId }),
            type: "session.started",
            payload: {
              message: "pi session started",
            },
          });
          yield* emit({
            ...buildEventBase({ threadId: input.threadId }),
            type: "session.state.changed",
            payload: { state: "ready", reason: "pi session ready" },
          });
          yield* emit({
            ...buildEventBase({ threadId: input.threadId }),
            type: "thread.started",
            payload: {},
          });

          return session;
        });

      const sendTurn: PiAdapterShape["sendTurn"] = (input) =>
        Effect.gen(function* () {
          const ctx = yield* requireSession(input.threadId);

          const prompt = input.input?.trim();
          if (!prompt || prompt.length === 0) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "pi turns require non-empty text input.",
            });
          }

          // One turn at a time per thread. In RPC mode a second `prompt`
          // frame while one is in flight would interleave notifications
          // and break turn attribution; in JSON mode it would spawn a
          // second subprocess. Phase 2.3 will supersede this with
          // automatic routing to `steer` / `follow_up` queues.
          if (ctx.activeTurn) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "sendTurn",
              detail: "A pi turn is already in flight on this thread; wait for it to settle or interrupt.",
            });
          }

          // Attachments are only supported on the RPC transport — JSON mode's
          // per-turn CLI invocation has no path to stream image bytes. In RPC
          // mode we resolve them up front so a bad attachment blocks the
          // spawn rather than reaching pi as a half-formed prompt.
          const attachments = input.attachments ?? [];
          if (attachments.length > 0 && ctx.transport !== "rpc") {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue:
                "pi attachments require the rpc transport; set providers.pi.transport to 'rpc'.",
            });
          }
          const piImages =
            ctx.transport === "rpc" && attachments.length > 0
              ? yield* Effect.forEach(
                  attachments,
                  (attachment) => resolvePiImageAttachment(input.threadId, attachment),
                  { concurrency: 1 },
                )
              : [];

          const modelSelection =
            input.modelSelection?.provider === PROVIDER ? input.modelSelection : undefined;
          const rawModel = modelSelection?.model ?? ctx.session.model ?? DEFAULT_MODEL;
          // pi's CLI expects bare slugs (e.g. `gpt-5.4`), not `openai/gpt-5`
          // style. Strip any `provider/` prefix for backwards compatibility
          // with older composer state.
          const model = normalizePiModelSlug(rawModel);

          const piSettings = yield* serverSettings.getSettings.pipe(
            Effect.map((s) => s.providers.pi),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          const binaryPath = piSettings.binaryPath.trim().length > 0 ? piSettings.binaryPath : "pi";

          const turnId = TurnId.make(`pi-turn-${randomUUID()}`);

          // ────────── RPC transport ──────────
          if (ctx.transport === "rpc") {
            const rpc = ctx.rpc;
            if (!rpc || !ctx.rpcChild || ctx.rpcChild.exitCode !== null) {
              return yield* new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: "pi rpc child is not running; restart the session.",
              });
            }

            // Refresh max-tokens against the catalog (cheap after first
            // lookup) and compute the target provider/model tuple.
            const capacityMap = yield* Effect.promise(() =>
              resolveModelContextWindow(binaryPath),
            );
            const { provider: rpcProvider, model: rpcModel } = splitPiModelSlug(model);
            const maxTokens = capacityMap.get(rpcModel) ?? capacityMap.get(model);

            // Switch model in pi if the requested one differs from the
            // currently-active one. pi's `set_model` RPC requires both a
            // provider backend and a model id — so only backend-qualified
            // slugs (`{backend}/{model}`) can drive a mid-session switch.
            // Bare slugs are ambiguous across pi backends and we refuse to
            // guess: we emit a warning and keep running on the prior model
            // rather than silently claim a switch we didn't perform.
            let effectiveModel = ctx.rpcActiveModel ?? model;
            let effectiveMaxTokens = ctx.rpcMaxTokens ?? maxTokens;
            if (ctx.rpcActiveModel !== model) {
              if (rpcProvider) {
                const switchResult = yield* Effect.promise(() =>
                  rpc.call("set_model", { provider: rpcProvider, modelId: rpcModel }),
                );
                if (switchResult.success) {
                  ctx.rpcActiveModel = model;
                  ctx.rpcMaxTokens = maxTokens;
                  effectiveModel = model;
                  effectiveMaxTokens = maxTokens;
                } else {
                  // Not fatal — pi keeps its current model. Warn so the
                  // UI can reflect the discrepancy.
                  yield* emit({
                    ...buildEventBase({ threadId: input.threadId }),
                    type: "runtime.warning",
                    payload: {
                      message: `pi set_model failed; staying on '${ctx.rpcActiveModel ?? "unknown"}': ${switchResult.error}`,
                    },
                  });
                }
              } else {
                yield* emit({
                  ...buildEventBase({ threadId: input.threadId }),
                  type: "runtime.warning",
                  payload: {
                    message: `pi cannot switch model to bare slug '${model}'; use '{backend}/{model}' for mid-session switching. Staying on '${ctx.rpcActiveModel ?? "unknown"}'.`,
                  },
                });
              }
            }

            const turn: PiTurnContext = {
              turnId,
              child: undefined,
              settled: false,
              assistantTextSeen: false,
              toolItems: new Map(),
              maxTokens: effectiveMaxTokens,
            };
            ctx.activeTurn = turn;
            ctx.session = {
              ...ctx.session,
              status: "running",
              activeTurnId: turnId,
              model: effectiveModel,
              updatedAt: nowIso(),
            };

            yield* emit({
              ...buildEventBase({ threadId: input.threadId, turnId }),
              type: "turn.started",
              payload: { model },
            });

            // Fire-and-forget: we don't await the prompt response because
            // it only acknowledges receipt. Turn completion flows through
            // the notification pump (→ `turn_end`).
            const promptResult = yield* Effect.promise(() =>
              rpc.call("prompt", {
                message: prompt,
                ...(piImages.length > 0 ? { images: piImages } : {}),
              }),
            );
            if (!promptResult.success) {
              if (!turn.settled) {
                turn.settled = true;
                yield* emit({
                  ...buildEventBase({ threadId: input.threadId, turnId }),
                  type: "turn.completed",
                  payload: {
                    state: "failed",
                    errorMessage: `pi prompt failed: ${promptResult.error}`,
                  },
                });
              }
              clearActiveTurn(ctx, turn);
            }

            return { threadId: input.threadId, turnId };
          }

          // ────────── JSON transport (legacy fallback) ──────────
          const resumeArgs = ctx.piSessionUuid ? ["--session", ctx.piSessionUuid] : [];
          const args = [...resumeArgs, "-p", "--mode", "json", "--model", model, prompt];

          const capacityMap = yield* Effect.promise(() => resolveModelContextWindow(binaryPath));
          const maxTokens = capacityMap.get(model);

          const child: ChildProcess = yield* Effect.try({
            try: () =>
              spawn(binaryPath, args, {
                stdio: ["ignore", "pipe", "pipe"],
                shell: process.platform === "win32",
                env: { ...process.env, PI_CACHE_RETENTION: "long" },
                ...(ctx.session.cwd ? { cwd: ctx.session.cwd } : {}),
              }),
            catch: (cause) =>
              new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: cause instanceof Error ? cause.message : "Failed to spawn pi.",
                cause,
              }),
          });

          const turn: PiTurnContext = {
            turnId,
            child,
            settled: false,
            assistantTextSeen: false,
            toolItems: new Map(),
            maxTokens,
          };
          ctx.activeTurn = turn;
          ctx.session = {
            ...ctx.session,
            status: "running",
            activeTurnId: turnId,
            model,
            updatedAt: nowIso(),
          };

          yield* emit({
            ...buildEventBase({ threadId: input.threadId, turnId }),
            type: "turn.started",
            payload: { model },
          });

          attachTurnListeners(ctx, turn, { value: "" });

          return {
            threadId: input.threadId,
            turnId,
          };
        });

      const interruptTurn: PiAdapterShape["interruptTurn"] = (threadId, turnId) =>
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          const active = ctx.activeTurn;
          if (!active) return;
          if (turnId !== undefined && active.turnId !== turnId) return;

          if (ctx.transport === "rpc" && ctx.rpc) {
            // Graceful abort: pi responds with a turn_end whose stopReason
            // reflects the cancellation, which our notification pump maps
            // to `turn.completed{state:"interrupted"}`. If the abort RPC
            // fails (e.g. pi hung) we fall back to SIGKILL on the rpc child.
            const abortResult = yield* Effect.promise(() => ctx.rpc!.call("abort"));
            if (!abortResult.success) {
              if (ctx.rpcChild && !ctx.rpcChild.killed) {
                try {
                  ctx.rpcChild.kill("SIGKILL");
                } catch {
                  // ignore
                }
              }
            }
            return;
          }

          // JSON transport: kill the per-turn child; exit handler emits
          // turn.completed{state:"interrupted"}.
          killTurn(active);
        });

      const respondToRequest: PiAdapterShape["respondToRequest"] = () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToRequest",
            detail: "pi does not yet support interactive approvals",
          }),
        );

      const respondToUserInput: PiAdapterShape["respondToUserInput"] = () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToUserInput",
            detail: "pi does not yet support structured user input",
          }),
        );

      const stopSession: PiAdapterShape["stopSession"] = (threadId) =>
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (!ctx) {
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId,
            });
          }
          if (ctx.stopped) return;
          ctx.stopped = true;
          if (ctx.activeTurn) {
            killTurn(ctx.activeTurn);
            clearActiveTurn(ctx);
          }
          if (ctx.rpc) {
            try {
              ctx.rpc.dispose("session stopped");
            } catch {
              // ignore
            }
          }
          if (ctx.rpcChild && !ctx.rpcChild.killed) {
            try {
              ctx.rpcChild.kill("SIGTERM");
            } catch {
              // ignore
            }
          }
          sessions.delete(threadId);
          yield* emit({
            ...buildEventBase({ threadId }),
            type: "session.exited",
            payload: {
              reason: "Session stopped.",
              recoverable: false,
              exitKind: "graceful",
            },
          });
        });

      const listSessions: PiAdapterShape["listSessions"] = () =>
        Effect.sync(() =>
          [...sessions.values()].filter((c) => !c.stopped).map((c) => Object.assign({}, c.session)),
        );

      const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
        Effect.sync(() => {
          const c = sessions.get(threadId);
          return c !== undefined && !c.stopped;
        });

      const readThread: PiAdapterShape["readThread"] = (threadId) =>
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (!ctx || ctx.stopped || ctx.transport !== "rpc" || !ctx.rpc) {
            return { threadId, turns: [] };
          }

          const result = yield* Effect.promise(() =>
            ctx.rpc!.call<{ messages: ReadonlyArray<unknown> }>("get_messages"),
          );
          if (!result.success) {
            return { threadId, turns: [] };
          }
          const messages = Array.isArray(result.data?.messages) ? result.data.messages : [];
          return { threadId, turns: mapPiMessagesToTurns(messages) };
        });

      const rollbackThread: PiAdapterShape["rollbackThread"] = () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "rollbackThread",
            detail: "pi does not yet support rollback",
          }),
        );

      const stopAll: PiAdapterShape["stopAll"] = () =>
        Effect.gen(function* () {
          const threadIds = [...sessions.keys()];
          for (const threadId of threadIds) {
            yield* stopSession(threadId).pipe(Effect.ignore);
          }
        });

      const streamEvents = Stream.fromPubSub(runtimeEvents);

      return {
        provider: PROVIDER,
        capabilities: { sessionModelSwitch: "unsupported" },
        startSession,
        sendTurn,
        interruptTurn,
        respondToRequest,
        respondToUserInput,
        stopSession,
        listSessions,
        hasSession,
        readThread,
        rollbackThread,
        stopAll,
        streamEvents,
      } satisfies PiAdapterShape;
    }),
  );
}

export const PiAdapterLive = makePiAdapterLive();
