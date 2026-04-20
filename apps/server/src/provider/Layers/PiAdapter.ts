/**
 * PiAdapterLive — pi coding agent (`pi -p --mode json`) per-turn subprocess.
 *
 * pi is spawned anew for each `sendTurn` call. stdout is parsed line-by-line
 * as NDJSON and mapped onto the canonical ProviderRuntimeEvent stream. The
 * first line of pi's JSON output is a session header carrying a UUID; we
 * capture it on the first turn and pass `--session <uuid>` on subsequent
 * turns so pi restores the prior transcript and tool-call state from
 * `~/.pi/agent/sessions/`. Extended prompt caching is enabled via
 * `PI_CACHE_RETENTION=long` on the child env. A single subprocess is tracked
 * per-thread so it can be SIGKILL'd on interrupt or session stop.
 *
 * Unsupported features (approvals, structured user-input, rollback) fail
 * fast with ProviderAdapterRequestError; `readThread` returns an empty
 * snapshot since pi owns the transcript on its side.
 *
 * @module PiAdapterLive
 */
import { type ChildProcess, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";

import {
  RuntimeItemId,
  EventId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ThreadId,
  type ThreadTokenUsageSnapshot,
  type ToolLifecycleItemType,
  TurnId,
} from "@workbench/contracts";
import { Effect, Layer, PubSub, Stream } from "effect";

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
  readonly child: ChildProcess;
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
  /**
   * pi's session UUID, captured from the first `{"type":"session","id":...}`
   * line of NDJSON output. Undefined until the first turn emits one; passed
   * back via `--session <uuid>` on every subsequent turn so pi resumes the
   * existing transcript and tool state.
   */
  piSessionUuid: string | undefined;
  /**
   * Last usage snapshot derived from `AssistantMessage.usage` on pi's
   * `message_end`/`turn_end` events, carried forward across turns so the
   * context-window UI reflects cumulative usage.
   */
  lastKnownTokenUsage: ThreadTokenUsageSnapshot | undefined;
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

function killTurn(turn: PiTurnContext, signal: NodeJS.Signals = "SIGKILL") {
  if (!turn.child.killed) {
    try {
      turn.child.kill(signal);
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

      const handlePiEvent = (ctx: PiSessionContext, turn: PiTurnContext, raw: unknown) => {
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
            if (ctx.activeTurn === turn) {
              ctx.activeTurn = undefined;
            }
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
            if (ctx.activeTurn === turn) {
              ctx.activeTurn = undefined;
            }
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
          if (ctx.activeTurn === turn) {
            ctx.activeTurn = undefined;
          }
        });

        child.once("exit", (code, signal) => {
          consumeLines(true);
          if (turn.settled) {
            if (ctx.activeTurn === turn) ctx.activeTurn = undefined;
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
          if (ctx.activeTurn === turn) {
            ctx.activeTurn = undefined;
          }
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
            existing.stopped = true;
            sessions.delete(input.threadId);
          }

          const createdAt = nowIso();
          const modelSelection =
            input.modelSelection?.provider === PROVIDER ? input.modelSelection : undefined;
          const session: ProviderSession = {
            provider: PROVIDER,
            status: "ready",
            runtimeMode: input.runtimeMode,
            ...(input.cwd ? { cwd: input.cwd } : {}),
            ...(modelSelection ? { model: modelSelection.model } : {}),
            threadId: input.threadId,
            createdAt,
            updatedAt: createdAt,
          };

          const ctx: PiSessionContext = {
            threadId: input.threadId,
            session,
            activeTurn: undefined,
            stopped: false,
            piSessionUuid: undefined,
            lastKnownTokenUsage: undefined,
          };
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
              issue: "pi turns require non-empty text input (attachments not supported in MVP).",
            });
          }

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
          killTurn(active);
          // Exit handler will emit turn.completed{state: "interrupted"}.
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
            ctx.activeTurn = undefined;
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
        Effect.sync(() => ({ threadId, turns: [] }));

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
