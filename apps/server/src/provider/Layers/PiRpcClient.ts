/**
 * PiRpcClient — minimal JSONL transport for `pi --mode rpc`.
 *
 * pi's RPC protocol is LF-delimited JSONL over stdio (see pi-mono
 * `packages/coding-agent/docs/rpc.md`). Commands sent to stdin carry a
 * `type` and optional `id`; responses echo the `id` and add
 * `type: "response"`. Everything else — `turn_end`, `message_update`,
 * `tool_execution_*`, `queue_update`, `extension_ui_request`, etc — is a
 * notification with no correlation id.
 *
 * This module only handles framing and id correlation. The adapter owns
 * command choreography and mapping notifications to canonical events.
 *
 * @module PiRpcClient
 */
import { randomUUID } from "node:crypto";
import type { ChildProcess } from "node:child_process";

export type RpcResponse<T = unknown> =
  | { readonly success: true; readonly data?: T }
  | { readonly success: false; readonly error: string };

export interface PiRpcClientShape {
  /**
   * Send a command and resolve with pi's response. Commands that pi sends
   * stream notifications for (like `prompt`) still resolve their RPC response
   * immediately to acknowledge receipt — callers that need to track turn
   * completion should subscribe to notifications for `turn_end`.
   */
  call<T = unknown>(type: string, params?: Record<string, unknown>): Promise<RpcResponse<T>>;

  /**
   * Send a frame with no response expected. Used for `extension_ui_response`
   * replies, which do not round-trip as RPC responses.
   */
  send(frame: Record<string, unknown>): void;

  /** Subscribe to notifications. Returns an unsubscribe function. */
  onNotification(handler: (raw: unknown) => void): () => void;

  /** Subscribe to process-level exit. Returns an unsubscribe function. */
  onExit(handler: (code: number | null, signal: NodeJS.Signals | null) => void): () => void;

  /**
   * Reject all pending calls with the given reason and stop listening. The
   * caller still owns the child — close() does not kill it.
   */
  dispose(reason?: string): void;
}

interface PendingCall {
  readonly resolve: (response: RpcResponse) => void;
  readonly reject: (error: Error) => void;
  readonly type: string;
}

export function createPiRpcClient(child: ChildProcess): PiRpcClientShape {
  if (!child.stdin || !child.stdout) {
    throw new Error("PiRpcClient requires a child process with piped stdin and stdout.");
  }

  child.stdout.setEncoding("utf8");
  // stderr is left to the caller — pi writes framing-relevant content only
  // to stdout, but auto-retry spinners and diagnostic output can land on
  // stderr. We don't parse it here.

  const pending = new Map<string, PendingCall>();
  const notificationHandlers = new Set<(raw: unknown) => void>();
  const exitHandlers = new Set<
    (code: number | null, signal: NodeJS.Signals | null) => void
  >();

  let buffer = "";
  let disposed = false;

  const emitNotification = (raw: unknown) => {
    for (const handler of notificationHandlers) {
      try {
        handler(raw);
      } catch {
        // Best-effort — one bad handler must not break the stream.
      }
    }
  };

  const handleLine = (line: string) => {
    const trimmed = line.endsWith("\r") ? line.slice(0, -1) : line;
    if (trimmed.length === 0) return;

    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      // pi sometimes prints non-JSON diagnostic lines during startup (e.g.
      // warnings about deprecated config). Drop them silently; a strict
      // parser would spam the UI.
      return;
    }

    if (
      parsed !== null &&
      typeof parsed === "object" &&
      (parsed as { type?: unknown }).type === "response"
    ) {
      const envelope = parsed as {
        readonly type: "response";
        readonly command?: string;
        readonly id?: string;
        readonly success?: boolean;
        readonly data?: unknown;
        readonly error?: unknown;
      };
      const id = typeof envelope.id === "string" ? envelope.id : undefined;
      if (!id) {
        // Orphan response with no id — nothing to correlate. Surface it as
        // a notification so callers can at least log it.
        emitNotification(parsed);
        return;
      }
      const call = pending.get(id);
      if (!call) {
        // Response arrived after we timed out or gave up; ignore.
        return;
      }
      pending.delete(id);
      if (envelope.success === true) {
        call.resolve({ success: true, data: envelope.data });
      } else {
        const errorText =
          typeof envelope.error === "string" && envelope.error.length > 0
            ? envelope.error
            : `pi ${call.type} failed.`;
        call.resolve({ success: false, error: errorText });
      }
      return;
    }

    emitNotification(parsed);
  };

  const onData = (chunk: string) => {
    buffer += chunk;
    let newlineIndex = buffer.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = buffer.slice(0, newlineIndex);
      buffer = buffer.slice(newlineIndex + 1);
      handleLine(line);
      newlineIndex = buffer.indexOf("\n");
    }
  };

  const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
    // Flush any trailing partial line (no LF terminator).
    if (buffer.length > 0) {
      handleLine(buffer);
      buffer = "";
    }
    for (const handler of exitHandlers) {
      try {
        handler(code, signal);
      } catch {
        // Ignore.
      }
    }
    const reason = `pi rpc child exited${code !== null ? ` with code ${code}` : ""}${signal ? ` via ${signal}` : ""}.`;
    rejectAllPending(reason);
  };

  const rejectAllPending = (reason: string) => {
    for (const [id, call] of pending) {
      pending.delete(id);
      call.reject(new Error(reason));
    }
  };

  child.stdout.on("data", onData);
  child.once("exit", onExit);

  const write = (frame: Record<string, unknown>): void => {
    if (disposed) {
      throw new Error("PiRpcClient is disposed.");
    }
    if (!child.stdin || child.stdin.destroyed) {
      throw new Error("pi rpc stdin is not writable.");
    }
    child.stdin.write(`${JSON.stringify(frame)}\n`);
  };

  return {
    call: <T = unknown>(type: string, params: Record<string, unknown> = {}) =>
      new Promise<RpcResponse<T>>((resolve, reject) => {
        const id = randomUUID();
        pending.set(id, {
          resolve: resolve as (response: RpcResponse) => void,
          reject,
          type,
        });
        try {
          write({ ...params, type, id });
        } catch (error) {
          pending.delete(id);
          reject(error instanceof Error ? error : new Error(String(error)));
        }
      }),

    send: (frame) => {
      write(frame);
    },

    onNotification: (handler) => {
      notificationHandlers.add(handler);
      return () => notificationHandlers.delete(handler);
    },

    onExit: (handler) => {
      exitHandlers.add(handler);
      return () => exitHandlers.delete(handler);
    },

    dispose: (reason = "PiRpcClient disposed by caller.") => {
      if (disposed) return;
      disposed = true;
      rejectAllPending(reason);
      notificationHandlers.clear();
      exitHandlers.clear();
      child.stdout?.off("data", onData);
      child.off("exit", onExit);
    },
  };
}
