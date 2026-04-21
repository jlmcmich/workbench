/**
 * piSessionStore — per-thread persistence of pi's own session identity.
 *
 * Pi persists its transcript, tool state, and streaming flag to JSONL
 * files under `~/.pi/agent/sessions/`. When our server restarts, the
 * in-memory PiSessionContext that knew which file belongs to which
 * Workbench thread is gone. Without a pointer we have no way to tell a
 * fresh `pi --mode rpc` child "resume that session" — pi would open a
 * brand-new file and the user loses their cached prompt affinity and
 * running state.
 *
 * This module persists a tiny record per thread — just the pi session
 * file path (and an update timestamp for debugging). On the next
 * `startSession` the adapter reads it back and calls pi's
 * `switch_session` RPC. Failure at any step is non-fatal: if the stored
 * path is gone, pi rejects `switch_session`, and we fall back to a fresh
 * session.
 *
 * Files live at `${stateDir}/pi-sessions/{threadId}.json`. Format:
 *
 *   { "sessionFile": "/Users/.../~/.pi/agent/sessions/...jsonl",
 *     "sessionId": "abc123",
 *     "updatedAt": "2026-04-20T..." }
 *
 * @module piSessionStore
 */
import { join as pathJoin } from "node:path";

import type { ThreadId } from "@workbench/contracts";
import { FileSystem } from "effect";
import { Effect } from "effect";

export interface PiSessionRecord {
  readonly sessionFile: string;
  readonly sessionId?: string;
  readonly updatedAt: string;
}

/**
 * Normalise a ThreadId into something safe for use as a filesystem
 * filename. ThreadIds are UUID-like today but we defensively strip any
 * character that could escape the directory.
 */
function threadIdSegment(threadId: ThreadId): string {
  return String(threadId).replace(/[^a-zA-Z0-9_-]/g, "_");
}

function storePathFor(stateDir: string, threadId: ThreadId): string {
  return pathJoin(stateDir, "pi-sessions", `${threadIdSegment(threadId)}.json`);
}

function storeDirFor(stateDir: string): string {
  return pathJoin(stateDir, "pi-sessions");
}

export const loadPiSessionRecord = (input: {
  readonly stateDir: string;
  readonly threadId: ThreadId;
}): Effect.Effect<PiSessionRecord | undefined, never, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = storePathFor(input.stateDir, input.threadId);
    const exists = yield* fs.exists(path).pipe(Effect.orElseSucceed(() => false));
    if (!exists) return undefined;
    const text = yield* fs
      .readFileString(path)
      .pipe(Effect.orElseSucceed(() => undefined));
    if (text === undefined) return undefined;
    try {
      const parsed = JSON.parse(text) as Partial<PiSessionRecord>;
      if (typeof parsed?.sessionFile !== "string" || parsed.sessionFile.length === 0) {
        return undefined;
      }
      return {
        sessionFile: parsed.sessionFile,
        ...(typeof parsed.sessionId === "string" ? { sessionId: parsed.sessionId } : {}),
        updatedAt:
          typeof parsed.updatedAt === "string" ? parsed.updatedAt : new Date(0).toISOString(),
      };
    } catch {
      return undefined;
    }
  });

export const savePiSessionRecord = (input: {
  readonly stateDir: string;
  readonly threadId: ThreadId;
  readonly sessionFile: string;
  readonly sessionId?: string;
}): Effect.Effect<void, never, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const dir = storeDirFor(input.stateDir);
    yield* fs.makeDirectory(dir, { recursive: true }).pipe(Effect.orElseSucceed(() => undefined));
    const path = storePathFor(input.stateDir, input.threadId);
    const record: PiSessionRecord = {
      sessionFile: input.sessionFile,
      ...(input.sessionId ? { sessionId: input.sessionId } : {}),
      updatedAt: new Date().toISOString(),
    };
    yield* fs
      .writeFileString(path, JSON.stringify(record, null, 2))
      .pipe(Effect.orElseSucceed(() => undefined));
  });

export const clearPiSessionRecord = (input: {
  readonly stateDir: string;
  readonly threadId: ThreadId;
}): Effect.Effect<void, never, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = storePathFor(input.stateDir, input.threadId);
    yield* fs.remove(path).pipe(Effect.orElseSucceed(() => undefined));
  });
