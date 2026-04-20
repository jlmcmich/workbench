/**
 * piRuntime - Helpers for probing and invoking the pi CLI.
 *
 * pi auth differs from other providers: there is no `pi login` subcommand.
 * pi reads provider-specific API keys or OAuth tokens from environment
 * variables. We surface those requirements through the provider snapshot
 * (install status, configured-backend, env-var readiness) rather than
 * silently inheriting another provider's credentials.
 *
 * @module piRuntime
 */
import { spawn } from "node:child_process";
import { readFileSync, watch as fsWatch, type FSWatcher } from "node:fs";
import { homedir, platform as osPlatform } from "node:os";
import { dirname as pathDirname, join as joinPath } from "node:path";
import type { ModelCapabilities, ServerProviderModel } from "@workbench/contracts";

export interface PiCommandResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly code: number;
}

export async function runPiCommand(input: {
  readonly binaryPath: string;
  readonly args: ReadonlyArray<string>;
}): Promise<PiCommandResult> {
  const child = spawn(input.binaryPath, [...input.args], {
    stdio: ["ignore", "pipe", "pipe"],
    shell: process.platform === "win32",
    env: process.env,
  });

  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");

  const stdoutChunks: Array<string> = [];
  const stderrChunks: Array<string> = [];

  child.stdout?.on("data", (chunk: string) => stdoutChunks.push(chunk));
  child.stderr?.on("data", (chunk: string) => stderrChunks.push(chunk));

  const code = await new Promise<number>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (exitCode) => resolve(exitCode ?? 0));
  });

  return {
    stdout: stdoutChunks.join(""),
    stderr: stderrChunks.join(""),
    code,
  };
}

export const DEFAULT_PI_MODEL_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  contextWindowOptions: [],
  promptInjectedEffortLevels: [],
};

/**
 * Minimum pi version we test against. Below this, pi may lack `--mode rpc`
 * stability required by the adapter (Slice 3) — warn but do not block.
 */
export const PI_MIN_RECOMMENDED_VERSION = "0.60.0";

/**
 * Fallback model slugs surfaced when pi is installed but `pi --list-models`
 * enumeration fails (CLI missing flag, pi auth not yet ready, etc.). These
 * use pi's real bare-slug format so `pi --model <slug>` accepts them.
 *
 * Prefer the dynamically-loaded catalog from `loadPiModelCatalog`; this list
 * is only a last-resort fallback.
 */
export const DEFAULT_PI_BUILTIN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "openai-codex/gpt-5.4",
    name: "openai-codex/gpt-5.4",
    isCustom: false,
    capabilities: DEFAULT_PI_MODEL_CAPABILITIES,
  },
  {
    slug: "anthropic/claude-sonnet-4-6",
    name: "anthropic/claude-sonnet-4-6",
    isCustom: false,
    capabilities: DEFAULT_PI_MODEL_CAPABILITIES,
  },
  {
    slug: "anthropic/claude-haiku-4-5",
    name: "anthropic/claude-haiku-4-5",
    isCustom: false,
    capabilities: DEFAULT_PI_MODEL_CAPABILITIES,
  },
];

/**
 * Default pi model when no other slug has been supplied. Use a
 * backend-qualified slug (`<backend>/<model>`) so pi routes to a specific
 * backend — bare slugs like `gpt-5.4` are ambiguous across pi backends
 * (e.g. `openai-codex/gpt-5.4` vs `azure-openai-responses/gpt-5.4`) and pi
 * picks whichever backend it finds first, which may be one the user isn't
 * authed for.
 */
export const DEFAULT_PI_MODEL_SLUG = "openai-codex/gpt-5.4";

/**
 * Normalise a pi model slug for the CLI. Pi's `--model` flag accepts both
 * bare model names and `<backend>/<model>` patterns. When a slug carries
 * a backend prefix we keep it verbatim — bare slugs are ambiguous across
 * backends (e.g. `gpt-5.4` exists under `openai-codex` AND
 * `azure-openai-responses`) and pi picks whichever backend it finds first,
 * which may be one the user isn't authed for.
 *
 * Returns the slug as-is after trimming; kept as a function so the adapter
 * has a single hook point if we ever need to reshape slugs further.
 */
export function normalizePiModelSlug(slug: string): string {
  return slug.trim();
}

export interface PiCatalogEntry {
  /** pi backend this model routes through (e.g. `anthropic`, `openai-codex`, `google`). */
  readonly backend: string;
  /** Bare model slug pi accepts via --model. */
  readonly model: string;
  /** Raw context-window label (e.g. "200K") as reported by pi. */
  readonly context: string;
  /** Raw max-output label (e.g. "64K") as reported by pi. */
  readonly maxOut: string;
  readonly supportsThinking: boolean;
  readonly supportsImages: boolean;
}

/**
 * Parse the fixed-column text table emitted by `pi --list-models`. The table
 * format is:
 *
 *   provider      model                       context  max-out  thinking  images
 *   anthropic     claude-3-5-sonnet-20240620  200K     8.2K     no        yes
 *   ...
 *
 * plus a trailing `(n/m)` pagination marker and occasional blank lines. We
 * skip the header and pagination; everything else with 6 whitespace-separated
 * tokens starting with a known-ish backend name becomes an entry.
 */
export function parsePiListModels(output: string): ReadonlyArray<PiCatalogEntry> {
  const entries: Array<PiCatalogEntry> = [];
  const lines = output.split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (line.length === 0) continue;
    if (line.startsWith("(")) continue; // `(12/32)` pagination marker
    if (line.startsWith("Only showing")) continue;
    const tokens = line.split(/\s+/);
    if (tokens.length < 6) continue;
    const [backend, model, context, maxOut, thinking, images] = tokens;
    if (!backend || !model || !context || !maxOut || !thinking || !images) continue;
    if (backend === "provider" && model === "model") continue; // header row
    if (thinking !== "yes" && thinking !== "no") continue;
    if (images !== "yes" && images !== "no") continue;
    entries.push({
      backend,
      model,
      context,
      maxOut,
      supportsThinking: thinking === "yes",
      supportsImages: images === "yes",
    });
  }
  return entries;
}

/**
 * Run `pi --list-models` and return the parsed catalog. On any failure
 * (missing pi, no configured keys, CLI error) returns an empty array so the
 * caller can fall back to DEFAULT_PI_BUILTIN_MODELS.
 */
export async function loadPiModelCatalog(input: {
  readonly binaryPath: string;
}): Promise<ReadonlyArray<PiCatalogEntry>> {
  try {
    const result = await runPiCommand({
      binaryPath: input.binaryPath,
      args: ["--list-models"],
    });
    // pi writes the table to stdout on success; keep stderr as a fallback.
    const source = result.stdout.trim().length > 0 ? result.stdout : result.stderr;
    return parsePiListModels(source);
  } catch {
    return [];
  }
}

/**
 * Convert pi's compact context-window label ("200K", "1M", "32k") into a
 * numeric token count. Returns undefined for unrecognised inputs so callers
 * can fall back gracefully.
 */
export function parsePiContextLabel(label: string): number | undefined {
  const match = label.trim().match(/^(\d+(?:\.\d+)?)([kKmM]?)$/);
  if (!match) return undefined;
  const [, digits, suffix] = match;
  if (!digits) return undefined;
  const value = Number.parseFloat(digits);
  if (!Number.isFinite(value) || value <= 0) return undefined;
  const multiplier = suffix === "m" || suffix === "M" ? 1_000_000 : suffix ? 1_000 : 1;
  return Math.round(value * multiplier);
}

/**
 * Convert a parsed pi catalog into ServerProviderModel entries for the
 * provider snapshot. Both slug and name are `{backend}/{model}` so pi can
 * route turns to the right backend — bare model slugs are ambiguous across
 * backends (pi defaults to the first backend it finds, which may be one
 * the user isn't authed for).
 */
export function piCatalogToServerModels(
  entries: ReadonlyArray<PiCatalogEntry>,
): ReadonlyArray<ServerProviderModel> {
  return entries.map((entry) => {
    const qualified = `${entry.backend}/${entry.model}`;
    return {
      slug: qualified,
      name: qualified,
      isCustom: false,
      capabilities: {
        ...DEFAULT_PI_MODEL_CAPABILITIES,
        supportsThinkingToggle: entry.supportsThinking,
      },
    };
  });
}

export interface PiBackendOption {
  /** Stable id matching pi's `--provider <name>` flag. */
  readonly id: string;
  /** Human-readable label for the settings dropdown. */
  readonly label: string;
  /** Environment variables pi will read for this backend, in priority order. */
  readonly envVars: ReadonlyArray<string>;
  /**
   * Keys pi writes into `~/.pi/agent/auth.json` when the user runs
   * `pi` → `/login` for this backend. Presence of any key here means
   * the user has an OAuth session without setting any env var.
   */
  readonly oauthKeys: ReadonlyArray<string>;
  /** Short instruction for the setup UI when no env var or OAuth is detected. */
  readonly setupHint: string;
}

export const PI_BACKEND_OPTIONS: ReadonlyArray<PiBackendOption> = [
  {
    id: "anthropic",
    label: "Anthropic (Claude Pro/Max)",
    envVars: ["ANTHROPIC_OAUTH_TOKEN", "ANTHROPIC_API_KEY"],
    oauthKeys: ["anthropic"],
    setupHint:
      "Run `pi` in a terminal and type `/login` to connect Claude Pro/Max, or export ANTHROPIC_API_KEY before starting the server.",
  },
  {
    id: "openai",
    label: "ChatGPT Plus/Pro (Codex Subscription)",
    envVars: ["OPENAI_API_KEY"],
    oauthKeys: ["openai-codex"],
    setupHint:
      "Run `pi` in a terminal and type `/login` to connect a ChatGPT Plus/Pro account, or export OPENAI_API_KEY.",
  },
  {
    id: "google",
    label: "Google Cloud Code Assist (Gemini)",
    envVars: ["GEMINI_API_KEY"],
    oauthKeys: ["google-cca", "google"],
    setupHint:
      "Run `pi` in a terminal and type `/login`, or export GEMINI_API_KEY before starting the server.",
  },
  {
    id: "github-copilot",
    label: "GitHub Copilot",
    envVars: [],
    oauthKeys: ["github-copilot"],
    setupHint: "Run `pi` in a terminal and type `/login` to connect GitHub Copilot.",
  },
  {
    id: "antigravity",
    label: "Antigravity (Gemini 3, Claude, GPT-OSS)",
    envVars: [],
    oauthKeys: ["antigravity"],
    setupHint: "Run `pi` in a terminal and type `/login` to connect Antigravity.",
  },
  {
    id: "groq",
    label: "Groq",
    envVars: ["GROQ_API_KEY"],
    oauthKeys: [],
    setupHint: "Export GROQ_API_KEY before starting the server.",
  },
  {
    id: "openrouter",
    label: "OpenRouter",
    envVars: ["OPENROUTER_API_KEY"],
    oauthKeys: [],
    setupHint: "Export OPENROUTER_API_KEY before starting the server.",
  },
  {
    id: "xai",
    label: "xAI (Grok)",
    envVars: ["XAI_API_KEY"],
    oauthKeys: [],
    setupHint: "Export XAI_API_KEY before starting the server.",
  },
  {
    id: "mistral",
    label: "Mistral",
    envVars: ["MISTRAL_API_KEY"],
    oauthKeys: [],
    setupHint: "Export MISTRAL_API_KEY before starting the server.",
  },
];

export function findPiBackendOption(backendId: string): PiBackendOption | undefined {
  const trimmed = backendId.trim();
  if (trimmed.length === 0) {
    return undefined;
  }
  return PI_BACKEND_OPTIONS.find((option) => option.id === trimmed);
}

/**
 * Resolve the pi auth storage file. pi reads `PI_CODING_AGENT_DIR` or
 * defaults to `~/.pi/agent`. The file is `auth.json` inside that dir.
 */
export function resolvePiAuthFilePath(env: NodeJS.ProcessEnv): string {
  const configured = env.PI_CODING_AGENT_DIR?.trim();
  const base =
    configured && configured.length > 0 ? configured : joinPath(homedir(), ".pi", "agent");
  return joinPath(base, "auth.json");
}

/**
 * Read the set of provider keys present in pi's auth.json. We only inspect
 * top-level keys — never token values — and tolerate a missing or malformed
 * file by returning an empty set.
 */
export function readPiOAuthKeys(authFilePath: string): ReadonlySet<string> {
  try {
    const raw = readFileSync(authFilePath, "utf8");
    const parsed: unknown = JSON.parse(raw);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      return new Set();
    }
    const keys = new Set<string>();
    for (const key of Object.keys(parsed)) {
      const value = (parsed as Record<string, unknown>)[key];
      if (value !== undefined && value !== null) {
        keys.add(key);
      }
    }
    return keys;
  } catch {
    return new Set();
  }
}

export type PiAuthSource = "env-var" | "oauth-file";

export interface PiAuthDetection {
  readonly authenticated: boolean;
  readonly source: PiAuthSource | undefined;
  readonly detectedEnvVar: string | undefined;
  readonly detectedOAuthKey: string | undefined;
  readonly checkedBackend: PiBackendOption | undefined;
  /**
   * All backends that look logged in across both env vars and OAuth file,
   * so the UI can show a per-backend status grid.
   */
  readonly availableBackends: ReadonlyArray<PiBackendOption>;
}

function backendMatchesEnv(option: PiBackendOption, env: NodeJS.ProcessEnv): string | undefined {
  for (const envVar of option.envVars) {
    const value = env[envVar];
    if (typeof value === "string" && value.trim().length > 0) {
      return envVar;
    }
  }
  return undefined;
}

function backendMatchesOAuth(
  option: PiBackendOption,
  oauthKeys: ReadonlySet<string>,
): string | undefined {
  for (const key of option.oauthKeys) {
    if (oauthKeys.has(key)) {
      return key;
    }
  }
  return undefined;
}

/**
 * Decide whether pi has a usable credential. Detection order:
 *   1. If `defaultProvider` is set, check env vars then OAuth file for that
 *      backend only.
 *   2. Otherwise, scan all backends and pick the first one with any signal,
 *      preferring env vars over OAuth (env vars are explicit user intent).
 *
 * We intentionally do NOT auto-adopt Codex/Claude credentials from this app's
 * own provider stores — pi can point at a different account or provider.
 */
export function detectPiAuth(input: {
  readonly defaultProvider: string;
  readonly env: NodeJS.ProcessEnv;
  readonly oauthKeys: ReadonlySet<string>;
}): PiAuthDetection {
  const availableBackends = PI_BACKEND_OPTIONS.filter(
    (option) =>
      backendMatchesEnv(option, input.env) !== undefined ||
      backendMatchesOAuth(option, input.oauthKeys) !== undefined,
  );

  const configured = findPiBackendOption(input.defaultProvider);

  if (configured) {
    const envHit = backendMatchesEnv(configured, input.env);
    if (envHit) {
      return {
        authenticated: true,
        source: "env-var",
        detectedEnvVar: envHit,
        detectedOAuthKey: undefined,
        checkedBackend: configured,
        availableBackends,
      };
    }
    const oauthHit = backendMatchesOAuth(configured, input.oauthKeys);
    if (oauthHit) {
      return {
        authenticated: true,
        source: "oauth-file",
        detectedEnvVar: undefined,
        detectedOAuthKey: oauthHit,
        checkedBackend: configured,
        availableBackends,
      };
    }
    return {
      authenticated: false,
      source: undefined,
      detectedEnvVar: undefined,
      detectedOAuthKey: undefined,
      checkedBackend: configured,
      availableBackends,
    };
  }

  for (const option of PI_BACKEND_OPTIONS) {
    const envHit = backendMatchesEnv(option, input.env);
    if (envHit) {
      return {
        authenticated: true,
        source: "env-var",
        detectedEnvVar: envHit,
        detectedOAuthKey: undefined,
        checkedBackend: option,
        availableBackends,
      };
    }
  }

  for (const option of PI_BACKEND_OPTIONS) {
    const oauthHit = backendMatchesOAuth(option, input.oauthKeys);
    if (oauthHit) {
      return {
        authenticated: true,
        source: "oauth-file",
        detectedEnvVar: undefined,
        detectedOAuthKey: oauthHit,
        checkedBackend: option,
        availableBackends,
      };
    }
  }

  return {
    authenticated: false,
    source: undefined,
    detectedEnvVar: undefined,
    detectedOAuthKey: undefined,
    checkedBackend: undefined,
    availableBackends,
  };
}

export interface PiLoginSpawnResult {
  readonly launched: boolean;
  /** Human-readable message to surface in the UI after attempting the spawn. */
  readonly message: string;
  /** Shell command the user can run manually if we couldn't launch a terminal. */
  readonly fallbackCommand: string;
}

/**
 * Launch `pi` in a new terminal window so the user can run `/login` and
 * complete pi's OAuth flow. On macOS we use osascript to open Terminal.app
 * (the user's default). On other platforms we return a fallback command
 * the UI can surface verbatim.
 *
 * We never send keystrokes to pi or try to automate `/login`; the user
 * drives the TUI. Our PiProvider's fs.watch on auth.json picks up the
 * resulting session automatically.
 */
export async function spawnPiLoginTerminal(input: {
  readonly backendLabel: string;
  readonly binaryPath: string;
}): Promise<PiLoginSpawnResult> {
  const binary = input.binaryPath.trim().length > 0 ? input.binaryPath : "pi";
  const fallbackCommand = `${binary}  # then type /login and pick "${input.backendLabel}"`;

  if (osPlatform() !== "darwin") {
    return {
      launched: false,
      message: `Open a terminal and run \`${binary}\`, then type \`/login\` and pick "${input.backendLabel}". We'll detect the login once it completes.`,
      fallbackCommand,
    };
  }

  // AppleScript string: escape backslashes and double quotes inside the inner command.
  const innerCommand = binary.replaceAll("\\", "\\\\").replaceAll('"', '\\"');
  const script = `tell application "Terminal" to activate
tell application "Terminal" to do script "${innerCommand}"`;

  return new Promise((resolve) => {
    const child = spawn("osascript", ["-e", script], {
      stdio: ["ignore", "ignore", "pipe"],
    });
    let stderr = "";
    child.stderr?.setEncoding("utf8");
    child.stderr?.on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.once("error", () => {
      resolve({
        launched: false,
        message: `Couldn't open Terminal automatically. Run \`${binary}\` yourself and type \`/login\`.`,
        fallbackCommand,
      });
    });
    child.once("exit", (code) => {
      if (code === 0) {
        resolve({
          launched: true,
          message: `Opened Terminal with pi. Type \`/login\` and pick "${input.backendLabel}" — we'll detect it automatically.`,
          fallbackCommand,
        });
        return;
      }
      resolve({
        launched: false,
        message:
          stderr.trim().length > 0
            ? `osascript failed: ${stderr.trim()}. Run \`${binary}\` yourself and type \`/login\`.`
            : `osascript exited ${code}. Run \`${binary}\` yourself and type \`/login\`.`,
        fallbackCommand,
      });
    });
  });
}

/**
 * Start a best-effort watcher on pi's auth.json. Fires `onChange` whenever
 * the file appears, is modified, or disappears. The watcher re-attaches
 * itself every few seconds if the target file doesn't exist yet — handles
 * first-login gracefully.
 *
 * Returns a cleanup function.
 */
export function watchPiAuthFile(input: {
  readonly authFilePath: string;
  readonly onChange: () => void;
}): () => void {
  let watcher: FSWatcher | null = null;
  let retryTimer: NodeJS.Timeout | null = null;
  let closed = false;

  const attach = (): void => {
    if (closed) return;
    try {
      watcher = fsWatch(input.authFilePath, { persistent: false }, () => {
        input.onChange();
      });
      watcher.on("error", () => {
        watcher?.close();
        watcher = null;
        scheduleReattach();
      });
    } catch {
      scheduleReattach();
    }
  };

  const attachToDirectory = (): void => {
    if (closed) return;
    try {
      const dir = pathDirname(input.authFilePath);
      const dirWatcher = fsWatch(dir, { persistent: false }, (_event, filename) => {
        if (!filename || filename === "auth.json") {
          input.onChange();
          // Re-attach directly to the file so we get writes too.
          dirWatcher.close();
          attach();
        }
      });
      dirWatcher.on("error", () => {
        dirWatcher.close();
        scheduleReattach();
      });
      watcher = dirWatcher;
    } catch {
      scheduleReattach();
    }
  };

  const scheduleReattach = (): void => {
    if (closed) return;
    retryTimer = setTimeout(() => {
      retryTimer = null;
      // Try the file first; if it doesn't exist, fall back to watching the dir.
      try {
        readFileSync(input.authFilePath, "utf8");
        attach();
      } catch {
        attachToDirectory();
      }
    }, 5_000);
  };

  // Initial attach: file first, directory as fallback.
  try {
    readFileSync(input.authFilePath, "utf8");
    attach();
  } catch {
    attachToDirectory();
  }

  return () => {
    closed = true;
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
    watcher?.close();
    watcher = null;
  };
}

/**
 * Compare two semver-ish version strings. Returns -1, 0, or 1. Non-numeric
 * components are treated as 0. Used for the min-recommended-version warning —
 * not security-sensitive, so "close enough" lexicographic handling is fine.
 */
export function compareVersions(a: string, b: string): number {
  const parse = (v: string): [number, number, number] => {
    const parts = v.split(".").slice(0, 3);
    const asNum = (index: number): number => {
      const raw = parts[index];
      if (raw === undefined) return 0;
      const n = Number.parseInt(raw, 10);
      return Number.isFinite(n) ? n : 0;
    };
    return [asNum(0), asNum(1), asNum(2)];
  };
  const [am, an, ap] = parse(a);
  const [bm, bn, bp] = parse(b);
  if (am !== bm) return am > bm ? 1 : -1;
  if (an !== bn) return an > bn ? 1 : -1;
  if (ap !== bp) return ap > bp ? 1 : -1;
  return 0;
}
