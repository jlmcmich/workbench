import type { ViewerDocumentMode } from "./components/console/ViewerPane";

type ViewerWindowRouteTarget =
  | {
      routeKind: "server";
      environmentId: string;
      threadId: string;
    }
  | {
      routeKind: "draft";
      draftId: string;
    };

export interface ViewerWindowRouteSearch {
  path: string | null;
  mode: ViewerDocumentMode;
}

export function parseViewerWindowRouteSearch(
  search: Record<string, unknown>,
): ViewerWindowRouteSearch {
  const path = normalizeSearchString(search.path) ?? null;
  const mode = normalizeViewerDocumentMode(search.mode);

  return { path, mode };
}

export function buildViewerWindowPath(
  target: ViewerWindowRouteTarget,
  search: ViewerWindowRouteSearch,
): string {
  const pathname =
    target.routeKind === "server"
      ? `/viewer/${encodeURIComponent(target.environmentId)}/${encodeURIComponent(target.threadId)}`
      : `/viewer/draft/${encodeURIComponent(target.draftId)}`;
  const params = new URLSearchParams();
  if (search.path) {
    params.set("path", search.path);
  }
  if (search.mode !== "preview") {
    params.set("mode", search.mode);
  }
  const query = params.toString();
  return query.length > 0 ? `${pathname}?${query}` : pathname;
}

export function buildViewerWindowUrl(options: {
  baseUrl: string;
  target: ViewerWindowRouteTarget;
  search: ViewerWindowRouteSearch;
  useHashRouting?: boolean;
}): string {
  const routePath = buildViewerWindowPath(options.target, options.search);
  const base = new URL(options.baseUrl);

  if (options.useHashRouting) {
    base.pathname = "/";
    base.search = "";
    base.hash = routePath;
    return base.toString();
  }

  return new URL(routePath, base.origin).toString();
}

function normalizeSearchString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : undefined;
}

function normalizeViewerDocumentMode(value: unknown): ViewerDocumentMode {
  return value === "source" || value === "edit" ? value : "preview";
}
