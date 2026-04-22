import { describe, expect, it } from "vitest";

import { buildViewerWindowPath, parseViewerWindowRouteSearch } from "./viewerWindowRoute";

describe("viewerWindowRoute", () => {
  it("parses the viewer search params with preview as the default mode", () => {
    expect(parseViewerWindowRouteSearch({ path: "notes/today.md" })).toEqual({
      path: "notes/today.md",
      mode: "preview",
    });
  });

  it("normalizes invalid or empty search params", () => {
    expect(parseViewerWindowRouteSearch({ path: "   ", mode: "weird" })).toEqual({
      path: null,
      mode: "preview",
    });
  });

  it("builds a server-thread viewer window path", () => {
    expect(
      buildViewerWindowPath(
        {
          routeKind: "server",
          environmentId: "environment-local",
          threadId: "thread-123",
        },
        {
          path: "docs/spec.md",
          mode: "edit",
        },
      ),
    ).toBe("/_viewer/environment-local/thread-123?path=docs%2Fspec.md&mode=edit");
  });

  it("builds a draft viewer window path", () => {
    expect(
      buildViewerWindowPath(
        {
          routeKind: "draft",
          draftId: "draft-123",
        },
        {
          path: "docs/spec.md",
          mode: "preview",
        },
      ),
    ).toBe("/_viewer/draft/draft-123?path=docs%2Fspec.md");
  });
});
