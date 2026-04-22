import { describe, expect, it } from "vitest";

import {
  buildViewerWindowPath,
  buildViewerWindowUrl,
  parseViewerWindowRouteSearch,
} from "./viewerWindowRoute";

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
    ).toBe("/viewer/environment-local/thread-123?path=docs%2Fspec.md&mode=edit");
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
    ).toBe("/viewer/draft/draft-123?path=docs%2Fspec.md");
  });

  it("builds a browser viewer window url", () => {
    expect(
      buildViewerWindowUrl({
        baseUrl: "http://127.0.0.1:5733/",
        target: {
          routeKind: "server",
          environmentId: "environment-local",
          threadId: "thread-123",
        },
        search: {
          path: "docs/spec.md",
          mode: "edit",
        },
      }),
    ).toBe(
      "http://127.0.0.1:5733/viewer/environment-local/thread-123?path=docs%2Fspec.md&mode=edit",
    );
  });

  it("builds a hash-routed viewer window url for electron", () => {
    expect(
      buildViewerWindowUrl({
        baseUrl: "http://127.0.0.1:5733/#/",
        target: {
          routeKind: "draft",
          draftId: "draft-123",
        },
        search: {
          path: "docs/spec.md",
          mode: "preview",
        },
        useHashRouting: true,
      }),
    ).toBe("http://127.0.0.1:5733/#/viewer/draft/draft-123?path=docs%2Fspec.md");
  });
});
