import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useEffect, useMemo } from "react";

import { threadHasStarted } from "../components/ChatView.logic";
import { WorkspaceViewerPage } from "../components/console/WorkspaceViewerPage";
import { DraftId, useComposerDraftStore } from "../composerDraftStore";
import { createThreadSelectorAcrossEnvironments } from "../storeSelectors";
import { useStore } from "../store";
import { buildThreadRouteParams } from "../threadRoutes";
import { parseViewerWindowRouteSearch } from "../viewerWindowRoute";

function DraftViewerRouteView() {
  const navigate = useNavigate();
  const { draftId: rawDraftId } = Route.useParams();
  const { path, mode } = Route.useSearch();
  const draftId = DraftId.make(rawDraftId);
  const draftSession = useComposerDraftStore((store) => store.getDraftSession(draftId));
  const serverThread = useStore(
    useMemo(
      () => createThreadSelectorAcrossEnvironments(draftSession?.threadId ?? null),
      [draftSession?.threadId],
    ),
  );
  const serverThreadStarted = threadHasStarted(serverThread);
  const canonicalThreadRef = useMemo(
    () =>
      draftSession?.promotedTo
        ? serverThreadStarted
          ? draftSession.promotedTo
          : null
        : serverThread
          ? {
              environmentId: serverThread.environmentId,
              threadId: serverThread.id,
            }
          : null,
    [draftSession?.promotedTo, serverThread, serverThreadStarted],
  );

  useEffect(() => {
    if (!canonicalThreadRef) {
      return;
    }
    void navigate({
      to: "/$environmentId/$threadId",
      params: buildThreadRouteParams(canonicalThreadRef),
      replace: true,
    });
  }, [canonicalThreadRef, navigate]);

  useEffect(() => {
    if (draftSession || canonicalThreadRef) {
      return;
    }
    void navigate({ to: "/", replace: true });
  }, [canonicalThreadRef, draftSession, navigate]);

  if (canonicalThreadRef) {
    return (
      <WorkspaceViewerPage
        environmentId={canonicalThreadRef.environmentId}
        threadId={canonicalThreadRef.threadId}
        initialPath={path}
        initialMode={mode}
      />
    );
  }

  if (!draftSession) {
    return null;
  }

  return (
    <WorkspaceViewerPage
      environmentId={draftSession.environmentId}
      threadId={draftSession.threadId}
      initialPath={path}
      initialMode={mode}
    />
  );
}

export const Route = createFileRoute("/viewer/draft/$draftId")({
  validateSearch: (search) => parseViewerWindowRouteSearch(search),
  component: DraftViewerRouteView,
});
