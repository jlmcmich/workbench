import { EnvironmentId, ThreadId } from "@workbench/contracts";
import { createFileRoute } from "@tanstack/react-router";

import { WorkspaceViewerPage } from "../components/console/WorkspaceViewerPage";
import { parseViewerWindowRouteSearch } from "../viewerWindowRoute";

function ThreadViewerRouteView() {
  const { environmentId, threadId } = Route.useParams({
    select: (params) => ({
      environmentId: EnvironmentId.make(params.environmentId),
      threadId: ThreadId.make(params.threadId),
    }),
  });
  const { path, mode } = Route.useSearch();

  return (
    <WorkspaceViewerPage
      environmentId={environmentId}
      threadId={threadId}
      initialPath={path}
      initialMode={mode}
    />
  );
}

export const Route = createFileRoute("/viewer/$environmentId/$threadId")({
  validateSearch: (search) => parseViewerWindowRouteSearch(search),
  component: ThreadViewerRouteView,
});
