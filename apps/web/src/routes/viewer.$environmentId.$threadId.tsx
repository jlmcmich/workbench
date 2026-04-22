import { EnvironmentId, ThreadId } from "@workbench/contracts";
import { createFileRoute } from "@tanstack/react-router";

import ChatView from "../components/ChatView";
import { parseViewerWindowRouteSearch } from "../viewerWindowRoute";

function ThreadViewerRouteView() {
  const { environmentId, threadId } = Route.useParams({
    select: (params) => ({
      environmentId: EnvironmentId.make(params.environmentId),
      threadId: ThreadId.make(params.threadId),
    }),
  });
  const search = Route.useSearch();

  return (
    <ChatView
      environmentId={environmentId}
      threadId={threadId}
      routeKind="server"
      reserveTitleBarControlInset={false}
      viewerWindow={search}
    />
  );
}

export const Route = createFileRoute("/viewer/$environmentId/$threadId")({
  validateSearch: (search) => parseViewerWindowRouteSearch(search),
  component: ThreadViewerRouteView,
});
