import type { EnvironmentId, ThreadId } from "@workbench/contracts";
import { useQuery } from "@tanstack/react-query";
import { useCallback, useMemo, useState } from "react";

import { readEnvironmentApi } from "~/environmentApi";
import { readLocalApi } from "~/localApi";
import { projectReadFileQueryOptions } from "~/lib/projectReactQuery";
import { openInPreferredEditor } from "~/editorPreferences";
import { resolveWorkspaceSelectionPath } from "~/filePathDisplay";
import { resolvePathLinkTarget } from "~/terminal-links";
import { scopeProjectRef, scopeThreadRef } from "@workbench/client-runtime";
import { useSettings } from "~/hooks/useSettings";
import { createProjectSelectorByRef, createThreadSelectorByRef } from "~/storeSelectors";
import { useStore } from "~/store";
import { useTheme } from "~/hooks/useTheme";
import { describeWorkspaceArtifact } from "~/workspaceArtifacts";
import { toastManager } from "../ui/toast";

import { ViewerPane, type ViewerDocumentMode } from "./ViewerPane";

interface WorkspaceViewerPageProps {
  environmentId: EnvironmentId;
  threadId: ThreadId;
  initialPath: string | null;
  initialMode: ViewerDocumentMode;
}

export function WorkspaceViewerPage({
  environmentId,
  threadId,
  initialPath,
  initialMode,
}: WorkspaceViewerPageProps) {
  const threadRef = useMemo(
    () => scopeThreadRef(environmentId, threadId),
    [environmentId, threadId],
  );
  const serverThread = useStore(useMemo(() => createThreadSelectorByRef(threadRef), [threadRef]));
  const projectRef = serverThread
    ? scopeProjectRef(serverThread.environmentId, serverThread.projectId)
    : null;
  const project = useStore(useMemo(() => createProjectSelectorByRef(projectRef), [projectRef]));
  const workspaceRoot = serverThread?.worktreePath ?? project?.cwd ?? undefined;

  const settings = useSettings();
  const timestampFormat = settings.timestampFormat;
  const { resolvedTheme } = useTheme();

  const [selectedPath, setSelectedPath] = useState<string | null>(initialPath);
  const [documentViewMode, setDocumentViewMode] = useState<ViewerDocumentMode>(initialMode);
  const [selectedDocumentText, setSelectedDocumentText] = useState("");

  const selectedDescriptor = useMemo(
    () => (selectedPath ? describeWorkspaceArtifact(selectedPath) : null),
    [selectedPath],
  );

  const textFileQuery = useQuery(
    projectReadFileQueryOptions({
      environmentId,
      cwd: workspaceRoot ?? null,
      relativePath: selectedPath
        ? resolveWorkspaceSelectionPath(selectedPath, workspaceRoot)
        : null,
      enabled: !!workspaceRoot && selectedDescriptor?.previewKind === "text",
      maxBytes: 24_000,
    }),
  );

  const refreshWorkspace = useCallback(() => {
    void textFileQuery.refetch();
  }, [textFileQuery]);

  const resolveArtifactTargetPath = useCallback(
    (path: string) => (workspaceRoot ? resolvePathLinkTarget(path, workspaceRoot) : path),
    [workspaceRoot],
  );

  const openInEditor = useCallback(
    (path: string) => {
      const api = readLocalApi();
      if (!api) return;
      const targetPath = resolveArtifactTargetPath(path);
      void openInPreferredEditor(api, targetPath).catch((error) => {
        toastManager.add({
          type: "error",
          title: "Could not open file",
          description: error instanceof Error ? error.message : "An error occurred.",
        });
      });
    },
    [resolveArtifactTargetPath],
  );

  const openInNativeApp = useCallback(
    (path: string) => {
      const api = readLocalApi();
      if (!api) return;
      const targetPath = resolveArtifactTargetPath(path);
      void api.shell.openInEditor(targetPath, "file-manager").catch((error) => {
        toastManager.add({
          type: "error",
          title: "Could not open file",
          description: error instanceof Error ? error.message : "An error occurred.",
        });
      });
    },
    [resolveArtifactTargetPath],
  );

  const saveWorkspaceFile = useCallback(
    async ({ path, contents }: { path: string; contents: string }) => {
      if (!workspaceRoot) {
        toastManager.add({
          type: "error",
          title: "Could not save file",
          description: "No workspace folder is selected.",
        });
        throw new Error("workspace root unavailable");
      }
      const api = readEnvironmentApi(environmentId);
      if (!api) {
        toastManager.add({
          type: "error",
          title: "Could not save file",
          description: "Workspace connection isn't ready.",
        });
        throw new Error("environment api unavailable");
      }
      const relativePath = resolveWorkspaceSelectionPath(path, workspaceRoot);
      if (relativePath === null) {
        toastManager.add({
          type: "error",
          title: "Could not save file",
          description: "The selected file is outside the active workspace.",
        });
        throw new Error("file path outside workspace");
      }
      try {
        const result = await api.projects.writeFile({
          cwd: workspaceRoot,
          relativePath,
          contents,
        });
        toastManager.add({
          type: "success",
          title: "File saved",
          description: result.relativePath,
        });
        void textFileQuery.refetch();
      } catch (error) {
        toastManager.add({
          type: "error",
          title: "Could not save file",
          description: error instanceof Error ? error.message : "An error occurred.",
        });
        throw error;
      }
    },
    [environmentId, textFileQuery, workspaceRoot],
  );

  const syncSelectedDocumentText = useCallback(() => {
    const selection = window.getSelection();
    if (!selection || selection.rangeCount === 0 || selection.isCollapsed) {
      setSelectedDocumentText("");
      return;
    }
    setSelectedDocumentText(selection.toString());
  }, []);

  const openWorkspaceFileFromLink = useCallback(
    (path: string): boolean => {
      const selectionPath = resolveWorkspaceSelectionPath(path, workspaceRoot);
      if (selectionPath === null) return false;
      setSelectedPath(selectionPath);
      setDocumentViewMode("preview");
      return true;
    },
    [workspaceRoot],
  );

  const closeWindow = useCallback(() => {
    window.close();
  }, []);

  return (
    <div className="flex h-dvh min-h-0 flex-col bg-background text-foreground">
      <ViewerPane
        workspaceRoot={workspaceRoot}
        markdownCwd={workspaceRoot}
        resolvedTheme={resolvedTheme}
        timestampFormat={timestampFormat}
        selectedPath={selectedPath}
        selectedArtifact={null}
        documentViewMode={documentViewMode}
        documentText={textFileQuery.data?.contents ?? null}
        documentTextTruncated={textFileQuery.data?.truncated ?? false}
        documentTextLoading={textFileQuery.isLoading}
        patchPreview={null}
        selectedDocumentTextSelection={selectedDocumentText}
        onSetDocumentViewMode={setDocumentViewMode}
        onRefresh={refreshWorkspace}
        onOpenInApp={openInNativeApp}
        onOpenInEditor={openInEditor}
        onSyncSelection={syncSelectedDocumentText}
        onClearSelection={() => setSelectedDocumentText("")}
        onOpenWorkspaceFileLink={openWorkspaceFileFromLink}
        onOpenTurnDiff={undefined}
        onSaveFile={saveWorkspaceFile}
        onClosePane={closeWindow}
      />
    </div>
  );
}
