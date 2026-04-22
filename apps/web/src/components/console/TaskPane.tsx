import type { TimestampFormat } from "@workbench/contracts/settings";
import { CheckIcon, ChevronDownIcon, ChevronRightIcon, Loader2Icon, Rows3Icon } from "lucide-react";
import { useState } from "react";

import { cn } from "~/lib/utils";

import {
  buildProposedPlanMarkdownFilename,
  downloadPlanAsTextFile,
  normalizePlanMarkdownForExport,
  proposedPlanTitle,
  stripDisplayedPlanMarkdown,
} from "../../proposedPlan";
import type { ActivePlanState, LatestProposedPlanState } from "../../session-logic";
import { formatTimestamp } from "../../timestampFormat";
import ChatMarkdown from "../ChatMarkdown";
import { Button } from "../ui/button";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../ui/menu";

interface TaskPaneProps {
  workspaceRoot: string | undefined;
  markdownCwd: string | undefined;
  timestampFormat: TimestampFormat;
  activePlan: ActivePlanState | null;
  activeProposedPlan: LatestProposedPlanState | null;
  isSavingPlanToWorkspace: boolean;
  isPlanCopied: boolean;
  onCopyPlan: (markdown: string) => void;
  onSavePlanToWorkspace: () => void;
  onOpenWorkspaceFileLink: (path: string) => boolean;
}

/**
 * Body content of the Task card. The card chrome (icon + title + collapse +
 * close X) is provided by the surrounding `PaneCard` in `ConsoleRail`.
 */
export function TaskPane({
  workspaceRoot,
  markdownCwd,
  timestampFormat,
  activePlan,
  activeProposedPlan,
  isSavingPlanToWorkspace,
  isPlanCopied,
  onCopyPlan,
  onSavePlanToWorkspace,
  onOpenWorkspaceFileLink,
}: TaskPaneProps) {
  const [showPlanDetails, setShowPlanDetails] = useState(false);

  const planMarkdown = activeProposedPlan?.planMarkdown ?? null;
  const displayedPlanMarkdown = planMarkdown ? stripDisplayedPlanMarkdown(planMarkdown) : null;
  const planTitle = planMarkdown ? proposedPlanTitle(planMarkdown) : null;

  const hasContent = !!activePlan || !!activeProposedPlan;

  return (
    <div className="min-h-0 flex-1">
      <div className="space-y-3 p-3">
        {!hasContent ? (
          <div className="rounded-2xl border border-dashed border-border/60 bg-background/45 p-4 text-sm leading-6 text-muted-foreground/72">
            No active task. When the agent proposes a plan or starts work, you'll see the steps and
            their progress here.
          </div>
        ) : null}

        {activePlan?.explanation ? (
          <div className="rounded-2xl border border-border/55 bg-background/60 p-4 text-sm leading-6 text-muted-foreground/82">
            {activePlan.explanation}
          </div>
        ) : null}

        {activePlan?.steps.length ? (
          <ol className="space-y-2.5 rounded-2xl border border-border/55 bg-background/60 p-4">
            {activePlan.steps.map((step) => {
              const isCompleted = step.status === "completed";
              const isInProgress = step.status === "inProgress";
              return (
                <li key={`${step.status}:${step.step}`} className="flex items-start gap-3">
                  <PlanStepStatusIcon status={step.status} />
                  <p
                    className={cn(
                      "min-w-0 flex-1 text-sm leading-6",
                      isCompleted && "text-muted-foreground/65 line-through",
                      isInProgress && "font-medium text-foreground",
                      !isCompleted && !isInProgress && "text-foreground/80",
                    )}
                  >
                    {step.step}
                  </p>
                </li>
              );
            })}
          </ol>
        ) : null}

        {displayedPlanMarkdown && planMarkdown ? (
          <div className="rounded-2xl border border-border/55 bg-background/60">
            <div className="flex items-center justify-between gap-3 border-b border-border/50 px-4 py-3">
              <div>
                <p className="text-sm font-medium text-foreground/88">
                  {planTitle ?? "Task outline"}
                </p>
                <p className="text-[11px] text-muted-foreground/65">
                  {formatTimestamp(activeProposedPlan!.updatedAt, timestampFormat)}
                </p>
              </div>
              <Menu>
                <MenuTrigger
                  render={<Button size="icon-xs" variant="ghost" aria-label="Task actions" />}
                >
                  <Rows3Icon className="size-3.5" />
                </MenuTrigger>
                <MenuPopup align="end">
                  <MenuItem onClick={() => onCopyPlan(planMarkdown)}>
                    {isPlanCopied ? "Copied!" : "Copy plan"}
                  </MenuItem>
                  <MenuItem
                    onClick={() =>
                      downloadPlanAsTextFile(
                        buildProposedPlanMarkdownFilename(planMarkdown),
                        normalizePlanMarkdownForExport(planMarkdown),
                      )
                    }
                  >
                    Download markdown
                  </MenuItem>
                  <MenuItem
                    onClick={onSavePlanToWorkspace}
                    disabled={!workspaceRoot || isSavingPlanToWorkspace}
                  >
                    Save to workspace
                  </MenuItem>
                </MenuPopup>
              </Menu>
            </div>
            <button
              type="button"
              className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left"
              onClick={() => setShowPlanDetails((value) => !value)}
            >
              <span className="text-sm text-muted-foreground/78">
                Review the current task outline
              </span>
              {showPlanDetails ? (
                <ChevronDownIcon className="size-4 text-muted-foreground/65" />
              ) : (
                <ChevronRightIcon className="size-4 text-muted-foreground/65" />
              )}
            </button>
            {showPlanDetails ? (
              <div className="border-t border-border/55 px-4 py-4">
                <ChatMarkdown
                  cwd={markdownCwd}
                  text={displayedPlanMarkdown}
                  onOpenWorkspaceFile={onOpenWorkspaceFileLink}
                />
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function PlanStepStatusIcon({ status }: { status: "completed" | "inProgress" | "pending" }) {
  if (status === "completed") {
    return (
      <span
        aria-label="Completed"
        className="mt-0.5 flex size-4 shrink-0 items-center justify-center rounded-full bg-emerald-500/15 text-emerald-600 dark:text-emerald-400"
      >
        <CheckIcon className="size-3" strokeWidth={3} />
      </span>
    );
  }
  if (status === "inProgress") {
    return (
      <span
        aria-label="In progress"
        className="mt-0.5 flex size-4 shrink-0 items-center justify-center rounded-full bg-blue-500/15 text-blue-600 dark:text-blue-400"
      >
        <Loader2Icon className="size-3 animate-spin" />
      </span>
    );
  }
  return (
    <span
      aria-label="Pending"
      className="mt-0.5 size-4 shrink-0 rounded-full border border-border/60"
    />
  );
}
