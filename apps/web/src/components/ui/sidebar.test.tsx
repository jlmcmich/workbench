import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  resolveAcceptedSidebarWidth,
  SidebarMenuAction,
  SidebarMenuButton,
  SidebarMenuSubButton,
  SidebarProvider,
} from "./sidebar";

function renderSidebarButton(className?: string) {
  return renderToStaticMarkup(
    <SidebarProvider>
      <SidebarMenuButton className={className}>Projects</SidebarMenuButton>
    </SidebarProvider>,
  );
}

describe("sidebar interactive cursors", () => {
  it("uses a pointer cursor for menu buttons by default", () => {
    const html = renderSidebarButton();

    expect(html).toContain('data-slot="sidebar-menu-button"');
    expect(html).toContain("cursor-pointer");
  });

  it("lets project drag handles override the default pointer cursor", () => {
    const html = renderSidebarButton("cursor-grab");

    expect(html).toContain("cursor-grab");
    expect(html).not.toContain("cursor-pointer");
  });

  it("uses a pointer cursor for menu actions", () => {
    const html = renderToStaticMarkup(
      <SidebarMenuAction aria-label="Create thread">
        <span>+</span>
      </SidebarMenuAction>,
    );

    expect(html).toContain('data-slot="sidebar-menu-action"');
    expect(html).toContain("cursor-pointer");
  });

  it("uses a pointer cursor for submenu buttons", () => {
    const html = renderToStaticMarkup(
      <SidebarMenuSubButton render={<button type="button" />}>Show more</SidebarMenuSubButton>,
    );

    expect(html).toContain('data-slot="sidebar-menu-sub-button"');
    expect(html).toContain("cursor-pointer");
  });
});

describe("resolveAcceptedSidebarWidth", () => {
  const context = {
    rail: {} as HTMLButtonElement,
    side: "left" as const,
    sidebarRoot: {} as HTMLElement,
    wrapper: {} as HTMLElement,
  };

  it("clamps widths without extra acceptance rules", () => {
    expect(
      resolveAcceptedSidebarWidth({
        currentWidth: 224,
        desiredWidth: 420,
        options: {
          maxWidth: 320,
          minWidth: 192,
          storageKey: null,
        },
        ...context,
      }),
    ).toBe(320);
  });

  it("shrinks to the widest width accepted by the layout rule", () => {
    expect(
      resolveAcceptedSidebarWidth({
        currentWidth: 260,
        desiredWidth: 320,
        options: {
          maxWidth: 400,
          minWidth: 192,
          shouldAcceptWidth: ({ nextWidth }) => nextWidth <= 240,
          storageKey: null,
        },
        ...context,
      }),
    ).toBe(240);
  });
});
