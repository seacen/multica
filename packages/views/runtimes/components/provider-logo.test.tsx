import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import museLogo from "./muse-logo.svg";
import { ProviderLogo } from "./provider-logo";

// Vite inlines a small SVG as a data: URL and Next hands back a { src } object,
// so the test compares against the imported asset itself rather than a filename.
const museSrc = typeof museLogo === "string" ? museLogo : museLogo.src;

describe("ProviderLogo", () => {
  it("draws Muse's own mark rather than the generic fallback", () => {
    const { container } = render(<ProviderLogo provider="muse" className="h-4 w-4" />);
    const img = container.querySelector("img");
    expect(img).not.toBeNull();
    expect(img?.getAttribute("src")).toBe(museSrc);
  });

  it("still falls back for a provider it has no mark for", () => {
    const { container } = render(<ProviderLogo provider="no-such-provider" />);
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("svg")).not.toBeNull();
  });
});
