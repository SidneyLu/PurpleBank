import { expect, test } from "@playwright/test";

test("guest can open search page", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "PurpleBank Sequence Explorer" })).toBeVisible();
});

test("navbar exposes auth links for guest", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("link", { name: "Login" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Register" })).toBeVisible();
});
