import { test, expect } from '@playwright/test';

test('user can login', async ({ page }) => {
  await page.goto('http://localhost:5173/login');
  await page.fill('input[type="text"]', 'admin');
  await page.fill('input[type="password"]', 'admin');
  await page.click('button[type="submit"]');
  await expect(page).toHaveURL('http://localhost:5173/');
});
