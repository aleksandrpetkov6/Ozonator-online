import fs from 'node:fs';
import path from 'node:path';

import { test } from '@playwright/test';
import { _electron as electron, ElectronApplication, Page } from 'playwright';

import { UiRobot } from '../helpers/uiRobot';

function findPackagedElectronExe(): string | null {
  const roots = ['release', 'out'].map((p) => path.resolve(process.cwd(), p));

  for (const root of roots) {
    if (!fs.existsSync(root)) continue;

    const stack: string[] = [root];
    while (stack.length) {
      const dir = stack.pop()!;
      let entries: fs.Dirent[] = [];
      try {
        entries = fs.readdirSync(dir, { withFileTypes: true });
      } catch {
        continue;
      }

      for (const entry of entries) {
        const full = path.join(dir, entry.name);

        if (entry.isDirectory()) {
          stack.push(full);
          continue;
        }

        if (!entry.isFile()) continue;
        if (!entry.name.toLowerCase().endsWith('.exe')) continue;

        const low = full.toLowerCase();
        const bad =
          low.includes('unins') ||
          low.includes('setup') ||
          low.includes('squirrel') ||
          low.includes('update');
        const inWinUnpacked = low.includes('win-unpacked');

        if (!bad && inWinUnpacked) {
          return full;
        }
      }
    }
  }

  return null;
}

async function launchApp(): Promise<{ app: ElectronApplication; page: Page }> {
  const exePath = findPackagedElectronExe();
  if (!exePath) {
    throw new Error(
      'Electron executable not found under release/out/win-unpacked. Build app first (npm run dist).'
    );
  }

  const app = await electron.launch({
    executablePath: exePath,
    timeout: 60_000,
  });

  const page = await app.firstWindow();
  await page.waitForLoadState('domcontentloaded');

  return { app, page };
}

test.describe('UI Robot smoke (generic interactions)', () => {
  test('click/type/filter/sort/scroll + runtime error checks', async () => {
    // Чтобы не ломать текущий CI, пока экран ещё не размечен data-testid:
    // включим тест позже одной переменной в workflow.
    test.skip(
      process.env.UI_ROBOT_ENABLE !== '1',
      'Set UI_ROBOT_ENABLE=1 after data-testid markup is added to the Products screen.'
    );

    const { app, page } = await launchApp();
    const robot = new UiRobot(page);

    try {
      await robot.attachConsoleCollector();

      await robot.runGenericInteractionPack({
        screenTestId: 'screen-products',
        viewportTestId: 'grid-products-viewport',
        rowTestId: 'grid-products-row',
        cellTextTestId: 'grid-products-cell-text',
        searchInputTestId: 'input-search',
        sortButtonTestId: 'sort-price',
        filterSelectTestId: 'filter-status',
        applyButtonTestId: 'btn-apply',
      });
    } finally {
      await app.close();
    }
  });
});
