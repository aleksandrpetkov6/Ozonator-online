import { expect, Page } from '@playwright/test';

type RobotOptions = {
  defaultTimeoutMs?: number;
  scrollSettleMs?: number;
};

export class UiRobot {
  private page: Page;
  private defaultTimeoutMs: number;
  private scrollSettleMs: number;

  constructor(page: Page, options: RobotOptions = {}) {
    this.page = page;
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? 10_000;
    this.scrollSettleMs = options.scrollSettleMs ?? 250;
  }

  // ---------- Base selectors ----------
  byTestId(testId: string) {
    return this.page.getByTestId(testId);
  }

  // ---------- Screen readiness / state ----------
  async waitScreenReady(screenTestId: string) {
    const screen = this.byTestId(screenTestId);
    await expect(screen).toBeVisible({ timeout: this.defaultTimeoutMs });
    await expect(screen).toHaveAttribute('data-ready', 'true', { timeout: this.defaultTimeoutMs });
    return screen;
  }

  async expectNotLoading(screenOrViewportTestId: string) {
    const el = this.byTestId(screenOrViewportTestId);
    await expect(el).toBeVisible({ timeout: this.defaultTimeoutMs });
    await expect(el).toHaveAttribute('data-loading', 'false', { timeout: this.defaultTimeoutMs });
  }

  async expectNoEmptyState(screenTestId: string) {
    const screen = this.byTestId(screenTestId);
    await expect(screen).toBeVisible({ timeout: this.defaultTimeoutMs });
    await expect(screen).toHaveAttribute('data-empty-state', 'false', { timeout: this.defaultTimeoutMs });
  }

  // ---------- Click / Type / Select ----------
  async click(testId: string) {
    const el = this.byTestId(testId);
    await expect(el).toBeVisible({ timeout: this.defaultTimeoutMs });
    await el.click();
  }

  async type(testId: string, text: string, clearBefore = true) {
    const el = this.byTestId(testId);
    await expect(el).toBeVisible({ timeout: this.defaultTimeoutMs });
    if (clearBefore) await el.fill('');
    await el.fill(text);
  }

  async select(testId: string, value: string) {
    const el = this.byTestId(testId);
    await expect(el).toBeVisible({ timeout: this.defaultTimeoutMs });
    await el.selectOption(value);
  }

  // ---------- Sort / filter generic ----------
  async clickAndSettle(testId: string, settleMs = 300) {
    await this.click(testId);
    await this.page.waitForTimeout(settleMs);
  }

  // ---------- Scroll ----------
  async scrollViewportFast(viewportTestId: string, options?: { steps?: number; jumpPx?: number; direction?: 'down' | 'up' }) {
    const steps = options?.steps ?? 12;
    const jumpPx = options?.jumpPx ?? 900;
    const dir = options?.direction ?? 'down';

    const viewport = this.byTestId(viewportTestId);
    await expect(viewport).toBeVisible({ timeout: this.defaultTimeoutMs });

    for (let i = 0; i < steps; i++) {
      await viewport.evaluate(
        (el, p) => {
          const node = el as HTMLElement;
          const sign = p.direction === 'down' ? 1 : -1;
          node.scrollTop = Math.max(0, node.scrollTop + sign * p.jumpPx);
        },
        { jumpPx, direction: dir }
      );
      await this.page.waitForTimeout(40);
    }

    await this.page.waitForTimeout(this.scrollSettleMs);
  }

  async dragScrollbarLikeHuman(viewportTestId: string, options?: { ratio?: number }) {
    const ratio = options?.ratio ?? 0.85;
    const viewport = this.byTestId(viewportTestId);
    await expect(viewport).toBeVisible({ timeout: this.defaultTimeoutMs });

    const box = await viewport.boundingBox();
    if (!box) throw new Error('Viewport bounding box not available');

    // Простой суррогат "ползунка": быстрое колесо + прыжок scrollTop
    await viewport.hover();
    await this.page.mouse.wheel(0, 3000);
    await viewport.evaluate(
      (el, r) => {
        const node = el as HTMLElement;
        const max = Math.max(0, node.scrollHeight - node.clientHeight);
        node.scrollTop = Math.floor(max * r);
      },
      ratio
    );
    await this.page.waitForTimeout(this.scrollSettleMs);
  }

  // ---------- UI invariants ----------
  async expectVisibleRowsHaveText(rowTestId: string, cellTextTestId: string) {
    const rows = this.byTestId(rowTestId);
    const rowCount = await rows.count();

    if (rowCount === 0) {
      throw new Error(`No visible rows found by testId=${rowTestId}`);
    }

    const cells = this.byTestId(cellTextTestId);
    const cellCount = await cells.count();
    if (cellCount === 0) {
      throw new Error(`No visible text cells found by testId=${cellTextTestId}`);
    }

    // Проверяем первые N видимых ячеек, чтобы быстро ловить "пустоту"
    const sample = Math.min(cellCount, 25);
    for (let i = 0; i < sample; i++) {
      const txt = (await cells.nth(i).innerText()).trim();
      if (!txt) {
        throw new Error(`Empty visible cell text detected at index ${i}`);
      }
    }
  }

  async expectNoUiRuntimeErrors() {
    const errors = await this.page.evaluate(() => {
      const bridge = (window as any).__OZ_UI_TEST__;
      return Array.isArray(bridge?.errors) ? bridge.errors : [];
    });

    if (errors.length > 0) {
      throw new Error(`UI runtime errors detected: ${errors.join(' | ')}`);
    }
  }

  async expectNoCriticalConsoleErrors() {
    // Этот метод полезен вместе с attachConsoleCollector() ниже
    const errors = await this.page.evaluate(() => {
      const w = window as any;
      return Array.isArray(w.__OZ_UI_CONSOLE_ERRORS__) ? w.__OZ_UI_CONSOLE_ERRORS__ : [];
    });

    if (errors.length > 0) {
      throw new Error(`Console errors detected: ${errors.join(' | ')}`);
    }
  }

  // ---------- Console collector (optional but useful) ----------
  async attachConsoleCollector() {
    await this.page.addInitScript(() => {
      (window as any).__OZ_UI_CONSOLE_ERRORS__ = [];

      const origError = console.error;
      console.error = (...args: any[]) => {
        try {
          const msg = args.map((x) => (typeof x === 'string' ? x : JSON.stringify(x))).join(' ');
          (window as any).__OZ_UI_CONSOLE_ERRORS__.push(msg);
        } catch {}
        origError(...args);
      };
    });
  }

  // ---------- Generic interaction pack ----------
  async runGenericInteractionPack(config: {
    screenTestId: string;
    viewportTestId: string;
    rowTestId: string;
    cellTextTestId: string;
    searchInputTestId?: string;
    sortButtonTestId?: string;
    filterSelectTestId?: string;
    applyButtonTestId?: string;
  }) {
    await this.waitScreenReady(config.screenTestId);
    await this.expectNotLoading(config.screenTestId);

    if (config.searchInputTestId) {
      await this.type(config.searchInputTestId, 'test');
      await this.page.waitForTimeout(200);
      await this.type(config.searchInputTestId, '');
    }

    if (config.filterSelectTestId) {
      // Пробуем выбрать первый option, если это select
      const filter = this.byTestId(config.filterSelectTestId);
      const isSelect = await filter.evaluate((el) => el.tagName.toLowerCase() === 'select');
      if (isSelect) {
        const values = await filter.evaluate((el) =>
          Array.from((el as HTMLSelectElement).options).map((o) => o.value).filter(Boolean)
        );
        if (values.length > 0) {
          await this.select(config.filterSelectTestId, values[0]);
        }
      } else {
        await this.click(config.filterSelectTestId);
      }
      if (config.applyButtonTestId) {
        await this.clickAndSettle(config.applyButtonTestId);
      }
    }

    if (config.sortButtonTestId) {
      await this.clickAndSettle(config.sortButtonTestId);
    }

    await this.expectVisibleRowsHaveText(config.rowTestId, config.cellTextTestId);

    // Быстрый скролл вниз/вверх + проверка, что видимые ячейки не пустые
    await this.scrollViewportFast(config.viewportTestId, { direction: 'down' });
    await this.expectVisibleRowsHaveText(config.rowTestId, config.cellTextTestId);

    await this.dragScrollbarLikeHuman(config.viewportTestId, { ratio: 0.9 });
    await this.expectVisibleRowsHaveText(config.rowTestId, config.cellTextTestId);

    await this.scrollViewportFast(config.viewportTestId, { direction: 'up' });
    await this.expectVisibleRowsHaveText(config.rowTestId, config.cellTextTestId);

    await this.expectNoUiRuntimeErrors();
    await this.expectNoCriticalConsoleErrors();
  }
}
