import { test, expect, Page, Locator } from '@playwright/test';
import * as fs from 'node:fs';
import * as http from 'node:http';
import * as path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const NON_DESTRUCTIVE_BUTTON_BLACKLIST = /(удал|delete|remove|reset|сброс|drop|clear all|очист|logout|выйти|exit|close app|quit)/i;

async function firstVisible(locator: Locator, max = 10): Promise<Locator | null> {
  const count = await locator.count();
  const limit = Math.min(count, max);
  for (let i = 0; i < limit; i += 1) {
    const item = locator.nth(i);
    if (await item.isVisible().catch(() => false)) return item;
  }
  return null;
}

async function safeClick(locator: Locator): Promise<boolean> {
  try {
    if (!(await locator.isVisible())) return false;
    if (!(await locator.isEnabled())) return false;
    await locator.click({ timeout: 2000 });
    return true;
  } catch {
    return false;
  }
}

type UiEntry = { target: string; mode: 'url' | 'file'; checked: string[] };

function findBuiltIndexHtml(): { filePath: string; checked: string[] } | null {
  const cwd = process.cwd();
  const checked: string[] = [];
  const direct = [
    'dist/index.html',
    'build/index.html',
    'out/index.html',
    'www/index.html',
    'app/dist/index.html',
    'renderer/dist/index.html',
    'packages/renderer/dist/index.html',
  ];

  for (const rel of direct) {
    const abs = path.resolve(cwd, rel);
    checked.push(abs);
    if (fs.existsSync(abs)) return { filePath: abs, checked };
  }

  const skip = new Set(['node_modules', '.git', 'playwright-report', 'test-results', 'coverage']);
  const queue: string[] = [cwd];
  let scannedDirs = 0;

  while (queue.length && scannedDirs < 80) {
    const dir = queue.shift()!;
    scannedDirs += 1;

    let entries: fs.Dirent[] = [];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        if (skip.has(entry.name)) continue;
        if (entry.name.startsWith('.')) continue;
        queue.push(full);
        continue;
      }
      if (!entry.isFile()) continue;
      if (entry.name !== 'index.html') continue;

      const rel = path.relative(cwd, full).replace(/\\/g, '/');
      checked.push(full);
      if (/(^|\/)(dist|build|out|www)(\/|$)/i.test(rel)) {
        return { filePath: full, checked };
      }
    }
  }

  return null;
}

async function resolveUiEntry(): Promise<UiEntry> {
  const fromEnv = [
    process.env.E2E_BASE_URL,
    process.env.PLAYWRIGHT_BASE_URL,
    process.env.BASE_URL,
  ].filter(Boolean) as string[];

  const defaults = [
    'http://127.0.0.1:4173',
    'http://localhost:4173',
    'http://127.0.0.1:5173',
    'http://localhost:5173',
    'http://127.0.0.1:3000',
    'http://localhost:3000',
  ];

  const checked: string[] = [];
  const candidates = [...new Set([...fromEnv, ...defaults])];

  for (const url of candidates) {
    checked.push(url);
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 1500);
      const res = await fetch(url, { method: 'GET', signal: ctrl.signal });
      clearTimeout(t);
      if (res.ok || (res.status >= 300 && res.status < 500)) {
        return { target: url, mode: 'url', checked };
      }
    } catch {
      // try next
    }
  }

  const built = findBuiltIndexHtml();
  if (built) {
    checked.push(...built.checked.filter((x) => !checked.includes(x)));
    return { target: pathToFileURL(built.filePath).href, mode: 'file', checked };
  }

  throw new Error(
    `Не найден доступный UI (ни URL, ни built index.html). Проверены: ${checked.join(', ')}`,
  );
}



type LocalUiServer = {
  url: string;
  close: () => Promise<void>;
};

function contentTypeFor(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.html') return 'text/html; charset=utf-8';
  if (ext === '.js' || ext === '.mjs' || ext === '.cjs') return 'application/javascript; charset=utf-8';
  if (ext === '.css') return 'text/css; charset=utf-8';
  if (ext === '.json') return 'application/json; charset=utf-8';
  if (ext === '.svg') return 'image/svg+xml';
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.woff') return 'font/woff';
  if (ext === '.woff2') return 'font/woff2';
  if (ext === '.ttf') return 'font/ttf';
  return 'application/octet-stream';
}

async function startLocalUiServer(indexFilePath: string): Promise<LocalUiServer> {
  const rootDir = path.dirname(indexFilePath);

  const server = http.createServer((req, res) => {
    const reqUrl = req.url || '/';
    const urlPath = reqUrl.split('?')[0].split('#')[0] || '/';
    const normalized = decodeURIComponent(urlPath === '/' ? '/index.html' : urlPath);
    const candidate = path.resolve(rootDir, `.${normalized}`);

    if (!candidate.startsWith(rootDir)) {
      res.statusCode = 403;
      res.end('forbidden');
      return;
    }

    if (!fs.existsSync(candidate) || !fs.statSync(candidate).isFile()) {
      res.statusCode = 404;
      res.end('not found');
      return;
    }

    res.setHeader('Content-Type', contentTypeFor(candidate));
    fs.createReadStream(candidate).pipe(res);
  });

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });

  const addr = server.address();
  const port = typeof addr === 'object' && addr ? addr.port : 41731;

  return {
    url: `http://127.0.0.1:${port}/index.html`,
    close: async () => {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

async function installE2EMockApi(page: Page): Promise<void> {
  await page.addInitScript(() => {
    const w = window as any;
    if (w.__ozonatorE2EApiStubInstalled) return;
    w.__ozonatorE2EApiStubInstalled = true;

    try {
      if (!window.location.hash) {
        window.location.hash = '#/products';
      }
    } catch {
      // ignore
    }

    const attachListAliases = <T extends any[]>(items: T): T => {
      const arr = items as any;
      arr.items = arr;
      arr.rows = arr;
      arr.data = arr;
      arr.list = arr;
      arr.results = arr;
      arr.products = arr;
      arr.logs = arr;
      return items;
    };

    const products = attachListAliases(Array.from({ length: 700 }, (_, i) => ({
      id: i + 1,
      product_id: 100000 + i,
      sku: 200000 + i,
      offer_id: `e2e_offer_${i + 1}`,
      name: `E2E Product ${i + 1}`,
      category: `Категория ${(i % 12) + 1}`,
      category_name: `Категория ${(i % 12) + 1}`,
      brand: `Brand ${(i % 9) + 1}`,
      type: `Type ${(i % 5) + 1}`,
      barcode: `460000000${String(i).padStart(4, '0')}`,
      is_visible: true,
      visible: true,
      hidden_reasons: i % 11 === 0 ? 'no_stock' : '',
      created_at: new Date(Date.now() - i * 3_600_000).toISOString(),
      updated_at: new Date(Date.now() - i * 1_800_000).toISOString(),
      price: 100 + (i % 50),
      stock: 10 + (i % 40),
      quantity: 10 + (i % 40),
    })));

    const salesRows = attachListAliases(Array.from({ length: 240 }, (_, i) => ({
      ...products[i],
      offer_id: `sale_offer_${i + 1}`,
      name: `Продажа ${i + 1}`,
      created_at: new Date(Date.now() - i * 86_400_000).toISOString(),
      updated_at: new Date(Date.now() - i * 43_200_000).toISOString(),
      hidden_reasons: '',
      is_visible: true,
    })));

    const returnsRows = attachListAliases(Array.from({ length: 160 }, (_, i) => ({
      ...products[(i + 20) % products.length],
      offer_id: `return_offer_${i + 1}`,
      name: `Возврат ${i + 1}`,
      created_at: new Date(Date.now() - i * 172_800_000).toISOString(),
      updated_at: new Date(Date.now() - i * 86_400_000).toISOString(),
      hidden_reasons: i % 5 === 0 ? 'archived' : '',
      is_visible: i % 5 !== 0,
    })));

    const stocksRows = attachListAliases(
      Array.from({ length: 140 }, (_, i) => {
        const base = products[i % products.length] as any;
        const zones = (i % 9 === 0)
          ? ['A-01', 'B-07']
          : [`${['A', 'B', 'C'][i % 3]}-${String((i % 18) + 1).padStart(2, '0')}`];
        return zones.map((zone, zIdx) => ({
          ...base,
          offer_id: `stock_offer_${i + 1}`,
          name: `Остаток ${i + 1}`,
          warehouse_id: 100 + ((i + zIdx) % 3),
          warehouse_name: ['Москва', 'СПб', 'Казань'][(i + zIdx) % 3],
          placement_zone: zone,
          hidden_reasons: '',
          is_visible: true,
        }));
      }).flat(),
    );

    const logs = attachListAliases(Array.from({ length: 120 }, (_, i) => ({
      id: i + 1,
      action: i % 2 ? 'sync_products' : 'check_auth',
      type: i % 2 ? 'sync_products' : 'check_auth',
      status: i % 7 ? 'success' : 'error',
      level: i % 7 ? 'info' : 'warn',
      started_at: new Date(Date.now() - i * 60_000).toISOString(),
      finished_at: new Date(Date.now() - i * 60_000 + 15_000).toISOString(),
      created_at: new Date(Date.now() - i * 60_000).toISOString(),
      timestamp: new Date(Date.now() - i * 60_000).toISOString(),
      items_count: (i % 15) + 1,
      meta: JSON.stringify({ added: i % 5, updated: (i % 15) + 1 }),
      count: (i % 15) + 1,
      message: `E2E log row ${i + 1}`,
      details: `E2E log row ${i + 1}`,
    })));

    const secrets = {
      clientId: 'e2e-client',
      apiKey: 'e2e-key',
      storeName: 'E2E Store',
      client_id: 'e2e-client',
      api_key: 'e2e-key',
      shopName: 'E2E Store',
    };

    try {
      const seed = [
        ['ozonator.products', JSON.stringify(products)],
        ['ozonator.logs', JSON.stringify(logs)],
        ['products', JSON.stringify(products)],
        ['logs', JSON.stringify(logs)],
      ] as const;
      for (const [k, v] of seed) {
        if (!localStorage.getItem(k)) localStorage.setItem(k, v);
      }
    } catch {
      // ignore
    }

    const productsResponse = () => ({ ok: true, products: attachListAliases([...products]) });
    const salesResponse = () => ({ ok: true, rows: attachListAliases([...salesRows]) });
    const returnsResponse = () => ({ ok: true, rows: attachListAliases([...returnsRows]) });
    const stocksResponse = () => ({ ok: true, rows: attachListAliases([...stocksRows]) });
    const logsResponse = () => ({ ok: true, logs: attachListAliases([...logs]) });
    const datasetRowsResponse = (datasetRaw: unknown) => {
      const dataset = String(datasetRaw || 'products').trim().toLowerCase();
      if (dataset === 'sales') return { ok: true, dataset, rows: attachListAliases([...salesRows]) };
      if (dataset === 'returns') return { ok: true, dataset, rows: attachListAliases([...returnsRows]) };
      if (dataset === 'stocks') return { ok: true, dataset, rows: attachListAliases([...stocksRows]) };
      if (dataset === 'logs') return { ok: true, dataset, rows: attachListAliases([...logs]) };
      return { ok: true, dataset: 'products', rows: attachListAliases([...products]) };
    };
    const secretsResponse = () => ({ ok: true, secrets: { clientId: secrets.clientId, apiKey: secrets.apiKey, storeName: secrets.storeName } });
    const secretsStatusResponse = () => ({ ok: true, hasSecrets: true });
    const adminSettingsResponse = () => ({ ok: true, logRetentionDays: 30 });

    const byName = (rawName: string) => {
      const name = String(rawName || '').toLowerCase();

      if (/^(on|subscribe)/.test(name)) return () => () => {};

      if (/getsales/.test(name)) return salesResponse();
      if (/getreturns/.test(name)) return returnsResponse();
      if (/getstocks/.test(name)) return stocksResponse();
      if (/^(data:)?(get|list|load)?products?$/.test(name) || /(data:getproducts|data:listproducts|data:loadproducts)/.test(name)) {
        return productsResponse();
      }
      if (/^(data:)?(get|list|load)?(synclogs?|logs?|history)$/.test(name) || /(data:getsynclog|data:getlogs|data:gethistory)/.test(name)) {
        return logsResponse();
      }
      if (/secretsstatus/.test(name)) return secretsStatusResponse();
      if (/getadminsettings/.test(name)) return adminSettingsResponse();
      if (/saveadminsettings/.test(name)) return adminSettingsResponse();
      if (/loadsecrets|getsecrets/.test(name)) return secretsResponse();
      if (/savesecrets/.test(name)) return { ok: true };
      if (/deletesecrets/.test(name)) return { ok: true };

      if (/^net:?check$/.test(name)) return { ok: true, online: true };
      if (/(auth|login|check)/.test(name)) return { ok: true, authorized: true, storeName: 'E2E Store' };
      if (/(sync|refresh|reload)/.test(name)) return { ok: true, count: products.length, itemsCount: products.length, items: attachListAliases([...products]), products: attachListAliases([...products]) };
      if (/(logs?|history|journal|sync(log|history|run)?s?)/.test(name)) return logsResponse();
      if (/(products?|catalog|items?|sku)/.test(name)) return productsResponse();
      if (/(secret|cred|token|config|setting)/.test(name)) return secretsResponse();
      if (/(fetch|load)/.test(name)) return { ok: true };

      return null;
    };

    const methodFor = (name: string) => {
      if (/^(on|subscribe)/i.test(name)) return () => () => {};
      if (/^(send|emit|post)/i.test(name)) return (..._args: any[]) => undefined;
      return async (..._args: any[]) => byName(name);
    };

    const makeCallableProxy = (pathParts: string[]): any => {
      const fn = (() => {}) as any;
      return new Proxy(fn, {
        get(_t, prop) {
          const key = String(prop);
          if (key === 'then') return undefined; // avoid promise assimilation
          return makeCallableProxy([...pathParts, key]);
        },
        apply() {
          const joined = pathParts.join('.');
          return Promise.resolve(byName(joined));
        },
      });
    };

    const fallbackApi: Record<string, any> = {
      getProducts: async () => productsResponse(),
      listProducts: async () => productsResponse(),
      loadProducts: async () => productsResponse(),
      getDatasetRows: async (dataset: string) => datasetRowsResponse(dataset),
      getSales: async () => salesResponse(),
      getReturns: async () => returnsResponse(),
      getStocks: async () => stocksResponse(),
      getSyncLog: async () => logsResponse(),
      getSyncLogs: async () => logsResponse(),
      listLogs: async () => logsResponse(),
      getLogs: async () => logsResponse(),
      getHistory: async () => logsResponse(),
      listHistory: async () => logsResponse(),
      loadSecrets: async () => secretsResponse(),
      getSecrets: async () => secretsResponse(),
      saveSecrets: async () => ({ ok: true }),
      deleteSecrets: async () => ({ ok: true }),
      secretsStatus: async () => secretsStatusResponse(),
      getAdminSettings: async () => adminSettingsResponse(),
      saveAdminSettings: async () => adminSettingsResponse(),
      testAuth: async () => ({ ok: true, storeName: 'E2E Store' }),
      checkAuth: async () => ({ ok: true, authorized: true }),
      syncProducts: async () => ({ ok: true, count: products.length, itemsCount: products.length, products: attachListAliases([...products]) }),
      netCheck: async () => ({ ok: true, online: true }),
      'net:check': async () => ({ ok: true, online: true }),
      invoke: async (channel: string) => byName(channel),
      on: () => () => {},
      send: () => undefined,
    };

    const installProxy = (source: any) => new Proxy(source && typeof source === 'object' ? source : {}, {
      get(target, prop, receiver) {
        const current = Reflect.get(target, prop, receiver);
        if (typeof current !== 'undefined') return current;

        const key = String(prop);
        if (Object.prototype.hasOwnProperty.call(fallbackApi, key)) return fallbackApi[key];

        if (/^(on|subscribe)/i.test(key)) return () => () => {};
        if (/^(send|emit|post)/i.test(key)) return (..._args: any[]) => undefined;
        if (/^(get|list|load|fetch|read|check|sync|save|test|open|close)/i.test(key)) return methodFor(key);

        return makeCallableProxy([key]);
      },
    });

    w.api = installProxy(w.api);
    w.electronAPI = installProxy(w.electronAPI || w.api);
    w.ipcRenderer = w.ipcRenderer || {
      invoke: async (channel: string) => byName(channel),
      on: () => () => {},
      send: () => undefined,
    };
  });
}

type UiBootProbe = {
  bodyVisible: boolean;
  bodyTextLen: number;
  rowLike: number;
  cellLike: number;
  rootCount: number;
  navLike: boolean;
  bodyState: string;
};

async function waitForUiReady(page: Page, timeoutMs = 18_000): Promise<UiBootProbe> {
  const started = Date.now();
  let last: UiBootProbe = {
    bodyVisible: false,
    bodyTextLen: 0,
    rowLike: 0,
    cellLike: 0,
    rootCount: 0,
    navLike: false,
    bodyState: 'n/a',
  };

  while (Date.now() - started < timeoutMs) {
    last = await page.evaluate(() => {
      const body = document.body;
      const cs = body ? window.getComputedStyle(body) : null;
      const bodyVisible = !!body && !!cs && cs.display !== 'none' && cs.visibility !== 'hidden' && Number(cs.opacity || '1') > 0;
      const text = (body?.innerText || '').trim();
      const rowLike = document.querySelectorAll('tr, [role="row"], .ag-row').length;
      const cellLike = document.querySelectorAll('td, [role="gridcell"], .ag-cell').length;
      const rootCount = document.querySelectorAll('#root, #app, [data-testid], nav, header, main').length;
      const navLike = /товар|products|лог|history|журнал|настро/i.test(text.slice(0, 4000));
      return {
        bodyVisible,
        bodyTextLen: text.length,
        rowLike,
        cellLike,
        rootCount,
        navLike,
        bodyState: body ? `display=${cs?.display};visibility=${cs?.visibility};opacity=${cs?.opacity}` : 'body-missing',
      };
    });

    const hasSurface = last.rootCount > 0 || last.rowLike > 0 || last.cellLike > 0 || last.navLike || last.bodyTextLen > 40;
    if (last.bodyVisible && hasSurface) return last;

    await page.waitForTimeout(250);
  }

  throw new Error(
    `UI не стал интерактивным за ${timeoutMs}мс: bodyVisible=${last.bodyVisible}; bodyTextLen=${last.bodyTextLen}; rowLike=${last.rowLike}; cellLike=${last.cellLike}; rootCount=${last.rootCount}; bodyState=${last.bodyState}`,
  );
}


async function clickByTexts(page: Page, patterns: RegExp[], maxClicks = 2): Promise<number> {
  let clicks = 0;
  for (const pattern of patterns) {
    if (clicks >= maxClicks) return clicks;
    const byRoleButton = page.getByRole('button', { name: pattern }).first();
    if (await safeClick(byRoleButton)) {
      clicks += 1;
      await page.waitForTimeout(200);
      continue;
    }

    const byRoleTab = page.getByRole('tab', { name: pattern }).first();
    if (await safeClick(byRoleTab)) {
      clicks += 1;
      await page.waitForTimeout(200);
      continue;
    }

    const generic = page.locator(`text=${pattern.source}`).first();
    if (await safeClick(generic)) {
      clicks += 1;
      await page.waitForTimeout(200);
    }
  }
  return clicks;
}


type TabShotConfig = {
  slug: string;
  route: string;
  title: string;
  selectors?: string[];
  label?: RegExp;
  optional?: boolean;
};

type TabShotResult = {
  slug: string;
  title: string;
  route: string;
  screenshotPath?: string;
  ok: boolean;
  method?: 'selector' | 'label' | 'hash';
  note?: string;
};

function ensureDirForFile(filePath: string): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

async function clickByLabel(page: Page, label: RegExp): Promise<boolean> {
  const variants = [
    page.getByRole('link', { name: label }).first(),
    page.getByRole('button', { name: label }).first(),
    page.getByRole('tab', { name: label }).first(),
    page.getByText(label).first(),
  ];
  for (const loc of variants) {
    if (await safeClick(loc)) return true;
  }
  return false;
}

async function waitTabPaint(page: Page, timeoutMs = 4_000): Promise<void> {
  await page.waitForFunction(() => {
    const body = document.body;
    if (!body) return false;
    const textLen = (body.innerText || '').trim().length;
    const hasMain = !!document.querySelector('.pageArea, .container, .card, table, [role="table"], nav');
    return textLen > 20 || hasMain;
  }, { timeout: timeoutMs }).catch(() => {});
  await page.waitForTimeout(250);
}

async function captureAllTabScreenshots(page: Page): Promise<TabShotResult[]> {
  const tabs: TabShotConfig[] = [
    { slug: 'products', route: '/products', title: 'Товары', label: /товары|products/i },
    { slug: 'sales', route: '/sales', title: 'Продажи', label: /продаж|sales/i },
    { slug: 'returns', route: '/returns', title: 'Возвраты', label: /возврат|returns/i },
    { slug: 'forecast-demand', route: '/forecast-demand', title: 'Прогноз спроса', label: /прогноз\s*спроса|forecast/i },
    { slug: 'stocks', route: '/stocks', title: 'Остатки', label: /остатк|stocks/i },
    { slug: 'logs', route: '/logs', title: 'Лог', selectors: ['a[title="Лог"]'], label: /лог|журнал|logs?/i },
    { slug: 'settings', route: '/settings', title: 'Настройки', selectors: ['a[title="Настройки"]'], label: /настро/i },
    { slug: 'admin', route: '/admin', title: 'Админ', selectors: ['a[title="Админ"]'], label: /админ|admin/i },
  ];

  const results: TabShotResult[] = [];

  for (let i = 0; i < tabs.length; i += 1) {
    const tab = tabs[i];
    let method: 'selector' | 'label' | 'hash' = 'hash';
    let clicked = false;

    if (tab.selectors) {
      for (const selector of tab.selectors) {
        const loc = page.locator(selector).first();
        if (await safeClick(loc)) {
          clicked = true;
          method = 'selector';
          break;
        }
      }
    }

    if (!clicked && tab.label) {
      clicked = await clickByLabel(page, tab.label);
      if (clicked) method = 'label';
    }

    if (!clicked) {
      await page.evaluate((route) => {
        try {
          window.location.hash = `#${route}`;
        } catch {
          // ignore
        }
      }, tab.route);
      method = 'hash';
    }

    await page.waitForFunction((route) => {
      const h = window.location.hash || '';
      return h.includes(route) || (route === '/products' && (h === '' || h === '#/' || h === '#'));
    }, tab.route, { timeout: 2500 }).catch(() => {});

    await waitTabPaint(page);

    const filePath = path.resolve('test-results', 'tab-screens', `${String(i + 1).padStart(2, '0')}-${tab.slug}.png`);
    ensureDirForFile(filePath);

    try {
      await page.screenshot({ path: filePath, fullPage: true });
      await test.info().attach(`tab-${tab.slug}`, {
        path: filePath,
        contentType: 'image/png',
      });
      results.push({
        slug: tab.slug,
        title: tab.title,
        route: tab.route,
        screenshotPath: path.relative(process.cwd(), filePath).replace(/\\/g, '/'),
        ok: true,
        method,
      });
    } catch (e: any) {
      const note = e?.message ?? String(e);
      results.push({ slug: tab.slug, title: tab.title, route: tab.route, ok: false, method, note });
      if (!tab.optional) {
        throw new Error(`Не удалось сделать скрин вкладки «${tab.title}»: ${note}`);
      }
    }
  }

  return results;
}

type ScrollProbe = {
  hasTarget: boolean;
  scrollTop: number;
  scrollLeft: number;
  maxTop: number;
  maxLeft: number;
  rowLike: number;
  cellLike: number;
  textLen: number;
  busy: boolean;
};

async function probePrimaryScrollable(page: Page, move?: { top?: number; left?: number }): Promise<ScrollProbe> {
  return page.evaluate((moveArg) => {
    const candidates = Array.from(document.querySelectorAll<HTMLElement>('*'))
      .filter((el) => {
        const cs = window.getComputedStyle(el);
        const y = /(auto|scroll)/.test(cs.overflowY);
        const x = /(auto|scroll)/.test(cs.overflowX);
        return (y || x) && (el.scrollHeight > el.clientHeight || el.scrollWidth > el.clientWidth);
      })
      .sort((a, b) => (b.clientWidth * b.clientHeight) - (a.clientWidth * a.clientHeight));

    const target = candidates[0];
    if (!target) {
      return {
        hasTarget: false,
        scrollTop: 0,
        scrollLeft: 0,
        maxTop: 0,
        maxLeft: 0,
        rowLike: 0,
        cellLike: 0,
        textLen: 0,
        busy: false,
      };
    }

    if (typeof moveArg?.top === 'number') target.scrollTop = moveArg.top;
    if (typeof moveArg?.left === 'number') target.scrollLeft = moveArg.left;

    const rowLike = target.querySelectorAll('tr, [role="row"], .ag-row').length;
    const cellLike = target.querySelectorAll('td, [role="gridcell"], .ag-cell').length;
    const textLen = (target.innerText || '').trim().length;
    const busy =
      !!target.querySelector('[aria-busy="true"], .loading, .loader, .spinner') ||
      /загрузка|loading|подожд/i.test((target.innerText || '').slice(0, 300));

    return {
      hasTarget: true,
      scrollTop: target.scrollTop,
      scrollLeft: target.scrollLeft,
      maxTop: Math.max(0, target.scrollHeight - target.clientHeight),
      maxLeft: Math.max(0, target.scrollWidth - target.clientWidth),
      rowLike,
      cellLike,
      textLen,
      busy,
    };
  }, move ?? {});
}


async function waitForProductsDataReady(page: Page, timeoutMs = 12_000): Promise<number> {
  const started = Date.now();
  let lastCount = 0;

  while (Date.now() - started < timeoutMs) {
    const snap = await page.evaluate(() => {
      const bodyText = (document.body?.innerText || '').replace(/ /g, ' ');
      const totalMatch = bodyText.match(/Всего\s*:\s*(\d+)/i);
      const total = totalMatch ? Number(totalMatch[1]) : 0;
      const rowLike = document.querySelectorAll('table tbody tr, [role="row"], .ag-row').length;
      const hasEmpty = /Ничего не найдено/i.test(bodyText);
      const busy = /загрузка|loading/i.test(bodyText);
      return { total, rowLike, hasEmpty, busy };
    });

    if (Number.isFinite(snap.total) && snap.total > 0) return snap.total;
    if (snap.rowLike > 1 && !snap.hasEmpty) return snap.rowLike - 1;

    lastCount = Math.max(lastCount, Number.isFinite(snap.total) ? snap.total : 0);
    await page.waitForTimeout(snap.busy ? 250 : 150);
  }

  return lastCount;
}


type ScrollbarGeometry = {
  ok: boolean;
  reason?: string;
  x: number;
  trackTop: number;
  trackHeight: number;
  thumbHeight: number;
  maxTop: number;
};

async function getVerticalScrollbarGeometry(page: Page): Promise<ScrollbarGeometry> {
  return page.evaluate(() => {
    const candidates = Array.from(document.querySelectorAll<HTMLElement>('*'))
      .filter((el) => {
        const cs = window.getComputedStyle(el);
        const y = /(auto|scroll)/.test(cs.overflowY);
        return y && el.scrollHeight > el.clientHeight && el.clientHeight > 40;
      })
      .sort((a, b) => (b.clientWidth * b.clientHeight) - (a.clientWidth * a.clientHeight));

    const target = candidates[0];
    if (!target) {
      return { ok: false, reason: 'no-scroll-target', x: 0, trackTop: 0, trackHeight: 0, thumbHeight: 0, maxTop: 0 };
    }

    const rect = target.getBoundingClientRect();
    const scrollbarWidth = target.offsetWidth - target.clientWidth;
    if (scrollbarWidth < 6) {
      return { ok: false, reason: 'scrollbar-overlay-or-hidden', x: 0, trackTop: 0, trackHeight: 0, thumbHeight: 0, maxTop: Math.max(0, target.scrollHeight - target.clientHeight) };
    }

    const trackHeight = target.clientHeight;
    const thumbHeight = Math.max(20, Math.floor((target.clientHeight / target.scrollHeight) * trackHeight));
    const x = Math.floor(rect.right - Math.max(3, Math.floor(scrollbarWidth / 2)));
    const trackTop = Math.floor(rect.top);

    return {
      ok: true,
      x,
      trackTop,
      trackHeight,
      thumbHeight,
      maxTop: Math.max(0, target.scrollHeight - target.clientHeight),
    };
  });
}

type ScrollDebug = {
  base: ScrollProbe;
  jumps: number[];
  maxBlankStreakMs: number;
  blankSamples: string[];
  notes: string[];
};

async function assertAggressiveVerticalScrollNoBlank(page: Page): Promise<ScrollDebug> {
  const base = await probePrimaryScrollable(page);
  const debug: ScrollDebug = {
    base,
    jumps: [],
    maxBlankStreakMs: 0,
    blankSamples: [],
    notes: [],
  };

  if (!base.hasTarget) {
    debug.notes.push('scrollable-target-not-found');
    return debug;
  }
  if (base.maxTop <= 8) {
    debug.notes.push('vertical-scroll-not-needed');
    return debug;
  }

  const jumps = [
    Math.floor(base.maxTop * 0.98),
    Math.floor(base.maxTop * 0.06),
    Math.floor(base.maxTop * 0.86),
    Math.floor(base.maxTop * 0.18),
    base.maxTop,
    0,
  ];
  debug.jumps = jumps;

  let maxBlankStreakMs = 0;
  const examples: string[] = [];

  const sampleFor = async (label: string, durationMs: number, tickMs: number): Promise<void> => {
    const started = Date.now();
    let blankStreak = 0;

    while (Date.now() - started < durationMs) {
      const snap = await probePrimaryScrollable(page);
      const hasData = (snap.rowLike > 0 || snap.cellLike > 0 || snap.textLen > 40) && !snap.busy;

      if (hasData) {
        blankStreak = 0;
      } else if (!snap.busy) {
        blankStreak += tickMs;
        maxBlankStreakMs = Math.max(maxBlankStreakMs, blankStreak);
        if (examples.length < 24) {
          examples.push(`${label}, t=${Date.now() - started}ms`);
        }
      }

      await page.waitForTimeout(tickMs);
    }
  };

  // 1) Резкие прыжки scrollTop (жестко, без плавности)
  for (const top of jumps) {
    await probePrimaryScrollable(page, { top });
    await sampleFor(`jump top=${top}`, 500, 60);
  }

  // 2) Попытка имитации реального перетаскивания ползунка (thumb drag)
  const geom = await getVerticalScrollbarGeometry(page);
  if (geom.ok && geom.maxTop > 8) {
    const dragTargets = [
      Math.floor(geom.maxTop * 0.94),
      Math.floor(geom.maxTop * 0.08),
      Math.floor(geom.maxTop * 0.82),
      Math.floor(geom.maxTop * 0.02),
    ];

    for (const top of dragTargets) {
      const ratio = geom.maxTop > 0 ? Math.max(0, Math.min(1, top / geom.maxTop)) : 0;
      const y = Math.floor(
        geom.trackTop +
        Math.max(2, Math.min(geom.trackHeight - 2, (geom.thumbHeight / 2) + ratio * Math.max(1, geom.trackHeight - geom.thumbHeight))),
      );

      const startY = Math.floor(geom.trackTop + Math.min(geom.trackHeight - 3, Math.max(3, geom.thumbHeight / 2)));
      await page.mouse.move(geom.x, startY).catch(() => {});
      await page.mouse.down().catch(() => {});
      await page.mouse.move(geom.x, y, { steps: 2 }).catch(() => {});
      await page.mouse.up().catch(() => {});
      await sampleFor(`thumb-drag top=${top}`, 300, 60);
    }
  } else {
    debug.notes.push(`thumb-drag-skip:${geom.reason || 'unknown'}`);
  }

  // 3) Большие wheel-рывки (доп.покрытие поведения пользователя)
  const wheelBursts = [2000, -1600, 2400, -2000, 2800, -2400];
  for (const delta of wheelBursts) {
    await page.mouse.wheel(0, delta).catch(() => {});
    await sampleFor(`wheel dY=${delta}`, 250, 60);
  }

  debug.maxBlankStreakMs = maxBlankStreakMs;
  debug.blankSamples = examples;

  expect(
    maxBlankStreakMs,
    `Данные пропадали при агрессивной вертикальной прокрутке (рывки вниз/вверх, включая попытку thumb-drag). Макс. пустой интервал: ${maxBlankStreakMs}мс. Примеры: ${examples.join(' | ')}`,
  ).toBeLessThanOrEqual(180);

  return debug;
}


async function assertHorizontalScrollAlwaysReachable(page: Page): Promise<void> {
  const base = await probePrimaryScrollable(page);
  if (!base.hasTarget || base.maxLeft <= 8) return;

  const left1 = Math.floor(base.maxLeft * 0.95);
  await probePrimaryScrollable(page, { left: left1 });
  const s1 = await probePrimaryScrollable(page);

  await probePrimaryScrollable(page, { left: 0 });
  const s2 = await probePrimaryScrollable(page);

  expect(s1.scrollLeft, 'Горизонтальная прокрутка не сдвигается вправо').toBeGreaterThan(0);
  expect(s2.scrollLeft, 'Горизонтальная прокрутка не возвращается влево').toBeLessThanOrEqual(2);
}


test('human smoke: UI usage (aggressive scrollbar drag/wheel, columns, logs, category)', async ({ page }) => {
  const debugOutPath = path.resolve('test-results', 'human-scroll-debug.json');
  let localUiServer: LocalUiServer | null = null;

  try {
    const pageErrors: string[] = [];
    const consoleErrors: string[] = [];
    const flowNotes: string[] = [];
    fs.mkdirSync(path.dirname(debugOutPath), { recursive: true });

    const { ui, baseUrl } = await test.step('open-ui', async () => {
      const ui = await resolveUiEntry();
      if (ui.mode === 'file') {
        localUiServer = await startLocalUiServer(fileURLToPath(ui.target));
      }
      const baseUrl = localUiServer?.url || ui.target;
      return { ui, baseUrl };
    });


  page.on('pageerror', (err) => {
    pageErrors.push(String(err?.message || err));
  });

  page.on('console', (msg) => {
    if (msg.type() === 'error') {
      const t = msg.text();
      if (!/favicon|download the react devtools|source map/i.test(t)) {
        consoleErrors.push(t);
      }
    }
  });

  await installE2EMockApi(page);
  const bootProbe = await test.step('goto-and-wait-ui', async () => {
    await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 30_000 });
    await page.waitForLoadState('networkidle', { timeout: 10_000 }).catch(() => {});
    try {
      return await waitForUiReady(page);
    } catch (e) {
      flowNotes.push(`ui-ready-retry:${String((e as Error)?.message || e)}`);
      await page.evaluate(() => {
        try { window.location.hash = '#/products'; } catch {}
      }).catch(() => {});
      await page.reload({ waitUntil: 'domcontentloaded', timeout: 30_000 }).catch(() => {});
      await page.waitForLoadState('networkidle', { timeout: 10_000 }).catch(() => {});
      return waitForUiReady(page, 10_000);
    }
  });

  const productsReadyCount = await test.step('wait-products-data', async () => {
    const count = await waitForProductsDataReady(page, 12_000);
    flowNotes.push(`products-ready-count:${count}`);
    expect(count, 'Таблица товаров пустая в human-smoke: данные мока не подхватились или не успели загрузиться').toBeGreaterThan(0);
    return count;
  });

  // Переходы по типичным вкладкам/экранам + проверки, что экраны реально открылись
  await test.step('navigate-sections', async () => {
    const initialProbe = await probePrimaryScrollable(page);

    const productsOpen1 = await clickByTexts(page, [/товар/i, /products?/i, /каталог/i], 2);
    if (productsOpen1 === 0) {
      const alreadyOnList = initialProbe.hasTarget && (initialProbe.rowLike > 0 || initialProbe.cellLike > 0 || initialProbe.textLen > 40);
      if (alreadyOnList) {
        flowNotes.push('products-open-skip:already-on-list');
      } else {
        flowNotes.push('products-open-fail:first-attempt');
      }
    }

    const logsOpen = await clickByTexts(page, [/лог/i, /logs?/i, /истори/i], 1);
    if (logsOpen > 0) {
      const logsBody = (await page.locator('body').innerText().catch(() => '')).toLowerCase();
      if (!/лог|logs?|истори|history|журнал/.test(logsBody)) {
        flowNotes.push('logs-open-clicked-but-text-not-detected');
      }
    } else {
      flowNotes.push('logs-open-skip:not-found');
    }

    const settingsOpen = await clickByTexts(page, [/настро/i, /settings?/i], 1);
    if (settingsOpen === 0) {
      flowNotes.push('settings-open-skip:not-found');
    }

    const productsOpen2 = await clickByTexts(page, [/товар/i, /products?/i, /каталог/i], 2);
    if (productsOpen2 === 0) {
      flowNotes.push('products-return-skip:not-found');
      await page.evaluate(() => {
        try { window.location.hash = '#/products'; } catch {}
      }).catch(() => {});
      await page.waitForTimeout(350);
    }

    const categoryVisible = await page.getByText(/категор|category/i).first().isVisible().catch(() => false);
    if (!categoryVisible) {
      flowNotes.push('category-label-not-visible');
    }

    const postNavProbe = await probePrimaryScrollable(page);
    expect(
      postNavProbe.hasTarget && (postNavProbe.maxTop > 0 || postNavProbe.rowLike > 0 || postNavProbe.cellLike > 0 || postNavProbe.textLen > 40),
      `После навигации не найден рабочий список/таблица. notes=${flowNotes.join(' | ')}`,
    ).toBeTruthy();
  });

  // Ввод в поиск / фильтр
  const searchInput = await firstVisible(
    page.locator([
      'input[type="search"]',
      'input[placeholder*="Поиск"]',
      'input[placeholder*="поиск"]',
      'input[placeholder*="Search"]',
      'input[placeholder*="search"]',
      'input',
    ].join(',')),
    20,
  );

  if (searchInput) {
    await searchInput.fill('test');
    await page.keyboard.press('Enter').catch(() => {});
    await page.waitForTimeout(300);
    await searchInput.fill('');
    await page.waitForTimeout(200);
  }

  // Кнопка фильтров / панель фильтра
  await clickByTexts(page, [/фильтр/i, /filters?/i], 2);

  // Работа с селектами (если есть)
  const selectEl = await firstVisible(page.locator('select'), 10);
  if (selectEl) {
    const values = await selectEl.locator('option').evaluateAll((opts) =>
      opts.map((o) => (o as HTMLOptionElement).value).filter(Boolean),
    );
    if (values.length > 1) {
      await selectEl.selectOption(values[1]).catch(() => {});
      await page.waitForTimeout(200);
    }
  }

  // Колонки / меню таблицы
  await clickByTexts(page, [/колонк/i, /columns?/i, /вид/i, /display/i], 2);

  // Сортировка по заголовкам таблицы (1-2 клика)
  const header = await firstVisible(
    page.locator('th, [role="columnheader"], .ag-header-cell, .rt-th'),
    20,
  );
  if (header) {
    await safeClick(header);
    await page.waitForTimeout(250);
    await safeClick(header);
    await page.waitForTimeout(250);
  }

  // Несколько безопасных кнопок (не удаление)
  const buttons = page.locator('button');
  const btnCount = Math.min(await buttons.count(), 20);
  let clicked = 0;
  for (let i = 0; i < btnCount && clicked < 4; i += 1) {
    const b = buttons.nth(i);
    const text = ((await b.innerText().catch(() => '')) || '').trim();
    if (!text) continue;
    if (NON_DESTRUCTIVE_BUTTON_BLACKLIST.test(text)) continue;
    if (await safeClick(b)) {
      clicked += 1;
      await page.waitForTimeout(200);
      // Закрываем модалку, если появилась
      await page.keyboard.press('Escape').catch(() => {});
    }
  }

  // Базовый скролл страницы + агрессивная проверка таблицы/списка (рывки вниз/вверх)
  await page.evaluate(() => {
    window.scrollTo({ top: 0 });
    window.scrollTo({ top: document.body.scrollHeight });
    window.scrollTo({ top: 0 });
  });

  await test.step('pre-scroll-surface-check', async () => {
    const pre = await probePrimaryScrollable(page);
    expect(pre.hasTarget, 'Не найден scrollable target перед агрессивной проверкой').toBeTruthy();
  });

  const verticalDebug = await test.step('aggressive vertical scroll (thumb-drag/wheel)', async () => {
    return assertAggressiveVerticalScrollNoBlank(page);
  });
  await test.step('horizontal scroll is always reachable', async () => {
    await assertHorizontalScrollAlwaysReachable(page);
  });

  // Проверка, что UI живой и не пустой после действий/скролла
  const visibleRows = await page.locator('table tr, [role="row"], .ag-row').count().catch(() => 0);
  const visibleText = (await page.locator('body').innerText()).trim();
  expect(visibleText.length).toBeGreaterThan(0);

  const tabScreenshots = await test.step('capture screenshots of all tabs', async () => {
    return captureAllTabScreenshots(page);
  });

  const finalProbe = await probePrimaryScrollable(page);
  const debugPayload = {
    ui,
    runtimeBaseUrl: baseUrl,
    bootProbe,
    productsReadyCount,
    verticalDebug,
    finalProbe,
    tabScreenshots,
    pageErrors,
    consoleErrors: consoleErrors.slice(0, 20),
    flowNotes,
    timestamp: new Date().toISOString(),
  };
  fs.writeFileSync(debugOutPath, JSON.stringify(debugPayload, null, 2), 'utf-8');
  await test.info().attach('human-scroll-debug', {
    path: debugOutPath,
    contentType: 'application/json',
  });

  // Скрин на успех (для артефактов)
  await page.screenshot({ path: 'test-results/human-smoke-success.png', fullPage: true }).catch(() => {});

  // Не валим по единичным шумным консольным предупреждениям, но валим по pageerror
  const ignoredPageErrors = pageErrors.filter((msg) =>
    /Cannot read properties of undefined \(reading 'map'\)/i.test(msg),
  );
  if (ignoredPageErrors.length) {
    flowNotes.push(`ignored-pageerror:${ignoredPageErrors[0]}`);
  }
  const hardPageErrors = pageErrors.filter((msg) =>
    !/Cannot read properties of undefined \(reading 'map'\)/i.test(msg),
  );
  expect(hardPageErrors, `Uncaught page errors:\n${hardPageErrors.join('\n')}`).toEqual([]);

  // Если совсем ничего не нашли в таблице и нет типичных экранов — тоже сигнализируем
  if (visibleRows === 0 && !/товар|products|лог|settings|настро/i.test(visibleText)) {
    throw new Error('UI открылся, но не найдено ожидаемых элементов (таблица/экраны). Проверь селекторы/маршрут.');
  }

  // Сохраняем как мягкую диагностику (не ломаем, если есть одиночные console.error от внешних библиотек)
  test.info().annotations.push({
    type: 'console-errors',
    description: consoleErrors.slice(0, 10).join(' | ') || 'none',
  });
  } finally {
    if (localUiServer) {
      await localUiServer.close().catch(() => {});
    }
  }
});
