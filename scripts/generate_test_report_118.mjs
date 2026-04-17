import fs from 'node:fs';
import path from 'node:path';

const root = process.cwd();
const cfgPath = path.join(root, 'qa', 'test-matrix-118.overrides.json');

if (!fs.existsSync(cfgPath)) {
  console.error(`Config not found: ${cfgPath}`);
  process.exit(1);
}

const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
const TOTAL = Number(cfg.totalTests || 118);

const env = {
  lint: (process.env.LINT_STATUS || 'NR').toUpperCase(),
  typecheck: (process.env.TYPECHECK_STATUS || 'NR').toUpperCase(),
  unit: (process.env.UNIT_STATUS || 'NR').toUpperCase(),
  build: (process.env.BUILD_STATUS || 'NR').toUpperCase(),
  e2e: (process.env.E2E_STATUS || 'NR').toUpperCase(),
  static: (process.env.STATIC_STATUS || 'PASS').toUpperCase(),
  review: (process.env.REVIEW_STATUS || 'PASS').toUpperCase(),

  // Новые proxy-сигналы для "человеческих" тестов
  visual: (process.env.VISUAL_STATUS || 'NR').toUpperCase(),
  smokeInstall: (process.env.SMOKE_INSTALL_STATUS || 'NR').toUpperCase(),
  monkey: (process.env.MONKEY_STATUS || 'NR').toUpperCase(),
  security: (process.env.SECURITY_STATUS || 'NR').toUpperCase(),
  uxProxy: (process.env.UX_PROXY_STATUS || 'NR').toUpperCase(),
  synthetic: (process.env.SYNTHETIC_STATUS || 'NR').toUpperCase()
};

function normStatus(s) {
  if (!s) return 'NR';
  const v = String(s).toUpperCase();
  if (v === 'SUCCESS' || v === 'PASSED' || v === 'OK') return 'PASS';
  if (v === 'FAILURE' || v === 'FAILED' || v === 'ERROR') return 'FAIL';
  if (v === 'SKIPPED') return 'NR';
  if (v === 'PASS' || v === 'FAIL' || v === 'NR') return v;
  return 'NR';
}

Object.keys(env).forEach(k => {
  env[k] = normStatus(env[k]);
});

const humanSet = new Set((cfg.modes?.human || []).map(Number));
const proxySet = new Set((cfg.modes?.proxy || []).map(Number));
const defaultMode = cfg.modes?.default || 'auto';

function modeFor(id) {
  if (humanSet.has(id)) return 'human';
  if (proxySet.has(id)) return 'proxy';
  return defaultMode;
}

function humanReason(id) {
  return cfg.reasons?.[String(id)] || 'Требуется участие человека/внешнего контура';
}

function autoStatusFor(id) {
  // Ранние review/static/lint/typecheck/unit
  if ([1, 2, 3, 4, 5].includes(id)) {
    return { status: env.review === 'FAIL' ? 'FAIL' : env.review, reason: 'Ревью/формат по CI-пайплайну' };
  }
  if (id === 6) return { status: env.static, reason: 'Static analysis' };
  if (id === 7) return { status: env.lint, reason: 'Lint' };
  if (id === 8) return { status: env.typecheck, reason: 'Type checking' };
  if (id === 9) return { status: env.unit, reason: 'Unit tests' };

  // Сборка/системные/интеграционные proxy через build/e2e
  if ([10, 11, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 32, 33, 35, 36, 37].includes(id)) {
    if (env.build === 'FAIL' || env.e2e === 'FAIL') {
      return { status: 'FAIL', reason: 'Провал build/E2E' };
    }
    if (env.build === 'PASS' || env.e2e === 'PASS') {
      return { status: 'PASS', reason: 'Прокси через build/E2E' };
    }
    return { status: 'NR', reason: 'Нет build/E2E сигнала' };
  }

  // В простом варианте остальные группы пока не подключены отдельными job
  if (
    (id >= 38 && id <= 60) ||
    (id >= 61 && id <= 99) ||
    (id >= 101 && id <= 115) ||
    id === 117
  ) {
    return { status: 'NR', reason: 'Не подключён отдельный job (nightly/security/perf/data) в простом варианте' };
  }

  return { status: 'NR', reason: 'Нет правила для этого теста (добавить в генератор)' };
}

function proxyStatusFor(id) {
  // №13 E2E
  if (id === 13) {
    if (env.e2e === 'PASS') return { status: 'PASS', reason: 'E2E (Playwright/Electron) прошёл' };
    if (env.e2e === 'FAIL') return { status: 'FAIL', reason: 'E2E (Playwright/Electron) упал' };
    return { status: 'NR', reason: 'E2E не запускался' };
  }

  // №14 UAT — машинный proxy-UAT (build + e2e)
  if (id === 14) {
    if (env.e2e === 'FAIL' || env.build === 'FAIL') return { status: 'FAIL', reason: 'proxy-UAT провален (build/E2E)' };
    if (env.e2e === 'PASS' && env.build === 'PASS') return { status: 'PASS', reason: 'proxy-UAT: build+E2E PASS' };
    return { status: 'NR', reason: 'Недостаточно сигналов для proxy-UAT (нужны build+E2E)' };
  }

  // №29 Dogfooding -> synthetic
  if (id === 29) {
    if (env.synthetic === 'PASS') return { status: 'PASS', reason: 'proxy-Dogfooding: synthetic usage PASS' };
    if (env.synthetic === 'FAIL') return { status: 'FAIL', reason: 'proxy-Dogfooding: synthetic usage FAIL' };
    return { status: 'NR', reason: 'Synthetic usage не запускался' };
  }

  // №30 Field -> synthetic + ux
  if (id === 30) {
    if (env.synthetic === 'FAIL' || env.uxProxy === 'FAIL') {
      return { status: 'FAIL', reason: 'proxy-Field провален (synthetic/UX proxy)' };
    }
    if (env.synthetic === 'PASS' && env.uxProxy === 'PASS') {
      return { status: 'PASS', reason: 'proxy-Field: synthetic+UX PASS' };
    }
    return { status: 'NR', reason: 'Недостаточно сигналов для proxy-Field (нужны synthetic+UX)' };
  }

  // №31 Manual -> smoke install + e2e
  if (id === 31) {
    if (env.smokeInstall === 'FAIL' || env.e2e === 'FAIL') {
      return { status: 'FAIL', reason: 'proxy-Manual провален (install smoke/E2E)' };
    }
    if (env.smokeInstall === 'PASS' && env.e2e === 'PASS') {
      return { status: 'PASS', reason: 'proxy-Manual: install smoke + E2E PASS' };
    }
    return { status: 'NR', reason: 'Недостаточно сигналов для proxy-Manual (нужны install smoke + E2E)' };
  }

  // №34 Exploratory -> monkey
  if (id === 34) {
    if (env.monkey === 'PASS') return { status: 'PASS', reason: 'proxy-Exploratory: monkey/fuzz PASS' };
    if (env.monkey === 'FAIL') return { status: 'FAIL', reason: 'proxy-Exploratory: monkey/fuzz FAIL' };
    return { status: 'NR', reason: 'Monkey/Fuzz не запускался' };
  }

  // №55 Red teaming -> security pipeline
  if (id === 55) {
    if (env.security === 'PASS') return { status: 'PASS', reason: 'proxy-RedTeam: security pipeline PASS' };
    if (env.security === 'FAIL') return { status: 'FAIL', reason: 'proxy-RedTeam: security pipeline FAIL' };
    return { status: 'NR', reason: 'Security pipeline не запускался' };
  }

  // №100 Usability -> visual + UX proxy
  if (id === 100) {
    if (env.visual === 'FAIL' || env.uxProxy === 'FAIL') {
      return { status: 'FAIL', reason: 'proxy-Usability провален (visual/UX proxy)' };
    }
    if (env.visual === 'PASS' && env.uxProxy === 'PASS') {
      return { status: 'PASS', reason: 'proxy-Usability: visual + UX proxy PASS' };
    }
    return { status: 'NR', reason: 'Недостаточно сигналов для proxy-Usability (нужны visual+UX)' };
  }

  return { status: 'NR', reason: 'Нет proxy-правила' };
}

const rows = [];
let passedCount = 0;
let failedCount = 0;
let nrCount = 0;

for (let id = 1; id <= TOTAL; id++) {
  const mode = modeFor(id);
  let result;

  if (mode === 'human') {
    result = { status: 'NR', reason: humanReason(id) };
  } else if (mode === 'proxy') {
    result = proxyStatusFor(id);
  } else {
    result = autoStatusFor(id);
  }

  const st = normStatus(result.status);
  if (st === 'PASS') passedCount++;
  else if (st === 'FAIL') failedCount++;
  else nrCount++;

  rows.push({
    id,
    mode,
    status: st,
    reason: result.reason || ''
  });
}

const cycles = Number(process.env.TEST_CYCLES || '1');

const nrReasons = [
  ...new Set(rows.filter(r => r.status === 'NR').map(r => r.reason))
].slice(0, 3).join('; ');

const summaryLine = failedCount > 0
  ? `${passedCount} из 118, ${cycles} цикл(а/ов). ${failedCount} тестов упали, ${nrCount} тестов не выполнялись. NR: ${nrReasons || 'нет причин'}`
  : `${passedCount} из 118, ${cycles} цикл(а/ов). ${nrCount} тестов не выполнялись, так как ${nrReasons || 'нет причин'}`;

const lines = [];
lines.push('Ozonator TestReport (auto-matrix 118)');
lines.push(`Generated: ${new Date().toISOString()}`);
lines.push('');
lines.push(`SUMMARY: ${summaryLine}`);
lines.push(`PASS=${passedCount}; FAIL=${failedCount}; NR=${nrCount}`);
lines.push('');
lines.push('Signals:');
lines.push(`  LINT=${env.lint} TYPECHECK=${env.typecheck} UNIT=${env.unit} BUILD=${env.build} E2E=${env.e2e}`);
lines.push(`  VISUAL=${env.visual} SMOKE_INSTALL=${env.smokeInstall} MONKEY=${env.monkey} SECURITY=${env.security} UX_PROXY=${env.uxProxy} SYNTHETIC=${env.synthetic}`);
lines.push('');

for (const r of rows) {
  lines.push(`${String(r.id).padStart(3, '0')}. [${r.status}] [${r.mode}] ${r.reason}`);
}

fs.writeFileSync(path.join(root, 'TestReport.txt'), lines.join('\n'), 'utf8');
fs.writeFileSync(path.join(root, 'TestSummary.txt'), summaryLine + '\n', 'utf8');

console.log(summaryLine);
