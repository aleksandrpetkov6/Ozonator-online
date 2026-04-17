# Copilot instructions for Ozonator (Electron + Vite + TypeScript)

## Definition of Done (hard)
- `windows-ci` workflow is **green**.
- Artifacts contain:
  - `Ozonator.exe` (installer)
  - `portable-win-unpacked.zip` (portable)
- No "local build" instructions for the user. All validation happens in GitHub Actions.

## Project build / CI facts
- Use **Node 20 LTS**.
- CI commands:
  - `npm ci`
  - `npm run lint`
  - `npm run typecheck`
  - `npm run dist`
- The installer must end up in `release/` and be collected into `artifacts/Ozonator.exe` by the workflow.

## How to work
- Make small, reviewable PRs.
- If CI fails: fix the root cause and rerun CI until it is green.
- Prefer changing code/config over relaxing checks.
- Avoid committing build outputs: `dist/`, `out/`, `release/`, `*.exe`, `*.blockmap`, `playwright-report/`, `test-results/`.
