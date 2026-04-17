# Ozonator — MVP‑0 (v0.1)

Исходники Ozonator: desktop-приложение для Windows/macOS, локальное защищённое хранение ключей и загрузка данных из Ozon Seller API в рамках текущей сессии приложения.

## Требования
- Node.js 20+
- npm

## Запуск в режиме разработки
```bash
npm install
npm run dev
```

## Сборка инсталлятора
```bash
npm run dist
```

На Windows сборка создаёт NSIS-инсталлятор `Ozonator.exe`. Для кросс‑сборки используйте CI (GitHub Actions).

## Где лежат данные
- Ключи: локально, в зашифрованном виде через Electron `safeStorage` (файл `secrets.json`).
- Локальная SQLite-база не используется: рабочие данные Ozon держатся только в памяти текущей сессии приложения.

Каталог служебного хранения по умолчанию:
- Windows: `%APPDATA%\Ozonator`
- macOS: `~/Library/Application Support/Ozonator`
