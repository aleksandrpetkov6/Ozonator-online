// src/renderer/testing/uiErrorBridge.ts

declare global {
  interface Window {
    __OZ_UI_TEST__?: {
      errors: string[];
      startedAt: number;
    };
  }
}

export function initUiErrorBridge() {
  if (typeof window === 'undefined') return;

  if (!window.__OZ_UI_TEST__) {
    window.__OZ_UI_TEST__ = {
      errors: [],
      startedAt: Date.now(),
    };
  }

  // Чтобы не вешать обработчики повторно
  const w = window as Window & { __OZ_UI_BRIDGE_READY__?: boolean };
  if (w.__OZ_UI_BRIDGE_READY__) return;
  w.__OZ_UI_BRIDGE_READY__ = true;

  window.addEventListener('error', (event) => {
    const msg = `[error] ${event.message || 'Unknown error'}`;
    window.__OZ_UI_TEST__?.errors.push(msg);
  });

  window.addEventListener('unhandledrejection', (event) => {
    const reason =
      typeof event.reason === 'string'
        ? event.reason
        : JSON.stringify(event.reason ?? 'unknown');
    const msg = `[unhandledrejection] ${reason}`;
    window.__OZ_UI_TEST__?.errors.push(msg);
  });
}
