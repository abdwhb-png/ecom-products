(function initFastFashionAuth() {
  const API_TOKEN_STORAGE_KEY = 'fast-fashion-api-token';
  const LEGACY_STORAGE = window.localStorage;
  const SESSION_STORAGE = window.sessionStorage;

  function normalizeToken(value) {
    return String(value || '').trim();
  }

  function migrateLegacyToken() {
    const sessionToken = normalizeToken(SESSION_STORAGE.getItem(API_TOKEN_STORAGE_KEY));
    const legacyToken = normalizeToken(LEGACY_STORAGE.getItem(API_TOKEN_STORAGE_KEY));
    if (!sessionToken && legacyToken) {
      SESSION_STORAGE.setItem(API_TOKEN_STORAGE_KEY, legacyToken);
    }
    if (legacyToken) {
      LEGACY_STORAGE.removeItem(API_TOKEN_STORAGE_KEY);
    }
  }

  function readToken() {
    migrateLegacyToken();
    return normalizeToken(SESSION_STORAGE.getItem(API_TOKEN_STORAGE_KEY));
  }

  function writeToken(value) {
    const token = normalizeToken(value);
    if (token) {
      SESSION_STORAGE.setItem(API_TOKEN_STORAGE_KEY, token);
    } else {
      SESSION_STORAGE.removeItem(API_TOKEN_STORAGE_KEY);
    }
    LEGACY_STORAGE.removeItem(API_TOKEN_STORAGE_KEY);
    return token;
  }

  function clearToken() {
    SESSION_STORAGE.removeItem(API_TOKEN_STORAGE_KEY);
    LEGACY_STORAGE.removeItem(API_TOKEN_STORAGE_KEY);
  }

  function buildHeaders(extraHeaders) {
    const headers = { ...(extraHeaders || {}) };
    const token = readToken();
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
    return headers;
  }

  function maskToken(value) {
    const raw = normalizeToken(value);
    if (!raw) return '—';
    if (raw.length <= 10) return `${raw.slice(0, 2)}***${raw.slice(-2)}`;
    return `${raw.slice(0, 4)}***${raw.slice(-4)}`;
  }

  window.FastFashionAuth = Object.freeze({
    storageKey: API_TOKEN_STORAGE_KEY,
    readToken,
    writeToken,
    clearToken,
    buildHeaders,
    maskToken,
  });
})();
