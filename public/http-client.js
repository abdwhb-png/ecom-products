(function () {
  function buildUrl(path, query = null) {
    if (!query) return path;
    const params = new URLSearchParams();
    Object.entries(query).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return;
      params.set(key, String(value));
    });
    const suffix = params.toString();
    return suffix ? `${path}?${suffix}` : path;
  }

  function prepareBody(body, headers) {
    if (body === undefined || body === null) return undefined;
    if (typeof body === 'string' || body instanceof FormData || body instanceof Blob) {
      return body;
    }
    if (!headers['Content-Type']) {
      headers['Content-Type'] = 'application/json';
    }
    return JSON.stringify(body);
  }

  async function requestJson(path, options = {}) {
    const requestHeaders = window.FastFashionAuth.buildHeaders({ ...(options.headers || {}) });
    const response = await fetch(buildUrl(path, options.query), {
      method: options.method || 'GET',
      credentials: options.credentials || 'same-origin',
      headers: requestHeaders,
      body: prepareBody(options.body, requestHeaders),
    });
    const payload = await response.json().catch(() => null);
    return { response, payload };
  }

  window.FastFashionHttp = {
    buildUrl,
    requestJson,
  };
})();
