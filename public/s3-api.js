(function () {
  const http = window.FastFashionHttp;

  function filterDatasets(payload) {
    return (payload?.datasets || []).filter((dataset) => ['shein', 'asos'].includes(dataset.id));
  }

  async function loadDatasets() {
    const { response, payload } = await http.requestJson('/api/datasets');
    return { response, payload, datasets: filterDatasets(payload) };
  }

  async function fetchJobDetail(jobId, page, pageSize) {
    return http.requestJson(`/api/s3/jobs/${encodeURIComponent(jobId)}`, {
      query: { page, page_size: pageSize },
      credentials: 'include',
    });
  }

  async function cancelJob(jobId) {
    return http.requestJson(`/api/s3/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
      credentials: 'include',
    });
  }

  async function createFamilyJob(endpoint, body) {
    return http.requestJson(endpoint, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body,
    });
  }

  window.FastFashionS3Api = {
    loadDatasets,
    fetchJobDetail,
    cancelJob,
    createFamilyJob,
  };
})();
