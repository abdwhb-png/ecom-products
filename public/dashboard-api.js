(function () {
  const http = window.FastFashionHttp;

  function filterDatasets(payload) {
    return (payload?.datasets || []).filter((dataset) => ['shein', 'asos'].includes(dataset.id));
  }

  async function loadDatasets() {
    const { response, payload } = await http.requestJson('/api/datasets');
    return { response, payload, datasets: filterDatasets(payload) };
  }

  async function fetchProducts(filters) {
    return http.requestJson('/api/products', { query: filters });
  }

  async function fetchCategories(filters) {
    return http.requestJson('/api/categories', { query: filters });
  }

  async function fetchProductDetail({ datasetId, productId }) {
    return http.requestJson(`/api/products/${encodeURIComponent(productId)}`, {
      query: { dataset: datasetId },
    });
  }

  window.FastFashionDashboardApi = {
    loadDatasets,
    fetchProducts,
    fetchCategories,
    fetchProductDetail,
  };
})();
