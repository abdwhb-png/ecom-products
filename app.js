const query = new URLSearchParams(window.location.search);

const state = {
  datasets: [],
  currentDataset: query.get('dataset') || 'shein',
  search: (query.get('search') || '').trim().toLowerCase(),
  category: (query.get('category') || '').trim(),
  imagesOnly: ['1', 'true', 'yes', 'on'].includes((query.get('imagesOnly') || '').toLowerCase()),
  sort: query.get('sort') || 'relevance',
  page: Math.max(1, Number.parseInt(query.get('page') || '1', 10) || 1),
  pageSize: Math.max(1, Number.parseInt(query.get('pageSize') || '24', 10) || 24),
  currentPayload: null,
};

const els = {
  datasetSelect: document.getElementById('datasetSelect'),
  searchInput: document.getElementById('searchInput'),
  categorySelect: document.getElementById('categorySelect'),
  sortSelect: document.getElementById('sortSelect'),
  imagesOnlyToggle: document.getElementById('imagesOnlyToggle'),
  resetFiltersBtn: document.getElementById('resetFiltersBtn'),
  pageSizeSelect: document.getElementById('pageSizeSelect'),
  prevPageBtn: document.getElementById('prevPageBtn'),
  nextPageBtn: document.getElementById('nextPageBtn'),
  pageIndicator: document.getElementById('pageIndicator'),
  paginationMeta: document.getElementById('paginationMeta'),
  productGrid: document.getElementById('productGrid'),
  emptyState: document.getElementById('emptyState'),
  statusBanner: document.getElementById('statusBanner'),
  summaryBar: document.getElementById('summaryBar'),
  activeDatasetLabel: document.getElementById('activeDatasetLabel'),
  resultsCount: document.getElementById('resultsCount'),
  categoryCount: document.getElementById('categoryCount'),
  productCardTemplate: document.getElementById('productCardTemplate'),
  activeFilters: document.getElementById('activeFilters'),
};

async function init() {
  bindEvents();
  syncControlsFromState();
  await loadDatasets();
  await refreshUI();
}

function bindEvents() {
  els.datasetSelect.addEventListener('change', async (event) => {
    state.currentDataset = event.target.value;
    state.category = '';
    state.page = 1;
    await refreshUI();
  });

  els.searchInput.addEventListener('input', debounce(async (event) => {
    state.search = event.target.value.trim().toLowerCase();
    state.page = 1;
    await refreshUI();
  }, 250));

  els.categorySelect.addEventListener('change', async (event) => {
    state.category = event.target.value;
    state.page = 1;
    await refreshUI();
  });

  els.sortSelect.addEventListener('change', async (event) => {
    state.sort = event.target.value;
    state.page = 1;
    await refreshUI();
  });

  els.imagesOnlyToggle.addEventListener('change', async (event) => {
    state.imagesOnly = event.target.checked;
    state.page = 1;
    await refreshUI();
  });

  els.pageSizeSelect.addEventListener('change', async (event) => {
    state.pageSize = Math.max(1, Number.parseInt(event.target.value, 10) || 24);
    state.page = 1;
    await refreshUI();
  });

  els.prevPageBtn.addEventListener('click', async () => {
    state.page = Math.max(1, state.page - 1);
    await refreshUI();
  });

  els.nextPageBtn.addEventListener('click', async () => {
    state.page += 1;
    await refreshUI();
  });

  els.resetFiltersBtn.addEventListener('click', async () => {
    state.search = '';
    state.category = '';
    state.imagesOnly = false;
    state.sort = 'relevance';
    state.page = 1;
    state.pageSize = 24;
    syncControlsFromState();
    await refreshUI();
  });
}

async function loadDatasets() {
  updateStatus('Chargement des datasets…', 'info');
  const response = await fetch('/api/datasets');
  if (!response.ok) throw new Error(`Impossible de charger les datasets (${response.status})`);
  const payload = await response.json();
  state.datasets = (payload.datasets || []).filter((dataset) => ['shein', 'asos'].includes(dataset.id));
  if (!state.datasets.find((dataset) => dataset.id === state.currentDataset) && state.datasets.length) {
    state.currentDataset = state.datasets[0].id;
  }
  hydrateDatasetSelect();
}

function hydrateDatasetSelect() {
  const options = state.datasets
    .map((dataset) => `<option value="${dataset.id}">${escapeHtml(dataset.label)}</option>`)
    .join('');
  els.datasetSelect.innerHTML = options;
  els.datasetSelect.value = state.currentDataset;
}

function syncControlsFromState() {
  els.searchInput.value = state.search;
  els.sortSelect.value = state.sort;
  els.imagesOnlyToggle.checked = state.imagesOnly;
  els.pageSizeSelect.value = String(state.pageSize);
}

async function refreshUI() {
  syncControlsFromState();
  updateStatus('Chargement des produits…', 'info');
  const payload = await fetchProducts();
  state.currentPayload = payload;
  render(payload);
  updateStatus('Catalogue chargé localement.', 'success');
}

async function fetchProducts() {
  const params = new URLSearchParams({
    dataset: state.currentDataset,
    search: state.search,
    category: state.category,
    sort: state.sort,
    imagesOnly: String(state.imagesOnly),
    page: String(state.page),
    pageSize: String(state.pageSize),
  });

  const response = await fetch(`/api/products?${params.toString()}`);
  if (!response.ok) throw new Error(`Impossible de charger les produits (${response.status})`);
  const payload = await response.json();
  state.page = payload.pagination?.page || 1;
  return payload;
}

function render(payload) {
  const dataset = payload.dataset;
  const products = payload.products || [];
  const categories = payload.categories || [];
  const pagination = payload.pagination || { page: 1, totalPages: 1, total: 0, from: 0, to: 0 };

  els.activeDatasetLabel.textContent = dataset?.label || '—';
  els.resultsCount.textContent = new Intl.NumberFormat('fr-FR').format(pagination.total || 0);
  els.categoryCount.textContent = new Intl.NumberFormat('fr-FR').format(categories.length);

  fillCategorySelect(categories);
  renderSummary(dataset, categories, pagination);
  renderActiveFilters();
  renderPagination(pagination);
  renderProducts(products);
}

function fillCategorySelect(categories) {
  const options = ['<option value="">Toutes les catégories</option>']
    .concat(categories.map((category) => `<option value="${escapeHtml(category.name)}">${escapeHtml(category.name)} (${category.count})</option>`))
    .join('');
  els.categorySelect.innerHTML = options;
  els.categorySelect.value = categories.some((item) => item.name === state.category) ? state.category : '';
  if (!categories.some((item) => item.name === state.category)) state.category = '';
}

function renderSummary(dataset, categories, pagination) {
  els.summaryBar.innerHTML = `
    <span><strong>${new Intl.NumberFormat('fr-FR').format(dataset?.total_count || 0)}</strong> produits chargés</span>
    <span><strong>${new Intl.NumberFormat('fr-FR').format(pagination.total || 0)}</strong> résultats après filtres</span>
    <span><strong>${new Intl.NumberFormat('fr-FR').format(categories.length)}</strong> catégories repérées</span>
    <span><strong>${new Intl.NumberFormat('fr-FR').format(dataset?.with_images_count || 0)}</strong> fiches avec image</span>
    <span><strong>${new Intl.NumberFormat('fr-FR').format(dataset?.with_reviews_count || 0)}</strong> fiches avec avis > 0</span>
  `;
}

function renderActiveFilters() {
  const chips = [];
  const activeDataset = state.datasets.find((dataset) => dataset.id === state.currentDataset);
  if (activeDataset) chips.push(`Dataset: ${activeDataset.label}`);
  if (state.search) chips.push(`Recherche: ${state.search}`);
  if (state.category) chips.push(`Catégorie: ${state.category}`);
  if (state.imagesOnly) chips.push('Images seulement');
  if (state.sort !== 'relevance') chips.push(`Tri: ${els.sortSelect.options[els.sortSelect.selectedIndex]?.text || state.sort}`);
  if (state.pageSize !== 24) chips.push(`Par page: ${state.pageSize}`);

  if (!chips.length) {
    els.activeFilters.classList.add('hidden');
    els.activeFilters.innerHTML = '';
    return;
  }

  els.activeFilters.classList.remove('hidden');
  els.activeFilters.innerHTML = chips.map((chip) => `<span class="filter-chip">${escapeHtml(chip)}</span>`).join('');
}

function renderPagination(pagination) {
  els.paginationMeta.textContent = `Affichage ${pagination.from || 0}-${pagination.to || 0} sur ${new Intl.NumberFormat('fr-FR').format(pagination.total || 0)} produits`;
  els.pageIndicator.textContent = `Page ${pagination.page || 1} / ${pagination.totalPages || 1}`;
  els.prevPageBtn.disabled = (pagination.page || 1) <= 1;
  els.nextPageBtn.disabled = (pagination.page || 1) >= (pagination.totalPages || 1);
}

function renderProducts(products) {
  els.productGrid.innerHTML = '';

  if (!products.length) {
    els.emptyState.classList.remove('hidden');
    return;
  }

  els.emptyState.classList.add('hidden');
  const fragment = document.createDocumentFragment();

  products.forEach((product) => {
    const node = els.productCardTemplate.content.cloneNode(true);
    const card = node.querySelector('.product-card');
    const mediaWrap = node.querySelector('.product-media-wrap');
    const img = node.querySelector('.product-media');
    const sourceBadge = node.querySelector('.badge-source');
    const categoryBadge = node.querySelector('.badge-category');
    const price = node.querySelector('.product-price');
    const title = node.querySelector('.product-title');
    const description = node.querySelector('.product-description');
    const meta = node.querySelector('.product-meta');
    const link = node.querySelector('.product-link');
    const prevBtn = node.querySelector('.carousel-btn-prev');
    const nextBtn = node.querySelector('.carousel-btn-next');
    const counter = node.querySelector('.carousel-counter');

    sourceBadge.textContent = product.source;
    categoryBadge.textContent = product.category || product.category_path || 'Sans catégorie';
    price.textContent = product.price_text || 'Prix non disponible';
    title.textContent = product.name;
    description.textContent = truncate(product.description || 'Pas de description fournie dans le dataset.', 220);

    const images = [product.image, ...(product.imageUrls || [])]
      .filter(Boolean)
      .filter((value, index, array) => array.indexOf(value) === index);

    if (images.length) {
      let imageIndex = 0;
      const setImage = () => {
        img.src = images[imageIndex];
        img.alt = `${product.name} (${imageIndex + 1}/${images.length})`;
        counter.textContent = `${imageIndex + 1} / ${images.length}`;
      };

      setImage();
      img.onerror = () => mediaWrap.classList.add('no-image');

      if (images.length > 1) {
        prevBtn.classList.remove('hidden');
        nextBtn.classList.remove('hidden');
        counter.classList.remove('hidden');
        prevBtn.addEventListener('click', () => {
          imageIndex = (imageIndex - 1 + images.length) % images.length;
          setImage();
        });
        nextBtn.addEventListener('click', () => {
          imageIndex = (imageIndex + 1) % images.length;
          setImage();
        });
      }
    } else {
      mediaWrap.classList.add('no-image');
    }

    const metaEntries = [
      ['Brand', product.brand || '—'],
      ['Couleur', product.color || '—'],
      ['Tailles', (product.sizes || []).join(', ') || product.size_text || '—'],
      ['Images', product.image_count ?? images.length ?? 0],
      ['Avis', product.reviews_count ?? '—'],
      ['Note', product.rating ?? '—'],
      ['Source', product.source || '—'],
    ];

    meta.innerHTML = metaEntries
      .filter(([, value]) => value !== '' && value !== null && value !== undefined && value !== '—')
      .slice(0, 6)
      .map(([label, value]) => `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(String(value))}</dd>`)
      .join('');

    if (product.url) {
      link.href = product.url;
    } else {
      link.removeAttribute('href');
      link.textContent = 'Pas de lien source';
      link.classList.add('disabled');
      card.classList.add('is-disabled');
    }

    fragment.appendChild(node);
  });

  els.productGrid.appendChild(fragment);
}

function updateStatus(message, tone = 'info') {
  els.statusBanner.className = `status-banner ${tone}`;
  els.statusBanner.textContent = message;
}

function truncate(value, maxLength = 180) {
  if (!value) return '';
  return value.length <= maxLength ? value : `${value.slice(0, maxLength).trim()}...`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function debounce(fn, delay = 200) {
  let timer = null;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

init().catch((error) => {
  console.error(error);
  updateStatus(`Erreur: ${error.message}`, 'error');
});
