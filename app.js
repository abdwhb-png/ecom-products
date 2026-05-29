const query = new URLSearchParams(window.location.search);
const API_TOKEN_STORAGE_KEY = 'fast-fashion-api-token';

const state = {
  apiToken: '',
  apiUnlocked: false,
  datasets: [],
  currentDataset: query.get('dataset') || 'shein',
  search: (query.get('search') || '').trim().toLowerCase(),
  category: (query.get('category') || '').trim(),
  imagesOnly: ['1', 'true', 'yes', 'on'].includes((query.get('imagesOnly') || '').toLowerCase()),
  savedOnS3: ['1', 'true', 'yes', 'on'].includes((query.get('savedOnS3') || '').toLowerCase()),
  sort: query.get('sort') || 'relevance',
  page: Math.max(1, Number.parseInt(query.get('page') || '1', 10) || 1),
  pageSize: Math.max(1, Number.parseInt(query.get('pageSize') || '24', 10) || 24),
  currentPayload: null,
  currentCategories: [],
  currentCategoryPagination: { page: 1, pageSize: 24, total: 0, totalPages: 1, from: 0, to: 0 },
  categoryPage: 1,
  categoryPageSize: 24,
  productDetailCache: {},
  selectedProductKey: null,
  isBusy: false,
};

const els = {
  datasetSelect: document.getElementById('datasetSelect'),
  searchInput: document.getElementById('searchInput'),
  categorySelect: document.getElementById('categorySelect'),
  sortSelect: document.getElementById('sortSelect'),
  imagesOnlyToggle: document.getElementById('imagesOnlyToggle'),
  savedOnS3Toggle: document.getElementById('savedOnS3Toggle'),
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
  datasetLoader: document.getElementById('datasetLoader'),
  contentLoader: document.getElementById('contentLoader'),
  contentLoaderTitle: document.getElementById('contentLoaderTitle'),
  contentLoaderText: document.getElementById('contentLoaderText'),
  catPrevBtn: document.getElementById('catPrevBtn'),
  catNextBtn: document.getElementById('catNextBtn'),
  catPageIndicator: document.getElementById('catPageIndicator'),
  productModalBackdrop: document.getElementById('productModalBackdrop'),
  productModalCloseBtn: document.getElementById('productModalCloseBtn'),
  productModalTitle: document.getElementById('productModalTitle'),
  productModalSubtitle: document.getElementById('productModalSubtitle'),
  productModalMeta: document.getElementById('productModalMeta'),
  productModalSync: document.getElementById('productModalSync'),
  productModalStatus: document.getElementById('productModalStatus'),
  productModalDisplayJson: document.getElementById('productModalDisplayJson'),
  productModalApiJson: document.getElementById('productModalApiJson'),
  authGate: document.getElementById('authGate'),
  apiAuthForm: document.getElementById('apiAuthForm'),
  apiTokenInput: document.getElementById('apiTokenInput'),
  apiTokenToggleBtn: document.getElementById('apiTokenToggleBtn'),
  unlockApiBtn: document.getElementById('unlockApiBtn'),
  apiAuthHint: document.getElementById('apiAuthHint'),
};

async function init() {
  renderGlobalNav('dashboard');
  initPasswordFieldToggle({ input: els.apiTokenInput, button: els.apiTokenToggleBtn, hiddenLabel: 'Afficher', shownLabel: 'Masquer' });
  bindEvents();
  hydrateApiToken();
  syncControlsFromState();
  await loadDatasets();
  await refreshUI();
}

function bindEvents() {
  els.apiAuthForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    await unlockApi();
  });

  els.datasetSelect.addEventListener('change', async (event) => {
    if (state.isBusy) return;
    state.currentDataset = event.target.value;
    state.category = '';
    state.page = 1;
    state.categoryPage = 1;
    await refreshUI({
      title: 'Changement de dataset…',
      text: 'On recharge les catégories et les fiches du catalogue sélectionné.',
    });
  });

  els.searchInput.addEventListener('input', debounce(async (event) => {
    if (state.isBusy) return;
    state.search = event.target.value.trim().toLowerCase();
    state.page = 1;
    await refreshUI({
      title: 'Mise à jour des résultats…',
      text: 'On applique le filtre de recherche et on recharge la page active.',
    });
  }, 250));

  els.categorySelect.addEventListener('change', async (event) => {
    if (state.isBusy) return;
    state.category = event.target.value;
    state.page = 1;
    await refreshUI({
      title: 'Mise à jour des résultats…',
      text: 'La catégorie sélectionnée est en cours de chargement.',
    });
  });

  els.catPrevBtn.addEventListener('click', async () => {
    if (state.isBusy) return;
    state.categoryPage = Math.max(1, state.categoryPage - 1);
    await refreshCategoryOptions();
  });

  els.catNextBtn.addEventListener('click', async () => {
    if (state.isBusy) return;
    state.categoryPage += 1;
    await refreshCategoryOptions();
  });

  els.sortSelect.addEventListener('change', async (event) => {
    if (state.isBusy) return;
    state.sort = event.target.value;
    state.page = 1;
    await refreshUI({
      title: 'Tri en cours…',
      text: 'On recalcule la liste avec le nouvel ordre d’affichage.',
    });
  });

  els.imagesOnlyToggle.addEventListener('change', async (event) => {
    if (state.isBusy) return;
    state.imagesOnly = event.target.checked;
    state.page = 1;
    await refreshUI({
      title: 'Filtre image…',
      text: 'On recharge uniquement les produits compatibles avec le filtre actif.',
    });
  });

  els.savedOnS3Toggle.addEventListener('change', async (event) => {
    if (state.isBusy) return;
    state.savedOnS3 = event.target.checked;
    state.page = 1;
    await refreshUI({
      title: 'Filtre S3…',
      text: 'On recharge uniquement les produits enregistrés sur S3.',
    });
  });

  els.pageSizeSelect.addEventListener('change', async (event) => {
    if (state.isBusy) return;
    state.pageSize = Math.max(1, Number.parseInt(event.target.value, 10) || 24);
    state.page = 1;
    await refreshUI({
      title: 'Changement de pagination…',
      text: 'On recharge la page active avec la nouvelle taille d’affichage.',
    });
  });

  els.prevPageBtn.addEventListener('click', async () => {
    if (state.isBusy) return;
    state.page = Math.max(1, state.page - 1);
    await refreshUI({ title: 'Navigation de page…', text: 'On charge la page précédente du catalogue.' });
  });

  els.nextPageBtn.addEventListener('click', async () => {
    if (state.isBusy) return;
    state.page += 1;
    await refreshUI({ title: 'Navigation de page…', text: 'On charge la page suivante du catalogue.' });
  });

  els.resetFiltersBtn.addEventListener('click', async () => {
    if (state.isBusy) return;
    state.search = '';
    state.category = '';
    state.imagesOnly = false;
    state.savedOnS3 = false;
    state.sort = 'relevance';
    state.page = 1;
    state.pageSize = 24;
    syncControlsFromState();
    await refreshUI({ title: 'Réinitialisation…', text: 'On remet le catalogue dans son état de départ.' });
  });

  els.productModalCloseBtn?.addEventListener('click', closeProductModal);
  els.productModalBackdrop?.addEventListener('click', (event) => {
    if (event.target === els.productModalBackdrop) closeProductModal();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeProductModal();
  });
}

function hydrateApiToken() {
  const stored = window.localStorage.getItem(API_TOKEN_STORAGE_KEY) || '';
  state.apiToken = stored.trim();
  if (els.apiTokenInput) {
    els.apiTokenInput.value = state.apiToken;
  }
}

function getApiHeaders(extraHeaders = {}) {
  const headers = { ...extraHeaders };
  if (state.apiToken) {
    headers.Authorization = `Bearer ${state.apiToken}`;
  }
  return headers;
}

function isExpectedAuthError(error) {
  const message = String(error?.message || '');
  return Boolean(error?.expectedAuth || message.includes('Autorisation requise') || message.includes('Token API requis ou invalide'));
}

function setApiLocked(message = 'Token requis pour accéder à l’API.') {
  state.apiUnlocked = false;
  els.authGate?.classList.remove('hidden');
  if (els.apiAuthHint) {
    els.apiAuthHint.textContent = message;
  }
}

function setApiUnlocked(message = '') {
  state.apiUnlocked = true;
  els.authGate?.classList.add('hidden');
  if (els.apiAuthHint) {
    els.apiAuthHint.textContent = message;
  }
}

function setDashboardBusy(isBusy, { button = null, loadingText = 'Chargement…' } = {}) {
  state.isBusy = isBusy;
  document.body.classList.toggle('is-busy', isBusy);
  setElementsDisabled([
    els.datasetSelect,
    els.searchInput,
    els.categorySelect,
    els.sortSelect,
    els.imagesOnlyToggle,
    els.savedOnS3Toggle,
    els.resetFiltersBtn,
    els.pageSizeSelect,
    els.prevPageBtn,
    els.nextPageBtn,
    els.catPrevBtn,
    els.catNextBtn,
  ], isBusy);
  if (button) {
    setButtonLoading(button, isBusy, loadingText);
  }
}

async function unlockApi() {
  const candidate = (els.apiTokenInput?.value || '').trim();
  state.apiToken = candidate;
  if (candidate) {
    window.localStorage.setItem(API_TOKEN_STORAGE_KEY, candidate);
  } else {
    window.localStorage.removeItem(API_TOKEN_STORAGE_KEY);
  }
  setDashboardBusy(true, { button: els.unlockApiBtn, loadingText: 'Vérification…' });
  try {
    await loadDatasets();
    await refreshUI({ title: 'Vérification du token…', text: 'On teste l’accès à l’API protégée.' });
  } finally {
    setDashboardBusy(false, { button: els.unlockApiBtn });
  }
}

async function loadDatasets() {
  updateStatus('Chargement des datasets…', 'info');
  els.datasetLoader.classList.remove('hidden');
  try {
    const response = await fetch('/api/datasets', { headers: getApiHeaders() });
    if (response.status === 401) {
      setApiLocked('Token invalide ou manquant.');
      const error = new Error('Autorisation requise');
      error.expectedAuth = true;
      throw error;
    }
    if (!response.ok) throw new Error(`Impossible de charger les datasets (${response.status})`);
    const payload = await response.json();
    state.datasets = (payload.datasets || []).filter((dataset) => ['shein', 'asos'].includes(dataset.id));
    if (!state.datasets.find((dataset) => dataset.id === state.currentDataset) && state.datasets.length) {
      state.currentDataset = state.datasets[0].id;
    }
    hydrateDatasetSelect();
    setApiUnlocked(state.apiToken ? 'Token valide.' : '');
  } finally {
    els.datasetLoader.classList.add('hidden');
  }
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
  els.savedOnS3Toggle.checked = state.savedOnS3;
  els.pageSizeSelect.value = String(state.pageSize);
}

async function refreshUI(options = {}) {
  const loadingTitle = options.title || 'Chargement des produits…';
  const loadingText = options.text || 'On rafraîchit le catalogue et les visuels.';
  syncControlsFromState();
  updateStatus(loadingTitle, 'info');
  setContentLoading(true, loadingTitle, loadingText);
  setDashboardBusy(true);
  els.datasetLoader.classList.remove('hidden');
  try {
    const [payload, categoriesPayload] = await Promise.all([
      fetchProducts(),
      fetchCategories(),
    ]);
    state.currentPayload = payload;
    state.currentCategories = categoriesPayload.data || [];
    state.currentCategoryPagination = categoriesPayload.pagination || { page: state.categoryPage, pageSize: state.categoryPageSize, total: 0, totalPages: 1, from: 0, to: 0 };
    render(payload, state.currentCategories, state.currentCategoryPagination);
    updateStatus('Catalogue chargé localement.', 'success');
  } catch (error) {
    if (!isExpectedAuthError(error)) {
      console.error(error);
      updateStatus(`Erreur: ${error.message}`, 'error');
    } else {
      updateStatus('API verrouillée. Entre ton token pour charger le catalogue.', 'info');
    }
  } finally {
    els.datasetLoader.classList.add('hidden');
    setContentLoading(false);
    setDashboardBusy(false);
  }
}

async function fetchProducts() {
  const params = new URLSearchParams({
    dataset: state.currentDataset,
    search: state.search,
    category: state.category,
    sort: state.sort,
    imagesOnly: String(state.imagesOnly),
    savedOnS3: String(state.savedOnS3),
    page: String(state.page),
    pageSize: String(state.pageSize),
  });

  const response = await fetch(`/api/products?${params.toString()}`, { headers: getApiHeaders() });
  if (response.status === 401) {
    setApiLocked('Token invalide ou manquant.');
    const error = new Error('Autorisation requise');
    error.expectedAuth = true;
    throw error;
  }
  if (!response.ok) throw new Error(`Impossible de charger les produits (${response.status})`);
  const payload = await response.json();
  state.page = payload.pagination?.page || 1;
  return payload;
}

async function fetchCategories() {
  const params = new URLSearchParams({
    dataset: state.currentDataset,
    search: state.search,
    savedOnS3: String(state.savedOnS3),
    page: String(state.categoryPage),
    pageSize: String(state.categoryPageSize),
  });
  const response = await fetch(`/api/categories?${params.toString()}`, { headers: getApiHeaders() });
  if (response.status === 401) {
    setApiLocked('Token invalide ou manquant.');
    const error = new Error('Autorisation requise');
    error.expectedAuth = true;
    throw error;
  }
  if (!response.ok) throw new Error(`Impossible de charger les catégories (${response.status})`);
  return response.json();
}

function render(payload, categories = [], categoryPagination = null) {
  const dataset = payload.dataset;
  const products = payload.products || [];
  state.currentPayload = payload;
  state.currentCategories = categories;
  state.currentCategoryPagination = categoryPagination || state.currentCategoryPagination;
  const pagination = payload.pagination || { page: 1, totalPages: 1, total: 0, from: 0, to: 0 };

  els.activeDatasetLabel.textContent = dataset?.label || '—';
  els.resultsCount.textContent = new Intl.NumberFormat('fr-FR').format(pagination.total || 0);
  const totalCategories = Number(state.currentCategoryPagination?.total || categories.length || 0);
  els.categoryCount.textContent = new Intl.NumberFormat('fr-FR').format(totalCategories);

  renderCategorySelect(categories, state.currentCategoryPagination);
  renderSummary(dataset, totalCategories, pagination);
  renderActiveFilters();
  renderPagination(pagination);
  renderProducts(products);
}

function renderCategorySelect(categories, categoryPagination = null) {
  const totalPages = Math.max(1, Number(categoryPagination?.totalPages || 1));
  state.categoryPage = Math.min(state.categoryPage, totalPages);
  const options = ['<option value="">Toutes les catégories</option>']
    .concat((categories || []).map((category) => {
      const label = `${truncate(category.name, 32)} (${category.count})`;
      return `<option value="${escapeHtml(category.name)}" title="${escapeHtml(category.name)}">${escapeHtml(label)}</option>`;
    }))
    .join('');
  els.categorySelect.innerHTML = options;
  const categoryExists = (categories || []).some((item) => item.name === state.category);
  els.categorySelect.value = categoryExists ? state.category : '';
  if (!categoryExists && state.category && state.categoryPage !== 1) {
    els.categorySelect.value = '';
  }
  els.catPageIndicator.textContent = `${state.categoryPage} / ${totalPages}`;
  els.catPrevBtn.disabled = state.categoryPage <= 1 || state.isBusy;
  els.catNextBtn.disabled = state.categoryPage >= totalPages || state.isBusy;
}

function renderSummary(dataset, totalCategories, pagination) {
  els.summaryBar.innerHTML = `
    <span><strong>${new Intl.NumberFormat('fr-FR').format(dataset?.total_count || 0)}</strong> produits chargés</span>
    <span><strong>${new Intl.NumberFormat('fr-FR').format(pagination.total || 0)}</strong> résultats après filtres</span>
    <span><strong>${new Intl.NumberFormat('fr-FR').format(totalCategories || 0)}</strong> catégories repérées</span>
    <span><strong>${new Intl.NumberFormat('fr-FR').format(dataset?.with_images_count || 0)}</strong> fiches avec image</span>
    <span><strong>${new Intl.NumberFormat('fr-FR').format(dataset?.with_reviews_count || 0)}</strong> fiches avec avis > 0</span>
  `;
}

async function refreshCategoryOptions() {
  setDashboardBusy(true);
  try {
    const categoriesPayload = await fetchCategories();
    state.currentCategories = categoriesPayload.data || [];
    state.currentCategoryPagination = categoriesPayload.pagination || { page: state.categoryPage, pageSize: state.categoryPageSize, total: 0, totalPages: 1, from: 0, to: 0 };
    renderCategorySelect(state.currentCategories, state.currentCategoryPagination);
    const totalCategories = Number(state.currentCategoryPagination?.total || state.currentCategories.length || 0);
    els.categoryCount.textContent = new Intl.NumberFormat('fr-FR').format(totalCategories);
    if (state.currentPayload?.dataset && state.currentPayload?.pagination) {
      renderSummary(state.currentPayload.dataset, totalCategories, state.currentPayload.pagination);
    }
  } catch (error) {
    console.error(error);
    updateStatus(`Erreur catégories: ${error.message}`, 'error');
  } finally {
    setDashboardBusy(false);
  }
}

function renderActiveFilters() {
  const chips = [];
  const activeDataset = state.datasets.find((dataset) => dataset.id === state.currentDataset);
  if (activeDataset) chips.push(`Dataset: ${activeDataset.label}`);
  if (state.search) chips.push(`Recherche: ${state.search}`);
  if (state.category) chips.push(`Catégorie: ${state.category}`);
  if (state.imagesOnly) chips.push('Images source seulement');
  if (state.savedOnS3) chips.push('Enregistrés sur S3 seulement');
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
  els.prevPageBtn.disabled = (pagination.page || 1) <= 1 || state.isBusy;
  els.nextPageBtn.disabled = (pagination.page || 1) >= (pagination.totalPages || 1) || state.isBusy;
}

function getImageSyncSnapshot(product = {}) {
  const sourceImages = (product.sourceImageUrls || product.source_image_urls || product.imageUrls || [])
    .filter(Boolean)
    .filter((value, index, array) => array.indexOf(value) === index);
  const s3Images = (product.s3ImageUrls || product.s3_image_urls || [])
    .filter(Boolean)
    .filter((value, index, array) => array.indexOf(value) === index);
  const sourceCount = sourceImages.length;
  const s3Count = s3Images.length;
  const fullySynced = sourceCount > 0 && s3Count === sourceCount && Boolean(product.saved_on_s3);
  const partial = s3Count > 0 && s3Count < sourceCount;
  const tone = fullySynced ? 'full' : partial ? 'partial' : sourceCount > 0 ? 'source' : 'empty';
  const label = fullySynced ? 'Fully synced' : partial ? 'Partial sync' : sourceCount > 0 ? 'Source only' : 'No images';
  const note = fullySynced
    ? `${s3Count}/${sourceCount} images mirrored to S3`
    : partial
      ? `${s3Count}/${sourceCount} images mirrored — resync needed`
      : sourceCount > 0
        ? `${sourceCount} source image${sourceCount > 1 ? 's' : ''} waiting for S3`
        : 'No source image available';
  return { sourceImages, s3Images, sourceCount, s3Count, fullySynced, partial, tone, label, note };
}

function renderProductSyncStrip(product = {}) {
  const sync = getImageSyncSnapshot(product);
  return `
    <div class="sync-pill is-${sync.tone}">${escapeHtml(sync.label)}</div>
    <div class="sync-kpi"><span>Source</span><strong>${sync.sourceCount}</strong></div>
    <div class="sync-kpi"><span>S3</span><strong>${sync.s3Count}</strong></div>
  `;
}

function renderProductModalSync(product = {}) {
  const sync = getImageSyncSnapshot(product);
  return `
    <article class="product-modal-sync-panel is-${sync.tone}">
      <div class="product-modal-sync-head">
        <div>
          <p class="eyebrow">Image sync</p>
          <strong>${escapeHtml(sync.label)}</strong>
        </div>
        <span class="sync-pill is-${sync.tone}">${escapeHtml(sync.fullySynced ? 'ready' : sync.partial ? 'needs sync' : sync.sourceCount ? 'pending' : 'empty')}</span>
      </div>
      <div class="product-modal-sync-kpis">
        <div class="product-modal-meta-card"><span>Images source</span><strong>${sync.sourceCount}</strong></div>
        <div class="product-modal-meta-card"><span>Images S3</span><strong>${sync.s3Count}</strong></div>
      </div>
      <p class="product-modal-sync-note">${escapeHtml(sync.note)}</p>
    </article>
  `;
}

function openProductModalShell() {
  if (!els.productModalBackdrop) return;
  els.productModalBackdrop.classList.remove('hidden');
  els.productModalBackdrop.classList.add('is-open');
  els.productModalBackdrop.setAttribute('aria-hidden', 'false');
}

function closeProductModal() {
  state.selectedProductKey = null;
  if (!els.productModalBackdrop) return;
  els.productModalBackdrop.classList.remove('is-open');
  els.productModalBackdrop.classList.add('hidden');
  els.productModalBackdrop.setAttribute('aria-hidden', 'true');
}

function renderProductModalLoading(product) {
  els.productModalTitle.textContent = product?.name || 'Produit';
  els.productModalSubtitle.textContent = product?.goods_id || product?.id || 'Chargement…';
  els.productModalMeta.innerHTML = `
    <div class="product-modal-meta-card"><span>Dataset</span><strong>${escapeHtml(state.currentDataset || '—')}</strong></div>
    <div class="product-modal-meta-card"><span>Goods ID</span><strong>${escapeHtml(product?.goods_id || '—')}</strong></div>
    <div class="product-modal-meta-card"><span>Product ID</span><strong>${escapeHtml(product?.id || '—')}</strong></div>
    <div class="product-modal-meta-card"><span>Source</span><strong>${escapeHtml(product?.source || '—')}</strong></div>
  `;
  if (els.productModalSync) {
    els.productModalSync.innerHTML = renderProductModalSync(product || {});
  }
  els.productModalStatus.textContent = 'Chargement à la demande des JSON complets…';
  els.productModalDisplayJson.textContent = 'Chargement…';
  els.productModalApiJson.textContent = 'Chargement…';
}

function renderProductModalError(product, error) {
  els.productModalTitle.textContent = product?.name || 'Produit';
  els.productModalSubtitle.textContent = product?.goods_id || product?.id || 'Erreur';
  if (els.productModalSync) {
    els.productModalSync.innerHTML = renderProductModalSync(product || {});
  }
  els.productModalStatus.textContent = `Erreur: ${error.message}`;
  els.productModalDisplayJson.textContent = 'Impossible de charger la version dashboard.';
  els.productModalApiJson.textContent = 'Impossible de charger la version API.';
}

function renderProductModal(detail, fallbackProduct = {}) {
  const display = detail?.display || {};
  const apiProduct = detail?.api || detail?.data || {};
  const dataset = detail?.dataset || {};
  const summaryProduct = { ...fallbackProduct, ...display, ...apiProduct };
  const name = display?.name || apiProduct?.name || fallbackProduct?.name || 'Produit';
  const goodsId = display?.goods_id || apiProduct?.goods_id || fallbackProduct?.goods_id || '—';
  const productId = display?.id || apiProduct?.goods_sn || fallbackProduct?.id || '—';
  const sourceUrl = display?.url || apiProduct?.product_url || fallbackProduct?.url || '—';
  const sync = getImageSyncSnapshot(summaryProduct);

  els.productModalTitle.textContent = name;
  els.productModalSubtitle.textContent = `${goodsId} · ${dataset?.label || fallbackProduct?.source || 'Catalogue local'}`;
  els.productModalMeta.innerHTML = `
    <div class="product-modal-meta-card"><span>Dataset</span><strong>${escapeHtml(dataset?.id || state.currentDataset || '—')}</strong></div>
    <div class="product-modal-meta-card"><span>Goods ID</span><strong>${escapeHtml(goodsId)}</strong></div>
    <div class="product-modal-meta-card"><span>Product ID</span><strong>${escapeHtml(productId)}</strong></div>
    <div class="product-modal-meta-card"><span>URL source</span><strong>${escapeHtml(sourceUrl)}</strong></div>
  `;
  if (els.productModalSync) {
    els.productModalSync.innerHTML = renderProductModalSync(summaryProduct);
  }
  els.productModalStatus.textContent = `Chargé à la demande — ${sync.note}. La grille reste légère, le détail n’est fetch que pour ce modal.`;
  els.productModalDisplayJson.textContent = JSON.stringify(display, null, 2);
  els.productModalApiJson.textContent = JSON.stringify(apiProduct, null, 2);
}

async function fetchProductDetail(product) {
  const productId = String(product?.id || '').trim() || String(product?.goods_id || '').trim();
  if (!productId) throw new Error('Produit sans identifiant exploitable');
  const datasetId = state.currentDataset || product?.dataset_id || 'shein';
  const cacheKey = `${datasetId}:${productId}`;
  if (state.productDetailCache[cacheKey]) {
    return { cacheKey, payload: state.productDetailCache[cacheKey] };
  }
  const response = await fetch(`/api/products/${encodeURIComponent(productId)}?dataset=${encodeURIComponent(datasetId)}`, { headers: getApiHeaders() });
  if (response.status === 401) {
    setApiLocked('Token invalide ou manquant.');
    throw new Error('Autorisation requise');
  }
  if (!response.ok) throw new Error(`Impossible de charger le détail produit (${response.status})`);
  const payload = await response.json();
  state.productDetailCache[cacheKey] = payload;
  return { cacheKey, payload };
}

async function openProductModal(product) {
  const productId = String(product?.id || '').trim() || String(product?.goods_id || '').trim();
  const datasetId = state.currentDataset || product?.dataset_id || 'shein';
  const requestKey = `${datasetId}:${productId}`;
  state.selectedProductKey = requestKey;
  renderProductModalLoading(product);
  openProductModalShell();
  try {
    const { cacheKey, payload } = await fetchProductDetail(product);
    if (state.selectedProductKey !== cacheKey) return;
    renderProductModal(payload, product);
  } catch (error) {
    console.error(error);
    if (state.selectedProductKey !== requestKey) return;
    renderProductModalError(product, error);
  }
}

function ensureProductCardControls(node) {
  const card = node.querySelector('.product-card');
  const body = node.querySelector('.product-body');
  const title = node.querySelector('.product-title');
  const description = node.querySelector('.product-description');
  let identity = node.querySelector('.product-identity');
  let syncStrip = node.querySelector('.product-sync-strip');
  let actions = node.querySelector('.product-actions');
  let inspectBtn = node.querySelector('.product-inspect-btn');
  let sourceLink = node.querySelector('.product-source-link');

  if (!identity && description && description.parentNode) {
    identity = document.createElement('p');
    identity.className = 'product-identity';
    description.parentNode.insertBefore(identity, description);
  }

  if (!syncStrip && body) {
    syncStrip = document.createElement('div');
    syncStrip.className = 'product-sync-strip';
    if (description && description.parentNode) {
      description.parentNode.insertBefore(syncStrip, body.querySelector('.product-meta'));
    } else {
      body.appendChild(syncStrip);
    }
  }

  if (!actions && body) {
    actions = document.createElement('div');
    actions.className = 'product-actions';
    body.appendChild(actions);
  }

  if (!inspectBtn && actions) {
    inspectBtn = document.createElement('button');
    inspectBtn.type = 'button';
    inspectBtn.className = 'product-inspect-btn';
    inspectBtn.textContent = 'Voir la fiche produit';
    actions.appendChild(inspectBtn);
  }

  if (!sourceLink && actions) {
    const legacyLink = node.querySelector('.product-link');
    if (legacyLink) {
      legacyLink.className = 'product-source-link';
      legacyLink.setAttribute('aria-label', 'Ouvrir l’URL d’origine');
      legacyLink.setAttribute('title', 'Ouvrir l’URL d’origine');
      legacyLink.innerHTML = '<span aria-hidden="true">↗</span>';
      sourceLink = legacyLink;
    } else {
      sourceLink = document.createElement('a');
      sourceLink.className = 'product-source-link';
      sourceLink.target = '_blank';
      sourceLink.rel = 'noopener noreferrer';
      sourceLink.setAttribute('aria-label', 'Ouvrir l’URL d’origine');
      sourceLink.setAttribute('title', 'Ouvrir l’URL d’origine');
      sourceLink.innerHTML = '<span aria-hidden="true">↗</span>';
      actions.appendChild(sourceLink);
    }
  }

  return { card, title, identity, description, syncStrip, inspectBtn, sourceLink };
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
    const controls = ensureProductCardControls(node);
    const card = controls.card;
    const mediaWrap = node.querySelector('.product-media-wrap');
    const img = node.querySelector('.product-media');
    const sourceBadge = node.querySelector('.badge-source');
    const categoryBadge = node.querySelector('.badge-category');
    const price = node.querySelector('.product-price');
    const title = controls.title;
    const identity = controls.identity;
    const description = controls.description;
    const syncStrip = controls.syncStrip;
    const highlights = node.querySelector('.product-highlights');
    const meta = node.querySelector('.product-meta');
    const inspectBtn = controls.inspectBtn;
    const sourceLink = controls.sourceLink;
    const prevBtn = node.querySelector('.carousel-btn-prev');
    const nextBtn = node.querySelector('.carousel-btn-next');
    const counter = node.querySelector('.carousel-counter');

    if (sourceBadge) sourceBadge.textContent = product.source;
    if (categoryBadge) categoryBadge.textContent = product.category || product.category_path || 'Sans catégorie';
    if (price) price.textContent = product.price_text || 'Prix non disponible';
    if (title) title.textContent = product.name;
    if (identity) identity.textContent = `Goods ID · ${product.goods_id || '—'}`;
    if (description) description.textContent = truncate(product.description || 'Pas de description fournie dans le dataset.', 220);

    const images = [product.image, ...(product.imageUrls || [])]
      .filter(Boolean)
      .filter((value, index, array) => array.indexOf(value) === index);

    if (images.length && img && mediaWrap) {
      let imageIndex = 0;
      const setImage = () => {
        img.src = images[imageIndex];
        img.alt = `${product.name} (${imageIndex + 1}/${images.length})`;
        if (counter) counter.textContent = `${imageIndex + 1} / ${images.length}`;
      };

      setImage();
      img.onerror = () => mediaWrap.classList.add('no-image');

      if (images.length > 1 && prevBtn && nextBtn && counter) {
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
    } else if (mediaWrap) {
      mediaWrap.classList.add('no-image');
    }

    const syncSnapshot = getImageSyncSnapshot(product);
    if (syncStrip) {
      syncStrip.innerHTML = renderProductSyncStrip(product);
    }

    const highlightEntries = [
      product.brand && { label: 'Brand', value: product.brand },
      product.color && { label: 'Couleur', value: product.color },
      (product.rating !== undefined && product.rating !== null && product.rating !== '') && { label: 'Note', value: product.rating },
      Number(product.reviews_count) > 0 && { label: 'Avis', value: product.reviews_count },
      syncSnapshot.sourceCount > 0 && { label: 'Source', value: syncSnapshot.sourceCount },
      syncSnapshot.s3Count > 0 && { label: 'S3', value: syncSnapshot.s3Count },
    ].filter(Boolean);

    if (highlights) {
      highlights.innerHTML = highlightEntries
        .slice(0, 4)
        .map((item) => `<span class="highlight-pill"><strong>${escapeHtml(item.label)}</strong> ${escapeHtml(String(item.value))}</span>`)
        .join('');
    }

    const metaEntries = [
      ['Goods ID', product.goods_id || '—'],
      ['Tailles', (product.sizes || []).join(', ') || product.size_text || '—'],
      ['Source', product.source || '—'],
      ['Sync', syncSnapshot.label],
      ['SKU', product.sku || product.id || '—'],
    ];

    if (meta) {
      meta.innerHTML = metaEntries
        .filter(([, value]) => value !== '' && value !== null && value !== undefined && value !== '—')
        .slice(0, 6)
        .map(([label, value]) => `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(String(value))}</dd>`)
        .join('');
    }

    if (inspectBtn) {
      inspectBtn.addEventListener('click', () => openProductModal(product));
    }

    if (sourceLink) {
      if (product.url) {
        sourceLink.href = product.url;
        sourceLink.removeAttribute('aria-disabled');
        sourceLink.classList.remove('disabled');
      } else {
        sourceLink.removeAttribute('href');
        sourceLink.setAttribute('aria-disabled', 'true');
        sourceLink.classList.add('disabled');
      }
    }

    fragment.appendChild(node);
  });

  els.productGrid.appendChild(fragment);
}

function updateStatus(message, tone = 'info') {
  els.statusBanner.className = `status-banner ${tone}`;
  els.statusBanner.textContent = message;
}

function setContentLoading(isLoading, title, text) {
  if (!els.contentLoader) return;
  els.contentLoader.classList.toggle('hidden', !isLoading);
  if (els.contentLoaderTitle && title) els.contentLoaderTitle.textContent = title;
  if (els.contentLoaderText && text) els.contentLoaderText.textContent = text;
  els.productGrid.setAttribute('aria-busy', String(isLoading));
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
  if (!isExpectedAuthError(error)) {
    console.error(error);
    updateStatus(`Erreur: ${error.message}`, 'error');
  } else {
    updateStatus('API verrouillée. Entre ton token pour charger le catalogue.', 'info');
  }
});
