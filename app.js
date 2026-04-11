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
  categoryPage: 1,
  categoryPageSize: 24,
  productDetailCache: {},
  selectedProductKey: null,
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
  productModalStatus: document.getElementById('productModalStatus'),
  productModalDisplayJson: document.getElementById('productModalDisplayJson'),
  productModalApiJson: document.getElementById('productModalApiJson'),
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
    state.categoryPage = 1;
    await refreshUI({
      title: 'Changement de dataset…',
      text: 'On recharge les catégories et les fiches du catalogue sélectionné.',
    });
  });

  els.searchInput.addEventListener('input', debounce(async (event) => {
    state.search = event.target.value.trim().toLowerCase();
    state.page = 1;
    await refreshUI({
      title: 'Mise à jour des résultats…',
      text: 'On applique le filtre de recherche et on recharge la page active.',
    });
  }, 250));

  els.categorySelect.addEventListener('change', async (event) => {
    state.category = event.target.value;
    state.page = 1;
    await refreshUI({
      title: 'Mise à jour des résultats…',
      text: 'La catégorie sélectionnée est en cours de chargement.',
    });
  });

  els.catPrevBtn.addEventListener('click', async () => {
    state.categoryPage = Math.max(1, state.categoryPage - 1);
    renderCategorySelect(state.currentPayload?.categories || []);
  });

  els.catNextBtn.addEventListener('click', async () => {
    state.categoryPage += 1;
    renderCategorySelect(state.currentPayload?.categories || []);
  });

  els.sortSelect.addEventListener('change', async (event) => {
    state.sort = event.target.value;
    state.page = 1;
    await refreshUI({
      title: 'Tri en cours…',
      text: 'On recalcule la liste avec le nouvel ordre d’affichage.',
    });
  });

  els.imagesOnlyToggle.addEventListener('change', async (event) => {
    state.imagesOnly = event.target.checked;
    state.page = 1;
    await refreshUI({
      title: 'Filtre image…',
      text: 'On recharge uniquement les produits compatibles avec le filtre actif.',
    });
  });

  els.pageSizeSelect.addEventListener('change', async (event) => {
    state.pageSize = Math.max(1, Number.parseInt(event.target.value, 10) || 24);
    state.page = 1;
    await refreshUI({
      title: 'Changement de pagination…',
      text: 'On recharge la page active avec la nouvelle taille d’affichage.',
    });
  });

  els.prevPageBtn.addEventListener('click', async () => {
    state.page = Math.max(1, state.page - 1);
    await refreshUI({
      title: 'Navigation de page…',
      text: 'On charge la page précédente du catalogue.',
    });
  });

  els.nextPageBtn.addEventListener('click', async () => {
    state.page += 1;
    await refreshUI({
      title: 'Navigation de page…',
      text: 'On charge la page suivante du catalogue.',
    });
  });

  els.resetFiltersBtn.addEventListener('click', async () => {
    state.search = '';
    state.category = '';
    state.imagesOnly = false;
    state.sort = 'relevance';
    state.page = 1;
    state.pageSize = 24;
    syncControlsFromState();
    await refreshUI({
      title: 'Réinitialisation…',
      text: 'On remet le catalogue dans son état de départ.',
    });
  });

  els.productModalCloseBtn?.addEventListener('click', closeProductModal);
  els.productModalBackdrop?.addEventListener('click', (event) => {
    if (event.target === els.productModalBackdrop) closeProductModal();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeProductModal();
  });
}

async function loadDatasets() {
  updateStatus('Chargement des datasets…', 'info');
  els.datasetLoader.classList.remove('hidden');
  try {
    const response = await fetch('/api/datasets');
    if (!response.ok) throw new Error(`Impossible de charger les datasets (${response.status})`);
    const payload = await response.json();
    state.datasets = (payload.datasets || []).filter((dataset) => ['shein', 'asos'].includes(dataset.id));
    if (!state.datasets.find((dataset) => dataset.id === state.currentDataset) && state.datasets.length) {
      state.currentDataset = state.datasets[0].id;
    }
    hydrateDatasetSelect();
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
  els.pageSizeSelect.value = String(state.pageSize);
}

async function refreshUI(options = {}) {
  const loadingTitle = options.title || 'Chargement des produits…';
  const loadingText = options.text || 'On rafraîchit le catalogue et les visuels.';
  syncControlsFromState();
  updateStatus(loadingTitle, 'info');
  setContentLoading(true, loadingTitle, loadingText);
  els.datasetLoader.classList.remove('hidden');
  try {
    const payload = await fetchProducts();
    state.currentPayload = payload;
    render(payload);
    updateStatus('Catalogue chargé localement.', 'success');
  } catch (error) {
    console.error(error);
    updateStatus(`Erreur: ${error.message}`, 'error');
  } finally {
    els.datasetLoader.classList.add('hidden');
    setContentLoading(false);
  }
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
  state.currentPayload = payload;
  const pagination = payload.pagination || { page: 1, totalPages: 1, total: 0, from: 0, to: 0 };

  els.activeDatasetLabel.textContent = dataset?.label || '—';
  els.resultsCount.textContent = new Intl.NumberFormat('fr-FR').format(pagination.total || 0);
  els.categoryCount.textContent = new Intl.NumberFormat('fr-FR').format(categories.length);

  renderCategorySelect(categories);
  renderSummary(dataset, categories, pagination);
  renderActiveFilters();
  renderPagination(pagination);
  renderProducts(products);
}

function renderCategorySelect(categories) {
  const pageSize = state.categoryPageSize;
  const totalPages = Math.max(1, Math.ceil((categories.length || 0) / pageSize));
  state.categoryPage = Math.min(state.categoryPage, totalPages);
  const start = (state.categoryPage - 1) * pageSize;
  const pageCategories = categories.slice(start, start + pageSize);
  const options = ['<option value="">Toutes les catégories</option>']
    .concat(pageCategories.map((category) => {
      const label = `${truncate(category.name, 32)} (${category.count})`;
      return `<option value="${escapeHtml(category.name)}" title="${escapeHtml(category.name)}">${escapeHtml(label)}</option>`;
    }))
    .join('');
  els.categorySelect.innerHTML = options;
  const categoryExists = categories.some((item) => item.name === state.category);
  els.categorySelect.value = categoryExists ? state.category : '';
  if (!categoryExists) state.category = '';
  els.catPageIndicator.textContent = `${state.categoryPage} / ${totalPages}`;
  els.catPrevBtn.disabled = state.categoryPage <= 1;
  els.catNextBtn.disabled = state.categoryPage >= totalPages;
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
  els.productModalStatus.textContent = 'Chargement à la demande des JSON complets…';
  els.productModalDisplayJson.textContent = 'Chargement…';
  els.productModalApiJson.textContent = 'Chargement…';
}

function renderProductModalError(product, error) {
  els.productModalTitle.textContent = product?.name || 'Produit';
  els.productModalSubtitle.textContent = product?.goods_id || product?.id || 'Erreur';
  els.productModalStatus.textContent = `Erreur: ${error.message}`;
  els.productModalDisplayJson.textContent = 'Impossible de charger la version dashboard.';
  els.productModalApiJson.textContent = 'Impossible de charger la version API.';
}

function renderProductModal(detail, fallbackProduct = {}) {
  const display = detail?.display || {};
  const apiProduct = detail?.api || detail?.data || {};
  const dataset = detail?.dataset || {};
  const name = display?.name || apiProduct?.name || fallbackProduct?.name || 'Produit';
  const goodsId = display?.goods_id || apiProduct?.goods_id || fallbackProduct?.goods_id || '—';
  const productId = display?.id || apiProduct?.goods_sn || fallbackProduct?.id || '—';
  const sourceUrl = display?.url || apiProduct?.product_url || fallbackProduct?.url || '—';

  els.productModalTitle.textContent = name;
  els.productModalSubtitle.textContent = `${goodsId} · ${dataset?.label || fallbackProduct?.source || 'Catalogue local'}`;
  els.productModalMeta.innerHTML = `
    <div class="product-modal-meta-card"><span>Dataset</span><strong>${escapeHtml(dataset?.id || state.currentDataset || '—')}</strong></div>
    <div class="product-modal-meta-card"><span>Goods ID</span><strong>${escapeHtml(goodsId)}</strong></div>
    <div class="product-modal-meta-card"><span>Product ID</span><strong>${escapeHtml(productId)}</strong></div>
    <div class="product-modal-meta-card"><span>URL source</span><strong>${escapeHtml(sourceUrl)}</strong></div>
  `;
  els.productModalStatus.textContent = 'Chargé à la demande — la grille reste légère, le détail n’est fetch que pour ce modal.';
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
  const response = await fetch(`/api/products/${encodeURIComponent(productId)}?dataset=${encodeURIComponent(datasetId)}`);
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
    const identity = node.querySelector('.product-identity');
    const description = node.querySelector('.product-description');
    const highlights = node.querySelector('.product-highlights');
    const meta = node.querySelector('.product-meta');
    const inspectBtn = node.querySelector('.product-inspect-btn');
    const sourceLink = node.querySelector('.product-source-link');
    const prevBtn = node.querySelector('.carousel-btn-prev');
    const nextBtn = node.querySelector('.carousel-btn-next');
    const counter = node.querySelector('.carousel-counter');

    sourceBadge.textContent = product.source;
    categoryBadge.textContent = product.category || product.category_path || 'Sans catégorie';
    price.textContent = product.price_text || 'Prix non disponible';
    title.textContent = product.name;
    identity.textContent = `Goods ID · ${product.goods_id || '—'}`;
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

    const highlightEntries = [
      product.brand && { label: 'Brand', value: product.brand },
      product.color && { label: 'Couleur', value: product.color },
      (product.rating !== undefined && product.rating !== null && product.rating !== '') && { label: 'Note', value: product.rating },
      Number(product.reviews_count) > 0 && { label: 'Avis', value: product.reviews_count },
      images.length > 0 && { label: 'Images', value: images.length },
      product.saved_on_s3 && { label: 'S3', value: 'Oui' },
    ].filter(Boolean);

    highlights.innerHTML = highlightEntries
      .slice(0, 4)
      .map((item) => `<span class="highlight-pill"><strong>${escapeHtml(item.label)}</strong> ${escapeHtml(String(item.value))}</span>`)
      .join('');

    const metaEntries = [
      ['Goods ID', product.goods_id || '—'],
      ['Tailles', (product.sizes || []).join(', ') || product.size_text || '—'],
      ['Source', product.source || '—'],
      ['S3', product.saved_on_s3 ? 'Oui' : 'Non'],
      ['SKU', product.sku || product.id || '—'],
    ];

    meta.innerHTML = metaEntries
      .filter(([, value]) => value !== '' && value !== null && value !== undefined && value !== '—')
      .slice(0, 6)
      .map(([label, value]) => `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(String(value))}</dd>`)
      .join('');

    inspectBtn.addEventListener('click', () => openProductModal(product));

    if (product.url) {
      sourceLink.href = product.url;
    } else {
      sourceLink.removeAttribute('href');
      sourceLink.setAttribute('aria-disabled', 'true');
      sourceLink.classList.add('disabled');
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
  console.error(error);
  updateStatus(`Erreur: ${error.message}`, 'error');
});
