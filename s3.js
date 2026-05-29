const API_TOKEN_STORAGE_KEY = 'fast-fashion-api-token';
const ACTIVE_JOB_STATUSES = ['running', 'queued', 'cancel_requested'];
const DETAIL_PAGE_SIZE = 50;
const PREVIEW_STATUS = 'preview';

const JOB_FAMILY_CONFIG = {
  upload: {
    family: 'upload',
    tab: 'upload',
    listEndpoint: '/api/s3/upload-jobs',
    createEndpoint: '/api/s3/upload-jobs',
    summaryEndpoint: null,
    metricLabel: 'Uploadés',
    emptyText: 'Aucun job d’upload pour le moment.',
    hintDefault: 'Le preview dry-run évalue les produits candidats sans écrire dans S3 ni SQLite.',
    previewButtonText: 'Prévisualiser l’upload',
    startButtonText: 'Lancer l’upload S3',
    stopButtonTextSingle: 'Stop job upload actif',
    stopButtonTextMany: 'Stop jobs upload actifs',
    progressSuccessLabel: 'Réussis / preview',
    title: 'Upload',
    detailTypeLabel: 'Upload S3',
    confirmStart: (summary) => `Lancer l’upload réel ? ${summary.total} produit(s) candidat(s) ont été évalués en preview.`,
    buildCreateBody: () => ({
      dataset_id: els.s3DatasetSelect.value,
      limit: Number.parseInt(els.s3LimitInput.value, 10) || 50,
      concurrency: Number.parseInt(els.s3ConcurrencyInput.value, 10) || 4,
    }),
    getHintEl: () => els.uploadHint,
    getListEl: () => els.uploadJobsList,
    getStopButton: () => els.stopUploadJobsBtn,
    getButtons: () => [els.previewUploadBtn, els.startUploadBtn, els.stopUploadJobsBtn],
    summaryFromCreateResponse: (payload) => ({
      total: payload?.data?.total || payload?.data?.processed || 0,
      sample_limit: payload?.data?.limit || 0,
      sample: payload?.data?.items || [],
    }),
  },
  url_migration: {
    family: 'url_migration',
    tab: 'url_migration',
    listEndpoint: '/api/s3/url-migration-jobs',
    createEndpoint: '/api/s3/url-migration-jobs',
    summaryEndpoint: '/api/s3/migration-summary',
    metricLabel: 'Migrés',
    emptyText: 'Aucun job de migration pour le moment.',
    hintDefault: 'AWS_URL sera utilisé pour réécrire les anciennes URLs stockées.',
    previewButtonText: 'Prévisualiser la migration',
    startButtonText: 'Lancer la migration',
    stopButtonTextSingle: 'Stop job migration actif',
    stopButtonTextMany: 'Stop jobs migration actifs',
    progressSuccessLabel: 'Migrés / preview',
    title: 'Migration',
    detailTypeLabel: 'Migration d’URL',
    confirmStart: (summary) => `Lancer la migration des URLs stockées ? ${summary.total} item(s) seront impacté(s). Un backup JSON sera créé avant écriture.`,
    buildCreateBody: () => ({ sample_limit: 25 }),
    getHintEl: () => els.migrationHint,
    getListEl: () => els.migrationJobsList,
    getStopButton: () => els.stopMigrationJobsBtn,
    getButtons: () => [els.previewMigrationBtn, els.startMigrationBtn, els.stopMigrationJobsBtn],
    summaryFromCreateResponse: (payload) => ({ total: payload?.data?.total || 0, sample_limit: 25, sample: payload?.data?.items || [] }),
  },
  state_cleanup: {
    family: 'state_cleanup',
    tab: 'state_cleanup',
    listEndpoint: '/api/s3/state-cleanup-jobs',
    createEndpoint: '/api/s3/state-cleanup-jobs',
    summaryEndpoint: '/api/s3/cleanup-summary',
    metricLabel: 'Nettoyés',
    emptyText: 'Aucun job de cleanup pour le moment.',
    hintDefault: 'Utilise ce cleanup pour réinitialiser les anciens états saved_on_s3 devenus obsolètes.',
    previewButtonText: 'Prévisualiser le cleanup',
    startButtonText: 'Lancer le cleanup',
    stopButtonTextSingle: 'Stop job cleanup actif',
    stopButtonTextMany: 'Stop jobs cleanup actifs',
    progressSuccessLabel: 'Nettoyés / preview',
    title: 'Cleanup',
    detailTypeLabel: 'Cleanup état S3',
    confirmStart: (summary) => `Lancer le cleanup des états S3 obsolètes ? ${summary.total} item(s) seront réinitialisé(s). Un backup JSON sera créé avant écriture.`,
    buildCreateBody: () => ({ sample_limit: 25 }),
    getHintEl: () => els.cleanupHint,
    getListEl: () => els.cleanupJobsList,
    getStopButton: () => els.stopCleanupJobsBtn,
    getButtons: () => [els.previewCleanupBtn, els.startCleanupBtn, els.stopCleanupJobsBtn],
    summaryFromCreateResponse: (payload) => ({ total: payload?.data?.total || 0, sample_limit: 25, sample: payload?.data?.items || [] }),
  },
};

const els = {
  authGate: document.getElementById('authGate'),
  authStateLabel: document.getElementById('authStateLabel'),
  s3AuthForm: document.getElementById('s3AuthForm'),
  passwordInput: document.getElementById('passwordInput'),
  passwordToggleBtn: document.getElementById('passwordToggleBtn'),
  unlockBtn: document.getElementById('unlockBtn'),
  authHint: document.getElementById('authHint'),
  s3Workspace: document.getElementById('s3Workspace'),
  uploadTabBtn: document.getElementById('uploadTabBtn'),
  migrationTabBtn: document.getElementById('migrationTabBtn'),
  cleanupTabBtn: document.getElementById('cleanupTabBtn'),
  uploadJobsList: document.getElementById('uploadJobsList'),
  migrationJobsList: document.getElementById('migrationJobsList'),
  cleanupJobsList: document.getElementById('cleanupJobsList'),
  s3DatasetSelect: document.getElementById('s3DatasetSelect'),
  s3BucketInput: document.getElementById('s3BucketInput'),
  s3PrefixInput: document.getElementById('s3PrefixInput'),
  s3LimitInput: document.getElementById('s3LimitInput'),
  s3ConcurrencyInput: document.getElementById('s3ConcurrencyInput'),
  previewUploadBtn: document.getElementById('previewUploadBtn'),
  startUploadBtn: document.getElementById('startUploadBtn'),
  stopUploadJobsBtn: document.getElementById('stopUploadJobsBtn'),
  refreshS3JobsBtn: document.getElementById('refreshS3JobsBtn'),
  previewMigrationBtn: document.getElementById('previewMigrationBtn'),
  startMigrationBtn: document.getElementById('startMigrationBtn'),
  stopMigrationJobsBtn: document.getElementById('stopMigrationJobsBtn'),
  migrationHint: document.getElementById('migrationHint'),
  previewCleanupBtn: document.getElementById('previewCleanupBtn'),
  startCleanupBtn: document.getElementById('startCleanupBtn'),
  stopCleanupJobsBtn: document.getElementById('stopCleanupJobsBtn'),
  cleanupHint: document.getElementById('cleanupHint'),
  uploadHint: document.getElementById('uploadHint'),
  s3ConfigHint: document.getElementById('s3ConfigHint'),
  activeJobsCount: document.getElementById('activeJobsCount'),
  s3ConfigState: document.getElementById('s3ConfigState'),
  jobModalBackdrop: document.getElementById('s3JobModalBackdrop'),
  jobModalCloseBtn: document.getElementById('s3JobModalCloseBtn'),
  jobModalCancelBtn: document.getElementById('s3JobModalCancelBtn'),
  jobModalTitle: document.getElementById('s3JobModalTitle'),
  jobModalStatus: document.getElementById('s3JobModalStatus'),
  jobModalDryRun: document.getElementById('s3JobModalDryRun'),
  jobModalSummary: document.getElementById('s3JobModalSummary'),
  jobModalConfig: document.getElementById('s3JobModalConfig'),
  jobModalProgress: document.getElementById('s3JobModalProgress'),
  jobModalStats: document.getElementById('s3JobModalStats'),
  jobModalItems: document.getElementById('s3JobModalItems'),
  jobModalPrev: document.getElementById('s3JobModalPrev'),
  jobModalNext: document.getElementById('s3JobModalNext'),
  jobModalPageLabel: document.getElementById('s3JobModalPageLabel'),
};

const state = {
  apiToken: '',
  isCancellingJobs: false,
  isSubmitting: false,
  datasets: [],
  familyJobs: {
    upload: [],
    url_migration: [],
    state_cleanup: [],
  },
  unlocked: false,
  pollTimer: null,
  selectedJobId: null,
  selectedJobDetail: null,
  detailPage: 1,
  detailPageSize: DETAIL_PAGE_SIZE,
  activeFamily: 'upload',
  lastSummaryByFamily: {
    upload: null,
    url_migration: null,
    state_cleanup: null,
  },
  serverConfig: {
    bucket: '',
    prefix: '',
    endpoint_url: '',
    public_url: '',
    region_name: '',
    config_source: 'env',
  },
};

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function formatTime(value) {
  if (!value) return '—';
  const date = new Date(value * 1000);
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString('fr-FR');
}

function formatDuration(startedAt, endedAt) {
  if (!startedAt) return '—';
  const end = endedAt || Date.now() / 1000;
  const seconds = Math.max(0, Math.round(end - startedAt));
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}

function getFamilyConfig(family) {
  return JOB_FAMILY_CONFIG[family] || JOB_FAMILY_CONFIG.upload;
}

function allJobs() {
  return Object.values(state.familyJobs).flat();
}

function getActiveJobsForFamily(family) {
  return (state.familyJobs[family] || []).filter((job) => ACTIVE_JOB_STATUSES.includes(job.status));
}

async function init() {
  renderGlobalNav('s3');
  initPasswordFieldToggle({ input: els.passwordInput, button: els.passwordToggleBtn, hiddenLabel: 'Afficher', shownLabel: 'Masquer' });
  initTabs({
    buttons: [els.uploadTabBtn, els.migrationTabBtn, els.cleanupTabBtn],
    panels: Array.from(document.querySelectorAll('[data-tab-panel]')),
    active: 'upload',
  });
  [els.uploadTabBtn, els.migrationTabBtn, els.cleanupTabBtn].forEach((button) => {
    button?.addEventListener('click', () => {
      state.activeFamily = button.dataset.tabTarget;
      syncCancelButtons();
    });
  });
  bindEvents();
  hydrateApiToken();
  await loadDatasets();
  await hydrateAuthState();
  await refreshAllFamilyJobs();
  state.pollTimer = setInterval(refreshAllFamilyJobs, 5000);
}

function bindEvents() {
  els.s3AuthForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    await unlockPage();
  });
  els.previewUploadBtn?.addEventListener('click', () => submitFamilyJobAction('upload', true));
  els.startUploadBtn?.addEventListener('click', () => submitFamilyJobAction('upload', false));
  els.stopUploadJobsBtn?.addEventListener('click', () => stopActiveFamilyJobs('upload'));
  els.previewMigrationBtn?.addEventListener('click', () => submitFamilyJobAction('url_migration', true));
  els.startMigrationBtn?.addEventListener('click', () => submitFamilyJobAction('url_migration', false));
  els.stopMigrationJobsBtn?.addEventListener('click', () => stopActiveFamilyJobs('url_migration'));
  els.previewCleanupBtn?.addEventListener('click', () => submitFamilyJobAction('state_cleanup', true));
  els.startCleanupBtn?.addEventListener('click', () => submitFamilyJobAction('state_cleanup', false));
  els.stopCleanupJobsBtn?.addEventListener('click', () => stopActiveFamilyJobs('state_cleanup'));
  els.refreshS3JobsBtn?.addEventListener('click', refreshAllFamilyJobs);
  els.jobModalCloseBtn?.addEventListener('click', closeJobModal);
  els.jobModalCancelBtn?.addEventListener('click', cancelSelectedJob);
  els.jobModalPrev?.addEventListener('click', () => {
    if (state.selectedJobId && state.detailPage > 1 && !state.isSubmitting) {
      openJobDetails(state.selectedJobId, state.detailPage - 1);
    }
  });
  els.jobModalNext?.addEventListener('click', () => {
    const totalPages = state.selectedJobDetail?.total_pages || 1;
    if (state.selectedJobId && state.detailPage < totalPages && !state.isSubmitting) {
      openJobDetails(state.selectedJobId, state.detailPage + 1);
    }
  });
  els.jobModalBackdrop?.addEventListener('click', (event) => {
    if (event.target === els.jobModalBackdrop) closeJobModal();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeJobModal();
  });
}

function hydrateApiToken() {
  state.apiToken = (window.localStorage.getItem(API_TOKEN_STORAGE_KEY) || '').trim();
}

function getApiHeaders(extraHeaders = {}) {
  const headers = { ...extraHeaders };
  if (state.apiToken) headers.Authorization = `Bearer ${state.apiToken}`;
  return headers;
}

function isExpectedS3AuthError(error) {
  const message = String(error?.message || '');
  return Boolean(error?.expectedAuth || message.includes('Token API requis ou invalide') || message.includes('S3 admin authentication required'));
}

function setS3Busy(isBusy, { button = null, loadingText = 'Chargement…' } = {}) {
  state.isSubmitting = isBusy;
  document.body.classList.toggle('is-busy', isBusy);
  setElementsDisabled([
    els.s3DatasetSelect,
    els.s3BucketInput,
    els.s3PrefixInput,
    els.s3LimitInput,
    els.s3ConcurrencyInput,
    els.refreshS3JobsBtn,
    els.uploadTabBtn,
    els.migrationTabBtn,
    els.cleanupTabBtn,
    els.jobModalPrev,
    els.jobModalNext,
    ...Object.values(JOB_FAMILY_CONFIG).flatMap((config) => config.getButtons()),
  ], isBusy);
  if (button) setButtonLoading(button, isBusy, loadingText);
  syncCancelButtons();
}

function openModalShell() {
  els.jobModalBackdrop.classList.remove('hidden');
  els.jobModalBackdrop.classList.add('is-open');
  els.jobModalBackdrop.setAttribute('aria-hidden', 'false');
}

function closeJobModal() {
  state.selectedJobId = null;
  state.selectedJobDetail = null;
  state.detailPage = 1;
  els.jobModalBackdrop.classList.remove('is-open');
  els.jobModalBackdrop.classList.add('hidden');
  els.jobModalBackdrop.setAttribute('aria-hidden', 'true');
}

async function hydrateAuthState() {
  const response = await fetch('/api/s3/auth-check', { credentials: 'include', headers: getApiHeaders() });
  const payload = await response.json().catch(() => ({}));
  if (payload?.data?.authenticated) {
    state.unlocked = true;
    els.authGate.classList.add('hidden');
    els.s3Workspace.classList.remove('hidden');
    els.authStateLabel.textContent = 'Déverrouillé';
    els.authHint.textContent = '';
  }
}

async function unlockPage() {
  const password = els.passwordInput.value.trim();
  setS3Busy(true, { button: els.unlockBtn, loadingText: 'Vérification…' });
  try {
    const response = await fetch('/api/s3/auth', {
      headers: getApiHeaders({ 'Content-Type': 'application/json' }),
      method: 'POST',
      credentials: 'include',
      body: JSON.stringify({ password }),
    });
    const payload = await response.json().catch(() => ({}));
    if (response.status === 401 && payload?.error?.code === 'unauthorized' && !password) {
      els.authStateLabel.textContent = 'API verrouillée';
      els.authHint.textContent = 'Le token API Bearer est requis avant le mot de passe admin S3.';
      return;
    }
    if (!response.ok) {
      els.authStateLabel.textContent = 'Verrouillé';
      els.authHint.textContent = payload?.error?.message || 'Mot de passe admin S3 invalide';
      return;
    }
    state.unlocked = true;
    els.authGate.classList.add('hidden');
    els.s3Workspace.classList.remove('hidden');
    els.authStateLabel.textContent = 'Déverrouillé';
    els.authHint.textContent = 'Accès valide pour 1h.';
    await refreshAllFamilyJobs();
  } finally {
    setS3Busy(false, { button: els.unlockBtn });
  }
}

async function loadDatasets() {
  const response = await fetch('/api/datasets', { headers: getApiHeaders() });
  if (!response.ok) {
    const error = new Error(response.status === 401 ? 'Token API requis ou invalide' : 'Impossible de charger les datasets');
    if (response.status === 401) error.expectedAuth = true;
    throw error;
  }
  const payload = await response.json();
  state.datasets = (payload.datasets || []).filter((dataset) => ['shein', 'asos'].includes(dataset.id));
  const options = state.datasets.map((dataset) => `<option value="${dataset.id}">${escapeHtml(dataset.label)}</option>`).join('');
  els.s3DatasetSelect.innerHTML = options;
  if (state.datasets[0]) els.s3DatasetSelect.value = state.datasets[0].id;
}

async function refreshAllFamilyJobs() {
  if (!state.unlocked && !state.apiToken) return;
  const results = await Promise.all(Object.keys(JOB_FAMILY_CONFIG).map((family) => refreshFamilyJobs(family, { quietAuth: true })));
  if (results.some(Boolean)) updateGlobalStateFromFamilies();
}

async function refreshFamilyJobs(family, { quietAuth = false } = {}) {
  const config = getFamilyConfig(family);
  const response = await fetch(config.listEndpoint, { credentials: 'include', headers: getApiHeaders() });
  if (!response.ok) {
    if (response.status === 401 && !quietAuth) {
      state.unlocked = false;
      els.authGate.classList.remove('hidden');
      els.s3Workspace.classList.add('hidden');
      els.authStateLabel.textContent = 'Verrouillé';
      els.authHint.textContent = 'Session expirée. Re-déverrouille la page.';
    }
    return false;
  }
  const payload = await response.json();
  state.familyJobs[family] = payload.data || [];
  if (payload.config) state.serverConfig = {
    bucket: payload.config?.bucket || '',
    prefix: payload.config?.prefix || '',
    endpoint_url: payload.config?.endpoint_url || '',
    public_url: payload.config?.public_url || '',
    region_name: payload.config?.region_name || '',
    config_source: payload.config?.config_source || 'env',
  };
  renderFamilyJobList(family, state.familyJobs[family]);
  updateFamilyHint(family, state.familyJobs[family]);
  if (state.selectedJobId && state.familyJobs[family].some((job) => job.job_id === state.selectedJobId)) {
    await openJobDetails(state.selectedJobId, state.detailPage, { quiet: true });
  }
  return true;
}

function updateGlobalStateFromFamilies() {
  const jobs = allJobs();
  const active = jobs.filter((job) => ACTIVE_JOB_STATUSES.includes(job.status));
  els.activeJobsCount.textContent = String(active.length);
  els.s3BucketInput.value = state.serverConfig.bucket;
  els.s3PrefixInput.value = state.serverConfig.prefix;
  els.s3ConfigState.textContent = state.serverConfig.bucket || '—';
  els.s3ConfigHint.textContent = state.serverConfig.bucket
    ? `Config env active · bucket=${state.serverConfig.bucket}${state.serverConfig.prefix ? ` · prefix=${state.serverConfig.prefix}` : ''}${state.serverConfig.region_name ? ` · region=${state.serverConfig.region_name}` : ''}`
    : 'AWS_BUCKET est manquant dans l’environnement du serveur.';
  syncCancelButtons();
}

function renderFamilyBadge(job) {
  const family = getFamilyConfig(job.job_family || 'upload');
  return `<span class="job-pill job-family job-family-${escapeHtml(job.job_family || 'upload')}">${escapeHtml(family.title)}</span>`;
}

function renderJobCard(job) {
  const family = getFamilyConfig(job.job_family || 'upload');
  const dryRunBadge = job.dry_run ? '<span class="job-pill job-dry-run">dry-run</span>' : '';
  return `
    <article class="s3-job-card${state.selectedJobId === job.job_id ? ' is-selected' : ''}" data-job-id="${escapeHtml(job.job_id)}" role="button" tabindex="0">
      <div class="s3-job-header">
        <strong>${escapeHtml(job.job_id)}</strong>
        <div class="s3-job-header-pills">
          ${renderFamilyBadge(job)}
          ${dryRunBadge}
          <span class="job-pill job-${escapeHtml(job.status || 'queued')}">${escapeHtml(job.status || 'queued')}</span>
        </div>
      </div>
      <div class="s3-job-meta">
        <span>${escapeHtml(job.dataset_id || '')}</span>
        <span>${job.processed || 0}/${job.total || 0} traités</span>
        <span>${escapeHtml(family.metricLabel)}: ${job.uploaded || 0}</span>
        <span>Ignorés: ${job.skipped || 0}</span>
        <span>Erreurs: ${job.failed || 0}</span>
      </div>
    </article>
  `;
}

function bindJobCards(root) {
  root.querySelectorAll('.s3-job-card').forEach((card) => {
    const open = () => {
      const jobId = card.getAttribute('data-job-id');
      openJobDetails(jobId, 1);
    };
    card.addEventListener('click', open);
    card.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        open();
      }
    });
  });
}

function renderFamilyJobList(family, jobs) {
  const config = getFamilyConfig(family);
  const root = config.getListEl();
  root.innerHTML = jobs.length ? jobs.map(renderJobCard).join('') : `<div class="s3-job-empty">${escapeHtml(config.emptyText)}</div>`;
  bindJobCards(root);
}

function updateFamilyHint(family, jobs) {
  const config = getFamilyConfig(family);
  const hintEl = config.getHintEl();
  if (!hintEl) return;
  const latest = jobs[0];
  if (!latest) {
    hintEl.textContent = config.hintDefault;
    return;
  }
  const mode = latest.dry_run ? 'dry-run' : 'write';
  hintEl.textContent = `Dernier ${config.title.toLowerCase()}: ${latest.status} · ${latest.processed || 0}/${latest.total || 0} · ${mode}`;
}

async function openJobDetails(jobId, page = 1, options = {}) {
  if (!jobId) return;
  state.selectedJobId = jobId;
  state.detailPage = page;
  openModalShell();
  if (!options.quiet) renderJobDetailsLoading(jobId, page);
  const response = await fetch(`/api/s3/jobs/${encodeURIComponent(jobId)}?page=${page}&page_size=${state.detailPageSize}`, { credentials: 'include', headers: getApiHeaders() });
  if (!response.ok) return;
  const payload = await response.json();
  state.selectedJobDetail = payload.data;
  renderJobDetailsModal(payload.data);
}

function syncCancelButtons() {
  Object.entries(JOB_FAMILY_CONFIG).forEach(([family, config]) => {
    const activeJobs = getActiveJobsForFamily(family);
    const btn = config.getStopButton();
    if (!btn) return;
    btn.disabled = state.isCancellingJobs || state.isSubmitting || activeJobs.length === 0;
    btn.textContent = state.isCancellingJobs
      ? 'Annulation…'
      : activeJobs.length > 1 ? config.stopButtonTextMany : config.stopButtonTextSingle;
  });
  const selectedStatus = state.selectedJobDetail?.job?.status || allJobs().find((job) => job.job_id === state.selectedJobId)?.status;
  const selectedIsActive = ACTIVE_JOB_STATUSES.includes(selectedStatus);
  if (els.jobModalCancelBtn) {
    els.jobModalCancelBtn.disabled = state.isCancellingJobs || state.isSubmitting || !state.selectedJobId || !selectedIsActive;
    els.jobModalCancelBtn.textContent = state.isCancellingJobs ? 'Annulation…' : 'Annuler ce job';
  }
}

function renderJobDetailsLoading(jobId, page) {
  els.jobModalTitle.textContent = jobId || 'Job';
  els.jobModalStatus.textContent = 'loading';
  els.jobModalDryRun.classList.add('hidden');
  syncCancelButtons();
  els.jobModalSummary.innerHTML = `
    <div class="s3-job-kpi"><span>Traités</span><strong>…</strong></div>
    <div class="s3-job-kpi"><span>Réussis / preview</span><strong>…</strong></div>
    <div class="s3-job-kpi"><span>Ignorés / Erreurs</span><strong>…</strong></div>
  `;
  els.jobModalConfig.innerHTML = `
    <div><span>Famille</span><strong>—</strong></div>
    <div><span>Type</span><strong>—</strong></div>
    <div><span>Bucket</span><strong>—</strong></div>
    <div><span>Prefix</span><strong>—</strong></div>
    <div><span>Source filter</span><strong>—</strong></div>
    <div><span>Démarré</span><strong>—</strong></div>
    <div><span>Terminé</span><strong>—</strong></div>
    <div><span>Durée</span><strong>—</strong></div>
    <div><span>Dernier message</span><strong>Chargement…</strong></div>
  `;
  els.jobModalProgress.innerHTML = `
    <section class="s3-job-progress-panel is-loading" aria-label="Chargement de la progression">
      <div class="s3-job-progress-head">
        <div>
          <p class="eyebrow">Progression par item</p>
          <h3>Chargement…</h3>
          <p class="muted small" style="margin: 6px 0 0;">Récupération des détails du job.</p>
        </div>
        <div class="s3-job-progress-hero">
          <div class="s3-job-progress-meter"><div class="s3-job-progress-meter-fill" style="width: 18%"></div></div>
          <div class="s3-job-progress-meta"><span>…</span><span>loading</span></div>
        </div>
      </div>
      <div class="s3-job-progress-track">
        ${Array.from({ length: 6 }, (_, index) => `
          <article class="s3-job-progress-item is-pending">
            <div class="s3-job-progress-item-top">
              <span class="s3-job-progress-index">${String(index + 1).padStart(2, '0')}</span>
              <span class="job-pill job-queued">Chargement</span>
            </div>
            <strong class="s3-job-progress-title">Chargement…</strong>
            <span class="s3-job-progress-subtitle">Le détail du job arrive.</span>
            <div class="s3-job-progress-bar"><span style="width: 18%"></span></div>
            <div class="s3-job-progress-footer">Patience…</div>
          </article>
        `).join('')}
      </div>
    </section>
  `;
  els.jobModalStats.innerHTML = `
    <div class="s3-job-kpi"><span>Page</span><strong>${page || 1}/…</strong></div>
    <div class="s3-job-kpi"><span>Éléments sur page</span><strong>…</strong></div>
    <div class="s3-job-kpi"><span>Total éléments</span><strong>…</strong></div>
    <div class="s3-job-kpi"><span>Déjà présents</span><strong>…</strong></div>
    <div class="s3-job-kpi"><span>URL manquante</span><strong>…</strong></div>
    <div class="s3-job-kpi"><span>Timeout / 403</span><strong>…</strong></div>
  `;
  els.jobModalPageLabel.textContent = `Page ${page || 1} / …`;
  els.jobModalPrev.disabled = true;
  els.jobModalNext.disabled = true;
  els.jobModalItems.innerHTML = '<div class="s3-job-empty-details">Chargement des éléments…</div>';
}

function renderJobDetailsModal(detail) {
  if (!detail) return;
  const job = detail.job || {};
  const items = detail.items || [];
  const totalPages = detail.total_pages || 1;
  const totalItems = Math.max(job.total || 0, detail.total_items || 0, items.length || 0);
  const processedCount = Math.min(job.processed || items.length || 0, totalItems);
  const runningCount = job.status === 'running' && processedCount < totalItems ? Math.min(Math.max(job.concurrency || 1, 1), totalItems - processedCount) : 0;
  const family = getFamilyConfig(job.job_family || 'upload');
  const reasons = items.reduce((acc, item) => {
    const message = String(item.message || '').toLowerCase();
    if (item.status === 'uploaded' || item.status === PREVIEW_STATUS) acc.uploaded += 1;
    else if (message.includes('already exists')) acc.exists += 1;
    else if (message.includes('no source url')) acc.noSource += 1;
    else if (message.includes('timeout')) acc.timeout += 1;
    else if (message.includes('403') || message.includes('forbidden')) acc.forbidden += 1;
    else if (item.status === 'skipped') acc.skipped += 1;
    else if (item.status === 'failed') acc.failed += 1;
    return acc;
  }, { uploaded: 0, exists: 0, noSource: 0, timeout: 0, forbidden: 0, skipped: 0, failed: 0 });

  els.jobModalTitle.textContent = job.job_id || 'Job';
  els.jobModalStatus.textContent = job.status || 'queued';
  els.jobModalStatus.className = `job-pill job-${escapeHtml(job.status || 'queued')}`;
  els.jobModalDryRun.classList.toggle('hidden', !job.dry_run);
  els.jobModalSummary.innerHTML = `
    <div class="s3-job-kpi"><span>Traités</span><strong>${job.processed || 0}/${job.total || 0}</strong></div>
    <div class="s3-job-kpi"><span>${escapeHtml(family.progressSuccessLabel)}</span><strong>${job.uploaded || 0}</strong></div>
    <div class="s3-job-kpi"><span>Ignorés / Erreurs</span><strong>${(job.skipped || 0)} / ${(job.failed || 0)}</strong></div>
  `;
  els.jobModalConfig.innerHTML = `
    <div><span>Famille</span><strong>${escapeHtml(family.detailTypeLabel)}</strong></div>
    <div><span>Type</span><strong>${escapeHtml(job.kind || 'upload')}</strong></div>
    <div><span>Bucket</span><strong>${escapeHtml(job.bucket || '—')}</strong></div>
    <div><span>Prefix</span><strong>${escapeHtml(job.prefix || '—')}</strong></div>
    <div><span>Source filter</span><strong>${escapeHtml(job.source_filter || '—')}</strong></div>
    <div><span>Démarré</span><strong>${escapeHtml(formatTime(job.started_at))}</strong></div>
    <div><span>Terminé</span><strong>${escapeHtml(formatTime(job.ended_at))}</strong></div>
    <div><span>Durée</span><strong>${escapeHtml(formatDuration(job.started_at, job.ended_at))}</strong></div>
    <div><span>Dernier message</span><strong>${escapeHtml(job.last_message || job.error || '—')}</strong></div>
  `;
  els.jobModalProgress.innerHTML = renderJobProgress(detail, { totalItems, processedCount, runningCount });
  els.jobModalStats.innerHTML = `
    <div class="s3-job-kpi"><span>Page</span><strong>${detail.page || 1}/${totalPages}</strong></div>
    <div class="s3-job-kpi"><span>Éléments sur page</span><strong>${items.length}</strong></div>
    <div class="s3-job-kpi"><span>Total éléments</span><strong>${detail.total_items || 0}</strong></div>
    <div class="s3-job-kpi"><span>Déjà présents</span><strong>${reasons.exists}</strong></div>
    <div class="s3-job-kpi"><span>URL manquante</span><strong>${reasons.noSource}</strong></div>
    <div class="s3-job-kpi"><span>Timeout / 403</span><strong>${reasons.timeout + reasons.forbidden}</strong></div>
  `;
  els.jobModalPageLabel.textContent = `Page ${detail.page || 1} / ${totalPages}`;
  els.jobModalPrev.disabled = (detail.page || 1) <= 1 || state.isSubmitting;
  els.jobModalNext.disabled = (detail.page || 1) >= totalPages || state.isSubmitting;
  syncCancelButtons();

  if (!items.length) {
    els.jobModalItems.innerHTML = '<div class="s3-job-empty-details">Aucun élément sur cette page.</div>';
    return;
  }

  els.jobModalItems.innerHTML = items.map((item, index) => {
    const tone = getItemTone(item);
    const note = getItemNote(item, tone);
    const message = String(item.message || '—');
    const indexLabel = String(((detail.page - 1) * state.detailPageSize) + index + 1).padStart(2, '0');
    return `
    <article class="s3-job-item is-${tone}">
      <div class="s3-job-item-top">
        <div class="s3-job-item-title">#${indexLabel} · ${escapeHtml(item.name || item.goods_id || item.product_id || 'Élément')}</div>
        <div class="s3-actions-inline">
          ${item.status === PREVIEW_STATUS ? '<span class="job-pill job-dry-run">dry-run</span>' : ''}
          <span class="job-pill job-${escapeHtml(item.status || 'skipped')}">${escapeHtml(item.status || 'skipped')}</span>
        </div>
      </div>
      <div class="s3-job-item-meta">
        <span><strong>ID:</strong> ${escapeHtml(item.goods_id || '—')}</span>
        <span><strong>Produit:</strong> ${escapeHtml(item.product_id || '—')}</span>
        <span><strong>Date:</strong> ${escapeHtml(formatTime(item.timestamp))}</span>
        <span><strong>Key:</strong> ${escapeHtml(item.key || '—')}</span>
      </div>
      <div class="s3-job-item-progress is-${tone}">
        <span style="width:${tone === 'error' || tone === 'warning' || tone === 'success' ? '100%' : '18%'}"></span>
      </div>
      <div class="s3-job-item-alert is-${tone}">${escapeHtml(note)}</div>
      <div class="s3-job-item-message is-${tone}">${escapeHtml(message)}</div>
      ${item.old_s3_url ? `<div class="s3-job-item-message"><strong>Ancienne URL:</strong> ${escapeHtml(item.old_s3_url)}</div>` : ''}
      ${item.new_s3_url ? `<div class="s3-job-item-message"><strong>Nouvelle URL:</strong> ${escapeHtml(item.new_s3_url)}</div>` : ''}
      ${Array.isArray(item.changed_fields) && item.changed_fields.length ? `<div class="s3-job-item-message"><strong>Champs:</strong> ${escapeHtml(item.changed_fields.join(', '))}</div>` : ''}
      ${item.reason ? `<div class="s3-job-item-message"><strong>Raison:</strong> ${escapeHtml(item.reason)}</div>` : ''}
      ${item.backup_path ? `<div class="s3-job-item-message"><strong>Backup:</strong> ${escapeHtml(item.backup_path)}</div>` : ''}
      ${item.source_url ? `<div class="s3-job-item-message"><a href="${escapeHtml(item.source_url)}" target="_blank" rel="noreferrer">Ouvrir la source</a></div>` : ''}
    </article>
  `; }).join('');
}

function renderJobProgress(detail, { totalItems, processedCount, runningCount }) {
  const job = detail.job || {};
  const items = detail.items || [];
  const total = Math.max(1, totalItems || job.total || items.length || 1);
  const processed = Math.min(processedCount || 0, total);
  const pct = Math.min(100, Math.round((processed / total) * 100));
  const page = Math.max(1, detail.page || 1);
  const pageSize = Math.max(1, detail.page_size || state.detailPageSize || items.length || 1);
  const pageStart = (page - 1) * pageSize;
  const pageEndExclusive = Math.min(total, pageStart + pageSize);
  const activeWindowEnd = Math.min(total, processed + runningCount);

  const slots = items.map((item, index) => {
    const absoluteIndex = pageStart + index;
    const isDone = absoluteIndex < processed;
    const isActive = job.status === 'running' && absoluteIndex >= processed && absoluteIndex < activeWindowEnd;
    const tone = item ? getItemTone(item) : (isActive ? 'running' : 'pending');
    const label = item ? getItemNote(item, tone) : (isActive ? 'En cours' : 'En attente');
    const title = item?.name || item?.goods_id || item?.product_id || `Item ${absoluteIndex + 1}`;
    const barPct = isDone ? 100 : isActive ? 62 : 12;
    const statusClass = isDone ? `is-${tone}` : isActive ? 'is-running' : 'is-pending';
    const statusText = isDone ? (tone === 'error' ? 'Erreur' : tone === 'warning' ? 'Ignoré' : 'Terminé') : (isActive ? 'Traitement' : 'En attente');
    const subText = item?.message ? escapeHtml(String(item.message)) : (isActive ? 'L’item est en cours de traitement.' : 'Pas encore traité.');
    return `
      <article class="s3-job-progress-item ${statusClass}" aria-label="Item ${absoluteIndex + 1}, ${statusText}">
        <div class="s3-job-progress-item-top">
          <span class="s3-job-progress-index">${String(absoluteIndex + 1).padStart(2, '0')}</span>
          <span class="job-pill job-${escapeHtml(item?.status || (isActive ? 'running' : 'queued'))}">${escapeHtml(statusText)}</span>
        </div>
        <strong class="s3-job-progress-title">${escapeHtml(title)}</strong>
        <span class="s3-job-progress-subtitle">${subText}</span>
        <div class="s3-job-progress-bar">
          <span style="width:${barPct}%"></span>
        </div>
        <div class="s3-job-progress-footer">${escapeHtml(label)}</div>
      </article>
    `;
  }).join('');

  return `
    <section class="s3-job-progress-panel" aria-label="Progression des items">
      <div class="s3-job-progress-head">
        <div>
          <p class="eyebrow">Progression par item</p>
          <h3>${processed}/${total} traités</h3>
          <p class="muted small" style="margin: 6px 0 0;">Affichage paginé des items ${pageStart + 1}-${pageEndExclusive} sur ${total}. On évite ainsi de rendre tout le job d’un coup dans le navigateur.</p>
        </div>
        <div class="s3-job-progress-hero">
          <div class="s3-job-progress-meter">
            <div class="s3-job-progress-meter-fill" style="width:${pct}%"></div>
          </div>
          <div class="s3-job-progress-meta">
            <span>${pct}%</span>
            <span>${job.status || 'queued'}</span>
          </div>
        </div>
      </div>
      <div class="s3-job-progress-track">${slots}</div>
    </section>
  `;
}

function getItemTone(item) {
  const message = String(item?.message || '').toLowerCase();
  if (item?.status === 'failed' || message.includes('timeout') || message.includes('forbidden') || message.includes('no source url') || message.includes('all candidate urls failed') || message.includes('preview failed')) {
    return 'error';
  }
  if (item?.status === PREVIEW_STATUS) return 'success';
  if (item?.status === 'partial' || message.includes('partial success') || message.includes('partial availability')) return 'warning';
  if (item?.status === 'skipped' || message.includes('already exists')) return 'warning';
  return 'success';
}

function getItemNote(item, tone) {
  const message = String(item?.message || '').toLowerCase();
  if (item?.status === PREVIEW_STATUS) return 'Dry-run';
  if (item?.status === 'partial' || message.includes('partial success') || message.includes('partial availability')) return 'Succès partiel';
  if (tone === 'error') {
    if (message.includes('no source url')) return 'Échec: URL source manquante';
    if (message.includes('timeout')) return 'Échec: timeout réseau';
    if (message.includes('forbidden')) return 'Échec: accès refusé';
    return 'Échec de traitement';
  }
  if (tone === 'warning') {
    if (message.includes('already exists')) return 'Ignoré: déjà présent sur S3';
    return 'Ignoré';
  }
  return 'Succès';
}

async function cancelJob(jobId) {
  if (!jobId) return;
  await fetch(`/api/s3/jobs/${encodeURIComponent(jobId)}/cancel`, { method: 'POST', credentials: 'include', headers: getApiHeaders() });
}

async function cancelSelectedJob() {
  if (!state.selectedJobId || state.isCancellingJobs || state.isSubmitting) return;
  state.isCancellingJobs = true;
  syncCancelButtons();
  try {
    await cancelJob(state.selectedJobId);
    await refreshAllFamilyJobs();
    if (state.selectedJobId) await openJobDetails(state.selectedJobId, state.detailPage, { quiet: true });
  } finally {
    state.isCancellingJobs = false;
    syncCancelButtons();
  }
}

async function submitFamilyJobAction(family, dryRun) {
  const config = getFamilyConfig(family);
  const button = dryRun ? config.getButtons()[0] : config.getButtons()[1];
  const loadingText = dryRun ? 'Prévisualisation…' : `${config.title}…`;
  setS3Busy(true, { button, loadingText });
  try {
    const hintEl = config.getHintEl();
    if (hintEl) hintEl.textContent = dryRun ? 'Prévisualisation en cours…' : `${config.title} en préparation…`;
    const summary = state.lastSummaryByFamily[family];
    if (!dryRun) {
      setS3Busy(false, { button });
      const confirmed = window.confirm(config.confirmStart(summary || { total: 0 }));
      if (!confirmed) return;
      setS3Busy(true, { button, loadingText });
    }
    const body = { ...(config.buildCreateBody() || {}), dry_run: dryRun };
    const response = await fetch(config.createEndpoint, {
      method: 'POST',
      credentials: 'include',
      headers: getApiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify(body),
    });
    if (!response.ok) throw new Error(`Impossible de lancer ${config.title.toLowerCase()} (${response.status})`);
    const payload = await response.json();
    const createdJob = payload?.data || null;
    if (dryRun && createdJob) {
      state.lastSummaryByFamily[family] = {
        total: createdJob.total || 0,
        sample_limit: createdJob.limit || 0,
        sample: createdJob.items || [],
      };
    }
    if (hintEl) {
      const total = (dryRun ? state.lastSummaryByFamily[family]?.total : summary?.total) ?? createdJob?.total ?? 0;
      hintEl.textContent = dryRun
        ? `Preview ${config.title.toLowerCase()} lancé · ${total} item(s) évalué(s).`
        : `${config.title} lancé${total ? ` · ${total} item(s) visé(s)` : ''}.`;
    }
    await refreshFamilyJobs(family);
    updateGlobalStateFromFamilies();
    if (createdJob?.job_id) await openJobDetails(createdJob.job_id, 1);
  } finally {
    setS3Busy(false, { button });
  }
}

async function stopActiveFamilyJobs(family) {
  const config = getFamilyConfig(family);
  const activeJobs = getActiveJobsForFamily(family);
  if (!activeJobs.length || state.isCancellingJobs || state.isSubmitting) return;
  const confirmed = window.confirm(
    activeJobs.length > 1
      ? `Annuler ${activeJobs.length} jobs ${config.title.toLowerCase()} actifs ?`
      : `Annuler le job ${config.title.toLowerCase()} actif ?`
  );
  if (!confirmed) return;
  state.isCancellingJobs = true;
  syncCancelButtons();
  try {
    await Promise.all(activeJobs.map((job) => cancelJob(job.job_id)));
    await refreshFamilyJobs(family);
    updateGlobalStateFromFamilies();
    if (state.selectedJobId) await openJobDetails(state.selectedJobId, state.detailPage, { quiet: true });
  } finally {
    state.isCancellingJobs = false;
    syncCancelButtons();
  }
}

window.addEventListener('beforeunload', () => {
  if (state.pollTimer) clearInterval(state.pollTimer);
});

init().catch((error) => {
  if (!isExpectedS3AuthError(error)) {
    console.error(error);
    els.authStateLabel.textContent = 'Erreur';
    els.authHint.textContent = error.message;
  } else {
    els.authStateLabel.textContent = 'API verrouillée';
    els.authHint.textContent = 'Renseigne d’abord le token API du dashboard, puis le mot de passe admin S3.';
  }
});
