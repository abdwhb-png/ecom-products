const els = {
  authGate: document.getElementById('authGate'),
  authStateLabel: document.getElementById('authStateLabel'),
  passwordInput: document.getElementById('passwordInput'),
  unlockBtn: document.getElementById('unlockBtn'),
  authHint: document.getElementById('authHint'),
  s3Workspace: document.getElementById('s3Workspace'),
  s3DatasetSelect: document.getElementById('s3DatasetSelect'),
  s3BucketInput: document.getElementById('s3BucketInput'),
  s3PrefixInput: document.getElementById('s3PrefixInput'),
  s3LimitInput: document.getElementById('s3LimitInput'),
  s3ConcurrencyInput: document.getElementById('s3ConcurrencyInput'),
  startS3JobBtn: document.getElementById('startS3JobBtn'),
  stopS3JobBtn: document.getElementById('stopS3JobBtn'),
  refreshS3JobsBtn: document.getElementById('refreshS3JobsBtn'),
  s3JobsList: document.getElementById('s3JobsList'),
  s3ConfigHint: document.getElementById('s3ConfigHint'),
  activeJobsCount: document.getElementById('activeJobsCount'),
  s3ConfigState: document.getElementById('s3ConfigState'),
  jobModalBackdrop: document.getElementById('s3JobModalBackdrop'),
  jobModalCloseBtn: document.getElementById('s3JobModalCloseBtn'),
  jobModalTitle: document.getElementById('s3JobModalTitle'),
  jobModalStatus: document.getElementById('s3JobModalStatus'),
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
  datasets: [],
  s3Jobs: [],
  unlocked: false,
  pollTimer: null,
  selectedJobId: null,
  selectedJobDetail: null,
  detailPage: 1,
  detailPageSize: 6,
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

async function init() {
  bindEvents();
  await loadDatasets();
  await hydrateAuthState();
  await refreshS3Jobs();
  state.pollTimer = setInterval(refreshS3Jobs, 5000);
}

function bindEvents() {
  els.unlockBtn.addEventListener('click', unlockPage);
  els.passwordInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') unlockPage();
  });
  els.startS3JobBtn.addEventListener('click', startS3Job);
  els.stopS3JobBtn.addEventListener('click', stopActiveS3Job);
  els.refreshS3JobsBtn.addEventListener('click', refreshS3Jobs);
  els.jobModalCloseBtn.addEventListener('click', closeJobModal);
  els.jobModalPrev.addEventListener('click', () => {
    if (state.selectedJobId && state.detailPage > 1) {
      openJobDetails(state.selectedJobId, state.detailPage - 1);
    }
  });
  els.jobModalNext.addEventListener('click', () => {
    const totalPages = state.selectedJobDetail?.total_pages || 1;
    if (state.selectedJobId && state.detailPage < totalPages) {
      openJobDetails(state.selectedJobId, state.detailPage + 1);
    }
  });
  els.jobModalBackdrop.addEventListener('click', (event) => {
    if (event.target === els.jobModalBackdrop) closeJobModal();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeJobModal();
  });
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
  const response = await fetch('/api/s3/auth-check', { credentials: 'include' });
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
  const response = await fetch('/api/s3/auth', {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password }),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    els.authStateLabel.textContent = 'Verrouillé';
    els.authHint.textContent = payload?.error?.message || 'Mot de passe invalide';
    return;
  }
  state.unlocked = true;
  els.authGate.classList.add('hidden');
  els.s3Workspace.classList.remove('hidden');
  els.authStateLabel.textContent = 'Déverrouillé';
  els.authHint.textContent = 'Accès valide pour 1h.';
  await refreshS3Jobs();
}

async function loadDatasets() {
  const response = await fetch('/api/datasets');
  const payload = await response.json();
  state.datasets = (payload.datasets || []).filter((dataset) => ['shein', 'asos'].includes(dataset.id));
  const options = state.datasets.map((dataset) => `<option value="${dataset.id}">${escapeHtml(dataset.label)}</option>`).join('');
  els.s3DatasetSelect.innerHTML = options;
  if (state.datasets[0]) els.s3DatasetSelect.value = state.datasets[0].id;
}

async function refreshS3Jobs() {
  const response = await fetch('/api/s3/jobs', { credentials: 'include' });
  if (!response.ok) {
    if (response.status === 401) {
      state.unlocked = false;
      els.authGate.classList.remove('hidden');
      els.s3Workspace.classList.add('hidden');
      els.authStateLabel.textContent = 'Verrouillé';
      els.authHint.textContent = 'Session expirée. Re-déverrouille la page.';
    }
    return;
  }
  const payload = await response.json();
  state.s3Jobs = payload.data || [];
  renderS3Jobs(state.s3Jobs);
  if (state.selectedJobId) {
    const selected = state.s3Jobs.find((job) => job.job_id === state.selectedJobId);
    if (selected) {
      await openJobDetails(state.selectedJobId, state.detailPage, { quiet: true });
    }
  }
  const active = state.s3Jobs.filter((job) => ['running', 'queued', 'cancel_requested'].includes(job.status));
  els.activeJobsCount.textContent = String(active.length);
  els.s3ConfigState.textContent = payload.config?.bucket ? payload.config.bucket : '—';
  els.s3ConfigHint.textContent = payload.config?.bucket ? `Bucket configuré: ${payload.config.bucket}` : 'Configure ton bucket ici, puis lance un job.';
  if (payload.config?.bucket) {
    els.s3BucketInput.value = payload.config.bucket;
  }
  if (payload.config?.prefix) {
    els.s3PrefixInput.value = payload.config.prefix;
  }
}

function renderS3Jobs(jobs) {
  if (!jobs.length) {
    els.s3JobsList.innerHTML = '<div class="s3-job-empty">Aucun job S3 pour le moment.</div>';
    return;
  }
  els.s3JobsList.innerHTML = jobs.map((job) => `
    <article class="s3-job-card${state.selectedJobId === job.job_id ? ' is-selected' : ''}" data-job-id="${escapeHtml(job.job_id)}" role="button" tabindex="0">
      <div class="s3-job-header">
        <strong>${escapeHtml(job.job_id)}</strong>
        <span class="job-pill job-${escapeHtml(job.status || 'queued')}">${escapeHtml(job.status || 'queued')}</span>
      </div>
      <div class="s3-job-meta">
        <span>${escapeHtml(job.dataset_id || '')}</span>
        <span>${job.processed || 0}/${job.total || 0} traités</span>
        <span>Uploadés: ${job.uploaded || 0}</span>
        <span>Ignorés: ${job.skipped || 0}</span>
        <span>Erreurs: ${job.failed || 0}</span>
      </div>
    </article>
  `).join('');

  els.s3JobsList.querySelectorAll('.s3-job-card').forEach((card) => {
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

async function openJobDetails(jobId, page = 1, options = {}) {
  if (!jobId) return;
  state.selectedJobId = jobId;
  state.detailPage = page;
  openModalShell();
  if (!options.quiet) renderJobDetailsLoading(jobId, page);
  const response = await fetch(`/api/s3/jobs/${encodeURIComponent(jobId)}?page=${page}&page_size=${state.detailPageSize}`, { credentials: 'include' });
  if (!response.ok) return;
  const payload = await response.json();
  state.selectedJobDetail = payload.data;
  renderJobDetailsModal(payload.data);
}

function renderJobDetailsLoading(jobId, page) {
  els.jobModalTitle.textContent = jobId || 'Job';
  els.jobModalStatus.textContent = 'loading';
  els.jobModalSummary.innerHTML = `
    <div class="s3-job-kpi"><span>Traités</span><strong>…</strong></div>
    <div class="s3-job-kpi"><span>Réussis</span><strong>…</strong></div>
    <div class="s3-job-kpi"><span>Ignorés / Erreurs</span><strong>…</strong></div>
  `;
  els.jobModalConfig.innerHTML = `
    <div><span>Dataset</span><strong>—</strong></div>
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
  const reasons = items.reduce((acc, item) => {
    const message = String(item.message || '').toLowerCase();
    if (item.status === 'uploaded') acc.uploaded += 1;
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
  els.jobModalSummary.innerHTML = `
    <div class="s3-job-kpi"><span>Traités</span><strong>${job.processed || 0}/${job.total || 0}</strong></div>
    <div class="s3-job-kpi"><span>Réussis</span><strong>${job.uploaded || 0}</strong></div>
    <div class="s3-job-kpi"><span>Ignorés / Erreurs</span><strong>${(job.skipped || 0)} / ${(job.failed || 0)}</strong></div>
  `;
  els.jobModalConfig.innerHTML = `
    <div><span>Dataset</span><strong>${escapeHtml(job.dataset_id || '—')}</strong></div>
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
  els.jobModalPrev.disabled = (detail.page || 1) <= 1;
  els.jobModalNext.disabled = (detail.page || 1) >= totalPages;

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
        <span class="job-pill job-${escapeHtml(item.status || 'skipped')}">${escapeHtml(item.status || 'skipped')}</span>
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
      ${item.source_url ? `<div class="s3-job-item-message"><a href="${escapeHtml(item.source_url)}" target="_blank" rel="noreferrer">Ouvrir la source</a></div>` : ''}
    </article>
  `;}).join('');
}

function renderJobProgress(detail, { totalItems, processedCount, runningCount }) {
  const job = detail.job || {};
  const items = detail.items || [];
  const total = Math.max(1, totalItems || job.total || items.length || 1);
  const processed = Math.min(processedCount || 0, total);
  const pct = Math.min(100, Math.round((processed / total) * 100));
  const activeWindowEnd = Math.min(total, processed + runningCount);

  const slots = Array.from({ length: total }, (_, index) => {
    const item = items[index] || null;
    const isDone = index < processed && Boolean(item);
    const isActive = job.status === 'running' && index >= processed && index < activeWindowEnd;
    const isPending = !isDone && !isActive;
    const tone = item ? getItemTone(item) : (isActive ? 'running' : 'pending');
    const label = item ? getItemNote(item, tone) : (isActive ? 'En cours' : 'En attente');
    const title = item?.name || item?.goods_id || item?.product_id || `Item ${index + 1}`;
    const barPct = isDone ? 100 : isActive ? 62 : 12;
    const statusClass = isDone ? `is-${tone}` : isActive ? 'is-running' : 'is-pending';
    const statusText = isDone ? (tone === 'error' ? 'Erreur' : tone === 'warning' ? 'Ignoré' : 'Terminé') : (isActive ? 'Traitement' : 'En attente');
    const subText = item?.message ? escapeHtml(String(item.message)) : (isActive ? 'L’item est en cours de traitement.' : 'Pas encore traité.');
    return `
      <article class="s3-job-progress-item ${statusClass}" aria-label="Item ${index + 1}, ${statusText}">
        <div class="s3-job-progress-item-top">
          <span class="s3-job-progress-index">${String(index + 1).padStart(2, '0')}</span>
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
          <p class="muted small" style="margin: 6px 0 0;">Chaque item a son état visible, même quand il est encore en file ou en cours.</p>
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
  if (item?.status === 'failed' || message.includes('timeout') || message.includes('forbidden') || message.includes('no source url') || message.includes('all candidate urls failed')) {
    return 'error';
  }
  if (item?.status === 'skipped' || message.includes('already exists')) {
    return 'warning';
  }
  return 'success';
}

function getItemNote(item, tone) {
  if (tone === 'error') {
    if (String(item?.message || '').toLowerCase().includes('no source url')) return 'Échec: URL source manquante';
    if (String(item?.message || '').toLowerCase().includes('timeout')) return 'Échec: timeout réseau';
    if (String(item?.message || '').toLowerCase().includes('forbidden')) return 'Échec: accès refusé';
    return 'Échec de traitement';
  }
  if (tone === 'warning') {
    if (String(item?.message || '').toLowerCase().includes('already exists')) return 'Ignoré: déjà présent sur S3';
    return 'Ignoré';
  }
  return 'Succès';
}

async function startS3Job() {
  const body = {
    dataset_id: els.s3DatasetSelect.value,
    bucket: els.s3BucketInput.value.trim(),
    prefix: els.s3PrefixInput.value.trim(),
    limit: Number.parseInt(els.s3LimitInput.value, 10) || 50,
    concurrency: Number.parseInt(els.s3ConcurrencyInput.value, 10) || 4,
  };
  const response = await fetch('/api/s3/jobs', {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`Impossible de lancer le job S3 (${response.status})`);
  await refreshS3Jobs();
}

async function stopActiveS3Job() {
  const active = state.s3Jobs.find((job) => ['running', 'queued', 'cancel_requested'].includes(job.status));
  if (!active) return;
  await fetch(`/api/s3/jobs/${encodeURIComponent(active.job_id)}/cancel`, { method: 'POST', credentials: 'include' });
  await refreshS3Jobs();
}

window.addEventListener('beforeunload', () => {
  if (state.pollTimer) clearInterval(state.pollTimer);
});

init().catch((error) => {
  console.error(error);
  els.authStateLabel.textContent = 'Erreur';
  els.authHint.textContent = error.message;
});
