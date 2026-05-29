function renderGlobalNav(current = 'dashboard') {
  const nav = document.createElement('div');
  nav.className = 'global-nav-shell';
  nav.innerHTML = `
    <nav class="global-nav" aria-label="Navigation principale">
      <div class="global-nav-brand">
        <strong>Fast Fashion Dashboard</strong>
        <span>Catalogue, contrôle S3 et docs API</span>
      </div>
      <div class="global-nav-links">
        <a class="global-nav-link ${current === 'dashboard' ? 'is-active' : ''}" href="/">
          <span>Dashboard</span>
          <small>Catalogue & filtres</small>
        </a>
        <a class="global-nav-link ${current === 's3' ? 'is-active' : ''}" href="/s3">
          <span>S3 Control</span>
          <small>Uploads & migrations</small>
        </a>
        <a class="global-nav-link ${current === 'docs' ? 'is-active' : ''}" href="/docs">
          <span>Docs API</span>
          <small>Contrats & endpoints</small>
        </a>
      </div>
    </nav>
  `;
  document.body.prepend(nav);
}

function initPasswordFieldToggle({ input, button, hiddenLabel = 'Afficher', shownLabel = 'Masquer' }) {
  if (!input || !button) return;
  const sync = () => {
    const shown = input.type === 'text';
    button.textContent = shown ? shownLabel : hiddenLabel;
    button.setAttribute('aria-pressed', String(shown));
  };
  button.addEventListener('click', () => {
    input.type = input.type === 'password' ? 'text' : 'password';
    sync();
    input.focus();
    const end = input.value.length;
    input.setSelectionRange?.(end, end);
  });
  sync();
}

function setButtonLoading(button, isLoading, loadingText = 'Chargement…') {
  if (!button) return;
  if (!button.dataset.originalText) {
    button.dataset.originalText = button.textContent || '';
  }
  if (isLoading) {
    button.disabled = true;
    button.classList.add('is-loading');
    button.textContent = loadingText;
  } else {
    button.disabled = false;
    button.classList.remove('is-loading');
    button.textContent = button.dataset.originalText || button.textContent;
  }
}

function setElementsDisabled(elements, isDisabled) {
  (elements || []).forEach((el) => {
    if (el) el.disabled = isDisabled;
  });
}

function initTabs({ buttons = [], panels = [], active }) {
  const activate = (target) => {
    buttons.forEach((button) => {
      const isActive = button.dataset.tabTarget === target;
      button.classList.toggle('is-active', isActive);
      button.setAttribute('aria-selected', String(isActive));
      button.tabIndex = isActive ? 0 : -1;
    });
    panels.forEach((panel) => {
      const isActive = panel.dataset.tabPanel === target;
      panel.classList.toggle('is-active', isActive);
      panel.hidden = !isActive;
    });
  };
  buttons.forEach((button) => {
    button.addEventListener('click', () => activate(button.dataset.tabTarget));
  });
  activate(active || buttons[0]?.dataset.tabTarget);
  return { activate };
}
