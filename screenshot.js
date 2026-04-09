const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 2200 } });

  async function waitForReady(label) {
    await page.waitForLoadState('networkidle');
    await page
      .waitForFunction(() => {
        const count = document.querySelector('#resultsCount');
        const status = document.querySelector('#statusBanner');
        return count && status && status.textContent.length > 0;
      }, { timeout: 30000 })
      .catch(() => {});
    await page.waitForTimeout(2500);
    console.log('ready:', label);
  }

  await page.goto('http://127.0.0.1:8765/', { waitUntil: 'domcontentloaded' });
  await waitForReady('home');
  await page.screenshot({ path: 'dashboard-home.png', fullPage: true });

  await page.selectOption('#datasetSelect', 'shein');
  await waitForReady('shein');
  await page.fill('#searchInput', 'dress');
  await page.waitForTimeout(1200);
  await page.screenshot({ path: 'dashboard-shein-search.png', fullPage: true });

  await page.fill('#searchInput', '');
  await page.check('#imagesOnlyToggle');
  await page.waitForTimeout(1200);
  await page.screenshot({ path: 'dashboard-shein-images.png', fullPage: true });

  await page.uncheck('#imagesOnlyToggle');
  await page.selectOption('#datasetSelect', 'zara');
  await waitForReady('zara');
  await page.selectOption('#sortSelect', 'price-desc');
  await page.waitForTimeout(1200);
  await page.screenshot({ path: 'dashboard-zara.png', fullPage: true });

  await browser.close();
})();
