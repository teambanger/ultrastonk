// Run against the still-running gateway after the verifier has written its comparison.
const { chromium } = require('playwright-core');
const evidence = require(process.argv[2]);

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/usr/bin/chromium-browser', args: ['--no-sandbox'] });
  try {
    const page = await browser.newPage({ viewport: { width: 1800, height: 950 }, deviceScaleFactor: 1 });
    await page.goto(process.env.GATEWAY_URL || 'http://127.0.0.1:38080/');
    await page.locator('#status').getByText('connected', { exact: true }).waitFor();
    await page.locator('#windowSize').fill(String(evidence.window_ms));
    await page.locator('#tickers').fill(evidence.crossover_ticker);
    await page.locator('#sub').click();
    await page.locator('#rows').getByText(evidence.crossover_ticker).waitFor();
    await page.locator('#rows').getByText('BUY', { exact: false }).waitFor();
    await page.screenshot({ path: process.argv[3], fullPage: true });
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exit(1); });
