// Start before the verifier; the ready file releases its publication gate.
const { chromium } = require('playwright-core');
const fs = require('fs');
const path = require('path');
const directory = process.argv[2];
const ticker = process.argv[3];
if (!directory || !ticker) throw new Error('usage: capture.cjs DIRECTORY TICKER');

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/usr/bin/chromium-browser', args: ['--no-sandbox'] });
  const frames = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1800, height: 1050 }, deviceScaleFactor: 1 });
    page.on('websocket', socket => socket.on('framereceived', frame => {
      const message = JSON.parse(frame.payload.toString());
      frames.push(message);
    }));
    await page.goto(process.env.GATEWAY_URL || 'http://127.0.0.1:38080/');
    await page.locator('#status').getByText('connected', { exact: true }).waitFor();
    await page.locator('#windowSize').fill('1000');
    await page.locator('#tickers').fill(ticker);
    await page.locator('#sub').click();
    await page.waitForFunction(t => document.querySelector('#log').textContent.includes(`"ticker":"${t}"`), ticker);
    const history = frames.find(m => m.type === 'history' && m.ticker === ticker);
    if (!history || history.events.length !== 0) throw new Error('subscription did not receive empty history');
    fs.writeFileSync(path.join(directory, 'ready'), 'browser subscribed with empty history\n', { flag: 'wx' });
    await page.waitForFunction(() => {
      const rows = [...document.querySelectorAll('#signalRows tr')];
      return rows.filter(r => /BUY|SELL/.test(r.cells[1]?.textContent) && !r.cells[2]?.textContent.includes('–') && !r.cells[3]?.textContent.includes('–')).length >= 3;
    }, null, { timeout: 180000 });
    await page.screenshot({ path: path.join(directory, 'browser-live.png'), fullPage: true });
    fs.writeFileSync(path.join(directory, 'browser-frames.json'), JSON.stringify(frames, null, 2) + '\n');
    const signals = frames.filter(m => m.ticker === ticker && (m.type === 'BUY' || m.type === 'SELL'));
    if (signals.map(m => m.type).join(',') !== 'BUY,SELL,BUY') throw new Error('unexpected live signals: ' + JSON.stringify(signals));
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exit(1); });
