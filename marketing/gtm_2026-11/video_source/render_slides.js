// Render each deck slide (project/slides/<id>.html) to a 1920x1080 PNG with the system Chrome.
// Usage: node render_slides.js <deck_root> <out_dir>
const path = require('path');
const fs = require('fs');
const { chromium } = require(process.env.PW_CORE || './pw/node_modules/playwright-core');

const [deckRoot, outDir] = process.argv.slice(2);
const deck = JSON.parse(fs.readFileSync(path.join(deckRoot, 'project/deck.json'), 'utf8'));
const fontLinks = Object.values(deck.faces).map(f => `<link rel="stylesheet" href="${f.href}">`).join('\n');
// Reset to the deck format's defaults: no margins, border-box, pinned children relative to the slide.
const css = `
*{margin:0;padding:0;box-sizing:border-box}
html,body{width:1920px;height:1080px;overflow:hidden;background:#fff}
section{position:relative;width:1920px;height:1080px;overflow:hidden}
section > *{flex-shrink:0}
h1{font-size:96px;font-weight:600;line-height:1.1} h2{font-size:64px;font-weight:600;line-height:1.15}
h3{font-size:44px;font-weight:600;line-height:1.2} p{line-height:1.4}
aside{display:none}
table{border-collapse:collapse;width:100%}
th,td{padding:0.35em 0.6em;text-align:left;vertical-align:top;border-bottom:1px solid #DCE5F0}
th{font-weight:600}
`;
fs.mkdirSync(outDir, { recursive: true });
(async () => {
  const browser = await chromium.launch({
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    args: ['--hide-scrollbars', '--force-color-profile=srgb'],
  });
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 1 });
  for (const id of deck.order) {
    const section = fs.readFileSync(path.join(deckRoot, `project/slides/${id}.html`), 'utf8');
    await page.setContent(`<!doctype html><html><head><meta charset="utf-8">${fontLinks}<style>${css}</style></head><body>${section}</body></html>`, { waitUntil: 'networkidle' });
    await page.evaluate(() => document.fonts.ready);
    // Report text that runs past the slide's bottom margin (y > 952) as a layout warning.
    const overflow = await page.evaluate(() => {
      const out = [];
      document.querySelectorAll('section *:not(aside)').forEach(el => {
        const r = el.getBoundingClientRect();
        // body content must end above the footer band (y 920); the footer row itself sits at y ~982
        if (r.height > 0 && el.children.length === 0 && ((r.bottom > 935 && r.top < 960) || r.right > 1800)) out.push(`${el.tagName} bottom=${Math.round(r.bottom)} "${el.textContent.slice(0, 40)}"`);
      });
      return out;
    });
    await page.screenshot({ path: path.join(outDir, `${id}.png`) });
    console.log(id, overflow.length ? 'OVERFLOW ' + overflow.join(' | ') : 'ok');
  }
  await browser.close();
})();
