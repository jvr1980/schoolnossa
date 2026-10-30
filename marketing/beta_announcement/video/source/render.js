// Render scene.html frame by frame with the system Chrome.
// Usage: node render.js frames <duration> [fps]   -> frames/f_00000.jpg ...
//        node render.js stills <t1,t2,...>         -> stills/s_<t>.png
const path = require('path');
const fs = require('fs');
const { chromium } = require(path.join(process.env.HOME, '.npm/_npx/9833c18b2d85bc59/node_modules/playwright-core'));

(async () => {
  const [mode, arg, fpsArg] = process.argv.slice(2);
  const browser = await chromium.launch({
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    args: ['--hide-scrollbars', '--force-color-profile=srgb'],
  });
  const page = await browser.newPage({ viewport: { width: 1080, height: 1920 }, deviceScaleFactor: 1 });
  await page.goto('file://' + path.join(__dirname, 'scene.html'));
  await page.evaluate(() => window.ready);

  if (mode === 'stills') {
    fs.mkdirSync(path.join(__dirname, 'stills'), { recursive: true });
    for (const t of arg.split(',').map(Number)) {
      await page.evaluate((x) => window.render(x), t);
      await page.screenshot({ path: path.join(__dirname, 'stills', `s_${t.toFixed(1)}.png`) });
    }
  } else {
    const fps = Number(fpsArg || 30);
    const n = Math.round(Number(arg) * fps);
    const dir = path.join(__dirname, 'frames');
    fs.rmSync(dir, { recursive: true, force: true });
    fs.mkdirSync(dir, { recursive: true });
    for (let i = 0; i < n; i++) {
      await page.evaluate((x) => window.render(x), i / fps);
      await page.screenshot({ path: path.join(dir, `f_${String(i).padStart(5, '0')}.jpg`), type: 'jpeg', quality: 92 });
      if (i % 150 === 0) console.log(`frame ${i}/${n}`);
    }
  }
  await browser.close();
})();
