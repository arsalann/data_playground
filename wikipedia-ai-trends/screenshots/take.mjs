import { chromium } from 'playwright';
import fs from 'node:fs';
import path from 'node:path';

const OUT = path.dirname(new URL(import.meta.url).pathname);

const browser = await chromium.launch();
const ctx = await browser.newContext({ viewport: { width: 1600, height: 8000 }, deviceScaleFactor: 1 });
const page = await ctx.newPage();

page.on('pageerror', e => console.log('[pageerror]', e.message));
page.on('console', msg => { if (msg.type() === 'error') console.log('[console.error]', msg.text()); });

await page.goto('http://localhost:8321/', { waitUntil: 'domcontentloaded', timeout: 60_000 });
await page.waitForTimeout(3000);

const link = page.locator('a').filter({ hasText: /Wikipedia AI Trends/i }).first();
if (await link.count() > 0) {
  await link.click();
  await page.waitForLoadState('domcontentloaded', { timeout: 60_000 });
} else {
  console.log('[warn] no dashboard link found');
}

await page.waitForTimeout(8000);

await page.screenshot({ path: path.join(OUT, '00_full_page.png'), fullPage: true });
console.log('saved 00_full_page.png');

const widgetTitles = [
  'Articles tracked',
  'AI seed articles',
  'Snapshot dates',
  'Latest cross-subject AI share',
  'AI-reference prevalence by subject',
  'Top 20 sub-subjects by AI-reference share',
  'AI-reference share over time, by subject',
  'Subjects omitted from line chart',
  'Subject-level change in AI-reference share',
  'Sub-subjects with HIGHER AI presence',
  'Sub-subjects with LOWER AI presence',
  'Top 25 Vital Articles by absolute growth',
];

// Use sharp to crop the full-page PNG into slices for readable review
import sharp from 'sharp';
{
  const fullPath = path.join(OUT, '00_full_page.png');
  const meta = await sharp(fullPath).metadata();
  const sliceH = 1400 * (meta.density ? 1 : 2); // deviceScaleFactor=2
  const slices = Math.ceil(meta.height / sliceH);
  for (let i = 0; i < slices; i++) {
    const top = i * sliceH;
    const h = Math.min(sliceH, meta.height - top);
    await sharp(fullPath)
      .extract({ left: 0, top, width: meta.width, height: h })
      .toFile(path.join(OUT, `slice_${String(i).padStart(2, '0')}.png`));
    console.log(`saved slice_${i}.png (top=${top} h=${h})`);
  }
}

let idx = 1;
for (const title of widgetTitles) {
  try {
    const titleEl = page.locator(`text=${title}`).first();
    if (await titleEl.count() === 0) {
      console.log(`[miss] "${title}"`);
      idx++;
      continue;
    }
    const card = await titleEl.evaluateHandle(el => {
      let n = el;
      while (n && n.parentElement) {
        n = n.parentElement;
        const cls = (n.className && typeof n.className === 'string') ? n.className : '';
        const r = n.getBoundingClientRect();
        if (r.height > 100 && (cls.includes('rounded') || cls.includes('border') || cls.includes('card'))) return n;
      }
      return el;
    });
    const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, '_').slice(0, 60);
    const fname = `${String(idx).padStart(2, '0')}_${slug}.png`;
    await card.asElement().screenshot({ path: path.join(OUT, fname) });
    console.log(`saved ${fname}`);
  } catch (e) {
    console.log(`[err] ${title}: ${e.message}`);
  }
  idx++;
}

await browser.close();
console.log('done');
