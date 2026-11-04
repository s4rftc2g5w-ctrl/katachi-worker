const fs = require('node:fs');

const html = fs.readFileSync('structured/ui/unifiedHTML.html', 'utf8');
const requiredLinks = [
  'l-rakuten', 'l-yahoo', 'l-amazon', 'l-keepa', 'l-kakaku',
  'l-yauc', 'l-ypay', 'l-rakuma', 'l-mon', 'l-hoff', 'l-msold', 'l-yclosed'
];

for (const id of requiredLinks) {
  if (!html.includes(`id="${id}"`) || !html.includes(`setLink("${id}"`)) {
    throw new Error(`Unified search link is incomplete: ${id}`);
  }
}

for (const marker of ['id="scanJan"', 'id="scanOverlay"', 'new BarcodeDetector', 'getUserMedia', 'function validJan']) {
  if (!html.includes(marker)) throw new Error(`Unified JAN reader is incomplete: ${marker}`);
}

console.log('PASS: unified UI contains JAN reading and all search destinations');
