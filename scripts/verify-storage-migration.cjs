const fs = require('node:fs');

const html = fs.readFileSync('structured/ui/unifiedHTML.html', 'utf8');
for (const marker of [
  '"bid-note-api-key-v1", "kataochi-key"',
  'localStorage.setItem(API_KEY, oldValue)',
  '以前の保存場所から合言葉を復元しました'
]) {
  if (!html.includes(marker)) throw new Error(`Missing safe API-key migration step: ${marker}`);
}
if (/removeItem\(oldKeys\[i\]\)/.test(html)) {
  throw new Error('Legacy API-key storage must remain until recovery is verified');
}

console.log('PASS: saved API key is copied from legacy storage without deleting the source');
