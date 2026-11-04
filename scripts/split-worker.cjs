// Mechanical, lossless extraction. Never executes the source Worker.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const input = process.argv[2];
if (!input) throw Error('Usage: node scripts/split-worker.cjs <worker.js>');
const bytes = fs.readFileSync(input);
const original = bytes.toString('utf8');
const root = path.resolve('structured');
if (fs.existsSync(root)) throw Error('structured already exists; refusing to overwrite');
let template = original;
const assets = [];
const regex = /^(const (HTML|STATUS_HTML|unifiedHTML)\s*=\s*)("(?:[^"\\]|\\.)*")(;)/gm;
template = template.replace(regex, (whole, declaration, name, literal, semicolon) => {
  const content = JSON.parse(literal);
  const file = 'ui/' + name + '.html';
  assets.push({name,file,content});
  return declaration + '__ASSET_' + name + '__' + semicolon;
});
if (assets.length !== 3) throw Error('Expected three HTML assets, got '+assets.length);
fs.mkdirSync(path.join(root,'ui'),{recursive:true});
fs.writeFileSync(path.join(root,'worker.template.js'),template);
for (const asset of assets) fs.writeFileSync(path.join(root,asset.file),asset.content);
fs.writeFileSync(path.join(root,'baseline.json'),JSON.stringify({source:path.basename(input),sha256:crypto.createHash('sha256').update(bytes).digest('hex'),assets:assets.map(({name,file,content})=>({name,file,sha256:crypto.createHash('sha256').update(content).digest('hex')}))},null,2));
console.log('Extracted three editable HTML files and Worker template');
