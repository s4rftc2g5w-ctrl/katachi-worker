const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const root = path.resolve('structured');
const baseline = JSON.parse(fs.readFileSync(path.join(root,'baseline.json'),'utf8'));
let output = fs.readFileSync(path.join(root,'worker.template.js'),'utf8');
for (const a of baseline.assets) {
  const content = fs.readFileSync(path.join(root,a.file),'utf8');
  const literal = JSON.stringify(content);
  const token = '__ASSET_'+a.name+'__';
  if (output.split(token).length !== 2) throw Error('Invalid asset placeholder '+a.name);
  output = output.replace(token,()=>literal);
}
fs.mkdirSync('build',{recursive:true});
fs.writeFileSync('build/worker.js',output);
console.log('Built build/worker.js (local only)');
