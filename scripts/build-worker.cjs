const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const root = path.resolve('structured');
const baseline = JSON.parse(fs.readFileSync(path.join(root,'baseline.json'),'utf8'));
let output = fs.readFileSync(path.join(root,'worker.template.js'),'utf8');
const modules = [{name:'candidateLedger',file:'server/candidate-ledger.js'}];
for (const module of modules) {
  const content = fs.readFileSync(path.join(root,module.file),'utf8');
  const token = '__MODULE_'+module.name+'__';
  if (output.split(token).length !== 2) throw Error('Invalid module placeholder '+module.name);
  output = output.replace(token,()=>content);
}
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
