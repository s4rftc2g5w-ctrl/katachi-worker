const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const root = path.resolve('structured');
const baseline = JSON.parse(fs.readFileSync(path.join(root,'baseline.json'),'utf8'));
for (const a of baseline.assets) {
  const content = fs.readFileSync(path.join(root,a.file),'utf8');
  const hash = crypto.createHash('sha256').update(content).digest('hex');
  if (hash !== a.sha256) throw Error(a.file+' differs from the fixed v1.5.80 baseline');
}
const template = fs.readFileSync(path.join(root,'worker.template.js'),'utf8');
for (const a of baseline.assets) {
  const token = '__ASSET_'+a.name+'__';
  if (template.split(token).length !== 2) throw Error('Missing or duplicate placeholder '+token);
}

const builtPath = path.resolve('build','worker.js');
if (fs.existsSync(builtPath)) {
  let rebuiltTemplate = fs.readFileSync(builtPath,'utf8');
  for (const a of baseline.assets) {
    const content = fs.readFileSync(path.join(root,a.file),'utf8');
    const literal = JSON.stringify(content);
    const token = '__ASSET_'+a.name+'__';
    if (rebuiltTemplate.split(literal).length !== 2) {
      throw Error('Built worker does not contain the expected asset '+a.name);
    }
    rebuiltTemplate = rebuiltTemplate.replace(literal,token);
  }
  if (rebuiltTemplate !== template) {
    throw Error('Built worker server code differs from worker.template.js');
  }
}

console.log('PASS: source assets and built worker preserve the fixed v1.5.80 structure');
