const fs = require('node:fs');

const vm = require('node:vm');
const assert = require('node:assert/strict');
const html = fs.readFileSync('structured/ui/unifiedHTML.html', 'utf8');
const source = html.slice(html.indexOf('  function storedApiKey(){'), html.indexOf('  function saveSyncSettings('));
for (const current of ['', 'test-current']) {
  const reads = [];
  const context = { API_KEY: 'test-current-slot', localStorage: {
    getItem(key) { reads.push(key); return key === 'test-current-slot' ? current : 'test-other-account'; },
    setItem() { throw Error('Reading credentials must not write storage'); }
  }};
  vm.createContext(context);
  assert.equal(vm.runInContext(source + '\nstoredApiKey()', context), current);
  assert.deepEqual(reads, ['test-current-slot']);
}
console.log('PASS: credential lookup does not import other credentials or rewrite storage');
