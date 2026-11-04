const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { TextEncoder } = require('node:util');
const { webcrypto } = require('node:crypto');
const source = fs.readFileSync('structured/server/candidate-ledger.js', 'utf8') + '\nthis.api={candidateDecision,applyCandidateObservation,candidateTop,readLatestCandidate,saveCandidateBatch,recordCandidateBatch};';
const context = {crypto:webcrypto,TextEncoder,console};
vm.createContext(context);
vm.runInContext(source, context);
const { candidateDecision, applyCandidateObservation, candidateTop, readLatestCandidate, saveCandidateBatch, recordCandidateBatch } = context.api;

const decision = candidateDecision({price:5000,sell:10000,cashProfit:1250,soldCount:3});
assert.equal(Math.round(decision.cashMargin),25);
assert.equal(decision.numericReady,true);
assert.equal(decision.buyReady,false);
assert.ok(decision.reasons.includes('メルカリ同一商品3件の本人確認が未完了'));
assert.equal(candidateDecision({price:5000,cashProfit:999}).numericReady,false);
assert.equal(candidateDecision({price:5000,cashProfit:1250}).reconsiderPrice,5000);

const ledger={candidates:{}};
applyCandidateObservation(ledger,'scan',{jan:'4943765053514',name:'商品A',price:6000,cashProfit:1000,soldCount:3},'2026-11-03T00:00:00Z');
applyCandidateObservation(ledger,'scan',{jan:'4943765053514',name:'商品A',price:5000,cashProfit:1500,soldCount:3},'2026-11-04T00:00:00Z');
assert.equal(ledger.candidates['jan:4943765053514'].history.length,2);
assert.equal(ledger.candidates['jan:4943765053514'].latest.priceChange,-1000);
for(let i=0;i<4;i++) applyCandidateObservation(ledger,'scan',{url:'https://example.test/'+i,name:'商品'+i,price:5000,cashProfit:1300+i*100,soldCount:3},'2026-11-04T00:00:00Z');
const top=candidateTop(ledger,3);
assert.equal(top.length,3);
assert.equal(top[0].latest.cashProfit,1600);
assert.equal(top[2].latest.cashProfit,1500);

class FakeStore {
  constructor() { this.values = new Map(); }
  async put(key, value) { this.values.set(key, value); }
  async get(key) { return this.values.has(key) ? this.values.get(key) : null; }
  async list({prefix='', limit=1000}) {
    return {keys:[...this.values.keys()].filter((key)=>key.startsWith(prefix)).sort().slice(0,limit).map((name)=>({name}))};
  }
}

(async()=>{
  const store = new FakeStore();
  const env = {STORE:store};
  await saveCandidateBatch(env,'scan',[{jan:'4943765053514',name:'商品A',price:6000,cashProfit:1000,soldCount:3}],'2026-11-03T00:00:00Z');
  await saveCandidateBatch(env,'yahoo-dig',[{jan:'4943765053514',name:'商品A',price:5000,cashProfit:1500,soldCount:3}],'2026-11-04T00:00:00Z');
  const latest = await readLatestCandidate(env,'jan:4943765053514');
  assert.equal(latest.latest.price,5000);
  assert.equal(latest.latest.previousPrice,6000);
  assert.equal([...store.values.keys()].filter((key)=>key.startsWith('candidate-journal-v1:')).length,2);
  const safeFailure = await recordCandidateBatch({STORE:{put:async()=>{throw new Error('KV停止');}}},'scan',[{jan:'4943765053514'}],'2026-11-04T00:00:00Z');
  assert.equal(safeFailure.saved,0);
  assert.match(safeFailure.error,/KV停止/);
  console.log('PASS: candidate history, purchase-margin gate, human confirmation, durable journal, safe failure, and top-three ranking');
})().catch((error)=>{ console.error(error); process.exitCode=1; });
