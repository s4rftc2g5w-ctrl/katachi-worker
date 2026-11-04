const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('structured/server/candidate-ledger.js', 'utf8') + '\nthis.api={candidateDecision,applyCandidateObservation,candidateTop};';
const context = {};
vm.createContext(context);
vm.runInContext(source, context);
const { candidateDecision, applyCandidateObservation, candidateTop } = context.api;

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
console.log('PASS: candidate history, purchase-margin gate, human confirmation, and top-three ranking');
