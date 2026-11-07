const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync('structured/worker.template.js', 'utf8');
const code = source.slice(source.indexOf('function digShouldNotify('), source.indexOf('// Keepa Tracking'));
(async () => {
  let state = {}, sentBodies = [], ok = true, items = [], keywords = [];
  const context = {Date, DIG_KW: Array.from({length:28}, (_,i)=>'kw'+i), APP_URL:'https://example.com',
    loadState: async()=>({}), mercariQ:()=>'',mercariSoldUrl:()=> 'https://example.com/sold',
    runDig:async(_e,_k,opt)=>{keywords.push(opt.kw);return {items};},
    fetch:async(_url,req)=>{sentBodies.push(JSON.parse(req.body));return {ok};}
  };
  vm.createContext(context);vm.runInContext(code,context);
  const env={STORE:{get:async()=>JSON.stringify(state),put:async(_k,v)=>{state=JSON.parse(v);}},DISCORD_WEBHOOK:'mock'};
  const day=Date.now(), item={url:'https://example.com/item',price:9000,ship:true};
  assert.equal(context.digShouldNotify({at:day-86400001,price:10000,ship:true},item,day),true);
  assert.equal(context.digShouldNotify({at:day-86400001,price:9200,ship:true},item,day),false);
  assert.equal(context.digShouldNotify({at:day-100,price:10000,ship:true},item,day),false);
  assert.equal(context.digShouldNotify(day-86400001,item,day),false);
  assert.equal(context.digShouldNotify({at:day-86400001,price:10000,ship:false},item,day),false);
  for(let i=0;i<24;i++) await context.runDigCron(env,{scheduledTime:day});
  await context.runDigCron(env,{scheduledTime:day+86400000});
  assert.equal(keywords[24],'kw24');
  items=Array.from({length:5},(_,i)=>({...item,url:item.url+i,lv:'only',brand:'test',name:'test',base:12000}));
  await context.runDigCron(env,{scheduledTime:day+86400000});
  assert.equal(Object.keys(state.sent).length,3);
  assert.match(sentBodies[0].content,/未確認/);
  assert.match(sentBodies[0].content,/\/unified>/);
  ok=false;
  await context.runDigCron(env,{scheduledTime:day+86400000});
  assert.equal(Object.keys(state.sent).length,3,'failed delivery must not mark unseen items');
  ok=true;
  await context.runDigCron(env,{scheduledTime:day+86400000});
  assert.equal(Object.keys(state.sent).length,5,'previously omitted items remain eligible');
  console.log('PASS: rotation, price-drop threshold, cooldown, legacy history, delivery failure, notification cap');
})().catch(e=>{console.error(e);process.exitCode=1;});
