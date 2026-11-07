// v1.5.80: 統合版専用manifestとiPhone起動名を追加。データ・同期・Cronは変更なし。
// v1.5.80: PC・iPhone共通の統合版 /unified を追加（HTMLを返すだけ。GET/HEADのみ）。同期は /unified-api/data（GET/PUT）と /unified-api/health（GET）を経由して ConoHa の同期サーバーへ中継する（合言葉は Bearer でそのまま渡し、判定はサーバー側。合言葉なしは401、別オリジンは403、12秒で打ち切り）。ブラウザの保存先は、8789（テスト）とそれ以外（本番）で分ける。サーバー保存分の移行・照合済み（型落ち0件／入札ノート1件）。既存の取得・判定・利益計算・Cron の処理は変更しない。
// v1.5.78: エディオン巡回は Cloudflare からも自宅PCからも 403（ボット対策）のため自動を止める。朝のまとめの点検対象から外すだけで、受け口（/api/edion-ingest）と巡回の処理は残す（Cron「0 1 * * *」は Cloudflare 側で削除）。ほかの処理は変更なし。
// v1.5.77: エディオン巡回の取得を外部に移せるように。Cloudflare から取ると 403 になるため、自宅PCやVPSでページを取り、POST /api/edion-ingest（合言葉が必要）で本文を送ると、今までと同じ読み取り・重複管理・Discord通知・実行記録を行う。巡回先は GET /api/edion-cats で取得（一か所で管理）。Cloudflare の Cron「0 1 * * *」は残っていれば従来どおり自分で取りに行く。ほかの処理は変更なし。
// v1.5.76: 朝のまとめ通知（毎朝7:30 JST・既存Cron「30 22 * * *」を利用、Cron追加不要）。自動実行の成否・ConoHa接続・入札ノート用データの鮮度・今日/明日のポイント日・大型セール・楽天アプリ期限・昨夜の候補を Discord に1通で送る（1日1回）。手動確認 /api/digest（send=1 で送信）。Yahoo!ポイントを point.lyLimitedBonusAmount から読む（point.amount は2022年から0固定）。既存の取得・判定・利益計算・Cron の処理は変更しない。
// v1.5.74: 自動スキャンは関連性を判定してから10件に絞る。保守契約とHP検索のタイヤ混入を除外。
// v1.5.74: API成功応答の共通再利用・利用枠待機・取得日時・消費記録。
// Read-only API reuse. KV shares successful responses across Worker instances.
// In-flight deduplication and pacing are per isolate, not an account-wide lock.
function createApiReuse({fetcher=fetch, now=Date.now, sleep=ms=>new Promise(r=>setTimeout(r,ms)), digest=async value=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value)))).map(x=>x.toString(16).padStart(2,'0')).join('')}={}) {
  const pending=new Map(), gates=new Map(), budgets=new Map();
  const metrics=(env,type,provider)=>{if(env.__diag){const m=env.__diag.apiReuse ||= {};const k=provider+':'+type;m[k]=(m[k]||0)+1;}};
  async function storage(env,method,...args){
    let timer;
    try {return await Promise.race([env.STORE?.[method](...args),new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('cache timeout')),2000);})]);}
    catch {metrics(env,'storageFailure','cache');return null;}finally{clearTimeout(timer);}
  }
  function policy(u,method){
    if(u.hostname==='api.keepa.com') {
      const ttl={'/product':3600,'/search':21600,'/deal':900,'/token':0}[u.pathname];
      if(ttl!==undefined && (method==='GET'||u.pathname==='/deal'&&method==='POST'))return {provider:'keepa',ttl,interval:250};
    }
    if(method==='GET'&&u.hostname==='shopping.yahooapis.jp'&&u.pathname==='/ShoppingWebService/V3/itemSearch')return {provider:'yahoo',ttl:300,interval:1100};
    if(method==='GET'&&u.hostname==='openapi.rakuten.co.jp'&&u.pathname.includes('/IchibaItem/Search/'))return {provider:'rakuten',ttl:300,interval:1100};
    return null;
  }
  function response(record,source){
    const headers=new Headers(record.headers);
    headers.set('X-Katachi-API-Source',source);headers.set('X-Katachi-API-Fetched-At',record.at);
    let body=record.body;
    // A stored token balance is not a current token balance.
    if(record.provider==='keepa'&&source!=='network'){
      try {const j=JSON.parse(body);for(const k of ['tokensLeft','refillIn','refillRate','tokensConsumed','tokenFlowReduction'])delete j[k];body=JSON.stringify(j);} catch {}
    }
    return new Response(body,{status:record.status,headers});
  }
  async function ordered(account,interval,action){
    const before=gates.get(account)||Promise.resolve();
    let release;const after=new Promise(r=>{release=r;});gates.set(account,after);
    await before;
    try{return await action();}finally{await sleep(interval);release();if(gates.get(account)===after)gates.delete(account);}
  }
  async function readNetwork(url,init){
    const controller=new AbortController();let timer;
    const external=init.signal;const abort=()=>controller.abort();
    if(external?.aborted)controller.abort();else external?.addEventListener('abort',abort,{once:true});
    try{return await Promise.race([(async()=>{const r=await fetcher(url,{...init,signal:controller.signal});const body=await r.text();return {status:r.status,headers:Object.fromEntries(r.headers),body,at:new Date(now()).toISOString()};})(),new Promise((_,reject)=>{timer=setTimeout(()=>{reject(Error('API通信が30秒で終了しませんでした'));controller.abort();},30000);})]);}
    finally{clearTimeout(timer);external?.removeEventListener('abort',abort);}
  }
  async function request(env,input,init={}){
    const u=new URL(input),method=(init.method||'GET').toUpperCase(),p=policy(u,method);
    if(!p)return fetcher(input,init);
    const credentials=p.provider==='keepa'?u.searchParams.get('key'):p.provider==='yahoo'?u.searchParams.get('appid'):[u.searchParams.get('applicationId'),u.searchParams.get('accessKey')].join(':');
    const account=p.provider+':'+await digest(credentials||'');
    // Full query/options partition cache entries; only the hash is stored as a key.
    u.searchParams.sort();
    const headers=[...new Headers(init.headers).entries()].sort();
    const key='api-reuse:v1:'+await digest(JSON.stringify([u.href,method,init.body||'',headers]));
    if(p.ttl){
      const raw=await storage(env,'get',key);let saved;
      try{saved=raw&&JSON.parse(raw);}catch{}
      if(saved&&saved.expires>now()&&saved.status===200&&typeof saved.body==='string'){
        try{const r=response(saved,'cache');metrics(env,'cacheHit',p.provider);return r;}catch{}
      }
    }
    if(pending.has(key)){const record=await pending.get(key);metrics(env,'coalesced',p.provider);return response(record,'shared');}
    const task=ordered(account,p.interval,async()=>{
      const budgetKey='api-reuse:budget:v1:'+account;
      let budget=budgets.get(account);
      if(!budget){try{budget=JSON.parse(await storage(env,'get',budgetKey)||'null');}catch{}}
      if(budget?.until>now()&&u.pathname!=='/token'){
        metrics(env,'deferred',p.provider);
        return {provider:p.provider,status:429,headers:{'content-type':'application/json','retry-after':String(Math.ceil((budget.until-now())/1000)),'x-katachi-deferred':'1'},body:JSON.stringify({error:'API利用枠の補充・待機時間を待っています'}),at:new Date(now()).toISOString()};
      }
      const record=await readNetwork(input,init);record.provider=p.provider;metrics(env,'network',p.provider);
      let data;try{data=JSON.parse(record.body);}catch{}
      let until=0;
      if(p.provider==='keepa'&&typeof data?.tokensLeft==='number'){
        if(env.__diag){const m=env.__diag.apiReuse ||= {};m['keepa:tokensConsumed']=(m['keepa:tokensConsumed']||0)+Math.max(0,Number(data.tokensConsumed)||0);}
        if(data.tokensLeft<=0)until=now()+Math.max(1000,Number(data.refillIn)||60000);
        budget={until,tokensLeft:data.tokensLeft,refillRate:data.refillRate,refillIn:data.refillIn,tokenFlowReduction:data.tokenFlowReduction,at:record.at};
      }
      if(record.status===429||record.status===403){
        const retry=record.headers['retry-after'];const seconds=Number(retry);const wait=retry?(Number.isFinite(seconds)?seconds*1000:Date.parse(retry)-now()):60000;
        until=Math.max(until,now()+Math.max(1000,Number.isFinite(wait)?wait:60000));budget={...budget,until,at:record.at,status:record.status};
      }
      if(budget){budgets.set(account,budget);await storage(env,'put',budgetKey,JSON.stringify(budget),{expirationTtl:Math.max(60,Math.ceil((budget.until-now())/1000)+60)});}
      // Never cache failed, malformed or partial-error responses.
      if(p.ttl&&record.status===200&&data&&!data.error&&!data.Error){
        let ttl=p.ttl;
        if(p.provider==='keepa'&&u.pathname==='/product'&&!data.products?.length)ttl=60;
        if(p.provider==='keepa'&&u.pathname==='/search'&&!data.asinList?.length)ttl=60;
        record.expires=now()+ttl*1000;
        if(new TextEncoder().encode(record.body).length<=512000)await storage(env,'put',key,JSON.stringify(record),{expirationTtl:ttl});
      }
      return record;
    });
    pending.set(key,task);
    try{const record=await task;return response(record,record.headers['x-katachi-deferred']?'deferred':'network');}
    finally{if(pending.get(key)===task)pending.delete(key);}
  }
  return request;
}
const reusableApiFetch=createApiReuse();

// v1.5.74: 診断の追加のみ。自動実行ごとに外向きIP（api.ipify.org、3秒で打ち切り・失敗しても処理は続行）を1回だけ記録（rec.egress）。Yahoo!の失敗応答がJSONでない場合、タグを除いた本文の先頭160文字と server / content-type を記録。Yahoo!へ送る条件・間隔・再試行、Cron、利益計算は変更しない。
// v1.5.71: v1.5.70本番を土台に、ログ確認の判定を修正。商品別エラー・Yahoo!の失敗応答を成功判定へ反映（issues）、Yahoo!深掘りは403/429後に条件を変えた再検索をしない（400のみ）、売り時ウォッチの例外を記録。状況ページ：記録なし・途中停止も要確認に数える／セール日の朝の未実行を検出／候補プール確認・エディオン巡回を表示／深掘りの「今夜分終了」の回が直前の検索結果を上書きしない。検索条件・Cron・利益計算は変更しない。
// v1.5.70: 本番v1.5.69（エディオン巡回）を土台に、Yahoo!応答診断・朝夜別実行記録・サーバー側バックアップ（読み取り専用 /api/backup、状況ページのボタン）を追加。画面の版番号を/versionと一致させた（v1.5.69は画面が1.5.68のまま）。検索条件・待機・Cronは変更しない。
// v1.5.69: エディオンアウトレット自動巡回（毎日10:00 JST）。理美容・健康・オーディオ・PC周辺機器・おもちゃの4カテゴリをチェックし、¥30,000以下の新着をDiscordへ通知（KV「edion-outlet-seen」で重複管理・30日で再通知）。
// v1.5.68: メルカリ貼り付け取り込みを「確認してから反映」に。件数・価格幅を先に表示（6件以上は先頭5件を使用と明記）、他と大きく離れた価格に「要確認」印（自動除外しない）。取り込めない時は案内を出し、入っている価格は変えない。商品切り替え・新規入力・状態変更で取り込み待ちを破棄。
// v1.5.67: 判定タブの送料に120/140/160サイズ（1,200/1,450/1,700円）を追加。状態に「展示品」を追加し、箱傷み・展示品は「新品相場から15%引く」ボタンで販売予定価格を下げられる。
// v1.5.66: 店指定の在庫処分探しを標準の並び順・既定1〜6万円に（安い小物ばかり拾う問題の修正）。
// v1.5.65: 楽天の在庫処分探しで店（shopCode）を指定できるように（家電量販店の公式店に絞る・試験）。/api/dead?find= で店コードを調べる。
// v1.5.64: 値上がり探索 /api/rise（手動・Keepa Deals isRisers＋Amazon本体なし、KV「keepa-rise」）。試験運用。
// v1.5.63: 動作状況ページ /status（iPhoneでも見られる。自動実行ごとの最終実行・最終成功・エラー、楽天の在庫処分、売り時ウォッチ）と /api/status。KV「cron-last-by-kind」に種類別の最終実行を保存。
// v1.5.62: GPT調査済みキャンペーン条件の登録・商品ごとの比較判定。v1.5.61を土台に既存処理を保持。
// 型落ち仕入れノート：アプリ本体 + 楽天/Yahoo!の中継 + データ同期 + 監視チェック（Cloudflare Workers用）
// v1.5.61: 楽天デッドストックで型番がない商品はJAN（商品名・URL）で比較。楽天候補はブランド不明でも型番/JANがあれば今日の候補に出す。
// v1.5.60: 楽天デッドストック探索（在庫処分・生産終了・廃番・旧モデル・最終在庫・型落ち×カテゴリ、手動実行 /api/dead、記録 KV「dead-log」）。今日の候補に楽天を統合し、ポイントは現金利益と分けて参考表示。
// v1.5.59: 売り時ウォッチ（/api/sell・KV「sell-watch」、毎晩20:45にKeepaでAmazon最安→メルカリ出品目安をDiscordへ）。
// v1.5.58: セール品は売値をAmazon通常価格（Keepa 90日平均）までで計算。
// v1.5.57: メルカリ売り切れリンクに仕入値（kc）を付与。拡張「相場チェック」0.6.0がメルカリの検索ページで売り切れ・販売中の価格と利益判定を表示する。
// v1.5.56: Discord通知（Yahoo!深掘りの🔥・価格監視の仕入上限到達）に「メルカリ売り切れ」を開くリンク（新品・新しい順）と、アプリを開くリンクを追加。通知から1タップで相場を確認できる。
// v1.5.55: 今日の候補の調整（機能追加なし）。並び順 ⭐→🟢→⏳再確認→🔥確認待ち、🟡はたたむ。件数の内訳（確認・価格差不足・🔴・重複・購入済み・外した）を表示。購入数の目安（相場信頼度 低＝テスト1個／中＝最大2個／高＝最大3個）。「仕入れを記録」→保存すると状態「仕入済」になり、今日の候補から外れる。
// v1.5.54: 「今日の候補」カード。Yahoo!深掘りとKeepa候補（楽天・Yahoo!比較で🔥/🟡）を1枚にまとめ、同じ商品は1件。表面は 仕入→想定売価→安全売価での利益・判断と理由1行・ボタン1つ（相場を確認→⭐/🟢なら仕入れを記録）。🔥＝安い商品を発見、🟢＝利益と相場を確認済み。最終チェック時刻と「該当なし」を表示。簡単版は 今日の候補＋値下がり待ち の2枚。
// v1.5.53: 簡単版（初期表示）。「探す」は Yahoo!深掘り・Keepa値下がり候補・Keepa価格監視 だけ、店頭タブも隠す。上の「全部表示」で元に戻せる（端末ごとに記憶）。
// v1.5.52: 夜の自動深掘り。Cron「*/10 14-17 * * *」（23:00〜02:50）で10分ごとに1キーワードずつYahoo!深掘りを回し、ブランド品の「🔥 この店だけ安い」をDiscordへ通知（同じ商品は7日に1回）。状態は /api/dig?cron=1。
// v1.5.51: Yahoo!深掘り。Yahoo!ショッピングAPIでセール中の新品をキーワード別に集め、通常価格より35%以上安く、他のYahoo!出品者・楽天の相場より20%以上安い「この店だけ安い」候補を残す（/api/dig）。メルカリ相場ボタンで最終確認。判定に⭐ホームラン（安全利益3,000円以上）。
// v1.5.50: 判定を「利益率25%」から「安全利益1,000円」中心へ。🟢安全仕入れ＝通常利益1,500円以上・安全売価で1,000円以上・売価10%下落でも黒字・利益率15%以上。仕入上限もこの4条件から計算。トップに今月の確定利益・目標・あと・在庫見込利益。
// v1.5.49: 市場価格比較。Keepa候補・価格監視のカードに楽天・Yahoo!の新品最安（送料込/別・ポイント）、通常EC相場、Amazon割安度、「Amazonだけ安い🔥／市場全体が安い🔴／相場回復🟢」を表示（/api/market、12時間キャッシュ）。メルカリと合わせた最終判定（🟢A−＝2個までテスト可 など）。
// v1.5.48: 安全売価（5件以上は中央値と下位25%の低い方、4件以下は最安値）と🟢A（安全売価でも利益＋信頼度中以上）／🟢B（中央値なら利益だが要確認）。仕入上限は安全売価から計算（拡張0.5.3）。
// v1.5.47: 相場信頼度（拡張0.5.2）。利益条件と相場信頼度（個人成約・Shops・販売者の目安）の2軸で表示し、信頼度が低いと🟢にせず🟡「利益あり・相場要確認」。
// v1.5.46: 監視の修正のみ。tokenFlowReductionを記録し「Keepaの状態を確認」で実測表示、表記を「約6時間間隔」に、監視中の商品の仕入上限を更新できるように、状態パネル（Webhook・間隔・期限）。
// v1.5.45: Keepa Tracking。メルカリ相場で今は高い商品を「価格監視に追加」→Keepaが6時間ごとに監視（最大50件・45日）→Amazonが仕入上限以下でWebhook（/hook/keepa、合言葉由来のトークンで確認）→Discord通知。「Keepa価格監視」カード。
// v1.5.44: PCの拡張「相場チェック」0.5.0と連動。Keepa候補・未開封の各商品に「メルカリ相場」ボタン（押した商品だけ確認）、結果を /api/mcache に3日保存して表示。自動巡回は廃止。
// v1.5.43: Keepa Dealsを家電・PC周辺・おもちゃ・ホビー・ゲームに限定、見せかけの値下がり（70%超・平均が現価格の3倍超）、ノーブランド表記、Amazon限定、英字のみタイトルを除外。
// v1.5.42: Keepa Deals（Amazonの値下がり新品、1ページ5トークン×2）を毎晩20:30に取得・足切りし、/api/deals と「Keepa値下がり候補」カードで表示。
// v1.5.41: KeepaでJANが見つからない／JANがない商品は、型番でKeepa検索し1件にしぼれたときだけ採用（1回最大3件）。
// v1.5.40: 未開封カードの検索結果に削除（1件ずつ・選んで・全部選ぶ・外した商品まとめて）を追加。削除した商品は次の検索でも出さない。
// v1.5.39: 相場チェックの自動実行（拡張0.3.0）で🟢があればDiscordに通知。
// v1.5.38: 相場チェック（Chrome拡張）の結果を受け取る /api/souba と、表示カード「相場チェック」を追加。
// v1.5.37: スキャンで検索語のブランド名を含まない商品を除外、同名重複を除去、KeepaはJANありを優先。
// v1.5.36: 未開封候補に価格.com比較リンクを追加。
// v1.5.35: 未開封候補に厳格ゲート（売り切れ実績=価格+成約日+URL・直近30日・重複なし3件以上、型番/色/個数/メーカー型落ち/仕入先未開封の確認、監視移動の無効化）。
// v1.5.34: Keepa自動選定の利益を現金ベースに統一、コツ文言修正。
// v1.5.10: 画面と /version の版番号を一致させ、誤った自動更新ループを修正。
//
// 設定（Settings > Variables and Secrets）
//   ACCESS_KEY         : 好きな合言葉（Secret・必須）。アプリの「合言葉」欄に同じものを入れる
//   YAHOO_APP_ID       : Yahoo!デベロッパーネットワークのClient ID（Secret）
//   RAKUTEN_APP_ID     : 楽天のアプリID（Secret）
//   RAKUTEN_ACCESS_KEY : 楽天のアクセスキー（Secret）
//   RAKUTEN_REFERER    : 楽天に登録した許可サイトのURL（例: https://xxx.workers.dev）
//   DISCORD_WEBHOOK    : 通知を受けたい場合のみ（Discordの Webhook URL）（Secret）
//   （任意）毎朝のセール検索：Cron Triggers に `30 22 * * *` を追加（日本時間 朝7:30）
//   KEEPA_KEY          : KeepaのAPIキー（Secret・任意。Amazonの価格・販売数の確認用）
// 設定（Settings > Bindings）
//   KV namespace       : 変数名は STORE
// 毎朝の自動チェックを使う場合（Settings > Triggers > Cron Triggers）
//   30 11 * * *（20:30 スキャン＋プール） / 45 11 * * *（20:45 監視チェック） / 30 22 * * *（7:30 セール日のみスキャン）
const HTML = __ASSET_HTML__;
const APP_VERSION = '1.5.80'; // v1.5.78: エディオン巡回の自動を止めたため、朝のまとめの点検対象から外す。
const STATUS_HTML = __ASSET_STATUS_HTML__;
const MANIFEST = {
  name: '型落ち仕入れノート',
  short_name: '型落ち',
  start_url: '/',
  scope: '/',
  display: 'standalone',
  background_color: '#EDF0F2',
  theme_color: '#1F4E6B',
};

const WATCH_MAX = 15;
const json = (o, s = 200) =>
  new Response(JSON.stringify(o), {
    status: s,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
  });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
__MODULE_candidateLedger__

const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9\u3040-\u30ff\u4e00-\u9fff]/g, '');

const ACC_WORDS = ['ケース', 'カバー', 'イヤーチップ', 'イヤーピース', 'イヤーパッド', 'フィルム', '保護', 'クリーナー', 'ケーブル', '充電器', 'スタンド', 'ホルダー', 'ストラップ', '交換用', '互換', 'ステッカー', 'スキン', 'ポーチ', 'ガラス', '部品', '部品コード', 'パーツ', 'フィルター', 'フタ', 'ふた', '蓋', 'パッキン', '替刃', '替え刃', 'ノズル', 'アタッチメント', '説明書', 'マニュアル', '専用袋', '巾着'];
const USED_RE = /中古|動作確認|ジャンク|リユース|リファービッシュ|再生品|整備済|美品|難あり|傷あり|[A-DＡ-Ｄ]ランク|ランク[A-DＡ-Ｄ]/;
function isUsedItem(it, q) {
  return it.condition === 'used' || USED_RE.test(String(it.name || ''));
}
function validJan(value) {
  const c = String(value || '');
  if (!/^(\d{8}|\d{13})$/.test(c) || /^0+$/.test(c)) return false;
  let sum = 0, weight = 3;
  for (let i = c.length - 2; i >= 0; i--, weight = 4 - weight) sum += Number(c[i]) * weight;
  return (10 - sum % 10) % 10 === Number(c[c.length - 1]);
}
function extractJan(title) {
  const text = String(title || '').normalize('NFKC');
  // A numeric product ID is not evidence of a barcode, even if its checksum passes.
  const found = [...text.matchAll(/(?:JAN|EAN|GTIN|バーコード)\s*(?:コード)?\s*[:：]?\s*(\d{13}|\d{8})(?!\d)/gi)]
    .map(m => m[1]).filter(validJan);
  const unique = [...new Set(found)];
  return unique.length === 1 ? unique[0] : '';
}
function matchesProduct(it, q) {
  const name = String(it.name || '').normalize('NFKC');
  const query = String(q || '').normalize('NFKC').trim();
  if (/^(\d{8}|\d{13})$/.test(query)) return String(it.jan || '') === query || name.includes(query);
  const tokens = query.split(/[\s　]+/).map(norm).filter(Boolean);
  const n = norm(name);
  if (!tokens.every(t => n.includes(t))) return false;
  // A model must not become a longer numeric model (X5007 != X50070).
  if (tokens.some(t => /[a-z]/i.test(t) && /\d$/.test(t) && new RegExp(t + '[0-9]').test(name.toLowerCase().replace(/[-_\s]/g, '')))) return false;
  if (/viper\s*v3\s*pro/i.test(query) && !/\bSE\b/i.test(query) && /viper\s*v3\s*pro\s*SE\b/i.test(name)) return false;
  return true;
}
function cleanItems(items, q, opts = {}) {
  const used = opts.condition === 'used';
  let out = items.filter(i => Number(i.price) > 0);
  out = out.filter(i => !/ジャンク|動作未確認|動作不良|故障|部品取り|箱のみ|空箱|説明書のみ|セット売り|まとめ売り/i.test(i.name));
  if (used) out = out.filter(i => isUsedItem(i, q));
  else if (opts.keepUsed !== true) out = out.filter(i => !isUsedItem(i, q));
  if (opts.min) out = out.filter(i => i.price >= opts.min);
  if (opts.scan) out = out.filter(i => scanRelevant(i, q));
  if (opts.acc) out = out.filter(i => !ACC_WORDS.some(w => String(i.name).includes(w) && !q.includes(w)));
  const bad = /電池|バッテリー|交換用|替え|詰替|詰め替え|工具|ドライバー|ビット|部品|パーツ|ケース|カバー|フィルム|ドングル|レシーバー|互換|イヤーパッド|イヤーピース|イヤーチップ|充電器|ステッカー|スキン|ポーチ|替刃|替え刃/i;
  out = out.filter(i => { const name = String(i.name); const m = name.match(bad); if (!m || String(q).toLowerCase().includes(m[0].toLowerCase())) return true; const k = name.indexOf(m[0]); return /付|内蔵|込|同梱/.test(name.slice(Math.max(0, k - 3), k + m[0].length + 3)); });
  // General sale keywords stay broad; model/JAN lookups require identifier matches.
  if (opts.model || (opts.acc && /\d/.test(q))) out = out.filter(i => matchesProduct(i, q));
  return out.sort((a,b) => (a.price - (a.pointAmount || 0)) - (b.price - (b.pointAmount || 0))).slice(0, 10);
}

async function searchRakuten(env, q, origin, opts = {}) {
  if (!env.RAKUTEN_APP_ID || !env.RAKUTEN_ACCESS_KEY) throw new Error('楽天のIDが未設定です');
  const p = new URLSearchParams({
    applicationId: env.RAKUTEN_APP_ID,
    accessKey: env.RAKUTEN_ACCESS_KEY,
    keyword: opts.condition === 'used' ? q + ' 中古' : q,
    hits: '30',
    sort: 'standard',
    availability: '1',
    format: 'json',
    formatVersion: '2',
  });
  if (opts.min) p.set('minPrice', String(Math.round(opts.min)));
  const ref = env.RAKUTEN_REFERER || origin || '';
  const headers = new Headers();
  if (ref) {
    headers.set('Referer', ref);
    try { headers.set('Origin', new URL(ref).origin); } catch (e) {}
  }
  const res = await reusableApiFetch(env,
    'https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20260701?' + p.toString(),
    { headers }
  );
  if (!res.ok) {
    let d = '';
    try { d = (await res.text()).slice(0, 120); } catch (e) {}
    throw new Error('楽天側のエラー（' + res.status + '）' + d);
  }
  const data = await res.json();
  return (data.items || data.Items || []).map((x) => {
    const h = x.Item || x;
    const rate = Number(h.pointRate) || 1;
    return {
      name: h.itemName,
      price: h.itemPrice,
      url: h.itemUrl,
      seller: h.shopName,
      jan: extractJan(h.itemName),
      condition: USED_RE.test(String(h.itemName || '')) ? 'used' : 'unknown',
      pointAmount: Math.floor((h.itemPrice * rate) / 100),
      shipping: h.postageFlag === 0 ? '送料込み' : '送料別', shippingIncluded: h.postageFlag === 0,
      reviewCount: h.reviewCount || 0,
    };
  });
}


// Diagnostic data never changes request parameters, timing, or retry policy.
function safeYahooMessage(text, env) {
  let value = String(text || '');
  for (const key of ['YAHOO_APP_ID','ACCESS_KEY','CONOHA_API_KEY','KEEPA_KEY','DISCORD_WEBHOOK']) {
    if (env[key]) for (const secret of [String(env[key]), encodeURIComponent(String(env[key]))]) value = value.split(secret).join('[redacted]');
  }
  return value.replace(/https?:\/\/[^\s<>"']+/g, '[url]').replace(/(appid|access_token|authorization|api_key|token|k)\s*[=:]\s*[^\s&,<>"']+/gi, '$1=[redacted]').slice(0, 300);
}
// v1.5.74: 外向きIPを自動実行ごとに1回だけ記録（診断専用。失敗しても処理は止めない）
async function recordEgress(env) {
  const d = env.__diag;
  if (!d || d.egress !== undefined) return;
  d.egress = null;
  const c = new AbortController();
  let timer;
  try {
    // Include response-body reading in the deadline, even if abort is ignored.
    const deadline = new Promise((_, reject) => {
      timer = setTimeout(() => {
        reject(new Error('診断通信の時間切れ'));
        c.abort();
      }, 3000);
    });
    const result = await Promise.race([
      (async () => {
        const r = await fetch('https://api.ipify.org?format=json', { signal: c.signal });
        const j = r.ok ? await r.json() : null;
        return j && j.ip ? { ip: String(j.ip).slice(0, 64), at: new Date().toISOString() } : { error: 'HTTP ' + r.status };
      })(),
      deadline
    ]);
    d.egress = result;
  } catch (_) {
    d.egress = { error: '取得できず' };
  } finally {
    clearTimeout(timer);
  }
}
async function yahooFetch(env, params, route) {
  const started = Date.now();
  const entry = { at: new Date(started).toISOString(), route, params: Object.fromEntries([...params].filter(([k]) => k !== 'appid')), runId: env.__diag && env.__diag.id || null };
  let response;
  try {
    response = await reusableApiFetch(env,'https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch?' + params.toString());
    entry.status = response.status;
    entry.source = response.headers.get("X-Katachi-API-Source") || "network";
    entry.fetchedAt = response.headers.get("X-Katachi-API-Fetched-At");
    if (entry.source === "network") await recordEgress(env);
    if (!response.ok) {
      // Only a short API message is retained; HTML responses are not copied.
      const body = await response.clone().text().catch(() => '');
      let message = '応答本文はJSON形式ではありません';
      try { const j = JSON.parse(body); message = j.Error && j.Error.Message || j.error && j.error.message || j.message || 'エラーメッセージなし'; } catch (_) {
        const text = body.replace(/<(script|style)[\s\S]*?<\/\1>/gi, ' ').replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 160);
        if (text) message += '：' + text;
      }
      entry.server = String(response.headers.get('server') || '').slice(0, 60);
      entry.ctype = String(response.headers.get('content-type') || '').slice(0, 60);
      entry.message = safeYahooMessage(message, env);
      entry.retryAfter = response.headers.get('retry-after');
    }
    return response;
  } catch (e) { entry.status = 0; entry.message = '通信に失敗しました'; throw e; }
  finally {
    entry.ms = Date.now() - started;
    if (env.__diag && env.__diag.requests.length < 100) env.__diag.requests.push(entry);
    console.log(JSON.stringify({ event: 'yahoo-request', ...entry }));
  }
}

async function searchYahoo(env, q, opts = {}) {
  if (!env.YAHOO_APP_ID) throw new Error('Yahoo!のIDが未設定です');
  const p = new URLSearchParams({
    appid: env.YAHOO_APP_ID,
    results: '50',
    sort: '-score',
    condition: opts.condition === 'used' ? 'used' : 'new',
    in_stock: 'true',
  });
  if (opts.min) p.set('price_from', String(Math.round(opts.min)));
  if (/^\d{13}$/.test(q)) p.set('jan_code', q);
  else p.set('query', q);
  const res = await yahooFetch(env, p, 'search');
  if (!res.ok) { const error = new Error('Yahoo!側のエラー（' + res.status + '）'); error.status = res.status; throw error; }
  const data = await res.json();
  return (data.hits || []).map((h) => ({
    name: h.name,
    price: h.price,
    url: h.url,
    seller: h.seller && h.seller.name,
    jan: h.janCode || '',
    condition: h.condition || (opts.condition === 'used' ? 'used' : 'new'),
    pointAmount: (h.point && (Number(h.point.lyLimitedBonusAmount) || Number(h.point.amount))) || 0,
    shipping: h.shipping && h.shipping.name, shippingIncluded: !!(h.shipping && h.shipping.name === '送料無料'),
    defaultPrice: (h.priceLabel && h.priceLabel.defaultPrice) || 0,
    reviewCount: (h.review && h.review.count) || 0,
  }));
}

// 型落ち・未開封を探す：楽天・Yahoo!の新品を集め、開封・箱なし・当たり単品などを外して、未開封の確からしさで分類
const UNO_NG = [
  [/中古|ジャンク|動作確認|リユース|美品|USED|ユーズド|[A-DＡ-Ｄ]ランク|ランク[A-DＡ-Ｄ]|難あり|傷あり/i, '中古・状態あり'],
  [/開封済|開封品|開封のみ|開封しました|中身確認|内袋未開封|内袋のみ未開封|シュリンク(なし|無し|破れ)/, '開封あり'],
  [/箱なし|箱無し|外箱なし|外箱無し|箱のみ|空箱|本体のみ|ベイのみ|単品のみ|パーツのみ|ランチャーなし|欠品|説明書なし/, '箱・付属品なし'],
  [/当たり|アタリ|確定|シークレット|レア(?!度)|開封済み/, '当たり単品・確定品'],
  [/まとめ売り|セット売り|大量|バラ売り/, 'まとめ売り・バラ売り'],
];
const UNO_WARN = [
  [/ランダム|ブースター|BOX|ボックス|パック/i, 'ランダム商品：未開封の箱同士で比べる'],
  [/箱潰れ|箱つぶれ|箱傷|箱ダメージ|外箱傷|パッケージ傷|訳あり|わけあり/, '箱にダメージ・訳あり'],
  [/予約|発売予定|入荷予定|取り寄せ|お取り寄せ/, '予約・取り寄せ'],
  [/海外版|並行輸入|輸入品/, '海外版・並行輸入'],
  [/\d+\s*(個|点|箱)セット|\d+\s*個入|[×x]\s*\d+/i, '個数を確認（メルカリ側と揃える）'],
];
const UNO_PARTS = /交換用|替え|替刃|部品|パーツ|ケース|カバー|フィルム|ケーブル|充電器|電池|バッテリー|ステッカー|シール|スタンド/;
function unoCode(name) {
  const t = String(name || '').normalize('NFKC');
  const bey = t.match(/\b([BUC]X)\s*-?\s*(\d{2,3})\b/i);
  if (bey) return (bey[1] + '-' + bey[2]).toUpperCase();
  const m = t.match(/[A-Za-z]{1,6}[-]?[A-Za-z]*\d{2,}[A-Za-z0-9-]*(?:\/\d+)?/);
  return m ? m[0] : '';
}
function unoClassify(it, q) {
  const name = String(it.name || '').normalize('NFKC');
  const query = String(q || '').normalize('NFKC');
  const ng = [], warn = [];
  for (const [re, label] of UNO_NG) { const m = name.match(re); if (m && !query.includes(m[0])) ng.push(label); }
  const pm = name.match(UNO_PARTS);
  if (pm && !query.includes(pm[0]) && !/付|内蔵|込|同梱/.test(name.slice(Math.max(0, name.indexOf(pm[0]) - 3), name.indexOf(pm[0]) + pm[0].length + 3))) ng.push('部品・付属品');
  for (const [re, label] of UNO_WARN) { const m = name.match(re); if (m && !query.includes(m[0])) warn.push(label); }
  const sealed = /未開封|シュリンク付|シュリンク未開封/.test(name);
  if (!sealed) warn.push(/新品/.test(name) ? '「新品」表記のみ：未開封か商品ページで確認' : '未開封の表記なし：商品ページで確認');
  if (!it.shippingIncluded) warn.push('送料別：送料を足して比べる');
  return { level: ng.length ? 'ng' : (sealed && !warn.length) ? 'ok' : 'warn', ng, warn: [...new Set(warn)], sealed };
}
async function searchUnopened(env, q, origin) {
  const out = [], errors = [];
  for (const shop of ['rakuten', 'yahoo']) {
    try {
      const raw = shop === 'rakuten' ? await searchRakuten(env, q, origin, {}) : await searchYahoo(env, q, {});
      for (const it of raw) {
        if (!(Number(it.price) > 0)) continue;
        const c = unoClassify(it, q);
        out.push(Object.assign({}, it, { shop, code: unoCode(it.name), eff: it.price - (it.pointAmount || 0) }, c));
      }
    } catch (e) {
      errors.push((shop === 'rakuten' ? '楽天' : 'Yahoo!') + '：' + e.message);
    }
  }
  const seen = new Set();
  const rank = { ok: 0, warn: 1, ng: 2 };
  const items = out.filter(x => x.url && !seen.has(x.url) && seen.add(x.url))
    .sort((a, b) => rank[a.level] - rank[b.level] || a.price - b.price).slice(0, 60);
  return { q, items, errors, checkedAt: Date.now() };
}

async function fetchKeepa(env, code) {
  const isAsin = /^[A-Z0-9]{10}$/i.test(code) && !/^\d{10}$/.test(code);
  const p = new URLSearchParams({ key: env.KEEPA_KEY, domain: '5', stats: '90', history: '0' });
  p.set(isAsin ? 'asin' : 'code', code);
  const res = await reusableApiFetch(env,'https://api.keepa.com/product?' + p.toString());
  if (!res.ok) throw new Error('Keepa側のエラー（' + res.status + '）');
  const data = await res.json();
  const pr = data.products && data.products[0];
  if (!pr) throw new Error('Keepaで商品が見つかりませんでした');
  const st = pr.stats || {};
  const pick = (arr, i) => (arr && arr[i] > 0 ? arr[i] : null);
  return {
    title: pr.title,
    asin: pr.asin,
    amazonNow: pick(st.current, 0),
    amazonAvg90: pick(st.avg90, 0),
    newNow: pick(st.current, 1),
    newAvg90: pick(st.avg90, 1),
    buyBoxNow: pick(st.current, 18),
    buyBoxAvg90: pick(st.avg90, 18),
    salesRankNow: pick(st.current, 3),
    salesRankAvg90: pick(st.avg90, 3),
    salesRankDrops30: st.salesRankDrops30,
    monthlySold: pr.monthlySold,
    sellersNow: pick(st.current, 11),
    tokensLeft: data.tokensLeft,
    fetchedAt: res.headers.get("X-Katachi-API-Fetched-At"),
    dataSource: res.headers.get("X-Katachi-API-Source") || "network",
  };
}

function keepaJudge(k, market) {
  const risks = [];
  const reasons = [];
  const cur = k.buyBoxNow || k.newNow || k.amazonNow;
  const avg = k.buyBoxAvg90 || k.newAvg90 || k.amazonAvg90;
  if (k.amazonNow) risks.push('Amazon本体が出品中です');
  if (cur && avg) {
    const drop = ((avg - cur) / avg) * 100;
    if (drop >= 10) risks.push('Amazonの価格が90日平均より' + Math.round(drop) + '%安く、値下がり傾向です');
    else if (drop < 3) reasons.push('Amazonの価格は90日平均と同程度です');
  }
  if (cur && market > cur) risks.push('メルカリの売り切れ価格がAmazonの価格を上回っています。割高で売れにくい可能性があります');
  if (typeof k.monthlySold === 'number') {
    if (k.monthlySold >= 10) reasons.push('Amazonでの月間販売数の目安は' + k.monthlySold + '件です');
    else if (k.monthlySold < 3) risks.push('Amazonでの月間販売数が少ないです（' + k.monthlySold + '件）');
  }
  const verdict = risks.length >= 2 ? '注意' : risks.length === 1 ? '要確認' : '問題なし';
  const yen = (n) => (n ? '¥' + Math.round(n).toLocaleString('ja-JP') : '不明');
  return {
    verdict,
    summary: 'Amazon確認価格 ' + yen(cur) + '（90日平均 ' + yen(avg) + '）' + (k.fetchedAt ? ' ／ 取得 ' + new Date(k.fetchedAt).toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' }) : '') + (k.dataSource === 'cache' || k.dataSource === 'shared' ? '・保存情報を再利用' : ''),
    fetchedAt: k.fetchedAt || null,
    reasons,
    risks,
    next_check: 'Keepaの画面で、価格推移のグラフを目で確認する',
  };
}

function validState(j) { return j && typeof j === 'object' && !Array.isArray(j) && !j.error && (Array.isArray(j.watch) || Array.isArray(j.scanKeywords) || Array.isArray(j.pool) || (j.settings && typeof j.settings === 'object')); }
async function cacheState(env, state) {
  if (!env.STORE || !validState(state)) return false;
  await env.STORE.put('state-conoha-backup', JSON.stringify({ savedAt: new Date().toISOString(), state }));
  return true;
}
async function loadState(env, info = {}) {
  if (env.CONOHA_API_URL && env.CONOHA_API_KEY) {
    try {
      const r = await fetch(env.CONOHA_API_URL.replace(/\/$/, '') + '/api/data', { headers: { authorization: 'Bearer ' + env.CONOHA_API_KEY } });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const j = await r.json();
      if (!validState(j)) throw new Error('invalid state');
      info.source = 'ConoHa'; info.stateUpdatedAt = j.updatedAt || null;
      try { info.backupSaved = await cacheState(env, j); } catch (e) { info.warning = 'KVへの予備保存に失敗しました'; }
      return j;
    } catch (e) { info.warning = 'ConoHaを取得できないためKVの予備データを使用'; }
  }
  if (!env.STORE) throw new Error('設定を読み込めません（ConoHa・KV）');
  const backup = JSON.parse((await env.STORE.get('state-conoha-backup')) || 'null');
  if (backup && validState(backup.state)) {
    info.source = 'KV'; info.backupAt = backup.savedAt; info.stateUpdatedAt = backup.state.updatedAt || null;
    return backup.state;
  }
  const legacy = JSON.parse((await env.STORE.get('state')) || '{}');
  if (!validState(legacy)) throw new Error('スキャン設定の保存データがありません');
  info.source = 'KV（旧データ）'; info.warning = '古い予備データを使用しています。キーワードを確認してください';
  return legacy;
}
async function runPoolCheck(env, origin) {
  const st = (await loadState(env));
  let previous = {};
  try { previous = JSON.parse((await env.STORE.get('pool-results')) || '{}').results || {}; } catch (e) {}
  const allPool = st.pool || [];
  function needsConditionRefresh(c) {
    const r = previous[c.id];
    return !r || r.condition !== (c.condition || 'new');
  }
  const pool = allPool.slice().sort((a, b) =>
    Number(needsConditionRefresh(b)) - Number(needsConditionRefresh(a)) ||
    (Date.parse((previous[a.id] || {}).checkedAt) || 0) - (Date.parse((previous[b.id] || {}).checkedAt) || 0)
  ).slice(0, 10);
  const results = Object.fromEntries(allPool.filter(c => previous[c.id]).map(c => [c.id, previous[c.id]]));
  const checkedAt = new Date().toISOString();
  for (const c of pool) {
    const q = String(c.model || c.name || '').trim().slice(0, 60);
    if (!q) { results[c.id] = { checkedAt, condition: c.condition || 'new', error: '検索語がありません' }; continue; }
    const market = Number(c.market) || 0;
    const so = { min: market ? Math.round(market * 0.4) : 1500, condition: c.condition === 'used' ? 'used' : 'new' };
    const cands = [];
    const errs = [];
    for (const shop of ['rakuten', 'yahoo']) {
      try {
        const items = shop === 'rakuten' ? await searchRakuten(env, q, origin, so) : await searchYahoo(env, q, so);
        const cleaned = cleanItems(items, q, { min: so.min, acc: true, model: !!c.model, condition: so.condition });
        for (const it of cleaned) cands.push(Object.assign({ shop, eff: it.price - (it.pointAmount || 0) }, it));
      } catch (e) {
        errs.push((shop === 'rakuten' ? '楽天' : 'Yahoo!') + '：' + e.message);
      }
      await sleep(1100);
    }
    if (!cands.length) {
      results[c.id] = { checkedAt, condition: c.condition || 'new', error: errs.length ? errs.join(' / ') : '該当する商品が見つかりませんでした' };
      continue;
    }
    const best = cands.reduce((a, b) => (Number(b.price) < Number(a.price) ? b : a));
    results[c.id] = {
      checkedAt, shop: best.shop, price: best.price, pointAmount: best.pointAmount || 0, eff: best.eff,
      name: best.name, url: best.url, shipping: best.shipping, shippingIncluded: best.shippingIncluded === true, condition: so.condition, previousPrice: (previous[c.id] || {}).price || null, firstSeenAt: (previous[c.id] || {}).firstSeenAt || checkedAt, warnings: errs,
    };
  }
  const out = { checkedAt, results, checkedCount: pool.length, totalCount: allPool.length };
  await env.STORE.put('pool-results', JSON.stringify(out));
  return out;
}

async function runCheck(env, origin) {
  const st = (await loadState(env));
  const set = Object.assign({ minProfit: 1500, minMargin: 25, feeMercari: 10, feeYflea: 5 }, st.settings);
  if (set.marginBase !== 'sell' && Number(set.minMargin) <= 10) set.minMargin = 25;
  const watch = (st.watch || []).slice(0, WATCH_MAX);
  const results = {};
  const hist = JSON.parse((await env.STORE.get('price-history')) || '{}');
  const today = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
  const notes = [];
  for (const w of watch) {
    const q = String(w.model || w.name || '').trim();
    const market = Number(w.market) || 0;
    if (!q || !market) { results[w.id] = { error: '型番と売り切れ価格を入れてください' }; continue; }
    const cands = [];
    const errs = [];
    let usedExcluded = 0;
    for (const shop of ['rakuten', 'yahoo']) {
      try {
        const so = { min: Math.round(market * 0.4) };
        const items = shop === 'rakuten' ? await searchRakuten(env, q, origin, so) : await searchYahoo(env, q, so);
        usedExcluded += items.filter((it) => isUsedItem(it, q)).length;
        const cleaned = cleanItems(items, q, { min: Math.round(market * 0.4), acc: true, model: !!w.model });
        for (const it of cleaned) {
          cands.push(Object.assign({ shop, eff: it.price - (it.pointAmount || 0) }, it));
        }
      } catch (e) {
        errs.push((shop === 'rakuten' ? '楽天' : 'Yahoo!') + '：' + e.message);
      }
      await sleep(1100);
    }
    if (!cands.length) {
      results[w.id] = { error: errs.length ? errs.join(' / ') : '該当する商品が見つかりませんでした' };
      continue;
    }
    const best = cands.reduce((a, b) => (Number(b.price) < Number(a.price) ? b : a));
    const rate = Number(w.channel === 'yahoo_flea' ? set.feeYflea : set.feeMercari) || (w.channel === 'yahoo_flea' ? 5 : 10);
    const fixedCost = (Number(w.ship) || 0) + (Number(w.pack) || 0);
    const calcNet = (eff) => market - (market * rate) / 100 - fixedCost - eff;
    const isOk = (eff) => {
      const n = calcNet(eff);
      return n >= Number(set.minProfit) && (market > 0 ? (n / market) * 100 : 0) >= Number(set.minMargin);
    };
    const cashBest = Number(best.price) || best.eff;
    const net = calcNet(cashBest);
    const margin = market > 0 ? (net / market) * 100 : 0;
    const ok = isOk(cashBest);

    const h = hist[w.id] || [];
    const past = h.filter((e) => e.d !== today);
    const prev = past.length ? past[past.length - 1] : null;
    const dropYen = prev ? prev.eff - best.eff : 0;
    const dropPct = prev && prev.eff > 0 ? (dropYen / prev.eff) * 100 : 0;
    const histMin = past.length ? Math.min(...past.map((e) => e.eff)) : null;
    const isDrop = !!prev && dropYen >= 100 && dropPct >= 2;
    const isLow = past.length >= 2 && best.eff < histMin;
    const prevOk = prev ? isOk(Number(prev.price) || prev.eff) : false;
    hist[w.id] = past.concat([{ d: today, eff: best.eff, shop: best.shop, price: best.price }]).slice(-30);

    results[w.id] = Object.assign({}, best, {
      net, margin, ok, warnings: errs,
      prevEff: prev ? prev.eff : null, dropYen, dropPct, isDrop, isLow, histMin,
      checks: past.length + 1,
      usedExcluded,
    });
    if (ok && (!prevOk || isDrop)) {
      notes.push(
        (w.model || w.name) + '：' + (best.shop === 'rakuten' ? '楽天' : 'Yahoo!') + ' 実質¥' + Math.round(best.eff).toLocaleString('ja-JP') +
        (isDrop ? '（前回比 −¥' + Math.round(dropYen).toLocaleString('ja-JP') + '）' : '') +
        ' 予想利益¥' + Math.round(net).toLocaleString('ja-JP') + ' ' + best.url
      );
    }
  }
  for (const id of Object.keys(hist)) if (!watch.some((x) => x.id === id)) delete hist[id];
  await env.STORE.put('price-history', JSON.stringify(hist));
  if (env.DISCORD_WEBHOOK && notes.length) {
    try {
      await fetch(env.DISCORD_WEBHOOK, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ content: '【型落ち仕入れノート】買える候補・値下がり\n' + notes.join('\n').slice(0, 1800) }),
      });
    } catch (e) {}
  }
  const out = { checkedAt: new Date().toISOString(), results, checkedCount: watch.length, totalCount: (st.watch || []).length };
  await env.STORE.put('watch-results', JSON.stringify(out));
  return out;
}


// 自動選出：候補のJANでKeepaを調べ、利益の概算・売れ行き・Amazon本体・高値づかみを見て 🟢🟡🔴 を付ける
function autoJudge(it, k, minProfit) {
  const sell = k.buyBoxNow || k.newNow || 0;
  const avg = k.buyBoxAvg90 || k.newAvg90 || 0;
  const reasons = [];
  let level = 'ok';
  const down = (to) => { if (to === 'ng' || level === 'ok') level = to; };
  if (!sell) { return { level: 'hold', reasons: ['Amazonの新品価格がKeepaで取れませんでした'], sell: 0, net: 0 }; }
  const fba = sell < 3000 ? 350 : sell < 8000 ? 500 : sell < 15000 ? 700 : 1000;
  const net = Math.round(sell * 0.85 - it.price - fba);
  if (net <= 0) { down('ng'); reasons.push('利益の概算がマイナスです（' + net + '円）'); }
  else if (net < minProfit) { down('hold'); reasons.push('利益の概算 ' + net + '円 は最低利益 ' + minProfit + '円 に届きません'); }
  else reasons.push('利益の概算 ' + net + '円（手数料15%＋配送費' + fba + '円で計算）');
  if (k.amazonNow) { down('ng'); reasons.push('Amazon本体が出品中です'); }
  const rank = k.salesRankNow || 0;
  if (!rank) { down('hold'); reasons.push('ランキングが取れませんでした'); }
  else if (rank > 30000) { down('hold'); reasons.push('ランキング ' + rank + '位：3万位超で需要が弱いです'); }
  else reasons.push('ランキング ' + rank + '位');
  const ms = typeof k.monthlySold === 'number' ? k.monthlySold : null;
  const dr = typeof k.salesRankDrops30 === 'number' ? k.salesRankDrops30 : null;
  if (ms !== null && ms < 3) { down('hold'); reasons.push('月間販売数 ' + ms + '件：少ないです'); }
  else if (ms === null && dr !== null && dr < 3) { down('hold'); reasons.push('30日のランク下がり ' + dr + '回：売れ行きが細いです'); }
  else if (ms === null && dr === null) { down('hold'); reasons.push('売れ行きの数字が取れませんでした'); }
  else reasons.push(ms !== null ? '月間販売数 ' + ms + '件' : '30日のランク下がり ' + dr + '回');
  if (k.sellersNow && k.sellersNow > 10) { down('hold'); reasons.push('新品出品者 ' + k.sellersNow + '人：競争が激しいです'); }
  if (avg && sell > avg * 1.15) { down('hold'); reasons.push('Amazon価格が90日平均の ' + Math.round((sell / avg) * 100) + '%：高値づかみに注意'); }
  // 確実に1個売る用：回転が速く、競争が少なく、高値づかみでない商品に ⭐ を付ける（利益は500円以上あればよい）
  const cost = it.price;
  const spd = ms !== null ? ms : dr;
  const sure = !!(rank && rank <= 10000 && spd !== null && spd >= 10 && !k.amazonNow &&
    (!k.sellersNow || k.sellersNow <= 10) && (!avg || sell <= avg * 1.1) && net >= 500 && cost <= 15000);
  const maxBuy = Math.max(0, Math.round((sell * 0.85 - fba - minProfit) / 10) * 10);
  const maxBuySure = Math.max(0, Math.round((sell * 0.85 - fba - 500) / 10) * 10);
  return { level, reasons, sell, net, rank, monthlySold: ms, drops30: dr, sellers: k.sellersNow || null, asin: k.asin || '', sure, maxBuy, maxBuySure };
}

const KEEPA_MODEL_MAX = 3;
// JANでKeepaに見つからないとき、型番でKeepa検索し、1件にしぼれたときだけ採用する
async function findAsinByModel(env, it, cache) {
  const toks = modelTokens(it.name).filter((t) => t.replace(/[^A-Za-z0-9]/g, '').length >= 6);
  for (const t of toks) {
    const ck = 'm:' + t.toUpperCase();
    if (cache[ck] !== undefined) { if (cache[ck]) return { asin: cache[ck], term: t, cached: true }; continue; }
    const p = new URLSearchParams({ key: env.KEEPA_KEY, domain: '5', type: 'product', term: t, 'asins-only': '1' });
    const res = await reusableApiFetch(env,'https://api.keepa.com/search?' + p.toString());
    if (!res.ok) throw new Error('Keepa側のエラー（' + res.status + '）');
    const data = await res.json();
    const list = Array.isArray(data.asinList) ? data.asinList : [];
    cache[ck] = list.length === 1 ? list[0] : '';
    if (list.length === 1) return { asin: list[0], term: t, tokensLeft: data.tokensLeft };
    if (typeof data.tokensLeft === 'number' && data.tokensLeft < 10) return { tokensLeft: data.tokensLeft };
  }
  return null;
}

async function keepaAutoSelect(env, items, minProfit) {
  const out = { used: false, checked: 0, note: '' };
  if (!env.KEEPA_KEY) { out.note = 'KEEPA_KEY が未設定のため、Keepaの自動チェックは行っていません'; return out; }
  out.used = true;
  let left = Infinity, lookups = 0, modelTries = 0;
  let janCache = {};
  try { janCache = JSON.parse((env.STORE && await env.STORE.get('jan-cache')) || '{}'); } catch (e) {}
  const strong = (x) => validJan(x.jan) || modelTokens(x.name).some((t) => t.replace(/[^A-Za-z0-9]/g, '').length >= 6);
  const pickK = items.filter(strong).concat(items.filter((x) => !strong(x))).slice(0, 5);
  for (const it of pickK) {
    if (!validJan(it.jan) && lookups < JAN_LOOKUP_MAX) {
      lookups++;
      const found = await findJanViaYahoo(env, it, janCache);
      if (found) { it.jan = found; it.janFrom = 'Yahoo!'; out.janFound = (out.janFound || 0) + 1; }
    }
    if (left < 10) { out.note = 'Keepaのトークン残りが少ないため、途中で止めました'; break; }
    let k = null, via = '', lastErr = '';
    try {
      if (validJan(it.jan)) {
        try { k = await fetchKeepa(env, String(it.jan)); via = 'JAN'; }
        catch (e) { if (/429/.test(e.message)) throw e; lastErr = e.message; }
      }
      if (!k && modelTries < KEEPA_MODEL_MAX && modelTokens(it.name).length) {
        modelTries++;
        const f = await findAsinByModel(env, it, janCache);
        if (f && typeof f.tokensLeft === 'number') left = f.tokensLeft;
        if (f && f.asin) { await sleep(600); k = await fetchKeepa(env, f.asin); via = '型番（' + f.term + '）'; it.asinFrom = f.term; }
      }
    } catch (e) {
      lastErr = e.message;
      if (/429/.test(e.message)) { it.kv = { level: 'hold', reasons: ['Keepaで確認できませんでした：' + e.message] }; out.note = 'Keepaの利用上限に達したため、途中で止めました'; break; }
    }
    if (k) {
      if (typeof k.tokensLeft === 'number') left = k.tokensLeft;
      it.kv = autoJudge(it, k, minProfit);
      it.kv.via = via;
      out.checked++;
      if (via !== 'JAN') out.byModel = (out.byModel || 0) + 1;
    } else {
      it.kv = { level: 'hold', reasons: [validJan(it.jan) ? 'Keepaで確認できませんでした：' + (lastErr || 'JANも型番も一致しませんでした') : 'JANが取れず、型番でもKeepaで1件にしぼれませんでした'] };
    }
    await sleep(600);
  }
  try {
    const keys = Object.keys(janCache);
    if (keys.length > 300) for (const k of keys.slice(0, keys.length - 300)) delete janCache[k];
    if (env.STORE) await env.STORE.put('jan-cache', JSON.stringify(janCache));
  } catch (e) {}
  return out;
}

// 楽天の商品はJANが取れないため、商品名の型番でYahoo!を検索してJANを補う（1回のスキャンで最大4件、結果はKVに保存）
const JAN_LOOKUP_MAX = 4;
const SPEC_RE = /^\d+(?:\.\d+)?(?:mah|wh|w|v|a|mm|cm|m|g|kg|ml|l|gb|tb|mb|hz|khz|mhz|ghz|p|k|db|dpi|cc|inch|in|年|代|個|枚|本|色)$/i;
function modelTokens(name) {
  const text = String(name || '').normalize('NFKC').replace(/【[^】]*】|\[[^\]]*\]|（[^）]*）|\([^)]*\)/g, ' ');
  const seen = new Set();
  return (text.match(/[A-Za-z0-9][A-Za-z0-9\-\/]{3,}/g) || [])
    .filter(t => /[A-Za-z]/.test(t) && /\d/.test(t) && !SPEC_RE.test(t) && !/^(usb|type|gen|ver|wifi|ipx|ip\d)/i.test(t))
    .filter(t => { const k = t.toLowerCase(); if (seen.has(k)) return false; seen.add(k); return true; })
    .sort((a, b) => b.length - a.length)
    .slice(0, 2);
}
async function findJanViaYahoo(env, it, cache) {
  const key = String(it.url || it.name || '').slice(0, 200);
  if (cache[key] !== undefined) return cache[key] || '';
  const tokens = modelTokens(it.name);
  if (!tokens.length || !env.YAHOO_APP_ID) return '';
  const q = tokens.join(' ');
  let jan = '';
  try {
    const hits = await searchYahoo(env, q, {});
    const jans = [...new Set(hits.filter(h => validJan(h.jan) && !isUsedItem(h, q) && matchesProduct(h, q)).map(h => h.jan))];
    jan = jans.length === 1 ? jans[0] : '';
    cache[key] = jan;
  } catch (e) { return ''; }
  await sleep(1100);
  return jan;
}

const SCAN_GENERIC = /アウトレット|在庫処分|在庫一掃|型落ち|旧モデル|旧型|訳あり|わけあり|処分品|処分|セール|特価|新品|未開封|未使用|激安|outlet/gi;
const BRAND_ALIAS = [['ロジクール', 'logicool', 'logitech'], ['アンカー', 'anker'], ['dell'], ['エイチピー', 'hp'], ['ソニー', 'sony'], ['パナソニック', 'panasonic']];
function scanNorm(x) { return String(x || '').normalize('NFKC').toLowerCase(); }
function scanBrandTokens(kw) {
  const toks = scanNorm(kw).replace(SCAN_GENERIC, ' ').split(/\s+/).filter((t) => t.length >= 2);
  return toks.map((t) => { const g = BRAND_ALIAS.find((a) => a.includes(t)); return g || [t]; });
}
function scanRelevant(it, kw) {
  const groups = scanBrandTokens(kw);
  const n = scanNorm(it.name), query = scanNorm(kw);
  // A support contract is not stock for resale. Explicit service searches remain available.
  if (/care\s*pack|保守サービス|保守契約|ハードウェアオンサイト|延長保証のみ/i.test(n) && !/care\s*pack|保守|オンサイト|延長保証/i.test(query)) return false;
  const hp = groups.some(g => g.includes('hp'));
  if (hp && /タイヤ|vanti|centara|\d{3}\s*\/\s*\d{2}\s*(?:zr|r)\s*\d{2}/i.test(n) && !/タイヤ|vanti|centara/i.test(query)) return false;
  if (!groups.length) return true;
  return groups.every(g => g.some(t => {
    const escaped = t.replace(/[.*+?^$\x7b\x7d()|[\]\\]/g, '\\$&');
    return /^[a-z0-9]+$/.test(t) ? new RegExp("(^|[^a-z0-9'’])" + escaped + "([^a-z0-9'’]|$)").test(n) : n.includes(t);
  }));
}
function scanNameKey(x) { return scanNorm(x).replace(/【[^】]*】|\[[^\]]*\]|[☆★◆■●※]/g, '').replace(/\s+/g, '').slice(0, 40); }

async function runScan(env, origin) {
  const stateInfo = {};
  const st = await loadState(env, stateInfo);
  const kws = (st.scanKeywords || []).map((x) => String(x).trim()).filter(Boolean).slice(0, 6);
  const min = Number(st.scanMin) || 0;
  const all = [];
  const errors = [];
  let usedExcluded = 0, offTopic = 0;
  for (const kw of kws) {
    for (const shop of ['rakuten', 'yahoo']) {
      try {
        const raw = shop === 'rakuten' ? await searchRakuten(env, kw, origin, { min }) : await searchYahoo(env, kw, { min });
        usedExcluded += raw.filter((it) => isUsedItem(it, kw)).length;
        offTopic += raw.filter(it => !scanRelevant(it, kw)).length;
        const items = cleanItems(raw, kw, { min, acc: true, model: false, scan: true });
        for (const it of items) {
          const pts = it.pointAmount || 0;
          const disc = it.defaultPrice && it.defaultPrice > it.price ? ((it.defaultPrice - it.price) / it.defaultPrice) * 100 : 0;
          const ptPct = it.price ? (pts / it.price) * 100 : 0;
          all.push(Object.assign({}, it, {
            shop, keyword: kw,
            eff: it.price - pts,
            discountPct: Math.round(disc * 10) / 10,
            pointPct: Math.round(ptPct * 10) / 10,
            score: disc + ptPct,
          }));
        }
      } catch (e) {
        errors.push(kw + '（' + (shop === 'rakuten' ? '楽天' : 'Yahoo!') + '）：' + e.message);
      }
      await sleep(1100);
    }
  }
  const seen = new Set(), seenName = new Set();
  const ranked = all
    .sort((a, b) => b.score - a.score || (b.reviewCount || 0) - (a.reviewCount || 0))
    .filter((x) => (x.url && !seen.has(x.url) ? seen.add(x.url) : false))
    .filter((x) => { const k = scanNameKey(x.name); if (!k) return true; if (seenName.has(k)) return false; seenName.add(k); return true; })
    ;
  const top = ranked.slice(0, 15);
  const minProfit = Number(st.settings && st.settings.minProfit) || 1500;
  const kinfo = await keepaAutoSelect(env, top, minProfit);
  if (kinfo.used) {
    const rankOf = { ok: 0, hold: 1, ng: 2 };
    const sureOf = (x) => (x.kv && x.kv.sure ? 0 : 1);
    top.forEach((x, i) => { x._i = i; });
    top.sort((a, b) => (sureOf(a) - sureOf(b)) || (rankOf[(a.kv && a.kv.level) || 'hold'] - rankOf[(b.kv && b.kv.level) || 'hold']) || (a._i - b._i));
    top.forEach((x) => { delete x._i; });
  }
  let prevUrls = new Set();
  try {
    const prev = JSON.parse((await env.STORE.get('scan-results')) || '{}');
    prevUrls = new Set((prev.items || []).map((x) => x.url));
  } catch (e) {}
  const out = { at: new Date().toISOString(), items: top.concat(ranked.slice(15)), errors, usedExcluded, offTopic, keepa: kinfo, keywords: kws, minPrice: min, stateInfo, fetchedCount: all.length };
  await env.STORE.put('scan-results', JSON.stringify(out));
  out.ledger = await recordCandidateBatch(env, 'scan', out.items, out.at);
  const fresh = (kinfo.used ? top.filter((x) => x.kv && (x.kv.level === 'ok' || x.kv.sure)) : top.slice(0, 3)).slice(0, 5).filter((x) => !prevUrls.has(x.url));
  if (env.DISCORD_WEBHOOK && fresh.length) {
    try {
      await fetch(env.DISCORD_WEBHOOK, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          content: (kinfo.used ? '【自動選出】Keepa確認済みの候補（⭐＝優先確認）\n' : '【セールの押し検索】新しく上位に入った商品\n') + '検索語：' + kws.join(' / ') + '（設定元：' + stateInfo.source + '）\n' + fresh.map((x) =>
            (x.shop === 'rakuten' ? '楽天' : 'Yahoo!') + ' ¥' + Math.round(x.price).toLocaleString('ja-JP') +
            '（' + (x.discountPct ? '通常比−' + x.discountPct + '% ／ ' : '') + 'ポイント' + x.pointPct + '%）' + (x.kv && x.kv.net ? ' 利益概算¥' + x.kv.net : '') + (x.kv && x.kv.sure ? ' ⭐' : '') + (x.kv && x.kv.maxBuySure ? ' 仕入れ上限¥' + x.kv.maxBuySure : '') + ' ' + x.name.slice(0, 60) + ' ' + x.url
          ).join('\n').slice(0, 1800),
        }),
      });
    } catch (e) {}
  }
  return out;
}

// Keepa Deals：Amazonで値下がりした新品を取得し、足切りして上位を保存（1ページ5トークン）
const DEAL_NG = /中古|整備済|再生品|互換|フィルム|保護|交換用|替え|詰め替え|詰替|食品|飲料|サプリ|医薬|化粧|コスメ|コミック|書籍|文庫|DVD|Blu-?ray|CD|ゲームソフト|ダウンロード版|ギフト券|ケース|カバー|ストラップ|ステッカー|シール|消耗品|インク|トナー|電池|ペット|お試し|サンプル|アダルト|Amazon\.co\.jp限定|Amazon限定|改良|進化モデル|最新モデル】|最新版】|【20\d\d|20\d\d年(新|最新|改良)|業界初|互換性|対応機種/i;
// 家電・パソコン周辺・おもちゃ・ホビー・ゲーム（Amazon.co.jpのルートカテゴリ）
const DEAL_CATS = [3210981, 2127209051, 13299531, 2277721051, 637394];
const DEAL_DEF = { min: 3000, max: 20000, drop: 20, dropMax: 70, rankMax: 50000, dateRange: 1, keep: 30 };
function dealPick(arr, dr, t) { if (!Array.isArray(arr)) return null; const v = Array.isArray(arr[0]) ? (arr[dr] || [])[t] : arr[t]; return typeof v === 'number' && v >= 0 ? v : null; }
async function runDeals(env, opt) {
  if (!env.KEEPA_KEY) return { error: 'KEEPA_KEY が未設定です' };
  let st = {}; try { st = (await loadState(env)) || {}; } catch (e) {}
  const s = Object.assign({}, DEAL_DEF, (st.settings && st.settings.deal) || {}, opt || {});
  const out = { at: new Date().toISOString(), settings: s, fetched: 0, dropped: 0, items: [], errors: [], tokensLeft: null };
  const seen = new Set();
  for (const pt of [0, 1]) {
    const sel = { page: 0, domainId: 5, priceTypes: [pt], dateRange: s.dateRange, isFilterEnabled: true, isRangeEnabled: true,
      currentRange: [s.min, s.max], deltaPercentRange: [Math.max(10, s.drop), 100], salesRankRange: [1, s.rankMax],
      hasReviews: true, singleVariation: true, filterErotic: true, sortType: 4, includeCategories: DEAL_CATS };
    try {
      const res = await reusableApiFetch(env,'https://api.keepa.com/deal?key=' + encodeURIComponent(env.KEEPA_KEY), { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(sel) });
      const data = await res.json().catch(() => ({}));
      if (typeof data.tokensLeft === 'number') out.tokensLeft = data.tokensLeft;
      if (!res.ok) { out.errors.push((pt ? '新品' : 'Amazon本体') + '：Keepa側のエラー（' + res.status + '）'); if (res.status === 429) break; continue; }
      const dr = (data.deals && data.deals.dr) || [];
      out.fetched += dr.length;
      for (const d of dr) {
        if (!d || !d.asin || seen.has(d.asin)) continue;
        const title = String(d.title || '');
        const price = dealPick(d.current, s.dateRange, pt);
        const avg = dealPick(d.avg, s.dateRange, pt), pct = dealPick(d.deltaPercent, s.dateRange, pt);
        const rank = dealPick(d.current, s.dateRange, 3);
        if (!price || DEAL_NG.test(title) || !title) { out.dropped++; continue; }
        // 出品者の極端な値付けで平均が歪んだ「見せかけの値下がり」を外す
        if ((pct != null && pct > s.dropMax) || (avg && avg > price * 3)) { out.dropped++; out.fake = (out.fake || 0) + 1; continue; }
        // 英字だけのタイトル（洋書・海外品）を外す
        if (!/[ぁ-んァ-ヶ一-龠]/.test(title)) { out.dropped++; continue; }
        seen.add(d.asin);
        out.items.push({ asin: d.asin, title: title.slice(0, 140), price, avg, drop: pct, rank, avg30: dealPick(d.avg, 2, pt), avg90: dealPick(d.avg, 3, pt), seller: pt ? '新品最安' : 'Amazon本体',
          url: 'https://www.amazon.co.jp/dp/' + d.asin, keepa: 'https://keepa.com/#!product/5-' + d.asin });
      }
    } catch (e) { out.errors.push((pt ? '新品' : 'Amazon本体') + '：' + e.message); }
  }
  out.items.sort((a, b) => (b.drop || 0) - (a.drop || 0) || (a.rank || 1e9) - (b.rank || 1e9));
  out.items = out.items.slice(0, s.keep);
  if (env.STORE) {
    await env.STORE.put('keepa-deals', JSON.stringify(out));
    out.ledger = await recordCandidateBatch(env, 'keepa-deals', out.items, out.at);
  }
  return out;
}


// 値上がり探索（v1.5.64・手動）：Amazon本体が売っていない（在庫切れ等）のに新品最安が90日平均より上がっている売れ筋を、Keepa Dealsの「値上がり」で探す（5トークン）
async function runRise(env, opt) {
  if (!env.KEEPA_KEY) return { error: 'KEEPA_KEY が未設定です' };
  const o = Object.assign({ min: 3000, max: 30000, rise: 15, rankMax: 50000, dateRange: 2 }, opt || {});
  const out = { at: new Date().toISOString(), fetched: 0, dropped: 0, items: [], errors: [], tokensLeft: null };
  const sel = { page: 0, domainId: 5, priceTypes: [1], dateRange: o.dateRange, isFilterEnabled: true, isRangeEnabled: true, isRisers: true, mustNotHaveAmazonOffer: true,
    currentRange: [o.min, o.max], deltaPercentRange: [o.rise, 300], salesRankRange: [1, o.rankMax], hasReviews: true, singleVariation: true, filterErotic: true, sortType: 4, includeCategories: DEAL_CATS };
  if (o.sel && typeof o.sel === 'object') Object.assign(sel, o.sel); // 試験用：条件の上書き
  out.sel = sel;
  try {
    const res = await reusableApiFetch(env,'https://api.keepa.com/deal?key=' + encodeURIComponent(env.KEEPA_KEY), { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(sel) });
    const data = await res.json().catch(() => ({}));
    if (typeof data.tokensLeft === 'number') out.tokensLeft = data.tokensLeft;
    if (!res.ok) { out.errors.push('Keepa側のエラー（' + res.status + '）' + (data.error ? JSON.stringify(data.error).slice(0, 120) : '')); return out; }
    const dr = (data.deals && data.deals.dr) || [];
    out.fetched = dr.length;
    for (const d of dr) {
      const title = String(d.title || '');
      const price = dealPick(d.current, o.dateRange, 1), avg = dealPick(d.avg, o.dateRange, 1), pct = dealPick(d.deltaPercent, o.dateRange, 1);
      const amz90 = dealPick(d.avg, 3, 0), rank = dealPick(d.current, o.dateRange, 3);
      if (!d.asin || !price || !title || DEAL_NG.test(title) || !/[ぁ-んァ-ヶ一-龠]/.test(title)) { out.dropped++; continue; }
      if (avg && price > avg * 3) { out.dropped++; continue; } // 1店だけの極端な値付け
      out.items.push({ asin: d.asin, title: title.slice(0, 140), price, avg, rise: pct != null ? Math.abs(pct) : null, avg90: dealPick(d.avg, 3, 1), amz90, rank,
        url: 'https://www.amazon.co.jp/dp/' + d.asin, keepa: 'https://keepa.com/#!product/5-' + d.asin });
    }
  } catch (e) { out.errors.push(e.message); }
  out.items.sort((a, b) => (a.rank || 1e9) - (b.rank || 1e9));
  if (env.STORE) await env.STORE.put('keepa-rise', JSON.stringify(out));
  return out;
}

// 市場価格比較（v1.5.49）：楽天・Yahoo!の新品最安とAmazonを比べ、「Amazonだけ安い／市場全体が安い」を判定
const MARKET_TTL_H = 12, MARKET_MAX = 200;
const MK_NG = /アウトレット|訳あり|わけあり|訳有|展示品|展示処分|箱潰れ|箱つぶれ|箱傷|外箱傷|箱ダメージ|並行輸入|海外版|輸入品|再生品|整備済|リファービッシュ|開封品|開封済|中古|ジャンク|取り寄せ|取寄せ|取寄|お取り寄せ|予約/;
const MK_ACC = /交換用|互換|ポップフィルター|ショックマウント|イヤーパッド|イヤーピース|イヤーチップ|保護フィルム|ケースのみ|カバー|スキンシール|ステッカー|部品|パーツ|替え|空箱|箱のみ/;
const MK_COLORS = [['ブラック', '黒', 'black'], ['ホワイト', '白', 'white'], ['グレー', 'gray', 'grey'], ['シルバー', 'silver'], ['レッド', '赤', 'red'], ['ブルー', '青', 'blue'], ['ピンク', 'pink'], ['グリーン', '緑', 'green'], ['イエロー', '黄', 'yellow'], ['パープル', '紫', 'purple']];
function mkColors(s) {
  const t = String(s || '').normalize('NFKC').toLowerCase();
  const out = [];
  MK_COLORS.forEach((c, i) => { if (c.some((w) => (/^[a-z]/.test(w) ? new RegExp('(^|[^a-z])' + w + '([^a-z]|$)').test(t) : t.includes(w)))) out.push(i); });
  return out;
}
function mkCode(title) {
  const t = String(title || '').normalize('NFKC').toUpperCase();
  const m = t.match(/[A-Z0-9]{2,}(?:-[A-Z0-9]+)+/g) || [];
  return m.filter((x) => /\d/.test(x) && /[A-Z]/.test(x) && x.length >= 5)[0] || '';
}
function mkWords(title) {
  const t = String(title || '').normalize('NFKC').replace(/[【\[(（「][^】\])）」]*[】\])）」]/g, ' ').replace(/[、,/|｜:：・]/g, ' ');
  const w = t.split(/\s+/).filter((x) => x && x.length <= 20 && !/^(新品|未開封|送料無料|正規品|国内正規品|日本正規品|公式|限定|セール|Amazon限定)$/i.test(x));
  const out = [];
  for (const x of w) { if (out.length >= 4 || (out.length >= 2 && /[ぁ-んァ-ヶ一-龠]/.test(x) && x.length > 4)) break; out.push(x); }
  return out.join(' ') || t.trim().slice(0, 30);
}
function mkMedian(a) { const s = a.slice().sort((x, y) => x - y); const n = s.length; return n ? (n % 2 ? s[(n - 1) / 2] : Math.round((s[n / 2 - 1] + s[n / 2]) / 2)) : null; }
// 判定：EC相場との差（gap）とKeepa 90日平均との差（kDrop）
function mkJudge(amazon, ec, avg90, prev) {
  const gap = amazon && ec ? Math.round(((ec - amazon) / ec) * 100) : null;
  const kDrop = amazon && avg90 ? Math.round(((avg90 - amazon) / avg90) * 100) : null;
  if (gap == null) return { type: 'none', label: '⚪ 楽天・Yahoo!で同じ商品が見つからない', gap, kDrop, note: '検索語を変えるか、手で確認してください' };
  if (prev && prev.type === 'sale' && prev.amazon && amazon > prev.amazon && gap < 15)
    return { type: 'recover', label: '🟢 相場回復確認（Amazonの特価が終わり、楽天・Yahoo!並みに戻った）', gap, kDrop, note: 'セール時に仕入れた分は、メルカリ相場が崩れていなければ通常どおり出品できます' };
  if (gap >= 25) {
    const sure = kDrop == null ? '中' : kDrop >= 15 ? '高' : '中';
    return { type: 'sale', label: gap >= 30 ? '🔥 セール仕入れ候補（Amazonだけ安い）' : '🔥 Amazonだけ安い（一時的な特価）', gap, kDrop, sure,
      note: '一時的な特価の可能性：' + sure + (kDrop != null ? '（Keepa 90日平均より' + kDrop + '%安い）' : '') + '。Amazonのセール終了後、相場回復の期待あり' };
  }
  if (gap >= 10) return { type: 'cheap', label: '🟡 やや割安（Amazonが' + gap + '%安い）', gap, kDrop, note: '楽天・Yahoo!との差は小さめ。メルカリ相場で判断してください' };
  if (kDrop != null && kDrop >= 15) return { type: 'fall', label: '🔴 相場下落注意（市場全体が安い）', gap, kDrop, note: 'Amazonだけでなく楽天・Yahoo!も同じくらい安い。メルカリ相場も下がる可能性があります' };
  return { type: 'flat', label: '⚪ 割安感なし（楽天・Yahoo!と同程度）', gap, kDrop, note: 'Amazonが特別安いわけではありません' };
}
async function marketCheck(env, origin, p) {
  const all = JSON.parse((await env.STORE.get('market-cache')) || '{}');
  const key = p.asin || ('q:' + (p.q || p.title || '').slice(0, 60));
  const prev = all[key];
  let amazon = Math.round(Number(p.amazon)) || (prev && prev.amazon) || null, amazonSrc = 'deal', avg90 = Number(p.avg90) || (prev && prev.avg90) || null;
  if (prev && !p.refresh && !p.keepa && Date.now() - Date.parse(prev.at) < MARKET_TTL_H * 3600000 && (!amazon || amazon === prev.amazon)) return Object.assign({ cached: true }, prev);
  const errors = [];
  if (p.keepa && p.asin && env.KEEPA_KEY) {
    try { const k = await fetchKeepa(env, p.asin); const now = k.amazonNow || k.newNow || k.buyBoxNow; if (now) { amazon = now; amazonSrc = 'keepa'; } avg90 = k.amazonAvg90 || k.newAvg90 || avg90; } catch (e) { errors.push('Keepa：' + e.message); }
  }
  const title = String(p.title || (prev && prev.title) || '');
  const qs = [];
  const words = mkWords(title), noBrand = words.split(' ').slice(1).join(' ');
  for (const x of [p.q, mkCode(title), words, noBrand.split(' ').length >= 2 || /\d/.test(noBrand) ? noBrand : '']) { const v = String(x || '').trim(); if (v && !qs.includes(v)) qs.push(v); }
  const want = mkColors(title);
  let items = [], usedQ = '';
  for (const q of qs.slice(0, 3)) {
    if (usedQ) await sleep(1100); // 楽天は1秒に1回まで
    const opts = { model: true, acc: false, condition: 'new', min: amazon ? Math.round(amazon * 0.6) : 0 };
    // 楽天は1文字の語を受け付けないので外して検索し、絞り込みは元の語で行う
    const rq = q.split(/\s+/).filter((x) => !/^[\x00-\x7f]$/.test(x)).join(' ') || q;
    const shortNum = q.split(/\s+/).filter((x) => /^\d{1,2}$/.test(x));
    const [r, y] = await Promise.all([
      searchRakuten(env, rq, origin, opts).catch((e) => { errors.push('楽天：' + e.message); return []; }),
      searchYahoo(env, q, opts).catch((e) => { errors.push('Yahoo!：' + e.message); return []; }),
    ]);
    // 比較用の絞り込み：同じ商品名・型番を含む新品だけ。部品・付属品は名前と価格（Amazonの6割未満）で外す
    const pick = (arr, shop) => arr.filter((i) => Number(i.price) > 0 && !isUsedItem(i, q) && matchesProduct(i, q)).filter((i) => {
      if (MK_NG.test(String(i.name))) return false;
      const acc = String(i.name).match(MK_ACC);
      if (acc && !title.includes(acc[0])) return false;
      // 「Pro 3」の3が「SN30」に部分一致しないよう、短い数字は単独の語として一致を確認
      const nm = String(i.name).normalize('NFKC');
      if (shortNum.some((d) => !new RegExp('(^|[^0-9])' + d + '([^0-9]|$)').test(nm))) return false;
      if (amazon && (i.price < amazon * 0.6 || i.price > amazon * 4)) return false;
      const c = mkColors(i.name);
      return !(want.length && c.length && !c.some((x) => want.includes(x)));
    }).map((i) => ({ shop, price: Math.round(i.price), ship: !!i.shippingIncluded, shipTxt: String(i.shipping || ''), pt: Math.round(Number(i.pointAmount) || 0), name: String(i.name || '').slice(0, 60), url: String(i.url || '').slice(0, 300), seller: String(i.seller || '').slice(0, 30) }));
    // 型番と商品名の両方で探して合わせる（店によって型番を書かない出品がある）
    const seenU = new Set(items.map((i) => i.url));
    for (const i of pick(r, 'rakuten').concat(pick(y, 'yahoo'))) if (!seenU.has(i.url)) { seenU.add(i.url); items.push(i); }
    items.sort((a, b) => a.price - b.price);
    usedQ = usedQ ? usedQ + '／' + q : q;
    const nR = items.filter((i) => i.shop === 'rakuten').length, nY = items.length - nR;
    if (qs.indexOf(q) >= 1 && nR >= 1 && nY >= 1 && items.length >= 4) break;
  }
  const best = (shop) => items.filter((i) => i.shop === shop)[0] || null;
  const top = items.slice(0, 6);
  const ec = mkMedian(top.map((i) => i.price));
  const j = mkJudge(amazon, ec, avg90, prev);
  const hist = ((prev && prev.hist) || []).concat([{ at: new Date().toISOString(), amazon, ec, type: j.type }]).slice(-12);
  // セールを一度観測した商品は、その後「回復」を判定できるよう基準のAmazon価格を残す
  const saleAmazon = j.type === 'sale' ? amazon : (prev && prev.saleAmazon) || null;
  const rec = { at: new Date().toISOString(), asin: p.asin || '', title: title.slice(0, 140), q: usedQ, amazon, amazonSrc, avg90, rakuten: best('rakuten'), yahoo: best('yahoo'),
    ec, ecN: top.length, shipOut: top.filter((i) => !i.ship).length, gap: j.gap, kDrop: j.kDrop, type: j.type, label: j.label, sure: j.sure || null, note: j.note,
    items: items.slice(0, 8), hist, saleAmazon, errors: errors.slice(0, 3) };
  if (j.type !== 'sale' && saleAmazon && amazon > saleAmazon && j.gap != null && j.gap < 15 && j.type !== 'recover') {
    Object.assign(rec, mkJudge(amazon, ec, avg90, { type: 'sale', amazon: saleAmazon }));
    rec.type = 'recover';
  }
  all[key] = rec;
  const keys = Object.keys(all).sort((a, b) => String(all[b].at).localeCompare(String(all[a].at)));
  for (const k of keys.slice(MARKET_MAX)) delete all[k];
  await env.STORE.put('market-cache', JSON.stringify(all));
  return rec;
}

// Yahoo!深掘り（v1.5.51）：Yahoo!のセール中の新品をキーワードごとに集め、通常価格より大きく安く、
// しかも他のYahoo!出品者・楽天の相場より安い「この店だけ安い」商品だけ残す。最後のメルカリ確認は拡張で人が押す。
const DIG_KW = ['ワイヤレスイヤホン', 'ヘッドホン', 'Bluetoothスピーカー', 'ゲーミングマウス', 'ゲーミングキーボード', 'ゲーミングヘッドセット', 'USBマイク', 'Webカメラ', 'モバイルバッテリー', 'スマートウォッチ', 'ドライヤー', 'ヘアアイロン', '電動歯ブラシ', 'シェーバー', '美顔器', '電気ケトル', 'ハンディファン', 'ゲームコントローラー', 'SSD', 'キャプチャーボード', 'ベイブレード', 'プラモデル', 'トミカ', 'レゴ', 'ラベルライター', '電動工具', '無線LANルーター', 'ポータブルSSD'];
const DIG_NG = /中古|ジャンク|訳あり|わけあり|アウトレット|展示品|再生品|整備済|リファービッシュ|開封品|箱潰れ|箱つぶれ|並行輸入|海外版|互換|交換用|替え|替刃|部品|パーツ|ケースのみ|カバーのみ|専用ケース|保護ケース|保護カバー|フィルム|変換アダプタ|イヤーピース|イヤーパッド|ストラップ|単品販売|他社|時間限定|特典あり|\d点購入|\d点で|\d個購入|1点あたり|１点あたり|まとめ買い|クーポン利用で|クーポンで|\d個セット|\d点セット|予約|取り寄せ|取寄|受注生産|福袋|まとめ売り|セット売り|ノーブランド|汎用/;
async function yahooRaw(env, params) {
  const p = new URLSearchParams(Object.assign({ appid: env.YAHOO_APP_ID, results: '100', condition: 'new', in_stock: 'true' }, params));
  let res;
  for (let t = 0; t < 3; t++) {
    res = await yahooFetch(env, p, 'dig');
    if (res.status !== 429) break;
    await sleep(2000 + t * 1500); // 1秒に1回の制限：待ってやり直す
  }
  await sleep(1100);
  if (!res.ok) { const error = new Error('Yahoo!側のエラー（' + res.status + '）'); error.status = res.status; throw error; }
  return ((await res.json()).hits || []);
}
function digItem(h) {
  const pl = h.priceLabel || {};
  const base = Math.max(Number(pl.defaultPrice) || 0, Number(pl.fixedPrice) || 0);
  return { name: String(h.name || '').slice(0, 120), price: Math.round(Number(h.price) || 0), base: Math.round(base), jan: validJan(h.janCode) ? String(h.janCode) : '',
    url: String(h.url || '').slice(0, 300), seller: String((h.seller && h.seller.name) || '').slice(0, 40), sellerId: String((h.seller && h.seller.sellerId) || ''),
    ship: !!(h.shipping && h.shipping.name === '送料無料'), pt: Math.round(Number(h.point && (Number(h.point.lyLimitedBonusAmount) || h.point.amount)) || 0), brand: String((h.brand && h.brand.name) || '').slice(0, 40),
    genre: String((h.genreCategory && h.genreCategory.name) || '').slice(0, 30), reviews: Number(h.review && h.review.count) || 0, img: String((h.image && h.image.medium) || '').slice(0, 300) };
}
function digKey(it) { return it.jan || norm(it.name).slice(0, 30); }
async function runDig(env, origin, o) {
  if (!env.YAHOO_APP_ID) return { error: 'Yahoo!のIDが未設定です' };
  const kw = String(o.kw || '').trim().slice(0, 40);
  if (!kw) return { error: 'キーワードがありません' };
  const min = Math.max(1000, Number(o.min) || 4000), max = Math.min(100000, Number(o.max) || 15000), off = Math.min(90, Math.max(10, Number(o.off) || 35));
  const out = { kw, at: new Date().toISOString(), fetched: 0, sale: 0, checked: 0, items: [], errors: [] };
  let hits = [];
  try { hits = await yahooRaw(env, { query: kw, price_from: String(min), price_to: String(max), is_discount: 'true', sort: '-score' }); }
  catch (e) { if (e.status !== 400) { out.errors.push(e.message); return out; } try { hits = await yahooRaw(env, { query: kw, price_from: String(min), price_to: String(max), sort: '-score' }); } catch (e2) { out.errors.push(e2.message); return out; } }
  out.fetched = hits.length;
  // 1) 通常価格より大きく安いセール品（中古・部品・訳ありなどは外す）
  const seen = new Set();
  let cand = hits.map(digItem).filter((it) => it.price > 0 && it.base > it.price && it.brand && !/ブランド登録なし|ノーブランド|その他/.test(it.brand) && !DIG_NG.test(it.name) && !USED_RE.test(it.name))
    .map((it) => Object.assign(it, { off: Math.round((1 - it.price / it.base) * 100) }))
    .filter((it) => it.off >= off && it.off <= 85)
    .sort((a, b) => b.off - a.off)
    .filter((it) => { const k = digKey(it); if (seen.has(k)) return false; seen.add(k); return true; });
  out.sale = cand.length;
  // 2) 他店の相場と比べる（JANがあればJANで、なければ商品名の先頭で）。上位6件まで（1件あたりAPI 2〜3回）
  for (const it of cand.slice(0, 6)) {
    try {
      const q = it.jan || mkWords(it.name);
      const others = [];
      const ys = await yahooRaw(env, it.jan ? { jan_code: it.jan, sort: '+price', results: '30' } : { query: q, sort: '+price', results: '30' });
      for (const h of ys) { const x = digItem(h); if (x.sellerId !== it.sellerId && x.price > it.price * 0.5 && !DIG_NG.test(x.name) && (it.jan ? x.jan === it.jan || !x.jan : matchesProduct(x, q))) others.push({ shop: 'yahoo', price: x.price, ship: x.ship, name: x.name.slice(0, 50), url: x.url }); }
      // 商品名（ブランド＋型番）でも探す：Yahoo!のJAN検索は1店しか出ないことが多く、楽天はJANでほぼ見つからない
      const wq = mkCode(it.name) || mkWords(it.name);
      if (it.jan && others.length < 2 && wq) {
        const ys2 = await yahooRaw(env, { query: wq, sort: '+price', results: '30' }).catch(() => []);
        for (const h of ys2) { const x = digItem(h); if (x.sellerId !== it.sellerId && x.price > it.price * 0.5 && !DIG_NG.test(x.name) && matchesProduct(x, wq) && !others.some((o) => o.url === x.url)) others.push({ shop: 'yahoo', price: x.price, ship: x.ship, name: x.name.slice(0, 50), url: x.url }); }
      }
      const rq = (wq || q).split(/\s+/).filter((x) => !/^[\x00-\x7f]$/.test(x)).join(' ');
      const rs = await searchRakuten(env, rq, origin, { condition: 'new' }).catch((e) => { out.errors.push('楽天：' + e.message); return []; });
      await sleep(1100);
      for (const x of rs) if (x.price > it.price * 0.5 && !DIG_NG.test(String(x.name)) && !USED_RE.test(String(x.name)) && matchesProduct(x, wq || q)) others.push({ shop: 'rakuten', price: Math.round(x.price), ship: !!x.shippingIncluded, name: String(x.name).slice(0, 50), url: String(x.url || '').slice(0, 300) });
      others.sort((a, b) => a.price - b.price);
      const top = others.slice(0, 6);
      it.mkt = mkMedian(top.map((x) => x.price));
      it.mktN = top.length;
      it.gap = it.mkt ? Math.round((1 - it.price / it.mkt) * 100) : null;
      it.others = others.slice(0, 5);
      it.q = wq || q;
      // 「この店だけ安い」＝他店が2件以上あり、相場より20%以上安い
      it.lv = it.mktN >= 2 && it.gap >= 20 ? 'only' : it.mktN >= 2 && it.gap < 10 ? 'flat' : it.mktN < 2 ? 'few' : 'cheap';
      out.checked++;
      await sleep(1100);
    } catch (e) { out.errors.push(e.message); }
  }
  // 他店と比べられない商品は、JANがあるときだけ残す（無名品の「見せかけの通常価格」を外す）
  out.items = cand.slice(0, 6).filter((it) => it.lv === 'only' || it.lv === 'cheap' || (it.lv === 'few' && it.jan));
  // 保存（URLごとに上書き・最大80件）
  if (env.STORE) {
    const all = JSON.parse((await env.STORE.get('dig-results')) || '{}');
    for (const it of out.items) all[it.url] = Object.assign({ kw, at: out.at }, it);
    const keys = Object.keys(all).sort((a, b) => String(all[b].at).localeCompare(String(all[a].at)));
    for (const k of keys.slice(80)) delete all[k];
    await env.STORE.put('dig-results', JSON.stringify(all));
  }
  out.ledger = await recordCandidateBatch(env, 'yahoo-dig', out.items, out.at);
  return out;
}


// 楽天デッドストック探索：在庫処分・生産終了・廃番などの新品を楽天で探し、Yahoo!・楽天の他店と比べて「この店だけ安い」を残す（手動実行）
const DEAD_KW = ['在庫処分', '生産終了', '廃番', '旧モデル', '最終在庫', '型落ち'];
const DEAD_CAT = ['ワイヤレスイヤホン', 'ヘッドホン', 'Bluetoothスピーカー', 'ゲーミングマウス', 'ゲーミングキーボード', 'ゲーミングヘッドセット', 'USBマイク', 'Webカメラ', 'スマートウォッチ', 'ドライヤー', '電動歯ブラシ', 'シェーバー', '美顔器', 'ゲームコントローラー', 'キャプチャーボード', 'ロボット掃除機', 'コーヒーメーカー', 'デジタルカメラ'];
const DEAD_OK = /在庫処分|生産終了|廃番|旧モデル|旧型|最終在庫|型落ち|倉庫整理|現品限り|在庫限り/g;
async function rakutenRaw(env, origin, params) {
  if (!env.RAKUTEN_APP_ID || !env.RAKUTEN_ACCESS_KEY) throw new Error('楽天のIDが未設定です');
  const p = new URLSearchParams(Object.assign({ applicationId: env.RAKUTEN_APP_ID, accessKey: env.RAKUTEN_ACCESS_KEY, hits: '30', availability: '1', format: 'json', formatVersion: '2' }, params));
  const ref = env.RAKUTEN_REFERER || origin || '';
  const headers = new Headers();
  if (ref) { headers.set('Referer', ref); try { headers.set('Origin', new URL(ref).origin); } catch (e) {} }
  let res;
  for (let t = 0; t < 3; t++) {
    res = await reusableApiFetch(env,'https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20260701?' + p.toString(), { headers });
    if (res.status !== 429) break;
    await sleep(2000 + t * 1500);
  }
  await sleep(1100);
  if (!res.ok) { let d = ''; try { d = (await res.text()).slice(0, 120); } catch (e) {} throw new Error('楽天側のエラー（' + res.status + '）' + d); }
  const data = await res.json();
  return (data.items || data.Items || []).map((x) => { const h = x.Item || x; const rate = Number(h.pointRate) || 1; return {
    name: String(h.itemName || '').slice(0, 120), price: Math.round(Number(h.itemPrice) || 0), url: String(h.itemUrl || '').slice(0, 300), seller: String(h.shopName || '').slice(0, 40), sellerId: String(h.shopCode || ''),
    ship: h.postageFlag === 0, pt: Math.floor((Number(h.itemPrice) || 0) * rate / 100), jan: extractJan(h.itemName) || '', reviews: Number(h.reviewCount) || 0 }; });
}
async function runDead(env, origin, o) {
  const kw = String(o.kw || '').trim().slice(0, 20), cat = String(o.cat || '').trim().slice(0, 30), shop = String(o.shop || '').trim().slice(0, 40);
  if (!kw || (!cat && !shop)) return { error: 'キーワードがありません' };
  const min = Math.max(1000, Number(o.min) || (o.shop ? 10000 : 4000)), max = Math.min(100000, Number(o.max) || (o.shop ? 60000 : 20000));
  const q = (kw + ' ' + cat).trim();
  const out = { src: 'rakuten', shop, kw: q + (shop ? '＠' + shop : ''), at: new Date().toISOString(), fetched: 0, sale: 0, checked: 0, items: [], errors: [], drop: { ng: 0, noCode: 0, dup: 0 } };
  let hits = [];
  try { hits = await rakutenRaw(env, origin, Object.assign({ keyword: q, minPrice: String(min), maxPrice: String(max), NGKeyword: '中古 ジャンク 訳あり', sort: 'standard' }, shop ? { shopCode: shop } : {})); }
  catch (e) { out.errors.push(e.message); return out; }
  out.fetched = hits.length;
  const seen = new Set();
  const cand = [];
  for (const it of hits) {
    const nm = it.name.replace(DEAD_OK, ' ');
    if (!(it.price > 0) || DIG_NG.test(nm) || USED_RE.test(nm)) { out.drop.ng++; continue; }
    const um = String(it.url).match(/\/(4\d{12}|49\d{11}|45\d{11})(?:[\/?_-]|$)/);
    if (!it.jan && um && validJan(um[1])) it.jan = um[1];
    const code = mkCode(it.name) || it.jan;
    if (!code) { out.drop.noCode++; continue; }
    if (seen.has(code)) { out.drop.dup++; continue; }
    seen.add(code); it.code = code; cand.push(it);
  }
  out.sale = cand.length;
  for (const it of cand.slice(0, 6)) {
    try {
      const others = [], brands = {};
      const isJan = /^\d{13}$/.test(it.code);
      const ys = await yahooRaw(env, isJan ? { jan_code: it.code, sort: '+price', results: '30' } : { query: it.code, sort: '+price', results: '30' }).catch((e) => { out.errors.push('Yahoo!：' + e.message); return []; });
      for (const h of ys) { const x = digItem(h); if (!isJan && !matchesProduct(x, it.code)) continue; if (isJan && x.jan && x.jan !== it.code) continue; if ( DIG_NG.test(x.name) || USED_RE.test(x.name)) continue; if (x.brand && !/ブランド登録なし|ノーブランド|その他/.test(x.brand)) brands[x.brand] = (brands[x.brand] || 0) + 1; if (!it.jan && x.jan) it.jan = x.jan; if (x.price > it.price * 0.5) others.push({ shop: 'yahoo', price: x.price, ship: x.ship, name: x.name.slice(0, 50), url: x.url }); }
      const rs = await rakutenRaw(env, origin, { keyword: it.code, sort: '+itemPrice', NGKeyword: '中古 ジャンク' }).catch((e) => { out.errors.push('楽天：' + e.message); return []; });
      for (const x of rs) if (x.sellerId !== it.sellerId && x.url !== it.url && x.price > it.price * 0.5 && !DIG_NG.test(x.name.replace(DEAD_OK, ' ')) && !USED_RE.test(x.name) && (isJan ? x.url.includes(it.code) || x.name.includes(it.code) || (it.model && matchesProduct(x, it.model)) : matchesProduct(x, it.code))) others.push({ shop: 'rakuten', price: x.price, ship: x.ship, name: x.name.slice(0, 50), url: x.url });
      others.sort((a, b) => a.price - b.price);
      const top = others.slice(0, 6);
      // JANで見つけたときは、Yahoo!側の商品名から型番を拾い直す（メルカリ検索用）
      if (isJan) { const nm = ys.map(digItem).map((x) => mkCode(x.name)).filter(Boolean)[0]; if (nm) it.model = nm; }
      it.brand = Object.keys(brands).sort((a, b) => brands[b] - brands[a])[0] || '';
      it.mkt = mkMedian(top.map((x) => x.price));
      it.mktN = top.length;
      it.gap = it.mkt ? Math.round((1 - it.price / it.mkt) * 100) : null;
      it.others = others.slice(0, 5);
      it.q = (it.brand ? String(it.brand).split(/[\s（(]/)[0] + ' ' : '') + (it.model || (/^\d{13}$/.test(it.code) ? mkWords(it.name) : it.code));
      it.base = it.mkt || 0; it.off = it.gap;
      it.src = 'rakuten';
      it.lv = it.mktN >= 2 && it.gap >= 20 ? 'only' : it.mktN >= 2 && it.gap < 10 ? 'flat' : it.mktN < 2 ? 'few' : 'cheap';
      out.checked++;
    } catch (e) { out.errors.push(e.message); }
  }
  out.items = cand.slice(0, 6).filter((it) => it.lv === 'only' || it.lv === 'cheap');
  out.drop.flat = cand.slice(0, 6).filter((it) => it.lv === 'flat' || it.lv === 'few').length;
  if (env.STORE) {
    const all = JSON.parse((await env.STORE.get('dig-results')) || '{}');
    for (const it of out.items) all[it.url] = Object.assign({ kw: '楽天:' + q + (shop ? '＠' + it.seller : ''), at: out.at }, it);
    const keys = Object.keys(all).sort((a, b) => String(all[b].at).localeCompare(String(all[a].at)));
    for (const k of keys.slice(100)) delete all[k];
    await env.STORE.put('dig-results', JSON.stringify(all));
    try { const lg = JSON.parse((await env.STORE.get('dead-log')) || '[]'); lg.unshift({ at: out.at, kw: out.kw, fetched: out.fetched, sale: out.sale, checked: out.checked, hit: out.items.length, drop: out.drop, errors: out.errors }); await env.STORE.put('dead-log', JSON.stringify(lg.slice(0, 50))); } catch (e) {}
  }
  out.ledger = await recordCandidateBatch(env, 'rakuten-deadstock', out.items, out.at);
  return out;
}

// メルカリ売り切れ（新品・新しい順）の検索URL：通知から人が開いて確認する用
const APP_URL = 'https://katachi.5bkj2n54ch.workers.dev/';
function mercariSoldUrl(q, kc) { return 'https://jp.mercari.com/search?keyword=' + encodeURIComponent(String(q || '').slice(0, 60)) + '&status=sold_out&item_condition_id=1&sort=created_time&order=desc' + (kc > 0 ? '&kc=' + Math.round(kc) : ''); }
function mercariQ(name, brand) { const code = mkCode(name); const w = mkWords(name); const b = String(brand || '').split(/[\s（(]/)[0]; return (code ? (b && !code.includes(b) ? b + ' ' + code : code) : w).trim(); }
// 夜の自動深掘り（v1.5.52）：23:00〜02:50に10分ごと、1キーワードずつYahoo!深掘りを回し、🔥（この店だけ安い）をDiscordへ

// 売り時ウォッチ：仕入れ済みの在庫について、毎日AmazonのいちばんAmazon安い新品価格（Keepa）を見て、メルカリの出品目安をDiscordに知らせる
const SELL_FEE = 0.9, SELL_COST = 820; // 手数料10%・送料750・梱包70
const sellFloor = (cost, min) => Math.ceil((cost + SELL_COST + (min || 500)) / SELL_FEE / 100) * 100;
const sellNet = (p, cost) => Math.round(p * SELL_FEE - SELL_COST - cost);
function mercariSaleUrl(q) { return 'https://jp.mercari.com/search?keyword=' + encodeURIComponent(String(q || '').slice(0, 60)) + '&status=on_sale&item_condition_id=1&sort=price&order=asc'; }
async function runSellWatch(env, force) {
  if (!env.KEEPA_KEY) return { error: 'KEEPA_KEYがありません' };
  const list = JSON.parse((await env.STORE.get('sell-watch')) || '[]');
  const out = [], msgs = [], Y = (n) => (n ? '¥' + Math.round(n).toLocaleString('ja-JP') : '—');
  for (const w of list) {
    if (!(w.qty > 0)) continue;
    try {
      const k = await fetchKeepa(env, w.asin);
      const comp = Math.min(...[k.amazonNow, k.newNow, k.buyBoxNow].filter((x) => x > 0));
      if (!isFinite(comp)) throw new Error('Amazonの新品価格が取れません');
      const floor = w.floor || sellFloor(w.cost);
      const sug = Math.max(floor, Math.floor(comp * 1.2 / 100) * 100);
      const net = sellNet(sug, w.cost);
      const lv = net >= 3000 ? '🎯 売り時（強気でOK）' : net >= 1000 ? '🟢 出品OK' : '🟡 待ち（最低価格で様子見）';
      const r = { asin: w.asin, at: new Date().toISOString(), comp, amazon: k.amazonNow, newLow: k.newNow, buyBox: k.buyBoxNow, sellers: k.sellersNow, sug, net, floor, lv };
      const last = w.last || {};
      if (force || !last.sug || last.lv !== lv || Math.abs(last.sug - sug) >= 500) {
        msgs.push('**' + lv + '**　' + String(w.name || w.asin).slice(0, 50) + '（在庫' + w.qty + '個・仕入' + Y(w.cost) + '）\n' +
          'Amazon最安 ' + Y(comp) + '（Amazon本体 ' + Y(k.amazonNow) + '／新品出品 ' + (k.sellersNow || '?') + '件）' + (last.comp ? '　前回 ' + Y(last.comp) : '') + '\n' +
          '→ メルカリ出品目安 **' + Y(sug) + '**（利益 ' + Y(net) + '）／これ以上下げない ' + Y(floor) + '\n' +
          '売り切れ相場 <' + mercariSoldUrl(w.q || w.name, w.cost) + '>\n販売中のライバル <' + mercariSaleUrl(w.q || w.name) + '>');
        r.sent = true;
      }
      w.last = Object.assign({}, last, r.sent ? { sug, lv, comp } : {}, { at: r.at, comp_now: comp });
      out.push(r);
    } catch (e) { out.push({ asin: w.asin, error: e.message }); }
  }
  if (msgs.length && env.DISCORD_WEBHOOK) {
    const body = '【売り時ウォッチ】\n' + msgs.join('\n\n') + (env.APP_URL ? '\n\nアプリ <' + env.APP_URL + '>' : '');
    try { await fetch(env.DISCORD_WEBHOOK, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ content: body.slice(0, 1900) }) }); } catch (e) {}
  }
  await env.STORE.put('sell-watch', JSON.stringify(list));
  return { at: new Date().toISOString(), items: out, sent: msgs.length };
}
const DIG_CRON = '*/10 14-17 * * *';
const EDION_PATROL_CRON = '0 1 * * *';
function parseEdionItems(html) {
  const links = [], prices = [], katas = [], items = [];
  let m;
  const lr = /class="item"><a href="([^"]+)">([^<]+)<\/a>/g;
  while ((m = lr.exec(html)) !== null) { const u = m[1].replace(/&amp;/g, '&'), pm = u.match(/p_cd=([^&]+)/); if (pm) links.push({ id: pm[1], url: u, name: m[2].trim(), pos: m.index }); }
  const pr = /class="price2">￥([\d,]+)</g;
  while ((m = pr.exec(html)) !== null) prices.push({ price: parseInt(m[1].replace(/,/g, ''), 10), pos: m.index });
  const kr = /class="kata">([^<]+)</g;
  while ((m = kr.exec(html)) !== null) katas.push({ model: m[1].trim(), pos: m.index });
  let pi = 0, ki = 0;
  for (const lk of links) {
    while (pi < prices.length && prices[pi].pos < lk.pos) pi++;
    if (pi >= prices.length) break;
    const pe = prices[pi++];
    while (ki < katas.length && katas[ki].pos < lk.pos) ki++;
    const ke = (ki < katas.length && katas[ki].pos < pe.pos) ? katas[ki++] : null;
    items.push({ id: lk.id, name: lk.name, model: ke ? ke.model : '', price: pe.price });
  }
  return items;
}
const EDION_CATS = [
    { name: '理美容・健康', url: 'https://www.edion.com/item_list.html?c_cd=001005&keyword=waol26&d_num=100' },
    { name: 'オーディオ・ヘッドホン', url: 'https://www.edion.com/item_list.html?c_cd=001007&keyword=waol26&d_num=100' },
    { name: 'PC周辺機器', url: 'https://www.edion.com/item_list.html?keyword=%E3%83%91%E3%82%BD%E3%82%B3%E3%83%B3%E5%91%A8%E8%BE%BA%E6%A9%9F%E5%99%A8%E3%81%8A%E8%B2%B7%E5%BE%97%E5%93%81&d_num=100' },
    { name: 'おもちゃ・ホビー', url: 'https://www.edion.com/item_list.html?keyword=toyoutlet&c_cd=001039&d_num=100' },
];
// v1.5.77: opt.pages = [{ name, html } | { name, status, error }] を渡すと、取得せずにその本文を使う（自宅PC・VPSから送られた分）
async function runEdionPatrol(env, opt = {}) {
  const CATS = EDION_CATS;
  const pushed = Array.isArray(opt.pages) ? opt.pages : null;
  const CAP = 30000;
  const seenRaw = await env.STORE.get('edion-outlet-seen');
  const seen = seenRaw ? JSON.parse(seenRaw) : {};
  const staleMs = 30 * 24 * 3600 * 1000;
  const now = Date.now();
  for (const id of Object.keys(seen)) { if (now - seen[id] > staleMs) delete seen[id]; }
  const newItems = [], errors = [], cur = [];
  for (const cat of CATS) {
    try {
      let html;
      if (pushed) {
        const p = pushed.find((x) => x && x.name === cat.name);
        if (!p) { errors.push(cat.name + ': 送られてきませんでした'); continue; }
        if (typeof p.html !== 'string' || !p.html) { errors.push(cat.name + ': ' + (p.status ? 'HTTP ' + p.status : String(p.error || '本文なし').slice(0, 80)) + '（' + String(opt.from || '外部') + '）'); continue; }
        html = p.html;
      } else {
        const r = await fetch(cat.url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language': 'ja,en-US;q=0.8,en;q=0.6', 'Referer': 'https://www.edion.com/' } });
        if (!r.ok) { errors.push(cat.name + ': HTTP ' + r.status); continue; }
        html = await r.text();
      }
      for (const it of parseEdionItems(html)) {
        if (it.price > CAP) continue; if (/ケーブル|ケース|カバー|フィルム|替刃|替え刃|交換用|アダプタ|変換|イヤーピース|ストラップ|保護|電池/.test(it.name)) continue;
        cur.push({ id: it.id, name: it.name, model: it.model, price: it.price, cat: cat.name, url: 'https://www.edion.com/detail.html?p_cd=' + it.id, first: seen[it.id] || now }); if (seen[it.id]) continue;
        newItems.push({ ...it, cat: cat.name });
      }
    } catch (e) { errors.push(cat.name + ': ' + (e.message || e)); }
  }
  if (newItems.length && env.DISCORD_WEBHOOK) {
    const lines = newItems.map(it => '【' + it.cat + '】 ' + it.name + (it.model ? ' / ' + it.model : '') + '\n¥' + it.price.toLocaleString() + ' <https://www.edion.com/detail.html?p_cd=' + it.id + '>');
    const body = '🛒 **エディオンアウトレット新着** ' + newItems.length + '件 (¥30,000以下)\n\n' + lines.join('\n\n');
    try { await fetch(env.DISCORD_WEBHOOK, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ content: body.slice(0, 1900) }) }); } catch (e) {}
  }
  for (const it of newItems) seen[it.id] = now;
  try { await env.STORE.put('edion-outlet-seen', JSON.stringify(seen)); } catch (e) {} if (cur.length) try { await env.STORE.put('edion-outlet-items', JSON.stringify({ at: new Date().toISOString(), items: cur })); } catch (e) {}
  return { newItems: newItems.length, errors, source: pushed ? String(opt.from || '外部') : 'cloudflare' };
}
function digShouldNotify(previous, item, now) {
  const at = typeof previous === 'number' ? previous : previous && previous.at;
  if (!at || now - at >= 7 * 86400000) return true;
  // 旧通知履歴は価格がないため、期限切れまで従来の抑制を維持。
  return typeof previous === 'object' && previous.price > 0 && item.price > 0 &&
    previous.ship === !!item.ship && now - at >= 24 * 3600000 &&
    previous.price - item.price >= Math.max(500, previous.price * 0.05);
}
async function runDigCron(env, ev) {
  const night = new Date(ev.scheduledTime).toISOString().slice(0, 10);
  const st = JSON.parse((await env.STORE.get('dig-cron')) || '{}');
  if (st.night !== night) { st.night = night; st.idx = 0; st.found = 0; }
  const sent = st.sent || {};
  const cut = Date.now() - 7 * 86400000; // 同じ商品の通知は7日に1回
  for (const u of Object.keys(sent)) if ((typeof sent[u] === 'number' ? sent[u] : sent[u].at) < cut) delete sent[u];
  let cfg = {}; try { const s = await loadState(env); cfg = (s && s.settings && s.settings.dig) || {}; } catch (e) {}
  const custom = Array.isArray(cfg.kw) ? [...new Set(cfg.kw.filter(x => typeof x === 'string' && x.trim()).map(x => x.trim()))] : [];
  const kws = custom.length ? custom : DIG_KW;
  if (st.idx >= kws.length) return { skipped: '今夜の分は終了', night, idx: st.idx };
  const cursor = Number.isSafeInteger(st.cursor) && st.cursor >= 0 ? st.cursor : st.idx;
  const kw = kws[cursor % kws.length];
  st.cursor = (cursor + 1) % kws.length;
  st.idx++;
  await env.STORE.put('dig-cron', JSON.stringify(Object.assign(st, { sent })));
  const r = await runDig(env, '', { kw, min: cfg.min || 4000, max: cfg.max || 15000, off: cfg.off || 35 });
  const hot = (r.items || []).filter((it) => it.lv === 'only' && it.brand && digShouldNotify(sent[it.url], it, Date.now()));
  const selected = hot.slice(0, 3);
  let notified = 0;
  if (selected.length && env.DISCORD_WEBHOOK) {
    const body = '【Yahoo!深掘り】要確認・他店より安い候補（「' + kw + '」）\n' + selected.map((it) =>
      '・［' + it.brand + '］' + it.name.slice(0, 60) + '\n　Yahoo! ¥' + it.price.toLocaleString('ja-JP') + (it.ship ? '' : '＋送料') + '（通常¥' + it.base.toLocaleString('ja-JP') + '・' + it.off + '%OFF）／他店相場 ¥' + Number(it.mkt || 0).toLocaleString('ja-JP') + '（' + it.mktN + '件）より' + it.gap + '%安い\n　Yahoo!：<' + it.url + '>\n　メルカリ売り切れ：<' + mercariSoldUrl(mercariQ(it.name, it.brand), it.price + (it.ship ? 0 : 800)) + '>').join('\n') +
      '\n→ 販売相場・利益は未確認。新品売り切れ3件以上を確認し、仕入額の25%・利益1,000円・予備費1,000円で判定：<' + (env.APP_URL || APP_URL).replace(/\/$/, '').replace(/\/unified$/, '') + '/unified>';
    try { const d = await fetch(env.DISCORD_WEBHOOK, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ content: body.slice(0, 1900) }) }); if (d.ok) { notified = selected.length; selected.forEach((it) => { sent[it.url] = { at: Date.now(), price: it.price, ship: !!it.ship }; }); } } catch (e) {}
  }
  st.found = (st.found || 0) + hot.length;
  await env.STORE.put('dig-cron', JSON.stringify(Object.assign(st, { sent, last: { at: new Date().toISOString(), kw, fetched: r.fetched, sale: r.sale, items: (r.items || []).length, hot: hot.length, notified, errors: r.errors } })));
  return { night, kw, idx: st.idx, of: kws.length, fetched: r.fetched, sale: r.sale, items: (r.items || []).length, hot: hot.length, notified, errors: r.errors };
}

// Keepa Tracking：Amazonが仕入上限以下になったらKeepaからWebhook→Discord
const TRACK_MAX = 50, TRACK_INTERVAL_H = 6, TRACK_TTL_H = 24 * 45;
async function hookToken(env) {
  const d = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(String(env.ACCESS_KEY || '') + ':keepa-hook'));
  return [...new Uint8Array(d)].slice(0, 12).map((b) => b.toString(16).padStart(2, '0')).join('');
}
async function keepaTracking(env, type, params, body) {
  const p = new URLSearchParams(Object.assign({ key: env.KEEPA_KEY, type }, params || {}));
  const res = await fetch('https://api.keepa.com/tracking?' + p.toString(), body ? { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) } : {});
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error('Keepa側のエラー（' + res.status + '）' + (data.error && data.error.message ? '：' + data.error.message : ''));
  return data;
}
async function addTrack(env, origin, b) {
  if (!env.KEEPA_KEY) return { error: 'KEEPA_KEY が未設定です' };
  const asin = String(b.asin || '').toUpperCase();
  const cap = Math.floor(Number(b.cap) || 0);
  if (!/^[A-Z0-9]{10}$/.test(asin)) return { error: 'ASINがありません' };
  if (!(cap > 0)) return { error: '仕入上限がありません（先にメルカリ相場を取ってください）' };
  const all = JSON.parse((await env.STORE.get('keepa-tracks')) || '{}');
  if (!all[asin] && Object.keys(all).filter((k) => !k.startsWith('__')).length >= TRACK_MAX) return { error: '監視は' + TRACK_MAX + '件までです。不要なものを外してください' };
  const hook = origin + '/hook/keepa?t=' + (await hookToken(env));
  if (!all.__hook || all.__hook !== hook) { await keepaTracking(env, 'webhook', { url: hook }); all.__hook = hook; }
  const obj = { asin, mainDomainId: 5, ttl: TRACK_TTL_H, expireNotify: false, desiredPricesInMainCurrency: true, updateInterval: TRACK_INTERVAL_H,
    metaData: 'katachi', individualNotificationInterval: 24 * 60, notificationType: [false, false, false, false, false, true, false],
    thresholdValues: [{ thresholdValue: cap, domain: 5, csvType: 0, isDrop: true }, { thresholdValue: cap, domain: 5, csvType: 1, isDrop: true }] };
  const r = await keepaTracking(env, 'add', {}, [obj]);
  const S = (v, n) => String(v == null ? '' : v).slice(0, n), Nn = (v) => (Number.isFinite(Number(v)) ? Math.round(Number(v)) : null);
  const prevCap = all[asin] ? all[asin].cap : null;
  all[asin] = Object.assign(all[asin] || {}, { asin, title: S(b.title, 140), url: 'https://www.amazon.co.jp/dp/' + asin, cap, priceAtAdd: Nn(b.price), med: Nn(b.med), n: Nn(b.n), q: S(b.q, 80),
    mercariAt: S(b.mercariAt, 30), minProfit: Nn(b.minProfit), minMargin: Nn(b.minMargin), addedAt: new Date().toISOString() });
  all.__stat = { at: new Date().toISOString(), tokenFlowReduction: r.tokenFlowReduction, refillRate: r.refillRate, tokensLeft: r.tokensLeft, webhook: true };
  all[asin].keepaTtl = TRACK_TTL_H; all[asin].interval = TRACK_INTERVAL_H; all[asin].expiresAt = new Date(Date.now() + TRACK_TTL_H * 3600000).toISOString();
  if (prevCap && prevCap !== cap) all[asin].prevCap = prevCap;
  await env.STORE.put('keepa-tracks', JSON.stringify(all));
  return { ok: true, asin, count: Object.keys(all).filter((k) => !k.startsWith('__')).length, tokenFlowReduction: r.tokenFlowReduction, tokensLeft: r.tokensLeft, refillRate: r.refillRate };
}
async function removeTrack(env, asin) {
  asin = String(asin || '').toUpperCase();
  const all = JSON.parse((await env.STORE.get('keepa-tracks')) || '{}');
  try { if (env.KEEPA_KEY) await keepaTracking(env, 'remove', { asin }); } catch (e) {}
  delete all[asin];
  await env.STORE.put('keepa-tracks', JSON.stringify(all));
  return { ok: true };
}
async function onKeepaHook(env, n) {
  const asin = String((n && n.asin) || '').toUpperCase();
  const all = JSON.parse((await env.STORE.get('keepa-tracks')) || '{}');
  const t = all[asin];
  const cp = Array.isArray(n && n.currentPrices) ? n.currentPrices : [];
  const prices = [cp[0], cp[1]].filter((v) => typeof v === 'number' && v > 0);
  const price = prices.length ? Math.min.apply(null, prices) : null;
  if (!t || asin.startsWith('__')) return;
  t.lastHit = { at: new Date().toISOString(), price, cause: n.trackingNotificationCause };
  t.hits = (t.hits || 0) + 1;
  await env.STORE.put('keepa-tracks', JSON.stringify(all));
  if (env.DISCORD_WEBHOOK && price && price <= t.cap) {
    const yenS = (v) => '¥' + Math.round(Number(v) || 0).toLocaleString('ja-JP');
    const days = t.mercariAt ? Math.floor((Date.now() - Date.parse(t.mercariAt)) / 86400000) : null;
    const body = '【価格監視】🔔 仕入上限に到達\n' + (t.title || asin).slice(0, 60) + '\nAmazon ' + yenS(price) + '（上限 ' + yenS(t.cap) + '）\n前回メルカリ相場 ' + yenS(t.med) + (t.n ? '（' + t.n + '件）' : '') + (days != null ? '・確認 ' + (days ? days + '日前' : '今日') : '') +
      '\nAmazon：<' + t.url + '>\nメルカリ売り切れ：<' + mercariSoldUrl(t.q || mercariQ(t.title || '', ''), price) + '>\n→ 相場が崩れていなければ、アプリ「値下がり待ち」で「メルカリ相場を再確認」してから購入：<' + (env.APP_URL || APP_URL) + '>';
    try { await fetch(env.DISCORD_WEBHOOK, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ content: body.slice(0, 1900) }) }); } catch (e) {}
  }
}

function runIssues(result, requests) {
  const issues = [];
  const add = (prefix, value) => { if (value) issues.push(prefix + String(value).slice(0, 300)); };
  const inspect = (r, prefix) => {
    if (!r || typeof r !== 'object') return;
    add(prefix, r.error);
    for (const key of ['errors', 'warnings']) if (Array.isArray(r[key])) for (const e of r[key]) add(prefix, e);
  };
  inspect(result, '');
  for (const [id, item] of Object.entries(result && result.results || {})) inspect(item, id + '：');
  inspect(result && result.sell, '売り時：');
  for (const item of result && result.sell && result.sell.items || []) inspect(item, '売り時：');
  for (const r of requests || []) if (!(r.status >= 200 && r.status < 300)) add('Yahoo!：', 'HTTP ' + r.status + (r.message ? ' ' + r.message : ''));
  return [...new Set(issues)].slice(0, 100);
}
// v1.5.76: 朝のまとめ通知
const DIGEST_SALES = [ // 画面の SALES（予測）と揃える
  { shop: 'Amazon', name: 'ブラックフライデー 先行セール', start: '2026-11-20T00:00', end: '2026-11-22T23:59' },
  { shop: 'Amazon', name: 'ブラックフライデー 本セール', start: '2026-11-23T00:00', end: '2026-11-30T23:59' },
  { shop: '楽天', name: '楽天スーパーSALE', start: '2026-12-04T20:00', end: '2026-12-11T01:59' },
];
const DIGEST_DAILY_SLOTS = [['scan-pm', '夜のスキャン'], ['pool', '候補の蓄積'], ['deals', 'Keepa Deals'], ['check', '売り時ウォッチ']]; // v1.5.78: エディオン巡回は自動を止めたので点検しない
const DIGEST_STALE_MS = 26 * 3600 * 1000;
function digestPointDays(d) {
  const out = [];
  if (d % 5 === 0 && d <= 30) out.push('楽天 5と0のつく日');
  if (d === 18) out.push('楽天 ご愛顧感謝デー');
  if (d % 10 === 5) out.push('Yahoo! 5のつく日');
  return out;
}
async function runMorningDigest(env, opt = {}) {
  const now = Number(opt.time) || Date.now();
  const jst = (ms) => new Date(ms + 32400000);
  const j = jst(now), dateKey = j.toISOString().slice(0, 10);
  const wd = ['日', '月', '火', '水', '木', '金', '土'];
  const md = (x) => (x.getUTCMonth() + 1) + '/' + x.getUTCDate();
  const hm = (iso) => { const x = jst(Date.parse(iso)); return md(x) + ' ' + String(x.getUTCHours()).padStart(2, '0') + ':' + String(x.getUTCMinutes()).padStart(2, '0'); };
  const yen = (n) => '¥' + Math.round(Number(n) || 0).toLocaleString('ja-JP');
  const read = async (k, d) => { try { return JSON.parse((await env.STORE.get(k)) || 'null') || d; } catch (e) { return d; } };
  const problems = [], lines = [], errors = [];

  // 1) 自動実行
  const runs = [];
  for (const [slot, label] of DIGEST_DAILY_SLOTS) {
    const rec = await read('cron-slot:' + slot, null);
    if (!rec) continue; // 一度も動いていない種類は対象外
    const at = rec.end || rec.start;
    const age = at ? now - Date.parse(at) : Infinity;
    if (!rec.end && age < 3600 * 1000) { runs.push(label + ' 実行中'); continue; }
    if (age > DIGEST_STALE_MS) problems.push(label + '：' + (at ? hm(at) + ' から動いていません' : '記録なし'));
    else if (rec.ok === false) problems.push(label + '：失敗（' + String(rec.err || (rec.issues || [])[0] || '詳細は /status').slice(0, 80) + '）');
    else runs.push(label + ' ✓');
  }

  // 2) ConoHa
  if (env.CONOHA_API_URL) {
    const c = new AbortController(); const timer = setTimeout(() => c.abort(), 5000);
    try {
      const r = await fetch(env.CONOHA_API_URL.replace(/\/$/, '') + '/api/health', { headers: { accept: 'application/json' }, signal: c.signal });
      const b = await r.json().catch(() => ({}));
      if (!r.ok || b.ok === false) problems.push('ConoHa VPS：応答が異常（HTTP ' + r.status + '）');
    } catch (e) { problems.push('ConoHa VPS：接続できません'); }
    finally { clearTimeout(timer); }
  }

  // 3) 入札ノート用データの鮮度
  const scan = await read('scan-results', {});
  if (!scan.at || now - Date.parse(scan.at) > DIGEST_STALE_MS) problems.push('入札ノート用データ：' + (scan.at ? hm(scan.at) + ' から更新なし' : 'まだありません'));

  // 4) 楽天アプリの期限
  const exp = String(env.RAKUTEN_APP_EXPIRES || '2027-09-21');
  const daysLeft = Math.ceil((Date.parse(exp + 'T00:00:00+09:00') - now) / 864e5);
  if (daysLeft <= 0) problems.push('楽天アプリの有効期限が切れています（' + exp + '）。管理画面で延長してください');
  else if (daysLeft <= 60) problems.push('楽天アプリの有効期限まであと' + daysLeft + '日（' + exp + '）。管理画面で延長してください');

  // 見出し
  lines.push('【katachi 朝のまとめ】' + md(j) + '(' + wd[j.getUTCDay()] + ')');
  lines.push(problems.length ? '⚠️ 要確認 ' + problems.length + '件' : '✅ 自動実行・接続ともに正常');
  for (const p of problems) lines.push('・' + p);

  // 5) ポイント日・セール
  const today = digestPointDays(j.getUTCDate());
  const tm = jst(now + 864e5), tomorrow = digestPointDays(tm.getUTCDate());
  lines.push('');
  lines.push('🛒 今日：' + (today.length ? today.join('、') + '（まとめ買いの日）' : 'ポイント日ではありません'));
  if (tomorrow.length) lines.push('　明日：' + tomorrow.join('、'));
  for (const s of DIGEST_SALES) {
    const st = Date.parse(s.start + ':00+09:00'), en = Date.parse(s.end + ':00+09:00');
    if (now >= st && now <= en) lines.push('🔥 開催中（予測）：' + s.shop + ' ' + s.name + '（〜' + hm(new Date(en).toISOString()) + '）');
    else if (st > now && st - now <= 30 * 864e5) lines.push('📅 ' + s.shop + ' ' + s.name + 'まであと' + Math.ceil((st - now) / 864e5) + '日（予測）');
  }

  // 6) 昨夜の候補
  const items = Array.isArray(scan.items) ? scan.items : [];
  const good = items.filter((x) => x && x.kv && (x.kv.level === 'ok' || x.kv.sure));
  const wr = await read('watch-results', {});
  const okWatch = Object.values(wr.results || {}).filter((x) => x && x.ok);
  lines.push('');
  lines.push('📦 昨夜の候補：' + items.length + '件（Keepa確認済み ' + good.length + '件）／ 売り時ウォッチで買える ' + okWatch.length + '件');
  for (const x of good.slice(0, 3)) lines.push('・' + String(x.name || '').slice(0, 40) + ' ' + yen(x.price) + (x.kv && x.kv.net ? ' 利益概算' + yen(x.kv.net) : '') + (x.kv && x.kv.sure ? ' ⭐' : ''));
  for (const x of okWatch.slice(0, 3)) lines.push('・' + String(x.name || x.model || '').slice(0, 40) + ' 実質' + yen(x.eff) + ' 予想利益' + yen(x.net));
  if (good.length || okWatch.length) lines.push('→ メルカリの売り切れ相場を取り込んでから判断');
  if (runs.length) { lines.push(''); lines.push('動作：' + runs.join(' ／ ')); }

  const text = lines.join('\n').slice(0, 1900);
  let sent = false;
  const last = await env.STORE.get('digest-last');
  const wantSend = opt.send !== false && (opt.force || last !== dateKey);
  if (wantSend && env.DISCORD_WEBHOOK) {
    try {
      const r = await fetch(env.DISCORD_WEBHOOK, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ content: text }) });
      if (r.ok) { sent = true; await env.STORE.put('digest-last', dateKey); } else errors.push('Discord への送信に失敗（HTTP ' + r.status + '）');
    } catch (e) { errors.push('Discord への送信に失敗'); }
  }
  return { date: dateKey, ok: problems.length === 0, problems, sent, skipped: wantSend ? false : (opt.send === false ? 'preview' : 'already-sent-today'), errors, text };
}
function isMorningSaleTime(time) {
  const j = new Date(time + 32400000), m = j.getUTCMonth() + 1, d = j.getUTCDate();
  return d % 5 === 0 || d === 18 || (m % 3 === 0 && d >= 4 && d <= 11) || (m === 7 && d >= 7 && d <= 14) || (m === 10 && d <= 20) || (m === 11 && d >= 20) || (m === 12 && d <= 11);
}

async function logRun(env, kind, ev, fn) {
  const slot = kind === 'scan' ? (ev.cron === '30 22 * * *' ? 'scan-am' : 'scan-pm') : kind;
  const rec = { kind, slot, cron: ev.cron, version: APP_VERSION, id: crypto.randomUUID(), scheduledTime: ev.scheduledTime || null, start: new Date().toISOString() };
  const scoped = { ...env, __diag: { id: rec.id, requests: [] } };
  async function put(key, value) { try { await env.STORE.put(key, JSON.stringify(value)); } catch (_) { console.error(JSON.stringify({ event:'cron-kv-error', runId:rec.id, key })); } }
  await put('cron-last-start', rec);
  await put('cron-slot-start:' + slot, rec);
  try {
    const r = await fn(scoped);
    rec.issues = runIssues(r, scoped.__diag.requests).map(x => safeYahooMessage(x, env));
    if (kind === 'dig' && r && r.skipped) { let prev = null; try { prev = JSON.parse(await env.STORE.get('cron-slot:' + slot) || 'null'); } catch (_) {} const pr = prev && prev.result && !prev.result.skipped ? prev.result : (prev && prev.result && prev.result.lastReal) || null; r.lastReal = pr; }
    rec.ok = rec.issues.length === 0;
    rec.result = r && r.keywords ? { keywords:r.keywords, minPrice:r.minPrice, stateInfo:r.stateInfo, fetchedCount:r.fetchedCount, resultCount:r.items.length, keepa:r.keepa, errors:r.errors } : r;
  } catch (e) { rec.ok = false; rec.err = safeYahooMessage(e && e.message || e, env); }
  rec.end = new Date().toISOString();
  rec.yahoo = scoped.__diag.requests;
  rec.egress = scoped.__diag.egress || null;
  rec.apiReuse = scoped.__diag.apiReuse || {};
  await put('cron-slot:' + slot, rec);
  if (rec.ok) await put('cron-slot-ok:' + slot, rec.end);
  try {
    const by = JSON.parse(await env.STORE.get('cron-last-by-kind') || '{}');
    by[kind] = rec; if (rec.ok) by[kind + ':ok'] = rec.end;
    await put('cron-last-by-kind', by);
    const old = JSON.parse(await env.STORE.get('cron-log') || '[]');
    await put('cron-log', [rec, ...old].slice(0,30));
  } catch (_) { console.error(JSON.stringify({event:'cron-kv-error',runId:rec.id,key:'legacy-log'})); }
  console.log(JSON.stringify({event:'cron-complete', ...rec}));
}
function requestAccessKey(request) {
  const auth = request.headers.get('authorization') || '';
  if (/^Bearer\s+/i.test(auth)) return auth.replace(/^Bearer\s+/i, '').trim();
  const headerKey = request.headers.get('x-katachi-key');
  if (headerKey) return headerKey.trim();
  return '';
}

const legacyWorker = {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === '/health') {
      if (!env.CONOHA_API_URL) return json({ ok: false, error: 'API URL未設定（CONOHA_API_URL）' }, 503);
      if (!env.CONOHA_API_KEY) return json({ ok: false, error: 'API認証キー未設定（CONOHA_API_KEY）' }, 503);
      const endpoint = env.CONOHA_API_URL.replace(/\/$/, '') + '/api/health';
      try {
        const upstream = await fetch(endpoint, { headers: { accept: 'application/json' } });
        const body = await upstream.text();
        let detail = {};
        try { detail = body ? JSON.parse(body) : {}; } catch (e) { detail = { response: body.slice(0, 120) }; }
        return json({ ok: upstream.ok && detail.ok !== false, worker: true, conoha: detail, status: upstream.status }, upstream.ok ? 200 : 502);
      } catch (e) {
        return json({ ok: false, worker: true, error: 'ConoHa APIへ接続できません', detail: String((e && e.message) || e) }, 502);
      }
    }

    if (url.pathname === '/hook/keepa') {
      if (!env.ACCESS_KEY || url.searchParams.get('t') !== (await hookToken(env))) return json({ error: 'forbidden' }, 403);
      if (request.method !== 'POST') return json({ ok: true });
      let n = null; try { n = await request.json(); } catch (e) {}
      try { if (env.STORE && n) await onKeepaHook(env, n); } catch (e) {}
      return json({ ok: true });
    }

    if (url.pathname.startsWith('/api/')) {
      if (!env.ACCESS_KEY) return json({ error: 'ACCESS_KEY を設定してください' }, 500);
      if (requestAccessKey(request) !== env.ACCESS_KEY) return json({ error: '合言葉が違います' }, 401);
      if (url.pathname === '/api/bidnote-discover' && request.method === 'GET') {
      if (!env.STORE) return json({ error: 'KV（STORE）が未設定です' }, 500);
      const deals = JSON.parse((await env.STORE.get('keepa-deals')) || '{"items":[]}');
      const scan = JSON.parse((await env.STORE.get('scan-results')) || '{"items":[]}');
      return json({
        ok: true,
        at: scan.at || deals.at || new Date().toISOString(),
        deals: (deals.items || []).slice(0, 30),
        scan: (scan.items || []).slice(0, 30)
      });
    }

    try {
        const q = (url.searchParams.get('q') || '').trim().slice(0, 100);


        if (url.pathname === '/api/campaigns') {
          if (!env.STORE) return json({ error: 'KV（STORE）が未設定です' }, 500);
          const current = JSON.parse((await env.STORE.get('campaigns-v1')) || '{"revision":"","campaigns":[]}');
          if (request.method === 'GET') return json(current);
          if (request.method !== 'PUT') return json({ error: 'method not allowed' }, 405);
          const body = await request.text();
          if (body.length > 60000) return json({ error: '条件は60KB以内にしてください' }, 400);
          let data; try { data = JSON.parse(body); } catch(e) { return json({ error: 'JSON形式を確認してください' }, 400); }
          if (typeof data.revision !== 'string' || data.revision !== current.revision) return json({ error: '別の端末で条件が更新されています。保存済み条件を読み直してください' }, 409);
          const rows = data.campaigns;
          if (!Array.isArray(rows) || rows.length > 30) return json({ error: '条件は30件以内のJSON配列にしてください' }, 400);
          const ids = new Set();
          for (const c of rows) {
            if (!c || typeof c !== 'object' || Array.isArray(c) || !/^[a-zA-Z0-9_-]{1,80}$/.test(c.id || '') || ids.has(c.id) || typeof c.title !== 'string' || !c.title.trim() || c.title.length > 150 || !['rakuten','yahoo'].includes(c.shop)) return json({ error: 'ID・タイトル・仕入れ先を確認してください' }, 400);
            ids.add(c.id);
            if (c.verified !== true || !/^https:\/\//.test(c.sourceUrl || '') || !/(?:Z|[+-]\d\d:\d\d)$/.test(c.start || '') || !/(?:Z|[+-]\d\d:\d\d)$/.test(c.end || '') || !Number.isFinite(Date.parse(c.start)) || !Number.isFinite(Date.parse(c.end)) || Date.parse(c.end) <= Date.parse(c.start)) return json({ error: '公式URL・確認済みフラグ・対象日時を確認してください' }, 400);
            if (typeof c.conditions !== 'string' || !c.conditions.trim() || c.conditions.length > 2000) return json({ error: '適用条件と併用可否を記入してください' }, 400);
            for (const k of ['minPurchase','discountYen','discountRate','discountCap','pointYen','pointRate','pointCap','remainingPointCap','includedPointYen']) if (c[k] != null && (typeof c[k] !== 'number' || !Number.isFinite(c[k]) || c[k] < 0)) return json({ error: '金額・率を確認してください' }, 400);
            if ((c.discountRate || 0) > 100 || (c.pointRate || 0) > 100 || ((c.discountYen || 0) && (c.discountRate || 0)) || ((c.pointYen || 0) && (c.pointRate || 0)) || typeof c.includedPointYen !== 'number' || c.stackableWithShopPoints !== true || ((c.pointRate || 0) && (typeof c.pointBaseRatio !== 'number' || c.pointBaseRatio <= 0 || c.pointBaseRatio > 1))) return json({ error: '率・ポイント計算額・既計上額・併用確認を見直してください' }, 400);
          }
          const out = { revision: crypto.randomUUID(), updatedAt: new Date().toISOString(), campaigns: rows };
          await env.STORE.put('campaigns-v1', JSON.stringify(out));
          return json(out);
        }

        if (url.pathname === '/api/rakuten') {
          if (!q) return json({ error: '検索語がありません' }, 400);
          const opts = { min: Number(url.searchParams.get('min')) || 0, acc: url.searchParams.get('acc') === '1', model: true, condition: url.searchParams.get('condition') === 'used' ? 'used' : 'new' };
          return json({ items: cleanItems(await searchRakuten(env, q, url.origin, opts), q, opts) });
        }
        if (url.pathname === '/api/yahoo') {
          if (!q) return json({ error: '検索語がありません' }, 400);
          const opts = { min: Number(url.searchParams.get('min')) || 0, acc: url.searchParams.get('acc') === '1', model: true, condition: url.searchParams.get('condition') === 'used' ? 'used' : 'new' };
          return json({ items: cleanItems(await searchYahoo(env, q, opts), q, opts) });
        }

        if (url.pathname === '/api/unopened') {
          if (!q) return json({ error: '検索語がありません' }, 400);
          return json(await searchUnopened(env, q, url.origin));
        }

        if (!env.STORE && url.pathname !== '/api/data' && url.pathname !== '/api/items') return json({ error: 'KV（STORE）が未設定です' }, 500);

        if (url.pathname === '/api/track') {
          if (request.method === 'GET') {
            const all = JSON.parse((await env.STORE.get('keepa-tracks')) || '{}');
            const stat = Object.assign({ webhook: !!all.__hook }, all.__stat || {});
            delete all.__hook; delete all.__stat;
            if (url.searchParams.get('live') === '1' && env.KEEPA_KEY) {
              try { const t = await reusableApiFetch(env,'https://api.keepa.com/token?key=' + encodeURIComponent(env.KEEPA_KEY)).then((x) => x.json()); stat.live = { at: new Date().toISOString(), tokenFlowReduction: t.tokenFlowReduction, refillRate: t.refillRate, tokensLeft: t.tokensLeft }; } catch (e) { stat.liveError = e.message; }
            }
            return json({ tracks: all, max: TRACK_MAX, interval: TRACK_INTERVAL_H, ttlDays: TRACK_TTL_H / 24, stat });
          }
          if (request.method === 'DELETE') return json(await removeTrack(env, url.searchParams.get('asin')));
          if (request.method !== 'POST') return json({ error: 'method not allowed' }, 405);
          let b; try { b = await request.json(); } catch (e) { return json({ error: 'JSONが読めません' }, 400); }
          const r = await addTrack(env, url.origin, b);
          return json(r, r.error ? 400 : 200);
        }
        if (url.pathname === '/api/mcache') {
          const all = JSON.parse((await env.STORE.get('mercari-cache')) || '{}');
          if (request.method === 'GET') return json({ cache: all });
          if (request.method !== 'POST') return json({ error: 'method not allowed' }, 405);
          let b; try { b = await request.json(); } catch (e) { return json({ error: 'JSONが読めません' }, 400); }
          const S = (v, n) => String(v == null ? '' : v).slice(0, n);
          const Nn = (v) => (Number.isFinite(Number(v)) ? Math.round(Number(v)) : null);
          for (const r of (Array.isArray(b.rows) ? b.rows : []).slice(0, 20)) {
            if (!r || !r.key) continue;
            all[S(r.key, 300)] = { at: S(r.at || new Date().toISOString(), 30), q: S(r.q, 80), mercariUrl: S(r.mercariUrl, 400), lv: ['ok', 'okb', 'hold', 'ng', 'err'].includes(r.lv) ? r.lv : 'hold', why: S(r.why, 120),
              med: Nn(r.med), n: Nn(r.n), net: Nn(r.net), margin: Nn(r.margin), cap: Nn(r.cap), cost: Nn(r.cost),
              profitOk: r.profitOk == null ? null : !!r.profitOk, trust: ['high', 'mid', 'low'].includes(r.trust) ? r.trust : null, trustWhy: S(r.trustWhy, 80),
              shops: Nn(r.shops), personal: Nn(r.personal), sellers: Nn(r.sellers),
              capMed: Nn(r.capMed), safePrice: Nn(r.safePrice), safeBasis: S(r.safeBasis, 30), safeNet: Nn(r.safeNet), safeMargin: Nn(r.safeMargin),
              kept: (Array.isArray(r.kept) ? r.kept : []).slice(0, 8).map((c) => ({ price: Nn(c.price), name: S(c.name, 40), url: S(c.url, 200) })) };
          }
          const keys = Object.keys(all).sort((a, b) => String(all[b].at).localeCompare(String(all[a].at)));
          for (const k of keys.slice(300)) delete all[k];
          await env.STORE.put('mercari-cache', JSON.stringify(all));
          return json({ ok: true, count: Math.min(keys.length, 300) });
        }
        if (url.pathname === '/api/sell') {
          let list = JSON.parse((await env.STORE.get('sell-watch')) || '[]');
          if (request.method === 'POST' && url.searchParams.get('run') === '1') return json(await runSellWatch(env, true));
          if (request.method === 'POST') {
            const b = await request.json().catch(() => ({}));
            const asin = String(b.asin || '').toUpperCase();
            if (!/^[A-Z0-9]{10}$/.test(asin) || !(Number(b.cost) > 0)) return json({ error: 'ASINと仕入値を入れてください' }, 400);
            const old = list.find((x) => x.asin === asin) || {};
            list = list.filter((x) => x.asin !== asin);
            list.push(Object.assign(old, { asin, name: String(b.name || old.name || '').slice(0, 120), q: String(b.q || old.q || '').slice(0, 60), cost: Math.round(Number(b.cost)), qty: Number(b.qty) >= 0 ? Number(b.qty) : (old.qty || 1), floor: Number(b.floor) > 0 ? Math.round(Number(b.floor)) : (old.floor || 0), added: old.added || new Date().toISOString() }));
            await env.STORE.put('sell-watch', JSON.stringify(list));
            return json({ ok: true, list });
          }
          if (request.method === 'DELETE') { list = list.filter((x) => x.asin !== (url.searchParams.get('asin') || '').toUpperCase()); await env.STORE.put('sell-watch', JSON.stringify(list)); return json({ ok: true, list }); }
          return json({ list });
        }
        if (url.pathname === '/api/dead') {
          if (url.searchParams.get('find')) { const r = await rakutenRaw(env, url.origin, { keyword: url.searchParams.get('find') }).catch((e) => ({ error: e.message })); if (r.error) return json(r); const c = {}; for (const x of r) { const k = x.sellerId + '｜' + x.seller; c[k] = (c[k] || 0) + 1; } return json({ shops: c }); }
          if (request.method === 'POST') return json(await runDead(env, url.origin, { kw: url.searchParams.get('kw'), cat: url.searchParams.get('cat'), shop: url.searchParams.get('shop'), min: url.searchParams.get('min'), max: url.searchParams.get('max') }));
          return json({ kw: DEAD_KW, cat: DEAD_CAT, log: JSON.parse((await env.STORE.get('dead-log')) || '[]') });
        }
        if (url.pathname === '/api/dig') {
          if (request.method === 'POST') return json(await runDig(env, url.origin, { kw: url.searchParams.get('kw'), min: url.searchParams.get('min'), max: url.searchParams.get('max'), off: url.searchParams.get('off') }));
          if (request.method === 'DELETE') { const all = JSON.parse((await env.STORE.get('dig-results')) || '{}'); delete all[url.searchParams.get('url') || '']; await env.STORE.put('dig-results', JSON.stringify(all)); return json({ ok: true }); }
          if (url.searchParams.get('cron') === '1') return json({ cron: DIG_CRON, state: JSON.parse((await env.STORE.get('dig-cron')) || '{}') });
          return json({ results: JSON.parse((await env.STORE.get('dig-results')) || '{}'), keywords: DIG_KW });
        }
        if (url.pathname === '/api/market') {
          const sp = url.searchParams;
          if (!sp.get('asin') && !sp.get('title') && !q) return json({ cache: JSON.parse((await env.STORE.get('market-cache')) || '{}') });
          const asin = (sp.get('asin') || '').toUpperCase();
          if (asin && !/^[A-Z0-9]{10}$/.test(asin)) return json({ error: 'ASINが正しくありません' }, 400);
          return json(await marketCheck(env, url.origin, { asin, title: (sp.get('title') || '').slice(0, 200), q, amazon: sp.get('amazon'), avg90: sp.get('avg90'), refresh: sp.get('refresh') === '1', keepa: sp.get('keepa') === '1' }));
        }
        if (url.pathname === '/api/edion') return json(JSON.parse((await env.STORE.get('edion-outlet-items')) || '{"items":[]}')); if (url.pathname === '/api/deals') {
          if (request.method === 'POST') return json(await runDeals(env));
          return json(JSON.parse((await env.STORE.get('keepa-deals')) || '{"items":[]}'));
        }
        if (url.pathname === '/api/souba') {
          const all = JSON.parse((await env.STORE.get('souba-runs')) || '[]');
          if (request.method === 'GET') return json({ runs: all });
          if (request.method === 'DELETE') {
            const id = url.searchParams.get('id') || '';
            const rest = all.filter((r) => r.id !== id);
            await env.STORE.put('souba-runs', JSON.stringify(rest));
            return json({ ok: true, runs: rest });
          }
          if (request.method !== 'POST') return json({ error: 'method not allowed' }, 405);
          let b; try { b = await request.json(); } catch (e) { return json({ error: 'JSONが読めません' }, 400); }
          const S = (v, n) => String(v == null ? '' : v).slice(0, n);
          const Nn = (v) => (Number.isFinite(Number(v)) ? Math.round(Number(v)) : null);
          const rows = (Array.isArray(b.rows) ? b.rows : []).slice(0, 200).map((r) => ({
            name: S(r.name, 140), url: S(r.url, 300), cost: Nn(r.cost), q: S(r.q, 80), mercariUrl: S(r.mercariUrl, 400),
            lv: ['ok', 'hold', 'ng', 'err'].includes(r.lv) ? r.lv : 'hold', why: S(r.why, 120), soldOut: !!r.soldOut,
            med: Nn(r.med), n: Nn(r.n), net: Nn(r.net), margin: Nn(r.margin), cap: Nn(r.cap),
            kept: (Array.isArray(r.kept) ? r.kept : []).slice(0, 8).map((c) => ({ price: Nn(c.price), name: S(c.name, 80), url: S(c.url, 200) })),
          }));
          if (!rows.length) return json({ error: '結果がありません' }, 400);
          const run = { id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6), at: new Date().toISOString(), title: S(b.title, 120), source: S(b.source, 300), settings: b.settings && typeof b.settings === 'object' ? b.settings : {}, rows };
          const next = [run].concat(all).slice(0, 10);
          await env.STORE.put('souba-runs', JSON.stringify(next));
          const oks = rows.filter((r) => r.lv === 'ok');
          let notified = false;
          if (b.auto && oks.length && env.DISCORD_WEBHOOK) {
            const yenS = (v) => '¥' + Math.round(Number(v) || 0).toLocaleString('ja-JP');
            const body = '【相場チェック・自動】🟢 ' + oks.length + '件（' + (run.title || '').replace(/^\[自動\]\s*/, '').slice(0, 40) + '）\n' +
              oks.slice(0, 8).map((r) => '・' + r.name.slice(0, 40) + ' 仕入' + yenS(r.cost) + ' → 相場' + yenS(r.med) + '（' + r.n + '件） 利益' + yenS(r.net) + '\n  ' + r.url).join('\n') +
              '\n※成約日・状態・同じ商品かはメルカリで確認してから';
            try { const d = await fetch(env.DISCORD_WEBHOOK, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ content: body.slice(0, 1900) }) }); notified = d.ok; } catch (e) {}
          }
          return json({ ok: true, id: run.id, count: rows.length, notified });
        }

        if (url.pathname === '/api/data' || url.pathname === '/api/items') {
          if (!env.CONOHA_API_URL || !env.CONOHA_API_KEY) {
            return json({ error: !env.CONOHA_API_URL ? 'API URL未設定（CONOHA_API_URL）' : 'API認証キー未設定（CONOHA_API_KEY）' }, 500);
          }
          if (url.pathname === '/api/items' && request.method !== 'GET') return json({ error: 'method not allowed' }, 405);
          const endpoint = env.CONOHA_API_URL.replace(/\/$/, '') + url.pathname;
          const headers = { authorization: 'Bearer ' + env.CONOHA_API_KEY };
          const init = { method: request.method, headers };
          if (url.pathname === '/api/data' && request.method === 'PUT') {
            headers['content-type'] = 'application/json';
            init.body = await request.text();
          }
          let upstream;
          try { upstream = await fetch(endpoint, init); }
          catch (e) { return json({ error: 'ConoHa APIへ接続できません', detail: String((e && e.message) || e) }, 502); }
          const upstreamText = await upstream.text();
          if (upstream.ok && url.pathname === '/api/data') {
            try {
              const data = JSON.parse(upstreamText);
              if (request.method === 'GET' && validState(data)) await cacheState(env, data);
              // Read the committed server state, including conflict resolution and timestamps.
              else if (request.method === 'PUT' && (data.ok || data.conflict)) await loadState(env);
            } catch (e) { /* Primary save remains authoritative; next scan retries backup. */ }
          }
          return new Response(upstreamText, {
            status: upstream.status,
            headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
          });
        }
        if (url.pathname === '/api/backup') {
          if (request.method !== 'GET') return json({ error: 'method not allowed' }, 405);
          if (!env.STORE) return json({ error: 'KV（STORE）が未設定です' }, 500);
          // 読み取り専用：KVの全キーを読むだけで、書き込み・削除・Cron記録の更新はしない
          const names = []; let cursor;
          do { const r = await env.STORE.list(cursor ? { cursor } : {}); for (const x of r.keys) names.push(x.name); cursor = r.list_complete ? undefined : r.cursor; } while (cursor);
          const data = {}, text = [], failed = [];
          for (const name of names) {
            try { const v = await env.STORE.get(name); if (v == null) continue; try { data[name] = JSON.parse(v); } catch (e) { data[name] = v; text.push(name); } }
            catch (e) { failed.push(name); }
          }
          const day = new Date(Date.now() + 32400000).toISOString().slice(0, 10);
          return new Response(JSON.stringify({ app: 'katachi', version: APP_VERSION, createdAt: new Date().toISOString(), readOnly: true, keyCount: Object.keys(data).length, textKeys: text, failedKeys: failed, note: 'KVの内容の控えです。APIキー等のSecretは含まれません。復元機能はありません。', data }), { headers: { 'content-type': 'application/json; charset=utf-8', 'content-disposition': 'attachment; filename="katachi-backup-' + day + '.json"', 'cache-control': 'no-store' } });
        }
        if (url.pathname === '/api/rise') {
          if (request.method === 'POST') return json(await runRise(env, { min: Number(url.searchParams.get('min')) || undefined, max: Number(url.searchParams.get('max')) || undefined, rise: Number(url.searchParams.get('rise')) || undefined, rankMax: Number(url.searchParams.get('rank')) || undefined, dateRange: url.searchParams.get('dr') != null ? Number(url.searchParams.get('dr')) : undefined, sel: await request.json().catch(() => null) }));
          return json(JSON.parse((await env.STORE.get('keepa-rise')) || '{"items":[]}'));
        }
        if (url.pathname === '/api/edion-cats') return json({ ok: true, cats: EDION_CATS, userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36' });
        if (url.pathname === '/api/edion-ingest') {
          if (request.method !== 'POST') return json({ error: 'method not allowed' }, 405);
          let b; try { b = await request.json(); } catch (e) { return json({ error: 'JSONが読めません' }, 400); }
          const pages = (Array.isArray(b && b.pages) ? b.pages : []).slice(0, EDION_CATS.length).map((p) => ({ name: String((p && p.name) || '').slice(0, 40), html: typeof (p && p.html) === 'string' ? p.html.slice(0, 3000000) : '', status: Number(p && p.status) || 0, error: String((p && p.error) || '').slice(0, 120) }));
          if (!pages.length) return json({ error: 'pages がありません' }, 400);
          const from = String((b && b.from) || '外部').slice(0, 20);
          let out = null;
          await logRun(env, 'edionPatrol', { cron: 'push:' + from, scheduledTime: Date.now() }, async (e2) => { out = await runEdionPatrol(e2, { pages, from }); return out; });
          return json({ ok: true, result: out });
        }
        if (url.pathname === '/api/digest') return json(await runMorningDigest(env, { send: url.searchParams.get('send') === '1', force: url.searchParams.get('send') === '1' }));
        if (url.pathname === '/api/status') { const lg = JSON.parse((await env.STORE.get('cron-log')) || '[]'); const by = JSON.parse((await env.STORE.get('cron-last-by-kind')) || '{}');
          for (const slot of ['scan-am','scan-pm','pool','deals','check','dig','edionPatrol','digest']) {
            const last = JSON.parse(await env.STORE.get('cron-slot:' + slot) || 'null');
            const start = JSON.parse(await env.STORE.get('cron-slot-start:' + slot) || 'null');
            if (last) by[slot] = last;
            if (start && (!last || Date.parse(start.start) > Date.parse(last.start))) by[slot] = start;
            const ok = JSON.parse(await env.STORE.get('cron-slot-ok:' + slot) || 'null');
            if (ok) by[slot + ':ok'] = ok;
          }
          // 種類別の記録がまだない間は、直近30件の実行記録から補う
          for (const r of lg) { if (!by[r.kind]) by[r.kind] = r; if (r.ok && r.end && !by[r.kind + ':ok']) by[r.kind + ':ok'] = r.end; }
          return json({ version: APP_VERSION, now: new Date().toISOString(), by, log: lg.slice(0, 12), dig: JSON.parse((await env.STORE.get('dig-cron')) || '{}').last || null, dead: JSON.parse((await env.STORE.get('dead-log')) || '[]').slice(0, 5), sell: JSON.parse((await env.STORE.get('sell-watch')) || '[]').map((x) => ({ name: x.name, qty: x.qty, last: x.last })) }); }
        if (url.pathname === '/api/cron-log') return json(JSON.parse((await env.STORE.get('cron-log')) || '[]'));
        if (url.pathname === '/api/watch') {
          const wr = JSON.parse((await env.STORE.get('watch-results')) || '{}');
          wr.keepa = JSON.parse((await env.STORE.get('keepa-results')) || '{}');
          return json(wr);
        }
        if (url.pathname === '/api/keepa' && request.method === 'POST') {
          if (!env.KEEPA_KEY) return json({ error: 'KEEPA_KEY が未設定です' }, 500);
          const id = url.searchParams.get('id') || '';
          const st = (await loadState(env));
          const wi = (st.watch || []).find((x) => x.id === id);
          const wr = JSON.parse((await env.STORE.get('watch-results')) || '{}');
          const r = wr.results && wr.results[id];
          if (!wi) return json({ error: '監視リストに商品がありません' }, 400);
          const code = String(wi.code || (r && r.jan) || '').trim();
          if (!code) return json({ error: 'JANまたはASINが未入力です（監視の追加欄に入れてください）' }, 400);
          const k = await fetchKeepa(env, code);
          const out = Object.assign({ at: new Date().toISOString() }, keepaJudge(k, Number(wi.market) || 0));
          const all = JSON.parse((await env.STORE.get('keepa-results')) || '{}');
          all[id] = out;
          await env.STORE.put('keepa-results', JSON.stringify(all));
          return json(out);
        }
        if (url.pathname === '/api/pool-check') {
          if (request.method === 'POST') return json(await runPoolCheck(env, url.origin));
          return json(JSON.parse((await env.STORE.get('pool-results')) || '{}'));
        }
        if (url.pathname === '/api/scan') {
          if (request.method === 'POST') return json(await runScan(env, url.origin));
          return json(JSON.parse((await env.STORE.get('scan-results')) || '{}'));
        }
        if (url.pathname === '/api/check' && request.method === 'POST') {
          return json(await runCheck(env, url.origin));
        }
        return json({ error: 'not found' }, 404);
      } catch (e) {
        return json({ error: String((e && e.message) || e) }, 502);
      }
    }

    if (url.pathname === '/manifest.webmanifest') {
      return new Response(JSON.stringify(MANIFEST), {
        headers: { 'content-type': 'application/manifest+json; charset=utf-8', 'cache-control': 'public, max-age=3600' },
      });
    }
    if (url.pathname === '/status') {
      return new Response(STATUS_HTML, { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' } });
    }
    if (url.pathname === '/version') {
      return new Response(APP_VERSION, { headers: { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' } });
    }

    return new Response(HTML, {
      headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' },
    });
  },

  async scheduled(event, env, ctx) {
    if (!env.STORE) return;
    if (event.cron === '30 22 * * *') ctx.waitUntil(logRun(env, 'digest', event, (env) => runMorningDigest(env, { time: event.scheduledTime })));
    if (event.cron === DIG_CRON) { ctx.waitUntil(logRun(env, 'dig', event, (env) => runDigCron(env, event))); return; }
    if (event.cron === EDION_PATROL_CRON) { ctx.waitUntil(logRun(env, 'edionPatrol', event, (env) => runEdionPatrol(env))); return; }
    if (event.cron === '30 11 * * *' || (event.cron === '30 22 * * *' && isMorningSaleTime(event.scheduledTime))) ctx.waitUntil(logRun(env, 'scan', event, async (env) => { const result = await runScan(env, ''); await logRun(env, 'pool', event, (env) => runPoolCheck(env, '')); if (event.cron === '30 11 * * *') await logRun(env, 'deals', event, (env) => runDeals(env)); return result; }));
    else if (event.cron === '30 22 * * *') return; // 朝はセール日だけ動かす
    else ctx.waitUntil(logRun(env, 'check', event, async (env) => { const r = await runCheck(env, ''); let sw = null; try { sw = await runSellWatch(env, false); } catch (e) { sw = { error: safeYahooMessage(e && e.message || e, env) }; } if (r && typeof r === 'object') r.sell = sw; return r; }));
  },
};

const unifiedHTML=__ASSET_unifiedHTML__;
export default {
  ...legacyWorker,
  async fetch(request, env, ctx) {
    const url=new URL(request.url);
    if(url.pathname==='/unified-manifest.webmanifest') {
      if(request.method!=='GET' && request.method!=='HEAD')return new Response('Method not allowed',{status:405,headers:{'allow':'GET, HEAD'}});
      const manifest={id:'/unified',name:'型落ち・入札 統合版',short_name:'統合版',start_url:'/unified',scope:'/',display:'standalone',lang:'ja',background_color:'#EDF0F2',theme_color:'#0F5C4D'};
      return new Response(request.method==='HEAD'?null:JSON.stringify(manifest),{headers:{'content-type':'application/manifest+json; charset=utf-8','cache-control':'no-store','x-content-type-options':'nosniff'}});
    }
    if(url.pathname==='/unified-api/data' || url.pathname==='/unified-api/health') {
      const methods=url.pathname.endsWith('/health')?['GET']:['GET','PUT'];
      if(!methods.includes(request.method))return new Response('Method not allowed',{status:405});
      const auth=request.headers.get('authorization');
      if(!auth || !auth.startsWith('Bearer '))return Response.json({error:'合言葉が必要です'},{status:401});
      const origin=request.headers.get('origin');
      if(origin && origin!==url.origin)return Response.json({error:'Origin not allowed'},{status:403});
      try {
        const headers={'authorization':auth,'accept':'application/json'};
        if(request.method==='PUT')headers['content-type']='application/json';
        const upstream=await fetch('https://research.tcg-watch.com/bidnote-api/api/'+(url.pathname.endsWith('/health')?'health':'unified-data'),{
          method:request.method,headers,body:request.method==='PUT'?await request.text():undefined,
          redirect:'manual'
        });
        return new Response(upstream.body,{status:upstream.status,headers:{'content-type':upstream.headers.get('content-type')||'application/json','cache-control':'no-store'}});
      }catch(error){return Response.json({error:'同期サーバーに接続できません：'+String(error&&error.message?error.message:error)},{status:502});}
    }
    if(url.pathname==='/unified' || url.pathname==='/unified/') {
      if(request.method!=='GET' && request.method!=='HEAD')return new Response('Method not allowed',{status:405});
      return new Response(request.method==='HEAD'?null:unifiedHTML,{headers:{'content-type':'text/html; charset=utf-8','cache-control':'no-store','x-content-type-options':'nosniff'}});
    }
    return legacyWorker.fetch(request,env,ctx);
  }
};
