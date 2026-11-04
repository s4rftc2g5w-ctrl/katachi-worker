const CANDIDATE_MIN_CASH_PROFIT = 1000;
const CANDIDATE_MIN_CASH_MARGIN = 25;

function candidateNumber(value) {
  if (value == null || typeof value === 'boolean' || (typeof value === 'string' && !value.trim())) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function candidateIdentity(item) {
  const jan = String(item.jan || item.janCode || '').replace(/\D/g, '');
  if (/^\d{13}$/.test(jan)) return 'jan:' + jan;
  const asin = String(item.asin || '').trim().toUpperCase();
  if (/^[A-Z0-9]{10}$/.test(asin)) return 'asin:' + asin;
  const model = String(item.model || item.code || '').trim().toUpperCase().replace(/\s+/g, '');
  if (model.length >= 4) return 'model:' + model.slice(0, 80);
  const url = String(item.url || '').trim().replace(/[?#].*$/, '');
  if (url) return 'url:' + url.slice(0, 300);
  const name = String(item.name || item.title || '').trim().toLowerCase().replace(/\s+/g, '');
  return name ? 'name:' + name.slice(0, 80) : '';
}

function candidateDecision(item) {
  const price = candidateNumber(item.price != null ? item.price : item.eff);
  const sell = candidateNumber(item.sell != null ? item.sell : item.market);
  const cashProfit = candidateNumber(item.cashProfit != null ? item.cashProfit : item.net);
  const cashMargin = price > 0 && cashProfit != null ? cashProfit / price * 100 : null;
  const mercariCount = Math.max(0, Number(item.mercariCount || item.soldCount || 0) || 0);
  const humanConfirmed = item.humanConfirmed === true;
  const reasons = [];
  if (cashProfit == null) reasons.push('現金利益を計算するための情報が不足');
  else if (cashProfit < CANDIDATE_MIN_CASH_PROFIT) reasons.push('現金利益が1,000円未満');
  if (cashMargin == null) reasons.push('仕入額に対する利益率を計算できない');
  else if (cashMargin < CANDIDATE_MIN_CASH_MARGIN) reasons.push('仕入額に対する利益率が25%未満');
  if (mercariCount < 3) reasons.push('メルカリ同一商品の売り切れ3件が未確認');
  else if (!humanConfirmed) reasons.push('メルカリ同一商品3件の本人確認が未完了');
  const numericReady = cashProfit != null && cashMargin != null && cashProfit >= CANDIDATE_MIN_CASH_PROFIT && cashMargin >= CANDIDATE_MIN_CASH_MARGIN;
  const contribution = price != null && cashProfit != null ? price + cashProfit : null;
  const reconsiderPrice = contribution == null ? null : Math.max(0, Math.floor(Math.min(contribution - CANDIDATE_MIN_CASH_PROFIT, contribution / 1.25)));
  return {
    price, sell, cashProfit, cashMargin, mercariCount, humanConfirmed, numericReady,
    buyReady: numericReady && mercariCount >= 3 && humanConfirmed,
    status: numericReady ? 'mercari-check' : 'watch',
    reasons: [...new Set(reasons)], reconsiderPrice
  };
}

function applyCandidateObservation(ledger, source, raw, observedAt) {
  const id = candidateIdentity(raw);
  if (!id) return null;
  const previous = ledger.candidates[id] || null;
  if (previous && Date.parse(previous.lastSeenAt) > Date.parse(observedAt)) return previous;
  const decision = candidateDecision(raw);
  const last = previous && previous.history.length ? previous.history[previous.history.length - 1] : null;
  const observation = {
    at: observedAt, source, price: decision.price, sell: decision.sell,
    cashProfit: decision.cashProfit, cashMargin: decision.cashMargin,
    mercariCount: decision.mercariCount, humanConfirmed: decision.humanConfirmed,
    numericReady: decision.numericReady, buyReady: decision.buyReady,
    status: decision.status, reasons: decision.reasons,
    reconsiderPrice: decision.reconsiderPrice,
    previousPrice: last && last.price != null ? last.price : null,
    priceChange: last && last.price != null && decision.price != null ? decision.price - last.price : null,
    url: String(raw.url || '').slice(0, 500)
  };
  const history = previous ? previous.history.slice() : [];
  history.push(observation);
  ledger.candidates[id] = {
    id,
    name: String(raw.name || raw.title || (previous && previous.name) || '').slice(0, 160),
    jan: String(raw.jan || (previous && previous.jan) || ''),
    asin: String(raw.asin || (previous && previous.asin) || ''),
    model: String(raw.model || raw.code || (previous && previous.model) || '').slice(0, 100),
    firstSeenAt: previous ? previous.firstSeenAt : observedAt,
    lastSeenAt: observedAt,
    latest: observation,
    history
  };
  ledger.updatedAt = observedAt;
  return ledger.candidates[id];
}

function candidateTop(ledger, limit = 3) {
  return Object.values((ledger && ledger.candidates) || {})
    .filter(candidate => candidate.latest && candidate.latest.numericReady)
    .sort((a, b) => Number(b.latest.cashProfit) - Number(a.latest.cashProfit) || Number(b.latest.cashMargin) - Number(a.latest.cashMargin))
    .slice(0, Math.max(1, Math.min(3, Number(limit) || 3)));
}
