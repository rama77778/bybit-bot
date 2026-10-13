// bybit-scalper.js v7.1
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = 3000;
const BASE_URL = 'https://api-demo.bybit.com';
const DB_FILE = path.join(__dirname, 'trades.json');
const BATCH_SIZE = 100;

let instrumentsCache = {};
let db = { trades: [], dailyPnl: {} };
try { db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch {}
const saveDB = () => fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
const today = () => new Date().toISOString().slice(0, 10);

const state = {
  running: false, apiKey: '', apiSecret: '',
  config: {
    min24hVolumeUSDT: 20000000, maxSpreadPct: 0.15,
    topN: 15, perTradeRiskPct: 1, dailyRiskPct: 5,
    maxLeverage: 5, stopLossPct: 0.4, takeProfitPct: 0.7,
    maxConcurrentPositions: 3,
    breakevenTriggerPct: 0.5, breakevenOffsetPct: 0.05,
    trailingEnabled: true, trailingDistancePct: 0.3,
    maxPositionMinutes: 15,
    cooldownMinutes: 10,
    blacklistThresholdUSDT: 5, blacklistPeriodHours: 24, blacklistLookback: 20,
    maxTradesPerDay: 60,
    exclude: ['BTCUSDT', 'ETHUSDT'],
  },
  balance: 0,
  balanceDetail: { wallet: 0, available: 0, upnl: 0, equity: 0 },
  positions: {}, engines: {}, scanResult: [],
  cooldowns: {}, blacklist: {},
  lastScan: 0, startedAt: null, lastTick: 0,
  log: [], loop: null,
};

function log(msg) {
  const line = '[' + new Date().toLocaleTimeString() + '] ' + msg;
  state.log.push(line);
  if (state.log.length > 400) state.log.shift();
  console.log(line);
}

function sign(ts, key, recv, payload, secret) {
  return crypto.createHmac('sha256', secret).update(ts + key + recv + payload).digest('hex');
}

async function signedRequest(method, endpoint, params) {
  const ts = Date.now().toString(), recv = '5000';
  const headers = {
    'X-BAPI-API-KEY': state.apiKey,
    'X-BAPI-TIMESTAMP': ts,
    'X-BAPI-RECV-WINDOW': recv,
  };
  let url = BASE_URL + endpoint, body;
  if (method === 'GET') {
    const qs = new URLSearchParams(params || {}).toString();
    headers['X-BAPI-SIGN'] = sign(ts, state.apiKey, recv, qs, state.apiSecret);
    if (qs) url += '?' + qs;
  } else {
    body = JSON.stringify(params || {});
    headers['X-BAPI-SIGN'] = sign(ts, state.apiKey, recv, body, state.apiSecret);
    headers['Content-Type'] = 'application/json';
  }
  const res = await fetch(url, { method, headers, body });
  return res.json();
}

async function publicGet(endpoint, params) {
  const qs = new URLSearchParams(params).toString();
  const res = await fetch(BASE_URL + endpoint + '?' + qs);
  return res.json();
}

async function refreshBalance() {
  if (!state.apiKey) return;
  try {
    const bal = await signedRequest('GET', '/v5/account/wallet-balance', { accountType: 'UNIFIED' });
    if (bal.retCode !== 0) return;
    const acc = bal.result.list[0];
    const usdt = acc.coin.find(c => c.coin === 'USDT') || {};
    state.balance = parseFloat(usdt.walletBalance || 0);
    state.balanceDetail = {
      wallet: parseFloat(usdt.walletBalance || 0),
      available: parseFloat(usdt.availableToWithdraw || usdt.walletBalance || 0),
      upnl: parseFloat(acc.totalPerpUPL || 0),
      equity: parseFloat(acc.totalEquity || 0),
    };
  } catch (e) { log('BAL ERR: ' + e.message); }
}

async function loadInstruments() {
  try {
    const res = await publicGet('/v5/market/instruments-info', { category: 'linear', limit: 1000 });
    if (!res.result || !res.result.list) { log('INSTRUMENTS: пустой ответ'); return; }
    instrumentsCache = {};
    for (const i of res.result.list) {
      if (i.quoteCoin !== 'USDT') continue;
      if (i.status !== 'Trading') continue;
      if (i.contractType !== 'LinearPerpetual') continue;
      const lot = i.lotSizeFilter || {};
      instrumentsCache[i.symbol] = {
        qtyStep: parseFloat(lot.qtyStep || '0.001'),
        minQty: parseFloat(lot.minOrderQty || '0.001'),
        maxQty: parseFloat(lot.maxOrderQty || '1000000'),
      };
    }
    log('Загружено ' + Object.keys(instrumentsCache).length + ' инструментов');
  } catch (e) { log('INSTRUMENTS ERR: ' + e.message); }
}

async function scanMarket() {
  const res = await publicGet('/v5/market/tickers', { category: 'linear' });
  if (!res.result || !res.result.list) return [];
  const cfg = state.config, out = [];
  const now = Date.now();
  for (const t of res.result.list) {
    const sym = t.symbol;
    if (!sym.endsWith('USDT')) continue;
    if (cfg.exclude.includes(sym)) continue;
    if (/^(USDC|DAI|TUSD|BUSD|FDUSD|USDE)/.test(sym)) continue;
    if (!instrumentsCache[sym]) continue;
    if (state.blacklist[sym] && state.blacklist[sym] > now) continue;
    const vol24 = parseFloat(t.turnover24h || 0);
    if (vol24 < cfg.min24hVolumeUSDT) continue;
    const last = parseFloat(t.lastPrice);
    const high = parseFloat(t.highPrice24h || last);
    const low = parseFloat(t.lowPrice24h || last);
    const changePct = parseFloat(t.price24hPcnt || 0) * 100;
    const rangePct = ((high - low) / low) * 100;
    const activity = vol24 / (high || 1);
    const score = Math.log10(vol24) * 1.5 + Math.log10(1 + rangePct) * 2.5 +
                  Math.log10(1 + Math.abs(changePct)) * 2.0 + Math.log10(1 + activity) * 0.5;
    out.push({ symbol: sym, last, changePct, vol24, rangePct, score: +score.toFixed(3) });
  }
  out.sort((a, b) => b.score - a.score);
  return out.slice(0, cfg.topN);
}

class Engine {
  constructor(symbol) {
    this.symbol = symbol;
    this.book = { bids: [], asks: [] };
    this.trades = []; this.klines = []; this.spreadPct = 0;
  }
  imbalance(d = 10) {
    const bv = this.book.bids.slice(0, d).reduce((s, x) => s + x[1], 0);
    const av = this.book.asks.slice(0, d).reduce((s, x) => s + x[1], 0);
    return bv + av ? (bv - av) / (bv + av) : 0;
  }
  cvd(w = 180) {
    const cut = Date.now() - w * 1000; let c = 0;
    for (const t of this.trades) if (t.ts >= cut) c += (t.side === 'Buy' ? t.size : -t.size);
    return c;
  }
  largeTrades(w = 60) {
    const cut = Date.now() - w * 1000;
    const r = this.trades.filter(t => t.ts >= cut);
    if (r.length < 20) return { buy: 0, sell: 0 };
    const sorted = r.map(t => t.size).sort((a, b) => a - b);
    const thr = sorted[Math.floor(sorted.length * 0.95)];
    let buy = 0, sell = 0;
    for (const t of r) { if (t.size < thr) continue; if (t.side === 'Buy') buy += t.size; else sell += t.size; }
    return { buy, sell };
  }
  natr() {
    if (this.klines.length < 6) return 0;
    const k = this.klines.slice(-6); let tr = 0;
    for (let i = 1; i < k.length; i++) {
      const h = k[i].high, l = k[i].low, pc = k[i - 1].close;
      tr += Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
    }
    const atr = tr / (k.length - 1), close = k[k.length - 1].close;
    return close ? (atr / close) * 100 : 0;
  }
  volumeZ() {
    if (this.klines.length < 10) return 0;
    const v = this.klines.map(k => k.volume);
    const r = v.slice(-5), b = v.slice(0, -5);
    const mean = b.reduce((a, x) => a + x, 0) / b.length;
    const std = Math.sqrt(b.reduce((s, x) => s + (x - mean) ** 2, 0) / b.length) || 1e-9;
    return (r.reduce((a, x) => a + x, 0) / r.length - mean) / std;
  }
  price() { return (this.book.bids[0] && this.book.bids[0][0]) || (this.book.asks[0] && this.book.asks[0][0]) || 0; }
  signal() {
    const imb = this.imbalance(), cvd = this.cvd(), lg = this.largeTrades();
    const z = this.volumeZ();
    let L = 0, S = 0;
    if (imb > 0.2) L++; else if (imb < -0.2) S++;
    if (cvd > 0) L++; else if (cvd < 0) S++;
    if (z > 1.5) { if (cvd > 0) L++; else S++; }
    if (lg.buy > lg.sell * 1.5) L += 2;
    if (lg.sell > lg.buy * 1.5) S += 2;
    return {
      action: L >= 4 ? 'LONG' : S >= 4 ? 'SHORT' : 'HOLD',
      metrics: { imb: +imb.toFixed(3), cvd: +cvd.toFixed(4), natr: +this.natr().toFixed(3),
                 z: +z.toFixed(2), largeBuy: +lg.buy.toFixed(3), largeSell: +lg.sell.toFixed(3) },
      score: { long: L, short: S },
    };
  }
}

async function loadSymbolData(sym) {
  if (!state.engines[sym]) state.engines[sym] = new Engine(sym);
  const e = state.engines[sym];
  const [book, tr, kl] = await Promise.all([
    publicGet('/v5/market/orderbook', { category: 'linear', symbol: sym, limit: 25 }),
    publicGet('/v5/market/recent-trade', { category: 'linear', symbol: sym, limit: 500 }),
    publicGet('/v5/market/kline', { category: 'linear', symbol: sym, interval: '5', limit: 30 }),
  ]);
  if (book.result) {
    e.book.bids = book.result.b.map(x => [+x[0], +x[1]]);
    e.book.asks = book.result.a.map(x => [+x[0], +x[1]]);
    const bid = e.book.bids[0] && e.book.bids[0][0], ask = e.book.asks[0] && e.book.asks[0][0];
    if (bid && ask && bid > 0) e.spreadPct = ((ask - bid) / bid) * 100;
  }
  if (tr.result && tr.result.list) e.trades = tr.result.list.map(t => ({ ts: +t.time, side: t.side, size: +t.size, price: +t.price }));
  if (kl.result && kl.result.list) e.klines = kl.result.list.map(k => ({ open: +k[1], high: +k[2], low: +k[3], close: +k[4], volume: +k[5] })).reverse();
}

async function restorePositions() {
  try {
    const r = await signedRequest('GET', '/v5/position/list', { category: 'linear', settleCoin: 'USDT' });
    if (r.retCode !== 0 || !r.result || !r.result.list) return;
    let restored = 0;
    const cfg = state.config;
    for (const pos of r.result.list) {
      const size = parseFloat(pos.size || 0);
      if (size <= 0) continue;
      const symbol = pos.symbol;
      if (state.positions[symbol]) continue;
      const side = pos.side === 'Buy' ? 'LONG' : 'SHORT';
      const entry = parseFloat(pos.avgPrice || 0);
      if (!entry) continue;
      const tp = side === 'LONG' ? entry * (1 + cfg.takeProfitPct / 100) : entry * (1 - cfg.takeProfitPct / 100);
      const sl = side === 'LONG' ? entry * (1 - cfg.stopLossPct / 100) : entry * (1 + cfg.stopLossPct / 100);
      state.positions[symbol] = {
        symbol, side, entry, qty: size, tp, sl,
        openedAt: Date.now(), restored: true, beMoved: false,
        bestPrice: entry,
        slPct: cfg.stopLossPct, tpPct: cfg.takeProfitPct,
        bePct: cfg.breakevenTriggerPct, beOffsetPct: cfg.breakevenOffsetPct,
        trailEnabled: cfg.trailingEnabled, trailDistPct: cfg.trailingDistancePct,
      };
      restored++;
    }
    if (restored > 0) log('Восстановлено ' + restored + ' позиций с биржи');
  } catch (e) { log('RESTORE ERR: ' + e.message); }
}

function updateBlacklist() {
  const now = Date.now();
  const cfg = state.config;
  for (const s in state.blacklist) {
    if (state.blacklist[s] < now) {
      delete state.blacklist[s];
      log(s + ' убран из чёрного списка');
    }
  }
  const bySymbol = {};
  for (const t of db.trades.slice(-1000)) {
    if (!bySymbol[t.symbol]) bySymbol[t.symbol] = [];
    bySymbol[t.symbol].push(t);
  }
  for (const sym in bySymbol) {
    if (state.blacklist[sym]) continue;
    const recent = bySymbol[sym].slice(-cfg.blacklistLookback);
    if (recent.length < cfg.blacklistLookback) continue;
    const pnl = recent.reduce((s, t) => s + t.pnl, 0);
    if (pnl < -cfg.blacklistThresholdUSDT) {
      state.blacklist[sym] = now + cfg.blacklistPeriodHours * 3600000;
      log(sym + ' в чёрный список на ' + cfg.blacklistPeriodHours + 'ч');
    }
  }
}

let balanceCounter = 0;
async function tick() {
  if (!state.running) return;
  state.lastTick = Date.now();
  const cfg = state.config;

  balanceCounter++;
  if (balanceCounter % 10 === 0) await refreshBalance();

  const dayPnl = db.dailyPnl[today()] || 0;
  const dayLimit = -(state.balance * cfg.dailyRiskPct / 100);
  if (dayPnl <= dayLimit) {
    log('ДНЕВНОЙ ЛИМИТ: ' + dayPnl.toFixed(2) + ' USDT - СТОП.');
    state.running = false; return;
  }

  const todayTrades = db.trades.filter(t => new Date(t.ts).toISOString().slice(0, 10) === today()).length;
  const dailyTradeLimit = cfg.maxTradesPerDay > 0 && todayTrades >= cfg.maxTradesPerDay;

  if (Date.now() - state.lastScan > 15000) {
    try {
      state.scanResult = await scanMarket();
      state.lastScan = Date.now();
      log('Скан: ' + state.scanResult.slice(0, 5).map(s => s.symbol).join(', ') + '...');
    } catch (e) { log('SCAN ERR: ' + e.message); }
  }

  for (const sym of Object.keys(state.positions)) {
    try {
      await loadSymbolData(sym);
      const m = state.engines[sym].price();
      if (m) await managePosition(sym, m);
    } catch (e) { log('POS ' + sym + ': ' + e.message); }
  }

  if (dailyTradeLimit) return;
  if (Object.keys(state.positions).length >= cfg.maxConcurrentPositions) return;

  const now = Date.now();
  for (const c of state.scanResult) {
    if (state.positions[c.symbol]) continue;
    if (state.blacklist[c.symbol] && state.blacklist[c.symbol] > now) continue;
    if (state.cooldowns[c.symbol] && state.cooldowns[c.symbol] > now) continue;
    try {
      await loadSymbolData(c.symbol);
      const e = state.engines[c.symbol];
      if (e.spreadPct > cfg.maxSpreadPct) continue;
      const s = e.signal();
      if (s.action !== 'HOLD') {
        await openPosition(c.symbol, s, c);
        if (Object.keys(state.positions).length >= cfg.maxConcurrentPositions) break;
      }
    } catch (e) { log('CHK ' + c.symbol + ': ' + e.message); }
  }
}

async function openPosition(symbol, sig, cand) {
  const cfg = state.config, e = state.engines[symbol], price = e.price();
  if (!price) return;
  const inst = instrumentsCache[symbol];
  if (!inst) return;

  const riskUsdt = state.balance * cfg.perTradeRiskPct / 100;
  const slDist = price * cfg.stopLossPct / 100;
  let qty = Math.min(riskUsdt / slDist, (state.balance * cfg.maxLeverage) / price);

  const step = inst.qtyStep;
  qty = Math.floor(qty / step) * step;
  const decimals = (step.toString().split('.')[1] || '').length;
  qty = +qty.toFixed(decimals);

  if (qty < inst.minQty) { log('SKIP ' + symbol + ': qty ' + qty + ' < minQty ' + inst.minQty); return; }
  if (qty > inst.maxQty) qty = inst.maxQty;
  if (qty <= 0) return;

  const side = sig.action === 'LONG' ? 'Buy' : 'Sell';
  try {
    const r = await signedRequest('POST', '/v5/order/create', {
      category: 'linear', symbol, side, orderType: 'Market', qty: qty.toString(),
    });
    if (r.retCode !== 0) { log('ORDER ' + symbol + ': ' + r.retMsg); return; }
    const tp = sig.action === 'LONG' ? price * (1 + cfg.takeProfitPct / 100) : price * (1 - cfg.takeProfitPct / 100);
    const sl = sig.action === 'LONG' ? price * (1 - cfg.stopLossPct / 100) : price * (1 + cfg.stopLossPct / 100);
    state.positions[symbol] = {
      symbol, side: sig.action, entry: price, qty, tp, sl,
      openedAt: Date.now(), signal: sig, scanScore: cand ? cand.score : 0,
      beMoved: false, bestPrice: price,
      slPct: cfg.stopLossPct, tpPct: cfg.takeProfitPct,
      bePct: cfg.breakevenTriggerPct, beOffsetPct: cfg.breakevenOffsetPct,
      trailEnabled: cfg.trailingEnabled, trailDistPct: cfg.trailingDistancePct,
    };
    log('OPEN ' + sig.action + ' ' + symbol + ' qty=' + qty + ' @ ' + price);
  } catch (e) { log('OPEN ' + symbol + ': ' + e.message); }
}

async function managePosition(symbol, mark) {
  const p = state.positions[symbol];
  if (!p) return;
  const cfg = state.config;
  let forceCloseReason = null;

  if (cfg.breakevenTriggerPct > 0 && !p.beMoved) {
    const profitPct = p.side === 'LONG'
      ? ((mark - p.entry) / p.entry) * 100
      : ((p.entry - mark) / p.entry) * 100;
    if (profitPct >= cfg.breakevenTriggerPct) {
      const offset = cfg.breakevenOffsetPct || 0;
      p.sl = p.side === 'LONG'
        ? p.entry * (1 + offset / 100)
        : p.entry * (1 - offset / 100);
      p.beMoved = true;
      p.bestPrice = mark;
      log('BE ' + symbol + ' (прибыль ' + profitPct.toFixed(2) + '%). SL=' + p.sl.toFixed(6));
    }
  }

  if (p.beMoved && p.trailEnabled && p.trailDistPct > 0) {
    if (p.side === 'LONG') {
      if (!p.bestPrice || mark > p.bestPrice) p.bestPrice = mark;
      const newSL = p.bestPrice * (1 - p.trailDistPct / 100);
      if (newSL > p.sl) p.sl = newSL;
    } else {
      if (!p.bestPrice || mark < p.bestPrice) p.bestPrice = mark;
      const newSL = p.bestPrice * (1 + p.trailDistPct / 100);
      if (newSL < p.sl) p.sl = newSL;
    }
  }

  if (cfg.maxPositionMinutes > 0) {
    const heldMin = (Date.now() - p.openedAt) / 60000;
    if (heldMin >= cfg.maxPositionMinutes) forceCloseReason = 'TIME';
  }

  const hitTP = p.side === 'LONG' ? mark >= p.tp : mark <= p.tp;
  const hitSL = p.side === 'LONG' ? mark <= p.sl : mark >= p.sl;
  if (!hitTP && !hitSL && !forceCloseReason) return;

  const inst = instrumentsCache[symbol];
  let closeQty = p.qty;
  if (inst) {
    const step = inst.qtyStep;
    const decimals = (step.toString().split('.')[1] || '').length;
    closeQty = +p.qty.toFixed(decimals);
  }

  let exitPrice, reason;
  if (forceCloseReason === 'TIME') {
    exitPrice = mark;
    reason = 'TIME';
  } else if (hitTP) {
    exitPrice = p.tp;
    reason = 'TP';
  } else {
    exitPrice = p.sl;
    reason = p.beMoved && p.trailEnabled ? 'TRAIL' : (p.beMoved ? 'BE' : 'SL');
  }

  const pnl = (p.side === 'LONG' ? exitPrice - p.entry : p.entry - exitPrice) * p.qty;
  const pnlPct = pnl / (p.entry * p.qty) * 100;

  try {
    const r = await signedRequest('POST', '/v5/order/create', {
      category: 'linear', symbol, side: p.side === 'LONG' ? 'Sell' : 'Buy',
      orderType: 'Market', qty: closeQty.toString(), reduceOnly: true,
    });
    if (r.retCode !== 0) log('CLOSE ' + symbol + ' ERR: ' + r.retMsg);
  } catch (e) { log('CLOSE ' + symbol + ': ' + e.message); }

  db.trades.push({
    ts: Date.now(), openedAt: p.openedAt,
    symbol, side: p.side, entry: p.entry, exit: exitPrice, qty: p.qty,
    pnl, pnl_pct: pnlPct, close_reason: reason,
    slPct: p.slPct, tpPct: p.tpPct, bePct: p.bePct, beOffsetPct: p.beOffsetPct,
  });
  db.dailyPnl[today()] = (db.dailyPnl[today()] || 0) + pnl;
  saveDB();

  if ((reason === 'SL' || reason === 'TRAIL' || reason === 'TIME') && cfg.cooldownMinutes > 0) {
    state.cooldowns[symbol] = Date.now() + cfg.cooldownMinutes * 60000;
    log('Cooldown ' + symbol + ' ' + cfg.cooldownMinutes + ' мин');
  }

  log('CLOSE ' + p.side + ' ' + symbol + ' @ ' + exitPrice + ' PnL=' + pnl.toFixed(4) + ' (' + reason + ')');
  delete state.positions[symbol];
  refreshBalance();
  updateBlacklist();
}

function computeStats() {
  const a = db.trades, w = a.filter(t => t.pnl > 0), l = a.filter(t => t.pnl <= 0);
  const gw = w.reduce((s, t) => s + t.pnl, 0), gl = Math.abs(l.reduce((s, t) => s + t.pnl, 0));
  return {
    total: a.length, wins: w.length, losses: l.length,
    winRate: a.length ? (w.length / a.length * 100).toFixed(2) : '0',
    totalPnl: a.reduce((s, t) => s + t.pnl, 0).toFixed(4),
    profitFactor: gl ? (gw / gl).toFixed(2) : (gw > 0 ? '999' : '0'),
    avgWin: w.length ? (gw / w.length).toFixed(4) : '0',
    avgLoss: l.length ? (gl / l.length).toFixed(4) : '0',
  };
}

function buildGroupStats(trades) {
  const w = trades.filter(t => t.pnl > 0);
  const l = trades.filter(t => t.pnl <= 0);
  const gw = w.reduce((s, t) => s + t.pnl, 0);
  const gl = Math.abs(l.reduce((s, t) => s + t.pnl, 0));
  const totalPnl = trades.reduce((s, t) => s + t.pnl, 0);
  let peak = 0, equity = 0, maxDD = 0;
  for (const t of trades) {
    equity += t.pnl;
    if (equity > peak) peak = equity;
    const dd = peak - equity;
    if (dd > maxDD) maxDD = dd;
  }
  const reasonCount = {};
  for (const t of trades) reasonCount[t.close_reason] = (reasonCount[t.close_reason] || 0) + 1;
  return {
    total: trades.length,
    wins: w.length, losses: l.length,
    winRate: trades.length ? +(w.length / trades.length * 100).toFixed(1) : 0,
    totalPnl: +totalPnl.toFixed(4),
    profitFactor: gl ? +(gw / gl).toFixed(2) : (gw > 0 ? 999 : 0),
    avgWin: w.length ? +(gw / w.length).toFixed(4) : 0,
    avgLoss: l.length ? +(gl / l.length).toFixed(4) : 0,
    maxDrawdown: +maxDD.toFixed(4),
    tpHits: reasonCount['TP'] || 0,
    slHits: reasonCount['SL'] || 0,
    beHits: reasonCount['BE'] || 0,
    trailHits: reasonCount['TRAIL'] || 0,
    timeHits: reasonCount['TIME'] || 0,
  };
}

function getParamGroups() {
  const groups = {};
  for (const t of db.trades) {
    const sl = t.slPct != null ? t.slPct : 0.4;
    const tp = t.tpPct != null ? t.tpPct : 0.7;
    const be = t.bePct != null ? t.bePct : 0.5;
    const beOff = t.beOffsetPct != null ? t.beOffsetPct : 0.05;
    const key = sl + '_' + tp + '_' + be + '_' + beOff;
    if (!groups[key]) {
      groups[key] = { key, slPct: sl, tpPct: tp, bePct: be, beOffsetPct: beOff, trades: [] };
    }
    groups[key].trades.push(t);
  }
  const arr = [];
  for (const k in groups) {
    const g = groups[k];
    const stats = buildGroupStats(g.trades);
    arr.push(Object.assign({
      key: g.key,
      slPct: g.slPct, tpPct: g.tpPct, bePct: g.bePct, beOffsetPct: g.beOffsetPct,
      ready: stats.total >= BATCH_SIZE,
      progress: Math.min(stats.total, BATCH_SIZE),
      batchSize: BATCH_SIZE,
    }, stats));
  }
  return arr;
}

function readBody(req) {
  return new Promise(res => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { res(JSON.parse(d || '{}')); } catch { res({}); } }); });
}
function json(res, o) { res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.end(JSON.stringify(o)); }

async function handleStart(body, res) {
  try {
    const apiKey = body.apiKey, apiSecret = body.apiSecret, config = body.config;
    if (!apiKey || !apiSecret) return json(res, { error: 'API keys required' });
    state.apiKey = apiKey; state.apiSecret = apiSecret;
    Object.assign(state.config, config || {});
    await refreshBalance();
    if (state.balance <= 0) return json(res, { error: 'Balance 0 или неверные ключи' });
    if (Object.keys(instrumentsCache).length === 0) await loadInstruments();
    await restorePositions();
    state.running = true;
    state.startedAt = state.startedAt || Date.now();
    if (!state.loop) state.loop = setInterval(() => tick().catch(e => log('TICK: ' + e.message)), 2000);
    log('Старт. Баланс: ' + state.balance + ' USDT');
    json(res, { ok: true, balance: state.balance });
  } catch (e) { json(res, { error: e.message }); }
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://' + req.headers.host), p = u.pathname;

  if (p === '/' && req.method === 'GET') { res.setHeader('Content-Type', 'text/html; charset=utf-8'); return res.end(HTML); }
  if (p === '/api/start' && req.method === 'POST') return handleStart(await readBody(req), res);
  if (p === '/api/stop' && req.method === 'POST') { state.running = false; state.startedAt = null; log('Остановлен'); return json(res, { ok: true }); }
  if (p === '/api/restart' && req.method === 'POST') { state.running = false; state.startedAt = null; log('Рестарт...'); setTimeout(() => { state.running = true; state.startedAt = Date.now(); log('Перезапущен'); }, 500); return json(res, { ok: true }); }
  if (p === '/api/balance' && req.method === 'POST') { await refreshBalance(); return json(res, state.balanceDetail); }

  if (p === '/api/export-csv') {
    const rows = [['openTime','closeTime','symbol','side','entry','exit','qty','pnl','pnl_pct','reason','slPct','tpPct','bePct','beOffsetPct']];
    for (const t of db.trades) {
      rows.push([
        t.openedAt ? new Date(t.openedAt).toISOString() : '',
        new Date(t.ts).toISOString(),
        t.symbol, t.side, t.entry, t.exit, t.qty,
        t.pnl.toFixed(6), t.pnl_pct.toFixed(4), t.close_reason,
        t.slPct || '', t.tpPct || '', t.bePct || '', t.beOffsetPct || ''
      ]);
    }
    const csv = rows.map(r => r.join(';')).join('\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="trades_' + today() + '.csv"');
    return res.end('\uFEFF' + csv);
  }

  if (p === '/api/param-groups') {
    const groups = getParamGroups();
    const curKey = state.config.stopLossPct + '_' + state.config.takeProfitPct + '_' + state.config.breakevenTriggerPct + '_' + state.config.breakevenOffsetPct;
    return json(res, { batchSize: BATCH_SIZE, currentKey: curKey, groups });
  }

  if (p === '/api/klines') {
    const sym = u.searchParams.get('symbol');
    const intv = u.searchParams.get('interval') || '1';
    const lim = u.searchParams.get('limit') || '1000';
    const r = await publicGet('/v5/market/kline', { category: 'linear', symbol: sym, interval: intv, limit: lim });
    if (!r.result || !r.result.list) return json(res, { klines: [] });
    const list = r.result.list.map(k => ({ ts: +k[0], open: +k[1], high: +k[2], low: +k[3], close: +k[4] })).reverse();
    return json(res, { klines: list });
  }

  if (p === '/api/symbols') {
    const map = {};
    for (const t of db.trades) map[t.symbol] = (map[t.symbol] || 0) + 1;
    const syms = Object.keys(map).map(s => ({ symbol: s, count: map[s] })).sort((a, b) => b.count - a.count);
    return json(res, { symbols: syms });
  }

  if (p === '/api/trades-by-symbol') {
    const sym = u.searchParams.get('symbol');
    const trs = db.trades.filter(t => t.symbol === sym).slice(-500);
    const w = trs.filter(t => t.pnl > 0), l = trs.filter(t => t.pnl <= 0);
    const gw = w.reduce((s, t) => s + t.pnl, 0);
    const gl = Math.abs(l.reduce((s, t) => s + t.pnl, 0));
    return json(res, {
      trades: trs,
      summary: {
        total: trs.length, wins: w.length, losses: l.length,
        winRate: trs.length ? (w.length / trs.length * 100).toFixed(2) : '0',
        totalPnl: trs.reduce((s, t) => s + t.pnl, 0).toFixed(4),
        profitFactor: gl ? (gw / gl).toFixed(2) : (gw > 0 ? '999' : '0'),
        avgWin: w.length ? (gw / w.length).toFixed(4) : '0',
        avgLoss: l.length ? (gl / l.length).toFixed(4) : '0',
      }
    });
  }

  if (p === '/api/state') {
    const now = Date.now();
    const posArr = Object.values(state.positions).map(pos => {
      const e = state.engines[pos.symbol], mark = e ? e.price() : 0;
      const upnl = mark ? (pos.side === 'LONG' ? mark - pos.entry : pos.entry - mark) * pos.qty : 0;
      const upnlPct = mark ? ((pos.side === 'LONG' ? mark - pos.entry : pos.entry - mark) / pos.entry) * 100 : 0;
      const notional = pos.entry * pos.qty;
      const margin = notional / (state.config.maxLeverage || 1);
      return Object.assign({}, pos, { mark, upnl, upnlPct, notional, margin });
    });
    const scan = state.scanResult.map(s => ({
      symbol: s.symbol, last: s.last, changePct: s.changePct, vol24: s.vol24, rangePct: s.rangePct, score: s.score,
      hasPosition: !!state.positions[s.symbol],
      cooldown: state.cooldowns[s.symbol] && state.cooldowns[s.symbol] > now,
      blacklisted: state.blacklist[s.symbol] && state.blacklist[s.symbol] > now,
      metrics: state.engines[s.symbol] ? state.engines[s.symbol].signal() : null,
    }));
    const todayTrades = db.trades.filter(t => new Date(t.ts).toISOString().slice(0, 10) === today()).length;
    return json(res, {
      running: state.running, balance: state.balance, balanceDetail: state.balanceDetail,
      config: state.config, positions: posArr, scan,
      dailyPnl: db.dailyPnl[today()] || 0,
      todayTrades,
      startedAt: state.startedAt,
      instrumentsCount: Object.keys(instrumentsCache).length,
      blacklistCount: Object.keys(state.blacklist).filter(s => state.blacklist[s] > now).length,
      cooldownCount: Object.keys(state.cooldowns).filter(s => state.cooldowns[s] > now).length,
      log: state.log.slice(-50),
    });
  }
  if (p === '/api/stats') return json(res, computeStats());
  if (p === '/api/trades') return json(res, db.trades.slice(-100).reverse());
  res.statusCode = 404; res.end('Not found');
});

const HTML = `<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>Bybit Scalper v7.1</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;font-family:system-ui,-apple-system,sans-serif;margin:0;padding:0}
body{background:#0a0d12;color:#e6e6e6;font-size:13px;padding:10px}
.hd{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:8px 14px;background:#12151c;border:1px solid #1e2530;border-radius:10px;margin-bottom:10px;flex-wrap:wrap}
.hd h1{font-size:16px;font-weight:700}
.chips{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.chip{background:#0a0d12;border:1px solid #1e2530;border-radius:6px;padding:4px 10px;font-size:12px;display:flex;gap:6px;align-items:center;font-variant-numeric:tabular-nums}
.chip b{font-weight:700}
.badge{padding:4px 12px;border-radius:16px;font-size:11px;font-weight:700}
.badge.on{background:#065f46;color:#6ee7b7}
.badge.off{background:#4c1d1d;color:#fca5a5}
.tabs{display:flex;gap:4px;margin-bottom:10px;flex-wrap:wrap}
.tab{background:#12151c;border:1px solid #1e2530;color:#6b7280;padding:8px 18px;font-weight:600;cursor:pointer;font-size:13px;border-radius:8px}
.tab.active{background:#1e3a5f;color:#93c5fd;border-color:#3b82f6}
.card{background:#12151c;border:1px solid #1e2530;border-radius:10px;padding:12px;margin-bottom:10px}
.card h2{font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.6px;margin-bottom:10px;font-weight:600;display:flex;justify-content:space-between;align-items:center}
.pos{color:#4ade80}.neg{color:#f87171}.neu{color:#60a5fa}
input,button,select{padding:7px 10px;border-radius:6px;border:1px solid #2a3240;background:#0a0d12;color:#e6e6e6;font-size:12px;width:100%;transition:.15s}
input:focus,select:focus{outline:none;border-color:#3b82f6}
label{display:block}
label>span{display:block;font-size:10px;color:#6b7280;margin-bottom:3px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}
.row{display:flex;gap:6px;margin-top:8px;flex-wrap:wrap;align-items:center}
button{cursor:pointer;font-weight:600;padding:8px 14px;width:auto;transition:.15s}
button.primary{background:#065f46;border-color:#10b981;color:#6ee7b7}
button.primary:hover{background:#047857}
button.danger{background:#4c1d1d;border-color:#ef4444;color:#fca5a5}
button.danger:hover{background:#7f1d1d}
button.secondary{background:#1e3a5f;border-color:#3b82f6;color:#93c5fd}
button.secondary:hover{background:#1e40af}
button.toggle{background:transparent;border:1px solid #2a3240;color:#9ca3af;padding:3px 10px;font-size:11px;font-weight:500}
table{width:100%;border-collapse:collapse;font-size:12px}
th,td{padding:6px 8px;text-align:left;border-bottom:1px solid #1e2530}
th{color:#6b7280;font-weight:500;font-size:10px;text-transform:uppercase;letter-spacing:.4px}
tbody tr:hover{background:#161a24}
tbody tr:last-child td{border-bottom:none}
.tag{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:700}
.tag.long{background:#065f46;color:#6ee7b7}
.tag.short{background:#4c1d1d;color:#fca5a5}
.tag.hold{background:#2a3240;color:#9ca3af}
.tag.be{background:#78350f;color:#fbbf24}
.tag.trail{background:#7c2d12;color:#fdba74}
.tag.time{background:#1e3a5f;color:#93c5fd}
.tag.ready{background:#065f46;color:#6ee7b7}
.tag.wait{background:#1e3a5f;color:#93c5fd}
.tag.cool{background:#312e81;color:#a5b4fc}
.tag.blk{background:#7f1d1d;color:#fca5a5}
.muted{color:#6b7280;font-size:11px}
.pos-card{background:linear-gradient(180deg,#0f131b,#0a0d12);border:1px solid #1e2530;border-radius:10px;padding:12px 14px;margin-bottom:8px;transition:.15s}
.pos-card:hover{border-color:#2a3240}
.pos-card.long{border-left:3px solid #10b981}
.pos-card.short{border-left:3px solid #ef4444}
.pos-head{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;gap:10px;flex-wrap:wrap}
.pos-sym{display:flex;align-items:center;gap:6px;font-size:15px;font-weight:700}
.pos-sym .sym-name{color:#60a5fa}
.pos-pnl{font-size:18px;font-weight:800;font-variant-numeric:tabular-nums;display:flex;align-items:baseline;gap:8px}
.pos-pnl .pnl-pct{font-size:12px;font-weight:600;opacity:.85}
.pos-body{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.pos-cell{background:#0a0d12;border:1px solid #1e2530;border-radius:8px;padding:8px 10px}
.pos-cell .lbl{color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:.4px;margin-bottom:4px}
.pos-cell .val{font-weight:700;font-size:13px;font-variant-numeric:tabular-nums;color:#e6e6e6}
.pos-cell .sub{color:#6b7280;font-size:10px;margin-top:3px;font-variant-numeric:tabular-nums}
@media(max-width:800px){.pos-body{grid-template-columns:1fr 1fr}}
pre{max-height:180px;overflow:auto;font-size:11px;color:#9ca3af;white-space:pre-wrap;font-family:ui-monospace,monospace;line-height:1.5;background:#0a0d12;padding:8px;border-radius:6px}
.stats-strip{display:flex;gap:8px;flex-wrap:wrap}
.stat-mini{background:#0a0d12;border:1px solid #1e2530;border-radius:6px;padding:6px 10px;font-size:11px;min-width:90px}
.stat-mini .l{color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:.3px}
.stat-mini .v{font-weight:700;font-size:14px;margin-top:2px;font-variant-numeric:tabular-nums}
.hidden{display:none}
.chart-wrap{position:relative;height:420px;background:#0a0d12;border-radius:8px;padding:8px}
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:11px;color:#9ca3af;margin-top:6px}
.legend span{display:flex;align-items:center;gap:4px}
.dot{width:10px;height:10px;border-radius:50%;display:inline-block}
.sec-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.sec-head h2{margin:0}
.pg-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px}
.pg-card{background:#0a0d12;border:1px solid #1e2530;border-radius:10px;padding:12px}
.pg-card.ready{border-color:#10b981;background:linear-gradient(180deg,#0a1714,#0a0d12)}
.pg-card.best{border-color:#fbbf24;background:linear-gradient(180deg,#18130a,#0a0d12)}
.pg-head{display:flex;justify-content:space-between;align-items:flex-start;gap:8px;margin-bottom:10px}
.pg-title{font-size:13px;font-weight:700;line-height:1.4}
.pg-title .p{color:#6b7280;font-weight:500}
.pg-title b{color:#60a5fa}
.pg-crown{font-size:18px}
.pg-progress{height:6px;background:#1e2530;border-radius:3px;overflow:hidden;margin-bottom:10px}
.pg-progress-bar{height:100%;background:#3b82f6;transition:width .3s}
.pg-progress-bar.done{background:#10b981}
.pg-count{font-size:11px;color:#6b7280;margin-bottom:10px}
.pg-count b{color:#e6e6e6}
.pg-metrics{display:grid;grid-template-columns:repeat(2,1fr);gap:6px;font-size:11px}
.pg-m{background:#12151c;border:1px solid #1e2530;border-radius:6px;padding:6px 8px}
.pg-m .l{color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:.3px}
.pg-m .v{font-weight:700;font-size:13px;margin-top:2px;font-variant-numeric:tabular-nums}
.pg-m .v.big{font-size:15px}
.pg-tags{display:flex;gap:4px;flex-wrap:wrap;margin-top:8px;font-size:10px}
.pg-tag{background:#12151c;border:1px solid #1e2530;border-radius:4px;padding:2px 6px;color:#9ca3af}
.pg-tag b{color:#e6e6e6}
@media(max-width:900px){.grid{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>

<div class="hd">
<h1>Bybit Scalper v7.1</h1>
<div class="chips">
<span class="chip">Wallet: <b id="bWallet">-</b></span>
<span class="chip">Avail: <b id="bAvail">-</b></span>
<span class="chip">uPnL: <b id="bUpnl">-</b></span>
<span class="chip">Day: <b id="bDaily">-</b></span>
<span class="chip">Today: <b id="bToday">0</b></span>
<span class="chip">Inst: <b id="instCount">0</b></span>
<span class="chip">BL: <b id="blCount">0</b></span>
<span class="chip">CD: <b id="cdCount">0</b></span>
<span class="chip">Up: <b id="uptime">-</b></span>
<span class="badge off" id="status">Остановлен</span>
</div>
</div>

<div class="tabs">
<button class="tab active" data-tab="main">Основная</button>
<button class="tab" data-tab="params">Аналитика параметров</button>
<button class="tab" data-tab="stats">Сделки на графике</button>
</div>

<div id="tab-main">

<div class="card">
<div class="sec-head">
<h2>Настройки и риск-менеджер</h2>
<div style="display:flex;gap:8px;align-items:center">
<label style="display:flex;gap:4px;align-items:center;font-size:11px;color:#9ca3af"><input type="checkbox" id="soundEnabled" style="width:auto;margin:0" checked> Звук</label>
<button class="toggle" id="toggleCfg">Развернуть</button>
</div>
</div>
<div id="cfgBody" class="hidden">
<div class="grid" style="margin-bottom:8px">
<label><span>API Key</span><input id="apiKey" placeholder="Demo API Key" autocomplete="off"></label>
<label><span>API Secret</span><input id="apiSecret" type="password" placeholder="Demo API Secret"></label>
<label><span>Исключить (через запятую)</span><input id="exclude" value="BTCUSDT,ETHUSDT"></label>
<label><span>Мин. объём 24ч (USDT)</span><input id="min24hVolumeUSDT" type="number" value="20000000"></label>
</div>
<div class="grid">
<label><span>Top-N монет</span><input id="topN" type="number" value="15"></label>
<label><span>Макс. спред %</span><input id="maxSpreadPct" type="number" value="0.15" step="0.01"></label>
<label><span>Риск/сделку %</span><input id="perTradeRiskPct" type="number" value="1" step="0.1"></label>
<label><span>Дневной лимит %</span><input id="dailyRiskPct" type="number" value="5" step="0.5"></label>
<label><span>Макс. плечо</span><input id="maxLeverage" type="number" value="5"></label>
<label><span>Одноврем. позиций</span><input id="maxConcurrentPositions" type="number" value="3"></label>
<label><span>Лимит сделок в день</span><input id="maxTradesPerDay" type="number" value="60"></label>
<label><span>Stop-Loss %</span><input id="stopLossPct" type="number" value="0.4" step="0.1"></label>
<label><span>Take-Profit %</span><input id="takeProfitPct" type="number" value="0.7" step="0.1"></label>
<label><span>Безубыток при %</span><input id="breakevenTriggerPct" type="number" value="0.5" step="0.1"></label>
<label><span>Смещение SL %</span><input id="breakevenOffsetPct" type="number" value="0.05" step="0.01"></label>
<label><span>Трейлинг (1=вкл)</span><input id="trailingEnabled" type="number" value="1" min="0" max="1" step="1"></label>
<label><span>Отступ трейлинга %</span><input id="trailingDistancePct" type="number" value="0.3" step="0.05"></label>
<label><span>Time-exit, мин</span><input id="maxPositionMinutes" type="number" value="15"></label>
<label><span>Cooldown, мин</span><input id="cooldownMinutes" type="number" value="10"></label>
<label><span>Порог блэклиста</span><input id="blacklistThresholdUSDT" type="number" value="5" step="0.5"></label>
<label><span>Срок блэклиста, ч</span><input id="blacklistPeriodHours" type="number" value="24"></label>
<label><span>Lookback</span><input id="blacklistLookback" type="number" value="20"></label>
</div>
<div class="row">
<button class="primary" id="startBtn">Старт</button>
<button class="secondary" id="restartBtn">Рестарт</button>
<button class="danger" id="stopBtn">Стоп</button>
<button class="secondary" id="refreshBalBtn">Баланс</button>
<button class="secondary" id="exportCsvBtn">CSV</button>
</div>
<div class="muted" style="margin-top:6px">Настройки сохраняются в браузере. При смене SL/TP/BE начинается новая партия из 100 сделок.</div>
</div>
</div>

<div class="card">
<h2>Открытые позиции (<span id="posCount">0</span>/<span id="posMax">3</span>)</h2>
<div id="positions"><div class="muted">Нет открытых позиций</div></div>
</div>

<div class="card">
<h2>Активные монеты</h2>
<table><thead><tr><th>Символ</th><th>Цена</th><th>24h %</th><th>Оборот</th><th>Диапазон</th><th>Score</th><th>Сигнал</th><th>Статус</th></tr></thead><tbody id="scanBody"></tbody></table>
</div>

<div class="card">
<h2>Общая статистика</h2>
<div class="stats-strip">
<div class="stat-mini"><div class="l">Сделок</div><div class="v" id="sTotal">0</div></div>
<div class="stat-mini"><div class="l">Win Rate</div><div class="v" id="sWR">0%</div></div>
<div class="stat-mini"><div class="l">Общий PnL</div><div class="v" id="sPnl">0</div></div>
<div class="stat-mini"><div class="l">Profit Factor</div><div class="v" id="sPF">0</div></div>
<div class="stat-mini"><div class="l">Ср. Win</div><div class="v pos" id="sAvgW">0</div></div>
<div class="stat-mini"><div class="l">Ср. Loss</div><div class="v neg" id="sAvgL">0</div></div>
</div>
</div>

<div class="card">
<h2>Последние сделки</h2>
<table><thead><tr><th>Время</th><th>Монета</th><th>Сторона</th><th>SL/TP/BE</th><th>Вход</th><th>Выход</th><th>PnL</th><th>Причина</th></tr></thead><tbody id="tradesBody"></tbody></table>
</div>

<div class="card">
<h2>Лог</h2>
<pre id="log">-</pre>
</div>

</div>

<div id="tab-params" class="hidden">
<div class="card">
<div class="sec-head">
<h2>Текущая партия</h2>
<button class="secondary" id="reloadParamsBtn" style="padding:5px 12px;font-size:12px">Обновить</button>
</div>
<div id="currentBatch"><div class="muted">Нет данных</div></div>
</div>

<div class="card">
<h2>Готовые к анализу (<span id="readyCount">0</span>) - лучший результат вверху</h2>
<div class="pg-grid" id="readyGroups"><div class="muted">Пока ни одна партия не набрала 100 сделок.</div></div>
</div>

<div class="card">
<h2>В процессе набора</h2>
<div class="pg-grid" id="pendingGroups"><div class="muted">Нет активных партий</div></div>
</div>

<div class="card">
<h2>Сводная таблица всех партий</h2>
<table><thead><tr>
<th>SL %</th><th>TP %</th><th>BE %</th><th>Смещ %</th><th>Сделок</th><th>WR</th><th>PnL</th><th>PF</th><th>MaxDD</th><th>TP/SL/BE/TRAIL/TIME</th>
</tr></thead><tbody id="paramTableBody"></tbody></table>
</div>
</div>

<div id="tab-stats" class="hidden">
<div class="card">
<div class="sec-head">
<h2>График сделок</h2>
<button class="secondary" id="reloadChartBtn" style="padding:5px 12px;font-size:12px">Обновить</button>
</div>
<div class="grid">
<label><span>Монета</span><select id="statsSymbol"><option value="">- выберите -</option></select></label>
<label><span>Таймфрейм</span><select id="statsInterval">
<option value="1">1 мин</option>
<option value="5">5 мин</option>
<option value="15">15 мин</option>
<option value="60">1 час</option>
</select></label>
<label><span>Свечей</span><select id="statsLimit">
<option value="500">500</option>
<option value="1000" selected>1000</option>
</select></label>
<label><span>Статус</span><div class="muted" id="chartInfo" style="padding-top:6px">-</div></label>
</div>
<div class="legend">
<span><span class="dot" style="background:#fbbf24"></span>Вход</span>
<span><span class="dot" style="background:#4ade80"></span>TP</span>
<span><span class="dot" style="background:#f87171"></span>SL</span>
<span><span class="dot" style="background:#fbbf24;opacity:.7"></span>BE</span>
<span><span class="dot" style="background:#fdba74"></span>TRAIL</span>
<span><span class="dot" style="background:#93c5fd"></span>TIME</span>
</div>
</div>

<div class="card">
<div class="chart-wrap"><canvas id="chart"></canvas></div>
</div>

<div class="card">
<h2>По монете</h2>
<div class="stats-strip">
<div class="stat-mini"><div class="l">Сделок</div><div class="v" id="ssTotal">0</div></div>
<div class="stat-mini"><div class="l">Win Rate</div><div class="v" id="ssWR">0%</div></div>
<div class="stat-mini"><div class="l">PnL</div><div class="v" id="ssPnl">0</div></div>
<div class="stat-mini"><div class="l">PF</div><div class="v" id="ssPF">0</div></div>
</div>
</div>

<div class="card">
<h2>Сделки по монете</h2>
<table><thead><tr><th>Время</th><th>Сторона</th><th>SL/TP/BE</th><th>Вход</th><th>Выход</th><th>PnL</th><th>%</th><th>Причина</th></tr></thead><tbody id="symbolTradesBody"></tbody></table>
</div>
</div>

<script>
var $=function(id){return document.getElementById(id)};
var CFG_KEYS=["apiKey","apiSecret","exclude","min24hVolumeUSDT","topN","maxSpreadPct","perTradeRiskPct","dailyRiskPct","maxLeverage","maxConcurrentPositions","maxTradesPerDay","stopLossPct","takeProfitPct","breakevenTriggerPct","breakevenOffsetPct","trailingEnabled","trailingDistancePct","maxPositionMinutes","cooldownMinutes","blacklistThresholdUSDT","blacklistPeriodHours","blacklistLookback"];
function saveCfg(){var o={};CFG_KEYS.forEach(function(k){if($(k))o[k]=$(k).value;});localStorage.setItem("bybitCfg",JSON.stringify(o));}
function loadCfg(){try{var o=JSON.parse(localStorage.getItem("bybitCfg")||"{}");CFG_KEYS.forEach(function(k){if(o[k]!==undefined&&$(k))$(k).value=o[k];});}catch(e){}}
CFG_KEYS.forEach(function(k){var e=$(k);if(e)e.addEventListener("input",saveCfg);});
loadCfg();
if(localStorage.getItem("soundEnabled")==="0")$("soundEnabled").checked=false;
$("soundEnabled").onchange=function(){localStorage.setItem("soundEnabled",this.checked?"1":"0");};
$("toggleCfg").onclick=function(){var b=$("cfgBody");var h=b.classList.toggle("hidden");$("toggleCfg").textContent=h?"Развернуть":"Свернуть";};

function getCfg(){return{
min24hVolumeUSDT:+$("min24hVolumeUSDT").value,
topN:+$("topN").value,maxSpreadPct:+$("maxSpreadPct").value,
perTradeRiskPct:+$("perTradeRiskPct").value,dailyRiskPct:+$("dailyRiskPct").value,
maxLeverage:+$("maxLeverage").value,maxConcurrentPositions:+$("maxConcurrentPositions").value,
maxTradesPerDay:+$("maxTradesPerDay").value,
stopLossPct:+$("stopLossPct").value,takeProfitPct:+$("takeProfitPct").value,
breakevenTriggerPct:+$("breakevenTriggerPct").value,breakevenOffsetPct:+$("breakevenOffsetPct").value,
trailingEnabled:+$("trailingEnabled").value===1,trailingDistancePct:+$("trailingDistancePct").value,
maxPositionMinutes:+$("maxPositionMinutes").value,cooldownMinutes:+$("cooldownMinutes").value,
blacklistThresholdUSDT:+$("blacklistThresholdUSDT").value,
blacklistPeriodHours:+$("blacklistPeriodHours").value,
blacklistLookback:+$("blacklistLookback").value,
exclude:$("exclude").value.split(",").map(function(s){return s.trim()}).filter(Boolean)};}

$("startBtn").onclick=async function(){
  saveCfg();
  var body={apiKey:$("apiKey").value.trim(),apiSecret:$("apiSecret").value.trim(),config:getCfg()};
  try{
    var r=await fetch("/api/start",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
    var j=await r.json();
    if(!j.ok)alert("Ошибка: "+(j.error||"?"));
    else $("cfgBody").classList.add("hidden");
  }catch(e){alert("Сеть: "+e.message);}
};
$("stopBtn").onclick=function(){fetch("/api/stop",{method:"POST"});};
$("restartBtn").onclick=async function(){if(!confirm("Перезапустить?"))return;await fetch("/api/restart",{method:"POST"});};
$("refreshBalBtn").onclick=async function(){await fetch("/api/balance",{method:"POST"});refreshState();};
$("exportCsvBtn").onclick=function(){window.location.href="/api/export-csv";};

function fmt(n,d){d=d||4;if(n==null||isNaN(n))return"-";return Number(n).toFixed(d);}
function money(n){return (n>=0?"+":"")+fmt(n,4);}
function cls(n){return n>0?"pos":n<0?"neg":"neu";}

var audioCtx=null;
function playSound(type){
  if(!$("soundEnabled").checked)return;
  try{
    if(!audioCtx)audioCtx=new (window.AudioContext||window.webkitAudioContext)();
    var o=audioCtx.createOscillator();
    var g=audioCtx.createGain();
    o.connect(g);g.connect(audioCtx.destination);
    if(type==="win"){o.frequency.value=880;}
    else if(type==="loss"){o.frequency.value=330;}
    else{o.frequency.value=550;}
    g.gain.value=0.08;
    o.start();
    setTimeout(function(){o.stop();},150);
  }catch(e){}
}

document.querySelectorAll(".tab").forEach(function(btn){
  btn.onclick=function(){
    document.querySelectorAll(".tab").forEach(function(b){b.classList.remove("active")});
    document.querySelectorAll("#tab-main,#tab-params,#tab-stats").forEach(function(c){c.classList.add("hidden")});
    btn.classList.add("active");
    $("tab-"+btn.dataset.tab).classList.remove("hidden");
    if(btn.dataset.tab==="stats")loadSymbolList();
    if(btn.dataset.tab==="params")loadParams();
  };
});

var lastTradeCount=0;

async function refreshState(){
  try{
    var s=await(await fetch("/api/state")).json();
    $("status").textContent=s.running?"Работает":"Остановлен";
    $("status").className="badge "+(s.running?"on":"off");
    if(s.running&&s.startedAt){var sec=Math.floor((Date.now()-s.startedAt)/1000);var h=Math.floor(sec/3600),m=Math.floor(sec%3600/60);$("uptime").textContent=h+"h "+m+"m";}else{$("uptime").textContent="-";}
    $("instCount").textContent=s.instrumentsCount||0;
    $("blCount").textContent=s.blacklistCount||0;
    $("cdCount").textContent=s.cooldownCount||0;
    $("bToday").textContent=s.todayTrades||0;
    var b=s.balanceDetail||{};
    $("bWallet").textContent=fmt(b.wallet,2);
    $("bAvail").textContent=fmt(b.available,2);
    $("bUpnl").textContent=money(b.upnl||0);$("bUpnl").className=cls(b.upnl||0);
    var dp=s.dailyPnl||0;$("bDaily").textContent=money(dp);$("bDaily").className=cls(dp);
    $("posCount").textContent=s.positions.length;
    $("posMax").textContent=s.config.maxConcurrentPositions;
    if(s.positions.length===0){$("positions").innerHTML="<div class=\\"muted\\">Нет открытых позиций</div>";}
    else{
      var h="";
      s.positions.forEach(function(p){
        var tCls=p.side==="LONG"?"long":"short";
        var uCls=p.upnl>0?"pos":p.upnl<0?"neg":"neu";
        var beTag=p.beMoved?" <span class=\\"tag be\\">BE</span>":"";
        var restTag=p.restored?" <span class=\\"tag time\\">R</span>":"";
        var notional=p.notional||(p.entry*p.qty);
        var margin=p.margin||(notional/(s.config.maxLeverage||1));
        var changePct=(p.mark&&p.entry)?((p.side==="LONG"?(p.mark-p.entry):(p.entry-p.mark))/p.entry*100):0;
        h+="<div class=\\"pos-card "+tCls+"\\">"+
          "<div class=\\"pos-head\\">"+
            "<div class=\\"pos-sym\\"><span class=\\"sym-name\\">"+p.symbol+"</span> <span class=\\"tag "+tCls+"\\">"+p.side+"</span>"+beTag+restTag+"</div>"+
            "<div class=\\"pos-pnl "+uCls+"\\">"+money(p.upnl)+" USDT <span class=\\"pnl-pct\\">("+fmt(changePct,2)+"%)</span></div>"+
          "</div>"+
          "<div class=\\"pos-body\\">"+
            "<div class=\\"pos-cell\\"><div class=\\"lbl\\">Размер позиции</div><div class=\\"val\\">"+fmt(notional,2)+" USDT</div><div class=\\"sub\\">маржа "+fmt(margin,2)+" USDT x"+s.config.maxLeverage+"</div></div>"+
            "<div class=\\"pos-cell\\"><div class=\\"lbl\\">Вход к Метке</div><div class=\\"val\\">"+p.entry+" к "+fmt(p.mark)+"</div><div class=\\"sub\\">"+(changePct>=0?"+":"")+fmt(changePct,2)+"%</div></div>"+
            "<div class=\\"pos-cell\\"><div class=\\"lbl\\">Кол-во</div><div class=\\"val\\">"+p.qty+"</div><div class=\\"sub\\">шт</div></div>"+
            "<div class=\\"pos-cell\\"><div class=\\"lbl\\">TP / SL</div><div class=\\"val\\"><span class=\\"pos\\">"+fmt(p.tp)+"</span> / <span class=\\"neg\\">"+fmt(p.sl)+"</span></div><div class=\\"sub\\">TP "+(p.tpPct||"?")+"% SL "+(p.slPct||"?")+"%</div></div>"+
          "</div>"+
        "</div>";
      });
      $("positions").innerHTML=h;
    }
    var rows="";
    (s.scan||[]).forEach(function(x){
      var sig=x.metrics?x.metrics.action:"-";
      var tag=sig==="LONG"?"long":sig==="SHORT"?"short":"hold";
      var pc=x.changePct>=0?"pos":"neg";
      var statusTag="";
      if(x.blacklisted)statusTag='<span class="tag blk">BLK</span>';
      else if(x.cooldown)statusTag='<span class="tag cool">CD</span>';
      else if(x.hasPosition)statusTag='<span class="tag long">POS</span>';
      else statusTag='<span class="muted">-</span>';
      rows+="<tr><td><b>"+x.symbol+"</b></td><td>"+fmt(x.last)+"</td><td class=\\""+pc+"\\">"+(x.changePct>=0?"+":"")+fmt(x.changePct,2)+"%</td><td>"+fmt(x.vol24/1e6,1)+"M</td><td>"+fmt(x.rangePct,2)+"%</td><td>"+x.score+"</td><td><span class=\\"tag "+tag+"\\">"+sig+"</span></td><td>"+statusTag+"</td></tr>";
    });
    $("scanBody").innerHTML=rows||"<tr><td colspan=\\"8\\" class=\\"muted\\">Ожидание данных...</td></tr>";
    $("log").textContent=(s.log||[]).join("\\n");
  }catch(e){}
}

async function refreshStats(){
  try{
    var st=await(await fetch("/api/stats")).json();
    if(lastTradeCount>0&&st.total>lastTradeCount){
      var lastTrade=await(await fetch("/api/trades")).json();
      if(lastTrade.length>0)playSound(lastTrade[0].pnl>=0?"win":"loss");
    }
    lastTradeCount=st.total;
    $("sTotal").textContent=st.total;
    $("sWR").textContent=st.winRate+"%";
    $("sPnl").textContent=st.totalPnl;$("sPnl").className="v "+cls(+st.totalPnl);
    $("sPF").textContent=st.profitFactor;
    $("sAvgW").textContent=st.avgWin;
    $("sAvgL").textContent=st.avgLoss;
  }catch(e){}
}

function paramStr(t){
  var sl=t.slPct!=null?t.slPct:"?";
  var tp=t.tpPct!=null?t.tpPct:"?";
  var be=t.bePct!=null?t.bePct:"?";
  return sl+"/"+tp+"/"+be;
}
function reasonTag(r){
  var m={"TP":"long","SL":"short","BE":"be","TRAIL":"trail","TIME":"time"};
  return '<span class="tag '+(m[r]||"hold")+'">'+r+'</span>';
}

async function refreshTrades(){
  try{
    var tr=await(await fetch("/api/trades")).json();
    var h="";
    tr.slice(0,20).forEach(function(t){
      var tCls=t.side==="LONG"?"long":"short";
      h+="<tr><td>"+new Date(t.ts).toLocaleTimeString()+"</td><td><b>"+t.symbol+"</b></td><td><span class=\\"tag "+tCls+"\\">"+t.side+"</span></td><td>"+paramStr(t)+"</td><td>"+t.entry+"</td><td>"+t.exit+"</td><td class=\\""+cls(t.pnl)+"\\">"+money(t.pnl)+"</td><td>"+reasonTag(t.close_reason)+"</td></tr>";
    });
    $("tradesBody").innerHTML=h||"<tr><td colspan=\\"8\\" class=\\"muted\\">Сделок пока нет</td></tr>";
  }catch(e){}
}

async function loadParams(){
  try{
    var r=await(await fetch("/api/param-groups")).json();
    var batchSize=r.batchSize||100;
    var groups=r.groups||[];
    var currentKey=r.currentKey;

    var cur=null;
    for(var i=0;i<groups.length;i++)if(groups[i].key===currentKey)cur=groups[i];
    if(!cur){
      $("currentBatch").innerHTML='<div class="muted">Сделок с текущими параметрами ещё нет.</div>';
    }else{
      var pct=Math.min(100,cur.progress/batchSize*100);
      var done=cur.ready;
      var html='<div class="pg-head"><div class="pg-title">SL <b>'+cur.slPct+'%</b> TP <b>'+cur.tpPct+'%</b> BE <b>'+cur.bePct+'%</b> Смещ <b>'+cur.beOffsetPct+'%</b></div>';
      if(done)html+='<span class="tag ready">ГОТОВА</span>';
      html+='</div>';
      html+='<div class="pg-progress"><div class="pg-progress-bar'+(done?" done":"")+'" style="width:'+pct+'%"></div></div>';
      html+='<div class="pg-count"><b>'+cur.total+'</b> / '+batchSize+' сделок ('+pct.toFixed(0)+'%)</div>';
      if(cur.total>0){
        html+='<div class="pg-metrics">';
        html+='<div class="pg-m"><div class="l">Win Rate</div><div class="v">'+cur.winRate+'%</div></div>';
        html+='<div class="pg-m"><div class="l">PnL</div><div class="v '+cls(cur.totalPnl)+'">'+money(cur.totalPnl)+'</div></div>';
        html+='<div class="pg-m"><div class="l">Profit Factor</div><div class="v">'+cur.profitFactor+'</div></div>';
        html+='<div class="pg-m"><div class="l">Max Drawdown</div><div class="v neg">'+fmt(cur.maxDrawdown,4)+'</div></div>';
        html+='</div>';
        html+='<div class="pg-tags"><span class="pg-tag">TP: <b>'+cur.tpHits+'</b></span><span class="pg-tag">SL: <b>'+cur.slHits+'</b></span><span class="pg-tag">BE: <b>'+cur.beHits+'</b></span><span class="pg-tag">TRAIL: <b>'+cur.trailHits+'</b></span><span class="pg-tag">TIME: <b>'+cur.timeHits+'</b></span></div>';
      }
      $("currentBatch").innerHTML=html;
    }

    var ready=groups.filter(function(g){return g.ready;}).sort(function(a,b){return b.totalPnl-a.totalPnl;});
    $("readyCount").textContent=ready.length;
    if(ready.length===0){
      $("readyGroups").innerHTML='<div class="muted">Пока ни одна партия не набрала '+batchSize+' сделок.</div>';
    }else{
      var h="";
      ready.forEach(function(g,i){
        var best=i===0?" best":"";
        h+='<div class="pg-card ready'+best+'">';
        h+='<div class="pg-head"><div class="pg-title">SL <b>'+g.slPct+'%</b> TP <b>'+g.tpPct+'%</b> BE <b>'+g.bePct+'%</b><br><span class="p">Смещение SL: '+g.beOffsetPct+'%</span></div>';
        if(i===0)h+='<div class="pg-crown">#1</div>';
        h+='</div>';
        h+='<div class="pg-count"><b>'+g.total+'</b> сделок</div>';
        h+='<div class="pg-metrics">';
        h+='<div class="pg-m"><div class="l">PnL</div><div class="v big '+cls(g.totalPnl)+'">'+money(g.totalPnl)+'</div></div>';
        h+='<div class="pg-m"><div class="l">Win Rate</div><div class="v">'+g.winRate+'%</div></div>';
        h+='<div class="pg-m"><div class="l">Profit Factor</div><div class="v">'+g.profitFactor+'</div></div>';
        h+='<div class="pg-m"><div class="l">Max DD</div><div class="v neg">'+fmt(g.maxDrawdown,4)+'</div></div>';
        h+='<div class="pg-m"><div class="l">Ср. Win</div><div class="v pos">'+fmt(g.avgWin,4)+'</div></div>';
        h+='<div class="pg-m"><div class="l">Ср. Loss</div><div class="v neg">'+fmt(g.avgLoss,4)+'</div></div>';
        h+='</div>';
        h+='<div class="pg-tags"><span class="pg-tag">TP: <b>'+g.tpHits+'</b></span><span class="pg-tag">SL: <b>'+g.slHits+'</b></span><span class="pg-tag">BE: <b>'+g.beHits+'</b></span><span class="pg-tag">TRAIL: <b>'+g.trailHits+'</b></span><span class="pg-tag">TIME: <b>'+g.timeHits+'</b></span></div>';
        h+='</div>';
      });
      $("readyGroups").innerHTML=h;
    }

    var pending=groups.filter(function(g){return !g.ready&&g.total>0;}).sort(function(a,b){return b.total-a.total;});
    if(pending.length===0){
      $("pendingGroups").innerHTML='<div class="muted">Нет активных партий</div>';
    }else{
      var h2="";
      pending.forEach(function(g){
        var pct=Math.min(100,g.progress/batchSize*100);
        h2+='<div class="pg-card">';
        h2+='<div class="pg-head"><div class="pg-title">SL <b>'+g.slPct+'%</b> TP <b>'+g.tpPct+'%</b> BE <b>'+g.bePct+'%</b></div><span class="tag wait">'+g.total+'/'+batchSize+'</span></div>';
        h2+='<div class="pg-progress"><div class="pg-progress-bar" style="width:'+pct+'%"></div></div>';
        h2+='<div class="pg-metrics">';
        h2+='<div class="pg-m"><div class="l">PnL</div><div class="v '+cls(g.totalPnl)+'">'+money(g.totalPnl)+'</div></div>';
        h2+='<div class="pg-m"><div class="l">Win Rate</div><div class="v">'+g.winRate+'%</div></div>';
        h2+='</div>';
        h2+='</div>';
      });
      $("pendingGroups").innerHTML=h2;
    }

    var allSorted=groups.slice().sort(function(a,b){return b.totalPnl-a.totalPnl;});
    var h3="";
    allSorted.forEach(function(g){
      h3+='<tr><td>'+g.slPct+'</td><td>'+g.tpPct+'</td><td>'+g.bePct+'</td><td>'+g.beOffsetPct+'</td><td>'+g.total+'</td><td>'+g.winRate+'%</td><td class="'+cls(g.totalPnl)+'">'+money(g.totalPnl)+'</td><td>'+g.profitFactor+'</td><td class="neg">'+fmt(g.maxDrawdown,4)+'</td><td>'+g.tpHits+' / '+g.slHits+' / '+g.beHits+' / '+g.trailHits+' / '+g.timeHits+'</td></tr>';
    });
    $("paramTableBody").innerHTML=h3||'<tr><td colspan="10" class="muted">Нет данных</td></tr>';
  }catch(e){console.error(e)}
}
$("reloadParamsBtn").onclick=loadParams;

var chartInstance=null;
async function loadSymbolList(){
  try{
    var r=await(await fetch("/api/symbols")).json();
    var sel=$("statsSymbol");
    var cur=sel.value;
    var html='<option value="">- выберите -</option>';
    (r.symbols||[]).forEach(function(s){html+='<option value="'+s.symbol+'">'+s.symbol+' ('+s.count+')</option>';});
    sel.innerHTML=html;
    var saved=localStorage.getItem("bybitStatsSymbol");
    if(saved&&r.symbols.some(function(s){return s.symbol===saved}))sel.value=saved;
    else if(cur)sel.value=cur;
  }catch(e){}
}

async function loadChart(){
  var symbol=$("statsSymbol").value;
  if(!symbol){$("chartInfo").textContent="Выберите монету";return;}
  var interval=$("statsInterval").value;
  var limit=$("statsLimit").value;
  localStorage.setItem("bybitStatsSymbol",symbol);
  $("chartInfo").textContent="Загрузка...";

  var klRes=await(await fetch("/api/klines?symbol="+symbol+"&interval="+interval+"&limit="+limit)).json();
  var trRes=await(await fetch("/api/trades-by-symbol?symbol="+symbol)).json();
  var klines=klRes.klines||[];
  var trades=trRes.trades||[];
  var summary=trRes.summary||{};

  $("ssTotal").textContent=summary.total||0;
  $("ssWR").textContent=(summary.winRate||0)+"%";
  $("ssPnl").textContent=summary.totalPnl||0;
  $("ssPnl").className="v "+cls(+(summary.totalPnl||0));
  $("ssPF").textContent=summary.profitFactor||0;

  var h="";
  trades.slice().reverse().forEach(function(t){
    var tCls=t.side==="LONG"?"long":"short";
    h+="<tr><td>"+new Date(t.ts).toLocaleString()+"</td><td><span class=\\"tag "+tCls+"\\">"+t.side+"</span></td><td>"+paramStr(t)+"</td><td>"+t.entry+"</td><td>"+t.exit+"</td><td class=\\""+cls(t.pnl)+"\\">"+money(t.pnl)+"</td><td class=\\""+cls(t.pnl)+"\\">"+fmt(t.pnl_pct,2)+"%</td><td>"+reasonTag(t.close_reason)+"</td></tr>";
  });
  $("symbolTradesBody").innerHTML=h||"<tr><td colspan=\\"8\\" class=\\"muted\\">Нет сделок</td></tr>";
  if(klines.length===0){$("chartInfo").textContent="Нет данных графика";return;}

  var labels=klines.map(function(k){
    var d=new Date(k.ts);
    if(interval==="60")return d.toLocaleDateString("ru-RU",{day:"2-digit",month:"2-digit"})+" "+d.getHours()+":00";
    return d.toLocaleTimeString("ru-RU",{hour:"2-digit",minute:"2-digit"});
  });
  var prices=klines.map(function(k){return k.close});
  var entryData=new Array(klines.length).fill(null);
  var exitWin=new Array(klines.length).fill(null);
  var exitLoss=new Array(klines.length).fill(null);
  var exitBE=new Array(klines.length).fill(null);
  var exitTrail=new Array(klines.length).fill(null);
  var exitTime=new Array(klines.length).fill(null);

  var minTs=klines[0].ts, maxTs=klines[klines.length-1].ts;
  function findNearest(ts){
    var best=-1,bestDiff=Infinity;
    for(var i=0;i<klines.length;i++){
      var d=Math.abs(klines[i].ts-ts);
      if(d<bestDiff){bestDiff=d;best=i;}
    }
    return best;
  }
  var inRange=0;
  trades.forEach(function(t){
    var openTs=t.openedAt||t.ts;
    var closeTs=t.ts;
    if(closeTs<minTs||openTs>maxTs)return;
    inRange++;
    var iOpen=findNearest(openTs);
    if(iOpen>=0)entryData[iOpen]=t.entry;
    var iClose=findNearest(closeTs);
    if(iClose>=0){
      if(t.close_reason==="TP")exitWin[iClose]=t.exit;
      else if(t.close_reason==="BE")exitBE[iClose]=t.exit;
      else if(t.close_reason==="TRAIL")exitTrail[iClose]=t.exit;
      else if(t.close_reason==="TIME")exitTime[iClose]=t.exit;
      else exitLoss[iClose]=t.exit;
    }
  });
  $("chartInfo").textContent=inRange+" из "+trades.length+" сделок";

  var ctx=$("chart").getContext("2d");
  if(chartInstance)chartInstance.destroy();
  chartInstance=new Chart(ctx,{
    type:"line",
    data:{labels:labels,datasets:[
      {label:"Цена",data:prices,borderColor:"#60a5fa",backgroundColor:"rgba(96,165,250,0.06)",borderWidth:1.5,pointRadius:0,tension:0.1,fill:true},
      {label:"Вход",data:entryData,borderColor:"#fbbf24",backgroundColor:"#fbbf24",pointRadius:6,pointStyle:"triangle",showLine:false,spanGaps:false},
      {label:"TP",data:exitWin,borderColor:"#4ade80",backgroundColor:"#4ade80",pointRadius:6,pointStyle:"circle",showLine:false,spanGaps:false},
      {label:"SL",data:exitLoss,borderColor:"#f87171",backgroundColor:"#f87171",pointRadius:6,pointStyle:"rectRot",showLine:false,spanGaps:false},
      {label:"BE",data:exitBE,borderColor:"#fbbf24",backgroundColor:"#fbbf24",pointRadius:6,pointStyle:"crossRot",showLine:false,spanGaps:false},
      {label:"TRAIL",data:exitTrail,borderColor:"#fdba74",backgroundColor:"#fdba74",pointRadius:6,pointStyle:"cross",showLine:false,spanGaps:false},
      {label:"TIME",data:exitTime,borderColor:"#93c5fd",backgroundColor:"#93c5fd",pointRadius:6,pointStyle:"star",showLine:false,spanGaps:false}
    ]},
    options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},
      plugins:{legend:{labels:{color:"#e6e6e6",boxWidth:12,font:{size:11}}},
      tooltip:{backgroundColor:"#12151c",titleColor:"#e6e6e6",bodyColor:"#e6e6e6",borderColor:"#2a3240",borderWidth:1}},
      scales:{x:{ticks:{color:"#6b7280",maxTicksLimit:12,autoSkip:true,font:{size:10}},grid:{color:"#1e2530"}},
      y:{ticks:{color:"#6b7280",font:{size:10}},grid:{color:"#1e2530"},position:"right"}}}
  });
}

$("statsSymbol").onchange=loadChart;
$("statsInterval").onchange=loadChart;
$("statsLimit").onchange=loadChart;
$("reloadChartBtn").onclick=loadChart;

setInterval(refreshState,2500);
setInterval(refreshStats,5000);
setInterval(refreshTrades,6000);
setInterval(function(){
  if(!$("tab-params").classList.contains("hidden"))loadParams();
  if(!$("tab-stats").classList.contains("hidden")&&$("statsSymbol").value)loadChart();
},15000);

refreshState();refreshStats();refreshTrades();
</script>
</body>
</html>`;

server.listen(PORT, () => {
  console.log('===========================================');
  console.log('  Bybit Altcoin Scalper v7.1');
  console.log('  http://localhost:' + PORT);
  console.log('===========================================');
});