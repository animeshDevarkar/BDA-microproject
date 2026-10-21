/* OnTime Check - customer site for the Flight Delay Analysis project.
   Reads window.SITE_DATASETS (written by src/build_website_data.py) - one entry per data
   option: International (USA, BTS) and Domestic (India, simulated) - and renders everything
   client-side, so the site runs from disk or on any static host with no server code. */
(function () {
  'use strict';

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  // ---------------------------------------------------------------- dataset choice
  const SETS = window.SITE_DATASETS || {};
  const DATASET_ORDER = ['usa', 'india'].filter(k => SETS[k]);
  const DATASET = (() => {
    try { const q = new URLSearchParams(location.search).get('region'); if (q && SETS[q]) return q; } catch (e) { /* no query */ }
    try { const v = JSON.parse(localStorage.getItem('ontime.dataset')); if (v && SETS[v]) return v; } catch (e) { /* storage blocked */ }
    return DATASET_ORDER[0];
  })();
  const D = SETS[DATASET];

  if (!D) {
    $('#view-check').innerHTML = '<div class="panel notice">Flight data is missing. Run <span class="code">python src/flight_delay_pipeline.py</span> (and <span class="code">--dataset india</span>) to create website/data/site-data-usa.js and site-data-india.js.</div>';
    return;
  }

  // ---------------------------------------------------------------- data indexes
  const rowsOf = t => t.rows.map(r => Object.fromEntries(t.cols.map((c, i) => [c, r[i]])));
  const groupBy = (rows, key) => {
    const m = new Map();
    rows.forEach(r => { if (!m.has(r[key])) m.set(r[key], []); m.get(r[key]).push(r); });
    return m;
  };
  const keyOf = (o, d) => o + '-' + d;

  const DS = D.dataset || { key: 'usa', option: 'International', country: 'USA', region_name: 'U.S.', simulated: false };
  const REGION = DS.region_name;                                    // "U.S. average" / "India average"
  const VS_REGION = REGION === 'U.S.' ? 'the U.S.' : REGION;
  const ADJ = DS.key === 'india' ? 'Indian' : 'U.S.';
  const NAMES = D.airline_names || {};
  const airlineName = code => NAMES[code] || code;
  const OVERALL = D.overall;
  const airports = rowsOf(D.airports).sort((a, b) => b.flights - a.flights);
  const airportBy = new Map(airports.map(a => [a.code, a]));
  const routes = rowsOf(D.routes);
  const routeBy = new Map(routes.map(r => [keyOf(r.origin, r.destination), r]));
  const routesFrom = groupBy(routes, 'origin');
  const routeAirlineRows = rowsOf(D.route_airlines).map(r => Object.assign(r, { route: keyOf(r.origin, r.destination) }));
  const routeAirlines = groupBy(routeAirlineRows, 'route');
  const airlineRoutes = groupBy(routeAirlineRows, 'airline');
  const hoursAt = groupBy(rowsOf(D.airport_hours), 'origin');
  const monthsAt = groupBy(rowsOf(D.airport_months), 'origin');
  const weekdaysAt = groupBy(rowsOf(D.airport_weekdays), 'origin');
  const airlines = rowsOf(D.airlines);
  const nationalMonths = rowsOf(D.months);
  const nationalHours = rowsOf(D.hours);
  const MONTHS = nationalMonths.map(m => m.month);
  const DAY = ['', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
  const MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

  // ---------------------------------------------------------------- helpers
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const fmtInt = n => Number(n).toLocaleString('en-US');
  const fmtPct = (n, d = 1) => n == null ? '–' : Number(n).toFixed(d) + '%';
  const fmtHour = h => (h % 12 === 0 ? 12 : h % 12) + (h < 12 ? ' AM' : ' PM');
  const monthLabel = (ym, short) => { const m = +ym.slice(5, 7) - 1; const n = MONTH_NAMES[m]; return short ? n.slice(0, 3) : n; };
  const monthYear = ym => monthLabel(ym, true) + ' ' + ym.slice(0, 4);
  const cityOf = code => (airportBy.get(code) || {}).city || '';
  const shortCity = code => cityOf(code).split(',')[0].split('/')[0];
  const fmtDate = iso => { if (!iso) return ''; const [y, m, d] = iso.split('-'); return `${MONTH_NAMES[+m - 1].slice(0, 3)} ${+d}, ${y}`; };

  const store = {
    // Searches and trips are kept per dataset, since airport codes differ between them.
    get(key, fallback, shared) { try { const v = localStorage.getItem('ontime.' + key + (shared ? '' : '.' + DATASET)); return v == null ? fallback : JSON.parse(v); } catch (e) { return fallback; } },
    set(key, value, shared) { try { localStorage.setItem('ontime.' + key + (shared ? '' : '.' + DATASET), JSON.stringify(value)); } catch (e) { /* storage blocked: keep working without it */ } },
  };

  let toastTimer;
  function toast(msg) {
    const t = $('#toast');
    t.textContent = msg; t.hidden = false;
    clearTimeout(toastTimer); toastTimer = setTimeout(() => { t.hidden = true; }, 2600);
  }

  function band(p) {
    if (p < 15) return { cls: 'good', label: 'Usually on time', icon: '●' };
    if (p < 25) return { cls: 'warn', label: 'Delays are common', icon: '▲' };
    return { cls: 'bad', label: 'Often late', icon: '■' };
  }
  const statusPill = p => { const b = band(p); return `<span class="status ${b.cls}"><span aria-hidden="true">${b.icon}</span>${b.label}</span>`; };

  // ---------------------------------------------------------------- estimate
  function estimate(s) {
    const route = routeBy.get(keyOf(s.from, s.to));
    if (!route) return null;
    let base = route; let baseLabel = 'All airlines on this route'; let note = '';
    if (s.airline) {
      const ra = (routeAirlines.get(keyOf(s.from, s.to)) || []).find(r => r.airline === s.airline);
      if (ra) { base = ra; baseLabel = airlineName(s.airline) + ' on this route'; }
      else note = `${airlineName(s.airline)} has fewer than ${OVERALL.min_flights.cell} flights on this route, so all airlines are used.`;
    }
    const ap = airportBy.get(s.from);
    const factors = [];
    if (ap && ap.late_pct > 0 && s.hour !== '' && s.hour != null) {
      const h = (hoursAt.get(s.from) || []).find(r => r.hour === +s.hour);
      if (h) factors.push({ label: `Leaving ${s.from} around ${fmtHour(+s.hour)}`, value: h.late_pct / ap.late_pct, detail: `${fmtPct(h.late_pct)} late vs ${fmtPct(ap.late_pct)} for the whole day` });
    }
    if (ap && ap.late_pct > 0 && s.month) {
      const m = (monthsAt.get(s.from) || []).find(r => r.month === s.month);
      if (m) factors.push({ label: `Travelling in ${monthLabel(s.month)}`, value: m.late_pct / ap.late_pct, detail: `${fmtPct(m.late_pct)} late in ${monthYear(s.month)} vs ${fmtPct(ap.late_pct)} all year` });
    }
    const raw = factors.reduce((p, f) => p * f.value, base.late_pct);
    const p = Math.max(1, Math.min(95, raw));
    return { p, base, baseLabel, note, factors, route };
  }

  // ---------------------------------------------------------------- charts
  function niceTicks(max, count = 4) {
    if (!(max > 0)) return [0, 1];
    const raw = max / count; const pow = Math.pow(10, Math.floor(Math.log10(raw)));
    const step = [1, 2, 2.5, 5, 10].map(s => s * pow).find(s => s >= raw);
    const ticks = []; for (let v = 0; v <= max + step * 0.001; v += step) ticks.push(+v.toFixed(6));
    if (ticks[ticks.length - 1] < max) ticks.push(+(ticks[ticks.length - 1] + step).toFixed(6));
    return ticks;
  }

  function attachTooltip(host, svg, W) {
    let tip = $('.tooltip', host);
    if (!tip) { tip = document.createElement('div'); tip.className = 'tooltip'; tip.hidden = true; host.appendChild(tip); }
    return {
      show(x, y, html) {
        const r = svg.getBoundingClientRect(); const k = r.width / W;
        tip.innerHTML = html; tip.hidden = false;
        const left = Math.max(70, Math.min(r.width - 70, x * k));
        tip.style.left = left + 'px'; tip.style.top = (y * k) + 'px';
      },
      hide() { tip.hidden = true; },
    };
  }

  /* Vertical columns. items: [{label, value, tip, active, key}] */
  function columnChart(host, items, opts = {}) {
    const W = 640, H = opts.height || 220, m = { t: 18, r: 8, b: 28, l: 40 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const values = items.map(d => d.value).filter(v => v != null);
    const ticks = niceTicks(Math.max(...values, opts.ref || 0, 1));
    const max = ticks[ticks.length - 1];
    const y = v => m.t + ih - (v / max) * ih;
    const band = iw / items.length, bw = Math.max(4, band * 0.68);
    const every = items.length > 16 ? 3 : items.length > 8 ? 2 : 1;
    let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(opts.label || 'Bar chart')}">`;
    ticks.forEach(t => { s += `<g class="tick"><line x1="${m.l}" x2="${W - m.r}" y1="${y(t)}" y2="${y(t)}"/><text x="${m.l - 6}" y="${y(t) + 4}" text-anchor="end">${t}%</text></g>`; });
    items.forEach((d, i) => {
      const x = m.l + band * i + (band - bw) / 2;
      if (d.value != null) {
        const top = y(d.value), h = m.t + ih - top, r = Math.min(4, h, bw / 2);
        const fill = d.active ? 'var(--series-2)' : 'var(--series-1)';
        s += `<path d="M${x},${m.t + ih} V${top + r} Q${x},${top} ${x + r},${top} H${x + bw - r} Q${x + bw},${top} ${x + bw},${top + r} V${m.t + ih} Z" fill="${fill}"/>`;
      }
      if (i % every === 0 || d.active) s += `<text x="${x + bw / 2}" y="${H - 8}" text-anchor="middle"${d.active ? ' style="fill:var(--ink);font-weight:700"' : ''}>${esc(d.short || d.label)}</text>`;
    });
    if (opts.ref != null) {
      s += `<line class="ref" x1="${m.l}" x2="${W - m.r}" y1="${y(opts.ref)}" y2="${y(opts.ref)}"/>`;
    }
    items.forEach((d, i) => { s += `<rect class="hit" data-i="${i}" x="${m.l + band * i}" y="${m.t}" width="${band}" height="${ih}" tabindex="${d.value == null ? -1 : 0}"/>`; });
    s += '</svg>';
    const legend = opts.ref != null ? `<div class="legend"><span><i style="background:var(--series-1)"></i>${esc(opts.seriesLabel || 'Late arrivals')}</span>${items.some(d => d.active) ? '<span><i style="background:var(--series-2)"></i>Your choice</span>' : ''}<span><i class="dash"></i>${esc(opts.refLabel || 'Average')}</span></div>` : '';
    host.innerHTML = `<div class="chart">${s}</div>${legend}`;
    const chart = $('.chart', host), svg = $('svg', chart), tt = attachTooltip(chart, svg, W);
    $$('.hit', svg).forEach(el => {
      const d = items[+el.dataset.i];
      if (d.value == null) return;
      const show = () => tt.show(m.l + band * (+el.dataset.i + 0.5), y(d.value), d.tip);
      el.addEventListener('mouseenter', show); el.addEventListener('focus', show);
      el.addEventListener('mouseleave', tt.hide); el.addEventListener('blur', tt.hide);
      if (opts.onPick) {
        el.addEventListener('click', () => opts.onPick(d));
        el.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); opts.onPick(d); } });
      }
    });
  }

  /* Lines over shared x labels. series: [{name, color, values:[number|null]}] */
  function lineChart(host, labels, series, opts = {}) {
    const W = 640, H = opts.height || 220, m = { t: 18, r: 12, b: 28, l: 40 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const all = series.flatMap(s => s.values).filter(v => v != null);
    const ticks = niceTicks(Math.max(...all, 1));
    const max = ticks[ticks.length - 1];
    const x = i => m.l + (labels.length === 1 ? iw / 2 : (iw * i) / (labels.length - 1));
    const y = v => m.t + ih - (v / max) * ih;
    let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(opts.label || 'Line chart')}">`;
    ticks.forEach(t => { s += `<g class="tick"><line x1="${m.l}" x2="${W - m.r}" y1="${y(t)}" y2="${y(t)}"/><text x="${m.l - 6}" y="${y(t) + 4}" text-anchor="end">${t}%</text></g>`; });
    labels.forEach((l, i) => {
      const anchor = i === 0 ? 'start' : i === labels.length - 1 ? 'end' : 'middle';
      if (labels.length <= 12 || i % 2 === 0) s += `<text x="${x(i)}" y="${H - 8}" text-anchor="${anchor}">${esc(l)}</text>`;
    });
    s += `<line class="cross" x1="0" x2="0" y1="${m.t}" y2="${m.t + ih}" stroke="var(--ink-3)" visibility="hidden"/>`;
    series.forEach(se => {
      let d = ''; let pen = false;
      se.values.forEach((v, i) => { if (v == null) { pen = false; return; } d += (pen ? 'L' : 'M') + x(i) + ',' + y(v); pen = true; });
      s += `<path d="${d}" fill="none" stroke="${se.color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"${se.dashed ? ' stroke-dasharray="5 5"' : ''}/>`;
      se.values.forEach((v, i) => { if (v != null && se.dots !== false) s += `<circle cx="${x(i)}" cy="${y(v)}" r="4" fill="${se.color}" stroke="var(--surface)" stroke-width="2"/>`; });
    });
    const step = labels.length > 1 ? iw / (labels.length - 1) : iw;
    labels.forEach((l, i) => { s += `<rect class="hit" data-i="${i}" x="${x(i) - step / 2}" y="${m.t}" width="${step}" height="${ih}" tabindex="0"/>`; });
    s += '</svg>';
    host.innerHTML = `<div class="chart">${s}</div><div class="legend">${series.map(se => `<span><i style="background:${se.color}"></i>${esc(se.name)}</span>`).join('')}</div>`;
    const chart = $('.chart', host), svg = $('svg', chart), tt = attachTooltip(chart, svg, W), cross = $('.cross', svg);
    $$('.hit', svg).forEach(el => {
      const i = +el.dataset.i;
      const show = () => {
        const top = Math.min(...series.map(se => se.values[i]).filter(v => v != null).map(y), m.t + ih);
        cross.setAttribute('x1', x(i)); cross.setAttribute('x2', x(i)); cross.setAttribute('visibility', 'visible');
        tt.show(x(i), top, `<b>${esc(opts.tipTitle ? opts.tipTitle(i) : labels[i])}</b><br>` + series.map(se => `${esc(se.name)}: ${fmtPct(se.values[i])}`).join('<br>'));
      };
      const hide = () => { tt.hide(); cross.setAttribute('visibility', 'hidden'); };
      el.addEventListener('mouseenter', show); el.addEventListener('focus', show);
      el.addEventListener('mouseleave', hide); el.addEventListener('blur', hide);
    });
  }

  /* Ranked horizontal bars; rows: [{key, name, sub, value, active}] */
  function barList(rows, { max, fmt = v => fmtPct(v), clickable } = {}) {
    const top = max || Math.max(...rows.map(r => r.value), 1);
    const tag = clickable ? 'button' : 'div';
    return `<div class="bars">${rows.map(r => `
      <${tag} class="bar-row${r.active ? ' active' : ''}" ${clickable ? `type="button" data-key="${esc(r.key)}" aria-pressed="${r.active ? 'true' : 'false'}"` : ''}>
        <span class="name">${esc(r.name)}${r.sub ? `<span class="sub">${esc(r.sub)}</span>` : ''}</span>
        <span class="track" aria-hidden="true"><i style="width:${(r.value / top) * 100}%"></i></span>
        <span class="val">${fmt(r.value)}</span>
      </${tag}>`).join('')}</div>`;
  }

  // ---------------------------------------------------------------- airport combobox
  function matchAirports(q, preferFrom) {
    q = q.trim().toUpperCase();
    const served = preferFrom ? new Set((routesFrom.get(preferFrom) || []).map(r => r.destination)) : null;
    let list = airports;
    if (q) {
      list = airports.map(a => {
        const city = (a.city || '').toUpperCase();
        let score = a.code === q ? 0 : a.code.startsWith(q) ? 1 : city.startsWith(q) ? 2 : city.includes(q) ? 3 : 9;
        return { a, score };
      }).filter(x => x.score < 9).sort((x, y) => x.score - y.score || y.a.flights - x.a.flights).map(x => x.a);
    }
    if (served) list = list.filter(a => served.has(a.code)).concat(q ? list.filter(a => !served.has(a.code)) : []);
    return list.slice(0, 12).map(a => ({ a, nonstop: served ? served.has(a.code) : null }));
  }

  function combo(input, onPick, preferFrom) {
    const list = document.getElementById(input.getAttribute('aria-controls'));
    let items = [], active = -1;
    const close = () => { list.hidden = true; input.setAttribute('aria-expanded', 'false'); active = -1; };
    const render = () => {
      items = matchAirports(input.value, preferFrom());
      if (!items.length) { list.innerHTML = '<li aria-disabled="true">No airport matches. Try a 3-letter code.</li>'; }
      else list.innerHTML = items.map((x, i) => `<li role="option" id="${list.id}-${i}" data-code="${x.a.code}" aria-selected="${i === active}"><span class="code">${x.a.code}</span><span>${esc(x.a.city)}</span>${x.nonstop === true ? '<small>nonstop</small>' : x.nonstop === false ? '<small>no nonstop</small>' : ''}</li>`).join('');
      list.hidden = false; input.setAttribute('aria-expanded', 'true');
      input.setAttribute('aria-activedescendant', active >= 0 ? `${list.id}-${active}` : '');
    };
    const pick = code => { const a = airportBy.get(code); if (!a) return; input.value = `${a.code} · ${shortCity(a.code)}`; input.dataset.code = a.code; close(); onPick(a.code); };
    input.addEventListener('focus', () => { input.select(); render(); });
    input.addEventListener('input', () => { delete input.dataset.code; active = -1; render(); });
    input.addEventListener('keydown', e => {
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault(); if (list.hidden) render();
        active = Math.max(0, Math.min(items.length - 1, active + (e.key === 'ArrowDown' ? 1 : -1))); render();
      } else if (e.key === 'Enter') {
        if (!list.hidden && items.length) { e.preventDefault(); pick(items[Math.max(active, 0)].a.code); }
      } else if (e.key === 'Escape') close();
    });
    input.addEventListener('blur', () => setTimeout(() => {
      close();
      if (!input.dataset.code) { const m = matchAirports(input.value)[0]; if (input.value.trim() && m) pick(m.a.code); }
    }, 150));
    list.addEventListener('mousedown', e => { const li = e.target.closest('li[data-code]'); if (li) { e.preventDefault(); pick(li.dataset.code); } });
    return { set(code) { const a = airportBy.get(code); input.value = a ? `${a.code} · ${shortCity(a.code)}` : ''; if (a) input.dataset.code = a.code; else delete input.dataset.code; } };
  }

  // ---------------------------------------------------------------- state
  const state = Object.assign({ from: '', to: '', airline: '', hour: '', month: '' }, store.get('last', {}));
  let airportView = null; // airport code shown on the Airports tab
  let airlineMetric = 'late_pct';

  (function readQuery() {
    try {
      const q = new URLSearchParams(location.search);
      if (q.get('from') && q.get('to')) {
        ['from', 'to', 'airline'].forEach(k => { state[k] = (q.get(k) || '').toUpperCase(); });
        state.hour = /^\d{1,2}$/.test(q.get('hour') || '') && +q.get('hour') < 24 ? q.get('hour') : '';
        state.month = MONTHS.includes(q.get('month')) ? q.get('month') : '';
      }
    } catch (e) { /* no query string available */ }
  })();
  if (!routeBy.has(keyOf(state.from, state.to))) {
    const busiest = routes.slice().sort((a, b) => b.flights - a.flights)[0];
    Object.assign(state, { from: busiest.origin, to: busiest.destination, airline: '', hour: '', month: '' });
  }

  const fromBox = combo($('#from'), code => { state.from = code; fillAirlines(); }, () => null);
  const toBox = combo($('#to'), code => { state.to = code; fillAirlines(); }, () => state.from);

  function fillAirlines() {
    const sel = $('#airline');
    const list = (routeAirlines.get(keyOf(state.from, state.to)) || []).slice().sort((a, b) => b.flights - a.flights);
    sel.innerHTML = '<option value="">Any airline</option>' + list.map(r => `<option value="${r.airline}">${esc(airlineName(r.airline))}</option>`).join('');
    sel.value = list.some(r => r.airline === state.airline) ? state.airline : '';
  }
  $('#hour').innerHTML += Array.from({ length: 24 }, (_, h) => `<option value="${h}">${fmtHour(h)} – ${fmtHour((h + 1) % 24)}</option>`).join('');
  $('#month').innerHTML += MONTHS.map(m => `<option value="${m}">${monthLabel(m)}</option>`).join('');

  function syncForm() {
    fromBox.set(state.from); toBox.set(state.to); fillAirlines();
    $('#hour').value = state.hour === '' ? '' : String(state.hour);
    $('#month').value = state.month || '';
  }

  function runSearch(s) {
    Object.assign(state, s);
    store.set('last', { from: state.from, to: state.to, airline: state.airline, hour: state.hour, month: state.month });
    syncForm(); go('check'); renderCheck();
  }

  $('#search').addEventListener('submit', e => {
    e.preventDefault();
    const from = $('#from').dataset.code || (matchAirports($('#from').value)[0] || {}).a?.code;
    const to = $('#to').dataset.code || (matchAirports($('#to').value)[0] || {}).a?.code;
    if (!from || !to) { toast('Choose both airports from the list.'); return; }
    if (from === to) { toast('Pick two different airports.'); return; }
    runSearch({ from, to, airline: $('#airline').value, hour: $('#hour').value, month: $('#month').value });
    $('#view-check').scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
  $('#swap').addEventListener('click', () => runSearch({ from: state.to, to: state.from, airline: '' }));

  const examples = routes.slice().sort((a, b) => b.flights - a.flights).slice(0, 5);
  $('#examples').innerHTML = examples.map(r => `<button class="chip" type="button" data-o="${r.origin}" data-d="${r.destination}">${r.origin} → ${r.destination}</button>`).join(' ');
  $('#examples').addEventListener('click', e => { const b = e.target.closest('.chip'); if (b) runSearch({ from: b.dataset.o, to: b.dataset.d, airline: '', hour: '', month: '' }); });

  // ---------------------------------------------------------------- saved trips
  const tripsKey = 'trips';
  const loadTrips = () => { const t = store.get(tripsKey, []); return Array.isArray(t) ? t : []; };
  function saveTrips(list) { store.set(tripsKey, list); updateTripCount(); }
  function updateTripCount() { const n = loadTrips().length; const b = $('#trip-count'); b.textContent = n; b.hidden = n === 0; }
  const tripId = s => [s.from, s.to, s.airline, s.hour, s.month].join('|');

  // ---------------------------------------------------------------- views
  function renderCheck() {
    const host = $('#view-check');
    const s = state;
    const from = airportBy.get(s.from), to = airportBy.get(s.to);
    if (!from || !to) { host.innerHTML = '<div class="panel">Choose a departure and arrival airport above to see the odds.</div>'; return; }
    const est = estimate(s);
    if (!est) {
      const alts = (routesFrom.get(s.from) || []).slice().sort((a, b) => b.flights - a.flights).slice(0, 12);
      host.innerHTML = `<div class="panel" style="display:grid;gap:14px">
        <div class="notice">No nonstop route ${s.from} → ${s.to} with at least ${OVERALL.min_flights.route} flights in the data. It may need a connection, or the route is too small to quote.</div>
        <h3>Nonstop routes from ${esc(s.from)} · ${esc(shortCity(s.from))}</h3>
        ${alts.length ? barList(alts.map(r => ({ key: r.destination, name: `${r.destination} · ${shortCity(r.destination)}`, sub: `${fmtInt(r.flights)} flights`, value: r.late_pct })), { clickable: true }) : '<p class="muted">No routes from this airport meet the minimum.</p>'}
      </div>`;
      $$('.bar-row', host).forEach(b => b.addEventListener('click', () => runSearch({ to: b.dataset.key, airline: '' })));
      return;
    }
    const b = est.base;
    const bd = band(est.p);
    const nat = OVERALL.late_pct;
    const saved = loadTrips().some(t => tripId(t) === tripId(s));
    const onTime = 100 - est.p;
    const whenParts = [s.airline ? airlineName(s.airline) : 'Any airline', s.hour !== '' ? 'around ' + fmtHour(+s.hour) : 'any time', s.month ? 'in ' + monthLabel(s.month) : 'any month'];

    host.innerHTML = `
      <article class="panel pass" aria-label="Delay odds for ${s.from} to ${s.to}">
        <div class="pass-main">
          <div class="route-line">
            <div class="ap"><span class="code">${s.from}</span><span>${esc(from.city)}</span></div>
            <div class="plane" aria-hidden="true"><svg width="26" height="26" viewBox="0 0 24 24" fill="currentColor" style="transform:translate(-50%,-50%) rotate(90deg)"><path d="M21 16v-2l-8-5V3.5C13 2.67 12.33 2 11.5 2S10 2.67 10 3.5V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5l8 2.5z"/></svg></div>
            <div class="ap" style="text-align:right"><span class="code">${s.to}</span><span>${esc(to.city)}</span></div>
          </div>
          <dl class="meta-row">
            <div><dt>Airline</dt><dd>${esc(whenParts[0])}</dd></div>
            <div><dt>Departure</dt><dd>${esc(whenParts[1])}</dd></div>
            <div><dt>Month</dt><dd>${esc(whenParts[2])}</dd></div>
            <div><dt>Flights checked</dt><dd class="num">${fmtInt(b.flights)}</dd></div>
          </dl>
          <div style="display:grid;gap:10px">
            <span class="label">Chance of arriving 15+ minutes late</span>
            <div class="big-figure"><span class="value num">${Math.round(est.p)}<small>%</small></span>${statusPill(est.p)}</div>
            <div class="meter" role="img" aria-label="${Math.round(est.p)} percent, ${REGION} average ${fmtPct(nat)}"><i style="width:${est.p}%"></i><b style="left:${nat}%" title="${REGION} average"></b></div>
            <p class="small muted">About ${Math.round(est.p / 10) || '<1'} in 10 flights like this arrive late. The amber mark is the ${REGION} average (${fmtPct(nat)}).</p>
          </div>
          ${est.note ? `<p class="small notice">${esc(est.note)}</p>` : ''}
          <details class="explain">
            <summary>How this number was worked out</summary>
            <ol>
              <li>${esc(est.baseLabel)}: <b>${fmtPct(b.late_pct)}</b> of ${fmtInt(b.flights)} flights arrived 15+ min late.</li>
              ${est.factors.map(f => `<li>${esc(f.label)}: × ${f.value.toFixed(2)} (${esc(f.detail)})</li>`).join('')}
              <li>Result: <b>${est.p.toFixed(1)}%</b>${est.factors.length ? ' (kept between 1% and 95%)' : ''}.</li>
            </ol>
          </details>
        </div>
        <aside class="pass-stub">
          <span class="label">At a glance</span>
          <div class="stat-list">
            <div><span>Arrives on time</span><strong>${Math.round(onTime)}%</strong></div>
            <div><span>Cancelled</span><strong>${fmtPct(b.cancel_pct)}</strong></div>
            <div><span>When late, typical delay</span><strong>${b.late_avg_min != null ? b.late_avg_min + ' min' : '–'}</strong></div>
            <div><span>${REGION} average late</span><strong>${fmtPct(nat)}</strong></div>
          </div>
          <div class="actions">
            <button class="btn primary small" type="button" id="save-trip" aria-pressed="${saved}">${saved ? 'Saved to My trips' : 'Save trip'}</button>
            <button class="btn small" type="button" id="share-trip">Copy link</button>
            <button class="btn small ghost" type="button" id="swap-trip">Return trip</button>
          </div>
        </aside>
      </article>
      <div class="grid-2">
        <section class="panel">
          <div class="panel-head"><h3>Airlines on this route</h3><p>Share of flights 15+ min late. Tap one to use it.</p></div>
          <div id="route-airlines"></div>
        </section>
        <section class="panel">
          <div class="panel-head"><h3>Best time to leave ${esc(s.from)}</h3><p>Late arrivals by scheduled departure hour. Tap a bar to use it.</p></div>
          <div id="hour-chart"></div>
          <p class="small muted" id="hour-tip"></p>
        </section>
      </div>
      <div class="grid-2">
        <section class="panel">
          <div class="panel-head"><h3>Month by month at ${esc(s.from)}</h3><p>Late arrivals for flights leaving ${esc(s.from)} vs ${VS_REGION}</p></div>
          <div id="month-chart"></div>
        </section>
        <section class="panel">
          <div class="panel-head"><h3>Day of the week at ${esc(s.from)}</h3><p>Late arrivals by day.</p></div>
          <div id="weekday-chart"></div>
        </section>
      </div>`;

    // Airlines on route
    const ras = (routeAirlines.get(keyOf(s.from, s.to)) || []).slice().sort((x, y) => x.late_pct - y.late_pct);
    const raHost = $('#route-airlines');
    if (ras.length) {
      raHost.innerHTML = barList(ras.map(r => ({ key: r.airline, name: airlineName(r.airline), sub: `${fmtInt(r.flights)} flights · ${fmtPct(r.cancel_pct)} cancelled`, value: r.late_pct, active: r.airline === s.airline })), { clickable: true, max: Math.max(...ras.map(r => r.late_pct), nat) })
        + (ras.length > 1 ? `<p class="small muted" style="margin-top:8px">Most reliable here: <b>${esc(airlineName(ras[0].airline))}</b> (${fmtPct(ras[0].late_pct)} late).</p>` : '');
      $$('.bar-row', raHost).forEach(btn => btn.addEventListener('click', () => runSearch({ airline: btn.dataset.key === s.airline ? '' : btn.dataset.key })));
    } else raHost.innerHTML = `<p class="muted">No single airline has ${OVERALL.min_flights.cell}+ flights on this route.</p>`;

    // Hours
    const hrs = hoursAt.get(s.from) || [];
    const byHour = new Map(hrs.map(r => [r.hour, r]));
    const hourItems = Array.from({ length: 24 }, (_, h) => {
      const r = byHour.get(h);
      return { key: h, label: fmtHour(h), short: String(h), value: r ? r.late_pct : null, active: s.hour !== '' && +s.hour === h, tip: r ? `<b>${fmtHour(h)}</b><br>${fmtPct(r.late_pct)} late · ${fmtInt(r.flights)} flights` : '' };
    }).filter((d, i, arr) => d.value != null || (i > 0 && i < 23 && arr[i - 1].value != null && arr[i + 1].value != null));
    if (hourItems.length) {
      columnChart($('#hour-chart'), hourItems, { ref: from.late_pct, refLabel: `${s.from} average ${fmtPct(from.late_pct)}`, label: `Late arrival rate by departure hour at ${s.from}`, onPick: d => runSearch({ hour: s.hour !== '' && +s.hour === d.key ? '' : String(d.key) }) });
      const best = hrs.filter(r => r.flights >= 50).sort((a, c) => a.late_pct - c.late_pct).slice(0, 3);
      if (best.length) $('#hour-tip').innerHTML = `Lowest delay odds: ${best.map(r => `<b>${fmtHour(r.hour)}</b> (${fmtPct(r.late_pct)})`).join(', ')}. Morning flights usually run best because delays build up during the day.`;
    } else $('#hour-chart').innerHTML = '<p class="muted">Not enough flights to break down by hour.</p>';

    // Months
    const ams = new Map((monthsAt.get(s.from) || []).map(r => [r.month, r]));
    lineChart($('#month-chart'), MONTHS.map(m => monthLabel(m, true)), [
      { name: s.from, color: 'var(--series-1)', values: MONTHS.map(m => ams.has(m) ? ams.get(m).late_pct : null) },
      { name: `${REGION} average`, color: 'var(--series-2)', values: nationalMonths.map(r => r.late_pct), dashed: true, dots: false },
    ], { label: `Late arrival rate by month at ${s.from}`, tipTitle: i => monthYear(MONTHS[i]) });

    // Weekdays
    const wds = new Map((weekdaysAt.get(s.from) || []).map(r => [r.weekday, r]));
    columnChart($('#weekday-chart'), [1, 2, 3, 4, 5, 6, 7].map(d => {
      const r = wds.get(d);
      return { key: d, label: DAY[d], value: r ? r.late_pct : null, tip: r ? `<b>${DAY[d]}</b><br>${fmtPct(r.late_pct)} late · ${fmtInt(r.flights)} flights` : '' };
    }), { ref: from.late_pct, refLabel: `${s.from} average`, label: `Late arrival rate by weekday at ${s.from}`, height: 200 });

    $('#save-trip').addEventListener('click', () => {
      const list = loadTrips(); const id = tripId(s);
      if (list.some(t => tripId(t) === id)) { saveTrips(list.filter(t => tripId(t) !== id)); toast('Removed from My trips'); }
      else { list.unshift({ from: s.from, to: s.to, airline: s.airline, hour: s.hour, month: s.month, note: '', saved: new Date().toISOString().slice(0, 10) }); saveTrips(list); toast('Saved to My trips'); }
      renderCheck();
    });
    $('#share-trip').addEventListener('click', () => copyLink(s));
    $('#swap-trip').addEventListener('click', () => runSearch({ from: s.to, to: s.from, airline: '' }));
  }

  function shareUrl(s) {
    const base = location.href.split(/[?#]/)[0];
    const q = new URLSearchParams({ region: DATASET });
    ['from', 'to', 'airline', 'hour', 'month'].forEach(k => { if (s[k] !== '' && s[k] != null) q.set(k, s[k]); });
    return `${base}?${q}#check`;
  }
  function copyLink(s) {
    const url = shareUrl(s);
    const fallback = () => {
      const box = document.createElement('input'); box.value = url; box.setAttribute('aria-label', 'Link to this trip');
      box.style.cssText = 'position:fixed;left:16px;right:16px;bottom:70px;z-index:40;height:40px;padding:0 10px;border-radius:8px;border:1px solid var(--line);background:var(--surface);color:var(--ink)';
      document.body.appendChild(box); box.select();
      box.addEventListener('blur', () => box.remove());
      toast('Press Ctrl+C (or Cmd+C) to copy the link');
    };
    try { navigator.clipboard.writeText(url).then(() => toast('Link copied'), fallback); } catch (e) { fallback(); }
  }

  // Airports tab -----------------------------------------------------------
  let airportSort = { key: 'flights', dir: -1 };
  let airportQuery = '';
  let airportLimit = 40;
  function renderAirports() {
    const host = $('#view-airports');
    host.innerHTML = `${airportView ? '<div id="airport-detail" style="display:grid;gap:20px"></div>' : ''}
      <section class="panel">
        <div class="panel-head"><h2>Airports</h2><p>${fmtInt(airports.length)} ${ADJ} airports with at least ${OVERALL.min_flights.airport} departures. Pick one for details.</p></div>
        <div class="toolbar">
          <input type="search" id="airport-q" placeholder="Search code or city" aria-label="Search airports" value="${esc(airportQuery)}" style="flex:1 1 220px">
          <label class="small muted" for="airport-sort">Sort by</label>
          <select id="airport-sort">
            <option value="flights">Busiest</option>
            <option value="late_pct:1">Most on time</option>
            <option value="late_pct:-1">Most delays</option>
            <option value="cancel_pct:-1">Most cancellations</option>
          </select>
        </div>
        <div class="table-wrap"><table>
          <thead><tr><th>Airport</th><th>City</th><th class="n">Departures</th><th class="n">Arrive late</th><th class="n">Leave late</th><th class="n">Cancelled</th><th>Rating</th></tr></thead>
          <tbody id="airport-rows"></tbody>
        </table></div>
        <div style="margin-top:12px;text-align:center"><button class="btn small" type="button" id="airport-more">Show more</button></div>
      </section>`;
    const sortSel = $('#airport-sort');
    sortSel.value = airportSort.key === 'flights' ? 'flights' : `${airportSort.key}:${airportSort.dir}`;
    const fill = () => {
      const q = airportQuery.trim().toUpperCase();
      const list = airports.filter(a => !q || a.code.includes(q) || (a.city || '').toUpperCase().includes(q))
        .sort((a, b) => airportSort.key === 'flights' ? b.flights - a.flights : (a[airportSort.key] - b[airportSort.key]) * airportSort.dir || b.flights - a.flights);
      $('#airport-rows').innerHTML = list.slice(0, airportLimit).map(a => `
        <tr class="clickable${a.code === airportView ? ' active' : ''}" data-code="${a.code}" tabindex="0">
          <td class="code">${a.code}</td><td>${esc(a.city)}</td><td class="n">${fmtInt(a.flights)}</td>
          <td class="n">${fmtPct(a.late_pct)}</td><td class="n">${fmtPct(a.dep_late_pct)}</td><td class="n">${fmtPct(a.cancel_pct)}</td><td>${statusPill(a.late_pct)}</td>
        </tr>`).join('') || '<tr><td colspan="7" class="muted">No airport matches that search.</td></tr>';
      $('#airport-more').hidden = list.length <= airportLimit;
    };
    fill();
    $('#airport-q').addEventListener('input', e => { airportQuery = e.target.value; airportLimit = 40; fill(); });
    sortSel.addEventListener('change', () => { const [k, d] = sortSel.value.split(':'); airportSort = { key: k, dir: +d || -1 }; fill(); });
    $('#airport-more').addEventListener('click', () => { airportLimit += 60; fill(); });
    const open = code => { airportView = code; renderAirports(); $('#view-airports').scrollIntoView({ behavior: 'smooth', block: 'start' }); };
    $('#airport-rows').addEventListener('click', e => { const tr = e.target.closest('tr[data-code]'); if (tr) open(tr.dataset.code); });
    $('#airport-rows').addEventListener('keydown', e => { const tr = e.target.closest('tr[data-code]'); if (tr && e.key === 'Enter') open(tr.dataset.code); });
    if (airportView) renderAirportDetail(airportView);
  }

  function renderAirportDetail(code) {
    const a = airportBy.get(code); const host = $('#airport-detail');
    if (!a) return;
    const dests = (routesFrom.get(code) || []).slice().sort((x, y) => y.flights - x.flights);
    host.innerHTML = `
      <section class="panel">
        <div class="panel-head"><h2><span class="code">${code}</span> · ${esc(a.city)}</h2><span class="spacer"></span>
          <button class="btn small ghost" type="button" id="close-airport">Close</button></div>
        <div class="tiles">
          <div class="tile"><span class="label">Departures</span><strong>${fmtInt(a.flights)}</strong><span class="small muted">in the year of data</span></div>
          <div class="tile"><span class="label">Arrive 15+ min late</span><strong>${fmtPct(a.late_pct)}</strong>${statusPill(a.late_pct)}</div>
          <div class="tile"><span class="label">Cancelled</span><strong>${fmtPct(a.cancel_pct)}</strong><span class="small muted">${REGION} ${fmtPct(OVERALL.cancel_pct)}</span></div>
          <div class="tile"><span class="label">Typical delay when late</span><strong>${a.late_avg_min} min</strong><span class="small muted">${REGION} ${OVERALL.late_avg_min} min</span></div>
        </div>
      </section>
      <div class="grid-2">
        <section class="panel"><div class="panel-head"><h3>By departure hour</h3></div><div id="ad-hours"></div></section>
        <section class="panel"><div class="panel-head"><h3>By month</h3></div><div id="ad-months"></div></section>
      </div>
      <section class="panel">
        <div class="panel-head"><h3>Nonstop destinations from ${code}</h3><p>Tap one to check that trip.</p></div>
        <div class="table-wrap"><table>
          <thead><tr><th>To</th><th>City</th><th class="n">Flights</th><th class="n">Arrive late</th><th class="n">Cancelled</th><th class="n">Typical delay when late</th></tr></thead>
          <tbody id="ad-dests">${dests.slice(0, 25).map(r => `<tr class="clickable" tabindex="0" data-to="${r.destination}"><td class="code">${r.destination}</td><td>${esc(cityOf(r.destination))}</td><td class="n">${fmtInt(r.flights)}</td><td class="n">${fmtPct(r.late_pct)}</td><td class="n">${fmtPct(r.cancel_pct)}</td><td class="n">${r.late_avg_min ?? '–'} min</td></tr>`).join('') || '<tr><td colspan="6" class="muted">No routes meet the minimum.</td></tr>'}</tbody>
        </table></div>
      </section>`;
    const byHour = new Map((hoursAt.get(code) || []).map(r => [r.hour, r]));
    const items = Array.from({ length: 24 }, (_, h) => ({ key: h, label: fmtHour(h), short: String(h), value: byHour.has(h) ? byHour.get(h).late_pct : null, tip: byHour.has(h) ? `<b>${fmtHour(h)}</b><br>${fmtPct(byHour.get(h).late_pct)} late · ${fmtInt(byHour.get(h).flights)} flights` : '' }))
      .filter((d, i, arr) => d.value != null || (i > 0 && i < 23 && arr[i - 1].value != null && arr[i + 1].value != null));
    if (items.length) columnChart($('#ad-hours'), items, { ref: a.late_pct, refLabel: `${code} average`, label: `Late arrival rate by hour at ${code}` });
    const ams = new Map((monthsAt.get(code) || []).map(r => [r.month, r]));
    lineChart($('#ad-months'), MONTHS.map(m => monthLabel(m, true)), [
      { name: code, color: 'var(--series-1)', values: MONTHS.map(m => ams.has(m) ? ams.get(m).late_pct : null) },
      { name: `${REGION} average`, color: 'var(--series-2)', values: nationalMonths.map(r => r.late_pct), dashed: true, dots: false },
    ], { tipTitle: i => monthYear(MONTHS[i]) });
    $('#close-airport').addEventListener('click', () => { airportView = null; renderAirports(); });
    const pick = tr => { if (tr) runSearch({ from: code, to: tr.dataset.to, airline: '', hour: '', month: '' }); };
    $('#ad-dests').addEventListener('click', e => pick(e.target.closest('tr[data-to]')));
    $('#ad-dests').addEventListener('keydown', e => { if (e.key === 'Enter') pick(e.target.closest('tr[data-to]')); });
  }

  // Airlines tab -------------------------------------------------------------
  let airlineFocus = null;
  function renderAirlines() {
    const host = $('#view-airlines');
    const metrics = {
      late_pct: { label: 'Arrive late', fmt: v => fmtPct(v), better: 'lower' },
      cancel_pct: { label: 'Cancelled', fmt: v => fmtPct(v), better: 'lower' },
      late_avg_min: { label: 'Minutes late, when late', fmt: v => Math.round(v) + ' min', better: 'lower' },
    };
    const mt = metrics[airlineMetric];
    const sorted = airlines.slice().sort((a, b) => a[airlineMetric] - b[airlineMetric]);
    host.innerHTML = `
      <section class="panel">
        <div class="panel-head"><h2>Airlines</h2><p>All ${DS.key === 'india' ? 'Indian domestic airlines' : 'U.S. reporting carriers'}, best at the top. Pick one to see its routes.</p><span class="spacer"></span>
          <div class="seg" role="group" aria-label="Compare by">${Object.entries(metrics).map(([k, v]) => `<button type="button" data-m="${k}" aria-pressed="${k === airlineMetric}">${v.label}</button>`).join('')}</div>
        </div>
        ${barList(sorted.map(a => ({ key: a.airline, name: airlineName(a.airline), sub: `${fmtInt(a.flights)} flights`, value: a[airlineMetric], active: a.airline === airlineFocus })), { fmt: mt.fmt, clickable: true })}
        <p class="small muted" style="margin-top:10px">${REGION} average: ${airlineMetric === 'late_pct' ? fmtPct(OVERALL.late_pct) : airlineMetric === 'cancel_pct' ? fmtPct(OVERALL.cancel_pct) : OVERALL.late_avg_min + ' min'}.</p>
      </section>
      <div id="airline-detail"></div>`;
    $$('.seg button', host).forEach(b => b.addEventListener('click', () => { airlineMetric = b.dataset.m; renderAirlines(); }));
    $$('.bar-row', host).forEach(b => b.addEventListener('click', () => { airlineFocus = airlineFocus === b.dataset.key ? null : b.dataset.key; renderAirlines(); if (airlineFocus) $('#airline-detail').scrollIntoView({ behavior: 'smooth', block: 'start' }); }));
    if (airlineFocus) {
      const rs = (airlineRoutes.get(airlineFocus) || []).slice().sort((a, b) => b.flights - a.flights).slice(0, 20);
      $('#airline-detail').innerHTML = `<section class="panel">
        <div class="panel-head"><h3>${esc(airlineName(airlineFocus))}: busiest routes</h3><p>Tap a route to check it.</p></div>
        <div class="table-wrap"><table><thead><tr><th>Route</th><th>Cities</th><th class="n">Flights</th><th class="n">Arrive late</th><th class="n">Cancelled</th><th>Rating</th></tr></thead>
        <tbody>${rs.map(r => `<tr class="clickable" tabindex="0" data-o="${r.origin}" data-d="${r.destination}"><td class="code">${r.origin} → ${r.destination}</td><td>${esc(shortCity(r.origin))} to ${esc(shortCity(r.destination))}</td><td class="n">${fmtInt(r.flights)}</td><td class="n">${fmtPct(r.late_pct)}</td><td class="n">${fmtPct(r.cancel_pct)}</td><td>${statusPill(r.late_pct)}</td></tr>`).join('')}</tbody></table></div>
      </section>`;
      const pick = tr => { if (tr) runSearch({ from: tr.dataset.o, to: tr.dataset.d, airline: airlineFocus, hour: '', month: '' }); };
      $('#airline-detail tbody').addEventListener('click', e => pick(e.target.closest('tr')));
      $('#airline-detail tbody').addEventListener('keydown', e => { if (e.key === 'Enter') pick(e.target.closest('tr')); });
    }
  }

  // My trips tab -------------------------------------------------------------
  let confirmClear = false;
  function renderTrips() {
    const host = $('#view-trips');
    const list = loadTrips();
    if (!list.length) {
      host.innerHTML = `<section class="panel"><div class="empty">
        <h2>No saved trips yet</h2>
        <p>Check a flight, then press <b>Save trip</b>. Saved trips stay in this browser so you can compare them and add notes.</p>
        <a class="btn primary" href="#check">Check a flight</a></div></section>`;
      return;
    }
    host.innerHTML = `<section class="panel">
      <div class="panel-head"><h2>My trips</h2><p>${list.length} saved in this browser, ordered by delay odds.</p><span class="spacer"></span>
        ${confirmClear ? '<span class="small">Remove all saved trips?</span><button class="btn small" type="button" id="clear-yes">Remove all</button><button class="btn small ghost" type="button" id="clear-no">Keep them</button>' : '<button class="btn small ghost" type="button" id="clear-all">Clear all</button>'}
      </div>
      <div class="trips">${list.map((t, i) => ({ t, i, e: estimate(t) })).sort((x, y) => (x.e ? x.e.p : 999) - (y.e ? y.e.p : 999)).map(({ t, i, e }) => `
        <article class="trip-card" data-i="${i}">
          <div class="route code">${t.from} → ${t.to}</div>
          <div class="small muted">${esc(shortCity(t.from))} to ${esc(shortCity(t.to))} · ${esc(t.airline ? airlineName(t.airline) : 'Any airline')}${t.hour !== '' ? ' · ' + fmtHour(+t.hour) : ''}${t.month ? ' · ' + monthLabel(t.month) : ''}</div>
          ${e ? `<div class="big-figure"><span class="value num" style="font-size:2.6rem">${Math.round(e.p)}<small style="font-size:1.3rem">%</small></span>${statusPill(e.p)}</div>
          <div class="small muted">chance of 15+ min late · ${fmtPct(e.base.cancel_pct)} cancelled</div>` : '<p class="notice small">No data for this route.</p>'}
          <label class="small muted" for="note-${i}">Your note</label>
          <input id="note-${i}" class="note" type="text" maxlength="120" placeholder="e.g. Mum's birthday, 12 May" value="${esc(t.note || '')}" style="height:36px;border-radius:8px;border:1px solid var(--line);background:var(--surface);color:var(--ink);padding:0 10px;font:inherit">
          <div class="actions"><button class="btn small primary" type="button" data-act="open">Open</button><button class="btn small" type="button" data-act="share">Copy link</button><button class="btn small ghost" type="button" data-act="remove">Remove</button></div>
        </article>`).join('')}</div>
    </section>`;
    $$('.trip-card', host).forEach(card => {
      const i = +card.dataset.i;
      card.addEventListener('click', e => {
        const act = e.target.closest('button') && e.target.closest('button').dataset.act; if (!act) return;
        const trips = loadTrips(); const t = trips[i];
        if (act === 'open') runSearch({ from: t.from, to: t.to, airline: t.airline || '', hour: t.hour ?? '', month: t.month || '' });
        if (act === 'share') copyLink(t);
        if (act === 'remove') { trips.splice(i, 1); saveTrips(trips); renderTrips(); toast('Trip removed'); }
      });
      $('.note', card).addEventListener('change', e => { const trips = loadTrips(); if (trips[i]) { trips[i].note = e.target.value.trim(); saveTrips(trips); toast('Note saved'); } });
    });
    const c = $('#clear-all'); if (c) c.addEventListener('click', () => { confirmClear = true; renderTrips(); });
    const y = $('#clear-yes'); if (y) y.addEventListener('click', () => { confirmClear = false; saveTrips([]); renderTrips(); toast('All trips removed'); });
    const n = $('#clear-no'); if (n) n.addEventListener('click', () => { confirmClear = false; renderTrips(); });
  }

  // About tab ----------------------------------------------------------------
  function renderAbout() {
    const worstHour = nationalHours.filter(h => h.flights > 10000).sort((a, b) => b.late_pct - a.late_pct)[0];
    const bestHour = nationalHours.filter(h => h.flights > 10000).sort((a, b) => a.late_pct - b.late_pct)[0];
    $('#view-about').innerHTML = `<section class="panel"><div class="prose">
      <h2>How OnTime Check works</h2>
      <p>The site has two data options. Use the switch at the top to change between them.</p>
      <ul>
        <li><b>International · USA</b>: real U.S. Department of Transportation (BTS) on-time records. Airlines must report every scheduled flight, so it is a full count, not a sample.</li>
        <li><b>Domestic · India</b>: <b>simulated</b> flights. India's regulator DGCA publishes only monthly on-time percentages per airline, not flight-by-flight records, so this data is generated with real airports, airlines and routes and with delay rates calibrated to DGCA on-time performance. It shows realistic patterns (winter fog in the north, monsoon on the west coast, evening congestion), but the numbers are not real observations.</li>
      </ul>
      <p>You are viewing <b>${esc(DS.option)} · ${esc(DS.country)}</b>: <b>${fmtInt(OVERALL.flights)}</b> flights from ${fmtDate(OVERALL.first_date)} to ${fmtDate(OVERALL.last_date)}.</p>
      <h3>What “late” means</h3>
      <ul>
        <li>A flight is <b>late</b> when it reaches the gate 15 minutes or more after schedule. This is the standard definition used by BTS in the U.S. and DGCA in India.</li>
        <li>The late percentage counts flights that operated. Cancelled and diverted flights are shown separately.</li>
        <li><b>Typical delay when late</b> is the average arrival delay of the late flights only.</li>
      </ul>
      <h3>How a trip estimate is made</h3>
      <ul>
        <li>We start from the late rate of your route, or of your airline on that route when it has at least ${OVERALL.min_flights.cell} flights.</li>
        <li>If you pick a departure time or month, the rate is scaled by how that hour or month compares with the rest of the year at your departure airport.</li>
        <li>Routes need at least ${OVERALL.min_flights.route} flights and airports ${OVERALL.min_flights.airport} departures to be shown, so small samples don't give misleading odds.</li>
      </ul>
      <h3>Good to know</h3>
      <ul>
        <li>Across ${VS_REGION}, ${fmtPct(OVERALL.late_pct)} of flights arrived late and ${fmtPct(OVERALL.cancel_pct)} were cancelled.</li>
        ${bestHour && worstHour ? `<li>Flights scheduled around ${fmtHour(bestHour.hour)} were late ${fmtPct(bestHour.late_pct)} of the time, against ${fmtPct(worstHour.late_pct)} around ${fmtHour(worstHour.hour)}.</li>` : ''}
        <li>These are historical odds. They cannot see today's weather or a specific aircraft. Check your airline for live status.</li>
        <li>Saved trips and notes are kept only in your browser. Nothing you type is sent anywhere.</li>
      </ul>
      <h3>Behind the site</h3>
      <p>The data is cleaned and aggregated with Apache Spark (PySpark) in the Flight Delay Analysis big-data project, then published as a static file. Last refreshed ${esc(OVERALL.generated_at)}.</p>
    </div></section>`;
  }

  // ---------------------------------------------------------------- router
  const VIEWS = { check: renderCheck, airports: renderAirports, airlines: renderAirlines, trips: renderTrips, about: renderAbout };
  function go(view) {
    if (location.hash !== '#' + view) { try { history.replaceState(null, '', '#' + view); } catch (e) { location.hash = view; } }
    show(view);
  }
  function show(view) {
    if (!VIEWS[view]) view = 'check';
    $$('main > section').forEach(s => { s.hidden = s.dataset.section !== view; });
    $$('nav.tabs a').forEach(a => {
      if (a.dataset.view === view) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current');
    });
    $('#hero').hidden = view !== 'check';
    VIEWS[view]();
  }
  window.addEventListener('hashchange', () => show(location.hash.slice(1)));

  // ---------------------------------------------------------------- theme
  function applyTheme(t) { if (t) document.documentElement.setAttribute('data-theme', t); }
  applyTheme(store.get('theme', null, true));
  $('#theme-toggle').addEventListener('click', () => {
    const current = document.documentElement.getAttribute('data-theme') || (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    const next = current === 'dark' ? 'light' : 'dark';
    applyTheme(next); store.set('theme', next, true);
    show(location.hash.slice(1) || 'check');
  });

  $('#footer-source').textContent = DS.simulated
    ? `Data: simulated Indian domestic flights calibrated to DGCA on-time performance, ${fmtDate(OVERALL.first_date)} – ${fmtDate(OVERALL.last_date)} (${fmtInt(OVERALL.flights)} flights). Not real flight records.`
    : `Data: U.S. DOT Bureau of Transportation Statistics, ${fmtDate(OVERALL.first_date)} – ${fmtDate(OVERALL.last_date)} (${fmtInt(OVERALL.flights)} flights).`;
  $('#hero-lede').textContent = DS.simulated
    ? 'Pick two Indian airports and we look up every flight on that route in a year of simulated domestic records calibrated to DGCA on-time data. You get the odds of a 15-minute-plus delay, the most reliable airline and the best time of day to fly.'
    : 'Pick your airports and we look up every flight on that route from the last year of official U.S. on-time records. You get the odds of a 15-minute-plus delay, the most reliable airline and the best time of day to fly.';
  $('#sim-banner').hidden = !DS.simulated;
  document.title = DATASET === 'usa' ? 'OnTime Check' : `OnTime Check · ${DS.country}`;

  // Data option switch. Reload so every index is rebuilt for the chosen dataset.
  const sw = $('#dataset-switch');
  if (DATASET_ORDER.length > 1) {
    sw.innerHTML = DATASET_ORDER.map(k => {
      const d = SETS[k].dataset || {};
      return `<button type="button" data-ds="${k}" aria-pressed="${k === DATASET}"><b>${esc(d.option || k)}</b> ${esc(d.country || '')}</button>`;
    }).join('');
    sw.addEventListener('click', e => {
      const b = e.target.closest('button[data-ds]');
      if (!b || b.dataset.ds === DATASET) return;
      store.set('dataset', b.dataset.ds, true);
      const view = VIEWS[location.hash.slice(1)] ? location.hash.slice(1) : 'check';
      location.href = `${location.href.split(/[?#]/)[0]}?region=${b.dataset.ds}#${view}`;
    });
  } else sw.hidden = true;
  updateTripCount();
  syncForm();
  show(location.hash.slice(1) || 'check');
})();
