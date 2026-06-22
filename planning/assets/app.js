/* FluxDB roadmap — overview + a page per phase. Hash routing, static content. */
(function () {
  'use strict';
  const D = window.FLUXDB;
  const $ = s => document.querySelector(s);
  const root = document.documentElement;

  const el = (tag, cls, html) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (html != null) e.innerHTML = html;
    return e;
  };
  const fmt = s => String(s).replace(/`([^`]+)`/g, '<code>$1</code>');
  const STATUS_LABEL = Object.fromEntries(D.legend.map(([k, v]) => [k, v]));
  const idx = Object.fromEntries(D.phases.map((p, i) => [p.id, i]));
  const ctaLabel = p => (p.status === 'shipped' ? 'How it works' : 'Implementation plan');
  // Short labels for the top-bar nav fields.
  const NAV = {
    foundation: 'Storage', index: 'Index', mvcc: 'MVCC', wal: 'WAL', recovery: 'Recovery',
    checkpointing: 'Checkpoints', reclamation: 'Reclamation', verification: 'Verification',
    performance: 'Performance', query: 'Query',
  };
  let _spy = null;

  // ---------- Top-bar nav ----------
  function buildTopnav() {
    const nav = $('#topnav');
    nav.innerHTML = D.phases.map(p =>
      `<a class="tn-link s-${p.status}" href="#/${p.id}" data-id="${p.id}">${NAV[p.id] || p.title}</a>`
    ).join('');
    nav.querySelectorAll('a').forEach(a => a.addEventListener('click', closeMenu));
  }
  function highlightTopnav(id) {
    document.querySelectorAll('#topnav .tn-link').forEach(a =>
      a.classList.toggle('active', a.dataset.id === id));
  }
  function openMenu() { $('#topnav').classList.add('open'); $('#scrim').classList.add('show'); }
  function closeMenu() { $('#topnav').classList.remove('open'); $('#scrim').classList.remove('show'); }
  function toggleMenu() { $('#topnav').classList.contains('open') ? closeMenu() : openMenu(); }

  // ---------- Overview ----------
  function renderOverview() {
    const c = $('#content');
    c.innerHTML = '';

    const spec = D.spec.map(([k, v]) => `<div class="spec-row"><dt>${k}</dt><dd>${v}</dd></div>`).join('');
    const hero = el('section', 'hero');
    hero.innerHTML = `
      <div class="eyebrow">${D.hero.eyebrow}</div>
      <h1 class="hero-title">${D.hero.title}</h1>
      <p class="hero-lead">${D.hero.lead}</p>
      <p class="hero-today">${D.hero.today}</p>
      <dl class="spec">${spec}</dl>`;
    c.appendChild(hero);

    const head = el('div', 'road-head');
    head.innerHTML = `<h2 class="road-title">Roadmap</h2><div class="legend">` +
      D.legend.map(([k, label]) => `<span class="leg"><span class="dot ${k}"></span>${label}</span>`).join('') +
      `</div>`;
    c.appendChild(head);

    const wrap = el('div', 'road-wrap');
    const tl = el('div', 'timeline');
    for (const p of D.phases) {
      const sec = el('section', 'phase');
      sec.id = 'ph-' + p.id;
      const pts = p.points.map(pt => {
        const st = pt.status || p.status;
        const showDot = p.status === 'progress' || (pt.status && pt.status !== p.status);
        return `<li class="pt"><span class="pt-dot ${showDot ? st : 'none'}"></span><span>${fmt(pt.text)}</span></li>`;
      }).join('');
      sec.innerHTML = `
        <div class="ph-mark"><span class="ph-dot ${p.status}"></span></div>
        <div class="ph-body">
          <div class="ph-eyebrow">Phase ${p.num}</div>
          <div class="ph-head">
            <a class="ph-title" href="#/${p.id}">${p.title}</a>
            <span class="badge ${p.status}">${STATUS_LABEL[p.status]}</span>
          </div>
          <p class="ph-summary">${fmt(p.summary)}</p>
          <ul class="ph-points">${pts}</ul>
          <a class="ph-cta" href="#/${p.id}">${ctaLabel(p)} <span class="arr">→</span></a>
        </div>`;
      tl.appendChild(sec);
    }

    const rail = el('nav', 'road-rail');
    rail.innerHTML = '<div class="rail-title">On the roadmap</div>' +
      D.phases.map(p => `<a href="#ph-${p.id}" data-target="ph-${p.id}"><span class="dot ${p.status}"></span>${p.title}</a>`).join('');
    rail.querySelectorAll('a').forEach(a => a.addEventListener('click', e => {
      e.preventDefault();
      const t = document.getElementById(a.dataset.target);
      if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }));

    wrap.appendChild(tl);
    wrap.appendChild(rail);
    c.appendChild(wrap);
    c.appendChild(footer());

    window.scrollTo(0, 0);
  }

  // ---------- Phase detail page ----------
  function renderDetail(id) {
    const p = D.phases[idx[id]];
    if (!p) { location.hash = ''; return; }
    document.title = `${p.title} — FluxDB`;
    const c = $('#content');
    c.innerHTML = '';

    const art = el('article', 'detail');

    const head = el('header', 'detail-head');
    head.innerHTML = `
      <a class="back" href="#"><span class="arr">←</span> Roadmap</a>
      <div class="dh-eyebrow"><span class="badge ${p.status}">${STATUS_LABEL[p.status]}</span><span class="dh-num">Phase ${p.num}</span></div>
      <h1 class="dh-title">${p.title}</h1>
      <p class="dh-lead">${fmt(p.summary)}</p>`;
    art.appendChild(head);

    const body = el('div', 'detail-body');
    for (const block of (p.detail || [])) {
      body.appendChild(el('h2', 'dt-h', block.h));
      for (const item of block.body) {
        if (Array.isArray(item)) {
          body.appendChild(el('ul', 'dt-list', item.map(b => `<li>${fmt(b)}</li>`).join('')));
        } else {
          body.appendChild(el('p', null, fmt(item)));
        }
      }
    }
    if (p.note) {
      const n = el('div', 'dt-note');
      n.innerHTML = fmt(p.note);
      body.appendChild(n);
    }

    const plan = (D.plans || {})[id];
    if (plan && plan.length) {
      body.appendChild(el('h2', 'dt-h', p.status === 'shipped' ? 'Build order' : 'Implementation plan'));
      const ol = el('ol', 'dt-plan',
        plan.map(st => `<li><strong>${fmt(st.s)}.</strong> ${fmt(st.d)}</li>`).join(''));
      body.appendChild(ol);
    }
    art.appendChild(body);

    // prev / next phase
    const i = idx[id];
    const prev = D.phases[i - 1], next = D.phases[i + 1];
    const nav = el('nav', 'detail-nav');
    nav.appendChild(prev
      ? linkCard('prev', prev, 'Previous')
      : el('span'));
    nav.appendChild(next
      ? linkCard('next', next, 'Next')
      : el('span'));
    art.appendChild(nav);

    c.appendChild(art);
    c.appendChild(footer());
    window.scrollTo(0, 0);
  }

  function linkCard(cls, p, label) {
    const a = el('a', 'dn ' + cls);
    a.href = '#/' + p.id;
    a.innerHTML = `<span class="dn-label">${label}</span><span class="dn-title">${p.title}</span>`;
    return a;
  }

  function footer() {
    return el('footer', 'foot', 'FluxDB — an embedded, MVCC, OLTP database engine in Rust. Built in the open.');
  }

  // ---------- Scroll-spy (overview rail) ----------
  function spy() {
    if (_spy) { window.removeEventListener('scroll', _spy); _spy = null; }
    const rail = $('.road-rail');
    if (!rail) return;
    const links = Array.from(rail.querySelectorAll('a'));
    const phases = Array.from(document.querySelectorAll('.timeline .phase'));
    _spy = () => {
      let active = 0;
      phases.forEach((s, n) => { if (s.getBoundingClientRect().top <= 130) active = n; });
      links.forEach((a, n) => a.classList.toggle('active', n === active));
    };
    window.addEventListener('scroll', _spy, { passive: true });
    _spy();
  }

  // ---------- Theme ----------
  function applyTheme(t) {
    root.classList.toggle('dark', t === 'dark');
    try { localStorage.setItem('fluxdb-theme', t); } catch (e) {}
    const btn = $('#themeBtn');
    if (btn) btn.textContent = t === 'dark' ? '☀ Light' : '☾ Dark';
  }

  // ---------- Router ----------
  function route() {
    const h = location.hash.replace(/^#\/?/, '');
    closeMenu();
    if (h && idx[h] != null) { renderDetail(h); highlightTopnav(h); document.title = D.phases[idx[h]].title + ' — FluxDB'; }
    else { renderOverview(); spy(); highlightTopnav(null); document.title = 'FluxDB — roadmap'; }
  }

  function init() {
    buildTopnav();
    $('#themeBtn').addEventListener('click', () =>
      applyTheme(root.classList.contains('dark') ? 'light' : 'dark'));
    $('#brand').addEventListener('click', e => { e.preventDefault(); location.hash = ''; });
    $('#menuToggle').addEventListener('click', toggleMenu);
    $('#scrim').addEventListener('click', closeMenu);

    let theme = 'light';
    try { theme = localStorage.getItem('fluxdb-theme') || 'light'; } catch (e) {}
    applyTheme(theme);

    window.addEventListener('hashchange', route);
    route();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
