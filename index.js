<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BLACKJACK · CENTURY 21 — Аналитика</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #0c0c0e; --surface: #141416; --surface2: #1c1c1f; --border: #2a2a2e;
    --accent: #e8c84a; --accent2: #e87d3e; --red: #e85555; --green: #4ac98a;
    --text: #f0ede8; --muted: #6b6b70; --muted2: #9b9ba0;
  }
  body { background: var(--bg); color: var(--text); font-family: 'IBM Plex Sans', sans-serif; font-weight: 300; min-height: 100vh; }
  header { display: flex; align-items: center; justify-content: space-between; padding: 20px 32px; border-bottom: 1px solid var(--border); position: sticky; top: 0; background: var(--bg); z-index: 100; }
  .logo { font-family: 'Bebas Neue', sans-serif; font-size: 28px; letter-spacing: 3px; color: var(--accent); }
  .logo span { color: var(--muted2); font-size: 14px; font-family: 'IBM Plex Mono', monospace; letter-spacing: 1px; margin-left: 16px; }
  .header-right { display: flex; align-items: center; gap: 16px; }
  .period-badge { background: var(--surface2); border: 1px solid var(--border); border-radius: 4px; padding: 6px 14px; font-family: 'IBM Plex Mono', monospace; font-size: 12px; color: var(--muted2); }
  .btn-refresh { background: var(--accent); color: #000; border: none; border-radius: 4px; padding: 8px 20px; font-family: 'IBM Plex Mono', monospace; font-size: 12px; font-weight: 500; cursor: pointer; letter-spacing: 1px; transition: opacity 0.2s; }
  .btn-refresh:hover { opacity: 0.85; } .btn-refresh:disabled { opacity: 0.4; cursor: wait; }
  .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--muted); transition: background 0.3s; }
  .status-dot.ok { background: var(--green); box-shadow: 0 0 6px var(--green); }
  .status-dot.err { background: var(--red); }
  .status-dot.loading { background: var(--accent); animation: pulse 1s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
  .config-panel { background: var(--surface); border-bottom: 1px solid var(--border); padding: 16px 32px; }
  .config-grid { display: flex; gap: 10px; align-items: end; }
  .field label { display: block; font-family: 'IBM Plex Mono', monospace; font-size: 10px; letter-spacing: 1px; color: var(--muted); text-transform: uppercase; margin-bottom: 6px; }
  .field input { background: var(--surface2); border: 1px solid var(--border); border-radius: 4px; padding: 8px 12px; color: var(--text); font-family: 'IBM Plex Mono', monospace; font-size: 12px; outline: none; transition: border-color 0.2s; }
  .field input:focus { border-color: var(--accent); }
  .field input[type="date"] { color-scheme: dark; }
  .btn-save { background: var(--surface2); border: 1px solid var(--accent); color: var(--accent); border-radius: 4px; padding: 8px 16px; font-family: 'IBM Plex Mono', monospace; font-size: 11px; cursor: pointer; transition: all 0.2s; letter-spacing: 1px; height: 36px; white-space: nowrap; }
  .btn-save:hover { background: var(--accent); color: #000; }
  main { padding: 32px; }
  .kpi-row { display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 32px; }
  .kpi-card { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; padding: 20px; position: relative; overflow: hidden; }
  .kpi-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px; background: var(--accent); }
  .kpi-card.red::before { background: var(--red); } .kpi-card.green::before { background: var(--green); } .kpi-card.orange::before { background: var(--accent2); }
  .kpi-label { font-family: 'IBM Plex Mono', monospace; font-size: 10px; letter-spacing: 1.5px; color: var(--muted); text-transform: uppercase; margin-bottom: 12px; }
  .kpi-value { font-family: 'Bebas Neue', sans-serif; font-size: 48px; line-height: 1; color: var(--text); }
  .kpi-sub { font-size: 11px; color: var(--muted2); margin-top: 6px; }
  .section-title { font-family: 'IBM Plex Mono', monospace; font-size: 11px; letter-spacing: 2px; color: var(--muted); text-transform: uppercase; margin-bottom: 16px; display: flex; align-items: center; gap: 12px; }
  .section-title::after { content: ''; flex: 1; height: 1px; background: var(--border); }
  .section-subtitle { font-size: 12px; color: var(--muted2); margin-bottom: 16px; margin-top: -8px; font-family: 'IBM Plex Mono', monospace; }
  .table-wrap { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; margin-bottom: 32px; overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; min-width: 860px; }
  thead tr { background: var(--surface2); border-bottom: 1px solid var(--border); }
  th { font-family: 'IBM Plex Mono', monospace; font-size: 10px; letter-spacing: 1.5px; color: var(--muted); text-transform: uppercase; padding: 12px 14px; text-align: left; font-weight: 400; white-space: nowrap; }
  th.num { text-align: right; }
  td { padding: 13px 14px; font-size: 13px; border-bottom: 1px solid var(--border); vertical-align: middle; }
  tr:last-child td { border-bottom: none; }
  tbody tr:hover { background: rgba(255,255,255,0.02); }
  td.num { text-align: right; font-family: 'IBM Plex Mono', monospace; font-size: 13px; }
  .broker-name { font-weight: 500; }
  .broker-link { cursor: pointer; color: var(--accent); text-decoration: none; border-bottom: 1px solid transparent; transition: border-color 0.15s; }
  .broker-link:hover { border-bottom-color: var(--accent); }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 3px; font-family: 'IBM Plex Mono', monospace; font-size: 11px; }
  .badge-warn { background: rgba(232,125,62,0.15); color: var(--accent2); }
  .badge-danger { background: rgba(232,85,85,0.15); color: var(--red); }
  .badge-ok { background: rgba(74,201,138,0.15); color: var(--green); }
  .badge-muted { background: rgba(107,107,112,0.15); color: var(--muted2); }
  .conv-bar { display: flex; align-items: center; gap: 8px; justify-content: flex-end; }
  .conv-track { width: 50px; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }
  .conv-fill { height: 100%; background: var(--green); border-radius: 2px; }
  .tag-pills { display: flex; flex-wrap: wrap; gap: 4px; }
  .tag-pill { background: var(--surface2); border: 1px solid var(--border); border-radius: 3px; padding: 2px 7px; font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: var(--muted2); white-space: nowrap; }
  .tag-pill .tag-count { color: var(--accent); margin-left: 4px; font-weight: 500; }
  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 32px; }
  .funnel-row { display: flex; align-items: center; gap: 12px; padding: 10px 16px; border-bottom: 1px solid var(--border); }
  .funnel-row:last-child { border-bottom: none; }
  .funnel-stage { font-size: 12px; color: var(--muted2); flex: 1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .funnel-count { font-family: 'Bebas Neue', sans-serif; font-size: 22px; color: var(--text); min-width: 36px; text-align: right; }
  .funnel-bar-wrap { width: 80px; height: 3px; background: var(--border); border-radius: 2px; overflow: hidden; }
  .funnel-bar { height: 100%; background: var(--accent); border-radius: 2px; }
  .leads-filter { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
  .filter-btn { background: var(--surface2); border: 1px solid var(--border); color: var(--muted2); border-radius: 4px; padding: 5px 12px; font-family: 'IBM Plex Mono', monospace; font-size: 11px; cursor: pointer; transition: all 0.15s; }
  .filter-btn.active, .filter-btn:hover { border-color: var(--accent); color: var(--accent); }
  .error-msg { background: rgba(232,85,85,0.1); border: 1px solid rgba(232,85,85,0.3); border-radius: 6px; padding: 20px 24px; color: var(--red); font-family: 'IBM Plex Mono', monospace; font-size: 13px; margin-bottom: 24px; display: none; }
  .info-msg { background: var(--surface2); border: 1px solid var(--border); border-radius: 6px; padding: 16px 24px; color: var(--muted2); font-family: 'IBM Plex Mono', monospace; font-size: 12px; margin-bottom: 24px; display: none; }
  .toast { position: fixed; bottom: 24px; right: 24px; background: var(--surface2); border: 1px solid var(--border); border-radius: 6px; padding: 12px 20px; font-family: 'IBM Plex Mono', monospace; font-size: 12px; color: var(--text); z-index: 999; transform: translateY(60px); opacity: 0; transition: all 0.3s; }
  .toast.show { transform: translateY(0); opacity: 1; }
  .toast.success { border-color: var(--green); color: var(--green); }
  .toast.error { border-color: var(--red); color: var(--red); }
  .empty { text-align: center; padding: 40px; color: var(--muted); font-family: 'IBM Plex Mono', monospace; font-size: 12px; }
  .dd-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6); z-index: 200; }
  .dd-overlay.open { display: block; }
  .dd-panel { position: fixed; top: 0; right: 0; bottom: 0; width: min(860px,100vw); background: var(--bg); border-left: 1px solid var(--border); overflow-y: auto; z-index: 201; transform: translateX(100%); transition: transform 0.28s cubic-bezier(.4,0,.2,1); }
  .dd-panel.open { transform: translateX(0); }
  .dd-header { display: flex; align-items: center; justify-content: space-between; padding: 20px 28px; border-bottom: 1px solid var(--border); position: sticky; top: 0; background: var(--bg); z-index: 10; gap: 16px; }
  .dd-name { font-family: 'Bebas Neue', sans-serif; font-size: 32px; letter-spacing: 2px; color: var(--accent); line-height: 1; }
  .dd-meta { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--muted2); margin-top: 4px; }
  .dd-close { background: var(--surface2); border: 1px solid var(--border); color: var(--muted2); border-radius: 4px; padding: 8px 16px; font-family: 'IBM Plex Mono', monospace; font-size: 12px; cursor: pointer; transition: all 0.15s; flex-shrink: 0; }
  .dd-close:hover { border-color: var(--accent); color: var(--accent); }
  .dd-kpi { display: grid; grid-template-columns: repeat(4,1fr); border-bottom: 1px solid var(--border); }
  .dd-kpi .kpi-card { border-radius: 0; border: none; border-right: 1px solid var(--border); }
  .dd-kpi .kpi-card:last-child { border-right: none; }
  .dd-kpi .kpi-value { font-size: 36px; }
  .dd-body { padding: 28px; }
  .stage-bar { display: flex; align-items: center; gap: 12px; padding: 9px 0; border-bottom: 1px solid var(--border); }
  .stage-bar:last-child { border-bottom: none; }
  .stage-bar-name { font-size: 12px; color: var(--muted2); flex: 1; font-family: 'IBM Plex Mono', monospace; }
  .stage-bar-track { width: 120px; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; flex-shrink: 0; }
  .stage-bar-fill { height: 100%; background: var(--accent); border-radius: 2px; transition: width 0.4s ease; }
  .stage-bar-count { font-family: 'Bebas Neue', sans-serif; font-size: 20px; color: var(--text); min-width: 28px; text-align: right; }
</style>
</head>
<body>

<header>
  <div class="logo">BLACKJACK <span>CENTURY 21 · КЛУБ ЧЕМПИОНОВ</span></div>
  <div class="header-right">
    <div class="status-dot" id="status-dot"></div>
    <div class="period-badge" id="period-label">—</div>
    <button class="btn-refresh" id="btn-refresh" onclick="loadAll()">↻ ОБНОВИТЬ</button>
  </div>
</header>

<div class="config-panel">
  <div class="config-grid">
    <div class="field"><label>Начало периода</label><input type="date" id="inp-date-from" /></div>
    <div class="field"><label>Конец периода</label><input type="date" id="inp-date-to" /></div>
    <div class="field"><label>&nbsp;</label><button class="btn-save" onclick="saveConfig()">ЗАГРУЗИТЬ</button></div>
  </div>
</div>

<main>
  <div class="error-msg" id="error-banner"></div>
  <div class="info-msg" id="info-banner"></div>

  <div class="kpi-row">
    <div class="kpi-card"><div class="kpi-label">В активной воронке</div><div class="kpi-value" id="kpi-total">—</div><div class="kpi-sub">все сделки сейчас</div></div>
    <div class="kpi-card green"><div class="kpi-label">Новых за период</div><div class="kpi-value" id="kpi-new">—</div><div class="kpi-sub">Новостройки 2.0</div></div>
    <div class="kpi-card orange"><div class="kpi-label">Квалифицировано</div><div class="kpi-value" id="kpi-qual">—</div><div class="kpi-sub">этап 3+ в воронке</div></div>
    <div class="kpi-card red"><div class="kpi-label">Просроч. задачи</div><div class="kpi-value" id="kpi-overdue">—</div><div class="kpi-sub">требуют внимания</div></div>
    <div class="kpi-card"><div class="kpi-label">Конверсия в работу</div><div class="kpi-value" id="kpi-conv">—</div><div class="kpi-sub">новые → квалиф.</div></div>
  </div>

  <div class="section-title">Активная воронка — текущее состояние</div>
  <div class="section-subtitle">Кликните на брокера для детального дашборда</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Брокер</th>
          <th class="num">Всего в воронке</th>
          <th class="num">В работе (3+)</th>
          <th class="num">Встречи</th>
          <th class="num">Показы</th>
          <th class="num">Бронь</th>
          <th class="num">ДДУ</th>
          <th class="num">Без задач</th>
          <th class="num">Просроч.</th>
          <th class="num">Конверсия</th>
        </tr>
      </thead>
      <tbody id="funnel-tbody"><tr><td colspan="10" class="empty">Нажмите «Загрузить»</td></tr></tbody>
    </table>
  </div>

  <div class="section-title">Новые лиды — Новостройки 2.0 за период</div>
  <div class="section-subtitle">Разбивка по брокерам и тегам</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Брокер</th>
          <th class="num">Всего новых</th>
          <th>Источники (тег → кол-во)</th>
          <th class="num">До квалиф.</th>
          <th class="num">В работе</th>
          <th class="num">Конверсия</th>
        </tr>
      </thead>
      <tbody id="new-leads-tbody"><tr><td colspan="6" class="empty">Нет данных</td></tr></tbody>
    </table>
  </div>

  <div class="section-title">Детальный список новых лидов</div>
  <div class="leads-filter" id="broker-filter"></div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr><th>Дата</th><th>Брокер</th><th>Тег / источник</th><th>ID</th><th>Название</th><th>Этап сейчас</th></tr>
      </thead>
      <tbody id="leads-tbody"><tr><td colspan="6" class="empty">Нет данных</td></tr></tbody>
    </table>
  </div>

  <div class="two-col">
    <div>
      <div class="section-title">Воронка (все брокеры)</div>
      <div class="table-wrap" id="funnel-wrap"><div class="empty">Нет данных</div></div>
    </div>
    <div>
      <div class="section-title">Источники новых лидов</div>
      <div class="table-wrap" id="sources-wrap"><div class="empty">Нет данных</div></div>
    </div>
  </div>
</main>

<div class="dd-overlay" id="dd-overlay" onclick="closeDrilldown()"></div>
<div class="dd-panel" id="dd-panel">
  <div class="dd-header">
    <div>
      <div class="dd-name" id="dd-name">—</div>
      <div class="dd-meta" id="dd-meta">—</div>
    </div>
    <button class="dd-close" onclick="closeDrilldown()">✕ ЗАКРЫТЬ</button>
  </div>
  <div class="dd-kpi" id="dd-kpi"></div>
  <div class="dd-body" id="dd-body"></div>
</div>

<div class="toast" id="toast"></div>

<script>
let cfg = {};
let allLeads = [], newLeads = [];
let _lastUsers = {}, _lastStages = {};
let currentBrokerFilter = 'all';
let _cache = { allLeads:[], newLeads:[], tasks:[], users:{}, stages:{} };

// ID этапов воронки НОВОСТРОЙКИ 2.0
const S = {
  QUAL:  82143450,
  MEET1: 82143454,
  MEET2: 82143458,
  SHOW1: 82143462,
  SHOW2: 83396946,
  BOOK:  82143466,
  DDU:   82143470,
};
const ACTIVE_IDS = new Set(Object.values(S));
const isActive = sid => ACTIVE_IDS.has(sid);

const STAGE_NAMES = {
  82141386:'1. Новая заявка', 82141390:'~ НЕ дозвонился', 82141394:'2. Взят в работу',
  82143450:'3. Квалифицирован', 82143454:'4. Встреча назначена', 82143458:'5. Встреча проведена',
  82143462:'6. Показ назначен', 83396946:'7. Показ проведен',
  82143466:'8. Бронь объекта', 82143470:'9. Подписание ДДУ',
  142:'11. Комиссия получена', 143:'~ Архив'
};

// Порядок воронки: от лучшего к худшему
const FUNNEL_ORDER     = [82143470,82143466,83396946,82143462,82143458,82143454,82143450];
const DRILLDOWN_ORDER  = [143,142,82143470,82143466,83396946,82143462,82143458,82143454,82143450,82141394,82141390,82141386];

function loadConfig() {
  const saved = localStorage.getItem('amo_cfg_v3');
  if (saved) cfg = JSON.parse(saved);
  const now = new Date(), day = now.getDay()||7;
  const mon = new Date(now); mon.setDate(now.getDate()-day+1);
  const sun = new Date(mon); sun.setDate(mon.getDate()+6);
  const fmt = d => d.toISOString().split('T')[0];
  document.getElementById('inp-date-from').value = cfg.dateFrom || fmt(mon);
  document.getElementById('inp-date-to').value   = cfg.dateTo   || fmt(sun);
}

function saveConfig() {
  cfg = {
    dateFrom: document.getElementById('inp-date-from').value,
    dateTo:   document.getElementById('inp-date-to').value,
  };
  localStorage.setItem('amo_cfg_v3', JSON.stringify(cfg));
  loadAll();
}

function showToast(msg,t=''){const el=document.getElementById('toast');el.textContent=msg;el.className='toast show '+t;setTimeout(()=>el.className='toast',3500);}
function showError(msg){document.getElementById('error-banner').style.display='block';document.getElementById('error-banner').textContent='⚠ '+msg;document.getElementById('info-banner').style.display='none';}
function hideMessages(){document.getElementById('error-banner').style.display='none';document.getElementById('info-banner').style.display='none';}
function setStatus(s){document.getElementById('status-dot').className='status-dot '+s;}

async function amoGet(path, params={}) {
  const url = new URL('/api/proxy/'+path, window.location.origin);
  Object.entries(params).forEach(([k,v])=>url.searchParams.set(k,v));
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`AMO API ошибка ${res.status}`);
  return res.json();
}

async function fetchAll(path, params={}) {
  let page=1, results=[];
  while(true){
    const data = await amoGet(path, {...params, limit:250, page});
    const items = data?._embedded?.leads||data?._embedded?.users||data?._embedded?.tasks||data?._embedded?.pipelines||[];
    if(!items.length) break;
    results=results.concat(items);
    if(items.length<250) break;
    page++;
  }
  return results;
}

function toTs(d){return Math.floor(new Date(d).getTime()/1000);}

async function loadAll() {
  hideMessages(); setStatus('loading');
  const btn=document.getElementById('btn-refresh');
  btn.disabled=true; btn.textContent='↻ ЗАГРУЗКА...';
  try {
    const tokenRes = await fetch('/api/token');
    const tokenData = await tokenRes.json();
    if (!tokenData.token) throw new Error('Токен не настроен. Задайте AMO_ACCESS_TOKEN.');
    cfg.token = tokenData.token;

    const groupRes = await fetch('/api/group_users');
    const groupData = await groupRes.json();
    const filteredUserIds = new Set((groupData.users||[]).map(u=>u.id));

    const usersRaw = await fetchAll('users');
    const users={};
    usersRaw.forEach(u=>users[u.id]=u.name);

    const pipelinesRaw = await fetchAll('leads/pipelines');
    const stages={};
    pipelinesRaw.forEach(p=>(p._embedded?.statuses||[]).forEach(s=>stages[s.id]={name:s.name,pipeline:p.name}));

    const novostroi = pipelinesRaw.find(p=>p.name&&p.name.includes('Новостройки 2.0'));
    const novostroiId = novostroi?novostroi.id:null;

    allLeads = (await fetchAll('leads',{'with':'tags'}))
      .filter(l=>filteredUserIds.has(l.responsible_user_id)&&(!novostroiId||l.pipeline_id===novostroiId));

    const dateFrom=toTs(cfg.dateFrom||document.getElementById('inp-date-from').value);
    const dateTo=toTs(cfg.dateTo||document.getElementById('inp-date-to').value)+86399;
    const newLeadsAll = await fetchAll('leads',{'filter[created_at][from]':dateFrom,'filter[created_at][to]':dateTo,'with':'tags'});
    newLeads = newLeadsAll.filter(l=>filteredUserIds.has(l.responsible_user_id)&&(!novostroiId||l.pipeline_id===novostroiId));

    const allLeadIds = new Set(allLeads.map(l=>l.id));
    const tasksRaw = await fetchAll('tasks',{'filter[is_completed]':0});
    const tasks = tasksRaw.filter(t=>t.entity_type==='leads'&&allLeadIds.has(t.entity_id));

    _cache={allLeads,newLeads,tasks,users,stages};
    _lastUsers=users; _lastStages=stages;
    renderDashboard(allLeads,newLeads,tasks,users,stages);
    setStatus('ok'); showToast('Данные обновлены','success');
    const from=cfg.dateFrom||document.getElementById('inp-date-from').value;
    const to=cfg.dateTo||document.getElementById('inp-date-to').value;
    document.getElementById('period-label').textContent=`${from} — ${to}`;
  } catch(e){setStatus('err');showError(e.message);showToast(e.message,'error');console.error(e);}
  finally{btn.disabled=false;btn.textContent='↻ ОБНОВИТЬ';}
}

function renderDashboard(leads,newL,tasks,users,stages) {
  const now=Math.floor(Date.now()/1000);
  const overdueTasks=tasks.filter(t=>t.complete_till&&t.complete_till<now);
  const overdueByUser={};
  overdueTasks.forEach(t=>overdueByUser[t.responsible_user_id]=(overdueByUser[t.responsible_user_id]||0)+1);
  const leadsWithTasks=new Set(tasks.map(t=>t.entity_id));

  const brokerMap={};
  leads.forEach(l=>{
    const uid=l.responsible_user_id, name=users[uid]||`ID:${uid}`;
    if(!brokerMap[uid]) brokerMap[uid]={name,uid,allInFunnel:[],activeLeads:[],stageCounts:{},newLeads:[]};
    brokerMap[uid].allInFunnel.push(l);
    if(isActive(l.status_id)){
      brokerMap[uid].activeLeads.push(l);
      brokerMap[uid].stageCounts[l.status_id]=(brokerMap[uid].stageCounts[l.status_id]||0)+1;
    }
  });
  newL.forEach(l=>{
    const uid=l.responsible_user_id;
    if(!brokerMap[uid]) brokerMap[uid]={name:users[uid]||`ID:${uid}`,uid,allInFunnel:[],activeLeads:[],stageCounts:{},newLeads:[]};
    brokerMap[uid].newLeads.push(l);
  });

  let totalQual=0;
  leads.forEach(l=>{if(isActive(l.status_id))totalQual++;});
  const totalConv=newL.length>0?Math.round(totalQual/newL.length*100):0;
  document.getElementById('kpi-total').textContent=leads.filter(l=>isActive(l.status_id)).length;
  document.getElementById('kpi-new').textContent=newL.length;
  document.getElementById('kpi-qual').textContent=totalQual;
  document.getElementById('kpi-overdue').textContent=overdueTasks.length;
  document.getElementById('kpi-conv').textContent=totalConv+'%';

  const brokers=Object.values(brokerMap).sort((a,b)=>b.allInFunnel.length-a.allInFunnel.length);

  document.getElementById('funnel-tbody').innerHTML=brokers.map(b=>{
    const sc=b.stageCounts;
    const inWork=b.activeLeads.length;
    const meeting=(sc[S.MEET1]||0)+(sc[S.MEET2]||0);
    const show=(sc[S.SHOW1]||0)+(sc[S.SHOW2]||0);
    const book=sc[S.BOOK]||0;
    const ddu=sc[S.DDU]||0;
    const overdue=overdueByUser[b.uid]||0;
    const noTask=b.activeLeads.filter(l=>!leadsWithTasks.has(l.id)).length;
    const conv=b.newLeads.length>0?Math.round(inWork/b.newLeads.length*100):'—';
    const convNum=typeof conv==='number'?conv:0;
    const odClass=overdue>10?'badge-danger':overdue>3?'badge-warn':'';
    return `<tr>
      <td class="broker-name"><span class="broker-link" onclick="openBroker(${b.uid})">${b.name}</span></td>
      <td class="num">${b.allInFunnel.length}</td>
      <td class="num">${inWork}</td>
      <td class="num">${meeting}</td>
      <td class="num">${show}</td>
      <td class="num">${book}</td>
      <td class="num">${ddu}</td>
      <td class="num"><span class="badge ${noTask>5?'badge-warn':''}">${noTask}</span></td>
      <td class="num"><span class="badge ${odClass}">${overdue}</span></td>
      <td class="num">
        <div class="conv-bar">
          <span style="font-size:12px">${typeof conv==='number'?conv+'%':'—'}</span>
          <div class="conv-track"><div class="conv-fill" style="width:${Math.min(convNum,100)}%;background:${convNum>=33?'var(--green)':convNum>0?'var(--accent2)':'var(--border)'}"></div></div>
        </div>
      </td>
    </tr>`;
  }).join('')||'<tr><td colspan="10" class="empty">Нет данных</td></tr>';

  document.getElementById('new-leads-tbody').innerHTML=brokers.map(b=>{
    const nl=b.newLeads;
    if(!nl.length&&!b.allInFunnel.length) return '';
    const tagMap={};
    nl.forEach(l=>{const tags=l._embedded?.tags||[];const src=tags.length?tags.map(t=>t.name).join('+'):'Без тега';tagMap[src]=(tagMap[src]||0)+1;});
    const tagPills=Object.entries(tagMap).sort((a,b)=>b[1]-a[1]).map(([t,c])=>`<span class="tag-pill">${t}<span class="tag-count">${c}</span></span>`).join('');
    const beforeQual=nl.filter(l=>!isActive(l.status_id)).length;
    const inWork=nl.filter(l=>isActive(l.status_id)).length;
    const conv=nl.length>0?Math.round(inWork/nl.length*100):0;
    return `<tr>
      <td class="broker-name"><span class="broker-link" onclick="openBroker(${b.uid})">${b.name}</span></td>
      <td class="num"><strong style="font-family:'Bebas Neue',sans-serif;font-size:20px">${nl.length}</strong></td>
      <td><div class="tag-pills">${tagPills||'<span style="color:var(--muted);font-size:11px">—</span>'}</div></td>
      <td class="num"><span class="badge badge-muted">${beforeQual}</span></td>
      <td class="num"><span class="badge badge-ok">${inWork}</span></td>
      <td class="num">
        <div class="conv-bar">
          <span style="font-size:12px">${conv}%</span>
          <div class="conv-track"><div class="conv-fill" style="width:${Math.min(conv,100)}%;background:${conv>=33?'var(--green)':conv>0?'var(--accent2)':'var(--border)'}"></div></div>
        </div>
      </td>
    </tr>`;
  }).join('')||'<tr><td colspan="6" class="empty">Нет новых лидов за период</td></tr>';

  // Воронка от лучшего к худшему
  const stageTotals={};
  leads.forEach(l=>{if(isActive(l.status_id))stageTotals[l.status_id]=(stageTotals[l.status_id]||0)+1;});
  const maxVal=Math.max(...Object.values(stageTotals),1);
  document.getElementById('funnel-wrap').innerHTML=FUNNEL_ORDER.filter(id=>stageTotals[id]).map(id=>`
    <div class="funnel-row">
      <div class="funnel-stage">${STAGE_NAMES[id]}</div>
      <div class="funnel-bar-wrap"><div class="funnel-bar" style="width:${Math.round(stageTotals[id]/maxVal*100)}%"></div></div>
      <div class="funnel-count">${stageTotals[id]}</div>
    </div>`).join('')||'<div class="empty">Нет данных</div>';

  const srcTotals={};
  newL.forEach(l=>{const tags=l._embedded?.tags||[];const src=tags.length?tags.map(t=>t.name).join(', '):'Без тега';srcTotals[src]=(srcTotals[src]||0)+1;});
  const srcMax=Math.max(...Object.values(srcTotals),1);
  document.getElementById('sources-wrap').innerHTML=Object.entries(srcTotals).sort((a,b)=>b[1]-a[1]).map(([s,c])=>`
    <div class="funnel-row">
      <div class="funnel-stage">${s}</div>
      <div class="funnel-bar-wrap"><div class="funnel-bar" style="width:${Math.round(c/srcMax*100)}%;background:var(--accent2)"></div></div>
      <div class="funnel-count">${c}</div>
    </div>`).join('')||'<div class="empty">Нет данных</div>';

  const brokerNames=[...new Set(newL.map(l=>users[l.responsible_user_id]||`ID:${l.responsible_user_id}`))].sort();
  document.getElementById('broker-filter').innerHTML=
    `<button class="filter-btn active" onclick="filterLeads('all',event)">Все</button>`+
    brokerNames.map(n=>`<button class="filter-btn" onclick="filterLeads('${n}',event)">${n}</button>`).join('');
  renderLeads(newL);
}

function filterLeads(broker,e){
  currentBrokerFilter=broker;
  document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));
  if(e) e.target.classList.add('active');
  renderLeads(newLeads);
}

function renderLeads(leads){
  const u=_lastUsers, s=_lastStages;
  const filtered=currentBrokerFilter==='all'?leads:leads.filter(l=>(u[l.responsible_user_id]||'')===currentBrokerFilter);
  const tbody=document.getElementById('leads-tbody');
  if(!filtered.length){tbody.innerHTML='<tr><td colspan="6" class="empty">Нет лидов</td></tr>';return;}
  tbody.innerHTML=filtered.sort((a,b)=>b.created_at-a.created_at).slice(0,200).map(l=>{
    const d=new Date(l.created_at*1000);
    const ds=d.toLocaleDateString('ru-RU',{day:'2-digit',month:'2-digit'})+' '+d.toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'});
    const broker=u[l.responsible_user_id]||`ID:${l.responsible_user_id}`;
    const tags=l._embedded?.tags?.map(t=>t.name).join(', ')||'—';
    const sname=STAGE_NAMES[l.status_id]||s[l.status_id]?.name||'—';
    const sc=isActive(l.status_id)?'badge-ok':l.status_id===143?'badge-danger':'badge-muted';
    return `<tr>
      <td style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:var(--muted2)">${ds}</td>
      <td style="font-size:12px">${broker}</td>
      <td style="color:var(--muted2);font-size:11px">${tags}</td>
      <td style="font-family:'IBM Plex Mono',monospace;font-size:11px">
        <a href="https://c21pp.amocrm.ru/leads/detail/${l.id}" target="_blank" style="color:var(--accent);text-decoration:none">${l.id}</a>
      </td>
      <td style="font-size:12px">${l.name||'—'}</td>
      <td><span class="badge ${sc}" style="font-size:10px">${sname}</span></td>
    </tr>`;
  }).join('');
}

function openBroker(uid) {
  const {allLeads,newLeads,tasks,users,stages}=_cache;
  const name=users[uid]||`ID:${uid}`;
  const now=Math.floor(Date.now()/1000);
  const bAll=allLeads.filter(l=>l.responsible_user_id===uid);
  const bActive=bAll.filter(l=>ACTIVE_IDS.has(l.status_id));
  const bNew=newLeads.filter(l=>l.responsible_user_id===uid);
  const bLeadIds=new Set(bAll.map(l=>l.id));
  const bTasks=tasks.filter(t=>bLeadIds.has(t.entity_id));
  const bOverdue=bTasks.filter(t=>t.complete_till&&t.complete_till<now);
  const conv=bNew.length>0?Math.round(bActive.length/bNew.length*100):0;

  document.getElementById('dd-name').textContent=name;
  document.getElementById('dd-meta').textContent=`${bAll.length} всего в воронке · ${bNew.length} новых за период`;
  document.getElementById('dd-kpi').innerHTML=`
    <div class="kpi-card"><div class="kpi-label">Всего в воронке</div><div class="kpi-value">${bAll.length}</div><div class="kpi-sub">все лиды сейчас</div></div>
    <div class="kpi-card orange"><div class="kpi-label">В работе (3+)</div><div class="kpi-value">${bActive.length}</div><div class="kpi-sub">квалиф. и выше</div></div>
    <div class="kpi-card red"><div class="kpi-label">Просрочено</div><div class="kpi-value">${bOverdue.length}</div><div class="kpi-sub">задач</div></div>
    <div class="kpi-card green"><div class="kpi-label">Конверсия</div><div class="kpi-value">${conv}%</div><div class="kpi-sub">новые → в работе</div></div>`;

  const stageCounts={};
  bAll.forEach(l=>{stageCounts[l.status_id]=(stageCounts[l.status_id]||0)+1;});
  const maxSC=Math.max(...Object.values(stageCounts),1);
  const stagesHtml=DRILLDOWN_ORDER.filter(id=>stageCounts[id]).map(id=>`
    <div class="stage-bar">
      <div class="stage-bar-name">${STAGE_NAMES[id]||stages[id]?.name||`ID:${id}`}</div>
      <div class="stage-bar-track"><div class="stage-bar-fill" style="width:${Math.round(stageCounts[id]/maxSC*100)}%"></div></div>
      <div class="stage-bar-count">${stageCounts[id]}</div>
    </div>`).join('')||'<div class="empty">Нет данных</div>';

  const leadsHtml=bNew.sort((a,b)=>b.created_at-a.created_at).slice(0,100).map(l=>{
    const d=new Date(l.created_at*1000);
    const ds=d.toLocaleDateString('ru-RU',{day:'2-digit',month:'2-digit'})+' '+d.toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'});
    const tags=l._embedded?.tags?.map(t=>t.name).join(', ')||'—';
    const sname=STAGE_NAMES[l.status_id]||stages[l.status_id]?.name||'—';
    const sc=ACTIVE_IDS.has(l.status_id)?'badge-ok':l.status_id===143?'badge-danger':'badge-muted';
    return `<tr>
      <td style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:var(--muted2)">${ds}</td>
      <td style="color:var(--muted2);font-size:11px">${tags}</td>
      <td style="font-family:'IBM Plex Mono',monospace;font-size:11px"><a href="https://c21pp.amocrm.ru/leads/detail/${l.id}" target="_blank" style="color:var(--accent);text-decoration:none">${l.id}</a></td>
      <td style="font-size:12px">${l.name||'—'}</td>
      <td><span class="badge ${sc}" style="font-size:10px">${sname}</span></td>
    </tr>`;
  }).join('')||'<tr><td colspan="5" class="empty">Нет новых лидов за период</td></tr>';

  document.getElementById('dd-body').innerHTML=`
    <div class="section-title">Распределение по этапам</div>
    <div style="margin-bottom:28px">${stagesHtml}</div>
    <div class="section-title">Новые лиды за период</div>
    <div class="table-wrap" style="margin-bottom:0">
      <table style="min-width:500px">
        <thead><tr><th>Дата</th><th>Тег</th><th>ID</th><th>Название</th><th>Этап сейчас</th></tr></thead>
        <tbody>${leadsHtml}</tbody>
      </table>
    </div>`;

  document.getElementById('dd-overlay').classList.add('open');
  document.getElementById('dd-panel').classList.add('open');
  document.body.style.overflow='hidden';
}

function closeDrilldown(){
  document.getElementById('dd-overlay').classList.remove('open');
  document.getElementById('dd-panel').classList.remove('open');
  document.body.style.overflow='';
}

loadConfig();
</script>
</body>
</html>
