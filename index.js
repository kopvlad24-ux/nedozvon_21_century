const express = require('express');
const axios = require('axios');
const cron = require('node-cron');
const { google } = require('googleapis');

const app = express();
app.use(express.json());
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

const PORT = process.env.PORT || 3000;
const AMO_DOMAIN = process.env.AMO_DOMAIN;
const AMO_TOKEN = process.env.AMO_TOKEN;
const GOOGLE_CREDENTIALS = process.env.GOOGLE_CREDENTIALS;
const SHEET_ID = process.env.SHEET_ID;

const PIPELINE_ID = 10391694;
const STAGE_NEDOZVON = 82141390;
const STAGE_NEW = 82141386;
const STAGE_ARCHIVE = 143;
const GROUP_ID = 689470;
// AmoCRM ID сотрудника (РОП), на которого ставится задача «Назначить агента» как сигнал боту
const TRANSFER_RECIPIENT_ID = process.env.TRANSFER_RECIPIENT_ID ? parseInt(process.env.TRANSFER_RECIPIENT_ID) : null;
// task_type_id задачи «Назначить агента» в AmoCRM (4101302 — из отладки)
const TRANSFER_TASK_TYPE_ID = process.env.TRANSFER_TASK_TYPE_ID ? parseInt(process.env.TRANSFER_TASK_TYPE_ID) : 4101302;

const DRY_RUN = process.env.DRY_RUN !== 'false';

// Лиды попавшие в НЕ дозвонился ДО этой даты — пропускаем
const WIDGET_START_DATE = new Date('2026-04-15T00:00:00+03:00');
const WIDGET_START_TS = Math.floor(WIDGET_START_DATE.getTime() / 1000);

// ─── AmoCRM ───────────────────────────────────────────────────────────────────

const amo = axios.create({
  baseURL: `https://${AMO_DOMAIN}/api/v4`,
  headers: { Authorization: `Bearer ${AMO_TOKEN}` }
});

// ─── Google Sheets ────────────────────────────────────────────────────────────

function getSheetsClient() {
  const creds = JSON.parse(GOOGLE_CREDENTIALS);
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  return google.sheets({ version: 'v4', auth });
}

const TRUTHY = ['TRUE', 'ИСТИНА', '1', 'YES'];

async function getQueueData() {
  const sheets = getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'queue!A2:D100'
  });
  const rows = res.data.values || [];
  const allQueueUsers = rows
    .filter(r => r[0] && r[1])
    .map(r => ({ id: parseInt(r[0]), name: r[1] }));
  const monitored = rows
    .filter(r => r[2] && TRUTHY.includes(r[2].toString().toUpperCase()))
    .map(r => ({ id: parseInt(r[0]), name: r[1] }));
  const distribute = rows
    .filter(r => r[3] && TRUTHY.includes(r[3].toString().toUpperCase()))
    .map(r => ({ id: parseInt(r[0]), name: r[1] }));
  return { monitored, distribute, allQueueUsers };
}

// Все сотрудники из листа queue (независимо от active/distribute)
async function getAllQueueUserIds() {
  const sheets = getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'queue!A2:A100'
  });
  const rows = res.data.values || [];
  return new Set(rows.map(r => parseInt(r[0])).filter(id => !isNaN(id)));
}

async function getLastAssignedId() {
  const sheets = getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'state!B2'
  });
  const val = res.data.values?.[0]?.[0];
  return val ? parseInt(val) : null;
}

async function setLastAssignedId(userId) {
  if (DRY_RUN) { console.log(`[DRY_RUN] Sheets: last_assigned_id = ${userId}`); return; }
  const sheets = getSheetsClient();
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: 'state!B2',
    valueInputOption: 'RAW',
    requestBody: { values: [[userId]] }
  });
}

// ─── Assignments — когда виджет назначил лид ──────────────────────────────────

// Читаем все assignments
// Возвращает:
//   tsMap: Map { leadId → lastAssignedTs } — когда последний раз виджет назначал лид
//   historyMap: Map { leadId → Set<userId> } — кто уже был ответственным за лид
async function getAssignmentsMap() {
  const sheets = getSheetsClient();
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'assignments!A2:D10000'
    });
    const rows = res.data.values || [];
    const tsMap = new Map();
    const historyMap = new Map();
    const countMap = new Map(); // сколько раз виджет переназначал этот лид
    for (const row of rows) {
      const leadId = parseInt(row[0]);
      const userId = parseInt(row[1]);
      const ts = parseInt(row[2]);
      const type = row[3] || 'assign'; // 'assign' = кому назначили, 'from' = от кого ушёл
      if (!leadId) continue;
      // countMap считает только реальные переназначения, не записи "от кого ушёл"
      if (type === 'assign') {
        countMap.set(leadId, (countMap.get(leadId) || 0) + 1);
      }
      // Последняя дата назначения
      if (ts && type === 'assign') {
        const existing = tsMap.get(leadId);
        if (!existing || ts > existing) tsMap.set(leadId, ts);
      }
      // История ответственных — все записи
      if (userId) {
        if (!historyMap.has(leadId)) historyMap.set(leadId, new Set());
        historyMap.get(leadId).add(userId);
      }
    }
    return { tsMap, historyMap, countMap };
  } catch (e) {
    console.warn('Не удалось прочитать assignments:', e.message);
    return { tsMap: new Map(), historyMap: new Map(), countMap: new Map() };
  }
}

// Записываем назначение в assignments.
// type='assign' — кому назначили (считается в countMap); type='from' — от кого ушёл (только история)
async function writeAssignment(leadId, userId, userName, type = 'assign') {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] assignments: лид ${leadId} ${type === 'from' ? 'от' : '→'} ${userName}`);
    return;
  }
  const sheets = getSheetsClient();
  const ts = Math.floor(Date.now() / 1000);
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: 'assignments!A:D',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [[leadId, userId, ts, type]] }
  });
}

// ─── Лог и статистика ─────────────────────────────────────────────────────────

async function writeLog(leadId, leadName, fromName, toName, reason = '') {
  if (DRY_RUN) { console.log(`[DRY_RUN] Лог: ${leadId} от ${fromName} → ${toName} [${reason}]`); return; }
  const sheets = getSheetsClient();
  const now = new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: 'log!A:F',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [[now, leadId, leadName || '', fromName, toName, reason]] }
  });
}

function getWeekStart() {
  const now = new Date();
  const day = now.getDay();
  const diff = (day === 0 ? -6 : 1 - day);
  const monday = new Date(now);
  monday.setDate(now.getDate() + diff);
  monday.setHours(0, 0, 0, 0);
  return monday;
}

function parseRuDate(str) {
  const [datePart] = str.split(', ');
  const [d, m, y] = datePart.split('.');
  return new Date(parseInt(y), parseInt(m) - 1, parseInt(d));
}

async function updateStats() {
  if (DRY_RUN) { console.log('[DRY_RUN] Пропуск обновления статистики'); return; }
  const sheets = getSheetsClient();

  const logRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'log!A2:F10000'
  });
  const rows = logRes.data.values || [];
  if (!rows.length) return;

  const MONTHS_RU = ['Январь','Февраль','Март','Апрель','Май','Июнь',
    'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь'];
  const weekStart = getWeekStart();

  function add(map, name, key) {
    if (!map[name]) map[name] = { received: 0, taken: 0, manual: 0, auto5: 0 };
    map[name][key]++;
  }

  const weekStats = {};
  const allMonthStats = {};

  for (const row of rows) {
    const [dateStr, , , fromName, toName, reason] = row;
    if (!dateStr || !fromName || !toName) continue;
    let date;
    try { date = parseRuDate(dateStr); } catch { continue; }

    if (date >= weekStart) {
      add(weekStats, fromName, 'taken');
      if (toName !== '~ Архив') add(weekStats, toName, 'received');
      if (reason === 'назначить агента') add(weekStats, fromName, 'manual');
      if (reason === '5 дней') add(weekStats, fromName, 'auto5');
    }

    const monthKey = `${MONTHS_RU[date.getMonth()]} ${date.getFullYear()}`;
    if (!allMonthStats[monthKey]) allMonthStats[monthKey] = {};
    add(allMonthStats[monthKey], fromName, 'taken');
    if (toName !== '~ Архив') add(allMonthStats[monthKey], toName, 'received');
    if (reason === 'назначить агента') add(allMonthStats[monthKey], fromName, 'manual');
    if (reason === '5 дней') add(allMonthStats[monthKey], fromName, 'auto5');
  }

  // Неделя
  const weekRows = [['Сотрудник', 'Получено лидов', 'Снято лидов', 'Передал вручную', 'Задержал (5 дней)']];
  for (const [name, s] of Object.entries(weekStats)) {
    weekRows.push([name, s.received || 0, s.taken || 0, s.manual || 0, s.auto5 || 0]);
  }
  await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'week!A:E' });
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID, range: 'week!A1',
    valueInputOption: 'RAW', requestBody: { values: weekRows }
  });

  // Месяцы блоками
  const monthRows = [];
  const sortedMonths = Object.keys(allMonthStats).sort((a, b) => {
    const [mA, yA] = a.split(' ');
    const [mB, yB] = b.split(' ');
    return (MONTHS_RU.indexOf(mA) + parseInt(yA) * 12) - (MONTHS_RU.indexOf(mB) + parseInt(yB) * 12);
  });
  for (const monthKey of sortedMonths) {
    monthRows.push([monthKey, '', '', '', '']);
    monthRows.push(['Сотрудник', 'Получено лидов', 'Снято лидов', 'Передал вручную', 'Задержал (5 дней)']);
    for (const [name, s] of Object.entries(allMonthStats[monthKey])) {
      monthRows.push([name, s.received || 0, s.taken || 0, s.manual || 0, s.auto5 || 0]);
    }
    monthRows.push(['', '', '', '', '']);
  }
  await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'month!A:E' });
  if (monthRows.length) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: 'month!A1',
      valueInputOption: 'RAW', requestBody: { values: monthRows }
    });
  }

  console.log('Статистика обновлена');
}

// ─── Синхронизация сотрудников ────────────────────────────────────────────────

async function syncUsers() {
  console.log('Синхронизация сотрудников...');
  const sheets = getSheetsClient();
  const { data } = await amo.get('/users', { params: { limit: 250 } });
  const allUsers = data._embedded?.users || [];
  const groupUsers = allUsers.filter(u =>
    u.rights?.group_id === GROUP_ID && u.rights?.is_active === true
  );
  const queueRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID, range: 'queue!A2:D100'
  });
  const existingIds = new Set((queueRes.data.values || []).map(r => parseInt(r[0])));
  const newRows = groupUsers
    .filter(u => !existingIds.has(u.id))
    .map(u => [u.id, u.name, 'FALSE', 'FALSE']);
  if (newRows.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: 'queue!A2',
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: newRows }
    });
    console.log(`Добавлено в queue: ${newRows.map(r => r[1]).join(', ')}`);
  } else {
    console.log('Новых сотрудников нет');
  }

  // Синхронизируем сотрудники — все из queue кто ещё не там
  const allQueueRows = (queueRes.data.values || []).concat(newRows.map(r => [r[0], r[1]]));
  const empRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID, range: 'сотрудники!A2:B100'
  });
  const existingEmpIds = new Set((empRes.data.values || []).map(r => parseInt(r[0])));
  const newEmpRows = allQueueRows
    .filter(r => r[0] && r[1] && !existingEmpIds.has(parseInt(r[0])))
    .map(r => [parseInt(r[0]), r[1]]);
  if (newEmpRows.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: 'сотрудники!A2',
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
      requestBody: { values: newEmpRows }
    });
    console.log(`Добавлено в сотрудники: ${newEmpRows.map(r => r[1]).join(', ')}`);
  }

  return { added: newRows.length, users: newRows.map(r => ({ id: r[0], name: r[1] })) };
}

// ─── Все сотрудники группы ───────────────────────────────────────────────────

async function getGroupUserIds() {
  const { data } = await amo.get('/users', { params: { limit: 250 } });
  const users = data._embedded?.users || [];
  return new Set(
    users
      .filter(u => u.rights?.group_id === GROUP_ID && u.rights?.is_active === true)
      .map(u => u.id)
  );
}

// ─── Архивирование ────────────────────────────────────────────────────────────

async function archiveLead(lead) {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] Сделка ${lead.id} → Архив (прошла по всем сотрудникам группы)`);
    return;
  }
  await amo.patch(`/leads/${lead.id}`, {
    status_id: STAGE_ARCHIVE,
    pipeline_id: PIPELINE_ID
  });
  await amo.post('/leads/notes', [{
    entity_id: lead.id,
    note_type: 'common',
    params: { text: '📁 Лид отправлен в архив — прошёл через всех сотрудников группы' }
  }]);
  console.log(`Сделка ${lead.id} → Архив`);
}

// ─── Очередь ──────────────────────────────────────────────────────────────────

function pickNextUser(distribute, lastAssignedId, currentResponsibleId, leadHistory) {
  if (!distribute.length) throw new Error('Список выдачи пуст');
  const lastIdx = distribute.findIndex(u => u.id === lastAssignedId);
  // Первый проход: ищем того кто не был ответственным за этот лид и не текущий
  for (let i = 1; i <= distribute.length; i++) {
    const candidate = distribute[(lastIdx + i) % distribute.length];
    if (candidate.id === currentResponsibleId) continue;
    if (leadHistory && leadHistory.has(candidate.id)) continue;
    return candidate;
  }
  // Второй проход: все уже были — просто пропускаем текущего ответственного
  for (let i = 1; i <= distribute.length; i++) {
    const candidate = distribute[(lastIdx + i) % distribute.length];
    if (candidate.id !== currentResponsibleId) return candidate;
  }
  return distribute[0];
}


// ─── История ответственных по сделке из AMO ───────────────────────────────────


// ─── Helpers ──────────────────────────────────────────────────────────────────

// ─── Производственный календарь России (isdayoff.ru) ─────────────────────────

const calendarCache = {};

async function getRussianCalendar(year) {
  if (calendarCache[year]) return calendarCache[year];
  try {
    const res = await axios.get(`https://isdayoff.ru/api/getdata?year=${year}&cc=ru`, { timeout: 5000 });
    calendarCache[year] = res.data;
    console.log(`Производственный календарь ${year} загружен (${res.data.length} дней)`);
    return calendarCache[year];
  } catch (e) {
    console.warn(`Не удалось загрузить календарь ${year}: ${e.message}. Считаем только сб/вс.`);
    return null;
  }
}

async function getWorkingDaysBetween(startTs, endTs) {
  let count = 0;
  const cur = new Date(startTs * 1000);
  const end = new Date(endTs * 1000);
  cur.setHours(0, 0, 0, 0);
  end.setHours(0, 0, 0, 0);
  while (cur < end) {
    const year = cur.getFullYear();
    const calendar = await getRussianCalendar(year);
    if (calendar) {
      const yearStart = new Date(year, 0, 1);
      const dayIndex = Math.floor((cur - yearStart) / 86400000);
      if (calendar[dayIndex] === '0') count++;
    } else {
      const day = cur.getDay();
      if (day !== 0 && day !== 6) count++;
    }
    cur.setDate(cur.getDate() + 1);
  }
  return count;
}

async function getLeadsInStage() {
  let page = 1;
  let leads = [];
  while (true) {
    const { data } = await amo.get('/leads', {
      params: {
        'filter[pipeline_id]': PIPELINE_ID,
        'filter[status_id]': STAGE_NEDOZVON,
        limit: 250, page
      }
    });
    const batch = data._embedded?.leads || [];
    const filtered = batch.filter(lead => {
      const ok = lead.pipeline_id === PIPELINE_ID && lead.status_id === STAGE_NEDOZVON;
      if (!ok) console.warn(`ПРОПУЩЕН ${lead.id} (pipeline=${lead.pipeline_id}, status=${lead.status_id})`);
      return ok;
    });
    leads = leads.concat(filtered);
    if (batch.length < 250) break;
    page++;
  }
  return leads;
}

// Сканирует ВСЕ лиды воронки (все этапы) и записывает текущего ответственного
// как 'from' в assignments если его там ещё нет. Гарантирует что человек не получит
// лид повторно независимо от того в каком этапе он его вёл раньше.
async function seedHistoryFromAllPipelineLeads(historyMap) {
  let page = 1;
  const toWrite = []; // [[leadId, userId, ts, 'from'], ...]
  const ts = Math.floor(Date.now() / 1000);

  while (true) {
    let batch;
    try {
      const { data } = await amo.get('/leads', {
        params: { 'filter[pipeline_id]': PIPELINE_ID, limit: 250, page }
      });
      batch = data._embedded?.leads || [];
    } catch (e) {
      console.warn('seedHistory: ошибка загрузки лидов стр.' + page + ':', e.message);
      break;
    }

    for (const lead of batch) {
      const uid = lead.responsible_user_id;
      if (!uid) continue;
      if (!historyMap.get(lead.id)?.has(uid)) {
        if (!historyMap.has(lead.id)) historyMap.set(lead.id, new Set());
        historyMap.get(lead.id).add(uid);
        toWrite.push([lead.id, uid, ts, 'from']);
      }
    }

    if (batch.length < 250) break;
    page++;
  }

  if (toWrite.length === 0) {
    console.log('seedHistory: все ответственные уже в истории');
    return;
  }

  if (!DRY_RUN) {
    try {
      const sheets = getSheetsClient();
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: 'assignments!A:D',
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: { values: toWrite }
      });
      console.log(`seedHistory: зафиксировано ${toWrite.length} новых ответственных по всей воронке`);
    } catch (e) {
      console.warn('seedHistory: не удалось записать в Sheets:', e.message);
    }
  } else {
    console.log(`[DRY_RUN] seedHistory: зафиксировал бы ${toWrite.length} ответственных`);
  }
}

async function getOpenTransferTasks() {
  if (!TRANSFER_RECIPIENT_ID) return [];
  try {
    const { data } = await amo.get('/tasks', {
      params: {
        'filter[responsible_user_id]': TRANSFER_RECIPIENT_ID,
        'filter[is_completed]': 0,
        limit: 250
      }
    });
    const tasks = data._embedded?.tasks || [];
    return tasks.filter(t => t.task_type_id === TRANSFER_TASK_TYPE_ID && t.entity_type === 'leads');
  } catch (e) {
    console.warn('Не удалось получить задачи "Назначить агента":', e.message);
    return [];
  }
}

async function getExistingTasks(leadId) {
  try {
    const { data } = await amo.get('/tasks', {
      params: { 'filter[entity_id]': leadId, 'filter[entity_type]': 'leads' }
    });
    return data._embedded?.tasks || [];
  } catch (e) { return []; }
}

async function createTask(leadId, responsibleUserId, text) {
  if (DRY_RUN) { console.log(`[DRY_RUN] Задача ${leadId}: "${text}"`); return; }
  const dueDate = Math.floor(Date.now() / 1000) + 86400;
  await amo.post('/tasks', [{
    task_type_id: 1, text,
    complete_till: dueDate,
    entity_id: leadId,
    entity_type: 'leads',
    responsible_user_id: responsibleUserId
  }]);
  console.log(`Задача создана: ${leadId}`);
}

async function reassignAndMove(lead, fromUser, nextUser, reason = '') {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] Сделка ${lead.id} → ${nextUser.name}, этап → Новая заявка [${reason}]`);
    return;
  }

  // Меняем ответственного; этап → "Новая заявка":
  // - всегда при "Назначить агента" (брокер явно решил передать, вне зависимости от этапа)
  // - при авто-5-дней только если лид в Недозвоне
  const patchData = { responsible_user_id: nextUser.id };
  if (lead.status_id === STAGE_NEDOZVON || reason === 'назначить агента') {
    patchData.status_id = STAGE_NEW;
    patchData.pipeline_id = PIPELINE_ID;
  }
  console.log(`Патч сделки ${lead.id}: status_id=${lead.status_id}, данные=${JSON.stringify(patchData)}`);
  try {
    await amo.patch(`/leads/${lead.id}`, patchData);
  } catch (e) {
    console.error(`Ошибка патча сделки ${lead.id}:`, JSON.stringify(e.response?.data));
    throw e;
  }

  // Меняем ответственного в контактах (retry при socket hang up)
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const { data: leadData } = await amo.get(`/leads/${lead.id}`, {
        params: { with: 'contacts' }
      });
      const contacts = leadData._embedded?.contacts || [];
      if (contacts.length) {
        await amo.patch('/contacts', contacts.map(c => ({
          id: c.id,
          responsible_user_id: nextUser.id
        })));
        console.log(`Контакты переназначены (${contacts.length} шт.)`);
      }
      break;
    } catch (e) {
      if (attempt < 3) {
        await new Promise(r => setTimeout(r, 2000 * attempt));
      } else {
        console.warn(`Не удалось переназначить контакты ${lead.id}: ${e.message}`);
      }
    }
  }

  // Комментарий
  await amo.post('/leads/notes', [{
    entity_id: lead.id,
    note_type: 'common',
    params: { text: `🔄 Лид передан по распределению → ${nextUser.name}` }
  }]);

  await writeLog(lead.id, lead.name, fromUser.name, nextUser.name, reason);
  await writeAssignment(lead.id, nextUser.id, nextUser.name);
  console.log(`Сделка ${lead.id} → ${nextUser.name} | Новая заявка`);
}

async function archiveLead(lead, fromUser) {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] Сделка ${lead.id} → Архив (прошла по всем сотрудникам)`);
    return;
  }
  await amo.patch(`/leads/${lead.id}`, {
    status_id: STAGE_ARCHIVE,
    pipeline_id: PIPELINE_ID
  });
  await amo.post('/leads/notes', [{
    entity_id: lead.id,
    note_type: 'common',
    params: { text: `📁 Лид отправлен в архив — прошёл через всех сотрудников без результата` }
  }]);
  console.log(`Сделка ${lead.id} → Архив`);
}

// ─── Перераспределение лида ───────────────────────────────────────────────────
// Максимум 3 переназначения (4-й → архив).
// Возвращает { newLastAssignedId } — обновлённый указатель очереди.

async function performRedistribution(lead, fromUser, distribute, countMap, lastAssignedId, historyMap, reason = '') {
  // Записываем fromUser в историю (type='from') чтобы он не получил этот лид снова
  if (!historyMap.get(lead.id)?.has(fromUser.id)) {
    await writeAssignment(lead.id, fromUser.id, fromUser.name, 'from');
    if (!historyMap.has(lead.id)) historyMap.set(lead.id, new Set());
    historyMap.get(lead.id).add(fromUser.id);
  }

  const redistributionCount = countMap.get(lead.id) || 0;

  if (redistributionCount >= 3) {
    console.log(`Сделка ${lead.id}: уже было ${redistributionCount} переназначений → Архив`);
    await archiveLead(lead, fromUser);
    await writeLog(lead.id, lead.name, fromUser.name, '~ Архив', reason);
    return { newLastAssignedId: null };
  }

  // Используем историю из Google Sheets — AMO Events API не поддерживает нужный фильтр
  const sheetHistory = historyMap?.get(lead.id) || new Set();
  const history = new Set([...sheetHistory]);
  history.add(fromUser.id);
  console.log(`Сделка ${lead.id}: история ответственных (${history.size}): [${[...history].join(', ')}]`);
  let candidates = distribute.filter(u => !history.has(u.id));

  if (!candidates.length) {
    console.log(`Сделка ${lead.id}: все из очереди уже были → Архив`);
    await archiveLead(lead, fromUser);
    await writeLog(lead.id, lead.name, fromUser.name, '~ Архив', reason);
    return { newLastAssignedId: null };
  }

  // Перебираем кандидатов — пропускаем тех, кого AmoCRM не принимает (нет доступа к воронке)
  while (candidates.length > 0) {
    const nextUser = pickNextUser(candidates, lastAssignedId, fromUser.id, null);
    try {
      await reassignAndMove(lead, fromUser, nextUser, reason);
      await setLastAssignedId(nextUser.id);
      return { newLastAssignedId: nextUser.id };
    } catch (e) {
      const errors = e.response?.data?.['validation-errors']?.[0]?.errors || [];
      const isInvalidUser = errors.some(err => err.path === 'responsible_user_id' && err.code === 'NotSupportedChoice');
      if (isInvalidUser) {
        console.warn(`Сотрудник ${nextUser.name} (${nextUser.id}) недоступен для назначения в AmoCRM — пропускаем, берём следующего`);
        candidates = candidates.filter(u => u.id !== nextUser.id);
      } else {
        throw e; // другая ошибка — пробрасываем наверх
      }
    }
  }

  // Все кандидаты оказались недоступны
  console.warn(`Сделка ${lead.id}: все кандидаты недоступны в AmoCRM → Архив`);
  await archiveLead(lead, fromUser);
  await writeLog(lead.id, lead.name, fromUser.name, '~ Архив', reason);
  return { newLastAssignedId: null };
}

// ─── Mutex — защита от параллельных запусков ─────────────────────────────────

let checkLeadsRunning = false;
let checkTransferRunning = false;

// ─── Основная проверка ────────────────────────────────────────────────────────

async function checkLeads() {
  if (checkLeadsRunning) {
    console.log('checkLeads уже выполняется — пропускаем параллельный запуск');
    return;
  }
  checkLeadsRunning = true;
  try {
    await _checkLeads();
  } finally {
    checkLeadsRunning = false;
  }
}

async function _checkLeads() {
  console.log(`\n=== Проверка ${new Date().toISOString()}${DRY_RUN ? ' [DRY_RUN]' : ''} ===`);
  try {
    const { monitored, distribute } = await getQueueData();
    if (!monitored.length) { console.log('Список мониторинга пуст'); return; }
    if (!distribute.length) { console.log('Список выдачи пуст'); return; }
    const monitoredIds = new Set(monitored.map(u => u.id));
    const monitoredMap = Object.fromEntries(monitored.map(u => [u.id, u]));
    // distribute уже получен выше из getQueueData
    console.log(`Мониторим: ${monitored.map(u => u.name).join(', ')}`);
    console.log(`Выдаём: ${distribute.map(u => u.name).join(', ')}`);

    const [leads, assignments, lastAssignedId, allQueueUserIds] = await Promise.all([
      getLeadsInStage(),
      getAssignmentsMap(),
      getLastAssignedId(),
      getAllQueueUserIds()
    ]);
    const { tsMap: assignmentsMap, historyMap, countMap } = assignments;
    // Фиксируем ответственных по всей воронке до обработки лидов
    await seedHistoryFromAllPipelineLeads(historyMap);
    console.log(`Лидов в этапе НЕ дозвонился: ${leads.length}`);
    const nowTs = Math.floor(Date.now() / 1000);
    let statsUpdated = false;
    let currentLastAssignedId = lastAssignedId; // обновляем локально по ходу цикла

    for (const lead of leads) {
      // Двойная защита — только нужный этап
      if (lead.pipeline_id !== PIPELINE_ID || lead.status_id !== STAGE_NEDOZVON) {
        console.warn(`ПРОПУСК ${lead.id} — неверный этап`);
        continue;
      }

      const responsibleId = lead.responsible_user_id;
      if (!monitoredIds.has(responsibleId)) {
        console.log(`Сделка ${lead.id}: ответственный не в очереди, пропуск`);
        continue;
      }

      // Фиксируем текущего ответственного в историю при первом же взгляде на лид.
      // Это гарантирует что он не получит лид снова при последующих перераспределениях,
      // даже если он был назначен вручную до начала работы бота.
      if (!historyMap.get(lead.id)?.has(responsibleId)) {
        await writeAssignment(lead.id, responsibleId, monitoredMap[responsibleId]?.name || `User ${responsibleId}`, 'from');
        if (!historyMap.has(lead.id)) historyMap.set(lead.id, new Set());
        historyMap.get(lead.id).add(responsibleId);
        console.log(`Сделка ${lead.id}: зафиксирован текущий ответственный ${monitoredMap[responsibleId]?.name} в историю`);
      }

      // Определяем точку отсчёта — всегда от момента попадания в этап Недозвон
      const leadHistory = historyMap.get(lead.id); // кто уже был ответственным (для исключения повторов)
      const statusChangedTs = lead.status_changed_at || lead.created_at;

      // Лид попал в Недозвон ДО запуска виджета — пропускаем
      if (statusChangedTs < WIDGET_START_TS) {
        console.log(`Сделка ${lead.id}: был в этапе до запуска виджета, пропуск`);
        continue;
      }

      const sinceTs = statusChangedTs;

      const workingDays = await getWorkingDaysBetween(sinceTs, nowTs);
      console.log(`Сделка ${lead.id} (${lead.name}): ${workingDays} рабочих дней`);

      if (workingDays >= 5) {
        const fromUser = monitoredMap[responsibleId];
        const result = await performRedistribution(lead, fromUser, distribute, countMap, currentLastAssignedId, historyMap, '5 дней');
        if (result.newLastAssignedId) currentLastAssignedId = result.newLastAssignedId;
        statsUpdated = true;
        continue;
      }

      if (workingDays >= 3) {
        const tasks = await getExistingTasks(lead.id);
        const hasTask = tasks.some(t => t.text && t.text.includes('Не удается выйти на клиента'));
        if (!hasTask) {
          await createTask(
            lead.id, responsibleId,
            `Не удается выйти на клиента уже ${workingDays} дня! Что планируешь делать?`
          );
        }
      }
    }

    if (statsUpdated) await updateStats();
    console.log('Проверка завершена');
  } catch (err) {
    console.error('Ошибка:', err.message);
    if (err.response) {
      console.error('URL:', err.config?.url);
      console.error('Status:', err.response.status);
      console.error('Data:', JSON.stringify(err.response.data));
    }
  }
}

async function checkTransferTasks() {
  if (checkTransferRunning) {
    console.log('checkTransferTasks уже выполняется — пропускаем параллельный запуск');
    return;
  }
  checkTransferRunning = true;
  try {
    await _checkTransferTasks();
  } finally {
    checkTransferRunning = false;
  }
}

async function _checkTransferTasks() {
  if (!TRANSFER_RECIPIENT_ID) return;
  const moscowHour = new Date(new Date().toLocaleString('en-US', { timeZone: 'Europe/Moscow' })).getHours();
  if (moscowHour >= 20 || moscowHour < 10) return;
  console.log(`\n=== Передача заявок ${new Date().toISOString()} ===`);
  try {
    const { distribute, allQueueUsers } = await getQueueData();
    if (!distribute.length) return;
    const userNameMap = new Map(allQueueUsers.map(u => [u.id, u.name]));
    const [assignments, lastAssignedId] = await Promise.all([getAssignmentsMap(), getLastAssignedId()]);
    const { countMap, historyMap } = assignments;
    // Фиксируем ответственных по всей воронке до обработки задач
    await seedHistoryFromAllPipelineLeads(historyMap);
    let currentLastAssignedId = lastAssignedId;
    const transferTasks = await getOpenTransferTasks();
    if (!transferTasks.length) { console.log('Задач "Назначить агента" нет'); return; }
    for (const task of transferTasks) {
      try {
        const { data: lead } = await amo.get(`/leads/${task.entity_id}`);
        if (lead.pipeline_id !== PIPELINE_ID) {
          console.log(`Задача ${task.id}: лид ${task.entity_id} не в нашей воронке, закрываем задачу`);
          if (!DRY_RUN) await amo.patch(`/tasks/${task.id}`, { is_completed: true, result: { text: 'Лид не в воронке' } }).catch(() => {});
          continue;
        }
        const responsibleId = lead.responsible_user_id;
        const fromUser = { id: responsibleId, name: userNameMap.get(responsibleId) || `User ${responsibleId}` };

        // Фиксируем текущего ответственного в историю до перераспределения
        if (!historyMap.get(lead.id)?.has(responsibleId)) {
          await writeAssignment(lead.id, responsibleId, fromUser.name, 'from');
          if (!historyMap.has(lead.id)) historyMap.set(lead.id, new Set());
          historyMap.get(lead.id).add(responsibleId);
        }
        console.log(`Сделка ${lead.id} (${lead.name}): задача "Назначить агента" → немедленное переназначение`);
        let redistributed = false;
        try {
          const result = await performRedistribution(lead, fromUser, distribute, countMap, currentLastAssignedId, historyMap, 'назначить агента');
          redistributed = true; // успешно: либо передан агенту, либо отправлен в архив
          if (result.newLastAssignedId) currentLastAssignedId = result.newLastAssignedId;
        } catch (e) {
          console.warn(`Сделка ${lead.id}: ошибка переназначения: ${e.message}`);
          if (e.response?.data) console.warn('Детали:', JSON.stringify(e.response.data));
        }
        // Закрываем задачу ТОЛЬКО если переназначение прошло успешно (retry при socket hang up)
        if (redistributed && !DRY_RUN) {
          let closed = false;
          for (let attempt = 1; attempt <= 5; attempt++) {
            try {
              await amo.patch(`/tasks/${task.id}`, { is_completed: true, result: { text: 'Выполнено ботом' } });
              console.log(`Задача ${task.id} закрыта`);
              closed = true;
              break;
            } catch (e) {
              console.warn(`Попытка ${attempt}/5 закрыть задачу ${task.id}: ${e.message}`);
              if (attempt < 5) await new Promise(r => setTimeout(r, 2000 * attempt));
            }
          }
          if (!closed) console.error(`Задача ${task.id}: не удалось закрыть за 5 попыток — будет повторно обработана на следующем запуске`);
        } else if (!redistributed) {
          console.warn(`Задача ${task.id}: переназначение не выполнено — задача остаётся открытой`);
        }
      } catch (e) {
        console.warn(`Ошибка задачи ${task.id}: ${e.message}`, e.response?.data);
      }
    }
    await updateStats();
  } catch (err) {
    console.error('Ошибка checkTransferTasks:', err.message);
  }
}

// ─── Cron ─────────────────────────────────────────────────────────────────────

// каждые 2 часа в рабочее время — синхронизация новых сотрудников из AmoCRM в таблицу
cron.schedule('0 9,11,13,15,17 * * 1-5', syncUsers, { timezone: 'Europe/Moscow' });

// 15:00 МСК — основная проверка лидов (Недозвон)
cron.schedule('0 15 * * 1-5', checkLeads, { timezone: 'Europe/Moscow' });

// каждые 5 минут — молниеносная передача по задаче "Назначить агента"
cron.schedule('*/5 * * * *', checkTransferTasks);

// ─── Routes ───────────────────────────────────────────────────────────────────

app.get('/', (req, res) => res.json({ status: 'ok', dry_run: DRY_RUN, time: new Date().toISOString() }));

app.post('/api/fix-log-names', async (req, res) => {
  try {
    const sheets = getSheetsClient();
    const { allQueueUsers } = await getQueueData();
    const nameMap = new Map(allQueueUsers.map(u => [u.id, u.name]));
    // Дополнительные известные ID не в queue
    if (req.body?.extra) {
      for (const [id, name] of Object.entries(req.body.extra)) nameMap.set(parseInt(id), name);
    }
    const logRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'log!A2:F5000' });
    const rows = logRes.data.values || [];
    let fixed = 0;
    const updated = rows.map((row, i) => {
      let changed = false;
      const newRow = [...row];
      for (const col of [3, 4]) {
        const val = row[col];
        if (val && val.startsWith('User ')) {
          const id = parseInt(val.replace('User ', ''));
          if (nameMap.has(id)) { newRow[col] = nameMap.get(id); changed = true; }
        }
      }
      if (changed) fixed++;
      return newRow;
    });
    if (fixed > 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID, range: 'log!A2',
        valueInputOption: 'RAW', requestBody: { values: updated }
      });
      await updateStats();
    }
    res.json({ fixed, message: `Исправлено ${fixed} записей в логе` });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/debug-tasks/:leadId', async (req, res) => {
  try {
    const tasks = await getExistingTasks(parseInt(req.params.leadId));
    res.json({
      TRANSFER_RECIPIENT_ID,
      taskCount: tasks.length,
      tasks: tasks.map(t => ({
        id: t.id,
        text: t.text,
        responsible_user_id: t.responsible_user_id,
        is_completed: t.is_completed,
        task_type_id: t.task_type_id
      }))
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/check', (req, res) => {
  checkLeads();
  res.json({ success: true, message: 'Проверка запущена' });
});

app.get('/api/queue', async (req, res) => {
  try {
    const { monitored, distribute } = await getQueueData();
    const lastId = await getLastAssignedId();
    res.json({ success: true, monitored, distribute, lastAssignedId: lastId });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/sync-users', async (req, res) => {
  try {
    const result = await syncUsers();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('Ошибка sync-users:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── Preview: показать лиды за 15–22 апреля без действий ─────────────────────

app.get('/api/preview', async (req, res) => {
  try {
    const PREVIEW_FROM = new Date('2026-04-15T00:00:00+03:00');
    const PREVIEW_TO   = new Date('2026-04-23T00:00:00+03:00');
    const PREVIEW_FROM_TS = Math.floor(PREVIEW_FROM.getTime() / 1000);
    const PREVIEW_TO_TS   = Math.floor(PREVIEW_TO.getTime()   / 1000);

    const { monitored, distribute } = await getQueueData();
    const monitoredIds = new Set(monitored.map(u => u.id));
    const monitoredMap = Object.fromEntries(monitored.map(u => [u.id, u]));

    const leads = await getLeadsInStage();
    const { tsMap: assignmentsMap } = await getAssignmentsMap();
    const nowTs = Math.floor(Date.now() / 1000);

    const result = [];

    for (const lead of leads) {
      const statusChangedTs = lead.status_changed_at || lead.created_at;

      // Только лиды попавшие в этап между 15 и 23 апреля
      if (statusChangedTs < PREVIEW_FROM_TS || statusChangedTs >= PREVIEW_TO_TS) continue;

      // Только если ответственный в monitored
      if (!monitoredIds.has(lead.responsible_user_id)) continue;

      const sinceTs = statusChangedTs;
      const workingDays = await getWorkingDaysBetween(sinceTs, nowTs);
      const responsible = monitoredMap[lead.responsible_user_id];

      result.push({
        id: lead.id,
        name: lead.name || '(без названия)',
        responsible: responsible?.name || lead.responsible_user_id,
        statusChangedAt: new Date(statusChangedTs * 1000).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }),
        workingDays,
        action: workingDays >= 5 ? '🔄 Переназначение' : workingDays >= 3 ? '📋 Задача' : '⏳ Ждём'
      });
    }

    result.sort((a, b) => b.workingDays - a.workingDays);

    res.json({
      success: true,
      total: result.length,
      monitored: monitored.map(u => u.name),
      distribute: distribute.map(u => u.name),
      leads: result
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Одноразовый ретроактивный фикс: восстановить 'from' историю из лога
app.post('/api/fix-history', async (req, res) => {
  try {
    const sheets = getSheetsClient();

    // Все сотрудники name→id
    const empRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'сотрудники!A2:B200' });
    const nameToId = new Map();
    for (const row of (empRes.data.values || [])) {
      if (row[0] && row[1]) nameToId.set(row[1], parseInt(row[0]));
    }

    // Текущие assignments
    const aRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'assignments!A2:D10000' });
    const existing = new Map(); // leadId → Set<userId>
    for (const row of (aRes.data.values || [])) {
      if (!row[0]) continue;
      const lid = parseInt(row[0]);
      const uid = parseInt(row[1]);
      if (!existing.has(lid)) existing.set(lid, new Set());
      existing.get(lid).add(uid);
    }

    // Читаем лог и собираем пропущенные 'from'
    const logRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'log!A2:F10000' });
    const ts = Math.floor(Date.now() / 1000);
    const toWrite = [];
    const seen = new Map(); // чтобы не дублировать в рамках этого запроса

    for (const row of (logRes.data.values || [])) {
      const fromName = row[3];
      if (!fromName || fromName === '~ Архив') continue;
      let lid;
      try { lid = parseInt(row[1]); } catch { continue; }
      const uid = nameToId.get(fromName);
      if (!uid) continue;
      const key = `${lid}_${uid}`;
      if (existing.get(lid)?.has(uid) || seen.has(key)) continue;
      seen.set(key, true);
      if (!existing.has(lid)) existing.set(lid, new Set());
      existing.get(lid).add(uid);
      toWrite.push([lid, uid, ts, 'from']);
    }

    if (toWrite.length > 0) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID, range: 'assignments!A:D',
        valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS',
        requestBody: { values: toWrite }
      });
    }

    res.json({ success: true, added: toWrite.length, message: `Добавлено ${toWrite.length} записей в историю` });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/update-stats', async (req, res) => {
  try {
    await updateStats();
    res.json({ success: true, message: 'Статистика обновлена' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/rollback', async (req, res) => {
  res.json({ success: true, message: 'Откат запущен' });
  try {
    const { monitored } = await getQueueData();
    const nameToId = {};
    monitored.forEach(u => { nameToId[u.name] = u.id; });
    const todayStart = Math.floor(new Date().setHours(0, 0, 0, 0) / 1000);
    const { data } = await amo.get('/events', {
      params: {
        'filter[entity_type]': 'leads',
        'filter[created_at][from]': todayStart,
        limit: 250, page: 1
      }
    });
    const events = (data._embedded?.events || [])
      .filter(e => e.type === 'lead_responsible_user_changed');
    const toRestore = {};
    for (const event of events) {
      const leadId = event.entity_id;
      const nameBefore = event.value_before?.[0]?.responsible_user?.name;
      if (!nameBefore || !nameToId[nameBefore]) continue;
      if (!toRestore[leadId]) toRestore[leadId] = nameToId[nameBefore];
    }
    const entries = Object.entries(toRestore);
    console.log(`Сделок для отката: ${entries.length}`);
    for (let i = 0; i < entries.length; i += 50) {
      const batch = entries.slice(i, i + 50);
      await amo.patch('/leads', batch.map(([id, userId]) => ({
        id: parseInt(id), responsible_user_id: userId,
        status_id: STAGE_NEDOZVON, pipeline_id: PIPELINE_ID
      })));
      await new Promise(r => setTimeout(r, 500));
    }
    console.log('Откат завершён');
  } catch (err) {
    console.error('Ошибка отката:', err.message);
  }
});

app.listen(PORT, () => {
  console.log(`Сервер на порту ${PORT} | DRY_RUN=${DRY_RUN}`);
});
