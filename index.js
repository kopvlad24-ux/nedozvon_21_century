const express = require('express');
const axios = require('axios');
const cron = require('node-cron');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

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

const DRY_RUN = process.env.DRY_RUN !== 'false';

// Лиды попавшие в НЕ дозвонился ДО этой даты — пропускаем
const WIDGET_START_DATE = new Date('2026-04-23T00:00:00+03:00');
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
  const monitored = rows
    .filter(r => r[2] && TRUTHY.includes(r[2].toString().toUpperCase()))
    .map(r => ({ id: parseInt(r[0]), name: r[1] }));
  const distribute = rows
    .filter(r => r[3] && TRUTHY.includes(r[3].toString().toUpperCase()))
    .map(r => ({ id: parseInt(r[0]), name: r[1] }));
  return { monitored, distribute };
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
      range: 'assignments!A2:C10000'
    });
    const rows = res.data.values || [];
    const tsMap = new Map();
    const historyMap = new Map();
    for (const row of rows) {
      const leadId = parseInt(row[0]);
      const userId = parseInt(row[1]);
      const ts = parseInt(row[2]);
      if (!leadId) continue;
      // Последняя дата назначения
      if (ts) {
        const existing = tsMap.get(leadId);
        if (!existing || ts > existing) tsMap.set(leadId, ts);
      }
      // История ответственных
      if (userId) {
        if (!historyMap.has(leadId)) historyMap.set(leadId, new Set());
        historyMap.get(leadId).add(userId);
      }
    }
    return { tsMap, historyMap };
  } catch (e) {
    console.warn('Не удалось прочитать assignments:', e.message);
    return { tsMap: new Map(), historyMap: new Map() };
  }
}

// Записываем новое назначение в assignments
async function writeAssignment(leadId, userId, userName) {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] assignments: лид ${leadId} → ${userName}`);
    return;
  }
  const sheets = getSheetsClient();
  const ts = Math.floor(Date.now() / 1000);
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: 'assignments!A:C',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [[leadId, userId, ts]] }
  });
}

// ─── Лог и статистика ─────────────────────────────────────────────────────────

async function writeLog(leadId, leadName, fromName, toName) {
  if (DRY_RUN) { console.log(`[DRY_RUN] Лог: ${leadId} от ${fromName} → ${toName}`); return; }
  const sheets = getSheetsClient();
  const now = new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: 'log!A:E',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [[now, leadId, leadName || '', fromName, toName]] }
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
    range: 'log!A2:E10000'
  });
  const rows = logRes.data.values || [];
  if (!rows.length) return;

  const MONTHS_RU = ['Январь','Февраль','Март','Апрель','Май','Июнь',
    'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь'];
  const weekStart = getWeekStart();

  function add(map, name, key) {
    if (!map[name]) map[name] = { received: 0, taken: 0 };
    map[name][key]++;
  }

  const weekStats = {};
  const allMonthStats = {};

  for (const row of rows) {
    const [dateStr, , , fromName, toName] = row;
    if (!dateStr || !fromName || !toName) continue;
    let date;
    try { date = parseRuDate(dateStr); } catch { continue; }

    if (date >= weekStart) {
      add(weekStats, fromName, 'taken');
      add(weekStats, toName, 'received');
    }

    const monthKey = `${MONTHS_RU[date.getMonth()]} ${date.getFullYear()}`;
    if (!allMonthStats[monthKey]) allMonthStats[monthKey] = {};
    add(allMonthStats[monthKey], fromName, 'taken');
    add(allMonthStats[monthKey], toName, 'received');
  }

  // Неделя
  const weekRows = [['Сотрудник', 'Получено лидов', 'Снято лидов']];
  for (const [name, s] of Object.entries(weekStats)) {
    weekRows.push([name, s.received, s.taken]);
  }
  await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'week!A:C' });
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
    monthRows.push([monthKey, '', '']);
    monthRows.push(['Сотрудник', 'Получено лидов', 'Снято лидов']);
    for (const [name, s] of Object.entries(allMonthStats[monthKey])) {
      monthRows.push([name, s.received, s.taken]);
    }
    monthRows.push(['', '', '']);
  }
  await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'month!A:C' });
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
    console.log(`Добавлено: ${newRows.map(r => r[1]).join(', ')}`);
  } else {
    console.log('Новых сотрудников нет');
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
  await amo.patch('/leads', [{
    id: lead.id,
    status_id: STAGE_ARCHIVE,
    pipeline_id: PIPELINE_ID
  }]);
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

// ─── Helpers ──────────────────────────────────────────────────────────────────

function getWorkingDaysBetween(startTs, endTs) {
  let count = 0;
  const cur = new Date(startTs * 1000);
  const end = new Date(endTs * 1000);
  cur.setHours(0, 0, 0, 0);
  end.setHours(0, 0, 0, 0);
  while (cur < end) {
    const day = cur.getDay();
    if (day !== 0 && day !== 6) count++;
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

async function reassignAndMove(lead, fromUser, nextUser) {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] Сделка ${lead.id} → ${nextUser.name}, этап → Новая заявка`);
    return;
  }

  // Меняем ответственного и этап в сделке
  await amo.patch('/leads', [{
    id: lead.id,
    responsible_user_id: nextUser.id,
    status_id: STAGE_NEW,
    pipeline_id: PIPELINE_ID
  }]);

  // Меняем ответственного в контактах
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
  } catch (e) {
    console.warn(`Не удалось переназначить контакты ${lead.id}: ${e.message}`);
  }

  // Комментарий
  await amo.post('/leads/notes', [{
    entity_id: lead.id,
    note_type: 'common',
    params: { text: `🔄 Лид передан по распределению → ${nextUser.name}` }
  }]);

  await writeLog(lead.id, lead.name, fromUser.name, nextUser.name);
  await writeAssignment(lead.id, nextUser.id, nextUser.name);
  console.log(`Сделка ${lead.id} → ${nextUser.name} | Новая заявка`);
}

async function archiveLead(lead, fromUser) {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] Сделка ${lead.id} → Архив (прошла по всем сотрудникам)`);
    return;
  }
  await amo.patch('/leads', [{
    id: lead.id,
    status_id: STAGE_ARCHIVE,
    pipeline_id: PIPELINE_ID
  }]);
  await amo.post('/leads/notes', [{
    entity_id: lead.id,
    note_type: 'common',
    params: { text: `📁 Лид отправлен в архив — прошёл через всех сотрудников без результата` }
  }]);
  console.log(`Сделка ${lead.id} → Архив`);
}

// ─── Основная проверка ────────────────────────────────────────────────────────

async function checkLeads() {
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
    const { tsMap: assignmentsMap, historyMap } = assignments;
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

      // Определяем точку отсчёта
      const assignedTs = assignmentsMap.get(lead.id); // когда виджет последний раз назначал
      const leadHistory = historyMap.get(lead.id); // кто уже был ответственным
      const statusChangedTs = lead.status_changed_at || lead.created_at;

      let sinceTs;
      if (assignedTs) {
        // Виджет назначал этот лид — считаем от этого момента
        sinceTs = assignedTs;
      } else if (statusChangedTs < WIDGET_START_TS) {
        // Лид попал в НЕ дозвонился ДО запуска виджета — пропускаем
        console.log(`Сделка ${lead.id}: был в этапе до запуска виджета, пропуск`);
        continue;
      } else {
        // Лид попал в НЕ дозвонился после запуска виджета — считаем от status_changed_at
        sinceTs = statusChangedTs;
      }

      const workingDays = getWorkingDaysBetween(sinceTs, nowTs);
      console.log(`Сделка ${lead.id} (${lead.name}): ${workingDays} рабочих дней`);

      if (workingDays >= 5) {
        const fromUser = monitoredMap[responsibleId];

        // Проверяем прошёл ли лид через всех сотрудников группы
        const allGroupIds = await getGroupUserIds();
        // Добавляем текущего ответственного в историю для проверки
        const fullHistory = leadHistory ? new Set([...leadHistory, responsibleId]) : new Set([responsibleId]);
        const allVisited = [...allGroupIds].every(id => fullHistory.has(id));

        if (allVisited) {
          // Все были — архив
          await archiveLead(lead);
          await writeLog(lead.id, lead.name, fromUser.name, '~ Архив');
          statsUpdated = true;
        } else {
          // Есть ещё кандидаты — переназначаем
          const nextUser = pickNextUser(distribute, currentLastAssignedId, responsibleId, leadHistory);
          await reassignAndMove(lead, fromUser, nextUser);
          await setLastAssignedId(nextUser.id);
          currentLastAssignedId = nextUser.id;
          statsUpdated = true;
        }
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

// ─── Cron: будни 12, 16, 19 МСК ──────────────────────────────────────────────

cron.schedule('0 12,16,19 * * 1-5', checkLeads, { timezone: 'Europe/Moscow' });

// ─── Routes ───────────────────────────────────────────────────────────────────

app.get('/', (req, res) => res.json({ status: 'ok', dry_run: DRY_RUN, time: new Date().toISOString() }));

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
  setTimeout(checkLeads, 5000);
});
