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
const GROUP_ID = 689470;

const DRY_RUN = process.env.DRY_RUN !== 'false';

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

// ─── Лог переназначений ───────────────────────────────────────────────────────

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

// ─── Обновление статистики ────────────────────────────────────────────────────

function getWeekStart() {
  const now = new Date();
  const day = now.getDay();
  const diff = (day === 0 ? -6 : 1 - day);
  const monday = new Date(now);
  monday.setDate(now.getDate() + diff);
  monday.setHours(0, 0, 0, 0);
  return monday;
}

function getMonthStart() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), 1);
}

function parseRuDate(str) {
  // Формат: "22.04.2026, 14:30:00"
  const [datePart] = str.split(', ');
  const [d, m, y] = datePart.split('.');
  return new Date(parseInt(y), parseInt(m) - 1, parseInt(d));
}

async function updateStats() {
  if (DRY_RUN) { console.log('[DRY_RUN] Пропуск обновления статистики'); return; }
  const sheets = getSheetsClient();

  // Читаем весь лог
  const logRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'log!A2:E10000'
  });
  const rows = logRes.data.values || [];
  if (!rows.length) return;

  const weekStart = getWeekStart();
  const monthStart = getMonthStart();

  const MONTHS_RU = ['Январь','Февраль','Март','Апрель','Май','Июнь',
    'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь'];

  function add(map, name, key) {
    if (!map[name]) map[name] = { received: 0, taken: 0 };
    map[name][key]++;
  }

  // Считаем статистику по неделе и по месяцам (все месяцы из лога)
  const weekStats = {};
  const allMonthStats = {}; // { 'Апрель 2026': { name: { received, taken } } }

  for (const row of rows) {
    const [dateStr, , , fromName, toName] = row;
    if (!dateStr || !fromName || !toName) continue;
    let date;
    try { date = parseRuDate(dateStr); } catch { continue; }

    // Неделя
    if (date >= weekStart) {
      add(weekStats, fromName, 'taken');
      add(weekStats, toName, 'received');
    }

    // Месяц — группируем по "Апрель 2026" и т.д.
    const monthKey = `${MONTHS_RU[date.getMonth()]} ${date.getFullYear()}`;
    if (!allMonthStats[monthKey]) allMonthStats[monthKey] = {};
    add(allMonthStats[monthKey], fromName, 'taken');
    add(allMonthStats[monthKey], toName, 'received');
  }

  // Записываем week (перезаписываем текущую неделю)
  const weekRows = [['Сотрудник', 'Получено лидов', 'Снято лидов']];
  for (const [name, s] of Object.entries(weekStats)) {
    weekRows.push([name, s.received, s.taken]);
  }
  await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'week!A:C' });
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: 'week!A1',
    valueInputOption: 'RAW',
    requestBody: { values: weekRows }
  });

  // Записываем month — все месяцы блоками
  const monthRows = [];
  const sortedMonths = Object.keys(allMonthStats).sort((a, b) => {
    // Сортируем хронологически
    const [mA, yA] = a.split(' ');
    const [mB, yB] = b.split(' ');
    const idxA = MONTHS_RU.indexOf(mA) + parseInt(yA) * 12;
    const idxB = MONTHS_RU.indexOf(mB) + parseInt(yB) * 12;
    return idxA - idxB;
  });

  for (const monthKey of sortedMonths) {
    monthRows.push([monthKey, '', '']);
    monthRows.push(['Сотрудник', 'Получено лидов', 'Снято лидов']);
    for (const [name, s] of Object.entries(allMonthStats[monthKey])) {
      monthRows.push([name, s.received, s.taken]);
    }
    monthRows.push(['', '', '']); // пустая строка между месяцами
  }

  await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'month!A:C' });
  if (monthRows.length) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: 'month!A1',
      valueInputOption: 'RAW',
      requestBody: { values: monthRows }
    });
  }

  console.log('Статистика обновлена');
}

// ─── Синхронизация сотрудников из AmoCRM ──────────────────────────────────────

async function syncUsers() {
  console.log('Синхронизация сотрудников из AmoCRM...');
  const sheets = getSheetsClient();

  // Получаем всех пользователей группы из AmoCRM
  const { data } = await amo.get('/users', { params: { limit: 250 } });
  const allUsers = data._embedded?.users || [];
  const groupUsers = allUsers.filter(u =>
    u.rights?.group_id === GROUP_ID && u.rights?.is_active === true
  );

  // Читаем текущий лист queue
  const queueRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'queue!A2:D100'
  });
  const existingRows = queueRes.data.values || [];
  const existingIds = new Set(existingRows.map(r => parseInt(r[0])));

  // Добавляем новых — только тех кого нет в таблице
  const newRows = groupUsers
    .filter(u => !existingIds.has(u.id))
    .map(u => [u.id, u.name, 'FALSE', 'FALSE']);

  if (newRows.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: 'queue!A2',
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: newRows }
    });
    console.log(`Добавлено новых сотрудников: ${newRows.length} — ${newRows.map(r => r[1]).join(', ')}`);
  } else {
    console.log('Новых сотрудников не найдено');
  }

  return { added: newRows.length, users: newRows.map(r => ({ id: r[0], name: r[1] })) };
}

// ─── Очередь ──────────────────────────────────────────────────────────────────

async function getNextUser(currentResponsibleId) {
  const { distribute } = await getQueueData();
  if (!distribute.length) throw new Error('Список выдачи пуст');
  const lastId = await getLastAssignedId();
  const lastIdx = distribute.findIndex(u => u.id === lastId);
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
        limit: 250,
        page
      }
    });
    const batch = data._embedded?.leads || [];
    const filtered = batch.filter(lead => {
      const ok = lead.pipeline_id === PIPELINE_ID && lead.status_id === STAGE_NEDOZVON;
      if (!ok) console.warn(`ПРОПУЩЕН лид ${lead.id} (pipeline=${lead.pipeline_id}, status=${lead.status_id})`);
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

  // Меняем ответственного в сделке и этап
  await amo.patch('/leads', [{
    id: lead.id,
    responsible_user_id: nextUser.id,
    status_id: STAGE_NEW,
    pipeline_id: PIPELINE_ID
  }]);

  // Меняем ответственного в контактах сделки
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
      console.log(`Контакты сделки ${lead.id} переназначены (${contacts.length} шт.)`);
    }
  } catch (e) {
    console.warn(`Не удалось переназначить контакты сделки ${lead.id}: ${e.message}`);
  }

  // Комментарий в карточку
  await amo.post('/leads/notes', [{
    entity_id: lead.id,
    note_type: 'common',
    params: { text: `🔄 Лид передан по распределению → ${nextUser.name}` }
  }]);

  await writeLog(lead.id, lead.name, fromUser.name, nextUser.name);
  console.log(`Сделка ${lead.id} → ${nextUser.name} | Новая заявка`);
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
    console.log(`Мониторим: ${monitored.map(u => u.name).join(', ')}`);
    console.log(`Выдаём: ${distribute.map(u => u.name).join(', ')}`);

    const leads = await getLeadsInStage();
    console.log(`Лидов в этапе НЕ дозвонился: ${leads.length}`);
    const nowTs = Math.floor(Date.now() / 1000);
    let statsUpdated = false;

    for (const lead of leads) {
      if (lead.pipeline_id !== PIPELINE_ID || lead.status_id !== STAGE_NEDOZVON) {
        console.warn(`ПРОПУСК ${lead.id} — неверный этап`);
        continue;
      }
      const responsibleId = lead.responsible_user_id;
      if (!monitoredIds.has(responsibleId)) {
        console.log(`Сделка ${lead.id}: ответственный не в очереди, пропуск`);
        continue;
      }

      const responsibleSinceTs = lead.status_changed_at || lead.created_at;
      const workingDays = getWorkingDaysBetween(responsibleSinceTs, nowTs);
      console.log(`Сделка ${lead.id} (${lead.name}): ${workingDays} рабочих дней`);

      if (workingDays >= 5) {
        const fromUser = monitoredMap[responsibleId];
        const nextUser = await getNextUser(responsibleId);
        await reassignAndMove(lead, fromUser, nextUser);
        await setLastAssignedId(nextUser.id);
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

// ─── Cron ─────────────────────────────────────────────────────────────────────

cron.schedule('0 9,11,13,15,17 * * 1-5', checkLeads, { timezone: 'Europe/Moscow' });

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

// Синхронизация сотрудников из AmoCRM → Sheets
app.post('/api/sync-users', async (req, res) => {
  try {
    const result = await syncUsers();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('Ошибка sync-users:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Пересчёт статистики вручную
app.post('/api/update-stats', async (req, res) => {
  try {
    await updateStats();
    res.json({ success: true, message: 'Статистика обновлена' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/rollback', async (req, res) => {
  res.json({ success: true, message: 'Откат запущен, смотри логи' });
  try {
    const { monitored } = await getQueueData();
    const nameToId = {};
    monitored.forEach(u => { nameToId[u.name] = u.id; });

    const todayStart = Math.floor(new Date().setHours(0, 0, 0, 0) / 1000);
    const { data } = await amo.get('/events', {
      params: {
        'filter[entity]': 'leads',
        'filter[created_at][from]': todayStart,
        limit: 250, page: 1
      }
    });
    const events = (data._embedded?.events || [])
      .filter(e => e.type === 'lead_responsible_user_changed');
    console.log(`Событий для отката: ${events.length}`);

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
      console.log(`Откатили ${i + batch.length} из ${entries.length}`);
      await new Promise(r => setTimeout(r, 500));
    }
    console.log('Откат завершён');
  } catch (err) {
    console.error('Ошибка отката:', err.message);
    if (err.response) console.error(JSON.stringify(err.response.data));
  }
});

app.listen(PORT, () => {
  console.log(`Сервер на порту ${PORT} | DRY_RUN=${DRY_RUN}`);
  setTimeout(checkLeads, 5000);
});
