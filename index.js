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

async function getActiveQueue() {
  const sheets = getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'queue!A2:C100'
  });
  const rows = res.data.values || [];
  return rows
    .filter(r => r[2] && ['TRUE', 'ИСТИНА', '1', 'YES'].includes(r[2].toString().toUpperCase()))
    .map(r => ({ id: parseInt(r[0]), name: r[1] }));
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
  if (DRY_RUN) {
    console.log(`[DRY_RUN] Sheets: last_assigned_id = ${userId}`);
    return;
  }
  const sheets = getSheetsClient();
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: 'state!B2',
    valueInputOption: 'RAW',
    requestBody: { values: [[userId]] }
  });
}

// ─── Очередь ──────────────────────────────────────────────────────────────────

async function getNextUser(currentResponsibleId) {
  const queue = await getActiveQueue();
  if (!queue.length) throw new Error('Очередь пуста');

  const lastId = await getLastAssignedId();
  const lastIdx = queue.findIndex(u => u.id === lastId);

  for (let i = 1; i <= queue.length; i++) {
    const candidate = queue[(lastIdx + i) % queue.length];
    if (candidate.id !== currentResponsibleId) return candidate;
  }
  return queue[0];
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

async function buildResponsibleChangeMap() {
  const fromTs = Math.floor(Date.now() / 1000) - 30 * 24 * 3600;
  let page = 1;
  const map = new Map();
  while (true) {
    const { data } = await amo.get('/events', {
      params: {
        'filter[type]': 'lead_responsible_user_changed',
        'filter[created_at][from]': fromTs,
        limit: 250,
        page
      }
    });
    const events = data._embedded?.events || [];
    for (const event of events) {
      const leadId = event.entity_id;
      const ts = event.created_at;
      const userId = event.value_after?.[0]?.responsible_user?.id;
      if (!userId) continue;
      const existing = map.get(leadId);
      if (!existing || ts > existing.timestamp) {
        map.set(leadId, { userId, timestamp: ts });
      }
    }
    if (events.length < 250) break;
    page++;
  }
  return map;
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

async function reassignAndMove(lead, nextUser) {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] Сделка ${lead.id} → ${nextUser.name}, этап → Новая заявка`);
    return;
  }
  await amo.patch('/leads', [{
    id: lead.id,
    responsible_user_id: nextUser.id,
    status_id: STAGE_NEW,
    pipeline_id: PIPELINE_ID
  }]);
  await amo.post('/leads/notes', [{
    entity_id: lead.id,
    note_type: 'common',
    params: { text: `🔄 Лид передан по распределению → ${nextUser.name}` }
  }]);
  console.log(`Сделка ${lead.id} → ${nextUser.name} | Новая заявка`);
}

// ─── Основная проверка ────────────────────────────────────────────────────────

async function checkLeads() {
  console.log(`\n=== Проверка ${new Date().toISOString()}${DRY_RUN ? ' [DRY_RUN]' : ''} ===`);
  try {
    const queue = await getActiveQueue();
    if (!queue.length) { console.log('Очередь пуста'); return; }
    const queueIds = new Set(queue.map(u => u.id));
    console.log(`Очередь: ${queue.map(u => u.name).join(', ')}`);

    const [leads, eventMap] = await Promise.all([
      getLeadsInStage(),
      buildResponsibleChangeMap()
    ]);
    console.log(`Лидов в этапе НЕ дозвонился: ${leads.length}`);
    const nowTs = Math.floor(Date.now() / 1000);

    for (const lead of leads) {
      if (lead.pipeline_id !== PIPELINE_ID || lead.status_id !== STAGE_NEDOZVON) {
        console.warn(`ПРОПУСК ${lead.id} — неверный этап`);
        continue;
      }
      const responsibleId = lead.responsible_user_id;
      if (!queueIds.has(responsibleId)) {
        console.log(`Сделка ${lead.id}: ответственный не в очереди, пропуск`);
        continue;
      }

      const eventData = eventMap.get(lead.id);
      const responsibleSinceTs = eventData ? eventData.timestamp : lead.created_at;
      const workingDays = getWorkingDaysBetween(responsibleSinceTs, nowTs);
      console.log(`Сделка ${lead.id} (${lead.name}): ${workingDays} рабочих дней у ответственного`);

      if (workingDays >= 5) {
        const nextUser = await getNextUser(responsibleId);
        await reassignAndMove(lead, nextUser);
        await setLastAssignedId(nextUser.id);
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
    const queue = await getActiveQueue();
    const lastId = await getLastAssignedId();
    res.json({ success: true, queue, lastAssignedId: lastId });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/queue', async (req, res) => {
  const { users } = req.body;
  if (!Array.isArray(users)) return res.status(400).json({ error: 'users must be array' });
  try {
    const sheets = getSheetsClient();
    const rows = users.map(u => [u.id, u.name, u.active ? 'TRUE' : 'FALSE']);
    await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'queue!A2:C100' });
    if (rows.length) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'queue!A2',
        valueInputOption: 'RAW',
        requestBody: { values: rows }
      });
    }
    res.json({ success: true, users });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/rollback', async (req, res) => {
  res.json({ success: true, message: 'Откат запущен, смотри логи' });
  try {
    const queue = await getActiveQueue();
    const nameToId = {};
    queue.forEach(u => { nameToId[u.name] = u.id; });

    const todayStart = Math.floor(new Date().setHours(0, 0, 0, 0) / 1000);
    const { data } = await amo.get('/events', {
      params: {
        'filter[type]': 'lead_responsible_user_changed',
        'filter[created_at][from]': todayStart,
        limit: 250, page: 1
      }
    });
    const events = data._embedded?.events || [];
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
