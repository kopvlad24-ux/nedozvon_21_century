const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

const AMO_DOMAIN = process.env.AMO_DOMAIN;
const AMO_TOKEN = process.env.AMO_TOKEN;
const PIPELINE_ID = 10391694;
const STAGE_ID = 82141390;
const GROUP_ID = 689470;

const amo = axios.create({
  baseURL: `https://${AMO_DOMAIN}/api/v4`,
  headers: { Authorization: `Bearer ${AMO_TOKEN}` }
});

function getWorkingDaysBetween(startTs, endTs) {
  let count = 0;
  const start = new Date(startTs * 1000);
  const end = new Date(endTs * 1000);
  const cur = new Date(start);
  cur.setHours(0, 0, 0, 0);
  end.setHours(0, 0, 0, 0);
  while (cur < end) {
    const day = cur.getDay();
    if (day !== 0 && day !== 6) count++;
    cur.setDate(cur.getDate() + 1);
  }
  return count;
}

async function getGroupUsers() {
  const envUsers = process.env.QUEUE_USERS;
  if (envUsers) {
    try { return JSON.parse(envUsers); } catch(e) {}
  }
  const { data } = await amo.get('/users?limit=250');
  const users = data._embedded?.users || [];
  return users
    .filter(u => u.rights?.group_id === GROUP_ID && u.rights?.is_active === true)
    .map(u => ({ id: u.id, name: u.name }));
}

async function getLeadsInStage(pipelineId, stageId) {
  let page = 1;
  let leads = [];
  while (true) {
    const { data } = await amo.get(`/leads?filter[pipeline_id]=${pipelineId}&filter[status_id]=${stageId}&limit=250&page=${page}`);
    const batch = data._embedded?.leads || [];
    leads = leads.concat(batch);
    if (batch.length < 250) break;
    page++;
  }
  return leads;
}

async function createTask(leadId, responsibleUserId, text) {
  const dueDate = Math.floor(Date.now() / 1000) + 86400;
  await amo.post('/tasks', [{
    task_type_id: 1,
    text,
    complete_till: dueDate,
    entity_id: leadId,
    entity_type: 'leads',
    responsible_user_id: responsibleUserId
  }]);
  console.log(`✅ Задача создана для сделки ${leadId}`);
}

async function getExistingTasks(leadId) {
  const { data } = await amo.get(`/tasks?filter[entity_id]=${leadId}&filter[entity_type]=leads`);
  return data._embedded?.tasks || [];
}

async function reassignLead(leadId, newUserId, newUserName) {
  await amo.patch('/leads', [{
    id: leadId,
    responsible_user_id: newUserId
  }]);
  await amo.post('/notes', [{
    entity_id: leadId,
    entity_type: 'leads',
    note_type: 'common',
    params: { text: `🔄 Переназначение: лид висел 5 рабочих дней. Новый ответственный: ${newUserName}` }
  }]);
  console.log(`🔄 Сделка ${leadId} переназначена на ${newUserName}`);
}

let queueIndex = 0;

function getNextUserFromQueue(users, currentUserId) {
  const currentIdx = users.findIndex(u => u.id === currentUserId);
  const nextIdx = (currentIdx + 1) % users.length;
  queueIndex = nextIdx;
  return users[nextIdx];
}

async function checkLeads() {
  console.log(`\n[${new Date().toISOString()}] Запуск проверки сделок...`);
  try {
    const users = await getGroupUsers();
    if (!users.length) {
      console.log('⚠️ Сотрудники не найдены, пропускаем');
      return;
    }
    console.log(`👥 Сотрудники: ${users.map(u => u.name).join(', ')}`);
    const userIds = new Set(users.map(u => u.id));
    const leads = await getLeadsInStage(PIPELINE_ID, STAGE_ID);
    const nowTs = Math.floor(Date.now() / 1000);
    console.log(`Найдено ${leads.length} сделок`);
    for (const lead of leads) {
      const responsibleId = lead.responsible_user_id;
      if (!userIds.has(responsibleId)) continue;
      const stageEnteredAt = lead.status_changed_at || lead.created_at;
      const workingDays = getWorkingDaysBetween(stageEnteredAt, nowTs);
      console.log(`Сделка ${lead.id}: ${workingDays} рабочих дней`);
      if (workingDays >= 5) {
        const nextUser = getNextUserFromQueue(users, responsibleId);
        await reassignLead(lead.id, nextUser.id, nextUser.name);
        continue;
      }
      if (workingDays >= 3) {
        const tasks = await getExistingTasks(lead.id);
        const reminderExists = tasks.some(t => t.text?.includes('Что делаем с лидом'));
        if (!reminderExists) {
          await createTask(lead.id, responsibleId, `⏰ Что делаем с лидом? Сделка "${lead.name}" уже ${workingDays} рабочих дней в этапе "~ НЕ дозвонился".`);
        }
      }
    }
    console.log('✅ Проверка завершена\n');
  } catch (err) {
    console.error('❌ Ошибка:', err.message);
  }
}

cron.schedule('0 9,11,13,15,17 * * 1-5', checkLeads, { timezone: 'Europe/Moscow' });

app.get('/', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));
app.post('/webhook', (req, res) => res.sendStatus(200));
app.get('/api/users', async (req, res) => {
  try {
    const users = await getGroupUsers();
    res.json({ success: true, users, currentQueueIndex: queueIndex });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});
app.post('/api/users', (req, res) => {
  const { users } = req.body;
  if (!Array.isArray(users)) return res.status(400).json({ error: 'users must be array' });
  process.env.QUEUE_USERS = JSON.stringify(users);
  queueIndex = 0;
  res.json({ success: true, users });
});
app.post('/api/check', (req, res) => {
  checkLeads();
  res.json({ success: true, message: 'Проверка запущена' });
});

app.listen(PORT, () => {
  console.log(`🚀 Сервер запущен на порту ${PORT}`);
  setTimeout(checkLeads, 5000);
});
