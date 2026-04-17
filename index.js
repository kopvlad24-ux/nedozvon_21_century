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

async function getGroupUsers() {
  const envUsers = process.env.QUEUE_USERS;
  if (envUsers) {
    try { return JSON.parse(envUsers); } catch(e) {}
  }
  const { data } = await amo.get('/users?limit=250');
  const users = data._embedded?.users || [];
  return users
    .filter(function(u) { return u.rights && u.rights.group_id === GROUP_ID && u.rights.is_active === true; })
    .map(function(u) { return { id: u.id, name: u.name }; });
}

async function getLeadsInStage(pipelineId, stageId) {
  let page = 1;
  let leads = [];
  while (true) {
    const { data } = await amo.get('/leads', {
      params: {
        'filter[pipeline_id]': pipelineId,
        'filter[status_id]': stageId,
        limit: 250,
        page: page
      }
    });
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
    text: text,
    complete_till: dueDate,
    entity_id: leadId,
    entity_type: 'leads',
    responsible_user_id: responsibleUserId
  }]);
  console.log('Задача создана для сделки ' + leadId);
}

async function getExistingTasks(leadId) {
  try {
    const { data } = await amo.get('/tasks', {
      params: {
        'filter[entity_id]': leadId,
        'filter[entity_type]': 'leads'
      }
    });
    return data._embedded?.tasks || [];
  } catch(e) {
    return [];
  }
}

async function reassignLead(leadId, newUserId, newUserName) {
  await amo.patch('/leads', [{
    id: leadId,
    responsible_user_id: newUserId
  }]);
  console.log('Сделка ' + leadId + ' переназначена на ' + newUserName);
}

let queueIndex = 0;

function getNextUserFromQueue(users, currentUserId) {
  const currentIdx = users.findIndex(function(u) { return u.id === currentUserId; });
  const nextIdx = (currentIdx + 1) % users.length;
  queueIndex = nextIdx;
  return users[nextIdx];
}

async function checkLeads() {
  console.log('Запуск проверки ' + new Date().toISOString());
  try {
    const users = await getGroupUsers();
    if (!users.length) {
      console.log('Сотрудники не найдены');
      return;
    }
    console.log('Сотрудники: ' + users.map(function(u) { return u.name; }).join(', '));
    const userIds = new Set(users.map(function(u) { return u.id; }));
    const leads = await getLeadsInStage(PIPELINE_ID, STAGE_ID);
    const nowTs = Math.floor(Date.now() / 1000);
    console.log('Найдено сделок: ' + leads.length);
    for (let i = 0; i < leads.length; i++) {
      const lead = leads[i];
      const responsibleId = lead.responsible_user_id;
      if (!userIds.has(responsibleId)) continue;
      const stageEnteredAt = lead.status_changed_at || lead.created_at;
      const workingDays = getWorkingDaysBetween(stageEnteredAt, nowTs);
      console.log('Сделка ' + lead.id + ': ' + workingDays + ' рабочих дней');
      if (workingDays >= 5) {
        const nextUser = getNextUserFromQueue(users, responsibleId);
        await reassignLead(lead.id, nextUser.id, nextUser.name);
        continue;
      }
      if (workingDays >= 3) {
        const tasks = await getExistingTasks(lead.id);
        const reminderExists = tasks.some(function(t) {
          return t.text && t.text.includes('Что делаем с лидом');
        });
        if (!reminderExists) {
          await createTask(
            lead.id,
            responsibleId,
            'Что делаем с лидом? Сделка висит ' + workingDays + ' рабочих дней в этапе НЕ дозвонился.'
          );
        }
      }
    }
    console.log('Проверка завершена');
  } catch (err) {
    console.error('Ошибка: ' + err.message);
    if (err.response) {
      console.error('URL: ' + err.config?.url);
      console.error('Status: ' + err.response.status);
      console.error('Data: ' + JSON.stringify(err.response.data));
    }
  }
}

cron.schedule('0 9,11,13,15,17 * * 1-5', checkLeads, { timezone: 'Europe/Moscow' });

app.get('/', function(req, res) {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

app.post('/webhook', function(req, res) {
  res.sendStatus(200);
});

app.get('/api/users', async function(req, res) {
  try {
    const users = await getGroupUsers();
    res.json({ success: true, users: users, currentQueueIndex: queueIndex });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/users', function(req, res) {
  const users = req.body.users;
  if (!Array.isArray(users)) {
    return res.status(400).json({ error: 'users must be array' });
  }
  process.env.QUEUE_USERS = JSON.stringify(users);
  queueIndex = 0;
  res.json({ success: true, users: users });
});

app.post('/api/rollback', async function(req, res) {
  res.json({ success: true, message: 'Откат запущен, смотри логи' });
  try {
    const nameToId = {
      'Капанадзе Ольга': 13725758,
      'Кунцевич Виктория': 13695178,
      'Питаев Алексей': 13299234,
      'Хаджиева Амина': 13430842,
      'Данченкова Екатерина': 13430866,
      'Скуратова Валерия': 13503250,
      'Гордон Денис': 13515178,
      'Фатхуллов Рустем': 13324978,
      'Моджилло Татьяна': 13299178,
      'Матвеева Аделина': 13729438
    };
    const todayStart = Math.floor(new Date().setHours(0,0,0,0) / 1000);
    const { data } = await amo.get('/events', {
      params: {
        'filter[type]': 'lead_responsible_user_changed',
        'filter[created_at][from]': todayStart,
        limit: 250,
        page: 1
      }
    });
    const events = data._embedded?.events || [];
    console.log('Событий для отката: ' + events.length);
    const toRestore = {};
    for (let i = 0; i < events.length; i++) {
      const event = events[i];
      const leadId = event.entity_id;
      const valueBefore = event.value_before && event.value_before[0] && event.value_before[0].responsible_user && event.value_before[0].responsible_user.name;
      if (!valueBefore || !nameToId[valueBefore]) continue;
      if (!toRestore[leadId]) {
        toRestore[leadId] = nameToId[valueBefore];
      }
    }
    const entries = Object.entries(toRestore);
    console.log('Сделок для отката: ' + entries.length);
    for (let i = 0; i < entries.length; i += 50) {
      const batch = entries.slice(i, i + 50);
      const payload = batch.map(function(e) {
        return { id: parseInt(e[0]), responsible_user_id: e[1] };
      });
      await amo.patch('/leads', payload);
      console.log('Откатили ' + (i + batch.length) + ' из ' + entries.length);
      await new Promise(function(r) { setTimeout(r, 500); });
    }
    console.log('Откат завершён!');
  } catch(err) {
    console.error('Ошибка отката: ' + err.message);
    if (err.response) console.error(JSON.stringify(err.response.data));
  }
});

app.post('/api/check', function(req, res) {
  checkLeads();
  res.json({ success: true, message: 'Проверка запущена' });
});

app.listen(PORT, function() {
  console.log('Сервер запущен на порту ' + PORT);
  setTimeout(checkLeads, 5000);
});
